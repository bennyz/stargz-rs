mod sectionreader;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, TimeZone, Utc};
use flate2::{read::GzDecoder, Compression, GzBuilder};
use sectionreader::SectionReader;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::{self, BufReader, BufWriter, Read, Write},
    os::unix::prelude::FileExt,
    path,
};
use tar::Archive;

/// Name of the JSON file in the tar archive containing the table of contents.
pub const TOC_TAR_NAME: &str = "stargz.index.json";

/// Size of the stargz footer in bytes.
pub const FOOTER_SIZE: usize = 47;

/// Default chunk size for splitting large files (4 MiB).
const DEFAULT_CHUNK_SIZE: usize = 4 << 20;

/// A Reader permits random access reads from a stargz file.
pub struct Reader<R: FileExt> {
    sr: R,
    size: u64,
    toc: JToc,
    m: HashMap<String, TOCEntry>,
    chunks: HashMap<String, Vec<TOCEntry>>,
}

impl<R: FileExt> Reader<R> {
    /// Opens a stargz file for reading.
    pub fn open(sr: R, size: u64) -> Result<Self> {
        if size < FOOTER_SIZE as u64 {
            return Err(anyhow!(
                "stargz size {} is smaller than the stargz footer size",
                size
            ));
        }

        let mut footer = [0u8; FOOTER_SIZE];
        sr.read_at(&mut footer, size - FOOTER_SIZE as u64)?;

        let toc_offset = parse_footer(&footer)?;

        let toc_size = size - toc_offset - FOOTER_SIZE as u64;
        let mut toc_targz = vec![0u8; toc_size as usize];
        sr.read_at(&mut toc_targz, toc_offset)?;

        let zr = GzDecoder::new(&toc_targz[..]);
        let mut archive = Archive::new(zr);
        let mut entries = archive.entries()?;
        let entry = entries
            .next()
            .ok_or_else(|| anyhow!("failed to find tar header in TOC gzip stream"))??;

        let header_name = entry.path()?;
        if header_name.to_str() != Some(TOC_TAR_NAME) {
            return Err(anyhow!(
                "TOC tar entry had name {:?}; expected {}",
                header_name,
                TOC_TAR_NAME
            ));
        }

        let toc: JToc = serde_json::from_reader(entry)?;

        let mut reader = Reader {
            sr,
            size,
            toc,
            m: HashMap::new(),
            chunks: HashMap::new(),
        };

        reader.init_fields()?;
        Ok(reader)
    }

    fn init_fields(&mut self) -> Result<()> {
        self.m = HashMap::with_capacity(self.toc.entries.len());
        self.chunks = HashMap::new();

        let mut last_path = String::new();
        let mut uname: HashMap<u32, String> = HashMap::new();
        let mut gname: HashMap<u32, String> = HashMap::new();
        let mut last_reg_size: Option<u64> = None;

        // First pass: process entries and build m/chunks maps
        for i in 0..self.toc.entries.len() {
            // Get a clone to work with to avoid borrow issues
            let mut entry = self.toc.entries[i].clone();
            entry.name = entry.name.trim_start_matches("./").to_string();

            if entry.entry_type == "reg" {
                last_reg_size = Some(entry.size);
            }

            if entry.entry_type == "chunk" {
                entry.name = last_path.clone();
                self.chunks
                    .entry(entry.name.clone())
                    .or_insert_with(Vec::new)
                    .push(entry.clone());

                if entry.chunk_size == 0 {
                    if let Some(reg_size) = last_reg_size {
                        entry.chunk_size = reg_size as i64 - entry.chunk_offset;
                    }
                }
            } else {
                last_path = entry.name.clone();

                if !entry.uname.is_empty() {
                    uname.insert(entry.uid, entry.uname.clone());
                } else if let Some(u) = uname.get(&entry.uid) {
                    entry.uname = u.clone();
                }

                if !entry.gname.is_empty() {
                    gname.insert(entry.gid, entry.gname.clone());
                } else if let Some(g) = gname.get(&entry.gid) {
                    entry.gname = g.clone();
                }

                if let Some(ref mod_time_str) = entry.mod_time_3339 {
                    if let Ok(dt) = DateTime::parse_from_rfc3339(mod_time_str) {
                        entry.mod_time = Some(dt.with_timezone(&Utc));
                    }
                }

                let key = if entry.entry_type == "dir" {
                    entry.num_link += 1;
                    entry.name.trim_end_matches('/').to_string()
                } else {
                    entry.name.clone()
                };
                self.m.insert(key, entry.clone());
            }

            if entry.entry_type == "reg" && entry.chunk_size > 0 && entry.chunk_size < entry.size as i64 {
                let cap = (entry.size as i64 / entry.chunk_size + 1) as usize;
                let mut chunks = Vec::with_capacity(cap);
                chunks.push(entry.clone());
                self.chunks.insert(entry.name.clone(), chunks);
            }

            if entry.chunk_size == 0 && entry.size != 0 {
                entry.chunk_size = entry.size as i64;
            }

            // Update the original entry
            self.toc.entries[i] = entry;
        }

        // Second pass: populate children and create implicit directories
        let entries_snapshot: Vec<TOCEntry> = self.toc.entries.clone();
        for entry in &entries_snapshot {
            if entry.entry_type == "chunk" {
                continue;
            }

            let name = if entry.entry_type == "dir" {
                entry.name.trim_end_matches('/').to_string()
            } else {
                entry.name.clone()
            };

            let parent = parent_dir(&name);
            self.get_or_create_dir(&parent);

            if let Some(e) = self.m.get_mut(&name) {
                e.num_link += 1;
            }

            let base = path::Path::new(&name)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();

            if entry.entry_type == "hardlink" {
                let link_name = entry.link_name.clone();
                let original_clone = self.m.get(&link_name).cloned();
                if let Some(original) = original_clone {
                    if let Some(orig_mut) = self.m.get_mut(&link_name) {
                        orig_mut.num_link += 1;
                    }
                    if let Some(pdir) = self.m.get_mut(&parent) {
                        pdir.add_child(&base, original);
                    }
                } else {
                    return Err(anyhow!(
                        "{} is a hardlink but the linkname {} isn't found",
                        entry.name,
                        link_name
                    ));
                }
            } else {
                if let Some(pdir) = self.m.get_mut(&parent) {
                    pdir.add_child(&base, entry.clone());
                }
            }
        }

        // Third pass: compute next_offset for each entry
        let mut last_offset = self.size as i64;
        for i in (0..self.toc.entries.len()).rev() {
            let entry = &mut self.toc.entries[i];
            if entry.is_data_type() {
                entry.next_offset = last_offset;
            }
            if entry.offset != 0 {
                last_offset = entry.offset;
            }
        }

        // Update m with next_offset values
        for entry in &self.toc.entries {
            if entry.entry_type != "chunk" {
                let key = if entry.entry_type == "dir" {
                    entry.name.trim_end_matches('/').to_string()
                } else {
                    entry.name.clone()
                };
                if let Some(e) = self.m.get_mut(&key) {
                    e.next_offset = entry.next_offset;
                }
            }
        }

        // Rebuild chunks HashMap with updated next_offset values
        self.chunks.clear();
        let mut last_path = String::new();
        for entry in &self.toc.entries {
            if entry.entry_type == "reg" && entry.chunk_size > 0 && entry.chunk_size < entry.size as i64 {
                last_path = entry.name.clone();
                self.chunks.insert(entry.name.clone(), vec![entry.clone()]);
            } else if entry.entry_type == "chunk" {
                self.chunks
                    .entry(last_path.clone())
                    .or_insert_with(Vec::new)
                    .push(entry.clone());
            } else if entry.entry_type == "reg" {
                last_path = entry.name.clone();
            }
        }

        Ok(())
    }

    fn get_or_create_dir(&mut self, d: &str) -> TOCEntry {
        if let Some(e) = self.m.get(d) {
            return e.clone();
        }

        let e = TOCEntry {
            name: d.to_string(),
            entry_type: "dir".to_string(),
            mode: 0o755,
            num_link: 2,
            ..Default::default()
        };
        self.m.insert(d.to_string(), e.clone());

        if !d.is_empty() {
            let parent = parent_dir(d);
            self.get_or_create_dir(&parent);
            let base = path::Path::new(d)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            if let Some(pdir) = self.m.get_mut(&parent) {
                pdir.add_child(&base, e.clone());
            }
        }

        e
    }

    pub fn lookup(&self, path: &str) -> Option<&TOCEntry> {
        let entry = self.m.get(path)?;
        if entry.entry_type == "hardlink" {
            return self.m.get(&entry.link_name);
        }
        Some(entry)
    }

    pub fn chunk_entry_for_offset(&self, name: &str, offset: i64) -> Option<&TOCEntry> {
        let entry = self.lookup(name)?;
        if !entry.is_data_type() {
            return None;
        }

        let chunks = match self.chunks.get(name) {
            Some(c) => c,
            None => {
                if offset >= entry.chunk_size {
                    return None;
                }
                return Some(entry);
            }
        };

        if chunks.len() < 2 {
            if offset >= entry.chunk_size {
                return None;
            }
            return Some(entry);
        }

        let i = chunks.iter().position(|e| {
            e.chunk_offset >= offset
                || (offset > e.chunk_offset && offset < e.chunk_offset + e.chunk_size)
        });

        match i {
            Some(idx) if idx < chunks.len() => Some(&chunks[idx]),
            _ => None,
        }
    }

    fn get_chunks(&self, entry: &TOCEntry) -> Vec<TOCEntry> {
        self.chunks
            .get(&entry.name)
            .cloned()
            .unwrap_or_else(|| vec![entry.clone()])
    }

    pub fn open_file(&self, name: &str) -> Result<FileReader<'_, R>> {
        let entry = self
            .lookup(name)
            .ok_or_else(|| anyhow!("file not found: {}", name))?;

        if entry.entry_type != "reg" {
            return Err(anyhow!("not a regular file: {}", name));
        }

        Ok(FileReader {
            reader: self,
            size: entry.size,
            ents: self.get_chunks(entry),
        })
    }

    pub fn toc(&self) -> &JToc {
        &self.toc
    }
}

pub struct FileReader<'a, R: FileExt> {
    reader: &'a Reader<R>,
    size: u64,
    ents: Vec<TOCEntry>,
}

impl<'a, R: FileExt> FileReader<'a, R> {
    pub fn read_at(&self, buf: &mut [u8], offset: i64) -> Result<usize> {
        if offset >= self.size as i64 {
            return Ok(0);
        }
        if offset < 0 {
            return Err(anyhow!("invalid offset"));
        }

        let mut i = 0;
        if self.ents.len() > 1 {
            i = self
                .ents
                .iter()
                .position(|e| e.chunk_offset >= offset)
                .unwrap_or(self.ents.len() - 1);
        }

        let mut entry = &self.ents[i];
        if entry.chunk_offset > offset {
            if i == 0 {
                return Err(anyhow!("internal error; first chunk offset is non-zero"));
            }
            entry = &self.ents[i - 1];
        }

        // Calculate offset within this chunk
        let offset_in_chunk = offset - entry.chunk_offset;

        let final_ent = &self.ents[self.ents.len() - 1];
        let gz_offset = entry.offset;
        let gz_bytes_remain = final_ent.next_offset - gz_offset;

        let sr = SectionReader::new(&self.reader.sr, gz_offset as u64, gz_bytes_remain as u64);

        const MAX_GZ_READ: usize = 2 << 20;
        let buf_size = std::cmp::min(MAX_GZ_READ, gz_bytes_remain as usize);

        let br = BufReader::with_capacity(buf_size, sr);
        let gz = GzDecoder::new(br);

        // Each gzip stream contains a tar entry - use tar reader to skip the header
        let mut archive = Archive::new(gz);
        let mut entries = archive.entries()?;
        let mut tar_entry = entries
            .next()
            .ok_or_else(|| anyhow!("no tar entry in gzip stream"))??;

        // Skip to the offset within the file data
        io::copy(&mut tar_entry.by_ref().take(offset_in_chunk as u64), &mut io::sink())?;

        // Calculate how much we can read from this chunk
        let chunk_size = if entry.chunk_size > 0 {
            entry.chunk_size
        } else {
            self.size as i64
        };
        let remaining_in_chunk = chunk_size - offset_in_chunk;
        let to_read = std::cmp::min(buf.len() as i64, remaining_in_chunk) as usize;

        // Read the data
        let mut total = 0;
        while total < to_read {
            let n = tar_entry.read(&mut buf[total..to_read])?;
            if n == 0 {
                break;
            }
            total += n;
        }
        Ok(total)
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

pub fn open<R: FileExt>(input: R, size: u64) -> Result<Reader<R>> {
    Reader::open(input, size)
}

fn parse_footer(content: &[u8]) -> Result<u64> {
    if content.len() != FOOTER_SIZE {
        return Err(anyhow!("footer size mismatch"));
    }

    let gz = GzDecoder::new(content);
    let extra = gz
        .header()
        .ok_or_else(|| anyhow!("no gzip header in footer"))?
        .extra()
        .ok_or_else(|| anyhow!("no extra field in footer"))?;

    if extra.len() != 16 + "STARGZ".len() {
        return Err(anyhow!("footer extra field has wrong length"));
    }

    if &extra[16..] != b"STARGZ" {
        return Err(anyhow!("footer not ending in STARGZ"));
    }

    let offset_str = std::str::from_utf8(&extra[..16])?;
    let toc_offset = u64::from_str_radix(offset_str, 16)?;

    Ok(toc_offset)
}

fn parent_dir(p: &str) -> String {
    match p.rfind('/') {
        Some(i) => p[..i].to_string(),
        None => String::new(),
    }
}

/// Generates the 47 byte footer containing the TOC offset.
pub fn footer_bytes(toc_offset: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_SIZE);
    let extra = format!("{:016x}STARGZ", toc_offset);
    let gz = GzBuilder::new()
        .extra(extra.as_bytes())
        .write(&mut buf, Compression::none());
    gz.finish().unwrap();
    assert_eq!(buf.len(), FOOTER_SIZE, "footer buffer size mismatch");
    buf
}

fn format_modtime(t: DateTime<Utc>) -> String {
    if t.timestamp() == 0 {
        return String::new();
    }
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn serialize_xattrs<S>(xattrs: &HashMap<String, Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(xattrs.len()))?;
    for (k, v) in xattrs {
        map.serialize_entry(k, &BASE64.encode(v))?;
    }
    map.end()
}

fn deserialize_xattrs<'de, D>(deserializer: D) -> Result<HashMap<String, Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let map: HashMap<String, String> = HashMap::deserialize(deserializer)?;
    let mut result = HashMap::new();
    for (k, v) in map {
        let decoded = BASE64.decode(&v).map_err(serde::de::Error::custom)?;
        result.insert(k, decoded);
    }
    Ok(result)
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JToc {
    pub version: u32,
    pub entries: Vec<TOCEntry>,
}

impl JToc {
    pub fn new(version: u32) -> Self {
        Self {
            version,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct TOCEntry {
    pub name: String,

    #[serde(rename = "type")]
    pub entry_type: String,

    #[serde(default, skip_serializing_if = "is_zero")]
    pub size: u64,

    #[serde(default, rename = "modtime", skip_serializing_if = "Option::is_none")]
    pub mod_time_3339: Option<String>,

    #[serde(skip)]
    pub mod_time: Option<DateTime<Utc>>,

    #[serde(default, skip_serializing_if = "is_zero")]
    pub mode: i64,

    #[serde(default, rename = "linkName", skip_serializing_if = "String::is_empty")]
    pub link_name: String,

    #[serde(default, skip_serializing_if = "is_zero")]
    pub uid: u32,

    #[serde(default, skip_serializing_if = "is_zero")]
    pub gid: u32,

    #[serde(default, rename = "userName", skip_serializing_if = "String::is_empty")]
    pub uname: String,

    #[serde(default, rename = "groupName", skip_serializing_if = "String::is_empty")]
    pub gname: String,

    #[serde(default, skip_serializing_if = "is_zero")]
    pub offset: i64,

    #[serde(skip)]
    pub next_offset: i64,

    #[serde(default, rename = "devMajor", skip_serializing_if = "is_zero")]
    pub dev_major: i64,

    #[serde(default, rename = "devMinor", skip_serializing_if = "is_zero")]
    pub dev_minor: i64,

    #[serde(skip)]
    pub num_link: u32,

    #[serde(
        default,
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "serialize_xattrs",
        deserialize_with = "deserialize_xattrs"
    )]
    pub xattrs: HashMap<String, Vec<u8>>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub digest: String,

    #[serde(default, rename = "chunkOffset", skip_serializing_if = "is_zero")]
    pub chunk_offset: i64,

    #[serde(default, rename = "chunkSize", skip_serializing_if = "is_zero")]
    pub chunk_size: i64,

    #[serde(skip)]
    pub children: HashMap<String, TOCEntry>,
}

fn is_zero<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

impl TOCEntry {
    pub fn mod_time(&self) -> Option<DateTime<Utc>> {
        self.mod_time
    }

    pub fn next_offset(&self) -> i64 {
        self.next_offset
    }

    pub fn add_child(&mut self, base_name: &str, child: TOCEntry) {
        if child.entry_type == "dir" {
            self.num_link += 1;
        }
        self.children.insert(base_name.to_string(), child);
    }

    pub fn lookup_child(&self, base_name: &str) -> Option<&TOCEntry> {
        self.children.get(base_name)
    }

    pub fn is_data_type(&self) -> bool {
        self.entry_type == "reg" || self.entry_type == "chunk"
    }

    pub fn foreach_child<F>(&self, mut f: F)
    where
        F: FnMut(&str, &TOCEntry) -> bool,
    {
        for (name, entry) in &self.children {
            if !f(name, entry) {
                return;
            }
        }
    }

    pub fn is_dir(&self) -> bool {
        self.entry_type == "dir"
    }

    pub fn is_symlink(&self) -> bool {
        self.entry_type == "symlink"
    }

    pub fn is_regular(&self) -> bool {
        self.entry_type == "reg"
    }
}

pub struct CountingWriter<W: Write> {
    inner: W,
    count: u64,
}

impl<W: Write> CountingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }

    pub fn count(&self) -> u64 {
        self.count
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.count += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// A Writer writes stargz files.
pub struct Writer<W: Write> {
    cw: CountingWriter<BufWriter<W>>,
    toc: JToc,
    diff_hash: Sha256,
    last_username: HashMap<u32, String>,
    last_groupname: HashMap<u32, String>,
    chunk_size: usize,
    closed: bool,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        let bw = BufWriter::new(writer);
        let cw = CountingWriter::new(bw);
        Self {
            cw,
            toc: JToc::new(1),
            diff_hash: Sha256::new(),
            last_username: HashMap::new(),
            last_groupname: HashMap::new(),
            chunk_size: 0,
            closed: false,
        }
    }

    pub fn set_chunk_size(&mut self, size: usize) {
        self.chunk_size = size;
    }

    fn chunk_size(&self) -> usize {
        if self.chunk_size == 0 {
            DEFAULT_CHUNK_SIZE
        } else {
            self.chunk_size
        }
    }

    fn name_if_changed(cache: &mut HashMap<u32, String>, id: u32, name: &str) -> String {
        if name.is_empty() {
            return String::new();
        }
        if cache.get(&id).map(|s| s.as_str()) == Some(name) {
            return String::new();
        }
        cache.insert(id, name.to_string());
        name.to_string()
    }

    fn write_gz_stream(&mut self, data: &[u8]) -> Result<()> {
        let gz = GzBuilder::new().write(&mut self.cw, Compression::best());
        let mut gz = gz;
        gz.write_all(data)?;
        gz.finish()?;
        Ok(())
    }

    pub fn append_tar<RD: Read>(&mut self, r: RD) -> Result<()> {
        let mut br = BufReader::new(r);

        let mut peek = [0u8; 3];
        if br.read(&mut peek)? < 3 {
            return Ok(()); // Empty input
        }
        let is_gzipped = peek[0] == 0x1f && peek[1] == 0x8b && peek[2] == 0x08;

        let full_reader = io::Cursor::new(peek).chain(br);

        let mut archive: Archive<Box<dyn Read>> = if is_gzipped {
            let gz = GzDecoder::new(full_reader);
            Archive::new(Box::new(gz))
        } else {
            Archive::new(Box::new(full_reader))
        };

        for entry_result in archive.entries()? {
            let mut entry = entry_result?;
            let path = entry.path()?.to_string_lossy().to_string();

            if path == TOC_TAR_NAME {
                continue;
            }

            // Clone the header data we need before any mutable borrows
            let entry_type = entry.header().entry_type();
            let size = entry.header().size()?;
            let mode = entry.header().mode()?;
            let uid = entry.header().uid()? as u32;
            let gid = entry.header().gid()? as u32;
            let mtime = entry.header().mtime()?;
            let username = entry.header().username()?.unwrap_or("").to_string();
            let groupname = entry.header().groupname()?.unwrap_or("").to_string();
            let link_name = entry.header().link_name()?.map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
            let dev_major = entry.header().device_major().ok().flatten().unwrap_or(0) as i64;
            let dev_minor = entry.header().device_minor().ok().flatten().unwrap_or(0) as i64;

            // Now we can mutably borrow for pax_extensions
            let mut xattrs: HashMap<String, Vec<u8>> = HashMap::new();
            if let Some(exts) = entry.pax_extensions()? {
                for ext in exts {
                    let ext = ext?;
                    if let Some(key) = ext.key().ok() {
                        if let Some(stripped) = key.strip_prefix("SCHILY.xattr.") {
                            xattrs.insert(stripped.to_string(), ext.value_bytes().to_vec());
                        }
                    }
                }
            }

            let mod_time = Utc.timestamp_opt(mtime as i64, 0).single().unwrap_or_else(Utc::now);

            let uname = Self::name_if_changed(&mut self.last_username, uid, &username);
            let gname = Self::name_if_changed(&mut self.last_groupname, gid, &groupname);

            let mut toc_entry = TOCEntry {
                name: path.clone(),
                mode: mode as i64,
                uid,
                gid,
                uname,
                gname,
                mod_time_3339: Some(format_modtime(mod_time)),
                xattrs,
                ..Default::default()
            };

            match entry_type {
                tar::EntryType::Link => {
                    toc_entry.entry_type = "hardlink".to_string();
                    toc_entry.link_name = link_name.clone();
                }
                tar::EntryType::Symlink => {
                    toc_entry.entry_type = "symlink".to_string();
                    toc_entry.link_name = link_name.clone();
                }
                tar::EntryType::Directory => {
                    toc_entry.entry_type = "dir".to_string();
                }
                tar::EntryType::Regular | tar::EntryType::Continuous => {
                    toc_entry.entry_type = "reg".to_string();
                    toc_entry.size = size;
                }
                tar::EntryType::Char => {
                    toc_entry.entry_type = "char".to_string();
                    toc_entry.dev_major = dev_major;
                    toc_entry.dev_minor = dev_minor;
                }
                tar::EntryType::Block => {
                    toc_entry.entry_type = "block".to_string();
                    toc_entry.dev_major = dev_major;
                    toc_entry.dev_minor = dev_minor;
                }
                tar::EntryType::Fifo => {
                    toc_entry.entry_type = "fifo".to_string();
                }
                other => {
                    return Err(anyhow!("unsupported input tar entry type: {:?}", other));
                }
            }

            if entry_type == tar::EntryType::Regular && size > 0 {
                let total_size = size;
                let mut written = 0u64;
                let mut payload_digest = Sha256::new();
                let mut first_entry = Some(toc_entry.clone());

                while written < total_size {
                    let chunk_size = std::cmp::min(self.chunk_size() as u64, total_size - written);

                    let mut current_entry = if let Some(fe) = first_entry.take() {
                        fe
                    } else {
                        TOCEntry {
                            name: path.clone(),
                            entry_type: "chunk".to_string(),
                            ..Default::default()
                        }
                    };

                    current_entry.offset = self.cw.count() as i64;
                    current_entry.chunk_offset = written as i64;
                    if chunk_size < total_size - written {
                        current_entry.chunk_size = chunk_size as i64;
                    }

                    // Read chunk from entry
                    let mut chunk_data = vec![0u8; chunk_size as usize];
                    entry.read_exact(&mut chunk_data)?;
                    payload_digest.update(&chunk_data);
                    self.diff_hash.update(&chunk_data);

                    // Build a tar entry in a gzip stream
                    let mut tar_buf = Vec::new();
                    {
                        let mut tw = tar::Builder::new(&mut tar_buf);
                        let mut h = tar::Header::new_gnu();
                        h.set_path(&path)?;
                        if written == 0 {
                            h.set_size(total_size);
                        } else {
                            h.set_size(chunk_size);
                        }
                        h.set_mode(mode);
                        h.set_uid(uid as u64);
                        h.set_gid(gid as u64);
                        h.set_mtime(mtime);
                        h.set_entry_type(tar::EntryType::Regular);
                        h.set_cksum();
                        tw.append(&h, &chunk_data[..])?;
                        tw.finish()?;
                    }

                    self.write_gz_stream(&tar_buf)?;
                    self.toc.entries.push(current_entry);
                    written += chunk_size;
                }

                // Set digest on the first entry
                if let Some(first) = self.toc.entries.iter_mut().rev().find(|e| e.name == path && e.entry_type == "reg") {
                    first.digest = format!("sha256:{:x}", payload_digest.finalize());
                }
            } else {
                // Non-regular files or empty files
                let mut tar_buf = Vec::new();
                {
                    let mut tw = tar::Builder::new(&mut tar_buf);
                    let mut h = tar::Header::new_gnu();
                    h.set_path(&path)?;
                    h.set_size(0);
                    h.set_mode(mode);
                    h.set_uid(uid as u64);
                    h.set_gid(gid as u64);
                    h.set_mtime(mtime);
                    h.set_entry_type(entry_type);
                    if !link_name.is_empty() {
                        h.set_link_name(&link_name)?;
                    }
                    h.set_cksum();
                    tw.append(&h, io::empty())?;
                    tw.finish()?;
                }

                self.write_gz_stream(&tar_buf)?;
                self.toc.entries.push(toc_entry);
            }
        }

        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        let toc_offset = self.cw.count();

        // Create TOC JSON
        let toc_json = serde_json::to_vec_pretty(&self.toc)?;

        // Create tar containing the TOC JSON
        let mut tar_buf = Vec::new();
        {
            let mut tw = tar::Builder::new(&mut tar_buf);
            let mut h = tar::Header::new_gnu();
            h.set_path(TOC_TAR_NAME)?;
            h.set_size(toc_json.len() as u64);
            h.set_mode(0o644);
            h.set_entry_type(tar::EntryType::Regular);
            h.set_cksum();
            tw.append(&h, &toc_json[..])?;
            tw.finish()?;
        }

        // Write TOC gzip stream
        let toc_gz = GzBuilder::new()
            .extra(b"stargz.toc")
            .write(Vec::new(), Compression::best());
        let mut toc_gz = toc_gz;
        toc_gz.write_all(&tar_buf)?;
        let toc_data = toc_gz.finish()?;
        self.diff_hash.update(&toc_data);
        self.cw.write_all(&toc_data)?;

        // Write the footer
        let footer = footer_bytes(toc_offset);
        self.cw.write_all(&footer)?;
        self.cw.flush()?;

        self.closed = true;
        Ok(())
    }

    pub fn diff_id(&self) -> String {
        format!("sha256:{:x}", self.diff_hash.clone().finalize())
    }
}

impl<W: Write> Drop for Writer<W> {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close();
        }
    }
}

/// A wrapper around a byte slice that implements FileExt for in-memory testing.
pub struct MemReader {
    data: Vec<u8>,
}

impl MemReader {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl FileExt for MemReader {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let offset = offset as usize;
        if offset >= self.data.len() {
            return Ok(0);
        }
        let available = &self.data[offset..];
        let to_read = std::cmp::min(buf.len(), available.len());
        buf[..to_read].copy_from_slice(&available[..to_read]);
        Ok(to_read)
    }

    fn write_at(&self, _buf: &[u8], _offset: u64) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::PermissionDenied, "read-only"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_tar(entries: Vec<(&str, &[u8], tar::EntryType)>) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut ar = tar::Builder::new(&mut buf);
            for (name, content, entry_type) in entries {
                let mut h = tar::Header::new_gnu();
                h.set_path(name).unwrap();
                h.set_size(content.len() as u64);
                h.set_mode(0o644);
                h.set_uid(1000);
                h.set_gid(1000);
                h.set_mtime(0);
                h.set_entry_type(entry_type);
                h.set_cksum();
                ar.append(&h, content).unwrap();
            }
            ar.finish().unwrap();
        }
        buf
    }

    #[test]
    fn test_footer_roundtrip() {
        for offset in (0..=200000).step_by(1023) {
            let footer = footer_bytes(offset);
            assert_eq!(footer.len(), FOOTER_SIZE);
            let parsed = parse_footer(&footer).unwrap();
            assert_eq!(parsed, offset, "offset {} failed roundtrip", offset);
        }
    }

    #[test]
    fn test_write_and_read_empty() {
        let mut output = Vec::new();
        {
            let mut w = Writer::new(&mut output);
            let tar_data = create_test_tar(vec![]);
            w.append_tar(Cursor::new(tar_data)).unwrap();
            w.close().unwrap();
        }

        let mem = MemReader::new(output);
        let size = mem.len();
        let reader = Reader::open(mem, size).unwrap();
        assert_eq!(reader.toc().entries.len(), 0);
    }

    #[test]
    fn test_write_and_read_single_file() {
        let content = b"Hello, World!";
        let mut output = Vec::new();
        {
            let mut w = Writer::new(&mut output);
            let tar_data = create_test_tar(vec![("hello.txt", content, tar::EntryType::Regular)]);
            w.append_tar(Cursor::new(tar_data)).unwrap();
            w.close().unwrap();
        }

        let mem = MemReader::new(output);
        let size = mem.len();
        let reader = Reader::open(mem, size).unwrap();

        let entry = reader.lookup("hello.txt").expect("file not found");
        assert_eq!(entry.entry_type, "reg");
        assert_eq!(entry.size, content.len() as u64);
    }

    #[test]
    fn test_write_and_read_directory() {
        let mut output = Vec::new();
        {
            let mut w = Writer::new(&mut output);
            let tar_data = create_test_tar(vec![("foo/", &[], tar::EntryType::Directory)]);
            w.append_tar(Cursor::new(tar_data)).unwrap();
            w.close().unwrap();
        }

        let mem = MemReader::new(output);
        let size = mem.len();
        let reader = Reader::open(mem, size).unwrap();

        let entry = reader.lookup("foo").expect("dir not found");
        assert_eq!(entry.entry_type, "dir");
    }

    #[test]
    fn test_parse_footer_invalid() {
        let invalid = [0u8; FOOTER_SIZE];
        assert!(parse_footer(&invalid).is_err());
    }

    #[test]
    fn test_write_and_read_file_contents() {
        let content = b"This is some test content that we want to read back";
        let mut output = Vec::new();
        {
            let mut w = Writer::new(&mut output);
            let tar_data = create_test_tar(vec![("test.txt", content, tar::EntryType::Regular)]);
            w.append_tar(Cursor::new(tar_data)).unwrap();
            w.close().unwrap();
        }

        let mem = MemReader::new(output);
        let size = mem.len();
        let reader = Reader::open(mem, size).unwrap();

        let file = reader.open_file("test.txt").unwrap();
        assert_eq!(file.size(), content.len() as u64);

        let mut buf = vec![0u8; content.len()];
        let n = file.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, content.len());
        assert_eq!(&buf, content);

        let mut partial = vec![0u8; 10];
        let n = file.read_at(&mut partial, 5).unwrap();
        assert_eq!(n, 10);
        assert_eq!(&partial, &content[5..15]);
    }

    #[test]
    fn test_write_and_read_nested_dirs() {
        let content = b"nested file content";
        let mut output = Vec::new();
        {
            let mut w = Writer::new(&mut output);
            let tar_data = create_test_tar(vec![
                ("foo/", &[], tar::EntryType::Directory),
                ("foo/bar/", &[], tar::EntryType::Directory),
                ("foo/bar/baz.txt", content, tar::EntryType::Regular),
            ]);
            w.append_tar(Cursor::new(tar_data)).unwrap();
            w.close().unwrap();
        }

        let mem = MemReader::new(output);
        let size = mem.len();
        let reader = Reader::open(mem, size).unwrap();

        let foo = reader.lookup("foo").expect("foo not found");
        assert!(foo.is_dir());

        let bar = reader.lookup("foo/bar").expect("foo/bar not found");
        assert!(bar.is_dir());

        let baz = reader.lookup("foo/bar/baz.txt").expect("baz.txt not found");
        assert_eq!(baz.entry_type, "reg");
        assert_eq!(baz.size, content.len() as u64);

        assert!(foo.children.contains_key("bar"));
        assert!(bar.children.contains_key("baz.txt"));
    }

    #[test]
    fn test_symlink() {
        let mut output = Vec::new();
        {
            let mut w = Writer::new(&mut output);
            let mut buf = Vec::new();
            {
                let mut ar = tar::Builder::new(&mut buf);

                let mut h = tar::Header::new_gnu();
                h.set_path("foo/").unwrap();
                h.set_size(0);
                h.set_mode(0o755);
                h.set_uid(1000);
                h.set_gid(1000);
                h.set_mtime(0);
                h.set_entry_type(tar::EntryType::Directory);
                h.set_cksum();
                ar.append(&h, io::empty()).unwrap();

                let mut h = tar::Header::new_gnu();
                h.set_path("foo/link").unwrap();
                h.set_size(0);
                h.set_mode(0o777);
                h.set_uid(1000);
                h.set_gid(1000);
                h.set_mtime(0);
                h.set_entry_type(tar::EntryType::Symlink);
                h.set_link_name("../target").unwrap();
                h.set_cksum();
                ar.append(&h, io::empty()).unwrap();

                ar.finish().unwrap();
            }
            w.append_tar(Cursor::new(buf)).unwrap();
            w.close().unwrap();
        }

        let mem = MemReader::new(output);
        let size = mem.len();
        let reader = Reader::open(mem, size).unwrap();

        let link = reader.lookup("foo/link").expect("link not found");
        assert!(link.is_symlink());
        assert_eq!(link.link_name, "../target");
    }

    #[test]
    fn test_chunked_file() {
        let content: Vec<u8> = (0..100u8).cycle().take(1000).collect();
        let mut output = Vec::new();
        {
            let mut w = Writer::new(&mut output);
            w.set_chunk_size(100);
            let tar_data = create_test_tar(vec![("big.txt", &content, tar::EntryType::Regular)]);
            w.append_tar(Cursor::new(tar_data)).unwrap();
            w.close().unwrap();
        }

        let mem = MemReader::new(output);
        let size = mem.len();
        let reader = Reader::open(mem, size).unwrap();

        let entry = reader.lookup("big.txt").expect("file not found");
        assert_eq!(entry.size, 1000);

        assert!(reader.toc().entries.len() >= 10);

        let file = reader.open_file("big.txt").unwrap();

        let mut buf = vec![0u8; 50];
        let n = file.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 50);
        assert_eq!(&buf[..], &content[..50]);

        let n = file.read_at(&mut buf, 500).unwrap();
        assert_eq!(n, 50);
        assert_eq!(&buf[..], &content[500..550]);
    }

    #[test]
    fn test_diff_id() {
        let content = b"test content for diff id";
        let mut output = Vec::new();
        let diff_id;
        {
            let mut w = Writer::new(&mut output);
            let tar_data = create_test_tar(vec![("file.txt", content, tar::EntryType::Regular)]);
            w.append_tar(Cursor::new(tar_data)).unwrap();
            w.close().unwrap();
            diff_id = w.diff_id();
        }

        assert!(diff_id.starts_with("sha256:"));
        assert_eq!(diff_id.len(), 7 + 64);
    }
}

mod sectionreader;
use anyhow::{anyhow, Ok, Result};
use chrono::{TimeZone, Utc};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use sectionreader::SectionReader;
use serde::Deserialize;
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File},
    io::Read,
    io::{self, BufReader, BufWriter, Write},
    os::unix::prelude::{FileExt, MetadataExt, PermissionsExt},
    rc::Rc,
    vec,
};
use tar::Archive;

static TOCT_TAR_NAME: &str = "stargz.index.json";
const FOOTER_SIZE: u32 = 47;

pub struct GzReader {
    sr: File,
    toc: JToc,
    m: HashMap<String, TocEntry>,
    chunks: HashMap<String, Vec<TocEntry>>,
}

impl GzReader {
    fn init_fields(&mut self) -> Result<()> {
        self.m = HashMap::with_capacity(self.toc.entries.len());
        self.chunks = HashMap::new();
        let mut last_reg_entry: Option<TocEntry> = None;
        let mut last_path: &str = "";
        let mut uname = HashMap::<u32, String>::new();
        let mut gname = HashMap::<u32, String>::new();
        for mut entry in &mut self.toc.entries.clone() {
            entry.name = entry.name.trim_start_matches("./").to_owned();
            match entry.entry_type.as_str() {
                "reg" => {
                    last_reg_entry = Some(entry.clone());
                }
                "chunk" => {
                    entry.name = last_path.to_owned();
                    match self.chunks.get_mut(&entry.name) {
                        Some(v) => {
                            v.push(entry.clone());
                        }
                        None => {
                            self.chunks
                                .insert(entry.name.to_owned(), vec![entry.clone()]);
                        }
                    };
                    if entry.chunk_size == 0 && last_reg_entry.is_some() {
                        let last_ent_size = last_reg_entry.clone().unwrap().size;
                        entry.chunk_size = last_ent_size - entry.chunk_offset;
                    }
                }
                _ => {
                    last_path = &entry.name;
                    match entry.uname.as_str() {
                        "" => {
                            entry.uname = uname.get(&entry.uid).unwrap().to_string();
                        }
                        _ => {
                            uname.insert(entry.uid, entry.uname.clone());
                        }
                    }
                    match entry.gname.as_str() {
                        "" => {
                            entry.gname = gname.get(&entry.gid).unwrap().to_string();
                        }
                        _ => {
                            gname.insert(entry.gid, entry.gname.clone());
                        }
                    }

                    if entry.mod_time_3339.is_some() {
                        entry.mod_time = Some(
                            chrono::DateTime::parse_from_rfc3339(
                                &entry.mod_time_3339.as_ref().unwrap(),
                            )?
                            .into(),
                        );
                    }
                    if entry.entry_type == "dir" {
                        entry.num_link += 1;
                        self.m
                            .insert(entry.name.trim_end_matches("/").to_owned(), entry.clone());
                    } else {
                        self.m.insert(entry.name.to_owned(), entry.clone());
                    }
                }
            }

            if entry.entry_type == "reg" && entry.chunk_size > 0 && entry.chunk_size < entry.size {
                let cap = (entry.size / entry.chunk_size + 1) as usize;
                let mut chunks: Vec<TocEntry> = Vec::with_capacity(cap);
                chunks.push(entry.clone());
                self.chunks.insert(entry.name.to_owned(), chunks);
            }
            if entry.chunk_size == 0 && entry.size != 0 {
                entry.chunk_size = entry.size;
            }

            for entry in &mut self.toc.entries.clone() {
                if entry.entry_type == "chunk" {
                    continue;
                }
                let mut name = entry.name.to_owned();
                if entry.entry_type == "dir" {
                    let bind = name.trim_end_matches("/").to_owned();
                    name = bind;
                }

                let mut parent_dir = self.get_or_create_parent_dir(&name);
                entry.num_link += 1;
                if entry.entry_type == "hardlink" {
                    let link_name = entry.link_name.clone();
                    match self.m.get_mut(&link_name) {
                        Some(original) => original.num_link += 1,
                        None => {
                            return Err(anyhow!(
                                "{0} is a hardlink but the linkname {link_name} isn't found",
                                entry.name
                            ))
                        }
                    };
                }
                parent_dir.add_child(entry.clone(), &name);
            }

            let mut last_offset = self.sr.metadata().unwrap().size();
            for i in (0..self.toc.entries.len()).rev() {
                match self.toc.entries.get_mut(i) {
                    Some(e) => {
                        if e.is_data_type() {
                            e.next_offset = last_offset;
                        }
                        if e.offset != 0 {
                            last_offset = e.offset
                        }
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }

    fn get_or_create_parent_dir(&self, name: &str) -> TocEntry {
        match self.m.get(&name.to_string()) {
            Some(e) => e.to_owned(),
            None => TocEntry {
                name: name.to_string(),
                entry_type: String::from("dir"),
                size: 0,
                mode: 0755,
                mod_time_3339: None,
                mod_time: None,
                link_name: "".to_string(),
                uid: 0,
                gid: 0,
                uname: "".to_string(),
                gname: "".to_string(),
                offset: 0,
                next_offset: 0,
                dev_major: 0,
                dev_minor: 0,
                num_link: 2,
                xattrs: HashMap::new(),
                digest: "".to_string(),
                chunk_offset: 0,
                chunk_size: 0,
                children: HashMap::new(),
            },
        }
    }

    pub fn lookup(&self, path: &str) -> Result<&TocEntry> {
        let mut ent = self.m.get(path).unwrap();
        if ent.entry_type == "hardlink" {
            let link_name = &ent.link_name;
            ent = self.m.get(link_name).unwrap()
        }
        return Ok(ent);
    }

    fn get_chunks(&self, entry: &TocEntry) -> Vec<TocEntry> {
        match self.chunks.get(&entry.name) {
            Some(entries) => entries.clone(),
            None => vec![entry.clone()],
        }
    }

    pub fn open_file(&self, name: &str) -> Result<SectionReader<File>> {
        let ent = self.lookup(name)?;
        if ent.entry_type != "reg" {
            return Err(anyhow!("Not a regular file"));
        }
        let file_reader = &FileReader {
            r: self,
            size: ent.size,
            ents: self.get_chunks(ent),
        };

        return Ok(SectionReader::new(
            &file_reader.r.sr,
            0,
            file_reader.size as u32,
        ));
    }

    pub fn chunk_entry_for_offset(&self, name: &str, offset: u64) -> Option<&TocEntry> {
        let ent = self.lookup(name);
        if ent.is_err() {
            return None;
        }
        let ent = ent.unwrap();
        if !ent.is_data_type() {
            return None;
        }
        let ents = self.chunks.get(&ent.name).unwrap();
        if ents.len() < 2 {
            if offset >= ent.chunk_size {
                return None;
            }
            return Some(ent);
        }
        let i = ents
            .iter()
            .position(|e| {
                e.offset >= offset
                    || (offset > e.chunk_offset && offset < e.chunk_offset + e.chunk_size)
            })
            .unwrap_or(ents.len() - 1);
        if i == ents.len() - 1 {
            return None;
        }
        return Some(&ents[i]);
    }
}

struct FileReader<'a> {
    r: &'a GzReader,
    size: u64,
    ents: Vec<TocEntry>,
}

impl<'a> FileReader<'a> {
    fn read_at(&self, buf: &mut [u8], mut offset: u64) -> Result<usize> {
        if offset > self.size {
            return Err(anyhow!("offset is greater than file size"));
        }
        let mut i: usize = 0;
        if self.ents.len() > 1 {
            // Is sorting useful here?
            let mut sorted = self.ents.clone();
            sorted.sort_unstable_by_key(|e| e.offset);

            // Find the first entity with an offset equal or great to offset
            i = sorted
                .iter()
                .position(|e| e.offset >= offset)
                .unwrap_or(self.ents.len() - 1);
        }

        let mut entry = self.ents.get(i).unwrap();
        if entry.chunk_offset > offset {
            if i == 0 {
                return Err(anyhow!("internal error; first chunk offset is non-zero"));
            }
            entry = self.ents.get(i - 1).unwrap();
        }

        offset -= entry.chunk_offset;
        let final_entry = &self.ents[self.ents.len() - 1];
        let gz_offset = entry.offset;
        let gz_bytes_remain = final_entry.next_offset() - gz_offset;
        let sr = SectionReader::new(&self.r.sr, gz_offset as u32, gz_bytes_remain as u32);

        const MAX_GZ_READ: i32 = 2 << 20;

        let mut buf_size = MAX_GZ_READ;
        if gz_bytes_remain > buf_size as u64 {
            buf_size = gz_bytes_remain as i32;
        }

        // Create a buffered reader with buf_size wrapper for sr
        let br = BufReader::with_capacity(buf_size as usize, sr);
        let mut gz = flate2::bufread::GzDecoder::new(br);
        // Discard until offset
        io::copy(&mut gz.by_ref().take(offset), &mut io::sink())?;
        let mut gz = gz.take(self.size as u64 - offset);
        return Ok(gz.read(buf)?);
    }
}

pub fn open<'a, R: FileExt>(input: File) -> Result<GzReader> {
    let size = input.metadata().unwrap().size();
    println!("File size {size}");

    if size < FOOTER_SIZE.into() {
        return Err(anyhow::anyhow!("size too small"));
    }

    let mut footer = [0; FOOTER_SIZE as usize];
    input.read_at(&mut footer, size - FOOTER_SIZE as u64)?;
    let toc_offset = parse_footer(&footer)?;
    println!("TOC offset {toc_offset:?}");
    let toc_size = size as usize - toc_offset as usize - FOOTER_SIZE as usize;
    println!("TOC size {toc_size}");
    let mut toc_targz: Vec<u8> = vec![0; toc_size];

    // Read the TOC which is a tar.gz file
    input.read_at(toc_targz.as_mut_slice(), toc_offset as u64)?;

    // Decompress gz
    let tar = GzDecoder::new(&toc_targz[..]);

    // Read tar
    let mut archive = Archive::new(tar);
    let mut header = archive.entries().unwrap().next().unwrap()?;
    let header_name = String::from_utf8_lossy(&header.header().as_old().name);
    if header_name.trim_end_matches('\0') != TOCT_TAR_NAME {
        return Err(anyhow!(
            "header name {header_name}, doesn't match {TOCT_TAR_NAME}"
        ));
    }

    // Now build the actual TOC
    header.set_preserve_permissions(true);
    header.set_unpack_xattrs(true);
    header.unpack_in(".")?;

    // Fix permissions, for some reason the index doesn't have permissions
    let mut permissions = fs::metadata(TOCT_TAR_NAME)?.permissions();
    permissions.set_readonly(true);
    permissions.set_mode(0o644);
    fs::set_permissions(TOCT_TAR_NAME, permissions)?;

    let f = File::options().read(true).open(TOCT_TAR_NAME)?;
    let toc: JToc = serde_json::from_reader(f)?;

    let mut reader = GzReader {
        sr: input,
        toc,
        m: HashMap::new(),
        chunks: HashMap::new(),
    };

    reader.init_fields()?;

    Ok(reader)
}

fn parse_footer(content: &[u8]) -> Result<i64> {
    let gz = GzDecoder::new(content);
    if FOOTER_SIZE < content.len().try_into()? {
        return Err(anyhow::anyhow!("Footer less than footer size"));
    }

    let extra = gz.header().unwrap().extra().unwrap();
    if extra.len() != 16 + "STARGZ".len() {
        return Err(anyhow::anyhow!("FOOTER is not STARGZ+16"));
    }

    if std::str::from_utf8(&extra[16..])? != "STARGZ" {
        return Err(anyhow::anyhow!("FOOTER not ending in STARGZ"));
    }

    let toc_offset = i64::from_str_radix(std::str::from_utf8(&extra[..16])?, 16)?;

    Ok(toc_offset)
}

#[derive(Debug, Deserialize, Clone)]
pub struct JToc {
    version: u32,
    entries: Vec<TocEntry>,
}

impl JToc {
    pub fn new(version: u32) -> Self {
        Self {
            version,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct TocEntry {
    name: String,

    #[serde(rename(serialize = "type", deserialize = "type"))]
    entry_type: String,

    #[serde(default)]
    size: u64,

    mod_time_3339: Option<String>,
    mod_time: Option<chrono::DateTime<Utc>>,

    #[serde(default)]
    mode: u32,

    #[serde(default, rename = "linkName")]
    link_name: String,

    #[serde(default)]
    uid: u32,
    #[serde(default)]
    gid: u32,

    #[serde(default)]
    uname: String,
    #[serde(default)]
    gname: String,

    #[serde(default)]
    offset: u64,

    #[serde(default)]
    next_offset: u64,

    #[serde(default, rename = "devMajor")]
    dev_major: u64,

    #[serde(default, rename = "devMinor")]
    dev_minor: u64,

    #[serde(default, rename(serialize = "NumLink", deserialize = "NumLink"))]
    num_link: u32,

    #[serde(default)]
    xattrs: HashMap<String, Vec<u8>>,

    #[serde(default)]
    digest: String,

    #[serde(default, rename = "chunkOffset")]
    chunk_offset: u64,
    #[serde(default, rename = "chunkSize")]
    chunk_size: u64,

    #[serde(skip)]
    children: HashMap<String, TocEntry>,
}

impl TocEntry {
    pub fn mod_time(&self) -> Option<chrono::DateTime<Utc>> {
        self.mod_time
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn add_child(&mut self, child: TocEntry, base_name: &str) {
        if child.entry_type == "dir" {
            self.num_link += 1;
        }

        self.children.insert(base_name.to_owned(), child);
    }

    pub fn lookup_child(&self, base_name: &str) -> Option<&TocEntry> {
        self.children.get(base_name)
    }

    pub fn is_data_type(&self) -> bool {
        self.entry_type == "reg" || self.entry_type == "chunk"
    }
}

struct FileInfo<'a>(&'a TocEntry);

impl<'a> FileInfo<'a> {
    pub fn new(toc_entry: &'a TocEntry) -> Self {
        Self(toc_entry)
    }

    pub fn is_dir(&self) -> bool {
        self.0.entry_type == "dir"
    }

    pub fn mode(&self) -> u32 {
        match self.0.entry_type.as_str() {
            "dir" => 0o755,
            "file" => 0o644,
            "symlink" => 0o777,
            "char" => 0o666,
            "block" => 0o666,
            "fifo" => 0o666,
            _ => 0,
        }
    }
}

struct CountingWriterWrapper<W: Write>(Rc<RefCell<CountingWriter<W>>>);

impl<W: Write> Write for CountingWriterWrapper<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (*self.0).borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (*self.0).borrow_mut().flush()
    }
}

pub struct Writer<'a, W: Write> {
    cw: Rc<RefCell<CountingWriter<W>>>,
    gz: Option<GzEncoder<CountingWriterWrapper<W>>>,
    toc: JToc,
    diff_hash: sha2::Sha256,
    last_username: HashMap<i32, &'a str>,
    last_groupname: HashMap<i32, &'a str>,
    chunk_size: usize,
    closed: bool,
}

impl<'a, W: Write> Writer<'a, W> {
    // Accept a writer and build Writer from it
    pub fn new(writer: W) -> Self {
        let jtoc = JToc::new(1);
        let bw = BufWriter::new(writer);
        let cw = Rc::new(RefCell::new(CountingWriter::new(bw)));
        Self {
            cw,
            gz: None,
            toc: jtoc,
            diff_hash: sha2::Digest::new(),
            last_username: HashMap::new(),
            last_groupname: HashMap::new(),
            chunk_size: 0,
            closed: false,
        }
    }

    pub fn chunk_size(&self) -> usize {
        if self.chunk_size <= 0 {
            return 4 << 20;
        }

        self.chunk_size
    }

    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.close_gz()?;

        //let toc_offset = self.

        self.closed = true;

        Ok(())
    }

    fn cond_open_gz(&mut self) -> Result<()> {
        if self.gz.is_none() {
            let gz = GzEncoder::new(CountingWriterWrapper(self.cw.clone()), Compression::best());
            self.gz = Some(gz);
        }

        Ok(())
    }

    fn close_gz(&mut self) -> Result<()> {
        if self.closed {
            return Err(anyhow!("Writer is closed"));
        }
        if let Some(gz) = self.gz.take() {
            let mut gz = gz.finish()?;
            gz.flush()?;
        }

        Ok(())
    }

    pub fn append_tar(&mut self, r: &mut dyn Read) -> Result<()> {
        let mut br = BufReader::new(r);
        let mut is_gzipped = [0; 3];
        br.read_exact(&mut is_gzipped)?;
        let is_gzipped = is_gzipped == [0x1f, 0x8b, 0x08];
        let mut tar: Archive<Box<dyn Read>>;
        if is_gzipped {
            let gz = GzDecoder::new(br);
            tar = tar::Archive::new(Box::new(gz));
        } else {
            tar = tar::Archive::new(Box::new(br));
        }
        for entry in tar.entries()? {
            let mut f = entry?;
            // check if name is TOCT_TAR_NAME
            if f.path()?.to_str().unwrap().contains(TOCT_TAR_NAME) {
                continue;
            }
            let mut xattrs: HashMap<String, Vec<u8>> = HashMap::new();
            if let Some(exts) = f.pax_extensions()? {
                for ext in exts {
                    let ext = ext?;
                    let key = ext.key().unwrap_or("");
                    if key.starts_with("SCHILY.xattr.") {
                        xattrs.insert(
                            key["SCHILY.xattr.".len()..].to_string(),
                            ext.value_bytes().to_vec(),
                        );
                    }
                }
            }

            // TODO: Might want to check the variant of LocalResult
            let datetime = Utc.timestamp_opt(f.header().mtime()? as i64, 0).unwrap();
            let mut ent = TocEntry {
                entry_type: "file".to_string(),
                name: f.path()?.to_str().unwrap().to_string(),
                size: f.size(),
                mod_time: Some(datetime),
                uid: f.header().uid()? as u32,
                gid: f.header().gid()? as u32,
                uname: f.header().username()?.unwrap_or("").to_string(),
                gname: f.header().groupname()?.unwrap_or("").to_string(),
                mode: f.header().mode()?,
                xattrs,
                ..Default::default()
            };
            self.cond_open_gz()?;
            let mut builder = tar::Builder::new(self.gz.as_mut().unwrap());
            // Create a new header and copy metadata from the entry's header
            let mut h = tar::Header::new_gnu();
            h.set_path(f.path()?)?;
            h.set_size(f.header().size()?);
            h.set_mode(f.header().mode()?);
            h.set_uid(f.header().uid()?);
            h.set_gid(f.header().gid()?);
            h.set_mtime(f.header().mtime()?);
            h.set_entry_type(f.header().entry_type());

            // Append the new header and the entry's content to the tar builder
            builder.append(&h, &mut f)?;

            match h.entry_type() {
                tar::EntryType::Link => {
                    ent.entry_type = "hardlink".to_string();
                    // TODO: do something more sensible here
                    ent.link_name = h.link_name()?.unwrap().to_str().unwrap().to_string();
                }
                tar::EntryType::Symlink => {
                    ent.entry_type = "symlink".to_string();

                    // TODO: do something more sensible here
                    ent.link_name = h.link_name()?.unwrap().to_str().unwrap().to_string();
                }
                tar::EntryType::Directory => {
                    ent.entry_type = "dir".to_string();
                }
                tar::EntryType::Regular => {
                    ent.entry_type = "reg".to_string();
                    ent.size = h.size()?;
                }
                tar::EntryType::Char => {
                    ent.entry_type = "char".to_string();
                    ent.dev_major = h.device_major()?.unwrap_or(0).try_into()?;
                    ent.dev_minor = h.device_minor()?.unwrap_or(0).try_into()?;
                }
                tar::EntryType::Block => {
                    ent.entry_type = "block".to_string();
                    ent.dev_major = h.device_major()?.unwrap_or(0).try_into()?;
                    ent.dev_minor = h.device_minor()?.unwrap_or(0).try_into()?;
                }
                tar::EntryType::Fifo => {
                    ent.entry_type = "fifo".to_string();
                }
                _ => {
                    return Err(anyhow!("unsupported input tar entry {:?}", h.entry_type()));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct CountingWriter<W: std::io::Write> {
    inner: BufWriter<W>,
    count: u64,
}

impl<W: std::io::Write> CountingWriter<W> {
    pub fn new(bw: BufWriter<W>) -> Self {
        Self {
            inner: bw,
            count: 0,
        }
    }
}
impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.inner.write(buf);
        if let Result::Ok(n) = result {
            self.count += n as u64;
        }

        result
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

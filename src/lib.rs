mod sectionreader;
use anyhow::{anyhow, Result};
use chrono::Utc;
use flate2::read::GzDecoder;
use sectionreader::SectionReader;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::{self, File},
    io,
    os::unix::prelude::{FileExt, MetadataExt, PermissionsExt},
    vec,
};
use tar::Archive;

static TOCT_TAR_NAME: &str = "stargz.index.json";
const FOOTER_SIZE: u32 = 47;

struct Reader {
    sr: File,
    toc: JToc,
    m: HashMap<String, TocEntry>,
    chunks: HashMap<String, Vec<TocEntry>>,
}

impl Reader {
    fn init_fields(mut self) -> Result<()> {
        self.m = HashMap::with_capacity(self.toc.entries.len());
        self.chunks = HashMap::new();
        let mut last_reg_entry: Option<TocEntry> = None;
        let mut last_path: &str;
        let mut uname = HashMap::<u32, String>::new();
        let mut gname = HashMap::<u32, String>::new();
        for mut entry in &mut self.toc.entries.clone() {
            entry.name = entry.name.trim_start_matches("./").to_owned();
            match entry.typ.as_str() {
                "reg" => {
                    last_reg_entry = Some(entry.clone());
                }
                "chunk" => {
                    last_path = &entry.name;
                    match self.chunks.get_mut(&entry.name) {
                        Some(v) => {
                            v.push(entry.clone());
                        }
                        None => {
                            self.chunks
                                .insert(entry.name.to_owned(), vec![entry.clone()]);
                        }
                    };
                    if &entry.chunk_size == &Some(0 as u64) && last_reg_entry.is_some() {
                        let offset = entry.offset;
                        let last_ent_size = last_reg_entry.as_ref().map(|e| e.size.unwrap());
                        entry.chunk_size = Some(last_ent_size.unwrap() - offset.unwrap());
                    }
                }
                _ => {
                    last_path = &entry.name;
                    match &entry.uname {
                        Some(euname) => {
                            uname.insert(entry.uid.unwrap(), euname.to_owned());
                        }
                        None => {
                            entry.uname = uname.get(&entry.uid.unwrap()).cloned();
                        }
                    }
                    match &entry.g_name {
                        Some(egname) => {
                            gname.insert(entry.gid.unwrap(), egname.to_owned());
                        }
                        None => {
                            entry.g_name = gname.get(&entry.gid.unwrap()).cloned();
                        }
                    }

                    entry.mod_time = Some(
                        chrono::DateTime::parse_from_rfc3339(
                            entry.mod_time_3339.as_ref().unwrap().as_str(),
                        )?
                        .into(),
                    );
                    if entry.typ == "dir" {
                        entry.num_link += 1;
                        self.m
                            .insert(entry.name.trim_end_matches("/").to_owned(), entry.clone());
                    } else {
                        self.m.insert(entry.name.to_owned(), entry.clone());
                    }
                }
            }

            if entry.typ == "reg"
                && entry.chunk_size.cmp(&Some(0)).is_gt()
                && entry.chunk_size < entry.size
            {
                let cap = (entry.size.unwrap() / entry.chunk_size.unwrap() + 1) as usize;
                let mut chunks: Vec<TocEntry> = Vec::with_capacity(cap);
                chunks.push(entry.clone());
                self.chunks.insert(entry.name.to_owned(), chunks);
            }
            if entry.chunk_size == Some(0) && entry.size != Some(0) {
                entry.chunk_size = entry.size;
            }

            for entry in &mut self.toc.entries.clone() {
                if entry.typ == "chunk" {
                    continue;
                }
                let mut name = entry.name.to_owned();
                if entry.typ == "dir" {
                    let bind = name.trim_end_matches("/").to_owned();
                    name = bind;
                }

                let mut parent_dir = self.get_or_create_parent_dir(&name);
                entry.num_link += 1;
                if entry.typ == "hardlink" {
                    let link_name = entry.clone().link_name.unwrap();
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
                            e.next_offset = Some(last_offset);
                        }
                        if e.offset != Some(0) || e.offset.is_none() {
                            last_offset = e.offset.unwrap()
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
                typ: String::from("dir"),
                size: None,
                mode: Some(0755),
                mod_time_3339: None,
                mod_time: None,
                link_name: None,
                uid: None,
                gid: None,
                uname: None,
                g_name: None,
                offset: None,
                next_offset: None,
                dev_major: None,
                dev_minor: None,
                num_link: 2,
                xattrs: None,
                digest: None,
                chunk_offset: None,
                chunk_size: None,
                children: None,
            },
        }
    }

    pub fn lookup(&self, path: &str) -> Result<&TocEntry> {
        let mut ent = self.m.get(path).unwrap();
        if ent.typ == "hardlink" {
            let link_name = ent.link_name.clone().unwrap();
            ent = self.m.get(&link_name).unwrap()
        }
        return Ok(ent);
    }

    fn get_chunks(&self, entry: &TocEntry) -> Vec<TocEntry> {
        match self.chunks.get(&entry.name) {
            Some(entries) => entries.clone(),
            None => vec![entry.clone()],
        }
    }

    pub fn open_file<'a>(&self, name: &str) -> Result<SectionReader<File>> {
        let ent = self.lookup(name)?;
        if ent.typ != "reg" {
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
            file_reader.size.unwrap() as u32,
        ));
    }
}

struct FileReader<'a> {
    r: &'a Reader,
    size: Option<u64>,
    ents: Vec<TocEntry>,
}

pub fn open<R: FileExt>(input: File) -> Result<()> {
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

    println!("TOC {toc:#?}");
    Ok(())
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
struct JToc {
    version: u32,
    entries: Vec<TocEntry>,
}

#[derive(Debug, Deserialize, Clone)]
struct TocEntry {
    name: String,

    #[serde(rename(serialize = "type", deserialize = "type"))]
    typ: String,
    size: Option<u64>,
    mod_time_3339: Option<String>,
    mod_time: Option<chrono::DateTime<Utc>>,
    mode: Option<i64>,
    link_name: Option<String>,
    uid: Option<u32>,
    gid: Option<u32>,
    uname: Option<String>,
    g_name: Option<String>,
    offset: Option<u64>,
    next_offset: Option<u64>,
    dev_major: Option<u32>,
    dev_minor: Option<u32>,

    #[serde(rename(serialize = "NumLink", deserialize = "NumLink"))]
    num_link: u32,
    xattrs: Option<HashMap<String, Vec<u8>>>,
    digest: Option<String>,
    chunk_offset: Option<u64>,
    chunk_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<HashMap<String, TocEntry>>,
}

impl TocEntry {
    pub fn mod_time(&self) -> Option<chrono::DateTime<Utc>> {
        self.mod_time
    }

    pub fn next_offset(&self) -> Option<u64> {
        self.next_offset
    }

    pub fn add_child(&mut self, child: TocEntry, base_name: &str) {
        if self.children.is_none() {
            self.children = Some(HashMap::new());
        }

        if child.typ == "dir" {
            self.num_link += 1;
        }

        self.children
            .as_mut()
            .unwrap()
            .insert(base_name.to_owned(), child);
    }

    pub fn lookup_child(self, base_name: &str) -> Option<TocEntry> {
        let children = self.children.unwrap();
        children.get(base_name).cloned()
    }

    pub fn is_data_type(&self) -> bool {
        self.typ == "reg" || self.typ == "chunk"
    }
}

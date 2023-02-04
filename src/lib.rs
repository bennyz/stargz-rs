use anyhow::Result;
use chrono::Utc;
use flate2::read::GzDecoder;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    os::unix::prelude::{FileExt, MetadataExt},
};

static TOCT_TAR_NAME: &str = "stargz.index.json";
const FOOTER_SIZE: u32 = 47;

struct Reader<'a, R: FileExt> {
    r: BufReader<R>,
    toc: &'a JToc<'a>,
    m: HashMap<&'a str, &'a TocEntry<'a>>,
    chunks: HashMap<&'a str, Vec<&'a TocEntry<'a>>>,
}

pub fn open<R: FileExt>(input: File) -> Result<()> {
    let size = input.metadata().unwrap().size();
    if size < FOOTER_SIZE.into() {
        return Err(anyhow::anyhow!("size too small"));
    }

    let mut footer = [0; FOOTER_SIZE as usize];
    input.read_at(&mut footer, size - FOOTER_SIZE as u64)?;
    let toc_offset = parse_footer(&footer)?;

    println!("TOC offset {toc_offset}");

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

struct JToc<'a> {
    version: u32,
    entries: [&'a TocEntry<'a>],
}

struct TocEntry<'a> {
    name: String,
    typ: String,
    size: u64,
    mod_time_3339: String,
    mod_time: chrono::DateTime<Utc>,
    link_name: String,
    uid: u32,
    gid: u32,
    uname: String,
    g_name: String,
    offset: u64,
    next_offset: u64,
    dev_major: u32,
    dev_minor: u32,
    num_link: u32,
    xattrs: HashMap<String, &'a [u8]>,
    digest: String,
    chunk_offset: u64,
    chunk_size: u64,
    children: Option<HashMap<&'a str, &'a TocEntry<'a>>>,
}

impl<'a> TocEntry<'a> {
    pub fn mod_time(&self) -> chrono::DateTime<Utc> {
        self.mod_time
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn add_child(&mut self, child: &'a TocEntry, base_name: &'a str) {
        if self.children.is_none() {
            self.children = Some(HashMap::new());
        }

        if child.typ == "dir" {
            self.num_link += 1;
        }

        self.children.as_mut().unwrap().insert(base_name, child);
    }

    pub fn lookup_child(self, base_name: &'a str) -> Option<&TocEntry> {
        self.children.unwrap().get(base_name).copied()
    }

    pub fn is_data_type(&self) -> bool {
        self.typ == "reg" || self.typ == "chunk"
    }
}

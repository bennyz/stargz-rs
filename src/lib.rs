use anyhow::{anyhow, Result};
use chrono::serde::ts_seconds::deserialize as from_ts;
use chrono::Utc;
use flate2::read::GzDecoder;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::BufReader,
    os::unix::prelude::{FileExt, MetadataExt, PermissionsExt},
    vec,
};
use tar::Archive;

static TOCT_TAR_NAME: &str = "stargz.index.json";
const FOOTER_SIZE: u32 = 47;

struct Reader<R: FileExt> {
    r: BufReader<R>,
    toc: JToc,
    m: HashMap<String, TocEntry>,
    chunks: HashMap<String, Vec<TocEntry>>,
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

#[derive(Deserialize)]
struct JToc {
    version: u32,
    entries: Vec<TocEntry>,
}

#[derive(Deserialize, Clone)]
struct TocEntry {
    name: String,

    #[serde(rename(serialize = "type", deserialize = "type"))]
    typ: String,
    size: Option<u64>,
    mod_time_3339: Option<String>,
    mod_time: Option<chrono::DateTime<Utc>>,
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

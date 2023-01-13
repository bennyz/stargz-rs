use std::collections::HashMap;

use chrono::Utc;

static TOCT_TAR_NAME: &str = "stargz.index.json";
const FOOTER_SIZE: u32 = 47;

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
    xattrs: HashMap<String, &'a[u8]>,
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
}
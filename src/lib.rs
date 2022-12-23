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
    children: HashMap<String, &'a TocEntry<'a>>,
}

impl<'a> TocEntry<'a> {

}
use std::fs::File;

use stargz_rs::open;

fn main() {
    let f = File::open("output.stargz").unwrap();
    open::<File>(f).unwrap();
}

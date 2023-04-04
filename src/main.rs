use std::fs::File;

use stargz_rs::open;

fn main() -> Result<(), Box<dyn std::error::Error>>{
    let f = File::open("output.stargz").unwrap();
    open::<File>(f)?;

    Ok(())
}

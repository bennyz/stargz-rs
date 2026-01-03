use std::fs::File;
use std::os::unix::fs::MetadataExt;

use stargz_rs::{open, Writer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: stargz-rs <command> [args...]");
        eprintln!("Commands:");
        eprintln!("  read <file.stargz>     - Read and list contents of a stargz file");
        eprintln!("  convert <input.tar> <output.stargz> - Convert tar to stargz");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "read" => {
            if args.len() < 3 {
                eprintln!("Usage: stargz-rs read <file.stargz>");
                std::process::exit(1);
            }
            read_stargz(&args[2])?;
        }
        "convert" => {
            if args.len() < 4 {
                eprintln!("Usage: stargz-rs convert <input.tar> <output.stargz>");
                std::process::exit(1);
            }
            convert_tar(&args[2], &args[3])?;
        }
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            std::process::exit(1);
        }
    }

    Ok(())
}

fn read_stargz(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let f = File::open(path)?;
    let size = f.metadata()?.size();

    println!("Opening stargz file: {} ({} bytes)", path, size);

    let reader = open(f, size)?;
    let toc = reader.toc();

    println!("TOC version: {}", toc.version);
    println!("Entries: {}", toc.entries.len());
    println!();

    for entry in &toc.entries {
        let type_char = match entry.entry_type.as_str() {
            "dir" => 'd',
            "reg" => '-',
            "symlink" => 'l',
            "hardlink" => 'h',
            "char" => 'c',
            "block" => 'b',
            "fifo" => 'p',
            "chunk" => ' ',
            _ => '?',
        };

        if entry.entry_type == "chunk" {
            println!(
                "  chunk offset={} chunk_offset={} chunk_size={}",
                entry.offset, entry.chunk_offset, entry.chunk_size
            );
        } else {
            println!(
                "{} {:>10} {:o} {}",
                type_char,
                entry.size,
                entry.mode,
                entry.name
            );
            if entry.entry_type == "symlink" || entry.entry_type == "hardlink" {
                println!("  -> {}", entry.link_name);
            }
        }
    }

    Ok(())
}

fn convert_tar(input_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let input = File::open(input_path)?;
    let output = File::create(output_path)?;

    println!("Converting {} to {}", input_path, output_path);

    let mut writer = Writer::new(output);
    writer.append_tar(input)?;
    writer.close()?;

    println!("Done. DiffID: {}", writer.diff_id());

    Ok(())
}

# stargz-rs

A Rust implementation of Google's CRFS stargz format. This library and CLI tool creates and reads stargz files - seekable tar.gz archives that enable lazy, on-demand loading of container image layers.

## What is stargz?

Stargz (seekable tar.gz) is a format designed by Google's CRFS project that allows random access to files within a compressed tar archive. Unlike regular tar.gz files which must be decompressed sequentially from the beginning, stargz files can seek directly to any file.

This is particularly useful for container images, where you might only need to access a few files from a large layer without downloading and decompressing the entire archive.

### Format Structure

A stargz file is a valid tar.gz with special structure:

1. **Per-file gzip streams** - Each file gets its own gzip member, enabling random access
2. **Chunking** - Large files are split into chunks (default 4MB) with separate gzip streams
3. **TOC (Table of Contents)** - JSON index at the end listing all files with their compressed offsets
4. **Footer** - 47-byte gzip stream with extra header containing offset to TOC

## Installation

```bash
cargo build --release
```

## CLI Usage

### Convert a tar archive to stargz

```bash
cargo run -- convert input.tar output.stargz
```

### Read and list stargz contents

```bash
cargo run -- read file.stargz
```

## Library Usage

### Writing stargz files

```rust
use stargz_rs::Writer;
use std::fs::File;

let input = File::open("input.tar")?;
let output = File::create("output.stargz")?;

let mut writer = Writer::new(output);
writer.append_tar(input)?;
writer.close()?;

println!("DiffID: {}", writer.diff_id());
```

### Reading stargz files

```rust
use stargz_rs::open;
use std::fs::File;
use std::os::unix::fs::MetadataExt;

let file = File::open("archive.stargz")?;
let size = file.metadata()?.size();

let reader = open(file, size)?;

// Look up a file
if let Some(entry) = reader.lookup("path/to/file.txt") {
    println!("Found: {} ({} bytes)", entry.name, entry.size);
}

// Read file contents
let file_reader = reader.open_file("path/to/file.txt")?;
let mut buf = vec![0u8; 1024];
let n = file_reader.read_at(&mut buf, 0)?;
```

## API Overview

### Core Types

- `Reader<R: FileExt>` - Opens and reads stargz files with random access
- `Writer<W: Write>` - Creates stargz files from tar archives
- `FileReader` - Reads file contents from within a stargz archive
- `JToc` / `TOCEntry` - JSON-serializable table of contents structures
- `MemReader` - In-memory `FileExt` implementation for testing

### Reader Methods

- `open(reader, size)` - Open a stargz file for reading
- `lookup(path)` - Look up a file or directory by path
- `open_file(path)` - Get a FileReader for reading file contents
- `toc()` - Access the table of contents

### Writer Methods

- `new(writer)` - Create a new stargz writer
- `set_chunk_size(size)` - Set chunk size for large files (default 4MB)
- `append_tar(reader)` - Add entries from a tar archive
- `close()` - Finalize the stargz file (writes TOC and footer)
- `diff_id()` - Get the sha256 digest of the content

## Running Tests

```bash
cargo test               # Run all tests
cargo test test_name     # Run a specific test
cargo test -- --nocapture # Run tests with output
```

## References

- [Google CRFS (Container Registry Filesystem)](https://github.com/google/crfs) - Original Go implementation
- [Stargz Snapshotter](https://github.com/containerd/stargz-snapshotter) - containerd remote snapshotter for stargz

## License

See LICENSE file for details.

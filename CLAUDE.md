# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

stargz-rs is a Rust implementation of Google's CRFS stargz format (https://github.com/google/crfs). It provides a library and CLI for creating and reading stargz files - seekable tar.gz archives that enable lazy, on-demand loading of container image layers.

## Build Commands

```bash
cargo build              # Build the project
cargo test               # Run all tests
cargo test test_name     # Run a specific test
cargo test -- --nocapture # Run tests with output
cargo run -- <args>      # Run the CLI
```

## CLI Usage

```bash
# Convert tar to stargz
cargo run -- convert input.tar output.stargz

# Read and list stargz contents
cargo run -- read file.stargz
```

## Architecture

### Stargz Format
The stargz format is a valid tar.gz with special structure:
1. **Per-file gzip streams**: Each file gets its own gzip member, enabling random access
2. **Chunking**: Large files are split into chunks (default 4MB) with separate gzip streams
3. **TOC (Table of Contents)**: JSON index at the end listing all files with their compressed offsets
4. **Footer**: 47-byte gzip stream with extra header containing offset to TOC

### Core Components

**`lib.rs`** - Main library with:
- `Reader<R: FileExt>`: Opens and reads stargz files. Uses `FileExt::read_at` for random access.
- `Writer<W: Write>`: Creates stargz files from tar archives
- `JToc` / `TOCEntry`: JSON-serializable table of contents structures
- `FileReader`: Reads file contents from within a stargz archive
- `MemReader`: In-memory `FileExt` implementation for testing

**`sectionreader.rs`** - `SectionReader<R: FileExt>`: Reads a bounded section of a file, similar to Go's `io.SectionReader`

### Key Data Flow

**Writing:**
1. `Writer::append_tar()` reads tar entries
2. Each entry is wrapped in its own gzip stream
3. Large files are chunked with separate streams per chunk
4. `Writer::close()` writes TOC (gzipped tar containing JSON) + footer

**Reading:**
1. `Reader::open()` reads footer to find TOC offset
2. Decompresses TOC, parses JSON into `JToc`
3. `init_fields()` builds lookup maps and computes `next_offset` for each entry
4. `open_file()` returns `FileReader` for reading file contents
5. `FileReader::read_at()` locates correct chunk, decompresses gzip stream, skips tar header, reads data

### Important Implementation Details

- `TOCEntry.offset`: Byte offset in stargz file to the gzip stream
- `TOCEntry.next_offset`: Computed field - offset of next entry's gzip stream (used to know how many bytes to read)
- `TOCEntry.chunk_offset`: For chunked files, offset within the logical file
- Chunks are rebuilt after computing `next_offset` to ensure they have correct values
- xattrs are base64 encoded in JSON

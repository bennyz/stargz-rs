use std::{
    io::Read,
    os::unix::prelude::FileExt,
};

/// A reader that reads a section of an underlying reader.
/// Similar to Go's io.SectionReader.
pub struct SectionReader<'a, R: FileExt> {
    reader: &'a R,
    #[allow(dead_code)]
    base: u64,
    offset: u64,
    limit: u64,
}

impl<'a, R: FileExt> SectionReader<'a, R> {
    pub fn new(reader: &'a R, offset: u64, n: u64) -> Self {
        let limit = offset.saturating_add(n);
        SectionReader {
            reader,
            base: offset,
            offset,
            limit,
        }
    }

    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.limit - self.base
    }

    #[allow(dead_code)]
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        if offset >= self.limit - self.base {
            return Ok(0); // EOF
        }

        let abs_offset = self.base + offset;
        let max = (self.limit - abs_offset) as usize;

        if buf.len() > max {
            self.reader.read_at(&mut buf[0..max], abs_offset)
        } else {
            self.reader.read_at(buf, abs_offset)
        }
    }
}

impl<'a, R: FileExt> Read for SectionReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.offset >= self.limit {
            return Ok(0); // EOF, not an error
        }
        let max = (self.limit - self.offset) as usize;
        let n = if buf.len() > max {
            self.reader.read_at(&mut buf[0..max], self.offset)?
        } else {
            self.reader.read_at(buf, self.offset)?
        };

        self.offset += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_section_reader_basic() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"Hello, World!").unwrap();
        file.flush().unwrap();

        let f = file.reopen().unwrap();
        let sr = SectionReader::new(&f, 7, 5); // "World"

        let mut buf = [0u8; 5];
        let n = sr.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"World");
    }

    #[test]
    fn test_section_reader_partial() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"Hello, World!").unwrap();
        file.flush().unwrap();

        let f = file.reopen().unwrap();
        let sr = SectionReader::new(&f, 7, 5);

        let mut buf = [0u8; 10]; // larger than section
        let n = sr.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..5], b"World");
    }

    #[test]
    fn test_section_reader_offset() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"Hello, World!").unwrap();
        file.flush().unwrap();

        let f = file.reopen().unwrap();
        let sr = SectionReader::new(&f, 7, 5);

        let mut buf = [0u8; 3];
        let n = sr.read_at(&mut buf, 2).unwrap(); // "rld"
        assert_eq!(n, 3);
        assert_eq!(&buf, b"rld");
    }

    #[test]
    fn test_section_reader_sequential() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"Hello, World!").unwrap();
        file.flush().unwrap();

        let f = file.reopen().unwrap();
        let mut sr = SectionReader::new(&f, 0, 5); // "Hello"

        let mut buf = [0u8; 3];
        let n = sr.read(&mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf, b"Hel");

        let n = sr.read(&mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"lo");

        let n = sr.read(&mut buf).unwrap();
        assert_eq!(n, 0); // EOF
    }
}

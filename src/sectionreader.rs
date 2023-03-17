use std::{
    io::{Error, ErrorKind, Read},
    os::unix::prelude::FileExt,
};

pub struct SectionReader<'a, R: FileExt> {
    reader: &'a R,
    base: u32,
    offset: u32,
    limit: u32,
}

impl<'a, R: FileExt> SectionReader<'a, R> {
    pub fn new(reader: &'a R, offset: u32, n: u32) -> Self {
        let remaining: u32;
        if offset <= u32::MAX - n {
            remaining = n + offset;
        } else {
            remaining = u32::MAX;
        }
        SectionReader {
            reader,
            base: offset,
            offset,
            limit: remaining,
        }
    }

    pub fn read_at(&mut self, buf: &mut [u8], mut offset: u32) -> std::io::Result<usize> {
        if offset >= self.limit - self.base {
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid offset"));
        }

        offset += self.base;
        let max = (self.limit - self.offset) as usize;
        let mut n: usize = 0;

        if buf.len() > max {
            n = self.reader.read_at(&mut buf[0..max], offset.into())?;
        } else {
            n = self.reader.read_at(buf, offset.into())?;
        }

        Ok(n)
    }

    pub fn inner(&self) -> &R {
        return &self.reader;
    }
}

impl<'a, R: FileExt> Read for SectionReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.offset >= self.limit {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "offset larger than limit",
            ));
        }
        let max = (self.limit - self.offset) as usize;
        let mut n: usize = 0;
        if buf.len() > max {
            n = self.reader.read_at(&mut buf[0..max], self.offset.into())?;
        } else {
            n = self.reader.read_at(buf, self.offset.into())?;
        }

        self.offset += n as u32;

        Ok(n)
    }
}

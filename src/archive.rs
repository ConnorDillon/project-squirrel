use std::{
    io::{self, Read, Write},
    path::Path,
};

use tar::{Builder, Header};

pub trait ArchiveWrite {
    fn add_file<P: AsRef<Path>, R: Read>(&mut self, path: P, size: u64, data: R) -> io::Result<()>;

    fn finish(&mut self) -> io::Result<()>;
}

impl<W: Write> ArchiveWrite for TarGzWriter<W> {
    fn add_file<P: AsRef<Path>, R: Read>(&mut self, path: P, size: u64, data: R) -> io::Result<()> {
        let mut header = Header::new_gnu();
        header.set_size(size);
        header.set_cksum();
        self.inner.append_data(&mut header, path, data)
    }

    fn finish(&mut self) -> io::Result<()> {
        self.inner.finish()
    }
}

pub struct TarGzWriter<W: Write> {
    inner: Builder<W>,
}

impl<W: Write> TarGzWriter<W> {
    pub fn new(inner: W) -> TarGzWriter<W> {
        TarGzWriter {
            inner: Builder::new(inner),
        }
    }
}

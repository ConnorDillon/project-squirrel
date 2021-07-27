use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::{self, Cursor};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::rc::Rc;

pub struct Volume<T> {
    pub inner: BufReader<T>,
}

pub fn open_volume<P: AsRef<Path>>(path: P) -> io::Result<Volume<File>> {
    Ok(Volume {
        inner: BufReader::with_capacity(1024 * 1024, File::open(path)?),
    })
}

impl<T: Read> Read for Volume<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.fill_buf()?;
        self.inner.read(buf)
    }
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_exact(buf)
    }
}

impl<T: Read> BufRead for Volume<T> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt)
    }
}

impl<T: Seek + Read> Seek for Volume<T> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(x) => {
                let curr_pos = self.stream_position()?;
                let buf_len = self.fill_buf()?.len();
                if x > curr_pos && x - curr_pos <= u64::try_from(buf_len).unwrap() {
                    self.inner
                        .seek_relative(i64::try_from(x - curr_pos).unwrap())?;
                } else if x < curr_pos
                    && curr_pos - x <= u64::try_from(self.inner.capacity() - buf_len).unwrap()
                {
                    self.inner
                        .seek_relative(-i64::try_from(curr_pos - x).unwrap())?;
                } else {
                    self.inner.seek(SeekFrom::Start(x - (x % 512)))?;
                    self.fill_buf()?;
                    self.inner.seek_relative(i64::try_from(x % 512).unwrap())?;
                }
                self.stream_position()
            }
            SeekFrom::Current(x) => {
                let offset = i64::try_from(self.stream_position()?).unwrap() + x;
                self.seek(SeekFrom::Start(u64::try_from(offset).unwrap()))
            }
            SeekFrom::End(_) => panic!("Cannot seek from end of volume"),
        }
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        self.inner.stream_position()
    }
}

#[derive(Debug, PartialEq)]
pub struct DataRun {
    pub offset: u64,
    pub virt_offset: u64,
    pub len: u64,
}

#[derive(Debug)]
pub enum Content {
    Resident {
        data: Rc<[u8]>,
    },
    NonResident {
        run_start_vcn: u64,
        run_end_vcn: u64,
        alloc_size: u64,
        size: u64,
        runs: Rc<[DataRun]>,
    },
}

impl Content {
    pub fn reader<T: Read + Seek>(&self, volume: T) -> ContentReader<T> {
        match self {
            Content::Resident { data } => ContentReader::Resident {
                inner: Cursor::new(data.clone()),
            },
            Content::NonResident { runs, .. } => ContentReader::NonResident {
                inner: RunReader::new(volume, runs.clone()),
            },
        }
    }
}

#[derive(Debug)]
pub enum ContentReader<T> {
    Resident { inner: Cursor<Rc<[u8]>> },
    NonResident { inner: RunReader<T> },
}

impl<T> ContentReader<T> {
    pub fn size(&self) -> u64 {
        match self {
            ContentReader::Resident { inner } => inner.get_ref().len().try_into().unwrap(),
            ContentReader::NonResident { inner } => inner.size,
        }
    }
}

impl<T: Read + Seek> Read for ContentReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            ContentReader::Resident { inner } => inner.read(buf),
            ContentReader::NonResident { inner } => inner.read(buf),
        }
    }
}

impl<T: Seek + Read> Seek for ContentReader<T> {
    fn seek(&mut self, seek_pos: SeekFrom) -> io::Result<u64> {
        match self {
            ContentReader::Resident { inner } => inner.seek(seek_pos),
            ContentReader::NonResident { inner } => inner.seek(seek_pos),
        }
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        match self {
            ContentReader::Resident { inner } => inner.stream_position(),
            ContentReader::NonResident { inner } => inner.stream_position(),
        }
    }
}

pub fn load_runs<T: IntoIterator<Item = (u64, u64)>>(iter: T, size: u64) -> Vec<DataRun> {
    let mut runs = Vec::new();
    let mut virt_offset = 0;
    for (offset, len) in iter {
        runs.push(DataRun {
            offset,
            virt_offset,
            len,
        });
        virt_offset = virt_offset + len;
    }
    let slack = virt_offset - size;
    let last_run = runs.len() - 1;
    let fixed_len = runs[last_run].len - slack;
    runs[last_run].len = fixed_len;
    runs
}

#[derive(Debug)]
pub struct RunReader<T> {
    volume: T,
    state: State,
    pub runs: Rc<[DataRun]>,
    pub size: u64,
}

impl<T> RunReader<T> {
    pub fn new(volume: T, runs: Rc<[DataRun]>) -> RunReader<T> {
        RunReader {
            size: runs.iter().map(|x| x.len).sum(),
            state: State { run: 0, pos: 0 },
            volume,
            runs,
        }
    }
    fn state_for(&self, pos: u64) -> State {
        let idx = self
            .runs
            .iter()
            .enumerate()
            .filter(|(_, x)| x.virt_offset > pos)
            .map(|(x, _)| x)
            .next()
            .unwrap_or(self.runs.len());
        State {
            run: idx - 1,
            pos: pos - &self.runs[idx - 1].virt_offset,
        }
    }
    fn seek_offset(&self, pos: SeekFrom) -> u64 {
        match pos {
            SeekFrom::Start(x) => x,
            SeekFrom::Current(x) => {
                u64::try_from(i64::try_from(self.virt_position()).unwrap() + x).unwrap()
            }
            SeekFrom::End(x) => u64::try_from(i64::try_from(self.size).unwrap() + x).unwrap(),
        }
    }
    fn run_remaining(&self) -> u64 {
        self.runs[self.state.run].len - self.state.pos
    }
    fn position(&self) -> u64 {
        self.runs[self.state.run].offset + self.state.pos
    }
    fn virt_position(&self) -> u64 {
        self.runs[self.state.run].virt_offset + self.state.pos
    }
}

impl<T: Seek> Seek for RunReader<T> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let offset = self.seek_offset(pos);
        self.state = self.state_for(offset);
        self.volume.seek(SeekFrom::Start(self.position()))?;
        Ok(self.virt_position())
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.virt_position())
    }
}

impl<T: Read + Seek> Read for RunReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let vpos = self.virt_position();
        if vpos == 0 || (self.run_remaining() == 0 && vpos < self.size) {
            self.seek(SeekFrom::Start(vpos))?;
        }
        let remaining = self.run_remaining();
        let nread = {
            let mut rdr = (&mut self.volume).take(remaining);
            rdr.read(buf)?
        };
        self.state.pos = self.state.pos + u64::try_from(nread).unwrap();
        Ok(nread)
    }
}

#[derive(Debug)]
struct State {
    run: usize,
    pos: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_reader() {
        let mut vol = open_volume("\\\\.\\C:").unwrap();
        let mut buf = [0u8; 9000];
        vol.read(&mut buf).unwrap();
        vol.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(vol.stream_position().unwrap(), 0);
        vol.read_exact(&mut buf).unwrap();
        assert_eq!(vol.stream_position().unwrap(), 9000);
        vol.seek(SeekFrom::Current(-50)).unwrap();
        assert_eq!(vol.stream_position().unwrap(), 8950);
        vol.seek(SeekFrom::Current(10000)).unwrap();
        assert_eq!(vol.stream_position().unwrap(), 18950);
        vol.read_exact(&mut buf).unwrap();
        assert_eq!(vol.stream_position().unwrap(), 27950);
        vol.seek(SeekFrom::Start(530)).unwrap();
        assert_eq!(vol.stream_position().unwrap(), 530);
        vol.seek(SeekFrom::Current(9000)).unwrap();
        assert_eq!(vol.stream_position().unwrap(), 9530);
    }

    #[test]
    fn test_run_reader_seek() {
        let mut rdr = RunReader::new(
            Cursor::new(vec![0u8; 10000]),
            load_runs(vec![(1000, 1000), (3000, 2000), (0, 1000)], 4000).into(),
        );
        let pos = rdr.seek(SeekFrom::Start(500)).unwrap();
        assert_eq!(pos, 500);
        assert_eq!(rdr.position(), 1500);
        let pos = rdr.seek(SeekFrom::Current(500)).unwrap();
        assert_eq!(pos, 1000);
        assert_eq!(rdr.position(), 3000);
        let pos = rdr.seek(SeekFrom::Current(-1)).unwrap();
        assert_eq!(pos, 999);
        assert_eq!(rdr.position(), 1999);
        let pos = rdr.seek(SeekFrom::End(-100)).unwrap();
        assert_eq!(pos, 3900);
        assert_eq!(rdr.position(), 900);
    }

    #[test]
    fn test_run_reader_read() {
        let mut rdr = RunReader::new(
            Cursor::new(b"4560123XXX789"),
            load_runs(vec![(3, 4), (0, 3), (10, 3)], 10).into(),
        );
        let mut buf = [0u8; 10];
        rdr.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"0123456789");
    }
}

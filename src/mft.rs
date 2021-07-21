use byteorder::{ReadBytesExt, LE};
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;

pub struct MFT {
    volume: String,
    data: ContentReader<Volume<File>>,
    boot: Boot,
}

impl MFT {
    pub fn open<T: Into<String>>(volume: T) -> io::Result<MFT> {
        let vol_path = volume.into();
        let mut vol = open_volume(&vol_path)?;
        let boot = parse_boot(&mut vol)?;
        go_to_mft(&boot, &mut vol)?;
        let entry = parse_mft_entry(&boot, vol_path.clone(), &mut vol)?;
        let data = entry.data()?.unwrap();
        go_to_mft(&boot, &mut vol)?;
        Ok(MFT {
            volume: vol_path,
            data,
            boot,
        })
    }
    pub fn open_entry(&mut self, idx: i64) -> io::Result<MFTEntry> {
        self.data
            .seek(SeekFrom::Start(u64::try_from(idx).unwrap() * 1024))?;
        parse_mft_entry(&self.boot, &self.volume, &mut self.data)
    }
}

#[derive(Debug)]
pub struct Boot {
    sector_size: u16,
    cluster_size: u16,
    mft_start: u64,
}

#[derive(Debug)]
pub struct MFTEntry {
    volume: String,
    header: MFTHeader,
    attrs: Vec<MFTAttr>,
}

impl MFTEntry {
    fn content(&self) -> Option<&Content> {
        for attr in &self.attrs {
            if attr.attr_type == 128 {
                return Some(&attr.content);
            }
        }
        None
    }
    pub fn data(&self) -> io::Result<Option<ContentReader<Volume<File>>>> {
        let vol = open_volume(&self.volume)?;
        Ok(self.content().map(|x| content_reader(vol, x)))
    }
}

#[derive(Debug)]
pub struct MFTHeader {
    fixup_offset: u16,
    fixup_entries: u16,
    attr_offset: u16,
    flags: u16,
    used_size: u32,
    alloc_size: u32,
}

#[derive(Debug)]
pub struct MFTAttr {
    attr_type: u32,
    length: u32,
    name_length: u8,
    name_offset: u16,
    flags: u16,
    attr_id: u16,
    content: Content,
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

fn parse_boot<T: Seek + Read>(vol: &mut T) -> io::Result<Boot> {
    vol.seek(SeekFrom::Current(11))?;
    let sector_size = vol.read_u16::<LE>()?;
    let sectors_per_cluster = vol.read_u16::<LE>()?;
    let cluster_size: u16 = (sector_size * sectors_per_cluster).into();
    vol.seek(SeekFrom::Current(33))?;
    let mft_start_cluster = vol.read_u64::<LE>()?;
    let mft_start = mft_start_cluster * u64::from(cluster_size);
    Ok(Boot {
        sector_size,
        cluster_size,
        mft_start,
    })
}

fn parse_mft_header<T: Read + Seek>(vol: &mut T) -> io::Result<MFTHeader> {
    let mut sig_buf = [0u8; 4];
    vol.read_exact(&mut sig_buf)?;
    assert_eq!(&sig_buf, b"FILE");
    let fixup_offset = vol.read_u16::<LE>()?;
    let fixup_entries = vol.read_u16::<LE>()?;
    vol.seek(SeekFrom::Current(12))?;
    let attr_offset = vol.read_u16::<LE>()?;
    let flags = vol.read_u16::<LE>()?;
    let used_size = vol.read_u32::<LE>()?;
    let alloc_size = vol.read_u32::<LE>()?;
    Ok(MFTHeader {
        fixup_offset,
        fixup_entries,
        attr_offset,
        flags,
        used_size,
        alloc_size,
    })
}

fn fixup_buf(sector_size: u16, mft_header: &MFTHeader, buf: &mut [u8]) {
    let offset: usize = mft_header.fixup_offset.into();
    let sig: [u8; 2] = buf[offset..offset + 2].try_into().unwrap();
    for entry in 1..usize::from(mft_header.fixup_entries) {
        let orig_offset = offset + (entry - 1) * 2;
        let orig: [u8; 2] = buf[orig_offset..orig_offset + 2].try_into().unwrap();
        let sector_end = entry * usize::from(sector_size);
        let check: &mut [u8] = &mut buf[sector_end - 2..sector_end];
        assert_eq!(&sig, check);
        check.copy_from_slice(&orig);
    }
}

fn parse_mft_attr<T: Read + Seek>(cur: &mut T, cluster_size: u16) -> io::Result<MFTAttr> {
    let start_pos = cur.stream_position()?;
    let attr_type = cur.read_u32::<LE>()?;
    let length = cur.read_u32::<LE>()?;
    let non_resident = cur.read_u8()?;
    let name_length = cur.read_u8()?;
    let name_offset = cur.read_u16::<LE>()?;
    let flags = cur.read_u16::<LE>()?;
    let attr_id = cur.read_u16::<LE>()?;
    let content = if non_resident == 0 {
        let size = cur.read_u32::<LE>()?;
        let offset = cur.read_u16::<LE>()?;
        cur.seek(SeekFrom::Start(start_pos + u64::from(offset)))?;
        let mut data = Vec::new();
        data.resize(size.try_into().unwrap(), 0);
        cur.read_exact(data.as_mut_slice())?;
        Content::Resident { data: data.into() }
    } else {
        let run_start_vcn = cur.read_u64::<LE>()?;
        let run_end_vcn = cur.read_u64::<LE>()?;
        let run_offset = cur.read_u16::<LE>()?;
        cur.seek(SeekFrom::Current(6))?;
        let alloc_size = cur.read_u64::<LE>()?;
        let size = cur.read_u64::<LE>()?;
        cur.seek(SeekFrom::Start(start_pos + u64::from(run_offset)))?;
        let runs = parse_run_list(cur, cluster_size, size)?.into();
        Content::NonResident {
            run_start_vcn,
            run_end_vcn,
            alloc_size,
            size,
            runs,
        }
    };
    Ok(MFTAttr {
        attr_type,
        length,
        name_length,
        name_offset,
        flags,
        attr_id,
        content,
    })
}

fn parse_mft_attrs<T: Read + Seek>(cur: &mut T, cluster_size: u16) -> io::Result<Vec<MFTAttr>> {
    let mut attrs: Vec<MFTAttr> = Vec::new();
    while cur.read_u16::<LE>()? != u16::MAX {
        let pos = cur.seek(SeekFrom::Current(-2))?;
        let attr = parse_mft_attr(cur, cluster_size)?;
        cur.seek(SeekFrom::Start(pos + u64::from(attr.length)))?;
        attrs.push(attr);
    }
    Ok(attrs)
}

fn read_int_bytes<T: Read>(num_bytes: u8, cur: &mut T, signed: bool) -> io::Result<[u8; 8]> {
    assert!(num_bytes <= 8);
    let mut bytes = Vec::with_capacity(8);
    for _ in 0..num_bytes {
        bytes.push(cur.read_u8()?);
    }
    let fill = if signed {
        if let Some(b) = bytes.iter().last() {
            if *b & 128 == 128 {
                255
            } else {
                0
            }
        } else {
            0
        }
    } else {
        0
    };
    bytes.resize(8, fill);
    Ok(bytes.try_into().unwrap())
}

fn parse_run_list<T: Read + Seek>(
    cur: &mut T,
    cluster_size: u16,
    size: u64,
) -> io::Result<Vec<DataRun>> {
    let mut runs = Vec::new();
    let mut offset = 0;
    let mut first_byte = cur.read_u8()?;
    while first_byte > 0 {
        let length_length = u8::MAX.wrapping_shr(4) & first_byte;
        let offset_length = first_byte.wrapping_shr(4);
        let length = u64::from_le_bytes(read_int_bytes(length_length, cur, false)?);
        let rel_offset = i64::from_le_bytes(read_int_bytes(offset_length, cur, true)?);
        offset = offset + rel_offset;
        runs.push((
            u64::try_from(offset).unwrap() * u64::from(cluster_size),
            length * u64::from(cluster_size),
        ));
        first_byte = cur.read_u8()?;
    }
    Ok(load_runs(runs, size))
}

fn load_runs<T: IntoIterator<Item = (u64, u64)>>(iter: T, size: u64) -> Vec<DataRun> {
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

fn parse_mft_entry<T: Read + Seek, U: Into<String>>(
    boot: &Boot,
    vol_name: U,
    vol: &mut T,
) -> io::Result<MFTEntry> {
    let mut buf = [0u8; 1024];
    vol.read_exact(&mut buf)?;
    let mut cur = Cursor::new(buf);
    let header = parse_mft_header(&mut cur)?;
    fixup_buf(boot.sector_size, &header, &mut buf);
    cur.seek(SeekFrom::Start(header.attr_offset.into()))?;
    let attrs = parse_mft_attrs(&mut cur, boot.cluster_size)?;
    Ok(MFTEntry {
        header,
        attrs,
        volume: vol_name.into(),
    })
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

pub fn content_reader<T: Read + Seek>(volume: T, content: &Content) -> ContentReader<T> {
    match content {
        Content::Resident { data } => ContentReader::Resident {
            inner: Cursor::new(data.clone()),
        },
        Content::NonResident { runs, .. } => ContentReader::NonResident {
            inner: RunReader::new(volume, runs.clone()),
        },
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

fn go_to_mft<T: Read + Seek>(boot: &Boot, vol: &mut T) -> io::Result<()> {
    vol.seek(SeekFrom::Start(boot.mft_start))?;
    Ok(())
}

fn extract_file<T: Into<String>, U: Write>(vol: T, dest: &mut U, entry: i64) -> io::Result<()> {
    let mut mft = MFT::open(vol)?;
    let entry = mft.open_entry(entry)?;
    io::copy(&mut entry.data()?.unwrap(), dest).unwrap();
    Ok(())
}

pub fn extract_mft<T: Into<String>, U: Write>(vol: T, dest: &mut U) -> io::Result<()> {
    println!("Copying $MFT");
    extract_file(vol, dest, 0)
}

pub fn extract_logfile<T: Into<String>, U: Write>(vol: T, dest: &mut U) -> io::Result<()> {
    println!("Copying $LogFile");
    extract_file(vol, dest, 2)
}

#[derive(Debug)]
pub struct RunReader<T> {
    volume: T,
    state: State,
    runs: Rc<[DataRun]>,
    size: u64,
}

impl<T> RunReader<T> {
    fn new(volume: T, runs: Rc<[DataRun]>) -> RunReader<T> {
        #[cfg(debug)]
        {
            let mut offset = 0;
            for run in &runs {
                assert_eq!(run.virt_offset, offset);
                offset = offset + run.len;
            }
        }

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

#[derive(Debug, PartialEq)]
pub struct DataRun {
    offset: u64,
    virt_offset: u64,
    len: u64,
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    fn hex_str<'a, T>(bs: T) -> String
    where
        T: Iterator<Item = &'a u8>,
    {
        bs.map(|x| {
            if *x < 16 {
                format!("0{:X} ", x)
            } else {
                format!("{:X} ", x)
            }
        })
        .collect::<String>()
    }

    #[test]
    fn test_parse_boot() {
        let mut buf: [u8; 1024] = [0; 1024];
        let mut vol = open_volume(r#"\\.\C:"#).unwrap();
        let boot = parse_boot(&mut vol).unwrap();
        go_to_mft(&boot, &mut vol).unwrap();
        vol.read_exact(&mut buf).unwrap();
        println!("{}", hex_str(buf.iter()));
        assert_eq!(&buf[0..4], "FILE".as_bytes())
    }

    #[test]
    fn test_fixup() {
        let mut buf: [u8; 1024] = [0; 1024];
        let mut vol = open_volume(r#"\\.\C:"#).unwrap();
        let boot = parse_boot(&mut vol).unwrap();
        go_to_mft(&boot, &mut vol).unwrap();
        vol.read_exact(&mut buf).unwrap();
        let header = parse_mft_header(&mut vol).unwrap();
        fixup_buf(boot.sector_size, &header, &mut buf);
        let fail = catch_unwind(move || fixup_buf(boot.sector_size, &header, &mut buf));
        assert!(fail.is_err())
    }

    #[test]
    fn test_parse_attrs() {
        let mut vol = open_volume(r#"\\.\C:"#).unwrap();
        let boot = parse_boot(&mut vol).unwrap();
        go_to_mft(&boot, &mut vol).unwrap();
        let entry = parse_mft_entry(&boot, r#"\\.\C:"#, &mut vol).unwrap();
        for attr in entry.attrs {
            println!("{:?}", &attr);
            match attr.content {
                Content::NonResident { runs, size, .. } => {
                    assert_eq!(size, runs.iter().map(|x| x.len).sum::<u64>())
                }
                Content::Resident { .. } => (),
            }
        }
    }

    #[test]
    fn test_parse_run_list() {
        let data: [u8; 8] = [0x21, 0x10, 0x00, 0x01, 0x11, 0x20, 0xE0, 0x00]; // 16/256 32/-32
        let mut cur = Cursor::new(data);
        let run_lists = parse_run_list(&mut cur, 1, 48).unwrap();
        let valid = vec![
            DataRun {
                len: 16,
                offset: 256,
                virt_offset: 0,
            },
            DataRun {
                len: 32,
                offset: 256 - 32,
                virt_offset: 16,
            },
        ];
        assert_eq!(run_lists, valid);
    }

    #[test]
    fn test_read_mft() {
        let mut mft = MFT::open(r#"\\.\C:"#).unwrap();
        let entry = mft.open_entry(0).unwrap();
        let mut data = entry.data().unwrap().unwrap();
        let mut buf = [0u8; 1024];
        for i in 0..data.size() / 1024 {
            data.read_exact(&mut buf).unwrap();
            assert!(
                buf[0..4] == [0u8; 4][..] || buf[0..4] == b"FILE"[..],
                "Failed at iteration {}",
                i
            );
        }
    }

    #[test]
    fn test_write_logfile() {
        let mut dest = File::create("LogFile").unwrap();
        let mut mft = MFT::open(r#"\\.\C:"#).unwrap();
        let entry = mft.open_entry(2).unwrap();
        let mut data = entry.data().unwrap().unwrap();
        io::copy(&mut data, &mut dest).unwrap();
        let file_size = dest.metadata().unwrap().len();
        std::fs::remove_file("LogFile").unwrap();
        assert_eq!(data.size(), file_size);
    }

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

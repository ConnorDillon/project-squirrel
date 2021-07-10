use byteorder::{ReadBytesExt, LE};
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
struct Boot {
    sector_size: u16,
    cluster_size: u16,
    mft_start: u64,
}

#[derive(Debug)]
struct MFTEntry {
    header: MFTHeader,
    attrs: Vec<MFTAttr>,
}

#[derive(Debug)]
struct MFTHeader {
    fixup_offset: u16,
    fixup_entries: u16,
    attr_offset: u16,
    flags: u16,
    used_size: u32,
    alloc_size: u32,
}

#[derive(Debug)]
struct MFTAttr {
    attr_type: u32,
    length: u32,
    name_length: u8,
    name_offset: u16,
    flags: u16,
    attr_id: u16,
    content: Content,
}

#[derive(Debug)]
enum Content {
    Resident {
        data: Vec<u8>,
    },
    NonResident {
        run_start_vcn: u64,
        run_end_vcn: u64,
        alloc_size: u64,
        size: u64,
        run_lists: Vec<RunList>,
    },
}

#[derive(Debug, PartialEq)]
struct RunList {
    length: u64,
    offset: i64,
}

pub struct Volume<T> {
    pub inner: BufReader<T>,
}

pub fn open_volume<P: AsRef<Path>>(path: P) -> io::Result<Volume<File>> {
    Ok(Volume {
        inner: BufReader::new(File::open(path)?),
    })
}

impl<T: Read> Read for Volume<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
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
            SeekFrom::Current(x) => {
                self.inner.fill_buf()?;
                self.inner.seek_relative(x)?;
                self.stream_position()
            }
            _ => self.inner.seek(pos),
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
    vol.seek(SeekFrom::Current(4))?;
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

fn fixup_buf(boot: &Boot, mft_header: &MFTHeader, buf: &mut [u8]) {
    let offset: usize = mft_header.fixup_offset.into();
    let sig: [u8; 2] = buf[offset..offset + 2].try_into().unwrap();
    for entry in 1..usize::from(mft_header.fixup_entries) {
        let orig_offset = offset + (entry - 1) * 2;
        let orig: [u8; 2] = buf[orig_offset..orig_offset + 2].try_into().unwrap();
        let sector_end = entry * usize::from(boot.sector_size);
        let check: &mut [u8] = &mut buf[sector_end - 2..sector_end];
        assert_eq!(&sig, check);
        check.copy_from_slice(&orig);
    }
}

fn parse_mft_attr<T: Read + Seek>(cur: &mut T) -> io::Result<MFTAttr> {
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
        Content::Resident { data }
    } else {
        let run_start_vcn = cur.read_u64::<LE>()?;
        let run_end_vcn = cur.read_u64::<LE>()?;
        let run_offset = cur.read_u16::<LE>()?;
        cur.seek(SeekFrom::Current(6))?;
        let alloc_size = cur.read_u64::<LE>()?;
        let size = cur.read_u64::<LE>()?;
        cur.seek(SeekFrom::Start(start_pos + u64::from(run_offset)))?;
        let run_lists = parse_run_lists(cur)?;
        Content::NonResident {
            run_start_vcn,
            run_end_vcn,
            alloc_size,
            size,
            run_lists,
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

fn parse_mft_attrs<T: Read + Seek>(cur: &mut T) -> io::Result<Vec<MFTAttr>> {
    let mut attrs: Vec<MFTAttr> = Vec::new();
    while cur.read_u16::<LE>()? != u16::MAX {
        let pos = cur.seek(SeekFrom::Current(-2))?;
        let attr = parse_mft_attr(cur)?;
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

fn parse_run_lists<T: Read + Seek>(cur: &mut T) -> io::Result<Vec<RunList>> {
    let mut runs = Vec::new();
    let mut first_byte = cur.read_u8()?;
    while first_byte > 0 {
        let length_length = u8::MAX.wrapping_shr(4) & first_byte;
        let offset_length = first_byte.wrapping_shr(4);
        let length = u64::from_le_bytes(read_int_bytes(length_length, cur, false)?);
        let offset = i64::from_le_bytes(read_int_bytes(offset_length, cur, true)?);
        runs.push(RunList { length, offset });
        first_byte = cur.read_u8()?;
    }
    Ok(runs)
}

fn parse_mft_entry<T: Read + Seek>(boot: &Boot, vol: &mut T) -> io::Result<MFTEntry> {
    let mut buf = [0u8; 1024];
    vol.read_exact(&mut buf)?;
    let mut cur = Cursor::new(buf);
    let header = parse_mft_header(&mut cur)?;
    fixup_buf(&boot, &header, &mut buf);
    cur.seek(SeekFrom::Start(header.attr_offset.into()))?;
    let attrs = parse_mft_attrs(&mut cur)?;
    Ok(MFTEntry { header, attrs })
}

fn write_content<T: Read + Seek, U: Write>(
    boot: &Boot,
    content: &Content,
    source: &mut T,
    dest: &mut U,
) -> io::Result<()> {
    match content {
        Content::Resident { data } => {
            dest.write(data.as_slice())?;
        }
        Content::NonResident {
            size, run_lists, ..
        } => {
            let cs = u64::from(boot.cluster_size);
            let mut offset: i64 = 0;
            let mut remaining = *size;
            let mut buf = Vec::new();
            buf.resize(boot.cluster_size.into(), 0);
            for run_list in run_lists {
                offset = offset + run_list.offset;
                source.seek(SeekFrom::Start(u64::try_from(offset).unwrap() * cs))?;
                for i in 0..run_list.length {
                    if remaining < cs {
                        buf.resize(remaining.try_into().unwrap(), 0);
                        assert_eq!(i + 1, run_list.length);
                    }
                    source.read_exact(buf.as_mut_slice())?;
                    dest.write(buf.as_slice())?;
                    remaining = remaining - cs;
                }
            }
        }
    };
    Ok(())
}

fn go_to_mft<T: Read + Seek>(boot: &Boot, vol: &mut T) -> io::Result<()> {
    vol.seek(SeekFrom::Start(boot.mft_start))?;
    Ok(())
}

pub fn extract_mft<T: Read + Seek, U: Write>(vol: &mut T, dest: &mut U) -> io::Result<()> {
    println!("Copying $MFT");
    let boot = parse_boot(vol)?;
    go_to_mft(&boot, vol)?;
    let entry = parse_mft_entry(&boot, vol)?;
    for attr in entry.attrs {
        if attr.attr_type == 128 {
            write_content(&boot, &attr.content, vol, dest)?;
        }
    }
    Ok(())
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
        fixup_buf(&boot, &header, &mut buf);
        let fail = catch_unwind(move || fixup_buf(&boot, &header, &mut buf));
        assert!(fail.is_err())
    }

    #[test]
    fn test_parse_attrs() {
        let mut vol = open_volume(r#"\\.\C:"#).unwrap();
        let boot = parse_boot(&mut vol).unwrap();
        go_to_mft(&boot, &mut vol).unwrap();
        let entry = parse_mft_entry(&boot, &mut vol).unwrap();
        for attr in entry.attrs {
            println!("{:?}", &attr);
            match attr.content {
                Content::NonResident {
                    run_lists,
                    alloc_size,
                    ..
                } => {
                    assert_eq!(
                        alloc_size,
                        u64::from(boot.cluster_size)
                            * run_lists.iter().map(|x| x.length).sum::<u64>()
                    )
                }
                Content::Resident { .. } => (),
            }
        }
    }

    #[test]
    fn test_parse_run_lists() {
        let data: [u8; 8] = [0x21, 0x10, 0x00, 0x01, 0x11, 0x20, 0xE0, 0x00];
        let mut cur = Cursor::new(data);
        let run_lists = parse_run_lists(&mut cur).unwrap();
        let valid = vec![
            RunList {
                length: 16,
                offset: 256,
            },
            RunList {
                length: 32,
                offset: -32,
            },
        ];
        assert_eq!(run_lists, valid);
    }

    #[test]
    fn test_write_mft() {
        let mut dest = File::create("MFT").unwrap();
        let mut vol = open_volume(r#"\\.\C:"#).unwrap();
        let boot = parse_boot(&mut vol).unwrap();
        go_to_mft(&boot, &mut vol).unwrap();
        let entry = parse_mft_entry(&boot, &mut vol).unwrap();
        for attr in entry.attrs {
            if attr.attr_type == 128 {
                write_content(&boot, &attr.content, &mut vol, &mut dest).unwrap();
                let file_size = dest.metadata().unwrap().len();
                std::fs::remove_file("MFT").unwrap();
                println!("MFT SIZE: {}", file_size);
                match attr.content {
                    Content::Resident { .. } => (),
                    Content::NonResident { size, .. } => assert_eq!(size, file_size),
                }
            }
        }
    }
}

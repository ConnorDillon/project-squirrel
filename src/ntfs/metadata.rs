use byteorder::{ReadBytesExt, LE};
use std::convert::{TryFrom, TryInto};
use std::io::{self, Cursor, Read, Seek, SeekFrom};

use super::content::{load_runs, Content, ContentReader, DataRun};

#[derive(Debug)]
pub struct MFTEntry<T> {
    volume: T,
    header: MFTHeader,
    attrs: Vec<MFTAttr>,
}

impl<T: Read + Seek> MFTEntry<T> {
    pub fn data(&mut self) -> io::Result<Option<ContentReader<&mut T>>> {
        for attr in &self.attrs {
            if attr.attr_type == 128 {
                return Ok(Some(attr.content.reader(&mut self.volume)));
            }
        }
	Ok(None)
    }
    pub fn into_data(self) -> io::Result<Option<ContentReader<T>>> {
        for attr in self.attrs {
            if attr.attr_type == 128 {
                return Ok(Some(attr.content.reader(self.volume)));
            }
        }
	Ok(None)
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
    pub attr_type: u32,
    length: u32,
    name_length: u8,
    name_offset: u16,
    flags: u16,
    attr_id: u16,
    pub content: Content,
}

pub fn parse_mft_entry<T, U: Read>(
    sector_size: u16,
    cluster_size: u16,
    volume: T,
    mut mft_reader: U,
) -> io::Result<MFTEntry<T>> {
    let mut buf = [0u8; 1024];
    mft_reader.read_exact(&mut buf)?;
    let mut cur = Cursor::new(buf);
    let header = parse_mft_header(&mut cur)?;
    fixup_buf(sector_size, &header, &mut buf);
    cur.seek(SeekFrom::Start(header.attr_offset.into()))?;
    let attrs = parse_mft_attrs(&mut cur, cluster_size)?;
    Ok(MFTEntry {
        header,
        attrs,
        volume,
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

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use super::super::file_system::MFT;
    use super::*;

    #[test]
    fn test_fixup() {
        let mut buf: [u8; 1024] = [0; 1024];
        let mut mft = MFT::open(r#"\\.\C:"#).unwrap();
        mft.data.read_exact(&mut buf).unwrap();
        let header = parse_mft_header(&mut mft.data).unwrap();
        fixup_buf(mft.boot.sector_size, &header, &mut buf);
        let fail = catch_unwind(move || fixup_buf(mft.boot.sector_size, &header, &mut buf));
        assert!(fail.is_err())
    }

    #[test]
    fn test_parse_attrs() {
        let mut mft = MFT::open(r#"\\.\C:"#).unwrap();
        let entry = parse_mft_entry(
            mft.boot.sector_size,
            mft.boot.cluster_size,
            r#"\\.\C:"#,
            &mut mft.data,
        )
        .unwrap();
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
}

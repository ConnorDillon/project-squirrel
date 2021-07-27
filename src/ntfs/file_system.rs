use byteorder::{ReadBytesExt, LE};
use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::io::Cursor;
use std::io::{Read, Seek, SeekFrom};

use super::content::{open_volume, ContentReader, Volume};
use super::metadata::{parse_mft_entry, MFTEntry};

pub struct MFT {
    pub data: ContentReader<Volume<File>>,
    pub boot: Boot,
}

impl MFT {
    pub fn open<T: Into<String>>(volume: T) -> io::Result<MFT> {
        let vol_path = volume.into();
        let mut vol = open_volume(&vol_path)?;
        let boot = parse_boot(&mut vol)?;
        go_to_mft(&boot, &mut vol)?;
        let mut buf = [0u8; 1024];
        vol.read_exact(&mut buf)?;
        let entry = parse_mft_entry(boot.sector_size, boot.cluster_size, vol, Cursor::new(buf))?;
        let data = entry.into_data()?.unwrap();
        Ok(MFT {
            data,
            boot,
        })
    }
    pub fn open_entry<T>(&mut self, volume: T, idx: i64) -> io::Result<MFTEntry<T>> {
        self.data
            .seek(SeekFrom::Start(u64::try_from(idx).unwrap() * 1024))?;
        parse_mft_entry(
            self.boot.sector_size,
            self.boot.cluster_size,
            volume,
            &mut self.data,
        )
    }
}

#[derive(Debug)]
pub struct Boot {
    pub sector_size: u16,
    pub cluster_size: u16,
    pub mft_start: u64,
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

fn go_to_mft<T: Read + Seek>(boot: &Boot, vol: &mut T) -> io::Result<()> {
    vol.seek(SeekFrom::Start(boot.mft_start))?;
    Ok(())
}

#[cfg(test)]
mod tests {
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
    fn test_read_mft() {
        let mut mft = MFT::open(r#"\\.\C:"#).unwrap();
        let mut buf = [0u8; 1024];
        for i in 0..mft.data.size() / 1024 {
            mft.data.read_exact(&mut buf).unwrap();
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
        let mut vol = open_volume(r#"\\.\C:"#).unwrap();
        let mut entry = mft.open_entry(&mut vol, 2).unwrap();
        let mut data = entry.data().unwrap().unwrap();
        io::copy(&mut data, &mut dest).unwrap();
        let file_size = dest.metadata().unwrap().len();
        std::fs::remove_file("LogFile").unwrap();
        assert_eq!(data.size(), file_size);
    }
}

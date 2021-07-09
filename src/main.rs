use byteorder::{ReadBytesExt, LE};
use getopts::Matches;
use getopts::Options;
use glob::glob;
use json;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::env;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::windows::fs::symlink_dir;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::str;
use zip::write::FileOptions;
use zip::ZipWriter;

fn set_opts() -> Options {
    let mut opts = Options::new();
    opts.optflag("h", "help", "Show this help information.");
    opts.optflag(
        "",
        "no-snapshot",
        "Don't create VSS shapshots. Please note that this will prevent \
        collecting locked files from a live system.",
    );
    opts.optopt(
        "w",
        "working-dir",
        "Where to store the files created during execution. \
         Will be created if it does not exist and will be cleaned up after. \
         Defaults to %TEMP%\\squirrel_work.",
        "PATH",
    );
    opts.optopt(
        "d",
        "destination",
        "Where to transfer the collected files. If this flag is not \
         specified the working dir won't be removed and archive.zip \
         can be collected mantually.",
        "URL",
    );
    opts.optmulti(
        "p",
        "path",
        "Collect files matching the path pattern (glob syntax), \
         the path must start with a drive letter.",
        "PATH",
    );
    opts.optflag("f", "prefetch", "Collect Prefetch files.");
    opts.optflag("r", "registry", "Collect system Registry files.");
    opts.optflag("e", "event-logs", "Collect Event Logs.");
    opts.optflag("n", "ntuser", "Collect NTUSER.DAT Registry files.");
    opts.optflag("c", "usrclass", "Collect UsrClass.dat Registry files.");
    opts.optflag("i", "hiberfile", "Collect hiberfile.sys.");
    opts.optflag(
        "j",
        "jump-lists",
        "Collect Jump Lists and LNK files in the recent folder.",
    );
    opts.optflag("s", "swapfile", "Collect swapfile.sys and pagefile.sys.");
    opts.optflag("u", "startup", "Collect files in the startup folder.");
    opts.optflag("t", "scheduled-tasks", "Collect Scheduled Tasks.");
    opts.optflag("m", "mft", "Collect NTFS Master File Table ($MFT).");
    return opts;
}

const PATHS: [(&str, &str); 11] = [
    ("prefetch", r#"C:\Windows\Prefetch\*.pf"#),
    ("registry", r#"C:\Windows\System32\config\*"#),
    ("event-logs", r#"C:\Windows\System32\winevt\logs\*.evtx"#),
    ("ntuser", r#"C:\Users\*\NTUSER.DAT*"#),
    (
        "usrclass",
        r#"C:\Users\*\AppData\Local\Microsoft\Windows\UsrClass.dat*"#,
    ),
    (
        "jump-lists",
        r#"C:\Users\*\AppData\Roaming\Microsoft\Windows\Recent\**\*"#,
    ),
    ("hiberfile", r#"C:\hiberfil.sys"#),
    ("swapfile", r#"C:\????file.sys"#),
    ("startup", r#"C:\Users\*\Start Menu\Programs\Startup\*"#),
    ("scheduled-tasks", r#"C:\Windows\System32\Tasks\**\*"#),
    ("mft", r#"C:\$MFT"#),
];

#[derive(Debug)]
struct Params {
    help: bool,
    no_snapshot: bool,
    working_dir: PathBuf,
    destination: Option<String>,
    paths: Paths,
}

fn read_params(opts: &Options, args: &Vec<String>) -> Params {
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!("{:?}", f),
    };
    Params {
        help: matches.opt_present("help"),
        no_snapshot: matches.opt_present("no-snapshot"),
        working_dir: matches.opt_str("working-dir").map_or_else(
            || join_path(env::temp_dir(), "squirrel_work"),
            PathBuf::from,
        ),
        destination: matches.opt_str("destination"),
        paths: get_paths(&matches),
    }
}

type Paths = HashMap<String, Vec<String>>;

fn get_paths(matches: &Matches) -> Paths {
    let mut paths: Paths = HashMap::new();
    let mut path_vec: Vec<String> = matches.opt_strs_pos("p").into_iter().map(|p| p.1).collect();
    for (flag, path) in PATHS.iter() {
        if matches.opt_present(flag) {
            path_vec.push(String::from(*path));
        }
    }
    for mut drive in path_vec {
        let pattern = drive.split_off(3);
        match paths.get_mut(&drive) {
            Some(ps) => ps.push(pattern),
            None => {
                paths.insert(drive, vec![pattern]);
            }
        }
    }
    paths
}

fn join_path<T: AsRef<Path>>(mut path: PathBuf, next: T) -> PathBuf {
    path.push(next);
    path
}

fn main() {
    let opts = set_opts();
    let args: Vec<String> = env::args().collect();
    let params = read_params(&opts, &args);
    if params.help {
        print!("{}", opts.usage("Usage: squirrel [options]"));
    } else {
        if !params.working_dir.exists() {
            fs::create_dir(&params.working_dir).unwrap();
        }
        let archive_path = join_path(params.working_dir.clone(), "archive.zip");
        let file = File::create(&archive_path).unwrap();
        let file_buf = BufWriter::new(file);
        let mut archive = ZipWriter::new(file_buf);

        for (drive, patterns) in params.paths.iter() {
            let drive_letter = &drive[0..1];

            let (volume, snap) = if params.no_snapshot {
                env::set_current_dir(&drive).unwrap();
                (format!("\\\\.\\{}:", drive_letter), None)
            } else {
                let shadow_id = create_snapshot(drive);
                let mount_point = join_path(
                    params.working_dir.clone(),
                    format!("mount-{}", drive_letter),
                );
                let device_id = get_device_object(&shadow_id);
                mount_snapshot(&device_id, &mount_point);
                env::set_current_dir(&mount_point).unwrap();
                (device_id, Some((shadow_id, mount_point)))
            };

            for pattern in patterns.iter() {
                copy_files(&volume, drive_letter, pattern, &mut archive);
            }

            if let Some((shadow_id, mount_point)) = snap {
                delete_snapshot(&shadow_id, &mount_point);
            }
        }

        archive.flush().unwrap();
        let mut file_buf = archive.finish().unwrap();
        file_buf.flush().unwrap();

        if let Some(dest) = params.destination {
            let file = File::open(&archive_path).unwrap();
            let file_buf = BufReader::new(file);
            transfer_archive(file_buf, &dest);
            fs::remove_file(&archive_path).unwrap();
            fs::remove_dir(&params.working_dir).unwrap();
        }
    }
}

fn transfer_archive<T: Read>(file: T, dest: &str) {
    let resp = ureq::post(&format!("{}/new", dest)).call().unwrap();
    let location = resp.header("Location").unwrap();
    ureq::post(&format!("{}{}", dest, location))
        .set("Content-Type", "application/octet-stream")
        .send(file)
        .unwrap();
}

fn create_snapshot(volume: &str) -> String {
    let command = format!(
        "ConvertTo-Json (Invoke-CimMethod -ClassName Win32_ShadowCopy -MethodName Create \
         -Arguments @{{Volume = \"{}\"}})",
        volume
    );
    let output = Command::new("powershell")
        .arg("-Command")
        .arg(command)
        .output()
        .expect("Failed to execute PowerShell");
    let stdout = str::from_utf8(&output.stdout).expect("Failed to parse stdout as UTF-8");
    let stderr = String::from_utf8(output.stderr).expect("Failed to parse stderr as UTF-8");
    match json::parse(&stdout) {
        Ok(result) => {
            let return_value = result["ReturnValue"].as_number().expect("No ReturnValue");
            if return_value == 0 {
                let shadow_id = result["ShadowID"].as_str().expect("No ShadowID");
                return shadow_id.to_string();
            } else {
                panic!(
                    "Snapshot creation failed, return_value: {}, stderr: {}",
                    return_value, stderr
                )
            }
        }
        Err(_) => panic!("Snapshot creation failed, stderr: {}", stderr),
    }
}

fn delete_snapshot(shadow_id: &str, mount_point: &Path) {
    let args = [
        "delete",
        "shadows",
        "/quiet",
        &format!("/shadow={}", shadow_id),
    ];
    Command::new("vssadmin")
        .args(&args)
        .output()
        .expect("Failed to execute vssadmin");
    fs::remove_dir(mount_point).unwrap();
}

fn get_device_object(shadow_id: &str) -> String {
    let command = format!(
        "(Get-CimInstance Win32_ShadowCopy | \
         Where-Object {{ $_.ID -eq \"{}\"}}).DeviceObject",
        shadow_id
    );
    let output = Command::new("powershell")
        .arg("-Command")
        .arg(command)
        .output()
        .expect("Failed to execute PowerShell");
    let stderr = str::from_utf8(&output.stderr).expect("Failed to parse stderr as UTF-8");
    if !stderr.is_empty() {
        panic!("{}", stderr)
    }
    let out = str::from_utf8(&output.stdout)
        .expect("Failed to parse stdout as UTF-8")
        .trim_end();
    String::from(out)
}

fn mount_snapshot(device_id: &str, mount_point: &Path) {
    let devid = format!("{}\\", device_id);
    symlink_dir(&devid, mount_point).expect(&format!(
        "Failed to create symlink: {} {:?}",
        devid, mount_point
    ));
}

fn copy_files<T: Write + Seek>(
    volume: &str,
    drive: &str,
    pattern: &str,
    archive: &mut ZipWriter<T>,
) {
    let opts = FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    match pattern {
        "$MFT" => {
            archive
                .start_file(format!("{}\\{}", drive, "MFT"), opts)
                .unwrap();
            extract_mft(volume, archive).unwrap()
        }
        _ => {
            for entry in glob(pattern).unwrap() {
                let path_buf = entry.unwrap();
                let path = path_buf.to_str().unwrap();
                if path_buf.as_path().is_file() {
                    println!("Copying {}", path);
                    let file = File::open(path).expect(&format!("Failed to open {}", path));
                    let mut file_buf = BufReader::new(file);
                    archive
                        .start_file(format!("{}\\{}", drive, path), opts)
                        .unwrap();
                    io::copy(&mut file_buf, archive).unwrap();
                }
            }
        }
    }
}

#[derive(Debug)]
struct Boot {
    sector_size: u16,
    cluster_size: u16,
    mft_start: u64,
}

fn parse_boot(buf: &[u8]) -> io::Result<Boot> {
    let mut cur = Cursor::new(buf);
    cur.seek(SeekFrom::Start(11))?;
    let sector_size = cur.read_u16::<LE>()?;
    let sectors_per_cluster = cur.read_u16::<LE>()?;
    let cluster_size: u16 = (sector_size * sectors_per_cluster).into();
    cur.seek(SeekFrom::Start(48))?;
    let mft_start_cluster = cur.read_u64::<LE>()?;
    let mft_start = mft_start_cluster * u64::from(cluster_size);
    Ok(Boot {
        sector_size,
        cluster_size,
        mft_start,
    })
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

fn parse_mft_header(buf: &[u8]) -> io::Result<MFTHeader> {
    let mut cur = Cursor::new(buf);
    cur.seek(SeekFrom::Start(4))?;
    let fixup_offset = cur.read_u16::<LE>()?;
    let fixup_entries = cur.read_u16::<LE>()?;
    cur.seek(SeekFrom::Start(20))?;
    let attr_offset = cur.read_u16::<LE>()?;
    let flags = cur.read_u16::<LE>()?;
    let used_size = cur.read_u32::<LE>()?;
    let alloc_size = cur.read_u32::<LE>()?;
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

#[derive(Debug, PartialEq)]
struct RunList {
    length: u64,
    offset: i64,
}

fn read_bytes<T: Read>(num_bytes: u8, cur: &mut T, signed: bool) -> io::Result<[u8; 8]> {
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
        let length = u64::from_le_bytes(read_bytes(length_length, cur, false)?);
        let offset = i64::from_le_bytes(read_bytes(offset_length, cur, true)?);
        runs.push(RunList { length, offset });
        first_byte = cur.read_u8()?;
    }
    Ok(runs)
}

#[derive(Debug)]
struct MFTEntry {
    header: MFTHeader,
    attrs: Vec<MFTAttr>,
}

fn parse_mft_entry(boot: &Boot, buf: &mut [u8]) -> io::Result<MFTEntry> {
    let header = parse_mft_header(buf)?;
    fixup_buf(&boot, &header, buf);
    let mut cur = Cursor::new(buf);
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

fn go_to_mft(volume: &str, buf: &mut [u8]) -> (File, Boot) {
    let mut file = File::open(volume).expect(volume);
    file.read_exact(buf).unwrap();
    let boot = parse_boot(&buf).unwrap();
    file.seek(SeekFrom::Start(boot.mft_start)).unwrap();
    file.read_exact(buf).unwrap();
    (file, boot)
}

fn extract_mft<T: Write>(volume: &str, dest: &mut T) -> io::Result<()> {
    println!("Copying $MFT");
    let mut buf: [u8; 1024] = [0; 1024];
    let (mut file, boot) = go_to_mft(volume, &mut buf);
    let entry = parse_mft_entry(&boot, &mut buf)?;
    for attr in entry.attrs {
        if attr.attr_type == 128 {
            write_content(&boot, &attr.content, &mut file, dest)?;
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
        let _ = go_to_mft(r#"\\.\C:"#, &mut buf);
        println!("{}", hex_str(buf.iter()));
        assert_eq!(&buf[0..4], "FILE".as_bytes())
    }

    #[test]
    fn test_fixup() {
        let mut buf: [u8; 1024] = [0; 1024];
        let (_, boot) = go_to_mft(r#"\\.\C:"#, &mut buf);
        let header = parse_mft_header(&buf).unwrap();
        fixup_buf(&boot, &header, &mut buf);
        let fail = catch_unwind(move || fixup_buf(&boot, &header, &mut buf));
        assert!(fail.is_err())
    }

    #[test]
    fn test_parse_attrs() {
        let mut buf: [u8; 1024] = [0; 1024];
        let (_, boot) = go_to_mft(r#"\\.\C:"#, &mut buf);
        let header = parse_mft_header(&buf).unwrap();
        fixup_buf(&boot, &header, &mut buf);
        let mut cur = Cursor::new(buf);
        cur.seek(SeekFrom::Start(header.attr_offset.into()))
            .unwrap();
        let attrs = parse_mft_attrs(&mut cur).unwrap();
        for attr in attrs {
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
        let mut buf: [u8; 1024] = [0; 1024];
        let (mut file, boot) = go_to_mft(r#"\\.\C:"#, &mut buf);
        let entry = parse_mft_entry(&boot, &mut buf).unwrap();
        for attr in entry.attrs {
            if attr.attr_type == 128 {
                write_content(&boot, &attr.content, &mut file, &mut dest).unwrap();
                let file_size = dest.metadata().unwrap().len();
                fs::remove_file("MFT").unwrap();
                println!("MFT SIZE: {}", file_size);
                match attr.content {
                    Content::Resident { .. } => (),
                    Content::NonResident { size, .. } => assert_eq!(size, file_size),
                }
            }
        }
    }
}

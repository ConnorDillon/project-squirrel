use getopts::{Options, Matches};
use glob::glob;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::os::windows::fs::symlink_dir;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, io, str};
use zip::write::FileOptions;
use zip::ZipWriter;

mod mft;

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
            mft::extract_mft(volume, archive).unwrap()
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

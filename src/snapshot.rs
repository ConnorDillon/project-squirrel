use std::os::windows::fs::symlink_dir;
use std::path::Path;
use std::process::Command;
use std::str;

pub fn create(volume: &str) -> String {
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

pub fn delete(shadow_id: &str) {
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
}

pub fn get_device_object(shadow_id: &str) -> String {
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

pub fn mount(device_id: &str, mount_point: &Path) {
    let devid = format!("{}\\", device_id);
    symlink_dir(&devid, mount_point).expect(&format!(
        "Failed to create symlink: {} {:?}",
        devid, mount_point
    ));
}

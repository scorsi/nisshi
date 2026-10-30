//! An argument that is not UTF-8 is a usage error, not a panic (validation P2).

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::process::{Command, Stdio};

#[test]
fn non_utf8_argument_is_a_usage_error() {
    let out = Command::new(env!("CARGO_BIN_EXE_nisshi-store"))
        .arg("check-config")
        .arg(OsStr::from_bytes(b"\xff.toml"))
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(
        stderr.starts_with("nisshi-store: argument not UTF-8: "),
        "{stderr}"
    );
    assert!(!stderr.contains("panicked"), "{stderr}");
}

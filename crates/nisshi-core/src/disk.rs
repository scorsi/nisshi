//! `statvfs(3)` and `flock(2)` declared by hand (no `libc` crate, ADR 0015). For `statvfs` only
//! the first five fields are read; the struct is oversized so that any libc layout fits.

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::raw::{c_char, c_int};
use std::path::Path;

#[cfg(target_os = "linux")]
#[repr(C)]
struct StatVfs {
    f_bsize: u64,
    f_frsize: u64,
    f_blocks: u64,
    f_bfree: u64,
    f_bavail: u64,
    _rest: [u64; 32],
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct StatVfs {
    f_bsize: u64,
    f_frsize: u64,
    f_blocks: u32,
    f_bfree: u32,
    f_bavail: u32,
    _rest: [u64; 32],
}

unsafe extern "C" {
    fn statvfs(path: *const c_char, buf: *mut StatVfs) -> c_int;
    fn flock(fd: c_int, operation: c_int) -> c_int;
}

/// Bytes available to an unprivileged writer on `path`'s filesystem.
// `f_bavail` is u64 on Linux but u32 on macOS: the conversion is only useless on Linux.
#[allow(clippy::useless_conversion)]
pub fn free_bytes(path: &Path) -> io::Result<u64> {
    let c = CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::other("path with NUL"))?;
    let mut st = StatVfs {
        f_bsize: 0,
        f_frsize: 0,
        f_blocks: 0,
        f_bfree: 0,
        f_bavail: 0,
        _rest: [0; 32],
    };
    // SAFETY: `c` is a valid NUL-terminated path and `st` outlives the call; the struct starts
    // with the fields every supported libc puts first and is larger than any real `statvfs`.
    if unsafe { statvfs(c.as_ptr(), &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(u64::from(st.f_bavail) * st.f_frsize)
}

/// `flock(LOCK_EX | LOCK_NB)`: same values on Linux and macOS.
pub fn lock_exclusive(f: &File) -> io::Result<()> {
    const LOCK_EX: c_int = 2;
    const LOCK_NB: c_int = 4;
    // SAFETY: a valid open descriptor; flock reads nothing else.
    if unsafe { flock(f.as_raw_fd(), LOCK_EX | LOCK_NB) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn statvfs_and_flock() {
        let free = super::free_bytes(std::path::Path::new(".")).unwrap();
        assert!(free > 0);
        assert!(super::free_bytes(std::path::Path::new("/nonexistent/x")).is_err());
        let a = std::fs::File::open(".").unwrap();
        let b = std::fs::File::open(".").unwrap();
        super::lock_exclusive(&a).unwrap();
        assert!(super::lock_exclusive(&b).is_err());
        drop(a);
        super::lock_exclusive(&b).unwrap();
    }
}

// SPDX-License-Identifier: MIT
//! Raw `sendmsg` / `recvmsg` wrappers for `SCM_RIGHTS` fd passing.

use std::io;
use std::os::fd::{BorrowedFd, FromRawFd, OwnedFd};

use crate::proto::{MAX_FD_SPACE, MAX_FDS};

pub const SCM_MAX_FD: usize = MAX_FDS as usize;

#[cfg(any(target_os = "linux", target_os = "android"))]
const SENDMSG_FLAGS: libc::c_int = libc::MSG_NOSIGNAL;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const SENDMSG_FLAGS: libc::c_int = 0;

#[cfg(any(target_os = "linux", target_os = "android"))]
const RECVMSG_FLAGS: libc::c_int = libc::MSG_CMSG_CLOEXEC;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const RECVMSG_FLAGS: libc::c_int = 0;

#[inline]
pub fn cmsg_space_for_fd_count(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    unsafe { libc::CMSG_SPACE((n * std::mem::size_of::<i32>()) as libc::c_uint) as usize }
}


#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn set_cloexec(fd: i32) -> io::Result<()> {
    loop {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(err);
        }

        let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(err);
    }
}

pub fn sendmsg_fds(socket_fd: BorrowedFd<'_>, data: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
    use std::os::fd::AsRawFd;

    if fds.len() > SCM_MAX_FD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("too many file descriptors for one fbsockets message: {} > {}", fds.len(), SCM_MAX_FD),
        ));
    }

    let cmsg_buf_len = cmsg_space_for_fd_count(fds.len());
    if cmsg_buf_len > MAX_FD_SPACE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("ancillary fd space too large: {} > {}", cmsg_buf_len, MAX_FD_SPACE),
        ));
    }

    let raw_fd = socket_fd.as_raw_fd();
    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut libc::c_void,
        iov_len: data.len(),
    };

    let mut cmsg_buf = vec![0u8; cmsg_buf_len];
    let mut mhdr: libc::msghdr = unsafe { std::mem::zeroed() };
    mhdr.msg_iov = &mut iov;
    mhdr.msg_iovlen = 1;

    if !fds.is_empty() {
        mhdr.msg_control = cmsg_buf.as_mut_ptr().cast();
        mhdr.msg_controllen = cmsg_buf_len as _;

        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&mhdr);
            if cmsg.is_null() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "CMSG_FIRSTHDR returned null"));
            }
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN((fds.len() * std::mem::size_of::<i32>()) as libc::c_uint) as _;

            let fd_dst: *mut i32 = libc::CMSG_DATA(cmsg).cast();
            for (i, fd) in fds.iter().enumerate() {
                std::ptr::write(fd_dst.add(i), fd.as_raw_fd());
            }
        }
    }

    let n = unsafe { libc::sendmsg(raw_fd, &mhdr, SENDMSG_FLAGS) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

pub fn recvmsg_fds(socket_fd: BorrowedFd<'_>, buf: &mut [u8], max_fds: usize) -> io::Result<(usize, Vec<OwnedFd>)> {
    use std::os::fd::AsRawFd;

    let max_fds = max_fds.min(SCM_MAX_FD);
    let cmsg_buf_len = cmsg_space_for_fd_count(max_fds);
    if cmsg_buf_len > MAX_FD_SPACE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("ancillary fd space too large: {} > {}", cmsg_buf_len, MAX_FD_SPACE),
        ));
    }

    let raw_fd = socket_fd.as_raw_fd();
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };

    let mut cmsg_buf = vec![0u8; cmsg_buf_len];
    let mut mhdr: libc::msghdr = unsafe { std::mem::zeroed() };
    mhdr.msg_iov = &mut iov;
    mhdr.msg_iovlen = 1;

    if max_fds > 0 {
        mhdr.msg_control = cmsg_buf.as_mut_ptr().cast();
        mhdr.msg_controllen = cmsg_buf_len as _;
    }

    let n = unsafe { libc::recvmsg(raw_fd, &mut mhdr, RECVMSG_FLAGS) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }

    if mhdr.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SCM_RIGHTS control message truncated by kernel",
        ));
    }

    let mut received_fds = Vec::new();
    if n > 0 && max_fds > 0 {
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(&mhdr);
            while !cmsg.is_null() {
                if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                    let data_ptr: *const i32 = libc::CMSG_DATA(cmsg).cast();
                    let data_len = ((*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize)
                        / std::mem::size_of::<i32>();

                    for i in 0..data_len {
                        let raw = std::ptr::read(data_ptr.add(i));
                        #[cfg(not(any(target_os = "linux", target_os = "android")))]
                        set_cloexec(raw)?;
                        received_fds.push(OwnedFd::from_raw_fd(raw));
                    }
                }
                cmsg = libc::CMSG_NXTHDR(&mhdr, cmsg);
            }
        }
    }

    Ok((n as usize, received_fds))
}

// SPDX-License-Identifier: MIT
//! Raw `sendmsg` / `recvmsg` wrappers for SCM_RIGHTS fd passing.
//!
//! These operate on the raw fd from an `AsyncFd` or `UnixStream` and
//! are **blocking** (called inside `tokio::task::spawn_blocking` or
//! after the socket is confirmed writable/readable via `AsyncFd`).
//!
//! # Safety
//!
//! All `unsafe` blocks are tightly scoped around the `libc` FFI calls
//! with full validation of return values.

use std::io;
use std::os::unix::io::{BorrowedFd, OwnedFd, FromRawFd};

// ── helpers ──────────────────────────────────────────────────────────

/// Compute the cmsg buffer size needed for `n` file descriptors.
fn cmsg_space(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    // SAFETY: simple arithmetic wrapper.
    unsafe { libc::CMSG_SPACE((n * std::mem::size_of::<i32>()) as libc::c_uint) as usize }
}

// ── sendmsg ──────────────────────────────────────────────────────────

/// Send `data` and optionally `fds` over `socket_fd`.
///
/// Returns the number of **data bytes** actually sent (may be less than
/// `data.len()` — the caller must handle partial sends).
pub fn sendmsg_fds(
    socket_fd: BorrowedFd<'_>,
    data: &[u8],
    fds: &[BorrowedFd<'_>],
) -> io::Result<usize> {
    use std::os::unix::io::AsRawFd;

    let raw_fd = socket_fd.as_raw_fd();

    let mut iov = libc::iovec {
        iov_base: data.as_ptr() as *mut libc::c_void,
        iov_len: data.len(),
    };

    let fd_count = fds.len();
    let cmsg_buf_len = cmsg_space(fd_count);
    // Heap-allocate + zero so alignment is correct for cmsghdr.
    let mut cmsg_buf = vec![0u8; cmsg_buf_len];

    let mut mhdr: libc::msghdr = unsafe { std::mem::zeroed() };
    mhdr.msg_iov = &mut iov;
    mhdr.msg_iovlen = 1;

    if fd_count > 0 {
        mhdr.msg_control = cmsg_buf.as_mut_ptr().cast();
        mhdr.msg_controllen = cmsg_buf_len as _;

        // SAFETY: we allocated cmsg_buf with the exact CMSG_SPACE.
        unsafe {
            let cmsg: *mut libc::cmsghdr = libc::CMSG_FIRSTHDR(&mhdr);
            assert!(!cmsg.is_null());
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len =
                libc::CMSG_LEN((fd_count * std::mem::size_of::<i32>()) as libc::c_uint) as _;

            let fd_dst: *mut i32 = libc::CMSG_DATA(cmsg).cast();
            for (i, fd) in fds.iter().enumerate() {
                std::ptr::write(fd_dst.add(i), fd.as_raw_fd());
            }
        }
    }

    // SAFETY: mhdr is fully initialised, socket_fd is valid (borrow).
    let n = unsafe { libc::sendmsg(raw_fd, &mhdr, libc::MSG_NOSIGNAL) };

    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

// ── recvmsg ──────────────────────────────────────────────────────────

/// Receive data (into `buf`) and any ancillary fds from `socket_fd`.
///
/// Returns `(bytes_read, received_fds)`.  `bytes_read == 0` signals
/// peer close.
pub fn recvmsg_fds(
    socket_fd: BorrowedFd<'_>,
    buf: &mut [u8],
    max_fds: usize,
) -> io::Result<(usize, Vec<OwnedFd>)> {
    use std::os::unix::io::AsRawFd;

    let raw_fd = socket_fd.as_raw_fd();

    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };

    let cmsg_buf_len = cmsg_space(max_fds);
    let mut cmsg_buf = vec![0u8; cmsg_buf_len.max(1)]; // at least 1 byte so ptr is valid

    let mut mhdr: libc::msghdr = unsafe { std::mem::zeroed() };
    mhdr.msg_iov = &mut iov;
    mhdr.msg_iovlen = 1;

    if max_fds > 0 {
        mhdr.msg_control = cmsg_buf.as_mut_ptr().cast();
        mhdr.msg_controllen = cmsg_buf_len as _;
    }

    let n = unsafe { libc::recvmsg(raw_fd, &mut mhdr, libc::MSG_CMSG_CLOEXEC) };

    if n < 0 {
        return Err(io::Error::last_os_error());
    }

    // Truncated control data means fds were dropped by the kernel.
    // We log but do not error — the fd_count in the header lets the
    // receiver detect the mismatch at the protocol layer.
    if mhdr.msg_flags & libc::MSG_CTRUNC != 0 {
        tracing::warn!("SCM_RIGHTS truncated — some fds may have been dropped");
    }

    let mut received_fds = Vec::new();

    if n > 0 && max_fds > 0 {
        // SAFETY: kernel wrote valid cmsg data into cmsg_buf.
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(&mhdr);
            while !cmsg.is_null() {
                if (*cmsg).cmsg_level == libc::SOL_SOCKET
                    && (*cmsg).cmsg_type == libc::SCM_RIGHTS
                {
                    let data_ptr: *const i32 = libc::CMSG_DATA(cmsg).cast();
                    let data_len = ((*cmsg).cmsg_len as usize
                        - libc::CMSG_LEN(0) as usize)
                        / std::mem::size_of::<i32>();

                    for i in 0..data_len {
                        let raw = std::ptr::read(data_ptr.add(i));
                        // Wrap in OwnedFd immediately so it gets closed on drop.
                        received_fds.push(OwnedFd::from_raw_fd(raw));
                    }
                }
                cmsg = libc::CMSG_NXTHDR(&mhdr, cmsg);
            }
        }
    }

    Ok((n as usize, received_fds))
}

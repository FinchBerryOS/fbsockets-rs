// SPDX-License-Identifier: MIT
//! Protocol definitions for fbsockets.
//!
//! On-wire header:
//!
//! ```text
//! ┌──────────┬────────────────┬────────────────┐
//! │  magic   │  payload_len   │    fd_count    │
//! │ (2 byte) │   (4 bytes)    │    (4 bytes)   │
//! └──────────┴────────────────┴────────────────┘
//! ```
//!
//! After the header, exactly `payload_len` bytes follow on the stream.
//! File descriptors are attached via `SCM_RIGHTS` to the header `sendmsg`.

use std::os::fd::OwnedFd;

pub const MAGIC: [u8; 2] = [0xFB, 0x01];
pub const HEADER_LEN: usize = 10;

pub const MAX_PAYLOAD: u32 = 16 * 1024 * 1024;
pub const MAX_FDS: u32 = 64;
/// Policy budget for ancillary FD space: 64 × 4-byte raw FDs + 200 bytes slack.
pub const MAX_FD_SPACE: u32 = (64 * 4 + 200) as u32;
pub const MAX_TOTAL_MESSAGE: u32 = MAX_PAYLOAD + MAX_FD_SPACE;

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("bad magic: expected 0xFB01, got 0x{0:02X}{1:02X}")]
    BadMagic(u8, u8),

    #[error("payload too large: {0} bytes (max {MAX_PAYLOAD})")]
    PayloadTooLarge(u32),

    #[error("too many file descriptors: {0} (max {MAX_FDS})")]
    TooManyFds(u32),

    #[error("ancillary fd space too large: {0} bytes (max {MAX_FD_SPACE})")]
    FdSpaceTooLarge(u32),

    #[error("fd count mismatch: header advertised {expected}, received {actual}")]
    FdCountMismatch { expected: usize, actual: usize },

    #[error("unexpected {0} file descriptor(s) received during payload read")]
    UnexpectedFds(usize),

    #[error("connection closed by peer")]
    PeerClosed,

    #[error("fork detected: original pid {expected}, current pid {actual}")]
    ForkDetected { expected: u32, actual: u32 },

    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    #[error("connection poisoned: a previous timeout or I/O error left the stream in an inconsistent state")]
    Poisoned,
}

pub type Result<T> = std::result::Result<T, ProtoError>;

#[derive(Debug)]
pub struct Message {
    pub data: Vec<u8>,
    pub fds: Vec<OwnedFd>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub send_timeout: Option<std::time::Duration>,
    pub recv_timeout: Option<std::time::Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            send_timeout: Some(std::time::Duration::from_secs(30)),
            recv_timeout: Some(std::time::Duration::from_secs(30)),
        }
    }
}

impl Config {
    pub fn unconstrained() -> Self {
        Self {
            send_timeout: None,
            recv_timeout: None,
        }
    }
}

#[inline]
pub const fn fd_space_for_count(fd_count: u32) -> u32 {
    fd_count.saturating_mul(4).saturating_add(200)
}

#[inline]
pub fn validate_message_shape(payload_len: usize, fd_count: usize) -> Result<()> {
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| ProtoError::PayloadTooLarge(u32::MAX))?;
    if payload_len_u32 > MAX_PAYLOAD {
        return Err(ProtoError::PayloadTooLarge(payload_len_u32));
    }

    let fd_count_u32 = u32::try_from(fd_count).map_err(|_| ProtoError::TooManyFds(u32::MAX))?;
    if fd_count_u32 > MAX_FDS {
        return Err(ProtoError::TooManyFds(fd_count_u32));
    }

    let fd_space = fd_space_for_count(fd_count_u32);
    if fd_space > MAX_FD_SPACE {
        return Err(ProtoError::FdSpaceTooLarge(fd_space));
    }

    let total = payload_len_u32.saturating_add(fd_space);
    if total > MAX_TOTAL_MESSAGE {
        return Err(ProtoError::PayloadTooLarge(payload_len_u32));
    }

    Ok(())
}

#[inline]
pub fn encode_header(buf: &mut [u8; HEADER_LEN], payload_len: u32, fd_count: u32) {
    buf[0] = MAGIC[0];
    buf[1] = MAGIC[1];
    buf[2..6].copy_from_slice(&payload_len.to_be_bytes());
    buf[6..10].copy_from_slice(&fd_count.to_be_bytes());
}

#[inline]
pub fn decode_header(buf: &[u8; HEADER_LEN]) -> Result<(u32, u32)> {
    if buf[0] != MAGIC[0] || buf[1] != MAGIC[1] {
        return Err(ProtoError::BadMagic(buf[0], buf[1]));
    }

    let payload_len = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
    let fd_count = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);

    validate_message_shape(payload_len as usize, fd_count as usize)?;
    Ok((payload_len, fd_count))
}

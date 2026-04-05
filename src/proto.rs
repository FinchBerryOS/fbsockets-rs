// SPDX-License-Identifier: MIT
//! Wire protocol definitions for fbsockets.
//!
//! Semantics:
//! - every application data frame is acknowledged explicitly by the peer
//! - the fixed header is always sent first
//! - if file descriptors are present, they are sent in a dedicated 1-byte fd prelude after the header
//! - payload bytes are sent only after header and optional fd prelude
//! - one logical message maps to exactly one DATA frame
//! - the logical-message payload limit is 256 KiB
//! - ACK is emitted only when the receiving application takes the queued message via `recv()`
//! - the protocol is stop-and-wait: at most one unacknowledged inbound DATA message may exist per connection direction

use std::os::fd::OwnedFd;

pub const MAGIC: [u8; 2] = [0xFB, 0x02];
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 32;
pub const FD_PRELUDE_LEN: usize = 1;
pub const FD_PRELUDE_MARKER: u8 = 0xFD;

pub const FRAME_KIND_DATA: u8 = 1;
pub const FRAME_KIND_ACK: u8 = 2;

pub const MAX_PAYLOAD: u32 = 256 * 1024;
pub const MAX_FDS: u32 = 64;
pub const MAX_IO_CHUNK: usize = 64 * 1024;
/// Policy budget for ancillary FD space: 64 × 4-byte raw FDs + 200 bytes slack.
pub const MAX_FD_SPACE: u32 = (64 * 4 + 200) as u32;
pub const MAX_TOTAL_MESSAGE: u32 = MAX_PAYLOAD + MAX_FD_SPACE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Data,
    Ack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub kind: FrameKind,
    pub payload_len: u32,
    pub fd_count: u32,
    pub message_id: u64,
    pub ack_id: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("bad magic: expected 0xFB02, got 0x{0:02X}{1:02X}")]
    BadMagic(u8, u8),

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("invalid frame kind: {0}")]
    InvalidFrameKind(u8),

    #[error("payload too large: {0} bytes (max {MAX_PAYLOAD})")]
    PayloadTooLarge(u32),

    #[error("too many file descriptors: {0} (max {MAX_FDS})")]
    TooManyFds(u32),

    #[error("ancillary fd space too large: {0} bytes (max {MAX_FD_SPACE})")]
    FdSpaceTooLarge(u32),

    #[error("fd count mismatch: header advertised {expected}, received {actual}")]
    FdCountMismatch { expected: usize, actual: usize },

    #[error("unexpected {0} file descriptor(s) received")]
    UnexpectedFds(usize),

    #[error("invalid fd prelude marker: expected 0xFD, got 0x{0:02X}")]
    InvalidFdPrelude(u8),

    #[error("invalid frame shape for ACK")]
    InvalidAckFrame,

    #[error("invalid frame shape for DATA")]
    InvalidDataFrame,

    #[error("connection closed by peer")]
    PeerClosed,

    #[error("fork detected: original pid {expected}, current pid {actual}")]
    ForkDetected { expected: u32, actual: u32 },

    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    #[error("protocol violation: {0}")]
    ProtocolViolation(&'static str),

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
pub fn encode_header(buf: &mut [u8; HEADER_LEN], header: Header) {
    buf.fill(0);
    buf[0] = MAGIC[0];
    buf[1] = MAGIC[1];
    buf[2] = VERSION;
    buf[3] = match header.kind {
        FrameKind::Data => FRAME_KIND_DATA,
        FrameKind::Ack => FRAME_KIND_ACK,
    };
    buf[4..8].copy_from_slice(&header.payload_len.to_be_bytes());
    buf[8..12].copy_from_slice(&header.fd_count.to_be_bytes());
    buf[12..20].copy_from_slice(&header.message_id.to_be_bytes());
    buf[20..28].copy_from_slice(&header.ack_id.to_be_bytes());
}

#[inline]
pub fn decode_header(buf: &[u8; HEADER_LEN]) -> Result<Header> {
    if buf[0] != MAGIC[0] || buf[1] != MAGIC[1] {
        return Err(ProtoError::BadMagic(buf[0], buf[1]));
    }
    if buf[2] != VERSION {
        return Err(ProtoError::UnsupportedVersion(buf[2]));
    }

    let kind = match buf[3] {
        FRAME_KIND_DATA => FrameKind::Data,
        FRAME_KIND_ACK => FrameKind::Ack,
        other => return Err(ProtoError::InvalidFrameKind(other)),
    };

    let payload_len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let fd_count = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let message_id = u64::from_be_bytes([
        buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
    ]);
    let ack_id = u64::from_be_bytes([
        buf[20], buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27],
    ]);

    match kind {
        FrameKind::Data => {
            validate_message_shape(payload_len as usize, fd_count as usize)?;
            if message_id == 0 || ack_id != 0 {
                return Err(ProtoError::InvalidDataFrame);
            }
        }
        FrameKind::Ack => {
            if payload_len != 0 || fd_count != 0 || ack_id == 0 {
                return Err(ProtoError::InvalidAckFrame);
            }
        }
    }

    Ok(Header {
        kind,
        payload_len,
        fd_count,
        message_id,
        ack_id,
    })
}

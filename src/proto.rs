// SPDX-License-Identifier: MIT
//! Wire protocol definitions.
//!
//! # Frame Layout (on the wire)
//!
//! ```text
//! ┌──────────┬──────────┬──────────┬──────────────────────┐
//! │  magic   │  flags   │ payload  │   fd_count (1 byte)  │
//! │ (2 bytes)│ (1 byte) │  length  │                      │
//! │  0xFB01  │          │ (4 bytes)│                      │
//! ├──────────┴──────────┴──────────┴──────────────────────┤
//! │                   payload bytes                       │
//! │              (0 .. 1_048_576 bytes)                   │
//! └───────────────────────────────────────────────────────┘
//! ```
//!
//! File descriptors (0..253) are transmitted as SCM_RIGHTS ancillary
//! data alongside the **header** bytes only.
//!
//! ## Flags
//!
//! Bit 0 (`0x01`) is **reserved** for keepalive and must not be used
//! by application code.  Bits 1..7 are free for application use.
//!
//! ## Keepalive
//!
//! A keepalive frame is defined by the conjunction of **all three**:
//!
//! 1. `flags == FLAG_KEEPALIVE` (exactly `0x01`, no other bits set)
//! 2. `payload_len == 0`
//! 3. `fd_count == 0`
//!
//! Receivers **must** silently consume keepalive frames and not
//! surface them to the application layer.

/// Wire magic: 0xFB 0x01  (mnemonic: **F**rame**B**inary v**01**)
pub const MAGIC: [u8; 2] = [0xFB, 0x01];

/// Fixed header size in bytes.
pub const HEADER_LEN: usize = 8; // 2 magic + 1 flags + 4 payload_len + 1 fd_count

/// Maximum payload size: 1 MiB.
pub const MAX_PAYLOAD: u32 = 1_048_576;

/// Maximum number of file descriptors per frame.
/// Linux SCM_RIGHTS hard-limits this to 253.
pub const MAX_FDS: u8 = 253;

// ── Flag bits ────────────────────────────────────────────────────────

/// No flags set.
pub const FLAG_NONE: u8 = 0x00;

/// Keepalive frame marker.  **Reserved — do not use in application flags.**
///
/// A frame is only treated as keepalive when `flags == FLAG_KEEPALIVE`
/// **and** payload and fd count are both zero.
pub const FLAG_KEEPALIVE: u8 = 0x01;

// Bits 1..7 are application-defined.
// Reserved for future protocol use:
// pub const FLAG_COMPRESSED: u8 = 0x02;

// ── Error types ──────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("bad magic: expected 0xFB01, got 0x{0:02X}{1:02X}")]
    BadMagic(u8, u8),

    #[error("payload too large: {0} bytes (max {MAX_PAYLOAD})")]
    PayloadTooLarge(u32),

    #[error("fd count too large: {0} (max {MAX_FDS})")]
    TooManyFds(u8),

    #[error("fd count mismatch: header advertised {expected}, received {actual}")]
    FdCountMismatch { expected: usize, actual: usize },

    #[error("connection closed by peer")]
    PeerClosed,

    #[error("fork detected: original pid {expected}, current pid {actual}")]
    ForkDetected { expected: u32, actual: u32 },

    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    #[error("connection poisoned: a previous timeout or I/O error left the stream in an inconsistent state")]
    Poisoned,

    #[error("flags bit 0 (FLAG_KEEPALIVE) is reserved and must not be set by application code")]
    ReservedFlag,

    #[error("unexpected {0} file descriptor(s) received on continuation read")]
    UnexpectedFds(usize),
}

pub type Result<T> = std::result::Result<T, ProtoError>;

// ── Frame (decoded) ──────────────────────────────────────────────────

/// A decoded frame ready for application consumption.
#[derive(Debug)]
pub struct Frame {
    /// Flags byte.  Bit 0 is reserved for keepalive; bits 1..7 are
    /// application-defined.
    pub flags: u8,
    /// Payload bytes (0 .. 1 MiB).
    pub payload: Vec<u8>,
    /// File descriptors received via SCM_RIGHTS.
    pub fds: Vec<std::os::unix::io::OwnedFd>,
}

impl Frame {
    /// Returns `true` if this is a keepalive frame.
    ///
    /// A keepalive is strictly: `flags == 0x01`, empty payload, no fds.
    #[inline]
    pub fn is_keepalive(&self) -> bool {
        self.flags == FLAG_KEEPALIVE && self.payload.is_empty() && self.fds.is_empty()
    }
}

// ── Config ───────────────────────────────────────────────────────────

/// Timeouts and keepalive configuration for a connection.
///
/// All timeouts are optional.  `None` = no timeout (infinite wait).
#[derive(Debug, Clone)]
pub struct Config {
    /// Timeout for a single `send()` call (entire frame).
    pub send_timeout: Option<std::time::Duration>,
    /// Timeout for a single `recv()` call (entire frame).
    pub recv_timeout: Option<std::time::Duration>,
    /// Interval at which the send-half emits keepalive frames.
    /// Set to `None` to disable keepalives.
    pub keepalive_interval: Option<std::time::Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            send_timeout: Some(std::time::Duration::from_secs(30)),
            recv_timeout: Some(std::time::Duration::from_secs(30)),
            keepalive_interval: Some(std::time::Duration::from_secs(15)),
        }
    }
}

impl Config {
    /// No timeouts, no keepalives.  Useful for tests.
    pub fn unconstrained() -> Self {
        Self {
            send_timeout: None,
            recv_timeout: None,
            keepalive_interval: None,
        }
    }
}

// ── Header codec ─────────────────────────────────────────────────────

/// Encode a frame header into `buf` (must be `HEADER_LEN` bytes).
pub fn encode_header(buf: &mut [u8; HEADER_LEN], flags: u8, payload_len: u32, fd_count: u8) {
    buf[0] = MAGIC[0];
    buf[1] = MAGIC[1];
    buf[2] = flags;
    buf[3..7].copy_from_slice(&payload_len.to_be_bytes());
    buf[7] = fd_count;
}

/// Decode and validate a frame header.
///
/// Returns `(flags, payload_len, fd_count)`.
pub fn decode_header(buf: &[u8; HEADER_LEN]) -> Result<(u8, u32, u8)> {
    if buf[0] != MAGIC[0] || buf[1] != MAGIC[1] {
        return Err(ProtoError::BadMagic(buf[0], buf[1]));
    }
    let flags = buf[2];
    let payload_len = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]);
    let fd_count = buf[7];

    if payload_len > MAX_PAYLOAD {
        return Err(ProtoError::PayloadTooLarge(payload_len));
    }
    if fd_count > MAX_FDS {
        return Err(ProtoError::TooManyFds(fd_count));
    }
    Ok((flags, payload_len, fd_count))
}

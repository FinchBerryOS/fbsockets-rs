// SPDX-License-Identifier: MIT
//! PID-based fork detection.
//!
//! The guard records `getpid()` at construction time.  Every call to
//! [`PidGuard::check`] compares the stored PID with the current one.
//! After `fork()` the child inherits the socket *and* the guard, but
//! `getpid()` returns a different value → instant detection.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::proto::ProtoError;

/// Lightweight fork detector.
///
/// Stored inside every [`Connection`] and checked on each `send` / `recv`.
#[derive(Debug)]
pub struct PidGuard {
    /// PID captured at construction time.
    owner_pid: AtomicU32,
}

impl PidGuard {
    /// Snapshot the current PID.
    pub fn new() -> Self {
        Self {
            // `std::process::id()` returns `u32` and is async-signal-safe.
            owner_pid: AtomicU32::new(std::process::id()),
        }
    }

    /// Return `Ok(())` if we are still in the original process,
    /// or `Err(ForkDetected)` if a fork happened.
    #[inline]
    pub fn check(&self) -> crate::proto::Result<()> {
        let expected = self.owner_pid.load(Ordering::Relaxed);
        let actual = std::process::id();
        if expected != actual {
            return Err(ProtoError::ForkDetected { expected, actual });
        }
        Ok(())
    }

    /// Return the PID that owns this guard.
    #[inline]
    pub fn owner_pid(&self) -> u32 {
        self.owner_pid.load(Ordering::Relaxed)
    }
}

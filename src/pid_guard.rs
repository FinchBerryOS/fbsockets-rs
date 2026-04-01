// SPDX-License-Identifier: MIT
//! PID-based fork detection.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::proto::ProtoError;

#[derive(Debug)]
pub struct PidGuard {
    owner_pid: AtomicU32,
    fork_seen: AtomicBool,
}

impl Default for PidGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl PidGuard {
    pub fn new() -> Self {
        Self {
            owner_pid: AtomicU32::new(std::process::id()),
            fork_seen: AtomicBool::new(false),
        }
    }

    #[inline]
    pub fn check(&self) -> crate::proto::Result<()> {
        let expected = self.owner_pid.load(Ordering::Relaxed);
        let actual = std::process::id();
        if expected != actual {
            self.fork_seen.store(true, Ordering::Relaxed);
            return Err(ProtoError::ForkDetected { expected, actual });
        }
        Ok(())
    }

    #[inline]
    pub fn owner_pid(&self) -> u32 {
        self.owner_pid.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn fork_seen(&self) -> bool {
        self.fork_seen.load(Ordering::Relaxed)
    }
}

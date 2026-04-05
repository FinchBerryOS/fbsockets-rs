// SPDX-License-Identifier: MIT
//! PID-based fork detection.

use crate::proto::ProtoError;

#[derive(Debug)]
pub struct PidGuard {
    owner_pid: u32,
}

impl Default for PidGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl PidGuard {
    pub fn new() -> Self {
        Self {
            owner_pid: std::process::id(),
        }
    }

    #[inline]
    pub fn check(&self) -> crate::proto::Result<()> {
        let expected = self.owner_pid;
        let actual = std::process::id();
        if expected != actual {
            return Err(ProtoError::ForkDetected { expected, actual });
        }
        Ok(())
    }

    #[inline]
    pub const fn owner_pid(&self) -> u32 {
        self.owner_pid
    }
}

// SPDX-License-Identifier: MIT
//! Tokio async wrapper around the blocking `fbsockets` core.
//!
//! This module intentionally does not implement a second protocol engine.
//! All framing, stop-and-wait semantics, ACK timing, FD handling, poisoning,
//! and fork detection live in the blocking core (`crate::connection` and
//! `crate::listener`). The async API delegates to that core via
//! `tokio::task::spawn_blocking`.

use std::os::fd::{BorrowedFd, OwnedFd, RawFd};
use std::path::Path;
use std::sync::Arc;

use tokio::task;

use crate::connection::{self as blocking, PeerCredentials};
use crate::listener as blocking_listener;
use crate::proto::{self, Config, ProtoError};

fn join_error_to_proto(err: task::JoinError) -> ProtoError {
    ProtoError::Io(std::io::Error::other(format!("async wrapper worker failed: {err}")))
}

fn clone_borrowed_fds(fds: &[BorrowedFd<'_>]) -> proto::Result<Vec<OwnedFd>> {
    fds.iter()
        .map(|fd| fd.try_clone_to_owned().map_err(ProtoError::from))
        .collect()
}

#[derive(Clone, Debug)]
pub struct Connection {
    inner: Arc<blocking::Connection>,
}

impl Connection {
    pub fn from_blocking(inner: blocking::Connection) -> Self {
        Self { inner: Arc::new(inner) }
    }

    pub fn from_owned_fd(fd: OwnedFd, config: Config) -> proto::Result<Self> {
        blocking::Connection::from_owned_fd(fd, config).map(Self::from_blocking)
    }

    /// # Safety
    ///
    /// `raw_fd` must be a valid owned Unix-domain stream socket file descriptor.
    /// After calling this function, ownership is transferred into the returned
    /// `Connection`, and the caller must not use or close `raw_fd` again.
    pub unsafe fn from_raw_fd(raw_fd: RawFd, config: Config) -> proto::Result<Self> {
        unsafe { blocking::Connection::from_raw_fd(raw_fd, config) }.map(Self::from_blocking)
    }

    pub fn into_blocking(self) -> Result<blocking::Connection, Self> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => Ok(inner),
            Err(inner) => Err(Self { inner }),
        }
    }

    pub async fn connect<P: AsRef<Path>>(path: P, config: Config) -> proto::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = task::spawn_blocking(move || blocking::Connection::connect(path, config))
            .await
            .map_err(join_error_to_proto)??;
        Ok(Self::from_blocking(conn))
    }

    #[inline]
    pub fn owner_pid(&self) -> u32 {
        self.inner.owner_pid()
    }

    #[inline]
    pub fn is_send_poisoned(&self) -> bool {
        self.inner.is_send_poisoned()
    }

    #[inline]
    pub fn is_recv_poisoned(&self) -> bool {
        self.inner.is_recv_poisoned()
    }

    #[inline]
    pub fn peer_credentials(&self) -> proto::Result<PeerCredentials> {
        self.inner.peer_credentials()
    }

    #[inline]
    pub fn peer_pid(&self) -> proto::Result<Option<u32>> {
        self.inner.peer_pid()
    }

    #[inline]
    pub fn peer_uid(&self) -> proto::Result<Option<u32>> {
        self.inner.peer_uid()
    }

    #[inline]
    pub fn peer_gid(&self) -> proto::Result<Option<u32>> {
        self.inner.peer_gid()
    }

    pub async fn send(&self, data: &[u8], fds: &[BorrowedFd<'_>]) -> proto::Result<()> {
        // Validate before cloning so callers get the same fast-path errors as the core.
        proto::validate_message_shape(data.len(), fds.len())?;

        let inner = Arc::clone(&self.inner);
        let data = data.to_vec();
        let owned_fds = clone_borrowed_fds(fds)?;

        task::spawn_blocking(move || {
            let borrowed: Vec<_> = owned_fds.iter().map(|fd| fd.as_fd()).collect();
            inner.send(&data, &borrowed)
        })
        .await
        .map_err(join_error_to_proto)?
    }

    pub async fn recv(&self) -> proto::Result<(Vec<u8>, Vec<OwnedFd>)> {
        let inner = Arc::clone(&self.inner);
        task::spawn_blocking(move || inner.recv())
            .await
            .map_err(join_error_to_proto)?
    }
}

#[derive(Clone, Debug)]
pub struct Listener {
    inner: Arc<blocking_listener::Listener>,
}

impl Listener {
    pub async fn bind<P: AsRef<Path>>(path: P) -> proto::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let listener = task::spawn_blocking(move || blocking_listener::Listener::bind(path))
            .await
            .map_err(join_error_to_proto)??;
        Ok(Self { inner: Arc::new(listener) })
    }

    pub async fn bind_clean<P: AsRef<Path>>(path: P) -> proto::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let listener = task::spawn_blocking(move || blocking_listener::Listener::bind_clean(path))
            .await
            .map_err(join_error_to_proto)??;
        Ok(Self { inner: Arc::new(listener) })
    }

    pub fn from_blocking(inner: blocking_listener::Listener) -> Self {
        Self { inner: Arc::new(inner) }
    }

    pub fn into_blocking(self) -> Result<blocking_listener::Listener, Self> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => Ok(inner),
            Err(inner) => Err(Self { inner }),
        }
    }

    pub async fn accept(&self, config: Config) -> proto::Result<Connection> {
        let inner = Arc::clone(&self.inner);
        let conn = task::spawn_blocking(move || inner.accept(config))
            .await
            .map_err(join_error_to_proto)??;
        Ok(Connection::from_blocking(conn))
    }
}

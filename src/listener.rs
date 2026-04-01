// SPDX-License-Identifier: MIT
//! Async listener that accepts incoming connections.
//!
//! [`Listener::bind`] is a thin wrapper around `tokio::net::UnixListener::bind`.
//! [`Listener::bind_clean`] is the ergonomic variant for production use: it
//! removes a stale socket file at the target path when that path is already a
//! Unix-domain socket, but refuses to delete non-socket files.

use std::fs;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;

use tokio::net::UnixListener;

use crate::connection::Connection;
use crate::proto::{self, Config};

/// A listening socket that produces [`Connection`]s.
pub struct Listener {
    inner: UnixListener,
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listener").finish_non_exhaustive()
    }
}

impl Listener {
    /// Bind to `path`.
    ///
    /// This is intentionally strict: if the path already exists, the bind will
    /// fail and the caller must decide how to handle the existing filesystem
    /// entry.
    pub fn bind<P: AsRef<Path>>(path: P) -> proto::Result<Self> {
        let inner = UnixListener::bind(path)?;
        Ok(Self { inner })
    }

    /// Bind to `path`, removing a stale Unix socket first if necessary.
    ///
    /// If `path` already exists and is a Unix-domain socket, it is removed
    /// before binding. If it exists and is **not** a socket, this returns the
    /// original `AlreadyExists` error rather than deleting an unrelated file.
    pub fn bind_clean<P: AsRef<Path>>(path: P) -> proto::Result<Self> {
        let path = path.as_ref();

        match fs::symlink_metadata(path) {
            Ok(meta) => {
                if meta.file_type().is_socket() {
                    fs::remove_file(path)?;
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!(
                            "refusing to remove existing non-socket path: {}",
                            path.display()
                        ),
                    )
                    .into());
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        Self::bind(path)
    }

    /// Wrap an existing `tokio::net::UnixListener`.
    pub fn from_listener(inner: UnixListener) -> Self {
        Self { inner }
    }

    /// Accept the next incoming connection with the given config.
    pub async fn accept(&self, config: Config) -> proto::Result<Connection> {
        let (stream, _addr) = self.inner.accept().await?;
        Ok(Connection::from_stream(stream, config))
    }

    /// Borrow the inner `UnixListener`.
    pub fn inner(&self) -> &UnixListener {
        &self.inner
    }
}

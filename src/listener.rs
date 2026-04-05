// SPDX-License-Identifier: MIT
//! Blocking listener that accepts incoming fbsockets connections.

use std::fs;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixListener;
use std::path::Path;

use crate::connection::Connection;
use crate::proto::{self, Config};

#[derive(Debug)]
pub struct Listener {
    inner: UnixListener,
}

impl Listener {
    pub fn bind<P: AsRef<Path>>(path: P) -> proto::Result<Self> {
        let inner = UnixListener::bind(path)?;
        Ok(Self { inner })
    }

    pub fn bind_clean<P: AsRef<Path>>(path: P) -> proto::Result<Self> {
        let path = path.as_ref();
        match fs::symlink_metadata(path) {
            Ok(meta) => {
                if meta.file_type().is_socket() {
                    fs::remove_file(path)?;
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!("refusing to remove non-socket path: {}", path.display()),
                    )
                    .into());
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        Self::bind(path)
    }

    pub fn from_listener(inner: UnixListener) -> Self {
        Self { inner }
    }

    pub fn accept(&self, config: Config) -> proto::Result<Connection> {
        let (stream, _addr) = self.inner.accept()?;
        Connection::from_stream(stream, config)
    }

    pub fn inner(&self) -> &UnixListener {
        &self.inner
    }
}

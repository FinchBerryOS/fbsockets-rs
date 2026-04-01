// SPDX-License-Identifier: MIT
//! `fbsockets` — low-level framed IPC over Unix domain sockets.
//!
//! Default API:
//! - blocking [`Connection`] and [`Listener`]
//! - one send operation and one receive operation at a time
//! - up to 16 MiB payload per message
//! - up to 64 file descriptors per message
//! - fork detection: inherited sockets are shut down in the child on first use
//!
//! Optional feature:
//! - `async`: enables [`r#async::Connection`] and [`r#async::Listener`] on top of Tokio
//!
//! # Blocking example
//! ```rust,no_run
//! use fbsockets::{Config, Connection};
//!
//! # fn demo() -> fbsockets::Result<()> {
//! let conn = Connection::connect("/tmp/fbsockets.sock", Config::default())?;
//! conn.send(b"ping", &[])?;
//! let (data, fds) = conn.recv()?;
//! assert_eq!(data, b"pong");
//! assert!(fds.is_empty());
//! # Ok(())
//! # }
//! ```
//!
//! # Async example
//! ```rust,no_run
//! # #[cfg(feature = "async")]
//! # async fn demo() -> fbsockets::Result<()> {
//! use fbsockets::{Config};
//! use fbsockets::r#async::Connection;
//!
//! let conn = Connection::connect("/tmp/fbsockets.sock", Config::default()).await?;
//! conn.send(b"ping", &[]).await?;
//! let (data, fds) = conn.recv().await?;
//! assert_eq!(data, b"pong");
//! assert!(fds.is_empty());
//! # Ok(())
//! # }
//! ```

pub mod connection;
pub mod listener;
pub mod pid_guard;
pub mod proto;
pub mod scm;

#[cfg(feature = "async")]
pub mod r#async;

pub use connection::Connection;
pub use listener::Listener;
pub use proto::{Config, Message, ProtoError, Result};

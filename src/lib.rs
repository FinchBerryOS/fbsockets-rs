// SPDX-License-Identifier: MIT
//! **wireframe** — robust async IPC protocol for Unix domain sockets.
//!
//! Features:
//! - Frame-based send/recv over Unix domain sockets (SOCK_STREAM).
//! - File descriptor passing via SCM_RIGHTS (up to 253 per frame).
//! - Payload up to 1 MiB per frame.
//! - **Split API**: `SendHalf` / `RecvHalf` — no interleaving possible.
//! - **Timeouts**: configurable per-operation deadlines.
//! - **Keepalive**: background heartbeat frames help detect dead connections.
//! - **Fork detection**: any send/recv after fork returns an error.
//! - Full EINTR / partial-write handling.
//! - `OwnedFd`-based FD lifecycle (CLOEXEC, auto-close on drop).
//!
//! # Quick start
//!
//! ```rust,no_run
//! use wireframe::{Connection, Listener};
//! use wireframe::proto::{Config, FLAG_NONE};
//!
//! # async fn demo() -> wireframe::proto::Result<()> {
//! // Server
//! let listener = Listener::bind_clean("/tmp/wireframe.sock")?;
//! let conn = listener.accept(Config::default()).await?;
//! let (tx, mut rx) = conn.split();
//! let frame = rx.recv().await?;
//!
//! // Client
//! let client = Connection::connect("/tmp/wireframe.sock", Config::default()).await?;
//! let (mut ctx, _crx) = client.split();
//! ctx.send(FLAG_NONE, b"ping", &[]).await?;
//! # Ok(())
//! # }
//! ```

pub mod proto;
pub mod pid_guard;
pub mod scm;
pub mod connection;
pub mod listener;

pub use connection::{Connection, SendHalf, RecvHalf};
pub use listener::Listener;

// SPDX-License-Identifier: MIT
//! `fbsockets` — framed IPC over Unix domain sockets.
//!
//! Current core semantics:
//! - blocking core is the single source of truth
//! - stop-and-wait delivery with ACK per logical message
//! - max 256 KiB payload per logical message
//! - optional FD passing via `SCM_RIGHTS`
//! - one background reader thread per blocking connection
//! - `recv()` returns only after its ACK was successfully sent
//! - a second DATA before ACK is a protocol violation
//! - inherited sockets are invalidated on first use after `fork()` in the child
//! - connections can also be constructed from an owned Unix socket file descriptor
//!
//! Optional feature:
//! - `async`: Tokio wrapper around the blocking core via `spawn_blocking`

pub mod connection;
pub mod listener;
pub mod pid_guard;
pub mod proto;
pub mod scm;

#[cfg(feature = "async")]
pub mod r#async;

pub use connection::{Connection, PeerCredentials};
pub use listener::Listener;
pub use proto::{Config, Message, ProtoError, Result};

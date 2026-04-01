// SPDX-License-Identifier: MIT
//! Async framed connection with send/recv split, progress-aware
//! poison-on-timeout, keepalive, FD passing, and fork detection.
//!
//! # Architecture
//!
//! A [`Connection`] is split into a [`SendHalf`] and [`RecvHalf`]
//! via [`Connection::split`].  Each half is `Send` and exclusively
//! owns its direction — no interleaving is possible.
//!
//! ## Poison semantics
//!
//! If a timeout fires **during** a partially-written or partially-read
//! frame (i.e. at least one byte has already been transferred), the
//! half is **poisoned**.  All subsequent operations on that half
//! return [`ProtoError::Poisoned`] immediately.  The other half
//! remains usable (a send timeout does not poison the recv half).
//!
//! Timeouts that fire **before** any bytes have been transferred
//! (e.g. waiting for the socket to become writable/readable) are safe
//! and do **not** poison the half — the caller may simply retry.
//!
//! ```rust,no_run
//! use wireframe::{Connection, proto::{Config, FLAG_NONE}};
//! use tokio::net::UnixStream;
//!
//! # async fn example() -> wireframe::proto::Result<()> {
//! let (a, _b) = UnixStream::pair()?;
//! let conn = Connection::from_stream(a, Config::unconstrained());
//! let (tx, rx) = conn.split();
//!
//! tx.send(FLAG_NONE, b"hello", &[]).await?;
//! let frame = rx.recv().await?;
//! # Ok(())
//! # }
//! ```

use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::Interest;
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use crate::pid_guard::PidGuard;
use crate::proto::{
    self, Config, Frame, ProtoError, FLAG_KEEPALIVE, HEADER_LEN, MAX_FDS, MAX_PAYLOAD,
};
use crate::scm;

// ── Internal shared socket ───────────────────────────────────────────

struct SharedSocket {
    stream: UnixStream,
}

impl SharedSocket {
    fn new(stream: UnixStream) -> Arc<Self> {
        Arc::new(Self { stream })
    }
}

// ── Connection (pre-split) ───────────────────────────────────────────

/// A framed IPC connection.
///
/// Call [`split`](Connection::split) to obtain the send and receive
/// halves.  The connection cannot be used directly for I/O — this
/// enforces the split pattern.
pub struct Connection {
    socket: Arc<SharedSocket>,
    guard: Arc<PidGuard>,
    config: Config,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("fd", &self.socket.stream.as_raw_fd())
            .field("owner_pid", &self.guard.owner_pid())
            .finish()
    }
}

impl Connection {
    /// Wrap an already-connected `UnixStream`.
    pub fn from_stream(stream: UnixStream, config: Config) -> Self {
        Self {
            socket: SharedSocket::new(stream),
            guard: Arc::new(PidGuard::new()),
            config,
        }
    }

    /// Connect to `path`.
    pub async fn connect<P: AsRef<std::path::Path>>(
        path: P,
        config: Config,
    ) -> proto::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self::from_stream(stream, config))
    }

    /// Split into independent send and receive halves.
    pub fn split(self) -> (SendHalf, RecvHalf) {
        let tx = SendHalf {
            socket: Arc::clone(&self.socket),
            guard: Arc::clone(&self.guard),
            send_timeout: self.config.send_timeout,
            keepalive_interval: self.config.keepalive_interval,
            poisoned: AtomicBool::new(false),
            mu: Mutex::new(()),
        };
        let rx = RecvHalf {
            socket: self.socket,
            guard: self.guard,
            recv_timeout: self.config.recv_timeout,
            poisoned: AtomicBool::new(false),
            mu: Mutex::new(()),
        };
        (tx, rx)
    }

    /// Return the PID of the process that created this connection.
    pub fn owner_pid(&self) -> u32 {
        self.guard.owner_pid()
    }
}

// ── SendHalf ─────────────────────────────────────────────────────────

/// The sending half of a split connection.
///
/// Protected by an internal `Mutex` — concurrent `send()` calls will
/// serialize rather than interleave.
///
/// If a timeout occurs **after** at least one byte has been written
/// to the socket, this half is **poisoned** and all future operations
/// return [`ProtoError::Poisoned`].
///
/// ## Flag restriction
///
/// The public `send()` method rejects flags with bit 0 set
/// (`FLAG_KEEPALIVE`).  Only the internal `send_keepalive()` may
/// use that bit.
pub struct SendHalf {
    socket: Arc<SharedSocket>,
    guard: Arc<PidGuard>,
    send_timeout: Option<Duration>,
    keepalive_interval: Option<Duration>,
    poisoned: AtomicBool,
    mu: Mutex<()>,
}

impl std::fmt::Debug for SendHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendHalf")
            .field("fd", &self.socket.stream.as_raw_fd())
            .field("owner_pid", &self.guard.owner_pid())
            .field("poisoned", &self.poisoned.load(Ordering::Relaxed))
            .finish()
    }
}

impl SendHalf {
    /// Send a frame with `flags`, `payload` (≤ 1 MiB), and optional `fds`.
    ///
    /// `flags` must **not** have bit 0 set — that bit is reserved for
    /// the keepalive protocol.  Use [`send_keepalive`](Self::send_keepalive)
    /// instead.
    pub async fn send(
        &self,
        flags: u8,
        payload: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> proto::Result<()> {
        if flags & FLAG_KEEPALIVE != 0 {
            return Err(ProtoError::ReservedFlag);
        }
        self.send_raw_frame(flags, payload, fds).await
    }

    /// Send a keepalive frame.
    pub async fn send_keepalive(&self) -> proto::Result<()> {
        self.send_raw_frame(FLAG_KEEPALIVE, &[], &[]).await
    }

    /// Spawn a background keepalive task.
    ///
    /// Returns `None` if keepalive is disabled.
    pub fn spawn_keepalive(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let interval = self.keepalive_interval?;
        let this = Arc::clone(self);

        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip immediate first tick

            loop {
                ticker.tick().await;
                match this.send_keepalive().await {
                    Ok(()) => tracing::trace!("keepalive sent"),
                    Err(ProtoError::ForkDetected { .. }) => {
                        tracing::debug!("keepalive: fork detected, stopping");
                        return;
                    }
                    Err(ProtoError::Poisoned) => {
                        tracing::debug!("keepalive: connection poisoned, stopping");
                        return;
                    }
                    Err(e) => {
                        tracing::warn!("keepalive failed: {e}");
                        return;
                    }
                }
            }
        }))
    }

    /// Returns `true` if this half is poisoned.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    /// Return the keepalive interval (if configured).
    pub fn keepalive_interval(&self) -> Option<Duration> {
        self.keepalive_interval
    }

    // ── internal ─────────────────────────────────────────────────────

    fn check_health(&self) -> proto::Result<()> {
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(ProtoError::Poisoned);
        }
        self.guard.check()
    }

    fn poison(&self) {
        if !self.poisoned.swap(true, Ordering::Relaxed) {
            tracing::error!("send half poisoned — connection is no longer usable for sending");
        }
    }

    /// Shared send path for both public `send()` and internal `send_keepalive()`.
    async fn send_raw_frame(
        &self,
        flags: u8,
        payload: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> proto::Result<()> {
        // Health is checked again after the per-direction mutex is held so a
        // concurrent poison or fork-detection event cannot race past us while
        // we are queued behind another operation.
        self.check_health()?;

        match self.send_timeout {
            Some(dur) => {
                // We need progress tracking across the timeout boundary.
                // Because tokio::time::timeout drops the future on expiry,
                // we use a shared progress flag via Arc<AtomicBool>.
                let progress = Arc::new(AtomicBool::new(false));
                let progress2 = Arc::clone(&progress);

                let fut = self.send_inner(flags, payload, fds, progress2);

                match tokio::time::timeout(dur, fut).await {
                    Ok(result) => result,
                    Err(_elapsed) => {
                        if progress.load(Ordering::Relaxed) {
                            self.poison();
                        }
                        Err(ProtoError::Timeout(dur))
                    }
                }
            }
            None => {
                // No timeout — progress flag is irrelevant.
                let progress = Arc::new(AtomicBool::new(false));
                self.send_inner(flags, payload, fds, progress).await
            }
        }
    }

    async fn send_inner(
        &self,
        flags: u8,
        payload: &[u8],
        fds: &[BorrowedFd<'_>],
        progress: Arc<AtomicBool>,
    ) -> proto::Result<()> {
        let payload_len: u32 = payload
            .len()
            .try_into()
            .map_err(|_| ProtoError::PayloadTooLarge(u32::MAX))?;

        if payload_len > MAX_PAYLOAD {
            return Err(ProtoError::PayloadTooLarge(payload_len));
        }

        let fd_count: u8 = fds
            .len()
            .try_into()
            .map_err(|_| ProtoError::TooManyFds(u8::MAX))?;

        if fd_count > MAX_FDS {
            return Err(ProtoError::TooManyFds(fd_count));
        }

        let mut hdr = [0u8; HEADER_LEN];
        proto::encode_header(&mut hdr, flags, payload_len, fd_count);

        // Hold the mutex for the entire frame to prevent interleaving.
        let _lock = self.mu.lock().await;

        // Re-check after acquiring the mutex so we do not proceed with I/O if
        // another in-flight operation poisoned this half while we were waiting.
        self.check_health()?;

        // Phase 1: header + fds.
        if let Err(e) = self.do_send_with_fds(&hdr, fds, &progress).await {
            self.poison();
            return Err(e);
        }

        // Phase 2: payload.
        if !payload.is_empty() {
            if let Err(e) = self.do_send(&progress, payload).await {
                self.poison();
                return Err(e);
            }
        }

        Ok(())
    }

    async fn do_send_with_fds(
        &self,
        data: &[u8],
        fds: &[BorrowedFd<'_>],
        progress: &Arc<AtomicBool>,
    ) -> proto::Result<()> {
        let stream = &self.socket.stream;
        let mut offset = 0usize;
        let mut fds_sent = false;

        while offset < data.len() {
            stream.writable().await?;

            let slice = &data[offset..];
            let send_fds: &[BorrowedFd<'_>] = if !fds_sent && !fds.is_empty() {
                fds
            } else {
                &[]
            };

            match stream.try_io(Interest::WRITABLE, || {
                scm::sendmsg_fds(stream.as_fd(), slice, send_fds)
            }) {
                Ok(0) => return Err(ProtoError::PeerClosed),
                Ok(n) => {
                    progress.store(true, Ordering::Relaxed);
                    if !fds_sent && !fds.is_empty() {
                        fds_sent = true;
                    }
                    offset += n;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    async fn do_send(
        &self,
        progress: &Arc<AtomicBool>,
        data: &[u8],
    ) -> proto::Result<()> {
        let stream = &self.socket.stream;
        let mut offset = 0usize;

        while offset < data.len() {
            stream.writable().await?;

            let slice = &data[offset..];

            match stream.try_io(Interest::WRITABLE, || {
                scm::sendmsg_fds(stream.as_fd(), slice, &[])
            }) {
                Ok(0) => return Err(ProtoError::PeerClosed),
                Ok(n) => {
                    progress.store(true, Ordering::Relaxed);
                    offset += n;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }
}

// ── RecvHalf ─────────────────────────────────────────────────────────

/// The receiving half of a split connection.
///
/// If a timeout occurs **after** at least one byte has been read from
/// the socket, this half is **poisoned** and all future operations
/// return [`ProtoError::Poisoned`].
pub struct RecvHalf {
    socket: Arc<SharedSocket>,
    guard: Arc<PidGuard>,
    recv_timeout: Option<Duration>,
    poisoned: AtomicBool,
    mu: Mutex<()>,
}

impl std::fmt::Debug for RecvHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvHalf")
            .field("fd", &self.socket.stream.as_raw_fd())
            .field("owner_pid", &self.guard.owner_pid())
            .field("poisoned", &self.poisoned.load(Ordering::Relaxed))
            .finish()
    }
}

impl RecvHalf {
    /// Receive exactly one application frame.
    ///
    /// Keepalive frames are silently consumed and never returned.
    pub async fn recv(&self) -> proto::Result<Frame> {
        self.check_health()?;

        match self.recv_timeout {
            Some(dur) => {
                let progress = Arc::new(AtomicBool::new(false));
                let progress2 = Arc::clone(&progress);

                let fut = self.recv_loop(progress2);

                match tokio::time::timeout(dur, fut).await {
                    Ok(result) => result,
                    Err(_elapsed) => {
                        if progress.load(Ordering::Relaxed) {
                            self.poison();
                        }
                        Err(ProtoError::Timeout(dur))
                    }
                }
            }
            None => {
                let progress = Arc::new(AtomicBool::new(false));
                self.recv_loop(progress).await
            }
        }
    }

    /// Returns `true` if this half is poisoned.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    // ── internal ─────────────────────────────────────────────────────

    fn check_health(&self) -> proto::Result<()> {
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(ProtoError::Poisoned);
        }
        self.guard.check()
    }

    fn poison(&self) {
        if !self.poisoned.swap(true, Ordering::Relaxed) {
            tracing::error!("recv half poisoned — connection is no longer usable for receiving");
        }
    }

    /// Loop until we get a non-keepalive frame.
    async fn recv_loop(&self, progress: Arc<AtomicBool>) -> proto::Result<Frame> {
        loop {
            let frame = self.recv_one_frame(&progress).await?;
            if frame.is_keepalive() {
                tracing::trace!("keepalive received — discarding");
                // Reset progress for the next frame attempt, since the
                // keepalive was fully consumed without desync risk.
                progress.store(false, Ordering::Relaxed);
                continue;
            }
            return Ok(frame);
        }
    }

    async fn recv_one_frame(&self, progress: &Arc<AtomicBool>) -> proto::Result<Frame> {
        let _lock = self.mu.lock().await;

        // Re-check after acquiring the mutex so we do not proceed with I/O if
        // another in-flight operation poisoned this half while we were waiting.
        self.check_health()?;

        // Phase 1: header + fds.
        let mut hdr_buf = [0u8; HEADER_LEN];
        let received_fds = match self
            .do_recv_with_fds(&mut hdr_buf, MAX_FDS as usize, progress)
            .await
        {
            Ok(fds) => fds,
            Err(e) => {
                // Only poison if we actually started reading.
                if progress.load(Ordering::Relaxed) {
                    self.poison();
                }
                return Err(e);
            }
        };

        let (flags, payload_len, fd_count) = match proto::decode_header(&hdr_buf) {
            Ok(decoded) => decoded,
            Err(e) => {
                self.poison();
                return Err(e);
            }
        };

        // Strict fd count validation.
        let expected = fd_count as usize;
        let actual = received_fds.len();
        if actual != expected {
            tracing::error!(expected, actual, "fd count mismatch — poisoning connection");
            self.poison();
            return Err(ProtoError::FdCountMismatch { expected, actual });
        }

        // Phase 2: payload.
        let mut payload = vec![0u8; payload_len as usize];
        if !payload.is_empty() {
            if let Err(e) = self.do_recv_exact(&mut payload, progress).await {
                self.poison();
                return Err(e);
            }
        }

        Ok(Frame {
            flags,
            payload,
            fds: received_fds,
        })
    }

    async fn do_recv_with_fds(
        &self,
        buf: &mut [u8],
        max_fds: usize,
        progress: &Arc<AtomicBool>,
    ) -> proto::Result<Vec<OwnedFd>> {
        let stream = &self.socket.stream;
        let mut offset = 0usize;
        let mut all_fds: Vec<OwnedFd> = Vec::new();
        let mut first_call = true;

        while offset < buf.len() {
            stream.readable().await?;

            let slice = &mut buf[offset..];
            let want_fds = if first_call { max_fds } else { 0 };

            match stream.try_io(Interest::READABLE, || {
                scm::recvmsg_fds(stream.as_fd(), slice, want_fds)
            }) {
                Ok((0, fds)) => {
                    drop(fds);
                    return Err(ProtoError::PeerClosed);
                }
                Ok((n, mut fds)) => {
                    progress.store(true, Ordering::Relaxed);
                    if first_call {
                        all_fds.append(&mut fds);
                        first_call = false;
                    } else if !fds.is_empty() {
                        // Unexpected FDs on a continuation read means
                        // something is deeply wrong with the stream.
                        tracing::error!(
                            count = fds.len(),
                            "unexpected fds on continuation read — poisoning"
                        );
                        // fds are OwnedFd → dropped and closed here.
                        return Err(ProtoError::UnexpectedFds(fds.len()));
                    }
                    offset += n;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(all_fds)
    }

    async fn do_recv_exact(
        &self,
        buf: &mut [u8],
        progress: &Arc<AtomicBool>,
    ) -> proto::Result<()> {
        let stream = &self.socket.stream;
        let mut offset = 0usize;

        while offset < buf.len() {
            stream.readable().await?;

            let slice = &mut buf[offset..];

            match stream.try_io(Interest::READABLE, || {
                scm::recvmsg_fds(stream.as_fd(), slice, 0)
            }) {
                Ok((0, _)) => return Err(ProtoError::PeerClosed),
                Ok((n, _)) => {
                    progress.store(true, Ordering::Relaxed);
                    offset += n;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }
}

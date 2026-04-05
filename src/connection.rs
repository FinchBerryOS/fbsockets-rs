// SPDX-License-Identifier: MIT
//! Blocking framed connection with stop-and-wait delivery ACK semantics.
//!
//! One logical message maps to exactly one DATA frame. The maximum payload per
//! logical message is 256 KiB. `send()` returns only after the peer application
//! has taken that message via `recv()` and sent an ACK.
//!
//! Backpressure / protocol rules:
//! - exactly one background reader thread reads from the socket per connection
//! - `recv()` never reads directly from the socket; it only drains a single-slot inbox
//! - the inbox may hold at most one logical message at a time
//! - the reader thread never auto-ACKs data frames
//! - `recv()` sends the ACK after removing the message from the inbox
//! - receiving a second DATA frame while the inbox is still occupied is a protocol violation

use std::collections::HashSet;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::pid_guard::PidGuard;
use crate::proto::{self, Config, FrameKind, Header, Message, ProtoError, FD_PRELUDE_LEN, FD_PRELUDE_MARKER, HEADER_LEN, MAX_IO_CHUNK};
use crate::scm;

#[derive(Debug)]
struct SharedSocket {
    stream: UnixStream,
}

impl SharedSocket {
    fn new(stream: UnixStream) -> Arc<Self> {
        Arc::new(Self { stream })
    }
}

#[derive(Debug, Clone, Copy)]
enum TerminalState {
    PeerClosed,
    Poisoned,
    ProtocolViolation,
}

impl TerminalState {
    fn as_error(self) -> ProtoError {
        match self {
            TerminalState::PeerClosed => ProtoError::PeerClosed,
            TerminalState::Poisoned => ProtoError::Poisoned,
            TerminalState::ProtocolViolation => ProtoError::ProtocolViolation(
                "received a new DATA message before the previous message was delivered and acknowledged",
            ),
        }
    }
}

#[derive(Debug)]
struct QueuedMessage {
    message_id: u64,
    msg: Message,
}

#[derive(Debug, Default)]
struct PendingState {
    acked: HashSet<u64>,
    inbox: InboxState,
    terminal: Option<TerminalState>,
}

#[derive(Debug)]
enum InboxState {
    Empty,
    Ready(QueuedMessage),
    Delivering(u64),
}

impl Default for InboxState {
    fn default() -> Self {
        Self::Empty
    }
}

#[derive(Debug)]
struct Inner {

    socket: Arc<SharedSocket>,
    guard: Arc<PidGuard>,
    send_timeout: Option<Duration>,
    recv_timeout: Option<Duration>,
    send_poisoned: AtomicBool,
    recv_poisoned: AtomicBool,
    fork_shutdown_done: AtomicBool,
    shutdown_requested: AtomicBool,
    next_message_id: AtomicU64,
    send_mu: Mutex<()>,
    pending: Mutex<PendingState>,
    state_cv: Condvar,
    reader_handle: Mutex<Option<JoinHandle<()>>>,
}

pub struct Connection {
    inner: Arc<Inner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentials {
    pub pid: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("fd", &self.inner.socket.stream.as_raw_fd())
            .field("owner_pid", &self.inner.guard.owner_pid())
            .field("send_poisoned", &self.inner.send_poisoned.load(Ordering::Relaxed))
            .field("recv_poisoned", &self.inner.recv_poisoned.load(Ordering::Relaxed))
            .finish()
    }
}

impl Connection {
    pub fn from_stream(stream: UnixStream, config: Config) -> proto::Result<Self> {
        let _ = stream.set_nonblocking(true);
        let inner = Arc::new(Inner {
            socket: SharedSocket::new(stream),
            guard: Arc::new(PidGuard::new()),
            send_timeout: config.send_timeout,
            recv_timeout: config.recv_timeout,
            send_poisoned: AtomicBool::new(false),
            recv_poisoned: AtomicBool::new(false),
            fork_shutdown_done: AtomicBool::new(false),
            shutdown_requested: AtomicBool::new(false),
            next_message_id: AtomicU64::new(1),
            send_mu: Mutex::new(()),
            pending: Mutex::new(PendingState::default()),
            state_cv: Condvar::new(),
            reader_handle: Mutex::new(None),
        });

        Self::spawn_reader_thread(&inner)?;
        Ok(Self { inner })
    }

    pub fn from_owned_fd(fd: OwnedFd, config: Config) -> proto::Result<Self> {
        let stream = UnixStream::from(fd);
        Self::from_stream(stream, config)
    }

    /// # Safety
    ///
    /// `raw_fd` must be a valid owned Unix-domain stream socket file descriptor.
    /// After calling this function, ownership is transferred into the returned
    /// `Connection`, and the caller must not use or close `raw_fd` again.
    pub unsafe fn from_raw_fd(raw_fd: RawFd, config: Config) -> proto::Result<Self> {
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        Self::from_owned_fd(fd, config)
    }

    pub fn connect<P: AsRef<std::path::Path>>(path: P, config: Config) -> proto::Result<Self> {
        let stream = UnixStream::connect(path)?;
        Self::from_stream(stream, config)
    }

    fn spawn_reader_thread(inner: &Arc<Inner>) -> proto::Result<()> {
        let thread_inner = Arc::clone(inner);
        let fd = thread_inner.socket.stream.as_raw_fd();
        let handle = thread::Builder::new()
            .name(format!("fbsockets-reader-{fd}"))
            .spawn(move || {
                thread_inner.reader_loop();
            })
            .map_err(ProtoError::from)?;
        *inner.reader_handle.lock().expect("reader_handle mutex poisoned") = Some(handle);
        Ok(())
    }

    #[inline]
    pub fn owner_pid(&self) -> u32 {
        self.inner.guard.owner_pid()
    }

    #[inline]
    pub fn is_send_poisoned(&self) -> bool {
        self.inner.send_poisoned.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_recv_poisoned(&self) -> bool {
        self.inner.recv_poisoned.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn peer_credentials(&self) -> proto::Result<PeerCredentials> {
        self.inner.peer_credentials()
    }

    #[inline]
    pub fn peer_pid(&self) -> proto::Result<Option<u32>> {
        Ok(self.peer_credentials()?.pid)
    }

    #[inline]
    pub fn peer_uid(&self) -> proto::Result<Option<u32>> {
        Ok(self.peer_credentials()?.uid)
    }

    #[inline]
    pub fn peer_gid(&self) -> proto::Result<Option<u32>> {
        Ok(self.peer_credentials()?.gid)
    }

    pub fn send(&self, data: &[u8], fds: &[BorrowedFd<'_>]) -> proto::Result<()> {
        proto::validate_message_shape(data.len(), fds.len())?;
        self.inner.check_send_health()?;
        self.inner.check_recv_health()?;

        let msg_id = self.inner.next_message_id.fetch_add(1, Ordering::Relaxed);
        let deadline = self.inner.send_timeout.map(|d| Instant::now() + d);

        self.inner.send_data_frame(msg_id, data, fds, deadline)?;
        self.inner.wait_for_ack(msg_id, deadline)
    }

    pub fn recv(&self) -> proto::Result<(Vec<u8>, Vec<OwnedFd>)> {
        self.inner.check_recv_health()?;
        let deadline = self.inner.recv_timeout.map(|d| Instant::now() + d);
        let queued = self.inner.take_message_and_ack(deadline)?;
        Ok((queued.msg.data, queued.msg.fds))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.inner.request_shutdown();
        self.inner.join_reader_thread();
    }
}

impl Inner {
    fn reader_loop(self: Arc<Self>) {
        loop {
            if self.shutdown_requested.load(Ordering::Relaxed) {
                return;
            }

            match self.read_one_incoming_frame(None) {
                Ok(FrameOutcome::Ack { ack_id }) => {
                    self.note_ack(ack_id);
                }
                Ok(FrameOutcome::Data { message_id, msg }) => {
                    if let Err(err) = self.try_queue_message(message_id, msg) {
                        if self.shutdown_requested.load(Ordering::Relaxed) {
                            return;
                        }
                        tracing::error!(fd = self.socket.stream.as_raw_fd(), error = %err, "reader thread protocol violation");
                        self.invalidate_due_to_protocol_violation();
                        return;
                    }
                }
                Err(ProtoError::PeerClosed) => {
                    if self.shutdown_requested.load(Ordering::Relaxed) {
                        return;
                    }
                    self.mark_terminal(TerminalState::PeerClosed);
                    return;
                }
                Err(ProtoError::ForkDetected { .. }) => {
                    self.handle_fork_shutdown();
                    self.send_poisoned.store(true, Ordering::Relaxed);
                    self.recv_poisoned.store(true, Ordering::Relaxed);
                    self.mark_terminal(TerminalState::Poisoned);
                    return;
                }
                Err(err) => {
                    if self.shutdown_requested.load(Ordering::Relaxed) {
                        return;
                    }
                    tracing::error!(fd = self.socket.stream.as_raw_fd(), error = %err, "reader thread aborted");
                    self.mark_terminal(TerminalState::Poisoned);
                    return;
                }
            }
        }
    }

    fn send_data_frame(
        &self,
        message_id: u64,
        data: &[u8],
        fds: &[BorrowedFd<'_>],
        deadline: Option<Instant>,
    ) -> proto::Result<()> {
        let _lock = self.send_mu.lock().expect("send mutex poisoned");
        self.check_send_health()?;

        let mut hdr = [0u8; HEADER_LEN];
        proto::encode_header(
            &mut hdr,
            Header {
                kind: FrameKind::Data,
                payload_len: data.len() as u32,
                fd_count: fds.len() as u32,
                message_id,
                ack_id: 0,
            },
        );

        self.write_all(&hdr, deadline)?;
        if !fds.is_empty() {
            let marker = [FD_PRELUDE_MARKER; FD_PRELUDE_LEN];
            self.write_all_with_fds(&marker, fds, deadline)?;
        }
        self.write_all(data, deadline)
    }

    fn send_ack_frame(&self, ack_id: u64, deadline: Option<Instant>) -> proto::Result<()> {
        let _lock = self.send_mu.lock().expect("send mutex poisoned");
        self.check_send_health()?;

        let mut hdr = [0u8; HEADER_LEN];
        proto::encode_header(
            &mut hdr,
            Header {
                kind: FrameKind::Ack,
                payload_len: 0,
                fd_count: 0,
                message_id: 0,
                ack_id,
            },
        );

        self.write_all(&hdr, deadline)
    }

    fn wait_for_ack(&self, msg_id: u64, deadline: Option<Instant>) -> proto::Result<()> {
        self.check_send_health()?;
        self.check_recv_health()?;
        let mut pending = self.pending.lock().expect("pending mutex poisoned");
        loop {
            if pending.acked.remove(&msg_id) {
                return Ok(());
            }
            if let Some(state) = pending.terminal {
                return Err(state.as_error());
            }

            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        self.poison_send();
                        return Err(ProtoError::Timeout(Duration::from_secs(0)));
                    }
                    let wait_for = deadline.saturating_duration_since(now);
                    let (next_pending, wait_res) = self
                        .state_cv
                        .wait_timeout(pending, wait_for)
                        .expect("state condvar poisoned");
                    pending = next_pending;
                    if wait_res.timed_out() {
                        if pending.acked.remove(&msg_id) {
                            return Ok(());
                        }
                        self.poison_send();
                        return Err(ProtoError::Timeout(wait_for));
                    }
                }
                None => {
                    pending = self.state_cv.wait(pending).expect("state condvar poisoned");
                }
            }
        }
    }

    fn take_message_and_ack(&self, deadline: Option<Instant>) -> proto::Result<QueuedMessage> {
        self.check_recv_health()?;

        let msg = {
            let mut pending = self.pending.lock().expect("pending mutex poisoned");
            loop {
                match std::mem::replace(&mut pending.inbox, InboxState::Empty) {
                    InboxState::Ready(msg) => {
                        pending.inbox = InboxState::Delivering(msg.message_id);
                        break msg;
                    }
                    InboxState::Empty => {
                        pending.inbox = InboxState::Empty;
                    }
                    InboxState::Delivering(id) => {
                        pending.inbox = InboxState::Delivering(id);
                    }
                }

                if let Some(state) = pending.terminal {
                    return Err(state.as_error());
                }

                match deadline {
                    Some(deadline) => {
                        let now = Instant::now();
                        if now >= deadline {
                            return Err(ProtoError::Timeout(Duration::from_secs(0)));
                        }
                        let wait_for = deadline.saturating_duration_since(now);
                        let (next_pending, wait_res) = self
                            .state_cv
                            .wait_timeout(pending, wait_for)
                            .expect("state condvar poisoned");
                        pending = next_pending;
                        if wait_res.timed_out() {
                            return Err(ProtoError::Timeout(wait_for));
                        }
                    }
                    None => {
                        pending = self.state_cv.wait(pending).expect("state condvar poisoned");
                    }
                }
            }
        };

        match self.send_ack_frame(msg.message_id, self.send_timeout.map(|d| Instant::now() + d)) {
            Ok(()) => {
                let mut pending = self.pending.lock().expect("pending mutex poisoned");
                match std::mem::replace(&mut pending.inbox, InboxState::Empty) {
                    InboxState::Delivering(id) if id == msg.message_id => {}
                    other => {
                        pending.inbox = other;
                        drop(pending);
                        self.mark_terminal(TerminalState::Poisoned);
                        return Err(ProtoError::Poisoned);
                    }
                }
                self.state_cv.notify_all();
                Ok(msg)
            }
            Err(err) => {
                self.mark_terminal(TerminalState::Poisoned);
                Err(err)
            }
        }
    }

    fn try_queue_message(&self, message_id: u64, msg: Message) -> proto::Result<()> {
        let mut pending = self.pending.lock().expect("pending mutex poisoned");
        match &pending.inbox {
            InboxState::Empty => {
                pending.inbox = InboxState::Ready(QueuedMessage { message_id, msg });
            }
            InboxState::Ready(_) | InboxState::Delivering(_) => {
                return Err(ProtoError::ProtocolViolation(
                    "received a second DATA message before the previous message was acknowledged",
                ));
            }
        }
        self.state_cv.notify_all();
        Ok(())
    }

    fn note_ack(&self, ack_id: u64) {
        let mut pending = self.pending.lock().expect("pending mutex poisoned");
        pending.acked.insert(ack_id);
        self.state_cv.notify_all();
    }

    fn mark_terminal(&self, terminal: TerminalState) {
        match terminal {
            TerminalState::PeerClosed => {}
            TerminalState::Poisoned | TerminalState::ProtocolViolation => {
                self.send_poisoned.store(true, Ordering::Relaxed);
                self.recv_poisoned.store(true, Ordering::Relaxed);
            }
        }

        let mut pending = self.pending.lock().expect("pending mutex poisoned");
        if pending.terminal.is_none() {
            pending.terminal = Some(terminal);
        }
        if !matches!(pending.inbox, InboxState::Empty) && !matches!(terminal, TerminalState::PeerClosed) {
            // keep the in-flight inbox state for diagnostics; no further delivery will occur once terminal is set
        }
        self.state_cv.notify_all();
    }

    fn request_shutdown(&self) {
        if self
            .shutdown_requested
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let fd = self.socket.stream.as_raw_fd();
            unsafe {
                libc::shutdown(fd, libc::SHUT_RDWR);
            }
            self.state_cv.notify_all();
        }
    }

    fn join_reader_thread(&self) {
        let current_id = thread::current().id();
        let handle = {
            let mut slot = self.reader_handle.lock().expect("reader_handle mutex poisoned");
            if let Some(handle) = slot.take() {
                if handle.thread().id() == current_id {
                    *slot = Some(handle);
                    return;
                }
                Some(handle)
            } else {
                None
            }
        };

        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    fn invalidate_due_to_protocol_violation(&self) {
        self.send_poisoned.store(true, Ordering::Relaxed);
        self.recv_poisoned.store(true, Ordering::Relaxed);
        self.request_shutdown();
        self.mark_terminal(TerminalState::ProtocolViolation);
    }

    fn read_one_incoming_frame(&self, deadline: Option<Instant>) -> proto::Result<FrameOutcome> {
        self.check_recv_health()?;

        let mut hdr_buf = [0u8; HEADER_LEN];
        self.read_exact(&mut hdr_buf, deadline)?;
        let header = proto::decode_header(&hdr_buf)?;

        match header.kind {
            FrameKind::Ack => Ok(FrameOutcome::Ack { ack_id: header.ack_id }),
            FrameKind::Data => {
                let fds = if header.fd_count > 0 {
                    self.read_fd_prelude(header.fd_count as usize, deadline)?
                } else {
                    Vec::new()
                };

                if fds.len() != header.fd_count as usize {
                    self.poison_recv();
                    return Err(ProtoError::FdCountMismatch {
                        expected: header.fd_count as usize,
                        actual: fds.len(),
                    });
                }

                let mut payload = vec![0u8; header.payload_len as usize];
                self.read_exact(&mut payload, deadline)?;

                Ok(FrameOutcome::Data {
                    message_id: header.message_id,
                    msg: Message { data: payload, fds },
                })
            }
        }
    }

    fn read_fd_prelude(&self, expected_fds: usize, deadline: Option<Instant>) -> proto::Result<Vec<OwnedFd>> {
        let mut marker = [0u8; FD_PRELUDE_LEN];
        let mut fds = Vec::new();
        let mut got_marker = 0usize;

        while got_marker < FD_PRELUDE_LEN {
            wait_for_fd(self.socket.stream.as_raw_fd(), false, deadline)?;
            match scm::recvmsg_fds(self.socket.stream.as_fd(), &mut marker[got_marker..], expected_fds) {
                Ok((0, _)) => return Err(ProtoError::PeerClosed),
                Ok((n, mut got_fds)) => {
                    got_marker += n;
                    fds.append(&mut got_fds);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        if marker[0] != FD_PRELUDE_MARKER {
            self.poison_recv();
            return Err(ProtoError::InvalidFdPrelude(marker[0]));
        }

        Ok(fds)
    }

    fn write_all_with_fds(&self, data: &[u8], fds: &[BorrowedFd<'_>], deadline: Option<Instant>) -> proto::Result<()> {
        if data.is_empty() {
            if fds.is_empty() {
                return Ok(());
            }

            loop {
                wait_for_fd(self.socket.stream.as_raw_fd(), true, deadline)?;
                match scm::sendmsg_fds(self.socket.stream.as_fd(), &[], fds) {
                    Ok(0) => return Err(ProtoError::PeerClosed),
                    Ok(_) => return Ok(()),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }

        let mut sent = 0usize;
        let mut first = true;
        while sent < data.len() {
            let end = (sent + MAX_IO_CHUNK).min(data.len());
            let slice = &data[sent..end];
            wait_for_fd(self.socket.stream.as_raw_fd(), true, deadline)?;
            let fds_now = if first { fds } else { &[] };
            match scm::sendmsg_fds(self.socket.stream.as_fd(), slice, fds_now) {
                Ok(0) => return Err(ProtoError::PeerClosed),
                Ok(n) => {
                    sent += n;
                    first = false;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn write_all(&self, data: &[u8], deadline: Option<Instant>) -> proto::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let mut sent = 0usize;
        while sent < data.len() {
            let end = (sent + MAX_IO_CHUNK).min(data.len());
            let slice = &data[sent..end];
            wait_for_fd(self.socket.stream.as_raw_fd(), true, deadline)?;
            match scm::sendmsg_fds(self.socket.stream.as_fd(), slice, &[]) {
                Ok(0) => return Err(ProtoError::PeerClosed),
                Ok(n) => {
                    sent += n;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn read_exact(&self, buf: &mut [u8], deadline: Option<Instant>) -> proto::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let mut read = 0usize;
        while read < buf.len() {
            wait_for_fd(self.socket.stream.as_raw_fd(), false, deadline)?;
            match scm::recvmsg_fds(self.socket.stream.as_fd(), &mut buf[read..], 0) {
                Ok((0, _)) => return Err(ProtoError::PeerClosed),
                Ok((n, fds)) => {
                    if !fds.is_empty() {
                        self.poison_recv();
                        return Err(ProtoError::UnexpectedFds(fds.len()));
                    }
                    read += n;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn check_send_health(&self) -> proto::Result<()> {
        if self.send_poisoned.load(Ordering::Relaxed) {
            return Err(ProtoError::Poisoned);
        }
        self.guard.check().map_err(|e| {
            self.handle_fork_shutdown();
            self.send_poisoned.store(true, Ordering::Relaxed);
            self.recv_poisoned.store(true, Ordering::Relaxed);
            self.mark_terminal(TerminalState::Poisoned);
            e
        })
    }

    fn check_recv_health(&self) -> proto::Result<()> {
        if self.recv_poisoned.load(Ordering::Relaxed) {
            return Err(ProtoError::Poisoned);
        }
        self.guard.check().map_err(|e| {
            self.handle_fork_shutdown();
            self.send_poisoned.store(true, Ordering::Relaxed);
            self.recv_poisoned.store(true, Ordering::Relaxed);
            self.mark_terminal(TerminalState::Poisoned);
            e
        })
    }

    fn handle_fork_shutdown(&self) {
        if self
            .fork_shutdown_done
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let fd = self.socket.stream.as_raw_fd();
            unsafe {
                libc::shutdown(fd, libc::SHUT_RDWR);
            }
            tracing::warn!(fd, owner_pid = self.guard.owner_pid(), "fork detected; socket shut down in child process");
            self.state_cv.notify_all();
        }
    }

    fn poison_send(&self) {
        if !self.send_poisoned.swap(true, Ordering::Relaxed) {
            tracing::error!(fd = self.socket.stream.as_raw_fd(), "send side poisoned");
            self.state_cv.notify_all();
        }
    }

    fn poison_recv(&self) {
        if !self.recv_poisoned.swap(true, Ordering::Relaxed) {
            tracing::error!(fd = self.socket.stream.as_raw_fd(), "recv side poisoned");
            self.state_cv.notify_all();
        }
    }

    fn peer_credentials(&self) -> proto::Result<PeerCredentials> {
        peer_credentials_for_stream(&self.socket.stream)
    }
}

#[derive(Debug)]
enum FrameOutcome {
    Ack { ack_id: u64 },
    Data { message_id: u64, msg: Message },
}


#[cfg(target_os = "linux")]
fn peer_credentials_for_stream(stream: &UnixStream) -> proto::Result<PeerCredentials> {
    let fd = stream.as_raw_fd();
    let mut cred = libc::ucred { pid: 0, uid: 0, gid: 0 };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(ProtoError::from(std::io::Error::last_os_error()));
    }
    if len as usize != std::mem::size_of::<libc::ucred>() {
        return Err(ProtoError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unexpected SO_PEERCRED size: {len}"),
        )));
    }
    Ok(PeerCredentials {
        pid: (cred.pid >= 0).then_some(cred.pid as u32),
        uid: Some(cred.uid as u32),
        gid: Some(cred.gid as u32),
    })
}

#[cfg(target_os = "macos")]
fn peer_credentials_for_stream(stream: &UnixStream) -> proto::Result<PeerCredentials> {
    let fd = stream.as_raw_fd();
    let (uid, gid) = getpeereid_for_fd(fd)?;

    let mut pid: libc::pid_t = 0;
    let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            (&mut pid as *mut libc::pid_t).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(ProtoError::from(std::io::Error::last_os_error()));
    }
    if len as usize != std::mem::size_of::<libc::pid_t>() {
        return Err(ProtoError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unexpected LOCAL_PEERPID size: {len}"),
        )));
    }
    Ok(PeerCredentials {
        pid: (pid >= 0).then_some(pid as u32),
        uid: Some(uid),
        gid: Some(gid),
    })
}

#[cfg(target_os = "freebsd")]
fn peer_credentials_for_stream(stream: &UnixStream) -> proto::Result<PeerCredentials> {
    let fd = stream.as_raw_fd();
    let (uid, gid) = getpeereid_for_fd(fd)?;

    let mut xucred = unsafe { std::mem::zeroed::<libc::xucred>() };
    let mut len = std::mem::size_of::<libc::xucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERCRED,
            (&mut xucred as *mut libc::xucred).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(ProtoError::from(std::io::Error::last_os_error()));
    }
    if len as usize != std::mem::size_of::<libc::xucred>() {
        return Err(ProtoError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unexpected LOCAL_PEERCRED size: {len}"),
        )));
    }

    #[allow(clippy::unnecessary_cast)]
    let pid = (xucred.cr_pid >= 0).then_some(xucred.cr_pid as u32);

    Ok(PeerCredentials { pid, uid: Some(uid), gid: Some(gid) })
}

#[cfg(any(target_os = "macos", target_os = "freebsd"))]
fn getpeereid_for_fd(fd: libc::c_int) -> proto::Result<(u32, u32)> {
    let mut euid: libc::uid_t = 0;
    let mut egid: libc::gid_t = 0;
    let rc = unsafe { libc::getpeereid(fd, &mut euid, &mut egid) };
    if rc != 0 {
        return Err(ProtoError::from(std::io::Error::last_os_error()));
    }
    Ok((euid as u32, egid as u32))
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
fn peer_credentials_for_stream(_stream: &UnixStream) -> proto::Result<PeerCredentials> {
    Err(ProtoError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "peer credentials are unsupported on this platform",
    )))
}

fn wait_for_fd(raw_fd: i32, writable: bool, deadline: Option<Instant>) -> proto::Result<()> {
    loop {
        let timeout = deadline.map(|d| d.saturating_duration_since(Instant::now()));
        let mut pfd = libc::pollfd {
            fd: raw_fd,
            events: if writable { libc::POLLOUT } else { libc::POLLIN },
            revents: 0,
        };

        let timeout_ms = match timeout {
            Some(d) => d.as_millis().min(i32::MAX as u128) as i32,
            None => -1,
        };

        let rc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if rc > 0 {
            if (pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0 {
                return Err(ProtoError::PeerClosed);
            }
            return Ok(());
        }
        if rc == 0 {
            return Err(ProtoError::Timeout(timeout.unwrap_or_default()));
        }

        let e = std::io::Error::last_os_error();
        if e.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(e.into());
    }
}

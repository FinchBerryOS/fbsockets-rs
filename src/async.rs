// SPDX-License-Identifier: MIT
//! Optional Tokio-based async API.

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::Interest;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::pid_guard::PidGuard;
use crate::proto::{self, Config, HEADER_LEN, MAX_FDS, Message, ProtoError};
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

#[derive(Debug, Default)]
struct IoProgress {
    transferred_any: AtomicBool,
}

impl IoProgress {
    #[inline]
    fn mark_progress(&self, n: usize) {
        if n > 0 {
            self.transferred_any.store(true, Ordering::Relaxed);
        }
    }

    #[inline]
    fn transferred_any(&self) -> bool {
        self.transferred_any.load(Ordering::Relaxed)
    }
}

pub struct Connection {
    socket: Arc<SharedSocket>,
    guard: Arc<PidGuard>,
    send_timeout: Option<Duration>,
    recv_timeout: Option<Duration>,
    send_poisoned: AtomicBool,
    recv_poisoned: AtomicBool,
    fork_shutdown_done: AtomicBool,
    send_mu: Mutex<()>,
    recv_mu: Mutex<()>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncConnection")
            .field("fd", &self.socket.stream.as_raw_fd())
            .field("owner_pid", &self.guard.owner_pid())
            .field("send_poisoned", &self.send_poisoned.load(Ordering::Relaxed))
            .field("recv_poisoned", &self.recv_poisoned.load(Ordering::Relaxed))
            .finish()
    }
}

impl Connection {
    pub fn from_stream(stream: UnixStream, config: Config) -> Self {
        Self {
            socket: SharedSocket::new(stream),
            guard: Arc::new(PidGuard::new()),
            send_timeout: config.send_timeout,
            recv_timeout: config.recv_timeout,
            send_poisoned: AtomicBool::new(false),
            recv_poisoned: AtomicBool::new(false),
            fork_shutdown_done: AtomicBool::new(false),
            send_mu: Mutex::new(()),
            recv_mu: Mutex::new(()),
        }
    }

    pub async fn connect<P: AsRef<Path>>(path: P, config: Config) -> proto::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self::from_stream(stream, config))
    }

    pub async fn send(&self, data: &[u8], fds: &[BorrowedFd<'_>]) -> proto::Result<()> {
        proto::validate_message_shape(data.len(), fds.len())?;
        self.check_send_health()?;

        let progress = Arc::new(IoProgress::default());
        let fut = self.send_inner(data, fds, Arc::clone(&progress));

        match self.send_timeout {
            Some(dur) => match tokio::time::timeout(dur, fut).await {
                Ok(result) => result,
                Err(_) => {
                    if progress.transferred_any() {
                        self.poison_send();
                    }
                    Err(ProtoError::Timeout(dur))
                }
            },
            None => fut.await,
        }
    }

    pub async fn recv(&self) -> proto::Result<(Vec<u8>, Vec<OwnedFd>)> {
        self.check_recv_health()?;

        let progress = Arc::new(IoProgress::default());
        let fut = self.recv_inner(Arc::clone(&progress));

        match self.recv_timeout {
            Some(dur) => match tokio::time::timeout(dur, fut).await {
                Ok(result) => result,
                Err(_) => {
                    if progress.transferred_any() {
                        self.poison_recv();
                    }
                    Err(ProtoError::Timeout(dur))
                }
            },
            None => fut.await,
        }
    }

    fn check_send_health(&self) -> proto::Result<()> {
        if self.send_poisoned.load(Ordering::Relaxed) {
            return Err(ProtoError::Poisoned);
        }
        self.guard.check().map_err(|e| {
            self.handle_fork_shutdown();
            self.send_poisoned.store(true, Ordering::Relaxed);
            self.recv_poisoned.store(true, Ordering::Relaxed);
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
        }
    }

    fn poison_send(&self) {
        if !self.send_poisoned.swap(true, Ordering::Relaxed) {
            tracing::error!(fd = self.socket.stream.as_raw_fd(), "send side poisoned");
        }
    }

    fn poison_recv(&self) {
        if !self.recv_poisoned.swap(true, Ordering::Relaxed) {
            tracing::error!(fd = self.socket.stream.as_raw_fd(), "recv side poisoned");
        }
    }

    async fn send_inner(&self, data: &[u8], fds: &[BorrowedFd<'_>], progress: Arc<IoProgress>) -> proto::Result<()> {
        let _lock = self.send_mu.lock().await;
        self.check_send_health()?;

        let mut hdr = [0u8; HEADER_LEN];
        proto::encode_header(&mut hdr, data.len() as u32, fds.len() as u32);

        if let Err(e) = self.send_raw_with_optional_fds(&hdr, fds, &progress).await {
            self.poison_send();
            return Err(e);
        }
        if let Err(e) = self.send_raw(data, &progress).await {
            self.poison_send();
            return Err(e);
        }

        Ok(())
    }

    async fn recv_inner(&self, progress: Arc<IoProgress>) -> proto::Result<(Vec<u8>, Vec<OwnedFd>)> {
        let _lock = self.recv_mu.lock().await;
        self.check_recv_health()?;

        let mut hdr_buf = [0u8; HEADER_LEN];
        let header_fds = match self.recv_header_with_fds(&mut hdr_buf, &progress).await {
            Ok(fds) => fds,
            Err(e) => {
                if progress.transferred_any() {
                    self.poison_recv();
                }
                return Err(e);
            }
        };

        let (payload_len, fd_count) = match proto::decode_header(&hdr_buf) {
            Ok(v) => v,
            Err(e) => {
                self.poison_recv();
                return Err(e);
            }
        };

        if header_fds.len() != fd_count as usize {
            self.poison_recv();
            return Err(ProtoError::FdCountMismatch {
                expected: fd_count as usize,
                actual: header_fds.len(),
            });
        }

        let mut payload = vec![0u8; payload_len as usize];
        if let Err(e) = self.recv_payload_exact(&mut payload, &progress).await {
            if progress.transferred_any() {
                self.poison_recv();
            }
            return Err(e);
        }

        let msg = Message { data: payload, fds: header_fds };
        Ok((msg.data, msg.fds))
    }

    async fn send_raw_with_optional_fds(
        &self,
        data: &[u8],
        fds: &[BorrowedFd<'_>],
        progress: &IoProgress,
    ) -> proto::Result<()> {
        let stream = &self.socket.stream;
        let mut offset = 0usize;
        let mut first_send = true;

        while offset < data.len() {
            stream.writable().await?;
            let slice = &data[offset..];
            let fds_now = if first_send { fds } else { &[] };

            match stream.try_io(Interest::WRITABLE, || scm::sendmsg_fds(stream.as_fd(), slice, fds_now)) {
                Ok(0) => return Err(ProtoError::PeerClosed),
                Ok(n) => {
                    progress.mark_progress(n);
                    offset += n;
                    first_send = false;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    async fn send_raw(&self, data: &[u8], progress: &IoProgress) -> proto::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let stream = &self.socket.stream;
        let mut offset = 0usize;

        while offset < data.len() {
            stream.writable().await?;
            let slice = &data[offset..];
            match stream.try_io(Interest::WRITABLE, || scm::sendmsg_fds(stream.as_fd(), slice, &[])) {
                Ok(0) => return Err(ProtoError::PeerClosed),
                Ok(n) => {
                    progress.mark_progress(n);
                    offset += n;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    async fn recv_header_with_fds(&self, buf: &mut [u8; HEADER_LEN], progress: &IoProgress) -> proto::Result<Vec<OwnedFd>> {
        let stream = &self.socket.stream;
        let mut offset = 0usize;
        let mut all_fds = Vec::new();
        let mut seen_any_fds = false;

        while offset < buf.len() {
            stream.readable().await?;
            let slice = &mut buf[offset..];
            match stream.try_io(Interest::READABLE, || scm::recvmsg_fds(stream.as_fd(), slice, MAX_FDS as usize)) {
                Ok((0, _)) => return Err(ProtoError::PeerClosed),
                Ok((n, mut fds)) => {
                    if seen_any_fds && !fds.is_empty() {
                        return Err(ProtoError::UnexpectedFds(fds.len()));
                    }
                    if !fds.is_empty() {
                        seen_any_fds = true;
                    }
                    progress.mark_progress(n);
                    offset += n;
                    all_fds.append(&mut fds);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(all_fds)
    }

    async fn recv_payload_exact(&self, buf: &mut [u8], progress: &IoProgress) -> proto::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let stream = &self.socket.stream;
        let mut offset = 0usize;

        while offset < buf.len() {
            stream.readable().await?;
            let slice = &mut buf[offset..];
            match stream.try_io(Interest::READABLE, || scm::recvmsg_fds(stream.as_fd(), slice, 0)) {
                Ok((0, _)) => return Err(ProtoError::PeerClosed),
                Ok((n, fds)) => {
                    if !fds.is_empty() {
                        return Err(ProtoError::UnexpectedFds(fds.len()));
                    }
                    progress.mark_progress(n);
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

#[derive(Debug)]
pub struct Listener {
    inner: UnixListener,
}

impl Listener {
    pub fn bind<P: AsRef<Path>>(path: P) -> proto::Result<Self> {
        let inner = UnixListener::bind(path)?;
        Ok(Self { inner })
    }

    pub fn from_listener(inner: UnixListener) -> Self {
        Self { inner }
    }

    pub async fn accept(&self, config: Config) -> proto::Result<Connection> {
        let (stream, _addr) = self.inner.accept().await?;
        Ok(Connection::from_stream(stream, config))
    }

    pub fn inner(&self) -> &UnixListener {
        &self.inner
    }
}

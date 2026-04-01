use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

use fbsockets::proto::{self, Config, MAX_FDS, MAX_PAYLOAD};
use fbsockets::Connection;

fn pair() -> (Connection, Connection) {
    let (a, b) = UnixStream::pair().unwrap();
    (
        Connection::from_stream(a, Config::unconstrained()),
        Connection::from_stream(b, Config::unconstrained()),
    )
}

#[test]
fn roundtrip_simple() {
    let (a, b) = pair();
    a.send(b"hello", &[]).unwrap();
    let (data, fds) = b.recv().unwrap();
    assert_eq!(data, b"hello");
    assert!(fds.is_empty());
}

#[test]
fn reject_oversized_payload() {
    let (a, _b) = pair();
    let too_big = vec![0u8; MAX_PAYLOAD as usize + 1];
    let err = a.send(&too_big, &[]).unwrap_err();
    assert!(matches!(err, proto::ProtoError::PayloadTooLarge(_)));
}

#[test]
fn reject_too_many_fds() {
    let (a, _b) = pair();
    let mut pipes: Vec<(OwnedFd, OwnedFd)> = Vec::new();

    for _ in 0..(MAX_FDS as usize + 1) {
        let mut pipe_fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        let r = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
        let w = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };
        pipes.push((r, w));
    }

    let borrowed: Vec<_> = pipes.iter().map(|(_, w)| w.as_fd()).collect();

    let err = a.send(b"x", &borrowed).unwrap_err();
    assert!(matches!(err, proto::ProtoError::TooManyFds(_)));
}

#[test]
fn fd_passing_pipe() {
    let (a, b) = pair();

    let mut pipe_fds = [0i32; 2];
    assert_eq!(unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    let pipe_r = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
    let pipe_w = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };

    a.send(b"fd", &[pipe_w.as_fd()]).unwrap();
    let (data, fds) = b.recv().unwrap();

    assert_eq!(data, b"fd");
    assert_eq!(fds.len(), 1);

    let msg = b"via-fd";
    let written = unsafe { libc::write(fds[0].as_raw_fd(), msg.as_ptr().cast(), msg.len()) };
    assert_eq!(written, msg.len() as isize);

    let mut buf = [0u8; 16];
    let n = unsafe { libc::read(pipe_r.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
    assert!(n > 0);
    assert_eq!(&buf[..n as usize], b"via-fd");
}

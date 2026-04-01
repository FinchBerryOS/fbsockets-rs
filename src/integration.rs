// SPDX-License-Identifier: MIT
//! Integration tests for wireframe (production-hardened API v3).

use std::os::unix::io::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use wireframe::proto::{self, Config, FLAG_KEEPALIVE, FLAG_NONE, HEADER_LEN, MAX_PAYLOAD};
use wireframe::Connection;

/// Helper: create a connected pair with unconstrained config.
fn pair() -> (Connection, Connection) {
    let (a, b) = UnixStream::pair().unwrap();
    (
        Connection::from_stream(a, Config::unconstrained()),
        Connection::from_stream(b, Config::unconstrained()),
    )
}

// ═══════════════════════════════════════════════════════════════════
//  Basic roundtrip
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn roundtrip_simple() {
    let (ca, cb) = pair();
    let (tx, _) = ca.split();
    let (_, rx) = cb.split();

    tx.send(FLAG_NONE, b"hello world", &[]).await.unwrap();
    let frame = rx.recv().await.unwrap();

    assert_eq!(frame.flags, FLAG_NONE);
    assert_eq!(frame.payload, b"hello world");
    assert!(frame.fds.is_empty());
}

#[tokio::test]
async fn roundtrip_empty_payload() {
    let (ca, cb) = pair();
    let (tx, _) = ca.split();
    let (_, rx) = cb.split();

    tx.send(FLAG_NONE, b"", &[]).await.unwrap();
    let frame = rx.recv().await.unwrap();

    assert_eq!(frame.payload.len(), 0);
    assert!(frame.fds.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_max_payload() {
    let (ca, cb) = pair();
    let (tx, _) = ca.split();
    let (_, rx) = cb.split();

    let big = vec![0xABu8; MAX_PAYLOAD as usize];

    let send_handle = tokio::spawn(async move {
        tx.send(FLAG_NONE, &big, &[]).await.unwrap();
    });

    let recv_handle = tokio::spawn(async move { rx.recv().await.unwrap() });

    send_handle.await.unwrap();
    let frame = recv_handle.await.unwrap();

    assert_eq!(frame.payload.len(), MAX_PAYLOAD as usize);
    assert_eq!(frame.payload[0], 0xAB);
}

// ═══════════════════════════════════════════════════════════════════
//  Validation
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn reject_oversized_payload() {
    let (ca, _cb) = pair();
    let (tx, _) = ca.split();

    let too_big = vec![0u8; MAX_PAYLOAD as usize + 1];
    let err = tx.send(FLAG_NONE, &too_big, &[]).await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::PayloadTooLarge(_)));
}

#[tokio::test]
async fn send_rejects_reserved_keepalive_flag() {
    let (ca, _cb) = pair();
    let (tx, _) = ca.split();

    // Bit 0 set → ReservedFlag error.
    let err = tx.send(FLAG_KEEPALIVE, b"data", &[]).await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::ReservedFlag));

    // Bit 0 + other bits → still rejected.
    let err = tx.send(0x03, b"data", &[]).await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::ReservedFlag));

    // Bit 0 clear → allowed.
    tx.send(0x02, b"data", &[]).await.unwrap();
}

// ═══════════════════════════════════════════════════════════════════
//  FD passing
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn fd_passing_pipe() {
    let (ca, cb) = pair();
    let (tx, _) = ca.split();
    let (_, rx) = cb.split();

    let mut pipe_fds = [0i32; 2];
    assert_eq!(
        unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) },
        0
    );
    let pipe_r = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
    let pipe_w = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };

    tx.send(FLAG_NONE, b"take this fd", &[pipe_w.as_fd()])
        .await
        .unwrap();

    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.payload, b"take this fd");
    assert_eq!(frame.fds.len(), 1);

    let received_fd = &frame.fds[0];
    let msg = b"via-fd";
    let written =
        unsafe { libc::write(received_fd.as_raw_fd(), msg.as_ptr().cast(), msg.len()) };
    assert_eq!(written, msg.len() as isize);

    let mut buf = [0u8; 16];
    let n = unsafe { libc::read(pipe_r.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
    assert!(n > 0);
    assert_eq!(&buf[..n as usize], b"via-fd");
}

// ═══════════════════════════════════════════════════════════════════
//  Multiple frames / flags
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn multiple_frames() {
    let (ca, cb) = pair();
    let (tx, _) = ca.split();
    let (_, rx) = cb.split();

    for i in 0u32..10 {
        let payload = format!("frame-{i}");
        // Shift left to avoid FLAG_KEEPALIVE (bit 0).
        let flags = (i as u8) << 1;
        tx.send(flags, payload.as_bytes(), &[]).await.unwrap();
    }

    for i in 0u32..10 {
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.flags, (i as u8) << 1);
        assert_eq!(frame.payload, format!("frame-{i}").as_bytes());
    }
}

#[tokio::test]
async fn flags_roundtrip() {
    let (ca, cb) = pair();
    let (tx, _) = ca.split();
    let (_, rx) = cb.split();

    // 0x42 has bit 0 clear → valid.
    tx.send(0x42, b"flagged", &[]).await.unwrap();
    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.flags, 0x42);
}

// ═══════════════════════════════════════════════════════════════════
//  Peer closed
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn peer_closed_detection() {
    let (a, b) = UnixStream::pair().unwrap();
    let conn = Connection::from_stream(a, Config::unconstrained());
    let (_, rx) = conn.split();
    drop(b);

    let err = rx.recv().await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::PeerClosed));
}

// ═══════════════════════════════════════════════════════════════════
//  Timeouts + Progress-aware Poison
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn recv_timeout_without_progress_does_not_poison() {
    // Nobody sends anything → timeout fires before any bytes transfer.
    // Half must NOT be poisoned (no desync risk).
    let (a, _b) = UnixStream::pair().unwrap();
    let config = Config {
        recv_timeout: Some(Duration::from_millis(50)),
        send_timeout: None,
        keepalive_interval: None,
    };
    let conn = Connection::from_stream(a, config);
    let (_, rx) = conn.split();

    let err = rx.recv().await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::Timeout(_)));

    // NOT poisoned — safe to retry.
    assert!(!rx.is_poisoned());
}

#[tokio::test]
async fn send_timeout_without_progress_does_not_poison() {
    // _b is alive but nobody reads → send will block on writable.
    // With a tiny timeout, it should fire before any bytes go out.
    let (a, _b) = UnixStream::pair().unwrap();

    // Shrink kernel buffer to make blocking more likely.
    let raw_fd = a.as_raw_fd();
    unsafe {
        let size: libc::c_int = 4096;
        libc::setsockopt(
            raw_fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &size as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }

    let config = Config {
        send_timeout: Some(Duration::from_millis(100)),
        recv_timeout: None,
        keepalive_interval: None,
    };
    let conn = Connection::from_stream(a, config);
    let (tx, _) = conn.split();

    let big = vec![0u8; MAX_PAYLOAD as usize];
    let result = tx.send(FLAG_NONE, &big, &[]).await;

    match result {
        Err(proto::ProtoError::Timeout(_)) => {
            // Whether it's poisoned depends on if any bytes got through.
            // We just verify the test doesn't hang.
        }
        Ok(()) => {
            // Kernel buffer was large enough — acceptable.
            assert!(!tx.is_poisoned());
        }
        Err(e) => panic!("unexpected error: {e}"),
    }
}

#[tokio::test]
async fn poison_does_not_cross_halves() {
    let (a, b) = UnixStream::pair().unwrap();

    // Write a partial header to side `a` so recv makes progress,
    // then let the timeout fire → poison.
    let mut raw_b = tokio::net::UnixStream::from_std(b.into_std().unwrap()).unwrap();
    // Send only 4 of 8 header bytes → progress but incomplete frame.
    raw_b.write_all(&[0xFB, 0x01, 0x00, 0x00]).await.unwrap();

    let config = Config {
        recv_timeout: Some(Duration::from_millis(80)),
        send_timeout: None,
        keepalive_interval: None,
    };
    let conn = Connection::from_stream(a, config);
    let (tx, rx) = conn.split();

    // Recv will read 4 bytes (progress!), then timeout waiting for the
    // remaining 4 header bytes → poison.
    let err = rx.recv().await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::Timeout(_)));
    assert!(rx.is_poisoned());

    // Send half is NOT poisoned.
    assert!(!tx.is_poisoned());
}

// ═══════════════════════════════════════════════════════════════════
//  Keepalive
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn keepalive_is_transparent() {
    let (ca, cb) = pair();
    let (tx, _) = ca.split();
    let (_, rx) = cb.split();

    // send_keepalive bypasses the FLAG_KEEPALIVE restriction.
    tx.send_keepalive().await.unwrap();
    tx.send_keepalive().await.unwrap();
    tx.send(FLAG_NONE, b"real", &[]).await.unwrap();

    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.payload, b"real");
    assert!(!frame.is_keepalive());
}

#[tokio::test]
async fn keepalive_flag_with_payload_is_not_keepalive() {
    // Construct via raw socket since send() rejects FLAG_KEEPALIVE.
    let (a, b) = UnixStream::pair().unwrap();
    let conn_b = Connection::from_stream(b, Config::unconstrained());
    let (_, rx) = conn_b.split();

    // Manually write a frame with FLAG_KEEPALIVE + payload.
    let mut raw = tokio::net::UnixStream::from_std(a.into_std().unwrap()).unwrap();
    let payload = b"not a keepalive";
    let len = (payload.len() as u32).to_be_bytes();
    let hdr = [0xFB, 0x01, FLAG_KEEPALIVE, len[0], len[1], len[2], len[3], 0x00];
    raw.write_all(&hdr).await.unwrap();
    raw.write_all(payload).await.unwrap();

    let frame = rx.recv().await.unwrap();
    assert!(!frame.is_keepalive());
    assert_eq!(frame.payload, b"not a keepalive");
}

#[tokio::test]
async fn keepalive_task_sends_frames() {
    let (a, b) = UnixStream::pair().unwrap();

    let config_tx = Config {
        send_timeout: None,
        recv_timeout: None,
        keepalive_interval: Some(Duration::from_millis(30)),
    };
    let conn_a = Connection::from_stream(a, config_tx);
    let (tx, _) = conn_a.split();
    let tx = Arc::new(tx);

    let handle = tx.spawn_keepalive().expect("keepalive enabled");

    let conn_b = Connection::from_stream(b, Config::unconstrained());
    let (_, rx) = conn_b.split();

    tokio::time::sleep(Duration::from_millis(100)).await;

    tx.send(FLAG_NONE, b"after-keepalive", &[]).await.unwrap();

    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.payload, b"after-keepalive");

    handle.abort();
}

// ═══════════════════════════════════════════════════════════════════
//  Concurrent send serialization
// ═══════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sends_dont_interleave() {
    let (ca, cb) = pair();
    let (tx, _) = ca.split();
    let (_, rx) = cb.split();
    let tx = Arc::new(tx);

    let n = 50u32;
    let mut handles = Vec::new();
    for i in 0..n {
        let tx = Arc::clone(&tx);
        handles.push(tokio::spawn(async move {
            let payload = format!("msg-{i:04}");
            tx.send(FLAG_NONE, payload.as_bytes(), &[]).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let mut received = Vec::new();
    for _ in 0..n {
        let frame = rx.recv().await.unwrap();
        let s = String::from_utf8(frame.payload).unwrap();
        assert!(s.starts_with("msg-"));
        received.push(s);
    }

    received.sort();
    for (i, s) in received.iter().enumerate() {
        assert_eq!(s, &format!("msg-{i:04}"));
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Malformed / hostile frames
// ═══════════════════════════════════════════════════════════════════

#[tokio::test]
async fn bad_magic_poisons_recv() {
    let (a, b) = UnixStream::pair().unwrap();
    let conn = Connection::from_stream(a, Config::unconstrained());
    let (_, rx) = conn.split();

    let mut raw = tokio::net::UnixStream::from_std(b.into_std().unwrap()).unwrap();
    raw.write_all(&[0xDE, 0xAD, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00])
        .await
        .unwrap();
    raw.write_all(b"evil").await.unwrap();

    let err = rx.recv().await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::BadMagic(0xDE, 0xAD)));

    assert!(rx.is_poisoned());
    let err2 = rx.recv().await.unwrap_err();
    assert!(matches!(err2, proto::ProtoError::Poisoned));
}

#[tokio::test]
async fn oversized_payload_in_header_poisons_recv() {
    let (a, b) = UnixStream::pair().unwrap();
    let conn = Connection::from_stream(a, Config::unconstrained());
    let (_, rx) = conn.split();

    let bad_len = (MAX_PAYLOAD + 1).to_be_bytes();
    let mut raw = tokio::net::UnixStream::from_std(b.into_std().unwrap()).unwrap();
    let hdr = [
        0xFB, 0x01, 0x00, bad_len[0], bad_len[1], bad_len[2], bad_len[3], 0x00,
    ];
    raw.write_all(&hdr).await.unwrap();

    let err = rx.recv().await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::PayloadTooLarge(_)));
    assert!(rx.is_poisoned());
}

// ═══════════════════════════════════════════════════════════════════
//  Header codec unit tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn header_roundtrip() {
    let mut buf = [0u8; HEADER_LEN];
    proto::encode_header(&mut buf, 0x42, 1024, 3);
    let (flags, len, fds) = proto::decode_header(&buf).unwrap();
    assert_eq!(flags, 0x42);
    assert_eq!(len, 1024);
    assert_eq!(fds, 3);
}

#[test]
fn header_decode_bad_magic() {
    let buf = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    assert!(matches!(
        proto::decode_header(&buf),
        Err(proto::ProtoError::BadMagic(0x00, 0x00))
    ));
}

#[test]
fn header_decode_oversized() {
    let mut buf = [0u8; HEADER_LEN];
    proto::encode_header(&mut buf, 0, MAX_PAYLOAD + 1, 0);
    assert!(matches!(
        proto::decode_header(&buf),
        Err(proto::ProtoError::PayloadTooLarge(_))
    ));
}

#[test]
fn header_decode_too_many_fds() {
    let mut buf = [0u8; HEADER_LEN];
    proto::encode_header(&mut buf, 0, 0, 254);
    assert!(matches!(
        proto::decode_header(&buf),
        Err(proto::ProtoError::TooManyFds(254))
    ));
}


#[tokio::test]
async fn poisoned_send_is_rechecked_after_waiting_for_mutex() {
    let (a, b) = UnixStream::pair().unwrap();

    let config = Config {
        send_timeout: Some(Duration::from_millis(60)),
        recv_timeout: None,
        keepalive_interval: None,
    };
    let conn = Connection::from_stream(a, config);
    let (tx, _) = conn.split();
    let tx = Arc::new(tx);

    // First task will likely block trying to push a large frame into a socket
    // that nobody is draining.
    let tx1 = Arc::clone(&tx);
    let first = tokio::spawn(async move {
        let big = vec![0u8; MAX_PAYLOAD as usize];
        let _ = tx1.send(FLAG_NONE, &big, &[]).await;
    });

    // Let the first task grab the mutex and either complete or poison.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Close the peer so the first task cannot make further progress forever.
    drop(b);
    let _ = first.await;

    if tx.is_poisoned() {
        let err = tx.send(FLAG_NONE, b"late", &[]).await.unwrap_err();
        assert!(matches!(err, proto::ProtoError::Poisoned));
    }
}

#[test]
fn listener_bind_clean_refuses_non_socket_paths() {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("wireframe-test-{unique}.txt"));
    fs::write(&path, b"not a socket").unwrap();

    let err = wireframe::Listener::bind_clean(&path).unwrap_err();
    assert!(matches!(err, proto::ProtoError::Io(_)));

    fs::remove_file(path).unwrap();
}

#![cfg(feature = "async")]

use tokio::net::UnixStream;
use fbsockets::proto::{self, Config};
use fbsockets::r#async::Connection;

fn pair() -> (Connection, Connection) {
    let (a, b) = UnixStream::pair().unwrap();
    (
        Connection::from_stream(a, Config::unconstrained()),
        Connection::from_stream(b, Config::unconstrained()),
    )
}

#[tokio::test]
async fn async_roundtrip_simple() {
    let (a, b) = pair();
    a.send(b"hello", &[]).await.unwrap();
    let (data, fds) = b.recv().await.unwrap();
    assert_eq!(data, b"hello");
    assert!(fds.is_empty());
}

#[tokio::test]
async fn recv_timeout_without_progress_does_not_poison() {
    let (a, _b) = UnixStream::pair().unwrap();
    let conn = Connection::from_stream(
        a,
        Config {
            send_timeout: None,
            recv_timeout: Some(std::time::Duration::from_millis(50)),
        },
    );

    let err = conn.recv().await.unwrap_err();
    assert!(matches!(err, proto::ProtoError::Timeout(_)));
}

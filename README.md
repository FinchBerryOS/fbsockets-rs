# fbsockets

<p align="center">
  <img src="https://raw.githubusercontent.com/FinchBerryOS/.github/refs/heads/main/profile/assets/FinchBerry-Sockets_Github.png" alt="FinchBerryOS Logo" width="400">
</p>

Low-level framed IPC over Unix domain sockets with FD passing and fork detection.

`fbsockets` is designed as a transport-layer crate. It sits close to the OS and gives you a small, explicit API for sending bytes and file descriptors over Unix domain sockets.

## What it provides

- blocking API by default
- optional Tokio-based async API via the `async` feature
- one `Connection` object for sending and receiving
- thread-safe serialization:
  - at most one send at a time
  - at most one receive at a time
- up to **16 MiB** payload per message
- up to **64 FDs** per message
- ancillary FD-space policy budget of **456 bytes**
- PID/fork detection: inherited sockets are shut down in the child on first use
- no hidden background tasks

## Why this crate is low level

This crate is low level because it works directly with:

- Unix domain sockets
- `sendmsg(2)` / `recvmsg(2)`
- `SCM_RIGHTS`
- raw `OwnedFd` / `BorrowedFd`
- explicit framing and timeout handling

It is transport infrastructure, not an RPC framework.

## Blocking API

The blocking API is the default API.

### Blocking client

```rust
use fbsockets::{Config, Connection};

fn main() -> fbsockets::Result<()> {
    let conn = Connection::connect("/tmp/fbsockets.sock", Config::default())?;

    conn.send(b"hello server", &[])?;

    let (data, fds) = conn.recv()?;
    println!("reply: {}", String::from_utf8_lossy(&data));
    println!("fds: {}", fds.len());

    Ok(())
}
```

### Blocking server

```rust
use fbsockets::{Config, Listener};

fn main() -> fbsockets::Result<()> {
    let listener = Listener::bind_clean("/tmp/fbsockets.sock")?;
    let conn = listener.accept(Config::default())?;

    let (data, fds) = conn.recv()?;
    println!("from client: {}", String::from_utf8_lossy(&data));
    println!("fds: {}", fds.len());

    conn.send(b"pong", &[])?;
    Ok(())
}
```

## Async API

Enable the `async` feature and use `fbsockets::async::Connection` and `fbsockets::async::Listener`.

### Async client

```rust
use fbsockets::Config;
use fbsockets::r#async::Connection;

#[tokio::main]
async fn main() -> fbsockets::Result<()> {
    let conn = Connection::connect("/tmp/fbsockets.sock", Config::default()).await?;

    conn.send(b"hello server", &[]).await?;

    let (data, fds) = conn.recv().await?;
    println!("reply: {}", String::from_utf8_lossy(&data));
    println!("fds: {}", fds.len());

    Ok(())
}
```

### Async server

```rust
use fbsockets::Config;
use fbsockets::r#async::Listener;

#[tokio::main]
async fn main() -> fbsockets::Result<()> {
    let listener = Listener::bind("/tmp/fbsockets.sock")?;
    let conn = listener.accept(Config::default()).await?;

    let (data, fds) = conn.recv().await?;
    println!("from client: {}", String::from_utf8_lossy(&data));
    println!("fds: {}", fds.len());

    conn.send(b"pong", &[]).await?;
    Ok(())
}
```

## Sending file descriptors

```rust
use std::fs::File;
use std::os::fd::AsFd;
use fbsockets::{Config, Connection};

fn main() -> fbsockets::Result<()> {
    let conn = Connection::connect("/tmp/fbsockets.sock", Config::default())?;
    let file = File::open("/tmp/demo.txt")?;

    conn.send(b"with-fd", &[file.as_fd()])?;
    Ok(())
}
```

Receiving returns both payload and any attached FDs:

```rust
let (data, fds) = conn.recv()?;
```

## Fork safety

If a `Connection` is inherited across `fork()`, the child process must not continue to use the inherited socket state as if nothing happened. `fbsockets` stores the original PID and checks it on each operation.

If the PID changed:

- the socket is shut down in the child
- both directions are marked unusable
- the operation fails with `ForkDetected`

That protects you from accidentally using inherited synchronization and stream state in the child.

## Timeouts and poisoning

Each `Connection` supports configurable send/receive timeouts via `Config`.

Important rule:

- if a timeout happens **before any bytes move**, the connection is not poisoned
- if a timeout or I/O error happens **after partial frame progress**, the corresponding direction is poisoned

That prevents silently continuing on a stream that may already be out of frame sync.

## Message limits

- payload: **16 MiB max**
- FDs: **64 max per message**
- FD ancillary budget: **456 bytes**

## Production notes

This code is structured for production-oriented use, but you should still do the usual engineering work before calling it fully hardened in your own environment:

- run `cargo test` on your real target OS
- test actual FD-passing workflows on Linux
- fuzz malformed headers and partial frames
- load-test timeouts and peer disconnect paths
- verify your fork model in integration tests

## License

BSD 3-Clause License

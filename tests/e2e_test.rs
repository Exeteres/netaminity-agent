use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bore_cli::{
    client::Client,
    server::Server,
    shared::{
        ClientMessage, Delimited, ServerMessage, CONTROL_IDLE_TIMEOUT, DEFAULT_CONTROL_PORT,
        HEALTH_CHECK_INTERVAL,
    },
};
use lazy_static::lazy_static;
use rstest::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};
use tokio::time;

lazy_static! {
    /// Guard to make sure that tests are run serially, not concurrently.
    static ref SERIAL_GUARD: Mutex<()> = Mutex::new(());
}

/// Spawn the server, giving some time for the control port TcpListener to start.
async fn spawn_server(secret: Option<&str>) {
    tokio::spawn(Server::new(1024..=65535, secret).listen());
    time::sleep(Duration::from_millis(50)).await;
}

/// Spawns a client with randomly assigned ports, returning the listener and remote address.
async fn spawn_client(secret: Option<&str>) -> Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind("localhost:0").await?;
    let local_port = listener.local_addr()?.port();
    let client = Client::new(
        "localhost",
        local_port,
        "localhost",
        0,
        secret,
        DEFAULT_CONTROL_PORT,
    )
    .await?;
    let remote_addr = ([127, 0, 0, 1], client.remote_port()).into();
    tokio::spawn(client.listen());
    Ok((listener, remote_addr))
}

async fn unused_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

async fn http_status(port: u16, path: &str) -> Result<u16> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let response = String::from_utf8(response)?;
    Ok(response
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("missing HTTP status"))?
        .parse()?)
}

async fn wait_for_status(port: u16, path: &str, expected: u16) -> Result<()> {
    time::timeout(Duration::from_secs(20), async {
        loop {
            if matches!(http_status(port, path).await, Ok(status) if status == expected) {
                return;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    Ok(())
}

#[rstest]
#[tokio::test]
async fn basic_proxy(#[values(None, Some(""), Some("abc"))] secret: Option<&str>) -> Result<()> {
    let _guard = SERIAL_GUARD.lock().await;

    spawn_server(secret).await;
    let (listener, addr) = spawn_client(secret).await?;

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut buf = [0u8; 11];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world");

        stream.write_all(b"I can send a message too!").await?;
        anyhow::Ok(())
    });

    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(b"hello world").await?;

    let mut buf = [0u8; 25];
    stream.read_exact(&mut buf).await?;
    assert_eq!(&buf, b"I can send a message too!");

    // Ensure that the client end of the stream is closed now.
    assert_eq!(stream.read(&mut buf).await?, 0);

    // Also ensure that additional connections do not produce any data.
    let mut stream = TcpStream::connect(addr).await?;
    assert_eq!(stream.read(&mut buf).await?, 0);

    Ok(())
}

#[rstest]
#[case(None, Some("my secret"))]
#[case(Some("my secret"), None)]
#[tokio::test]
async fn mismatched_secret(
    #[case] server_secret: Option<&str>,
    #[case] client_secret: Option<&str>,
) {
    let _guard = SERIAL_GUARD.lock().await;

    spawn_server(server_secret).await;
    assert!(spawn_client(client_secret).await.is_err());
}

#[tokio::test]
async fn invalid_address() -> Result<()> {
    // We don't need the serial guard for this test because it doesn't create a server.
    async fn check_address(to: &str, use_secret: bool) -> Result<()> {
        match Client::new(
            "localhost",
            5000,
            to,
            0,
            use_secret.then_some("a secret"),
            DEFAULT_CONTROL_PORT,
        )
        .await
        {
            Ok(_) => Err(anyhow!("expected error for {to}, use_secret={use_secret}")),
            Err(_) => Ok(()),
        }
    }
    tokio::try_join!(
        check_address("google.com", false),
        check_address("google.com", true),
        check_address("nonexistent.domain.for.demonstration", false),
        check_address("nonexistent.domain.for.demonstration", true),
        check_address("malformed !$uri$%", false),
        check_address("malformed !$uri$%", true),
    )?;
    Ok(())
}

#[tokio::test]
async fn very_long_frame() -> Result<()> {
    let _guard = SERIAL_GUARD.lock().await;

    spawn_server(None).await;
    let mut attacker = TcpStream::connect(("localhost", DEFAULT_CONTROL_PORT)).await?;

    // Slowly send a very long frame.
    for _ in 0..10 {
        let result = attacker.write_all(&[42u8; 100000]).await;
        if result.is_err() {
            return Ok(());
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    panic!("did not exit after a 1 MB frame");
}

#[test]
#[should_panic]
fn empty_port_range() {
    let min_port = 5000;
    let max_port = 3000;
    let _ = Server::new(min_port..=max_port, None);
}

#[tokio::test]
async fn half_closed_tcp_stream() -> Result<()> {
    // Check that "half-closed" TCP streams will not result in spontaneous hangups.
    let _guard = SERIAL_GUARD.lock().await;

    spawn_server(None).await;
    let (listener, addr) = spawn_client(None).await?;

    let (mut cli, (mut srv, _)) = tokio::try_join!(TcpStream::connect(addr), listener.accept())?;

    // Send data before half-closing one of the streams.
    let mut buf = b"message before shutdown".to_vec();
    cli.write_all(&buf).await?;

    // Only close the write half of the stream. This is a half-closed stream. In the
    // TCP protocol, it is represented as a FIN packet on one end. The entire stream
    // is only closed after two FINs are exchanged and ACKed by the other end.
    cli.shutdown().await?;

    srv.read_exact(&mut buf).await?;
    assert_eq!(buf, b"message before shutdown");
    assert_eq!(srv.read(&mut buf).await?, 0); // EOF

    // Now make sure that the other stream can still send data, despite
    // half-shutdown on client->server side.
    let mut buf = b"hello from the other side!".to_vec();
    srv.write_all(&buf).await?;
    cli.read_exact(&mut buf).await?;
    assert_eq!(buf, b"hello from the other side!");

    // We don't have to think about CLOSE_RD handling because that's not really
    // part of the TCP protocol, just the POSIX streams API. It is implemented by
    // the OS ignoring future packets received on that stream.

    Ok(())
}

#[tokio::test]
async fn reliable_health_distinguishes_backend_failure() -> Result<()> {
    let _guard = SERIAL_GUARD.lock().await;
    let control_port = unused_port().await?;
    let proxy_port = unused_port().await?;
    let proxy_health_port = unused_port().await?;
    let target_health_port = unused_port().await?;
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let backend_port = backend.local_addr()?.port();
    let backend_task = tokio::spawn(async move {
        loop {
            let _ = backend.accept().await;
        }
    });

    let mut server = Server::new(proxy_port..=proxy_port, None);
    server.set_bind_addr("127.0.0.1".parse()?);
    server.set_bind_tunnels("127.0.0.1".parse()?);
    server.set_control_port(control_port);
    server.set_reliable(true);
    server.set_health_addr(([127, 0, 0, 1], proxy_health_port).into());
    let server_task = tokio::spawn(server.listen());
    time::sleep(Duration::from_millis(50)).await;

    let mut client = Client::new(
        "127.0.0.1",
        backend_port,
        "127.0.0.1",
        proxy_port,
        None,
        control_port,
    )
    .await?;
    client.set_reliable(true);
    client.set_health_addr(([127, 0, 0, 1], target_health_port).into());
    let client_task = tokio::spawn(client.listen());

    wait_for_status(proxy_health_port, "/ready", 200)
        .await
        .context("proxy did not become ready")?;
    wait_for_status(target_health_port, "/ready", 200)
        .await
        .context("target did not become ready")?;

    backend_task.abort();
    wait_for_status(proxy_health_port, "/ready", 503)
        .await
        .context("proxy did not observe backend failure")?;
    wait_for_status(target_health_port, "/ready", 503)
        .await
        .context("target did not observe backend failure")?;
    assert_eq!(http_status(proxy_health_port, "/live").await?, 200);
    assert_eq!(http_status(target_health_port, "/live").await?, 200);

    client_task.abort();
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn reliable_target_reconnects_in_process() -> Result<()> {
    let _guard = SERIAL_GUARD.lock().await;
    let control_port = unused_port().await?;
    let proxy_port = unused_port().await?;
    let target_health_port = unused_port().await?;
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let backend_port = backend.local_addr()?.port();
    let backend_task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = backend.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut value = [0; 4];
                if stream.read_exact(&mut value).await.is_ok() {
                    let _ = stream.write_all(&value).await;
                }
            });
        }
    });
    let (reconnected, reconnected_rx) = oneshot::channel();
    let (finish, finish_rx) = oneshot::channel();
    let mut client = Client::new_reliable(
        "127.0.0.1",
        backend_port,
        "127.0.0.1",
        proxy_port,
        None,
        control_port,
    );
    client.set_health_addr(([127, 0, 0, 1], target_health_port).into());
    let client_task = tokio::spawn(client.listen());

    wait_for_status(target_health_port, "/live", 200).await?;
    assert_eq!(http_status(target_health_port, "/ready").await?, 503);
    assert!(!client_task.is_finished());

    let peer_task = tokio::spawn(async move {
        let control_listener = TcpListener::bind(("127.0.0.1", control_port)).await?;
        let mut reconnected = Some(reconnected);
        let mut finish_rx = Some(finish_rx);
        for session in 0..2 {
            let (stream, _) = control_listener.accept().await?;
            let mut stream = Delimited::new(stream);
            match stream.recv_timeout().await? {
                Some(ClientMessage::Hello(port)) if port == proxy_port => {}
                message => return Err(anyhow!("unexpected hello: {message:?}")),
            }
            stream.send(ServerMessage::Hello(proxy_port)).await?;
            let nonce = uuid::Uuid::new_v4();
            stream.send(ServerMessage::HealthCheck(nonce)).await?;
            match stream.recv_timeout().await? {
                Some(ClientMessage::Health(response_nonce, _)) if response_nonce == nonce => {}
                message => return Err(anyhow!("unexpected health response: {message:?}")),
            }
            if session == 0 {
                drop(stream);
            } else {
                let _ = reconnected.take().unwrap().send(());
                let _ = finish_rx.take().unwrap().await;
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    time::timeout(Duration::from_secs(10), reconnected_rx).await??;
    wait_for_status(target_health_port, "/ready", 200).await?;
    assert!(!client_task.is_finished());
    assert_eq!(http_status(target_health_port, "/live").await?, 200);

    let _ = finish.send(());
    peer_task.await??;
    client_task.abort();
    backend_task.abort();
    Ok(())
}

#[tokio::test]
async fn reliable_target_recovers_from_silent_control_peer() -> Result<()> {
    let control = TcpListener::bind("127.0.0.1:0").await?;
    let control_port = control.local_addr()?.port();
    let health_port = unused_port().await?;
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let mut client = Client::new_reliable(
        "127.0.0.1",
        backend.local_addr()?.port(),
        "127.0.0.1",
        12345,
        None,
        control_port,
    );
    client.set_health_addr(([127, 0, 0, 1], health_port).into());
    let backend_task = tokio::spawn(async move {
        loop {
            let _ = backend.accept().await?;
        }
        #[allow(unreachable_code)]
        anyhow::Ok(())
    });
    let client_task = tokio::spawn(client.listen());
    let (stream, _) = time::timeout(Duration::from_secs(3), control.accept()).await??;
    let mut silent_peer = Delimited::new(stream);
    assert!(matches!(
        silent_peer.recv_timeout().await?,
        Some(ClientMessage::Hello(12345))
    ));
    silent_peer.send(ServerMessage::Hello(12345)).await?;
    wait_for_status(health_port, "/ready", 200).await?;

    // Keep the socket open and ACKing TCP, but never send another protocol
    // message. TCP keepalive alone cannot detect this stalled application peer.
    let (replacement, _) = time::timeout(
        CONTROL_IDLE_TIMEOUT + Duration::from_secs(3),
        control.accept(),
    )
    .await??;
    assert_eq!(http_status(health_port, "/ready").await?, 503);
    assert_eq!(http_status(health_port, "/live").await?, 200);
    assert!(
        time::timeout(Duration::from_secs(1), silent_peer.recv::<ClientMessage>())
            .await??
            .is_none()
    );

    // The same process restores readiness after the new session authenticates.
    let mut replacement = Delimited::new(replacement);
    assert!(matches!(
        replacement.recv_timeout().await?,
        Some(ClientMessage::Hello(12345))
    ));
    replacement.send(ServerMessage::Hello(12345)).await?;
    let nonce = uuid::Uuid::new_v4();
    replacement.send(ServerMessage::HealthCheck(nonce)).await?;
    assert!(matches!(
        replacement.recv_timeout().await?,
        Some(ClientMessage::Health(response, true)) if response == nonce
    ));
    wait_for_status(health_port, "/ready", 200).await?;
    assert!(!client_task.is_finished());
    client_task.abort();
    backend_task.abort();
    Ok(())
}

#[rstest]
#[tokio::test]
async fn reliable_target_retains_idle_session_and_stream(
    #[values(false, true)] reliable_proxy: bool,
) -> Result<()> {
    let control_port = unused_port().await?;
    let proxy_port = unused_port().await?;
    let health_port = unused_port().await?;
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let backend_port = backend.local_addr()?.port();
    let backend_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await?;
            tokio::spawn(async move {
                let (mut read, mut write) = stream.split();
                tokio::io::copy(&mut read, &mut write).await
            });
        }
        #[allow(unreachable_code)]
        anyhow::Ok(())
    });
    let mut server = Server::new(proxy_port..=proxy_port, None);
    server.set_bind_addr("127.0.0.1".parse()?);
    server.set_bind_tunnels("127.0.0.1".parse()?);
    server.set_control_port(control_port);
    server.set_reliable(reliable_proxy);
    let server_task = tokio::spawn(server.listen());
    let mut client = Client::new_reliable(
        "127.0.0.1",
        backend_port,
        "127.0.0.1",
        proxy_port,
        None,
        control_port,
    );
    client.set_health_addr(([127, 0, 0, 1], health_port).into());
    let client_task = tokio::spawn(client.listen());
    wait_for_status(health_port, "/ready", 200).await?;
    let mut stream = TcpStream::connect(("127.0.0.1", proxy_port)).await?;
    stream.write_all(b"before").await?;
    let mut echo = [0; 6];
    time::timeout(Duration::from_secs(5), stream.read_exact(&mut echo)).await??;
    assert_eq!(&echo, b"before");

    // No application data or new consumers for longer than the watchdog.
    // Retaining this exact data connection proves the session was not replaced:
    // run_session drops (and aborts) its forwarding JoinSet on session loss.
    let until = time::Instant::now() + CONTROL_IDLE_TIMEOUT + HEALTH_CHECK_INTERVAL;
    while time::Instant::now() < until {
        assert_eq!(http_status(health_port, "/ready").await?, 200);
        time::sleep(Duration::from_millis(100)).await;
    }
    stream.write_all(b"after!").await?;
    time::timeout(Duration::from_secs(5), stream.read_exact(&mut echo)).await??;
    assert_eq!(&echo, b"after!");

    // New application work is still accepted through the same control session.
    let mut new_stream = TcpStream::connect(("127.0.0.1", proxy_port)).await?;
    new_stream.write_all(b"new!!!").await?;
    time::timeout(Duration::from_secs(5), new_stream.read_exact(&mut echo)).await??;
    assert_eq!(&echo, b"new!!!");
    client_task.abort();
    server_task.abort();
    backend_task.abort();
    Ok(())
}

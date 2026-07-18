//! Server implementation for the `bore` service.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::{io, ops::RangeInclusive, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{interval, sleep, timeout};
use tracing::{error, info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::auth::Authenticator;
use crate::health::{serve as serve_health, HealthState};
use crate::shared::{
    ClientMessage, Delimited, ServerMessage, DEFAULT_CONTROL_PORT, HEALTH_CHECK_INTERVAL,
    HEALTH_FAILURE_THRESHOLD, NETWORK_TIMEOUT,
};

/// State structure for the server.
pub struct Server {
    /// Range of TCP ports that can be forwarded.
    port_range: RangeInclusive<u16>,

    /// Optional secret used to authenticate clients.
    auth: Option<Authenticator>,

    /// Concurrent map of IDs to incoming connections.
    conns: Arc<DashMap<Uuid, TcpStream>>,

    /// IP address where the control server will bind to.
    bind_addr: IpAddr,

    /// IP address where tunnels will listen on.
    bind_tunnels: IpAddr,

    /// TCP port used for control connections with clients.
    control_port: u16,

    /// Enable Netaminity reliability behavior.
    reliable: bool,

    /// Address serving local HTTP health endpoints.
    health_addr: Option<SocketAddr>,

    /// Current tunnel health.
    health: Arc<HealthState>,

    /// Pending synthetic tunnel probes.
    probes: Arc<DashMap<Uuid, oneshot::Sender<()>>>,
}

impl Server {
    /// Create a new server with a specified minimum port number.
    pub fn new(port_range: RangeInclusive<u16>, secret: Option<&str>) -> Self {
        assert!(!port_range.is_empty(), "must provide at least one port");
        Server {
            port_range,
            conns: Arc::new(DashMap::new()),
            auth: secret.map(Authenticator::new),
            bind_addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            bind_tunnels: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            control_port: DEFAULT_CONTROL_PORT,
            reliable: false,
            health_addr: None,
            health: HealthState::new(),
            probes: Arc::new(DashMap::new()),
        }
    }

    /// Set the IP address where tunnels will listen on.
    pub fn set_bind_addr(&mut self, bind_addr: IpAddr) {
        self.bind_addr = bind_addr;
    }

    /// Set the IP address where the control server will bind to.
    pub fn set_bind_tunnels(&mut self, bind_tunnels: IpAddr) {
        self.bind_tunnels = bind_tunnels;
    }

    /// Set the TCP port used for control connections with clients.
    pub fn set_control_port(&mut self, control_port: u16) {
        self.control_port = control_port;
    }

    /// Enable reliability checks and restart behavior.
    pub fn set_reliable(&mut self, reliable: bool) {
        self.reliable = reliable;
    }

    /// Set the address serving HTTP health endpoints.
    pub fn set_health_addr(&mut self, health_addr: SocketAddr) {
        self.health_addr = Some(health_addr);
    }

    /// Start the server, listening for new connections.
    pub async fn listen(self) -> Result<()> {
        if let Some(addr) = self.health_addr {
            let health = Arc::clone(&self.health);
            tokio::spawn(async move {
                if let Err(err) = serve_health(addr, "proxy", health).await {
                    error!(%err, "health server exited");
                }
            });
        }
        let this = Arc::new(self);
        let listener = TcpListener::bind((this.bind_addr, this.control_port)).await?;
        info!(addr = ?this.bind_addr, port = this.control_port, "server listening");

        loop {
            let (stream, addr) = listener.accept().await?;
            let this = Arc::clone(&this);
            tokio::spawn(
                async move {
                    info!("incoming connection");
                    if let Err(err) = this.handle_connection(stream).await {
                        warn!(%err, "connection exited with error");
                    } else {
                        info!("connection exited");
                    }
                }
                .instrument(info_span!("control", ?addr)),
            );
        }
    }

    async fn create_listener(&self, port: u16) -> Result<TcpListener, &'static str> {
        let try_bind = |port: u16| async move {
            TcpListener::bind((self.bind_tunnels, port))
                .await
                .map_err(|err| match err.kind() {
                    io::ErrorKind::AddrInUse => "port already in use",
                    io::ErrorKind::PermissionDenied => "permission denied",
                    _ => "failed to bind to port",
                })
        };
        if port > 0 {
            // Client requests a specific port number.
            if !self.port_range.contains(&port) {
                return Err("client port number not in allowed range");
            }
            try_bind(port).await
        } else {
            // Client requests any available port in range.
            //
            // In this case, we bind to 150 random port numbers. We choose this value because in
            // order to find a free port with probability at least 1-δ, when ε proportion of the
            // ports are currently available, it suffices to check approximately -2 ln(δ) / ε
            // independently and uniformly chosen ports (up to a second-order term in ε).
            //
            // Checking 150 times gives us 99.999% success at utilizing 85% of ports under these
            // conditions, when ε=0.15 and δ=0.00001.
            for _ in 0..150 {
                let port = fastrand::u16(self.port_range.clone());
                match try_bind(port).await {
                    Ok(listener) => return Ok(listener),
                    Err(_) => continue,
                }
            }
            Err("failed to find an available port")
        }
    }

    async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let mut stream = Delimited::new(stream);
        if let Some(auth) = &self.auth {
            if let Err(err) = auth.server_handshake(&mut stream).await {
                warn!(%err, "server handshake failed");
                stream.send(ServerMessage::Error(err.to_string())).await?;
                return Ok(());
            }
        }

        match stream.recv_timeout().await? {
            Some(ClientMessage::Authenticate(_)) => {
                warn!("unexpected authenticate");
                Ok(())
            }
            Some(ClientMessage::Hello(port)) => {
                let listener = match self.create_listener(port).await {
                    Ok(listener) => listener,
                    Err(err) => {
                        stream.send(ServerMessage::Error(err.into())).await?;
                        return Ok(());
                    }
                };
                let host = listener.local_addr()?.ip();
                let port = listener.local_addr()?.port();
                info!(?host, ?port, "new client");
                stream.send(ServerMessage::Hello(port)).await?;

                if self.reliable {
                    self.health.set_control_connected(true);
                    info!(?host, ?port, "reliable target control session established");
                    let result = self.reliable_session(&mut stream, &listener).await;
                    self.health.set_control_connected(false);
                    self.health.fail_liveness();
                    match &result {
                        Err(err) => {
                            error!(error = %err, "reliable target control session failed; failing liveness")
                        }
                        Ok(()) => warn!("reliable target control session closed; failing liveness"),
                    }
                    return result;
                }

                loop {
                    if stream.send(ServerMessage::Heartbeat).await.is_err() {
                        // Assume that the TCP connection has been dropped.
                        return Ok(());
                    }
                    const TIMEOUT: Duration = Duration::from_millis(500);
                    if let Ok(result) = timeout(TIMEOUT, listener.accept()).await {
                        let (stream2, addr) = result?;
                        info!(?addr, ?port, "new connection");

                        let id = Uuid::new_v4();
                        let conns = Arc::clone(&self.conns);

                        conns.insert(id, stream2);
                        tokio::spawn(async move {
                            // Remove stale entries to avoid memory leaks.
                            sleep(Duration::from_secs(10)).await;
                            if conns.remove(&id).is_some() {
                                warn!(%id, "removed stale connection");
                            }
                        });
                        stream.send(ServerMessage::Connection(id)).await?;
                    }
                }
            }
            Some(ClientMessage::Accept(id)) => {
                info!(%id, "forwarding connection");
                if let Some((_, completed)) = self.probes.remove(&id) {
                    let _ = completed.send(());
                    return Ok(());
                }
                match self.conns.remove(&id) {
                    Some((_, mut stream2)) => {
                        let mut parts = stream.into_parts();
                        debug_assert!(parts.write_buf.is_empty(), "framed write buffer not empty");
                        stream2.write_all(&parts.read_buf).await?;
                        tokio::io::copy_bidirectional(&mut parts.io, &mut stream2).await?;
                    }
                    None => warn!(%id, "missing connection"),
                }
                Ok(())
            }
            None => Ok(()),
            Some(ClientMessage::Health(_, _)) => {
                warn!("unexpected health report");
                Ok(())
            }
        }
    }

    async fn reliable_session(
        &self,
        stream: &mut Delimited<TcpStream>,
        listener: &TcpListener,
    ) -> Result<()> {
        let mut checks = interval(HEALTH_CHECK_INTERVAL);
        let mut control_failures = 0;
        let mut tunnel_failures = 0;
        let mut previous_backend = None;
        let mut tunnel_verified = false;
        loop {
            tokio::select! {
                _ = checks.tick() => {
                    let nonce = Uuid::new_v4();
                    stream.send(ServerMessage::HealthCheck(nonce)).await?;
                    let backend_reachable = match receive_health(stream, nonce).await {
                        Ok(Some(reachable)) => {
                            if control_failures > 0 {
                                info!(previous_failures = control_failures, "control health recovered");
                            }
                            control_failures = 0;
                            self.health.set_control_connected(true);
                            reachable
                        }
                        Ok(None) => bail!("control connection closed"),
                        Err(err) => {
                            control_failures += 1;
                            warn!(control_failures, %err, "control health check failed");
                            self.health.set_control_connected(false);
                            if control_failures >= HEALTH_FAILURE_THRESHOLD {
                                self.health.fail_liveness();
                                send_restart(stream, "control health failure").await;
                                bail!("control health failed after {control_failures} checks");
                            }
                            continue;
                        }
                    };
                    self.health.set_backend_reachable(backend_reachable);
                    if previous_backend != Some(backend_reachable) {
                        if backend_reachable {
                            info!("target reports backend reachable");
                        } else {
                            warn!("target reports backend unreachable; suppressing restart-worthy tunnel probes");
                        }
                        previous_backend = Some(backend_reachable);
                    }

                    if !backend_reachable {
                        tunnel_failures = 0;
                        tunnel_verified = false;
                        continue;
                    }

                    let listener_addr = listener.local_addr()?;
                    let probe_addr = SocketAddr::new(
                        match listener_addr.ip() {
                            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
                            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
                            ip => ip,
                        },
                        listener_addr.port(),
                    );
                    let (probe_connection, (incoming, _)) = tokio::try_join!(
                        TcpStream::connect(probe_addr),
                        listener.accept(),
                    )?;
                    let id = Uuid::new_v4();
                    let (completed, completion) = oneshot::channel();
                    self.probes.insert(id, completed);
                    stream.send(ServerMessage::Connection(id)).await?;
                    match timeout(NETWORK_TIMEOUT, completion).await {
                        Ok(Ok(())) => {
                            if !tunnel_verified {
                                info!(consumer_port = listener.local_addr()?.port(), previous_failures = tunnel_failures, "end-to-end tunnel verified");
                            }
                            tunnel_failures = 0;
                            tunnel_verified = true;
                            self.health.set_tunnel_verified(true);
                        }
                        _ => {
                            self.probes.remove(&id);
                            tunnel_failures += 1;
                            tunnel_verified = false;
                            self.health.set_tunnel_verified(false);
                            warn!(tunnel_failures, "end-to-end tunnel probe failed");
                            if tunnel_failures >= HEALTH_FAILURE_THRESHOLD {
                                self.health.fail_liveness();
                                send_restart(stream, "end-to-end tunnel failure").await;
                                bail!("tunnel integrity failed after {tunnel_failures} probes");
                            }
                        }
                    }
                    drop(incoming);
                    drop(probe_connection);
                }
                result = listener.accept() => {
                    let (incoming, addr) = result?;
                    info!(?addr, "new connection");
                    let id = Uuid::new_v4();
                    self.conns.insert(id, incoming);
                    let conns = Arc::clone(&self.conns);
                    tokio::spawn(async move {
                        sleep(Duration::from_secs(10)).await;
                        if conns.remove(&id).is_some() {
                            warn!(%id, "removed stale connection");
                        }
                    });
                    stream.send(ServerMessage::Connection(id)).await?;
                }
            }
        }
    }
}

async fn send_restart(stream: &mut Delimited<TcpStream>, reason: &'static str) {
    match stream.send(ServerMessage::Restart).await {
        Ok(()) => error!(%reason, "requested coordinated target restart"),
        Err(err) => {
            error!(%reason, %err, "failed to request target restart; control loss is the fallback")
        }
    }
}

async fn receive_health(
    stream: &mut Delimited<TcpStream>,
    expected_nonce: Uuid,
) -> Result<Option<bool>> {
    timeout(NETWORK_TIMEOUT, async {
        loop {
            match stream.recv().await? {
                Some(ClientMessage::Health(nonce, reachable)) if nonce == expected_nonce => {
                    return Ok(Some(reachable));
                }
                Some(ClientMessage::Health(nonce, _)) => {
                    warn!(%nonce, %expected_nonce, "ignored stale control health response");
                }
                Some(message) => bail!("unexpected reliability message: {message:?}"),
                None => return Ok(None),
            }
        }
    })
    .await
    .context("timed out waiting for control health response")?
}

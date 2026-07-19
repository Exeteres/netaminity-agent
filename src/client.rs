//! Client implementation for the `bore` service.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::task::JoinSet;
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};
use tracing::{debug, error, info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::auth::Authenticator;
use crate::health::{serve as serve_health, HealthState};
use crate::shared::{
    ClientMessage, Delimited, ServerMessage, BACKEND_CHECK_INTERVAL, CONNECTION_ATTEMPT_TIMEOUT,
    CONNECT_TIMEOUT,
};

const BACKOFF_RESET_AFTER: Duration = Duration::from_secs(10);
const RECONNECT_DELAYS_MS: [u64; 5] = [100, 250, 500, 1_000, 2_000];

/// State structure for the client.
pub struct Client {
    /// Control connection to the server.
    conn: Option<Delimited<TcpStream>>,

    /// Destination address of the server.
    to: String,

    // Local host that is forwarded.
    local_host: String,

    /// Local port that is forwarded.
    local_port: u16,

    /// Port that is publicly available on the remote.
    remote_port: AtomicU16,

    /// Port requested when establishing a control session.
    requested_port: u16,

    /// Optional secret used to authenticate clients.
    auth: Option<Authenticator>,

    /// TCP port used for control connections with the server.
    control_port: u16,

    /// Enable Netaminity reliability behavior.
    reliable: bool,

    /// Address serving local HTTP health endpoints.
    health_addr: Option<SocketAddr>,

    /// Current tunnel health.
    health: Arc<HealthState>,
}

impl Client {
    /// Create a new client.
    pub async fn new(
        local_host: &str,
        local_port: u16,
        to: &str,
        port: u16,
        secret: Option<&str>,
        control_port: u16,
    ) -> Result<Self> {
        let mut stream = Delimited::new(connect_with_timeout(to, control_port).await?);
        let auth = secret.map(Authenticator::new);
        if let Some(auth) = &auth {
            auth.client_handshake(&mut stream).await?;
        }

        stream.send(ClientMessage::Hello(port)).await?;
        let remote_port = match stream.recv_timeout().await? {
            Some(ServerMessage::Hello(remote_port)) => remote_port,
            Some(ServerMessage::Error(message)) => bail!("server error: {message}"),
            Some(ServerMessage::Challenge(_)) => {
                bail!("server requires authentication, but no client secret was provided");
            }
            Some(_) => bail!("unexpected initial non-hello message"),
            None => bail!("unexpected EOF"),
        };
        info!(remote_port, "connected to server");
        info!("listening at {to}:{remote_port}");

        Ok(Client {
            conn: Some(stream),
            to: to.to_string(),
            local_host: local_host.to_string(),
            local_port,
            remote_port: AtomicU16::new(remote_port),
            requested_port: port,
            auth,
            control_port,
            reliable: false,
            health_addr: None,
            health: HealthState::new(),
        })
    }

    /// Create a reliable client whose initial connection is established by the supervisor.
    pub fn new_reliable(
        local_host: &str,
        local_port: u16,
        to: &str,
        port: u16,
        secret: Option<&str>,
        control_port: u16,
    ) -> Self {
        Self {
            conn: None,
            to: to.to_string(),
            local_host: local_host.to_string(),
            local_port,
            remote_port: AtomicU16::new(port),
            requested_port: port,
            auth: secret.map(Authenticator::new),
            control_port,
            reliable: true,
            health_addr: None,
            health: HealthState::new(),
        }
    }

    /// Enable health checks and automatic session recovery.
    pub fn set_reliable(&mut self, reliable: bool) {
        self.reliable = reliable;
    }

    /// Set the address serving HTTP health endpoints.
    pub fn set_health_addr(&mut self, health_addr: SocketAddr) {
        self.health_addr = Some(health_addr);
    }

    /// Returns the port publicly available on the remote.
    pub fn remote_port(&self) -> u16 {
        self.remote_port.load(Ordering::Relaxed)
    }

    /// Start the client, listening for new connections.
    pub async fn listen(mut self) -> Result<()> {
        if let Some(addr) = self.health_addr {
            let health = Arc::clone(&self.health);
            tokio::spawn(async move {
                if let Err(err) = serve_health(addr, "target", health).await {
                    error!(%err, "health server exited");
                }
            });
        }
        let mut conn = self.conn.take();
        let this = Arc::new(self);
        if this.reliable {
            let this = Arc::clone(&this);
            tokio::spawn(async move {
                let mut checks = tokio::time::interval(BACKEND_CHECK_INTERVAL);
                let mut previous = None;
                loop {
                    checks.tick().await;
                    let reachable = connect_with_timeout(&this.local_host, this.local_port)
                        .await
                        .is_ok();
                    this.health.set_backend_reachable(reachable);
                    if previous != Some(reachable) {
                        if reachable {
                            info!(
                                backend_host = %this.local_host,
                                backend_port = this.local_port,
                                "target backend became reachable"
                            );
                        } else {
                            warn!(
                                backend_host = %this.local_host,
                                backend_port = this.local_port,
                                "target backend became unreachable"
                            );
                        }
                        previous = Some(reachable);
                    }
                }
            });
        }

        let mut failed_attempts = 0usize;
        loop {
            let mut active_conn = match conn.take() {
                Some(conn) => conn,
                None => loop {
                    match this.connect_control().await {
                        Ok(conn) => break conn,
                        Err(err) => {
                            let delay = reconnect_delay(failed_attempts);
                            failed_attempts = failed_attempts.saturating_add(1);
                            warn!(
                                %err,
                                attempt = failed_attempts,
                                retry_ms = delay.as_millis(),
                                proxy_host = %this.to,
                                proxy_port = this.control_port,
                                "target control connect failed"
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                },
            };
            this.health.set_control_connected(true);
            info!(
                proxy_host = %this.to,
                proxy_port = this.control_port,
                backend_host = %this.local_host,
                backend_port = this.local_port,
                reliable = this.reliable,
                "target control session established"
            );
            let established_at = tokio::time::Instant::now();
            let result = this.run_session(&mut active_conn).await;
            this.health.set_control_connected(false);

            if !this.reliable {
                return result;
            }
            match result {
                Ok(()) => info!("target control session closed; reconnecting"),
                Err(err) => warn!(%err, "target control session failed; reconnecting"),
            }
            if established_at.elapsed() >= BACKOFF_RESET_AFTER {
                failed_attempts = 0;
            } else if failed_attempts > 0 {
                let delay = reconnect_delay(failed_attempts - 1);
                warn!(
                    retry_ms = delay.as_millis(),
                    "target control session was not stable; delaying reconnect"
                );
                tokio::time::sleep(delay).await;
            }
            failed_attempts = failed_attempts.saturating_add(1);

            loop {
                match this.connect_control().await {
                    Ok(new_conn) => {
                        conn = Some(new_conn);
                        break;
                    }
                    Err(err) => {
                        let delay = reconnect_delay(failed_attempts - 1);
                        warn!(
                            %err,
                            attempt = failed_attempts,
                            retry_ms = delay.as_millis(),
                            proxy_host = %this.to,
                            proxy_port = this.control_port,
                            "target control reconnect failed"
                        );
                        tokio::time::sleep(delay).await;
                        failed_attempts = failed_attempts.saturating_add(1);
                    }
                }
            }
        }
    }

    async fn run_session(self: &Arc<Self>, conn: &mut Delimited<TcpStream>) -> Result<()> {
        let mut tasks = JoinSet::new();
        loop {
            let message = match conn.recv().await {
                Ok(message) => message,
                Err(err) => {
                    return Err(err);
                }
            };
            match message {
                Some(ServerMessage::Hello(_)) => warn!("unexpected hello"),
                Some(ServerMessage::Challenge(_)) => warn!("unexpected challenge"),
                Some(ServerMessage::Heartbeat) => (),
                Some(ServerMessage::HealthCheck(nonce)) => {
                    conn.send(ClientMessage::Health(
                        nonce,
                        self.health.backend_reachable(),
                    ))
                    .await?;
                }
                Some(ServerMessage::TunnelProbe(id)) => {
                    let this = Arc::clone(self);
                    tasks.spawn(async move {
                        if let Err(err) = this.handle_probe(id).await {
                            warn!(%id, %err, "tunnel probe failed");
                        }
                    });
                }
                Some(ServerMessage::Connection(id)) => {
                    let this = Arc::clone(self);
                    tasks.spawn(
                        async move {
                            info!("forwarding connection started");
                            match this.handle_connection(id).await {
                                Ok(_) => info!("forwarding connection closed"),
                                Err(err) => warn!(%err, "forwarding connection failed"),
                            }
                        }
                        .instrument(info_span!("proxy", %id)),
                    );
                }
                Some(ServerMessage::Error(err)) => bail!("server error: {err}"),
                None => return Ok(()),
            }
        }
    }

    async fn connect_control(&self) -> Result<Delimited<TcpStream>> {
        timeout(CONNECTION_ATTEMPT_TIMEOUT, async {
            let mut stream =
                Delimited::new(connect_with_timeout(&self.to, self.control_port).await?);
            if let Some(auth) = &self.auth {
                auth.client_handshake(&mut stream).await?;
            }
            let assigned = self.remote_port.load(Ordering::Relaxed);
            let requested = if self.requested_port == 0 {
                assigned
            } else {
                self.requested_port
            };
            stream.send(ClientMessage::Hello(requested)).await?;
            match stream.recv_timeout().await? {
                Some(ServerMessage::Hello(port)) => {
                    if assigned != 0 && port != assigned {
                        bail!("server assigned unexpected remote port {port}");
                    }
                    self.remote_port.store(port, Ordering::Relaxed);
                    Ok(stream)
                }
                Some(ServerMessage::Error(message)) => bail!("server error: {message}"),
                Some(_) => bail!("unexpected initial non-hello message"),
                None => bail!("unexpected EOF"),
            }
        })
        .await
        .context("control connection attempt timed out")?
    }

    async fn handle_probe(&self, id: Uuid) -> Result<()> {
        let mut remote_conn =
            Delimited::new(connect_with_timeout(&self.to[..], self.control_port).await?);
        if let Some(auth) = &self.auth {
            auth.client_handshake(&mut remote_conn).await?;
        }
        let _backend = connect_with_timeout(&self.local_host, self.local_port).await?;
        remote_conn.send(ClientMessage::Accept(id)).await?;
        debug!(%id, "tunnel probe completed");
        Ok(())
    }

    async fn handle_connection(&self, id: Uuid) -> Result<()> {
        let mut remote_conn =
            Delimited::new(connect_with_timeout(&self.to[..], self.control_port).await?);
        if let Some(auth) = &self.auth {
            auth.client_handshake(&mut remote_conn).await?;
        }
        let mut local_conn = connect_with_timeout(&self.local_host, self.local_port).await?;
        remote_conn.send(ClientMessage::Accept(id)).await?;
        let mut parts = remote_conn.into_parts();
        debug_assert!(parts.write_buf.is_empty(), "framed write buffer not empty");
        local_conn.write_all(&parts.read_buf).await?; // mostly of the cases, this will be empty
        match tokio::io::copy_bidirectional(&mut local_conn, &mut parts.io).await {
            Ok((backend_to_proxy, proxy_to_backend)) => info!(
                %id,
                backend_to_proxy,
                proxy_to_backend,
                "forwarding streams closed"
            ),
            Err(err) if is_peer_reset(&err) => {
                info!(%id, %err, "forwarding stream closed by peer")
            }
            Err(err) => return Err(err.into()),
        }
        Ok(())
    }
}

async fn connect_with_timeout(to: &str, port: u16) -> Result<TcpStream> {
    match timeout(CONNECT_TIMEOUT, TcpStream::connect((to, port))).await {
        Ok(res) => res,
        Err(err) => Err(err.into()),
    }
    .with_context(|| format!("could not connect to {to}:{port}"))
}

fn reconnect_delay(failed_attempts: usize) -> Duration {
    let base = RECONNECT_DELAYS_MS[failed_attempts.min(RECONNECT_DELAYS_MS.len() - 1)];
    let jitter = fastrand::u64(0..=(base * 2 / 5));
    Duration::from_millis(base * 4 / 5 + jitter)
}

fn is_peer_reset(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
    )
}

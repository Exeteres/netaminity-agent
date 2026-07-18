//! Client implementation for the `bore` service.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};
use tracing::{error, info, info_span, warn, Instrument};
use uuid::Uuid;

use crate::auth::Authenticator;
use crate::health::{serve as serve_health, HealthState};
use crate::shared::{
    ClientMessage, Delimited, ServerMessage, HEALTH_CHECK_INTERVAL, NETWORK_TIMEOUT,
};

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
    remote_port: u16,

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
            remote_port,
            auth,
            control_port,
            reliable: false,
            health_addr: None,
            health: HealthState::new(),
        })
    }

    /// Enable reliability checks and restart behavior.
    pub fn set_reliable(&mut self, reliable: bool) {
        self.reliable = reliable;
    }

    /// Set the address serving HTTP health endpoints.
    pub fn set_health_addr(&mut self, health_addr: SocketAddr) {
        self.health_addr = Some(health_addr);
    }

    /// Returns the port publicly available on the remote.
    pub fn remote_port(&self) -> u16 {
        self.remote_port
    }

    /// Start the client, listening for new connections.
    pub async fn listen(mut self) -> Result<()> {
        self.health.set_control_connected(true);
        info!(
            proxy_host = %self.to,
            proxy_port = self.control_port,
            backend_host = %self.local_host,
            backend_port = self.local_port,
            reliable = self.reliable,
            "target control session established"
        );
        if let Some(addr) = self.health_addr {
            let health = Arc::clone(&self.health);
            tokio::spawn(async move {
                if let Err(err) = serve_health(addr, "target", health).await {
                    error!(%err, "health server exited");
                }
            });
        }
        let mut conn = self.conn.take().unwrap();
        let this = Arc::new(self);
        if this.reliable {
            let this = Arc::clone(&this);
            tokio::spawn(async move {
                let mut checks = tokio::time::interval(HEALTH_CHECK_INTERVAL);
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
        loop {
            let message = match conn.recv().await {
                Ok(message) => message,
                Err(err) => {
                    this.health.set_control_connected(false);
                    if this.reliable {
                        this.health.fail_liveness();
                    }
                    error!(%err, reliable = this.reliable, "target control session failed");
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
                        this.health.backend_reachable(),
                    ))
                    .await?;
                }
                Some(ServerMessage::Restart) => {
                    this.health.fail_liveness();
                    error!("proxy requested coordinated restart");
                    bail!("server requested restart after tunnel integrity failure");
                }
                Some(ServerMessage::Connection(id)) => {
                    let this = Arc::clone(&this);
                    tokio::spawn(
                        async move {
                            info!("new connection");
                            match this.handle_connection(id).await {
                                Ok(_) => info!("connection exited"),
                                Err(err) => warn!(%err, "connection exited with error"),
                            }
                        }
                        .instrument(info_span!("proxy", %id)),
                    );
                }
                Some(ServerMessage::Error(err)) => error!(%err, "server error"),
                None => {
                    this.health.set_control_connected(false);
                    if this.reliable {
                        this.health.fail_liveness();
                        error!("target control session closed; failing liveness");
                        bail!("control connection closed");
                    }
                    info!("target control session closed");
                    return Ok(());
                }
            }
        }
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
        tokio::io::copy_bidirectional(&mut local_conn, &mut parts.io).await?;
        Ok(())
    }
}

async fn connect_with_timeout(to: &str, port: u16) -> Result<TcpStream> {
    match timeout(NETWORK_TIMEOUT, TcpStream::connect((to, port))).await {
        Ok(res) => res,
        Err(err) => Err(err.into()),
    }
    .with_context(|| format!("could not connect to {to}:{port}"))
}

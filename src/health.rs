//! HTTP health endpoints and shared tunnel health state.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::info;

const HEALTHY: u8 = 1;
const UNHEALTHY: u8 = 2;

/// Health state shared by the tunnel and HTTP health server.
#[derive(Default)]
pub struct HealthState {
    live: AtomicBool,
    control_connected: AtomicBool,
    backend: AtomicU8,
    tunnel_verified: AtomicBool,
}

impl HealthState {
    /// Create health state for a running process.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            live: AtomicBool::new(true),
            ..Self::default()
        })
    }

    /// Set whether the control session is connected.
    pub fn set_control_connected(&self, connected: bool) {
        self.control_connected.store(connected, Ordering::Relaxed);
        if !connected {
            self.tunnel_verified.store(false, Ordering::Relaxed);
        }
    }

    /// Set whether the target backend is reachable.
    pub fn set_backend_reachable(&self, reachable: bool) {
        self.backend.store(
            if reachable { HEALTHY } else { UNHEALTHY },
            Ordering::Relaxed,
        );
        if !reachable {
            self.tunnel_verified.store(false, Ordering::Relaxed);
        }
    }

    /// Return whether the latest backend check succeeded.
    pub fn backend_reachable(&self) -> bool {
        self.backend.load(Ordering::Relaxed) == HEALTHY
    }

    /// Set whether the complete tunnel path has been verified.
    pub fn set_tunnel_verified(&self, verified: bool) {
        self.tunnel_verified.store(verified, Ordering::Relaxed);
    }

    /// Mark the process unhealthy enough to restart.
    pub fn fail_liveness(&self) {
        self.live.store(false, Ordering::Relaxed);
    }

    /// Return whether the process should remain running.
    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Relaxed)
    }

    /// Return target readiness.
    pub fn target_ready(&self) -> bool {
        self.control_connected.load(Ordering::Relaxed)
            && self.backend.load(Ordering::Relaxed) == HEALTHY
    }

    /// Return proxy readiness.
    pub fn proxy_ready(&self) -> bool {
        self.target_ready() && self.tunnel_verified.load(Ordering::Relaxed)
    }

    fn status(&self, role: &str) -> String {
        let backend = match self.backend.load(Ordering::Relaxed) {
            HEALTHY => "healthy",
            UNHEALTHY => "unhealthy",
            _ => "unknown",
        };
        format!(
            "{{\"role\":\"{role}\",\"live\":{},\"controlConnected\":{},\"backend\":\"{backend}\",\"tunnelVerified\":{}}}\n",
            self.is_live(),
            self.control_connected.load(Ordering::Relaxed),
            self.tunnel_verified.load(Ordering::Relaxed),
        )
    }
}

/// Serve liveness, readiness, and status over HTTP.
pub async fn serve(addr: SocketAddr, role: &'static str, state: Arc<HealthState>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%role, addr = ?listener.local_addr()?, "health server listening");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut request = [0; 1024];
            let Ok(length) = stream.read(&mut request).await else {
                return;
            };
            let request = String::from_utf8_lossy(&request[..length]);
            let path = request.split_whitespace().nth(1).unwrap_or("/");
            let (status, body) = match path {
                "/live" if state.is_live() => ("200 OK", "ok\n".to_string()),
                "/live" => ("503 Service Unavailable", "failed\n".to_string()),
                "/ready"
                    if match role {
                        "proxy" => state.proxy_ready(),
                        _ => state.target_ready(),
                    } =>
                {
                    ("200 OK", "ready\n".to_string())
                }
                "/ready" => ("503 Service Unavailable", "not ready\n".to_string()),
                "/status" => ("200 OK", state.status(role)),
                _ => ("404 Not Found", "not found\n".to_string()),
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::HealthState;

    #[test]
    fn backend_failure_is_not_a_liveness_failure() {
        let health = HealthState::new();
        health.set_control_connected(true);
        health.set_backend_reachable(false);

        assert!(health.is_live());
        assert!(!health.target_ready());
        assert!(!health.proxy_ready());
    }

    #[test]
    fn proxy_requires_verified_tunnel() {
        let health = HealthState::new();
        health.set_control_connected(true);
        health.set_backend_reachable(true);

        assert!(health.target_ready());
        assert!(!health.proxy_ready());

        health.set_tunnel_verified(true);
        assert!(health.proxy_ready());
    }

    #[test]
    fn tunnel_failure_fails_liveness() {
        let health = HealthState::new();
        health.set_control_connected(true);
        health.set_backend_reachable(true);
        health.set_tunnel_verified(true);
        health.fail_liveness();

        assert!(!health.is_live());
    }
}

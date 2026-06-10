//! Optional HTTP health endpoints for cluster deployment.
//!
//! The proxy's primary transport is MCP JSON-RPC over stdio (for IDE clients).
//! When deployed as a long-lived process (Kubernetes, systemd, etc.) we also
//! want kubelet-style liveness/readiness probes — those run over HTTP.
//!
//! `/healthz` is unconditionally 200 (the process is alive — stdio is being
//! consumed). `/readyz` flips to 200 only after the first successful skill-
//! mesh refresh, so traffic isn't routed at us during cold-start discovery.

use axum::{extract::State, http::StatusCode, routing::get, Router};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Notify;

/// Where the health server listens.
///
/// Default is loopback so a laptop using `--health-port` for local kubelet-
/// style probes doesn't expose /healthz on the LAN. K8s pods set
/// `--health-bind-all` so kubelet can reach the probe via the pod IP.
#[derive(Debug, Clone)]
pub enum HealthBind {
    Loopback,
    All,
}

impl HealthBind {
    fn addr(&self, port: u16) -> SocketAddr {
        match self {
            HealthBind::Loopback => SocketAddr::from(([127, 0, 0, 1], port)),
            HealthBind::All => SocketAddr::from(([0, 0, 0, 0], port)),
        }
    }
}

#[derive(Clone)]
pub struct HealthState {
    ready: Arc<std::sync::atomic::AtomicBool>,
    shutdown: Arc<Notify>,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Wake every waiter on the shutdown notify. Today there's only one
    /// (the axum graceful-shutdown future inside `serve`), but `notify_waiters`
    /// keeps the channel correct if a future refactor adds a second consumer.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    fn is_ready(&self) -> bool {
        self.ready.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn serve(port: u16, bind: HealthBind, state: HealthState) -> anyhow::Result<()> {
    let addr = bind.addr(port);
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state.clone());

    tracing::info!(%addr, "health endpoints listening on /healthz and /readyz");
    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { state.shutdown.notified().await })
        .await?;
    Ok(())
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn readyz(State(state): State<HealthState>) -> StatusCode {
    if state.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn readyz_flips_after_mark_ready() {
        let state = HealthState::new();
        assert_eq!(
            readyz(State(state.clone())).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        state.mark_ready();
        assert_eq!(readyz(State(state.clone())).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_always_ok() {
        assert_eq!(healthz().await, StatusCode::OK);
    }

    #[test]
    fn bind_loopback_uses_127_0_0_1() {
        let addr = HealthBind::Loopback.addr(8080);
        assert_eq!(addr.ip(), std::net::IpAddr::from([127, 0, 0, 1]));
    }

    #[test]
    fn bind_all_uses_0_0_0_0() {
        let addr = HealthBind::All.addr(8080);
        assert_eq!(addr.ip(), std::net::IpAddr::from([0, 0, 0, 0]));
    }
}

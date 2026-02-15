//! HTTP Health endpoints for Kubernetes/container orchestration
//!
//! Provides three endpoints per Architecture Section 10.6:
//! - `/health/live` - Liveness probe (process is running)
//! - `/health/ready` - Readiness probe (ready to accept traffic)
//! - `/health/startup` - Startup probe (initialization complete)

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{debug, error, info, trace};

/// Health state shared across the server
#[derive(Debug)]
pub struct HealthState {
    /// Server has completed startup initialization
    startup_complete: AtomicBool,
    /// Server is ready to accept traffic
    ready: AtomicBool,
    /// Server is alive (always true while running)
    alive: AtomicBool,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            startup_complete: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            alive: AtomicBool::new(true),
        }
    }

    /// Mark startup as complete
    #[allow(dead_code)]
    pub fn set_startup_complete(&self) {
        self.startup_complete.store(true, Ordering::Release);
        info!(target: "lance::health", "Startup complete");
    }

    /// Mark server as ready to accept traffic
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
        if ready {
            info!(target: "lance::health", "Server ready");
        } else {
            info!(target: "lance::health", "Server not ready");
        }
    }

    /// Mark server as not alive (during shutdown)
    pub fn set_not_alive(&self) {
        self.alive.store(false, Ordering::Release);
    }

    #[must_use]
    pub fn is_startup_complete(&self) -> bool {
        self.startup_complete.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

/// Response body type
type BoxBody = Full<Bytes>;

fn json_response(status: StatusCode, body: &str) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap_or_else(|_| {
            // SAFETY: This response is static and will always build successfully
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from("{\"status\":\"error\"}")))
                .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
        })
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<HealthState>,
) -> Result<Response<BoxBody>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/health/live") => {
            if state.is_alive() {
                json_response(StatusCode::OK, r#"{"status":"alive"}"#)
            } else {
                json_response(StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"not_alive"}"#)
            }
        },
        (&Method::GET, "/health/ready") => {
            if state.is_ready() {
                json_response(StatusCode::OK, r#"{"status":"ready"}"#)
            } else {
                json_response(StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"not_ready"}"#)
            }
        },
        (&Method::GET, "/health/startup") => {
            if state.is_startup_complete() {
                json_response(StatusCode::OK, r#"{"status":"started"}"#)
            } else {
                json_response(StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"starting"}"#)
            }
        },
        (&Method::GET, "/health") => {
            let alive = state.is_alive();
            let ready = state.is_ready();
            let startup = state.is_startup_complete();
            let status = if alive && ready && startup {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            let body = format!(
                r#"{{"alive":{},"ready":{},"startup_complete":{}}}"#,
                alive, ready, startup
            );
            json_response(status, &body)
        },
        (&Method::GET, "/metrics") => {
            // Trigger metrics export to Prometheus registry
            lnc_metrics::export_to_prometheus();
            // Note: Actual Prometheus metrics are served on the metrics_addr port
            json_response(
                StatusCode::OK,
                r#"{"status":"metrics_exported","note":"Use metrics_addr port for Prometheus scraping"}"#,
            )
        },
        // Admin API endpoints
        (&Method::GET, "/admin/info") => {
            let body = format!(
                r#"{{"version":"{}","build":"{}","uptime_secs":{}}}"#,
                env!("CARGO_PKG_VERSION"),
                option_env!("BUILD_SHA").unwrap_or("dev"),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            );
            json_response(StatusCode::OK, &body)
        },
        (&Method::GET, "/admin/config") => {
            // Return non-sensitive configuration info
            json_response(
                StatusCode::OK,
                r#"{"status":"ok","note":"Configuration available via CLI"}"#,
            )
        },
        (&Method::POST, "/admin/ready") => {
            // Toggle ready state (for maintenance)
            state.set_ready(true);
            json_response(StatusCode::OK, r#"{"status":"ready","action":"enabled"}"#)
        },
        (&Method::DELETE, "/admin/ready") => {
            // Set not ready (for maintenance)
            state.set_ready(false);
            json_response(
                StatusCode::OK,
                r#"{"status":"not_ready","action":"disabled"}"#,
            )
        },
        _ => json_response(StatusCode::NOT_FOUND, r#"{"error":"not_found"}"#),
    };

    // Log /health/live and /health/ready at trace level to reduce noise from k8s probes
    let path = req.uri().path();
    if path == "/health/live" || path == "/health/ready" {
        trace!(
            target: "lance::health",
            method = %req.method(),
            path = %path,
            status = %response.status(),
            "Health check"
        );
    } else {
        debug!(
            target: "lance::health",
            method = %req.method(),
            path = %path,
            status = %response.status(),
            "Health check"
        );
    }

    Ok(response)
}

/// Run the health HTTP server
pub async fn run_health_server(
    addr: SocketAddr,
    state: Arc<HealthState>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;

    info!(
        target: "lance::health",
        addr = %addr,
        "Health server listening"
    );

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _peer)) => {
                        let io = TokioIo::new(stream);
                        let state = Arc::clone(&state);

                        tokio::spawn(async move {
                            let service = service_fn(move |req| {
                                let state = Arc::clone(&state);
                                async move { handle_request(req, state).await }
                            });

                            if let Err(e) = http1::Builder::new()
                                .serve_connection(io, service)
                                .await
                            {
                                debug!(target: "lance::health", error = %e, "Health connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(target: "lance::health", error = %e, "Health accept failed");
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!(target: "lance::health", "Health server shutting down");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_health_state_defaults() {
        let state = HealthState::new();
        assert!(state.is_alive());
        assert!(!state.is_ready());
        assert!(!state.is_startup_complete());
    }

    #[test]
    fn test_health_state_transitions() {
        let state = HealthState::new();

        state.set_startup_complete();
        assert!(state.is_startup_complete());

        state.set_ready(true);
        assert!(state.is_ready());

        state.set_ready(false);
        assert!(!state.is_ready());

        state.set_not_alive();
        assert!(!state.is_alive());
    }
}

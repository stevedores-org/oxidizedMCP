//! Outbound-request authentication for skill mesh HTTP calls.
//!
//! When the mesh is talking to skill backends behind a cluster-managed
//! Gateway (GKE Gateway terminating ID-token-authenticated requests), every
//! `tools/list` and `tools/call` needs an `Authorization: Bearer <id-token>`
//! header. This module is the auth plug-in point.
//!
//! The Azure registry-fetch flow in `registry.rs` is a separate, pre-existing
//! code path; it stays as-is for now (production AKS still uses it during the
//! GKE-only migration). When that migration completes, `registry.rs` can be
//! refactored to use this same `Authenticator` and the Azure-specific code
//! can be removed.

use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::debug;

/// gcloud-issued identity tokens default to a 1-hour lifetime. Refresh a
/// little early so a request in flight when expiry hits doesn't fail.
const GCLOUD_ID_TOKEN_TTL: Duration = Duration::from_secs(55 * 60);

/// How the mesh authenticates outbound HTTP requests.
#[derive(Debug, Clone)]
pub enum AuthMode {
    /// Send no Authorization header. The default; safe for local / unauthenticated skills.
    None,

    /// Shell out to `gcloud auth print-identity-token`. Uses the developer's
    /// existing `gcloud auth login` session — zero secrets on disk. Suitable
    /// for IAP-protected services and GKE Gateway listeners that validate
    /// Google ID tokens.
    ///
    /// `audience` sets the `--audiences` flag (the `aud` claim). When `None`,
    /// gcloud's default audience is used.
    GcloudIdentity { audience: Option<String> },
}

impl AuthMode {
    /// Resolve an [`AuthMode`] from `OXIDIZED_MCP_AUTH_MODE` (+ optional
    /// `OXIDIZED_MCP_AUTH_AUDIENCE`). Unrecognised values fall back to
    /// [`AuthMode::None`].
    pub fn from_env() -> Self {
        Self::resolve(
            std::env::var("OXIDIZED_MCP_AUTH_MODE").ok().as_deref(),
            std::env::var("OXIDIZED_MCP_AUTH_AUDIENCE").ok().as_deref(),
        )
    }

    /// Pure parser — the part `from_env` delegates to once it's read the
    /// environment. Split out so tests don't fight over global env state.
    pub fn resolve(mode: Option<&str>, audience: Option<&str>) -> Self {
        let mode = mode.unwrap_or("").trim().to_ascii_lowercase();
        match mode.as_str() {
            "" | "none" => AuthMode::None,
            "gcloud-identity" | "gcp-identity" | "gcloud" => AuthMode::GcloudIdentity {
                audience: audience
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string()),
            },
            _ => AuthMode::None,
        }
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("failed to invoke `gcloud`: {0}")]
    GcloudInvoke(String),
    #[error("`gcloud auth print-identity-token` exited {code}: {stderr}")]
    GcloudExit { code: i32, stderr: String },
    #[error("gcloud token output was empty — run `gcloud auth login` and retry")]
    GcloudEmpty,
}

/// In-process token cache for [`AuthMode::GcloudIdentity`]. Hands out the
/// cached token while it's fresh; otherwise shells out for a new one.
///
/// Cheap to clone — the cache lives inside an `Arc<Mutex<_>>` so the
/// `SkillMesh` can hand a reference to background refresh loops + the
/// `tools/call` handler without per-request lock contention (gcloud is only
/// invoked on cache miss, which happens at most once per 55 minutes).
#[derive(Debug, Clone)]
pub struct Authenticator {
    mode: AuthMode,
    gcloud_binary: String,
    inner: Arc<Mutex<TokenState>>,
}

#[derive(Debug, Default)]
struct TokenState {
    token: Option<String>,
    fetched_at: Option<Instant>,
}

impl Authenticator {
    pub fn new(mode: AuthMode) -> Self {
        Self::with_gcloud_binary(mode, "gcloud")
    }

    /// Construct with an explicit gcloud binary path. Useful for tests that
    /// need to shadow gcloud without mutating PATH (which races with parallel
    /// `cargo test` jobs).
    pub fn with_gcloud_binary(mode: AuthMode, gcloud_binary: impl Into<String>) -> Self {
        Self {
            mode,
            gcloud_binary: gcloud_binary.into(),
            inner: Arc::new(Mutex::new(TokenState::default())),
        }
    }

    pub fn mode(&self) -> &AuthMode {
        &self.mode
    }

    /// Returns `Some(token)` if the mode requires auth, `None` if it doesn't.
    /// Errors propagate up so the caller can decide whether to retry, log, or
    /// fail-open (failing open on outbound auth is almost always wrong; the
    /// router treats `Err(_)` as a hard failure).
    pub async fn bearer_token(&self) -> Result<Option<String>, AuthError> {
        match &self.mode {
            AuthMode::None => Ok(None),
            AuthMode::GcloudIdentity { audience } => {
                let mut state = self.inner.lock().await;
                let fresh = state
                    .fetched_at
                    .map(|t| t.elapsed() < GCLOUD_ID_TOKEN_TTL)
                    .unwrap_or(false);
                if fresh {
                    return Ok(state.token.clone());
                }
                let token =
                    fetch_gcloud_identity_token(&self.gcloud_binary, audience.as_deref()).await?;
                state.token = Some(token.clone());
                state.fetched_at = Some(Instant::now());
                debug!("refreshed gcloud identity token (cache TTL ~55m)");
                Ok(Some(token))
            }
        }
    }
}

async fn fetch_gcloud_identity_token(
    binary: &str,
    audience: Option<&str>,
) -> Result<String, AuthError> {
    let mut cmd = Command::new(binary);
    cmd.arg("auth").arg("print-identity-token");
    if let Some(aud) = audience {
        cmd.arg("--audiences").arg(aud);
    }
    let output = cmd
        .output()
        .await
        .map_err(|e| AuthError::GcloudInvoke(e.to_string()))?;

    if !output.status.success() {
        return Err(AuthError::GcloudExit {
            code: output.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }

    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return Err(AuthError::GcloudEmpty);
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn none_mode_returns_no_token() {
        let auth = Authenticator::new(AuthMode::None);
        assert!(auth.bearer_token().await.unwrap().is_none());
    }

    #[test]
    fn resolve_defaults_to_none() {
        assert!(matches!(AuthMode::resolve(None, None), AuthMode::None));
        assert!(matches!(AuthMode::resolve(Some(""), None), AuthMode::None));
        assert!(matches!(AuthMode::resolve(Some("none"), None), AuthMode::None));
        assert!(
            matches!(AuthMode::resolve(Some("garbage"), None), AuthMode::None),
            "unknown values must fall back to None, not panic"
        );
    }

    #[test]
    fn resolve_recognises_gcloud_aliases() {
        for alias in ["gcloud-identity", "gcp-identity", "GCLOUD", "  Gcloud  "] {
            assert!(
                matches!(
                    AuthMode::resolve(Some(alias), None),
                    AuthMode::GcloudIdentity { audience: None }
                ),
                "alias '{alias}' should resolve to GcloudIdentity"
            );
        }
    }

    #[test]
    fn resolve_reads_audience() {
        match AuthMode::resolve(Some("gcloud-identity"), Some("https://gw.example.com")) {
            AuthMode::GcloudIdentity { audience } => {
                assert_eq!(audience.as_deref(), Some("https://gw.example.com"));
            }
            other => panic!("expected GcloudIdentity, got {:?}", other),
        }
        // Whitespace-only audience is treated as absent.
        match AuthMode::resolve(Some("gcloud-identity"), Some("   ")) {
            AuthMode::GcloudIdentity { audience } => assert!(audience.is_none()),
            other => panic!("expected GcloudIdentity, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn missing_gcloud_binary_surfaces_as_auth_error() {
        let auth = Authenticator::with_gcloud_binary(
            AuthMode::GcloudIdentity { audience: None },
            "/nonexistent/path/to/gcloud-binary",
        );
        let res = auth.bearer_token().await;
        assert!(res.is_err(), "expected AuthError when gcloud binary is absent");
        match res.unwrap_err() {
            AuthError::GcloudInvoke(_) => {}
            other => panic!("expected GcloudInvoke, got {other:?}"),
        }
    }
}

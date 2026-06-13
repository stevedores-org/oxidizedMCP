//! Outbound-request authentication for skill mesh HTTP calls.
//!
//! Two credential paths:
//! - [`Authenticator`] + [`AuthMode`] — GKE Gateway / gcloud identity tokens
//! - [`AzureAuthBroker`] — AKS hub access via `azure_identity` (Issue #5)

use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, warn};

// ── GKE / gcloud identity ───────────────────────────────────────────────

const GCLOUD_ID_TOKEN_TTL: Duration = Duration::from_secs(55 * 60);

#[derive(Debug, Clone)]
pub enum AuthMode {
    None,
    GcloudIdentity { audience: Option<String> },
}

impl AuthMode {
    pub fn from_env() -> Self {
        Self::resolve(
            std::env::var("OXIDIZED_MCP_AUTH_MODE").ok().as_deref(),
            std::env::var("OXIDIZED_MCP_AUTH_AUDIENCE").ok().as_deref(),
        )
    }

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

#[derive(Debug, Clone)]
pub struct Authenticator {
    mode: AuthMode,
    gcloud_binary: String,
    inner: Arc<Mutex<GcloudTokenState>>,
}

#[derive(Debug, Default)]
struct GcloudTokenState {
    token: Option<String>,
    fetched_at: Option<Instant>,
}

impl Authenticator {
    pub fn new(mode: AuthMode) -> Self {
        Self::with_gcloud_binary(mode, "gcloud")
    }

    pub fn with_gcloud_binary(mode: AuthMode, gcloud_binary: impl Into<String>) -> Self {
        Self {
            mode,
            gcloud_binary: gcloud_binary.into(),
            inner: Arc::new(Mutex::new(GcloudTokenState::default())),
        }
    }

    pub fn mode(&self) -> &AuthMode {
        &self.mode
    }

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

// ── Azure Entra ID (AKS hub) ────────────────────────────────────────────

use azure_core::credentials::TokenCredential;
use azure_identity::{DeveloperToolsCredential, WorkloadIdentityCredential};
use tokio::task::JoinHandle;

pub const AZ_LOGIN_HINT: &str = "Run `az login` to refresh your Azure CLI session, then retry.";

#[derive(Debug, Error)]
pub enum AzureAuthError {
    #[error("Azure authentication is not configured for this environment")]
    NotConfigured,
    #[error("Azure authentication failed: {0}. {AZ_LOGIN_HINT}")]
    TokenFetch(String),
    #[error("failed to initialize Azure credential: {0}. {AZ_LOGIN_HINT}")]
    CredentialInit(String),
}

#[derive(Clone)]
pub struct AzureAuthBroker {
    enabled: bool,
    scope: String,
    static_token: Option<String>,
    credential: Option<Arc<dyn TokenCredential>>,
}

impl std::fmt::Debug for AzureAuthBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureAuthBroker")
            .field("enabled", &self.enabled)
            .field("scope", &self.scope)
            .field(
                "static_token",
                &self.static_token.as_ref().map(|_| "<redacted>"),
            )
            .field("credential", &self.credential.is_some())
            .finish()
    }
}

impl AzureAuthBroker {
    pub fn from_env() -> Self {
        let enabled = Self::azure_auth_enabled();
        let scope = Self::resolve_scope();
        let static_token = Self::load_static_token();
        let credential = if static_token.is_none() && enabled {
            Self::try_build_credential().ok()
        } else {
            None
        };
        Self {
            enabled,
            scope,
            static_token,
            credential,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn should_authenticate(&self, url: &str) -> bool {
        url.starts_with("https://") && (self.enabled || self.static_token.is_some())
    }

    pub async fn bearer_token(&self) -> Result<String, AzureAuthError> {
        if let Some(ref token) = self.static_token {
            return Ok(token.clone());
        }
        if !self.enabled {
            return Err(AzureAuthError::NotConfigured);
        }
        let cred = self.credential.as_ref().ok_or_else(|| {
            AzureAuthError::CredentialInit("no Azure credential available".into())
        })?;
        let scope = self.scope.as_str();
        cred.get_token(&[scope], None)
            .await
            .map(|t| t.token.secret().to_string())
            .map_err(|e| AzureAuthError::TokenFetch(e.to_string()))
    }

    pub async fn authorization_header(&self) -> Result<String, AzureAuthError> {
        let token = self.bearer_token().await?;
        Ok(format!("Bearer {token}"))
    }

    pub fn user_facing_message(err: &AzureAuthError) -> String {
        err.to_string()
    }

    pub fn spawn_refresh_loop(self: Arc<Self>, interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            if !self.enabled || self.static_token.is_some() {
                return;
            }
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match self.bearer_token().await {
                    Ok(_) => debug!("azure auth token refreshed"),
                    Err(e) => warn!(error = %e, "azure auth token refresh failed"),
                }
            }
        })
    }

    fn azure_auth_enabled() -> bool {
        std::env::var("OXIDIZED_MCP_USE_AZURE_AD")
            .map(|v| v == "true")
            .unwrap_or(false)
            || std::env::var("OXIDIZED_MCP_ENV")
                .map(|v| matches!(v.as_str(), "staging" | "production"))
                .unwrap_or(false)
    }

    fn resolve_scope() -> String {
        let raw = std::env::var("OXIDIZED_MCP_AZURE_RESOURCE")
            .or_else(|_| std::env::var("OXIDIZED_MCP_AZURE_SCOPE"))
            .unwrap_or_else(|_| "https://management.azure.com/.default".to_string());
        if raw.contains("/.default") {
            raw
        } else {
            format!("{}/.default", raw.trim_end_matches('/'))
        }
    }

    fn load_static_token() -> Option<String> {
        if let Ok(token) = std::env::var("OXIDIZED_MCP_BEARER_TOKEN")
            .or_else(|_| std::env::var("LORNU_BEARER_TOKEN"))
        {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }

        if let Ok(token_file) = std::env::var("OXIDIZED_MCP_BEARER_TOKEN_FILE")
            .or_else(|_| std::env::var("LORNU_BEARER_TOKEN_FILE"))
        {
            if let Ok(content) = std::fs::read_to_string(token_file.trim()) {
                let trimmed = content.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }

        None
    }

    fn try_build_credential() -> Result<Arc<dyn TokenCredential>, AzureAuthError> {
        if std::env::var("AZURE_FEDERATED_TOKEN_FILE").is_ok() {
            return WorkloadIdentityCredential::new(None)
                .map(|c| c as Arc<dyn TokenCredential>)
                .map_err(|e| AzureAuthError::CredentialInit(e.to_string()));
        }

        DeveloperToolsCredential::new(None)
            .map(|c| c as Arc<dyn TokenCredential>)
            .map_err(|e| AzureAuthError::CredentialInit(e.to_string()))
    }
}

#[cfg(test)]
mod gcloud_tests {
    use super::*;

    #[tokio::test]
    async fn none_mode_returns_no_token() {
        let auth = Authenticator::new(AuthMode::None);
        assert!(auth.bearer_token().await.unwrap().is_none());
    }

    #[test]
    fn resolve_defaults_to_none() {
        assert!(matches!(AuthMode::resolve(None, None), AuthMode::None));
    }

    #[tokio::test]
    async fn missing_gcloud_binary_surfaces_as_auth_error() {
        let auth = Authenticator::with_gcloud_binary(
            AuthMode::GcloudIdentity { audience: None },
            "/nonexistent/path/to/gcloud-binary",
        );
        assert!(auth.bearer_token().await.is_err());
    }
}

#[cfg(test)]
mod azure_tests {
    use super::*;
    use crate::test_helpers::test_env::lock;

    #[test]
    fn resolves_scope_with_default_suffix() {
        let _guard = lock();
        std::env::set_var(
            "OXIDIZED_MCP_AZURE_RESOURCE",
            "https://management.azure.com",
        );
        assert_eq!(
            AzureAuthBroker::resolve_scope(),
            "https://management.azure.com/.default"
        );
        std::env::remove_var("OXIDIZED_MCP_AZURE_RESOURCE");
    }

    #[tokio::test]
    async fn static_token_returns_without_azure_login() {
        let _guard = lock();
        std::env::remove_var("OXIDIZED_MCP_BEARER_TOKEN");
        std::env::set_var("OXIDIZED_MCP_BEARER_TOKEN", "test-static-token");
        let broker = AzureAuthBroker::from_env();
        std::env::remove_var("OXIDIZED_MCP_BEARER_TOKEN");

        let token = broker.bearer_token().await.unwrap();
        assert_eq!(token, "test-static-token");
    }
}

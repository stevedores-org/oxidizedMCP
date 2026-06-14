//! Skill manifest loading from local files or remote registry URLs.

use crate::auth::{AzureAuthBroker, AzureAuthError};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("failed to read registry: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse registry: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("registry fetch failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("registry fetch returned status {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("no registry source configured")]
    NoSource,
    #[error("failed to retrieve Azure AD token: {0}")]
    AzureAuth(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillManifest {
    pub version: u32,
    #[serde(default)]
    pub environment: String,
    pub skills: Vec<SkillEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillEntry {
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// Base URL for the skill MCP HTTP endpoint (e.g. http://skill:8080/mcp)
    pub endpoint: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// OCI image reference for local Podman fallback when `endpoint` is
    /// unreachable. When set, the router checks `podman image exists <image>`
    /// and, if present, invokes the skill as `podman run -i --rm <image>`
    /// over the MCP stdio transport.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// Opt-in: the skill's `tools/call` endpoint speaks Server-Sent Events,
    /// emitting one JSON-RPC message per `data:` event (notifications first,
    /// a final JSON-RPC response last). The proxy relays each notification to
    /// the IDE's stdout while the upstream stream is still open, then forwards
    /// the terminal event as the final response with the IDE's request id.
    #[serde(default, skip_serializing_if = "is_false")]
    pub streaming: bool,
}

fn default_true() -> bool {
    true
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl SkillManifest {
    pub fn active_skills(&self) -> impl Iterator<Item = &SkillEntry> {
        self.skills.iter().filter(|s| s.enabled)
    }
}

pub struct RegistryLoader {
    client: reqwest::Client,
    auth: Arc<AzureAuthBroker>,
}

impl Default for RegistryLoader {
    fn default() -> Self {
        Self::new(Arc::new(AzureAuthBroker::from_env()))
    }
}

impl RegistryLoader {
    pub fn new(auth: Arc<AzureAuthBroker>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .expect("reqwest client"),
            auth,
        }
    }

    pub async fn load(&self, source: &RegistrySource) -> Result<SkillManifest, RegistryError> {
        match source {
            RegistrySource::File(path) => self.load_file(path),
            RegistrySource::Url(url) => self.load_url(url).await,
        }
    }

    pub fn load_file(&self, path: &Path) -> Result<SkillManifest, RegistryError> {
        let raw = std::fs::read_to_string(path)?;
        let manifest: SkillManifest = serde_yaml::from_str(&raw)?;
        Ok(manifest)
    }

    pub async fn load_url(&self, url: &str) -> Result<SkillManifest, RegistryError> {
        let mut req = self.client.get(url);

        if self.auth.should_authenticate(url) {
            match self.auth.authorization_header().await {
                Ok(header) => {
                    req = req.header("Authorization", header);
                }
                Err(e) => return Err(RegistryError::AzureAuth(auth_message(e))),
            }
        }

        let response = req.send().await?;
        if !response.status().is_success() {
            return Err(RegistryError::HttpStatus(response.status()));
        }
        let manifest: SkillManifest = response.json().await?;
        Ok(manifest)
    }
}

fn auth_message(err: AzureAuthError) -> String {
    AzureAuthBroker::user_facing_message(&err)
}

#[derive(Debug, Clone)]
pub enum RegistrySource {
    File(std::path::PathBuf),
    Url(String),
}

impl RegistrySource {
    pub fn resolve(
        explicit: Option<&Path>,
        env_url: Option<String>,
        default_file: Option<&Path>,
    ) -> Result<Self, RegistryError> {
        if let Some(path) = explicit {
            return Ok(RegistrySource::File(path.to_path_buf()));
        }
        if let Some(url) = env_url {
            return Ok(RegistrySource::Url(url));
        }
        if let Some(path) = default_file {
            if path.exists() {
                return Ok(RegistrySource::File(path.to_path_buf()));
            }
        }
        Err(RegistryError::NoSource)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_manifest_yaml() {
        let yaml = r#"
version: 1
environment: staging
skills:
  - name: echo
    description: Echo skill
    endpoint: http://127.0.0.1:9100/mcp
    enabled: true
"#;
        let manifest: SkillManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.skills.len(), 1);
        assert_eq!(manifest.skills[0].name, "echo");
        // Skills that don't declare `streaming:` default to the request/response
        // path — the field is opt-in for skills that emit SSE.
        assert!(!manifest.skills[0].streaming);
    }

    /// The optional `streaming: true` flag on a skill entry opts that skill
    /// into the SSE-aware `tools/call` path. Verifies parse + round-trip.
    #[test]
    fn parses_streaming_flag() {
        let yaml = r#"
version: 1
environment: staging
skills:
  - name: long-run
    description: Long-running skill that streams progress
    endpoint: http://127.0.0.1:9101/mcp
    streaming: true
"#;
        let manifest: SkillManifest = serde_yaml::from_str(yaml).unwrap();
        assert!(manifest.skills[0].streaming);

        // Round-trip via JSON: `streaming: false` is elided so the manifest
        // stays compact for the common case.
        let mut entry = manifest.skills[0].clone();
        entry.streaming = false;
        let json = serde_json::to_string(&entry).unwrap();
        assert!(
            !json.contains("\"streaming\""),
            "streaming=false must be elided, got: {json}"
        );
    }

    #[tokio::test]
    async fn load_url_with_static_bearer_token() {
        let loader = {
            let _guard = crate::test_helpers::test_env::lock();
            std::env::remove_var("OXIDIZED_MCP_BEARER_TOKEN");
            std::env::remove_var("LORNU_BEARER_TOKEN");
            std::env::set_var("OXIDIZED_MCP_BEARER_TOKEN", "test-registry-token");

            let loader = RegistryLoader::new(Arc::new(AzureAuthBroker::from_env()));
            std::env::remove_var("OXIDIZED_MCP_BEARER_TOKEN");
            loader
        };
        // Static token path is exercised; remote fetch may fail DNS — we only
        // verify auth wiring does not require az login when token is set.
        let res = loader.load_url("https://example.com/registry.json").await;

        assert!(res.is_err());
        match res.unwrap_err() {
            RegistryError::Http(_) | RegistryError::HttpStatus(_) => {}
            RegistryError::AzureAuth(msg) => panic!("unexpected auth error: {msg}"),
            other => panic!("expected HTTP error, got {:?}", other),
        }
    }
}

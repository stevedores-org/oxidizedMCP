//! Skill manifest loading from local files or remote registry URLs.

use serde::{Deserialize, Serialize};
use std::path::Path;
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
}

fn default_true() -> bool {
    true
}

impl SkillManifest {
    pub fn active_skills(&self) -> impl Iterator<Item = &SkillEntry> {
        self.skills.iter().filter(|s| s.enabled)
    }
}

pub struct RegistryLoader {
    client: reqwest::Client,
}

impl Default for RegistryLoader {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .expect("reqwest client"),
        }
    }
}

impl RegistryLoader {
    pub fn new() -> Self {
        Self::default()
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
        let use_azure = std::env::var("OXIDIZED_MCP_USE_AZURE_AD")
            .map(|v| v == "true")
            .unwrap_or(false)
            || std::env::var("OXIDIZED_MCP_ENV")
                .map(|v| v == "staging" || v == "production")
                .unwrap_or(false);

        let mut req = self.client.get(url);

        if use_azure && url.starts_with("https://") {
            match self.fetch_azure_token().await {
                Ok(token) => {
                    req = req.header("Authorization", format!("Bearer {token}"));
                }
                Err(e) => {
                    return Err(RegistryError::AzureAuth(e));
                }
            }
        }

        let response = req.send().await?;
        if !response.status().is_success() {
            return Err(RegistryError::HttpStatus(response.status()));
        }
        let manifest: SkillManifest = response.json().await?;
        Ok(manifest)
    }

    pub async fn fetch_azure_token(&self) -> Result<String, String> {
        // 1. Check if an explicit token is provided in env
        if let Ok(token) = std::env::var("OXIDIZED_MCP_BEARER_TOKEN")
            .or_else(|_| std::env::var("LORNU_BEARER_TOKEN"))
        {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }

        // 2. Check if a token file is provided in env
        if let Ok(token_file) = std::env::var("OXIDIZED_MCP_BEARER_TOKEN_FILE")
            .or_else(|_| std::env::var("LORNU_BEARER_TOKEN_FILE"))
        {
            if let Ok(content) = std::fs::read_to_string(token_file.trim()) {
                let trimmed = content.trim();
                if !trimmed.is_empty() {
                    return Ok(trimmed.to_string());
                }
            }
        }

        // 3. Try Azure Workload Identity / OIDC Federated Token Flow
        if let Ok(token_file) = std::env::var("AZURE_FEDERATED_TOKEN_FILE") {
            let federated_token = std::fs::read_to_string(token_file.trim())
                .map_err(|e| format!("failed to read federated token file: {e}"))?;
            let federated_token = federated_token.trim();

            let client_id = std::env::var("AZURE_CLIENT_ID").map_err(|_| {
                "AZURE_CLIENT_ID env var is required for workload identity".to_string()
            })?;
            let tenant_id = std::env::var("AZURE_TENANT_ID").map_err(|_| {
                "AZURE_TENANT_ID env var is required for workload identity".to_string()
            })?;
            let authority_host = std::env::var("AZURE_AUTHORITY_HOST")
                .unwrap_or_else(|_| "https://login.microsoftonline.com".to_string());

            let resource = std::env::var("OXIDIZED_MCP_AZURE_RESOURCE")
                .or_else(|_| std::env::var("OXIDIZED_MCP_AZURE_SCOPE"))
                .unwrap_or_else(|_| "https://management.azure.com/.default".to_string());

            let token_url = format!(
                "{}/{}/oauth2/v2.0/token",
                authority_host.trim_end_matches('/'),
                tenant_id
            );

            let params = [
                ("grant_type", "client_credentials"),
                ("client_id", &client_id),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:grant-type:jwt-bearer",
                ),
                ("client_assertion", federated_token),
                ("scope", &resource),
            ];

            let res = self
                .client
                .post(&token_url)
                .form(&params)
                .send()
                .await
                .map_err(|e| format!("failed to send token exchange request: {e}"))?;

            if !res.status().is_success() {
                let status = res.status();
                let body = res.text().await.unwrap_or_default();
                return Err(format!(
                    "token exchange failed with status {}: {}",
                    status, body
                ));
            }

            let val: serde_json::Value = res
                .json()
                .await
                .map_err(|e| format!("failed to parse token exchange JSON response: {e}"))?;

            let token = val
                .get("access_token")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "access_token missing from response".to_string())?;

            return Ok(token.to_string());
        }

        // 4. Fallback: CLI-based access token ('az account get-access-token')
        let resource = std::env::var("OXIDIZED_MCP_AZURE_RESOURCE").ok();
        let mut cmd = std::process::Command::new("az");
        cmd.args(["account", "get-access-token", "--output", "json"]);
        if let Some(ref res) = resource {
            cmd.args(["--resource", res]);
        }

        let output = cmd
            .output()
            .map_err(|e| format!("failed to run 'az': {e}"))?;
        if !output.status.success() {
            let err_msg = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(format!(
                "'az' command exited with error: {}",
                err_msg.trim()
            ));
        }

        let val: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| format!("failed to parse token JSON: {e}"))?;

        let token = val
            .get("accessToken")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "accessToken missing from JSON".to_string())?;

        Ok(token.to_string())
    }
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
    }

    #[tokio::test]
    async fn load_url_with_azure_ad_errors_on_invalid_resource() {
        std::env::set_var("OXIDIZED_MCP_USE_AZURE_AD", "true");
        std::env::set_var("OXIDIZED_MCP_AZURE_RESOURCE", "invalid-resource-id-12345");

        let loader = RegistryLoader::new();
        let res = loader.load_url("https://example.com/registry.json").await;

        std::env::remove_var("OXIDIZED_MCP_USE_AZURE_AD");
        std::env::remove_var("OXIDIZED_MCP_AZURE_RESOURCE");

        assert!(res.is_err());
        match res.unwrap_err() {
            RegistryError::AzureAuth(msg) => {
                assert!(msg.contains("exited with error") || msg.contains("failed to run"));
            }
            other => panic!("expected AzureAuth error, got {:?}", other),
        }
    }
}

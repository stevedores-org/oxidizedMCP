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
        let response = self.client.get(url).send().await?;
        if !response.status().is_success() {
            return Err(RegistryError::HttpStatus(response.status()));
        }
        let manifest: SkillManifest = response.json().await?;
        Ok(manifest)
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
}

//! oxidizedMCP core — registry, discovery, and skill routing.

pub mod auth;
pub mod local_runner;
pub mod mcp_types;
pub mod registry;
pub mod router;

#[cfg(test)]
mod test_helpers;

pub use auth::{
    AuthError, AuthMode, Authenticator, AzureAuthBroker, AzureAuthError, AZ_LOGIN_HINT,
};
pub use local_runner::{LocalRunError, PodmanRunner};
pub use mcp_types::{
    JsonRpcError, JsonRpcRequest, JsonRpcResponse, ToolCallParams, ToolCallResult,
    ToolContentBlock, ToolDescriptor, ToolsListResult, MCP_PROTOCOL_VERSION,
};
pub use registry::{RegistryError, RegistryLoader, RegistrySource, SkillEntry, SkillManifest};
pub use router::{
    namespaced_tool, parse_namespaced_tool, MeshError, SkillHealth, SkillMesh, SkillStatus,
    TOOL_NAMESPACE_SEP,
};

//! oxidizedMCP core — registry, discovery, and skill routing.

pub mod mcp_types;
pub mod registry;
pub mod router;

pub use mcp_types::{
    JsonRpcError, JsonRpcRequest, JsonRpcResponse, ToolCallParams, ToolCallResult,
    ToolContentBlock, ToolDescriptor, ToolsListResult, MCP_PROTOCOL_VERSION,
};
pub use registry::{RegistryError, RegistryLoader, RegistrySource, SkillEntry, SkillManifest};
pub use router::{namespaced_tool, parse_namespaced_tool, MeshError, SkillMesh, TOOL_NAMESPACE_SEP};

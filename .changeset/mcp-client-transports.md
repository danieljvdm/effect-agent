---
"@effect-agent/capabilities": minor
---

Connect agents to MCP servers over Streamable HTTP or stdio with `McpClient.layer`, `McpHttpTransport.make`, and `McpStdioTransport.make`. `connectMcp` now returns dynamic tools with a handler Layer that forwards `tools/call`, and `McpConnectionRequest.expectedToolkitSchemaDigest` rejects servers whose tools drifted.

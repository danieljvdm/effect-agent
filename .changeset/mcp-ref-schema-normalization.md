---
"effect-agent": patch
"@effect-agent/core": patch
"@effect-agent/engine": patch
"@effect-agent/capabilities": patch
"@effect-agent/sandbox": patch
"@effect-agent/sandbox-local": patch
"@effect-agent/session": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/pr-review": patch
"@effect-agent/testing": patch
---

Fix `validateMcpDiscovery` reporting a permanent schema drift for any MCP tool
whose parameters or success type is a named, refined Effect Schema (a common
pattern — a branded ID, a length-bounded string, a `Schema.Class`).

`Tool.getJsonSchema`/`Tool.getJsonSchemaFromSchema` hoist such a type into a
top-level `$ref` against a `$defs` map, but `McpSchema.Tool`'s `inputSchema`/
`outputSchema` can only ever be the flat `{ type: "object", ... }` shape a
real MCP server advertises on the wire — the exact shape effect 4.0.0-rc.110's
stricter `ToolJsonSchema` now enforces at `McpSchema.Tool` construction time.
`validateMcpDiscovery`'s own toolkit-side schema derivation used the
unflattened `$ref` form, so it could never match a real discovered server's
necessarily-flat schema for such a tool. Both derivations now resolve a single
top-level `$ref` before comparison, using effect's `JsonSchema.resolve$ref`.

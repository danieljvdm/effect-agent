---
"effect-agent": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/storage-sqlite": patch
"@effect-agent/storage-sql": patch
---

Remove obsolete runtime aliases, frozen context tools, and unused storage failpoint controls. Use indexed canonical reads for selected Thread records instead of decoding a cached full history.

BEHAVIOR CHANGE: Use `Subagent.make`, `ContextTools.toolkit` with `ContextTools.layer`, and the registered `runResolvedWorker` in place of `Subagent.define`, legacy context tools, and `runWorker`; classify delegation with `DelegationTool` metadata instead of name helpers. Replace the removed `DoStorageFailpointTestControl` and SQLite testing module with the corresponding storage failpoint service Layers.

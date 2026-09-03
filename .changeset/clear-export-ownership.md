---
"@effect-agent/capabilities": minor
"@effect-agent/core": minor
"@effect-agent/engine": minor
"effect-agent": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/platform-node": minor
"@effect-agent/platform-cloudflare": minor
"@effect-agent/sandbox": minor
"@effect-agent/sandbox-local": minor
"@effect-agent/pr-review": minor
"@effect-agent/testing": minor
"@effect-agent/thread": minor
"@effect-agent/workflow": minor
---

Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

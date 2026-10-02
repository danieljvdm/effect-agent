---
"@yielded/agent": minor
"@yielded/agent-storage-memory": minor
"@yielded/agent-storage-sqlite": minor
"@yielded/agent-storage-cloudflare": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-sandbox-local": minor
"@yielded/agent-pr-review": minor
"@yielded/agent-testing": minor
"@yielded/agent-workflow": minor
---

Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

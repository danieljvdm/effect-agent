---
"@effect-agent/core": patch
"@effect-agent/capabilities": patch
"@effect-agent/storage-memory": patch
"@effect-agent/storage-sqlite": patch
---

Define memory namespaces with branded Schema identities and retain their types through reads, writes, and semantic indexing. Use one canonical address for document, receipt, and index isolation.

BEHAVIOR CHANGE: Replace raw namespace strings with `MemoryNamespace.define(...).make(...)`, use `.Wire` Schemas at heterogeneous transport boundaries, and reset incompatible development memory and prepared processor data.

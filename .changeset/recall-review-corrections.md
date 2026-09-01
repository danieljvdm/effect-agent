---
"@effect-agent/capabilities": patch
"@effect-agent/engine": patch
---

Deduplicate equivalent recalled JSON metadata regardless of object member order. Load transient references after initial canonical compaction, then compact further when needed to fit the complete prompt.

---
"@effect-agent/engine": patch
"@effect-agent/thread": patch
"@effect-agent/capabilities": patch
"effect-agent": patch
---

Add bounded older-match pagination with optional `beforeRecordId` to context history search and explain literal queries in the native tool. BEHAVIOR CHANGE: update custom `ContextHistory` adapters to honor the exclusive canonical anchor or explicitly reject anchored requests before adopting the updated tool.

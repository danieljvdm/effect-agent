---
"@effect-agent/core": patch
"@effect-agent/engine": patch
"@effect-agent/thread": patch
"@effect-agent/capabilities": patch
"@effect-agent/platform-cloudflare": patch
---

Preserve Run limits across durable recovery, require explicit delegation replay authority, and reject unusable compaction summaries. Authorize settlement waits and aborts through the runtime authorizer and reject settlement Receipts whose Submission belongs to another Thread.

BEHAVIOR CHANGE: Reset private-development histories whose RunStarted records predate policy accounting version 1 before resuming them.

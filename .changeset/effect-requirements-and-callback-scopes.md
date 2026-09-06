---
"@effect-agent/capabilities": minor
"@effect-agent/engine": patch
"@effect-agent/thread": patch
"@effect-agent/platform-node": patch
"@effect-agent/platform-cloudflare": patch
"effect-agent": minor
---

Compose service-backed callbacks and reconciler Layers through Effect requirements, and finalize subscription and redaction resources per invocation. Preserve the caller's clock during late browser cleanup.

BEHAVIOR CHANGE: call `toRunThreadOptions(threadId, runId)` with `EphemeralThreads` provided; ephemeral runs now honor a provided `RunToolAuthorization` unless a per-run hook overrides it.

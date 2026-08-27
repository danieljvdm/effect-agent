---
"@effect-agent/platform-cloudflare": minor
"@effect-agent/storage-cloudflare": patch
"@effect-agent/engine": patch
---

Add opt-in native Conversation RPC tracing with binding/method client spans and transient current-span propagation. Remove routine storage codec, failpoint-wrapper, and engine identifier-helper spans while preserving validation, failures, and I/O tracing.

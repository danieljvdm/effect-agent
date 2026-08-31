---
"@effect-agent/platform-cloudflare": minor
"@effect-agent/storage-cloudflare": patch
"@effect-agent/engine": patch
---

Add opt-in native Thread RPC tracing with binding/method client spans, transient current-span propagation, and typed receiver invocation hooks. Remove routine storage codec, failpoint-wrapper, and engine identifier-helper spans while preserving validation, failures, and I/O tracing.

BEHAVIOR CHANGE: Upgrade the host's `effect-cf` dependency to `^0.34.0` for the native tracing contract.

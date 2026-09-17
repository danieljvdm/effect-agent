---
"effect-agent": patch
"@effect-agent/storage-cloudflare": patch
---

Preserve each invocation's tracing context and sampling decision when running registered attempts or preparing reports. Remove per-poll, digest, and response-part helper spans while retaining operation boundaries and errors.

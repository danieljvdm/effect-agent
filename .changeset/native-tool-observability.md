---
"effect-agent": patch
"@effect-agent/engine": patch
"@effect-agent/platform-cloudflare": patch
---

Export privacy-safe canonical Tool spans and bounded terminal logs from the engine, including
model-declared and programmatic broker calls, value-level failures, and delayed terminal event/
trace commit, while isolating complete span-lifecycle defects through Effect's error reporter.
Build Cloudflare Conversation Objects on `effect-cf`'s native `DurableObject.make` boundary so it
owns the cached runtime, event-scoped Layers, native RPC methods, `waitUntil`, and post-RPC OTLP
flush isolation. Upgrade to `effect-cf` 0.25.3 so the same upstream boundary flushes alarm
telemetry. Remove Effect Agent's duplicate telemetry service, flush coordinator, timeout
configuration, and lifecycle fixture matrix.

---
"@effect-agent/platform-cloudflare": patch
---

Retire alarm-owned host delivery and disposable projection work after finite dispatch opportunities while preserving native delivery timeout/retry commits and wake-driven overlap. BEHAVIOR CHANGE: Declare `ThreadHostMaintenance.dispatchTimeoutMillis`, keep admission listeners in the event Scope, and use the single `drainUntil(sourceFinished, dispatchUntil)` hook instead of the native `drain` fallback.

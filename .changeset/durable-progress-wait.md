---
"@effect-agent/thread": minor
"@effect-agent/platform-cloudflare": minor
---

Add the Effect-native durable progress wait from #94. Runtime and Cloudflare callers now subscribe
before an authoritative canonical read, wake from post-commit hints without polling, broadcast to
concurrent same-thread waiters, clean up on interruption, and reconnect safely after Durable
Object eviction. Cloudflare observation and resolution calls also preserve typed authorization
denials, and the client Layer now requires an explicit `Crypto.Crypto` provider for cancellation
identities.

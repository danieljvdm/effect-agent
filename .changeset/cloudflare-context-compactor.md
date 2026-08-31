---
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/platform-cloudflare": minor
"@effect-agent/thread": patch
---

Expose host-supplied model-context preparation through Cloudflare Thread Object options
(#49). A generic scoped `RunContextPreparation` service now composes after canonical durable
resume reconstruction, `contextCompactorRunContextLayer` adapts the digest-bound
`ContextCompactor` capability with typed failures, and `CloudflareDurableRuntimeOptions.runContext`
accepts a closed Layer or per-incarnation Layer factory. Compaction changes only model-visible
context; canonical history remains recoverable across Durable Object eviction and retries.

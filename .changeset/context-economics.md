---
"@effect-agent/core": minor
"@effect-agent/engine": minor
"@effect-agent/session": minor
"@effect-agent/capabilities": minor
"@effect-agent/testing": minor
"@effect-agent/pr-review": minor
---

Context economics (ADR-0018): application tool results are bounded by default (50 KiB
`TruncatedToolResult` envelopes), budget accounting becomes cache-aware with last-call
live-context tracking, every request can carry a derived run-status message, the token
dimension joins ADR-0019's `onExhaustion` soft landing with the `exhausted` dimension marker,
and the engine compacts natively at the pre-Turn seam (prune, then one metered summarize)
with a canonical `CompactionCreated` record that projections fold across Runs; provider
context-length rejections compact-and-retry once, then fail typed (`ContextOverflowError`).

---
"@effect-agent/core": minor
"@effect-agent/engine": minor
"@effect-agent/thread": minor
"@effect-agent/capabilities": minor
"@effect-agent/testing": minor
---

Context economics (#54, RUN-022–027/CAP-017): application tool results are bounded by default (50 KiB
`TruncatedToolResult` envelopes), budget accounting becomes cache-aware with last-call
live-context tracking, every request can carry a derived run-status message, the token
dimension joins the `onExhaustion` soft landing (RUN-018) with the `exhausted` dimension marker,
and the engine compacts natively at the pre-Turn seam (prune, then one metered summarize)
with a canonical `CompactionCreated` record that projections fold across Runs; provider
context-length rejections compact-and-retry once, then fail typed (`ContextOverflowError`).

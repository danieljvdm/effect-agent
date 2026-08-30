---
"@effect-agent/pr-review": patch
"@effect-agent/engine": patch
---

Warn against usable research tokens before the completion reserve blocks another call, and allow the completion tool to settle on the single final turn after turn exhaustion.

BEHAVIOR CHANGE: Allow reviews eight research turns plus a final response under a 1,440,000-token policy and 128 tool calls; difficult reviews can consume more tokens than before.

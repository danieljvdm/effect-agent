---
"@effect-agent/capabilities": patch
"@effect-agent/engine": patch
"@effect-agent/sandbox": patch
"@effect-agent/platform-cloudflare": patch
"@effect-agent/testing": patch
---

Allow authorized writes and bounded concurrent tool calls in Code Mode, with individual outcome reports after partial failure or interruption. Classify generated programs as uncertain to prevent automatic replay after ownership loss.

BEHAVIOR CHANGE: Host Tool authorization now checks inner calls; policies must allow the selected Tools explicitly.

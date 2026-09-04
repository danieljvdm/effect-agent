---
"@effect-agent/engine": patch
---

Enforce prepared-context limits and preserve protected history while reusing per-turn context estimates. Bound default summarizer requests to 80,000 characters and reject summaries above 65,536 characters.

BEHAVIOR CHANGE: Custom compaction strategies must emit summaries of at most 65,536 characters. Preparation hooks must preserve covered prefixes after compaction and retain a mappable instruction/input block for nondurable compaction.

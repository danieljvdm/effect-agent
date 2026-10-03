---
"effect-agent": patch
"@effect-agent/storage-sql": patch
---

Reduce nested runtime and SQL tracing overhead while preserving agent, model, tool, storage and recovery operation spans, attributes and typed failures.

BEHAVIOR CHANGE: If your trace filters target removed private helper spans, use their enclosing operation instead; selected helpers no longer create spans or Effect call frames.

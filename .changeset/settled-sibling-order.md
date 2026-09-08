---
"@effect-agent/thread": patch
---

Preserve earlier tool results when attached subagents suspend a later turn, and restore completed result batches in declaration order so context rollover can continue after recovery.

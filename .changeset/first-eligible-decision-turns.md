---
"effect-agent": minor
---

Add opt-in `DecisionTurn` registration to commit at most one decision per Run at its first eligible ordinary Turn and project an existing Tool call through native authorization, accounting, and recovery.

BEHAVIOR CHANGE: Recovery engine `@2` caches are invalidated by `@3` for all Runs, including those without a decision descriptor. The runtime rebuilds these disposable caches from canonical history without deleting that history.

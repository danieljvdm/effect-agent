---
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/session": patch
---

Replace compaction through `ContextCompactor` Layers, with a bounded default and configurable summary Model. Preserve metered usage, protected Tool pairs, and canonical coverage of the selected history.

BEHAVIOR CHANGE: migrate capabilities `ContextCompactor.compact(snapshot)` implementations to the engine request/decision contract; `contextCompactorRunContextLayer` now installs native compaction instead of a prompt hook.

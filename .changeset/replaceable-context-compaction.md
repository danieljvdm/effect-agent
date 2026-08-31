---
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/thread": patch
---

Replace compaction through `ContextCompactor` Layers, with a bounded default and configurable summary Model. Preserve metered usage, protected Tool pairs, and canonical coverage of the selected history.

BEHAVIOR CHANGE: migrate capabilities `ContextCompactor.compact(snapshot)` implementations to the engine request/decision contract; `contextCompactorRunContextLayer` now installs native compaction instead of a prompt hook.

BEHAVIOR CHANGE: durable compaction fails if its coverage cannot map to complete prior-Run records or its summary exceeds 65,536 characters. Empty or incomplete model summaries and whitespace-only custom summaries are rejected.

BEHAVIOR CHANGE: pressure compaction and overflow recovery share one prune, one summary, and one summary-model call per Turn.

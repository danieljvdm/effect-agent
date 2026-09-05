---
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/thread": minor
"effect-agent": minor
---

Inject compaction strategies directly through `ContextCompactor`, and supply Effect AI's `IdGenerator` when constructing `MemoryNotes.layer`. BEHAVIOR CHANGE: Replace `RunContextPreparation.compactor` and `contextCompactorRunContextLayer` with `Layer.provide(CompactorLive)` at the runtime Layer.

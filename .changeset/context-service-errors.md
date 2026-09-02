---
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/thread": minor
"@effect-agent/testing": patch
---

Provide optional context loading through `RunContextPreparation` for ephemeral and durable runs, and catch its concrete tagged errors directly.

BEHAVIOR CHANGE: replace `RunContextPreparationError.make` with a declared `AgentInputError`, `MemoryRecallError`, or `CompactionError`.

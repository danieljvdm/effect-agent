---
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/thread": minor
"@effect-agent/testing": patch
---

Provide context loading through `RunContextPreparation` for ephemeral and durable runs, and catch its concrete tagged errors directly.

BEHAVIOR CHANGE: provide a context Layer or `RunContextPreparationPassthrough` to ephemeral runs. Replace `RunContextPreparationError.make` with a declared `AgentInputError`, `MemoryRecallError`, or `CompactionError`.

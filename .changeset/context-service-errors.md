---
"@yielded/agent-testing": patch
"@yielded/agent": minor
---

Provide optional context loading through `RunContextPreparation` for ephemeral and durable runs, and catch its concrete tagged errors directly.

BEHAVIOR CHANGE: replace `RunContextPreparationError.make` with a declared `AgentInputError`, `MemoryRecallError`, or `CompactionError`.

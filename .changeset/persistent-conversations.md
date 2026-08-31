---
"@effect-agent/thread": minor
"@effect-agent/engine": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/testing": minor
---

Provide canonical thread history to `AgentRuntime.run`, `start`, and `stream` through `PersistentHistory.layer` without admitting durable work. Make checkpoint storage an optional `ThreadStore.checkpoints` capability.

BEHAVIOR CHANGE: Provide `ThreadHistory.layerTransient` for transient execution or `PersistentHistory.layer` with a ThreadStore for retained history. Use `store.checkpoints.save` and `store.checkpoints.load` after checking capability availability. `UserInputRecorded.submissionId` is present only for durably accepted input.

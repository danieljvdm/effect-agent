---
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/testing": minor
"effect-agent": minor
---

Provide canonical thread history to `AgentRuntime.run`, `start`, and `stream` through `PersistentHistory.layer` without admitting durable work. Make checkpoint storage an optional `ThreadStore.checkpoints` capability.

BEHAVIOR CHANGE: Provide `ThreadHistory.layer` for in-memory conversations or `PersistentHistory.layer` with a ThreadStore for atomic successful-Run retention. Use `store.checkpoints.save` and `store.checkpoints.load` after checking capability availability. `UserInputRecorded.submissionId` is present only for durably accepted input.

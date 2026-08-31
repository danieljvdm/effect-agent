---
"@effect-agent/session": minor
"@effect-agent/engine": minor
"@effect-agent/storage-memory": minor
"@effect-agent/storage-sqlite": minor
"@effect-agent/storage-cloudflare": minor
"@effect-agent/testing": minor
---

Run Agents with retained canonical history through `PersistentConversations` and the `session/history` subpath without admitting durable work. Make checkpoint storage an optional `ConversationStore.checkpoints` capability.

BEHAVIOR CHANGE: Use `store.checkpoints.save` and `store.checkpoints.load` after checking capability availability; adapters without checkpoint support need only the base store methods. `UserInputRecorded.submissionId` is present only for durably accepted input.

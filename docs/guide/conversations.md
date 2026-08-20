---
title: Conversations
description: Ephemeral interaction and persistent canonical history without a durability overclaim.
---

# Conversations

A Conversation is the ordered history shared across Runs. It is not a mutable Agent object and it
is not the same thing as a process Session, Submission, or model request.

## Process-local interaction

`@effect-agent/capabilities` provides bounded `EphemeralConversations`. The adapter gives the
engine an existing Conversation ID, exact Effect AI Prompt history, and an `onHistory` seam.

```ts
const runOptions = yield * toRunConversationOptions(conversations, conversationId, runId);

const result = yield * AgentRuntime.run(agent, input, runOptions);
```

The package also provides explicit command queues:

- **steering** is drained after a complete response and Tool batch, before the next model request;
- **follow-up** is drained only when the Agent would otherwise stop.

Neither can mutate in-flight work.

## Canonical history

`@effect-agent/session` defines versioned record and batch Schemas plus a pure Conversation reducer.
The Conversation Log is append-only. Projections and checkpoints are disposable derivatives.

The current canonical union records facts such as:

- Conversation creation and user input;
- completed model output;
- settled Tool Calls;
- compaction creation;
- Run failure and completion;
- repair annotations.

Partial Tool-argument deltas and live queue state are not canonical facts.

## Store contract

```ts
class ConversationStore extends Context.Service<ConversationStore, {
  readonly materialize: (request: ConversationMaterialization) => Effect<void, ...>
  readonly append: (request: FencedAppendRequest) => Effect<AppendResult, ...>
  readonly read: (request: ConversationRead) => Stream<CanonicalRecordEnvelope, ...>
  readonly observe: (request: ConversationObservation) => Stream<CanonicalRecordEnvelope, ...>
  readonly export: (request: ConversationExportRequest) => Effect<ConversationExport, ...>
  readonly saveCheckpoint: (request: SaveCheckpointRequest) => Effect<void, ...>
  readonly loadCheckpoint: (request: LoadCheckpointRequest) => Effect<Option<ConversationCheckpoint>, ...>
}>() {}
```

Appends are atomic, digest-bound, idempotent by batch ID, conflict-checked against the expected
tail, and fenced by producer epoch. Reads decode persisted values through Schema.

## Storage layers

Two adapters implement the same current contract:

| Package                        | Use                                       |
| ------------------------------ | ----------------------------------------- |
| `@effect-agent/storage-memory` | deterministic tests and local development |
| `@effect-agent/storage-sqlite` | restart-surviving Conversation history    |

The SQLite adapter supports only the current pre-1.0 schema. Incompatible data fails clearly and
may be reset; migrations are not yet promised.

Persistence is not durability: a persistent Conversation survives restart, but an active Run does
not, unless it was admitted through the durable runtime. Receipts, Attempt ownership, recovery,
and Settlement are covered in [Persistence & durability](../concepts/durability).

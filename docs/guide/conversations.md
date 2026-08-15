---
title: Conversations
description: Ephemeral interaction and persistent canonical history without a durability overclaim.
---

# Conversations

<StatusCallout status="available" phase="P2–P3" title="Ephemeral Conversations and persistent canonical records are implemented.">

Current deployment maturity is **P — Persistent**. Conversation state may survive restart, but an
active Run is not durably accepted and is not automatically recovered.

</StatusCallout>

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

## Storage Layers

Two adapters implement the same current contract:

| Package                        | Use                                       | Maturity       |
| ------------------------------ | ----------------------------------------- | -------------- |
| `@effect-agent/storage-memory` | deterministic tests and local development | process-local  |
| `@effect-agent/storage-sqlite` | restart-surviving Conversation history    | persistent `P` |

The SQLite adapter supports only the current private-development schema. Incompatible data fails
clearly and may be reset; migrations are not yet promised.

## The explicit non-claim

The current `SubmissionStore` says exactly what it can do:

```ts
{
  durability: "non-durable",
  acceptsDurableWork: false,
}
```

There is no Receipt, durable admission, Attempt ownership, recovery scheduler, or Settlement API in
Phase 3. Those interfaces are described in [Persistence & durability](../concepts/durability).

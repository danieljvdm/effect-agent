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

const result =
  yield *
  AgentRuntime.run(agent, input, runOptions).pipe(
    Effect.provide(ConversationHistory.layerTransient),
  );
```

The package also provides explicit command queues:

- **steering** is drained after a complete response and Tool batch, before the next model request;
- **follow-up** is drained only when the Agent would otherwise stop.

Neither can mutate in-flight work.

## Persistent interaction

Provide `PersistentHistory.layer` to the normal `AgentRuntime.run`, `start`, or `stream`
operation. The engine consumes the `ConversationHistory` service; the Layer captures a
`ConversationStore`. The same Agent can serve many Conversation IDs.

```ts
import { AgentRuntime, ConversationHistory } from "@effect-agent/engine";
import { PersistentHistory } from "@effect-agent/session/history";
import { layer as sqliteStore } from "@effect-agent/storage-sqlite";
import { Effect, Layer } from "effect";

const HistoryLive = PersistentHistory.layer.pipe(
  Layer.provide(sqliteStore({ filename: "./history.sqlite" })),
);

const program = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(agent, firstInput, { conversationId });
  const second = yield* AgentRuntime.run(agent, secondInput, { conversationId });
  const history = yield* ConversationHistory;
  const prompt = yield* history.load(conversationId);
  return { first, second, prompt };
}).pipe(Effect.provide(HistoryLive), Effect.scoped);
```

Supply `IdGenerator` and the Agent's other services at the application boundary. Provide the
history Layer around the whole program, including the lifetime of any `start` handle.
The [runnable SQLite example](https://github.com/danieljvdm/effect-agent/blob/main/examples/providers/src/history.ts)
supplies those Layers and runs without provider credentials:

```sh
vp run -F @effect-agent/example-providers history --database /tmp/effect-agent-history.sqlite seed
vp run -F @effect-agent/example-providers history --database /tmp/effect-agent-history.sqlite show
```

The commands run in separate processes. `show` loads the two-input history without an Agent or
model. Repeating `seed` appends two more Runs. A memory ConversationStore retains the same
canonical history only for its Layer's lifetime.

Each execution loads the current history, stages its encoded input and native messages, and
appends one atomic batch after success. The engine closes model resources before committing.
`run` and `start.await` validate their decoded result before commit. A `RunCompleted` event
becomes visible only after the commit succeeds, including through `start.observe` and `stream`.
Callers do not allocate producer epochs, batch IDs, or digests.

Retained history includes assistant messages, reasoning, provider options, evaluated instructions,
and settled Tool results. Context preparation and compaction change the model's current view;
the full source history remains retained. The engine supplies its already encoded input and
terminal output, so persistence does not repeat the Agent's Schema transformations.
`UserInputRecorded.submissionId` is absent for these Runs.

A failure, defect, timeout, or interruption before commit retains none of that Run. Existing
history remains available. A storage failure after commit can still leave the entire Run
recorded. Inspect history before retrying; model and Tool effects may already have happened.
No history implementation retries execution or resumes an interrupted Run.

The encoded input, output, and native Prompt suffix must each fit the existing 1 MiB canonical
value bound. Exports contain at most 65,536 canonical records. If the next Run's three records
would exceed that limit, execution fails before model or Tool calls. Start a new Conversation
to continue; the existing history remains loadable.

### History policy and append ownership

`ConversationHistory` remains explicit in the runtime's Effect or Stream requirements.
Use `ConversationHistory.layerTransient` for execution that retains no shared history. It also
leaves explicit `RunOptions.history` and `onHistory` hooks under their caller's ownership.
A persistent Layer rejects conflicting explicit history, input-queue, or durable recovery hooks.
Spawned children inherit their parent Run's history service and receive fresh Conversation IDs.

Persistent writers use epoch zero and compare the loaded tail sequence and digest at append.
Concurrent callers may both execute a model or Tool; a stale writer fails with
`ConversationHistoryError` whose `reason` is `"conflict"`. Serialize application calls when
concurrent external execution is unacceptable, and provide IDs that remain unique across restarts.

A newer producer epoch fails with reason `"fenced"`. Histories belonging to durable accepted work
fail with reason `"incompatible"`. Other history failures distinguish `"not-found"`, `"limit"`,
`"encoding"`, and `"storage"`; the original adapter failure remains the diagnostic cause.
Agent and hook failures keep their existing typed channels.

Use separate Conversation IDs for retained interaction and durable admission. The durable runtime
owns its journal and supplies transient history policy internally. It continues to provide the
accepted-work and recovery contract described by `@effect-agent/session/durability`.

Applications enforce tenant and Conversation access before execution. Engine Tool authorization
and tracing still apply. The history adapter adds no transcript content to telemetry.

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

Immediate history appends `UserInputRecorded`, `ModelCompleted`, and `RunCompleted` together.
The optional `ModelCompleted.messages` field carries the successful Run's exact native Prompt
suffix. Durable execution continues to use its per-Turn response and Tool records.

Partial Tool-argument deltas and live queue state are not canonical facts.

## Store contract

```ts
class ConversationStore extends Context.Service<ConversationStore, {
  readonly materialize: (request: ConversationMaterialization) => Effect<void, ...>
  readonly append: (request: FencedAppendRequest) => Effect<AppendResult, ...>
  readonly read: (request: ConversationRead) => Stream<CanonicalRecordEnvelope, ...>
  readonly observe: (request: ConversationObservation) => Stream<CanonicalRecordEnvelope, ...>
  readonly export: (request: ConversationExportRequest) => Effect<ConversationExport, ...>
  readonly inspectTail: (request: ConversationTailRequest) => Effect<ConversationTail, ...>
  readonly checkpoints?: ConversationCheckpoints
}>() {}
```

Appends are atomic, digest-bound, idempotent by batch ID, conflict-checked against the expected
tail, and fenced by producer epoch. Reads decode persisted values through Schema.

Checkpoint storage is optional. An adapter that supplies `store.checkpoints` exposes `save` and
`load` methods with `SaveCheckpointRequest` and `LoadCheckpointRequest`. The existing memory,
SQLite, and Cloudflare adapters retain this capability for explicit projection consumers such as
the Travel Planner fixture. Neither retained-history execution nor durable recovery uses it.
Administrative verification reports `checkpoint-binding` as skipped when support is absent,
passed when a supporting adapter has no checkpoint, and failed when the adapter rejects a stored
checkpoint. The optional checkpoint conformance suite is separate from the base store suite.

## Storage layers

These adapters implement the same base contract:

| Package                            | Use                                                       |
| ---------------------------------- | --------------------------------------------------------- |
| `@effect-agent/storage-memory`     | deterministic tests and local development                 |
| `@effect-agent/storage-sqlite`     | restart-surviving Conversation history                    |
| `@effect-agent/storage-cloudflare` | Durable Object SQLite history and routed store operations |

The SQLite adapter supports only the current pre-1.0 schema. Incompatible data fails clearly and
may be reset; migrations are not yet promised.

Persistence is not durability: a persistent Conversation survives restart, but an active Run does
not, unless it was admitted through the durable runtime. Receipts, Attempt ownership, recovery,
and Settlement are covered in [Persistence & durability](../concepts/durability).

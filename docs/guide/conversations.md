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

## Persistent interaction

Use `PersistentConversations` from `@effect-agent/session/history` to run an Agent against a
stored Conversation. Supply a `ConversationStore` and `IdGenerator`, plus the Agent's
own services. Each call loads the current history and appends one atomic batch after the Run
succeeds. Callers do not allocate producer epochs, batch IDs, digests, or Submissions.

```ts
import { PersistentConversations } from "@effect-agent/session/history";

const first = yield * PersistentConversations.run(agent, firstInput, { conversationId });
const second = yield * PersistentConversations.run(agent, secondInput, { conversationId });

// This also works in a new process with a store connected to the same database.
const prompt = yield * PersistentConversations.load(conversationId);
```

The [runnable SQLite example](https://github.com/danieljvdm/effect-agent/blob/main/examples/providers/src/history.ts)
supplies the Layers and runs without provider credentials:

```sh
vp run -F @effect-agent/example-providers history --database /tmp/effect-agent-history.sqlite seed
vp run -F @effect-agent/example-providers history --database /tmp/effect-agent-history.sqlite show
```

These commands execute in separate processes. The second reconstructs the two-input history
without a model. Repeating `seed` appends two more Runs.

Retained history carries the Schema-encoded input and output, native assistant messages,
reasoning, provider options, and settled Tool results. Evaluated instructions remain part of
the retained history, as with process-local Conversations. Context preparation and compaction affect the current
Run's model view; this composition retains the full source history and does not persist a
compaction decision. `UserInputRecorded.submissionId` is absent for these Runs.

The composition captures the engine's encoded input through `RunOptions.onInput` and its
encoded terminal event through the scoped Run handle. It does not repeat the Agent's input
decoder or output encoder to produce history.

An expected failure, defect, timeout, or interruption before append retains none of that Run.
Existing history remains available. The helper closes the Run's scoped resources before
appending, and a successful result means the append completed. A storage failure after commit
can still leave the entire Run recorded. Inspect the history before retrying; model and Tool
effects may already have happened. The helper never retries them automatically.

Each Run's encoded input, output, and native Prompt suffix must each fit the existing 1 MiB
canonical value bound. Exceeding a bound fails typed and appends no partial Run. History exports
contain at most 65,536 canonical records. If the next Run's three records would exceed that limit,
`run` fails with `PersistentConversationError` before model or Tool execution. Start a new
Conversation to continue; `load` still reads the retained history. `run` returns an
Effect with the Agent's errors and services preserved, plus history, journal, and store failures. `load`
returns the reconstructed Effect AI Prompt; observation remains a Stream on the store.

### Append ownership

History producers use epoch zero and compare the loaded tail sequence and digest at append.
Concurrent callers may both execute a model or Tool, but only one can append against that tail.
The other receives `AppendConflict`; it does not merge or replay its Run. Serialize calls in the
application when concurrent external execution is unacceptable. Provide an identity generator
that remains unique across restarts.

A newer producer epoch rejects history writes with `FenceRejected`. The helper also refuses
histories containing durable admission records. Use separate Conversation IDs for this API and
durable admission, including while work is being admitted. History persistence creates no
accepted-work obligation and does not promise to finish an interrupted Run. Use
`@effect-agent/session/durability` when that promise is required.

Access follows the supplied store's authority. Applications enforce tenant and Conversation
access before calling the helper. Engine Tool authorization and tracing still apply; the helper
adds no transcript content to telemetry.

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

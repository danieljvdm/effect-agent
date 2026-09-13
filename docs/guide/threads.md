---
title: Threads
description: Keep thread history across agent runs.
---

# Threads

A Thread is addressable ordered history shared across Runs. An Agent executes Runs within a Thread
and retains their history there. A Thread is separate from an Agent definition, process lifetime,
Submission, or model request.

## In-memory conversations

`Ephemeral.layer` is the default setup. It keeps conversation history in memory for the lifetime
of its application Scope. Runs with the same Thread ID load that conversation automatically:

```ts
import { AgentRuntime, Ephemeral } from "effect-agent";
import { Effect } from "effect";

const conversation = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(agent, "Plan a trip to Lisbon");
  return yield* AgentRuntime.run(agent, "Make it cheaper", { threadId: first.threadId });
}).pipe(Effect.provide(Ephemeral.layer));
```

Supply the model and tool handlers around this program. Provide the application Layer once
around all conversation Runs, or build one `ManagedRuntime` for a long-lived application.
Providing a fresh Layer separately to each Run creates separate stores. Omitting `threadId`
creates a new conversation; reuse the returned ID for follow-ups.

The layer shares one bounded `EphemeralThreads` store and the subagent reservation ledger.
`ThreadHistory.layer` supplies just the in-memory history services when assembling your own setup.
History retains complete native messages and Tool batches as execution advances. Recorded updates
remain after a later failure, defect, timeout, or interruption; incomplete streamed responses and
unfinished Tool batches are not recorded. Nothing automatically replays failed work. Scope closure
releases the store, and process loss loses both its history and any active execution.

The store permits 256 Threads, 1,024 messages and 4 MiB of encoded content per Thread, and 64 MiB
of encoded content overall. Exceeding a bound fails with `ThreadHistoryError` and reason `"limit"`;
it never silently evicts earlier conversations. Concurrent updates must extend the same recorded
prefix, or fail with reason `"conflict"` without overwriting history. Authorize thread access and
serialize same-thread Runs when concurrent external work is unacceptable.

## Retain completed runs

For history backed by an explicit store, use `PersistentHistory.layer`. It commits whole
successful Runs; its memory adapter is also useful when you need the canonical ThreadStore APIs.
Use SQLite when the history must survive a Node process restart.

Provide `PersistentHistory.layer` with a memory or SQLite `ThreadStore` layer. The same agent
can serve many thread IDs.

```ts
import { AgentRuntime } from "effect-agent";
import { ThreadHistory } from "effect-agent/thread-history";
import { PersistentHistory } from "@effect-agent/thread/persistent-history";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { SqliteThreadStore } from "@effect-agent/storage-sqlite";
import { Effect, Layer } from "effect";

const MemoryHistoryLive = PersistentHistory.layer.pipe(Layer.provide(MemoryThreadStoreLive));

const SqliteHistoryLive = PersistentHistory.layer.pipe(
  Layer.provide(SqliteThreadStore.layer({ filename: "./history.sqlite" })),
);

const HistoryLive = MemoryHistoryLive;

const program = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(agent, firstInput, { threadId });
  const second = yield* AgentRuntime.run(agent, secondInput, { threadId });
  const history = yield* ThreadHistory;
  const prompt = yield* history.load(threadId);
  return { first, second, prompt };
}).pipe(Effect.provide(HistoryLive));
```

Provide the history layer around the complete program, including any `start` handle. Also provide
the agent's model and tool services at the application boundary. Runtime IDs have an overridable
default and need no Layer.

Each successful execution appends its input and native messages as one atomic batch. The runtime
first closes run-owned resources, validates the result, and commits history. Only then does it
publish `RunCompleted`. Services from an enclosing application layer stay open for that layer's
lifetime.

Retained history includes evaluated instructions, assistant messages, reasoning, provider options,
and settled tool results. Context preparation and compaction change the current model view while
the source history stays intact.

A failure, defect, timeout, or interruption before commit retains none of the current run. A
storage error after commit can leave the whole run recorded, so inspect history before retrying.
The runtime never retries execution or resumes an interrupted run.

Each encoded input, output, and native Prompt suffix has a 1 MiB limit. The supplied adapters and
`ThreadExport` support up to 131,072 canonical records per Thread. If the next run would cross
that limit, execution fails before model or tool calls. Start a new thread to continue.

### Choose one history owner {#history-policy-and-append-ownership}

The default in-memory layer records incremental history and supports input queues and history
observers. `PersistentHistory.layer` owns an atomic successful-Run commit and rejects explicit `history`, `onHistory`, `input`, `durability`,
`subagent`, `resume`, and `resumeUsage` options before model or tool execution.

Persistent writers compare the loaded tail before appending. Concurrent callers may both execute,
but the stale writer fails with `ThreadHistoryError` and reason `"conflict"`. Serialize
calls when duplicate external work is unacceptable. Provide IDs that remain unique across
restarts.

Other reasons include `"fenced"`, `"incompatible"`, `"not-found"`, `"limit"`, `"encoding"`, and
`"storage"`. The adapter error remains available as the diagnostic cause.

Use separate thread IDs for retained interaction and durable admission. SQLite-backed history
survives restart, but it does not provide receipts, attempt ownership, recovery, or settlement.
Authorize tenant and thread access before execution.

## Use process-local history hooks {#advanced-history-integrations}

Ordinary interactive Runs use the same in-memory layer. Steering and follow-up queues do not
need a separate history adapter:

```ts
const program = AgentRuntime.run(agent, input, {
  threadId,
  input: toRunInputHook(commands),
}).pipe(Effect.provide(Ephemeral.layer));
```

The advanced hooks have these ownership rules:

| Integration            | Behavior                                                                                        |
| ---------------------- | ----------------------------------------------------------------------------------------------- |
| `RunOptions.history`   | Seeds a new in-memory Thread or extends its current history; a divergent prefix is rejected.    |
| `RunOptions.onHistory` | Observes incremental Prompt updates after in-memory retention. Its own writes are caller-owned. |
| `toRunThreadOptions`   | Adapts an existing `EphemeralThreads` snapshot when explicit history hooks are needed.          |
| Durable runtime hooks  | Own history through the journal and commit each turn for recovery.                              |

Use `Ephemeral.layer` or `ThreadHistory.layer` as the shared store for `toRunThreadOptions`.
The helper captures that store while constructing its hooks. Durable hosts supply their own
journal-based history; their execution does not also append to the in-memory store.

Snapshot updates append only their new suffix. A stale or rewritten prefix fails with
`ThreadHistoryDiverged`. A limit error records none of that update, while earlier updates
remain. Custom `onHistory` callbacks own their write guarantees.

Steering enters after a complete model response and tool batch. Follow-up enters only when the
agent would otherwise stop. Neither changes work already in progress.

## Read canonical history {#canonical-history}

`@effect-agent/thread` defines versioned record schemas and a pure reducer. The thread log
is append-only. It records user input, completed model output, settled tool calls, compaction, run
completion or failure, and repairs. Partial tool argument deltas and live queue state are absent.

Immediate history appends `UserInputRecorded`, `ModelCompleted`, and `RunCompleted` together.
Durable execution records each turn and tool result separately for recovery. It can resume from a
[disposable recovery checkpoint](../concepts/durability#recovery-checkpoints) plus a bounded suffix;
retained-history execution still loads the complete export.

`ThreadStore.read` returns at most 1,024 records per request, and each atomic `CanonicalBatch`
contains at most 256 records. SQLite and Cloudflare exports read payloads in bounded pages while
preserving one captured snapshot; the returned export still contains the complete record array.

<a id="store-contract"></a>

## Choose storage {#storage-layers}

| Package                            | Use                                                 |
| ---------------------------------- | --------------------------------------------------- |
| `@effect-agent/storage-memory`     | Tests and process-local development                 |
| `@effect-agent/storage-sqlite`     | History that survives a Node process restart        |
| `@effect-agent/storage-cloudflare` | Durable Object SQLite history and routed operations |

Persistent adapters upgrade supported predecessor formats atomically while preserving stored
history and accepted work. Unsupported or ambiguous formats fail without resetting the store.
See [supported storage upgrades](./operations#adopting-these-contracts) before adopting a new
version, and [Persistence & durability](../concepts/durability) for execution recovery guarantees.

For a custom adapter, follow the [store contract and certification guide](./certify-adapters#store-contract).

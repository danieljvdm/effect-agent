---
title: Threads
description: Keep thread history across agent runs.
---

# Threads

A Thread is an identified, ordered conversation shared across Runs. It exists whether messages
are held in memory or persisted. Durable execution adds a journal and recovery protocol around
that conversation.

## In-memory conversations

`InMemory.layer` is the default setup. It keeps conversation history in memory for the lifetime
of its application Scope. Runs with the same Thread ID load that conversation automatically:

```ts
import { AgentRuntime, InMemory } from "effect-agent";
import { Effect } from "effect";

const conversation = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(agent, "Plan a trip to Lisbon");
  return yield* AgentRuntime.run(agent, "Make it cheaper", { threadId: first.threadId });
}).pipe(Effect.provide(InMemory.layer));
```

Supply the model and tool handlers around this program. Provide the application Layer once
around all conversation Runs, or build one `ManagedRuntime` for a long-lived application.
Providing a fresh Layer separately to each Run creates separate stores. Omitting `threadId`
creates a new conversation; reuse the returned ID for follow-ups.

Conversations may span many Runs for as long as the application Scope stays open, within the
store's capacity limits. In-memory describes storage, not a short lifetime. Execution is
[ephemeral](../concepts/durability): active work cannot recover after process loss.

The layer shares one bounded `Thread.Store` and the subagent reservation ledger.
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

## Inspect a conversation

```ts
import { Thread } from "effect-agent";

const inspect = Effect.gen(function* () {
  const threads = yield* Thread.Store;
  const conversation = yield* threads.snapshot(threadId);
  return Thread.toPrompt(conversation);
});
```

Run this inside the same application Layer as the agent. `Thread.Thread` is the snapshot Schema;
its messages include native text, tool results, reasoning, and files. `Thread.layerMemory` provides
just the memory store; `InMemory.layer` also connects it to agent history and subagent reservations.
`Thread.Store` snapshots and the durable `ThreadStore` journal contract serve different purposes:
the latter also stores execution records needed for persistence and recovery.

## Retain completed runs

`PersistentHistory.layer` connects a `ThreadStore` to ordinary agent Runs and commits each
successful Run as a whole. The [SQLite](../storage/sqlite) and [PostgreSQL](../storage/postgres)
guides show how to build its `History` Layer. With that Layer, one agent can serve many thread IDs:

```ts
import { AgentRuntime, ThreadHistory } from "effect-agent";
import { Effect } from "effect";

const program = Effect.gen(function* () {
  const first = yield* AgentRuntime.run(agent, firstInput, { threadId });
  const second = yield* AgentRuntime.run(agent, secondInput, { threadId });
  const history = yield* ThreadHistory.ThreadHistory;
  const prompt = yield* history.load(threadId);
  return { first, second, prompt };
}).pipe(Effect.provide(History));
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

Use separate thread IDs for retained interaction and durable admission. Database-backed history
survives restart, but it does not provide receipts, attempt ownership, recovery, or settlement.
Authorize tenant and thread access before execution.

## Use process-local history hooks {#advanced-history-integrations}

Ordinary interactive Runs use the same in-memory layer. Steering and follow-up queues do not
need a separate history adapter:

```ts
const program = AgentRuntime.run(agent, input, {
  threadId,
  input: toRunInputHook(commands),
}).pipe(Effect.provide(InMemory.layer));
```

The advanced hooks have these ownership rules:

| Integration            | Behavior                                                                                        |
| ---------------------- | ----------------------------------------------------------------------------------------------- |
| `RunOptions.history`   | Seeds a new in-memory Thread or extends its current history; a divergent prefix is rejected.    |
| `RunOptions.onHistory` | Observes incremental Prompt updates after in-memory retention. Its own writes are caller-owned. |
| `toRunThreadOptions`   | Adapts an existing `Thread.Store` snapshot when explicit history hooks are needed.              |
| Durable runtime hooks  | Own history through the journal and commit each turn for recovery.                              |

Use `InMemory.layer` or `ThreadHistory.layer` as the shared store for `toRunThreadOptions`.
The helper captures that store while constructing its hooks. Durable hosts supply their own
journal-based history; their execution does not also append to the in-memory store.

Snapshot updates append only their new suffix. A stale or rewritten prefix fails with
`ThreadHistoryDiverged`. A limit error records none of that update, while earlier updates
remain. Custom `onHistory` callbacks own their write guarantees.

Steering enters after a complete model response and tool batch. Follow-up enters only when the
agent would otherwise stop. Neither changes work already in progress.

## Read canonical history {#canonical-history}

The durable modules in `effect-agent` define versioned record schemas and a pure reducer. The thread log
is append-only. It records user input, completed model output, settled tool calls, compaction, run
completion or failure, and repairs. Partial tool argument deltas and live queue state are absent.

`PersistentHistory.layer` appends `UserInputRecorded`, `ModelCompleted`, and `RunCompleted` together.
Durable execution records each turn and tool result separately for recovery. It can resume from a
[disposable recovery checkpoint](../concepts/durability#recovery-checkpoints) plus a bounded suffix;
retained-history execution still loads the complete export.

`ThreadStore.read` returns at most 1,024 records per request, and each atomic `CanonicalBatch`
contains at most 256 records. SQLite and Cloudflare exports read payloads in bounded pages while
preserving one captured snapshot; the returned export still contains the complete record array.

<a id="store-contract"></a>

## Choose storage {#storage-layers}

The [Storage guides](../storage/) compare backends and show how to connect each one.
SQLite and Cloudflare adapters upgrade supported predecessor formats atomically;
PostgreSQL rejects incompatible stored versions. See
[supported storage upgrades](./operations#adopting-these-contracts) before adopting a new
version, and [Persistence & durability](../concepts/durability) for execution recovery guarantees.

For a custom adapter, follow the [store contract and certification guide](./certify-adapters#store-contract).

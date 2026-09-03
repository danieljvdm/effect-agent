---
title: Threads
description: Keep thread history across agent runs.
---

# Threads

A Thread is addressable ordered history shared across Runs. An Agent executes Runs within a Thread
and retains their history there. A Thread is separate from an Agent definition, process lifetime,
Submission, or model request.

## Retain completed runs

Provide `PersistentHistory.layer` with a memory or SQLite `ThreadStore` layer. The same agent
can serve many thread IDs.

```ts
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { PersistentHistory } from "@effect-agent/thread/PersistentHistory";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { layer as sqliteStore } from "@effect-agent/storage-sqlite/SqliteThreadStore";
import { Effect, Layer } from "effect";

const MemoryHistoryLive = PersistentHistory.layer.pipe(Layer.provide(MemoryThreadStoreLive));

const SqliteHistoryLive = PersistentHistory.layer.pipe(
  Layer.provide(sqliteStore({ filename: "./history.sqlite" })),
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
`IdGenerator` and the agent's other services at the application boundary.

The [runnable SQLite example](https://github.com/danieljvdm/effect-agent/blob/main/examples/providers/src/history.ts)
stores history across two processes:

```sh
vp run -F @effect-agent/example-providers history --database /tmp/effect-agent-history.sqlite seed
vp run -F @effect-agent/example-providers history --database /tmp/effect-agent-history.sqlite show
```

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

Each encoded input, output, and native Prompt suffix has a 1 MiB limit. Exports have a 65,536
record limit. If the next run would cross that limit, execution fails before model or tool calls.
Start a new thread to continue.

### Choose one history owner {#history-policy-and-append-ownership}

Use `ThreadHistory.layerTransient` when you do not want successful-run retention.
`PersistentHistory.layer` rejects explicit `history`, `onHistory`, `input`, `durability`,
`subagent`, `resume`, and `resumeUsage` options before model or tool execution.

Persistent writers compare the loaded tail before appending. Concurrent callers may both execute,
but the stale writer fails with `ThreadHistoryError` and reason `"conflict"`. Serialize
calls when duplicate external work is unacceptable. Provide IDs that remain unique across
restarts.

Other reasons include `"fenced"`, `"incompatible"`, `"not-found"`, `"limit"`, `"encoding"`, and
`"storage"`. The adapter error remains available as the diagnostic cause.

Use separate thread IDs for retained interaction and durable admission. Persistent history
survives restart, but it does not provide receipts, attempt ownership, recovery, or settlement.
Authorize tenant and thread access before execution.

## Use process-local history hooks {#advanced-history-integrations}

These integrations require `ThreadHistory.layerTransient` and keep different commit rules:

| Integration            | Behavior                                                                                 |
| ---------------------- | ---------------------------------------------------------------------------------------- |
| `RunOptions.history`   | Supplies an initial Prompt. The runtime does not save it.                                |
| `RunOptions.onHistory` | Receives incremental Prompt updates inline. Earlier writes remain after a later failure. |
| `toRunThreadOptions`   | Loads and updates a bounded `EphemeralThreads` snapshot. Partial runs remain visible.    |
| Durable runtime hooks  | Rebuild context from the journal and commit each turn for recovery.                      |

Use explicit history when the caller supplies context as request data. For interactive local
history with steering and follow-up queues:

```ts
const program = Effect.gen(function* () {
  const runOptions = yield* toRunThreadOptions(threads, threadId, runId);
  return yield* AgentRuntime.run(agent, input, {
    ...runOptions,
    input: toRunInputHook(commands),
  }).pipe(Effect.provide(ThreadHistory.layerTransient));
});
```

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
Durable execution records each turn and tool result separately for recovery.

<a id="store-contract"></a>

## Choose storage {#storage-layers}

| Package                            | Use                                                 |
| ---------------------------------- | --------------------------------------------------- |
| `@effect-agent/storage-memory`     | Tests and process-local development                 |
| `@effect-agent/storage-sqlite`     | History that survives a Node process restart        |
| `@effect-agent/storage-cloudflare` | Durable Object SQLite history and routed operations |

The SQLite adapter supports only the current pre-1.0 schema. Incompatible data fails clearly and
may require a reset. See [Persistence & durability](../concepts/durability) before claiming that
active execution survives process loss.

For a custom adapter, follow the [store contract and certification guide](./certify-adapters#store-contract).

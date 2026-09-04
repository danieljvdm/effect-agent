---
title: Deterministic testing
description: Script model turns and test agent behavior with Effect layers.
---

# Deterministic testing

`@effect-agent/testing` provides a deterministic Effect AI `LanguageModel` layer. Use it to test
the real interpreter without network access, credentials, provider latency, or model variance.

## Provide a scripted model layer

```ts
import { Model } from "effect/unstable/ai";
import { ScriptedModel } from "@effect-agent/testing/ScriptedModel";

const TestModel = Model.make("scripted", "test-model", ScriptedModel.layer(turns));
```

A script can emit text, reasoning, tool calls, usage, malformed sequences, typed model failures,
or a stream that waits for interruption. Hooks can inspect the normalized Prompt and detect stream
finalization.

## Choose a testing entry point

The package root exports the `ScriptedModel` module namespace. Import its service, request,
turn, hook types, and schemas directly from `@effect-agent/testing/ScriptedModel`.
Specialized helpers have separate paths:

| Import                                          | Contents                                                |
| ----------------------------------------------- | ------------------------------------------------------- |
| `@effect-agent/testing/Certification`           | Durable adapter certification                           |
| `@effect-agent/testing/Chaos`                   | Seeded plans and convergence checks                     |
| `@effect-agent/testing/CodeExecutorConformance` | `CodeExecutor` adapter conformance                      |
| `@effect-agent/testing/CodeExecutorSubstitute`  | Deterministic in-process executor substitute            |
| `@effect-agent/testing/TravelPlanner`           | Travel Planner definitions, services, and scenarios     |
| `@effect-agent/testing/DocsResearcher`          | Docs Researcher definitions and MCP delegation fixtures |

These paths ship JavaScript and declarations. Install the storage and runtime adapters used by
your tests directly.

Mutable failpoint controls live in `@effect-agent/thread/testing/DurableFailpointTestControl`,
`@effect-agent/storage-sqlite/testing/SqliteStorageFailpointTesting`, and
`@effect-agent/storage-cloudflare/testing/DoStorageFailpointTesting`. Their
`.layer` values provide the production failpoint service and mutable test control over one Ref.
The Cloudflare path also exports `evictionFailpointHandler`.

## Exercise the public runtime

Provide the same layers as the application. Replace `IdGenerator.layer` with a deterministic
counter when assertions depend on stable IDs.

```ts
import { Effect, Layer, Ref, Schema } from "effect";
import { ThreadId, RunId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";

const DeterministicIdGeneratorLive = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const sequence = yield* Ref.make(0);
    const next = Ref.updateAndGet(sequence, (n) => n + 1);

    return IdGenerator.of({
      nextThreadId: next.pipe(Effect.map((n) => Schema.decodeSync(ThreadId)(`thread-${n}`))),
      nextRunId: next.pipe(Effect.map((n) => Schema.decodeSync(RunId)(`run-${n}`))),
      nextTurnId: next.pipe(Effect.map((n) => Schema.decodeSync(TurnId)(`turn-${n}`))),
    });
  }),
);

const TestRuntimeLive = Layer.mergeAll(
  ThreadHistory.layerTransient,
  RunContextPreparationPassthrough,
  ToolkitLive,
  DomainServicesTest,
  DeterministicIdGeneratorLive,
);

it.effect("commits tool results in declaration order", () =>
  AgentRuntime.run(Definition, input).pipe(
    Effect.provide(TestModel),
    Effect.provide(TestRuntimeLive),
    Effect.tap((result) => Effect.sync(() => expect(result.output).toEqual(expected))),
  ),
);
```

This runs the public definition, binding, toolkit handlers, scheduler, stream reducer, and output
decoder. Completed `run` calls need no extra caller Scope. Assert run-local finalizers after the
call or interrupted fiber finishes. Keep a Scope around `start` and resources acquired by the test.

## Assert behavior beyond output {#what-to-assert}

Useful assertions include:

- normalized Prompt content and available tool names;
- parameter decoding and typed failures;
- the complete semantic `RunEvent` trace;
- actual completion order and committed declaration order;
- policy exhaustion and approval suspension;
- interruption and finalizers;
- inferred `Effect<A, E, R>` types.

## Test storage contracts, not implementations

Run shared conformance cases against every memory, SQLite, or custom store. The thread cases
cover materialization, idempotent append, tail conflicts, fencing, observation offsets, export, and
corruption. Stores with `ThreadStore.checkpoints` also run
`threadCheckpointConformanceCases`.

Durable adapters also need ledger conformance, failpoint coverage, and real process-loss tests.
See [Certify storage adapters](./certify-adapters).

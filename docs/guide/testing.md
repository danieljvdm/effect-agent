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
import { ScriptedModel } from "@effect-agent/testing";

const TestModel = Model.make("scripted", "test-model", ScriptedModel.layer(turns));
```

A script can emit text, reasoning, tool calls, usage, malformed sequences, typed model failures,
or a stream that waits for interruption. Hooks can inspect the normalized Prompt and detect stream
finalization.

## Choose a testing entry point

The package root exports `ScriptedModel` and its request, turn, hook types, and schemas. Import
specialized helpers from these paths:

| Import                                           | Contents                                                |
| ------------------------------------------------ | ------------------------------------------------------- |
| `@effect-agent/testing/certification`            | Durable adapter certification                           |
| `@effect-agent/testing/chaos`                    | Seeded plans and convergence checks                     |
| `@effect-agent/testing/code-executor`            | `CodeExecutor` conformance and an in-process substitute |
| `@effect-agent/testing/fixtures/travel-planner`  | Travel Planner definitions, services, and scenarios     |
| `@effect-agent/testing/fixtures/docs-researcher` | Docs Researcher definitions and MCP delegation fixtures |

These paths ship JavaScript and declarations. Install the storage and runtime adapters used by
your tests directly.

Mutable failpoint controls live under `@effect-agent/thread/testing`,
`@effect-agent/storage-sqlite/testing`, and `@effect-agent/storage-cloudflare/testing`. Their
`.layer` values provide the production failpoint service and mutable test control over one Ref.
The Cloudflare path also exports `evictionFailpointHandler`.

## Exercise the public runtime

Provide the same layers as the application. Replace `IdGenerator.layer` with a deterministic
counter when assertions depend on stable IDs.

```ts
import { Effect, Layer, Ref, Schema } from "effect";
import { ThreadId, IdGenerator, RunId, TurnId } from "@effect-agent/core";

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

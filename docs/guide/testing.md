---
title: Deterministic testing
description: Script model Turns and test Agent behavior with ordinary Effect Layers.
---

# Deterministic testing

<StatusCallout status="available" phase="P0–P3" title="The scripted Model, fixtures, type proofs, and storage conformance tests exist today." />

Agent correctness should not depend on network access, credentials, provider latency, or a model's
mood. `@effect-agent/testing` provides a deterministic Effect AI `LanguageModel` Layer that drives
the real interpreter.

## Bind a scripted Model

```ts
import { Model } from "effect/unstable/ai";
import { Agent } from "@effect-agent/core";
import { ScriptedModel } from "@effect-agent/testing";

const TestAgent = Agent.withModel(
  Definition,
  Model.make("scripted", "test-model", ScriptedModel.layer(turns)),
);
```

The script can emit text, reasoning, Tool Calls, usage, malformed sequences, typed model failures,
or a stream that waits for interruption. Hooks can assert the normalized Prompt and detect stream
finalization.

## Exercise the public runtime

Tests provide the same Layers a real application would provide, swapping the default
`IdGenerator.layer` (random Web Crypto UUIDs) for a deterministic Ref-counter Layer so
identities are stable across runs:

```ts
import { Effect, Layer, Ref, Schema } from "effect";
import { ConversationId, IdGenerator, RunId, TurnId } from "@effect-agent/core";

const DeterministicIdGeneratorLive = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const sequence = yield* Ref.make(0);
    const next = Ref.updateAndGet(sequence, (n) => n + 1);

    return IdGenerator.of({
      nextConversationId: next.pipe(
        Effect.map((n) => Schema.decodeSync(ConversationId)(`conversation-${n}`)),
      ),
      nextRunId: next.pipe(Effect.map((n) => Schema.decodeSync(RunId)(`run-${n}`))),
      nextTurnId: next.pipe(Effect.map((n) => Schema.decodeSync(TurnId)(`turn-${n}`))),
    });
  }),
);

const TestRuntimeLive = Layer.mergeAll(
  ToolkitLive,
  DomainServicesTest,
  DeterministicIdGeneratorLive,
);

it.effect("commits Tool results in declaration order", () =>
  AgentRuntime.run(TestAgent, input).pipe(
    Effect.provide(TestRuntimeLive),
    Effect.scoped,
    Effect.tap((result) => Effect.sync(() => expect(result.output).toEqual(expected))),
  ),
);
```

This path uses the actual Definition, Binding, Toolkit handlers, scheduler, Stream reducer, and
output decoder. It is not a mock runtime.

## What to assert

Good Agent tests cover more than final text:

- the normalized Prompt and available Tool names;
- Tool parameter decoding and typed failures;
- the complete semantic `RunEvent` trace;
- actual completion order versus committed declaration order;
- policy exhaustion and approval suspension;
- interruption and every resource finalizer;
- exact inferred `Effect<A, E, R>` types.

The repository's cumulative Travel Planner fixture demonstrates the pattern through every current
phase: bounded parallel searches, safe-seam input, approval and budgets, then replayable persistent
Conversations.

## Test storage contracts, not implementations

Memory and SQLite stores run shared contract cases for materialization, idempotent append, tail
conflict, producer fencing, observation offsets, export, checkpoints, and corruption. A memory fake
must not teach the engine behavior that SQLite rejects.

Future durable adapters add process-kill crash matrices and state-machine equivalence. A durability
milestone cannot pass while those tests are skipped.

---
title: Deterministic testing
description: Script model Turns and test Agent behavior with ordinary Effect Layers.
---

# Deterministic testing

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

## Review quality evaluation

The private `examples/pr-review-eval` bench replays saved requests and scores findings against
source-adjudicated defects. Public fixtures are examples, not an unbiased quality estimate. Live
runs require `EFFECT_AGENT_LIVE=1` and credentials; ordinary tests make no provider calls.

Judge trial one separately. Detection counts an expected blocker found at any severity; blocking
recall requires `blocking` severity. Blocking precision penalizes invalid or overstated findings.
Later trials expose instability and cannot repair a first-trial miss. Named judgments bind to the
exact observation digest; unjudged or unclear findings leave the relevant precision unresolved.
New valid findings require corpus repair, not invented matches to existing defects.

Run `vp run pr-review-eval -- --help` from the repository root. Validate case selection before
creating output. Each completed trial is appended and synchronized to a new, exclusive file;
interruption keeps completed rows and never retries provider calls. Reports require the intended
`--trials` and `--case` selection and reject empty files, incomplete grids, and malformed trailing
lines. Keep private cases and raw results in the example's ignored `data/` and `results/` folders.

## Test storage contracts, not implementations

Memory and SQLite stores run shared contract cases for materialization, idempotent append, tail
conflict, producer fencing, observation offsets, export, checkpoints, and corruption. A memory fake
must not teach the engine behavior that SQLite rejects.

Durable adapters also add ledger conformance, failpoint, and process-kill crash coverage. See
[Certify storage adapters](./certify-adapters).

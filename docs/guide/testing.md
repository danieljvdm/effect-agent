---
title: Deterministic testing
description: Script model Turns and test Agent behavior with ordinary Effect Layers.
---

# Deterministic testing

Agent correctness should not depend on network access, credentials, provider latency, or a model's
mood. `@effect-agent/testing` provides a deterministic Effect AI `LanguageModel` Layer that drives
the real interpreter.

## Provide a scripted model Layer

```ts
import { Model } from "effect/unstable/ai";
import { ScriptedModel } from "@effect-agent/testing";

const TestModel = Model.make("scripted", "test-model", ScriptedModel.layer(turns));
```

The script can emit text, reasoning, Tool Calls, usage, malformed sequences, typed model failures,
or a stream that waits for interruption. Hooks can assert the normalized Prompt and detect stream
finalization.

## Choose a testing entry point

The package root exports only `ScriptedModel` and its request, Turn, and hook types and Schemas.
Specialized utilities and shared application fixtures use these supported imports:

| Import                                           | Contents                                                |
| ------------------------------------------------ | ------------------------------------------------------- |
| `@effect-agent/testing/certification`            | Durable adapter certification runner                    |
| `@effect-agent/testing/chaos`                    | Seeded plans and convergence checks                     |
| `@effect-agent/testing/code-executor`            | CodeExecutor conformance and the in-process substitute  |
| `@effect-agent/testing/fixtures/travel-planner`  | Travel Planner definitions, services, and scenarios     |
| `@effect-agent/testing/fixtures/docs-researcher` | Docs Researcher definitions and MCP delegation fixtures |

These paths ship JavaScript and declarations. The SQLite warehouse fixture stays private to the
repository's tests. Testing's Node runtime and storage adapter dependencies are development-only;
install whichever adapters your own tests use directly.

Mutable failpoint controls also use testing paths. Import `DurableRuntimeFailpointTestControl`
from `@effect-agent/session/testing`, `SqliteStorageFailpointTestControl` from
`@effect-agent/storage-sqlite/testing`, or `DoStorageFailpointTestControl` from
`@effect-agent/storage-cloudflare/testing`. Each control's `.layer` provides both the production
failpoint service and its mutable test control over the same Ref. This replaces
`DurableRuntimeFailpoint.layerTest`, `SqliteStorageFailpoint.layerTest`, and
`DoStorageFailpoint.layerTest`. The Cloudflare testing path also exports `evictionFailpointHandler`.
Production roots retain the failpoint services, handler types, errors, locations, and no-op `.layer`.

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
  ConversationHistory.layerTransient,
  ToolkitLive,
  DomainServicesTest,
  DeterministicIdGeneratorLive,
);

it.effect("commits Tool results in declaration order", () =>
  AgentRuntime.run(Definition, input).pipe(
    Effect.provide(TestModel),
    Effect.provide(TestRuntimeLive),
    Effect.tap((result) => Effect.sync(() => expect(result.output).toEqual(expected))),
  ),
);
```

This path uses the actual Definition, Binding, Toolkit handlers, scheduler, Stream reducer, and
output decoder. It is not a mock runtime.

Completed `run` calls need no extra caller Scope. Assert run-local finalizers immediately after
the call or interrupted fiber finishes, while any enclosing application Scope remains open.
Keep scoping for `start` and for explicit resource acquisition in the test or its supplied operations.

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

Quality reports distinguish succeeded, incomplete, and failed trials. An outcome with `incomplete`
or `exhausted` counts as incomplete even though the invocation returned findings. Those findings
remain eligible for adjudication and recall/precision scoring, and their tokens and cost remain
in resource totals. Incomplete first trials cannot pass clean controls or count as complete cases.
The text summary and version 3 report JSON expose incomplete counts, including costed and uncosted
incomplete trials. Regenerate older reports from the saved observations; their format is unchanged.

Run `vp run pr-review-eval -- --help` from the repository root. Validate case selection before
creating output. Each completed trial is appended and synchronized to a new, exclusive file;
interruption keeps completed rows and never retries provider calls. Reports require the intended
`--trials` and `--case` selection and reject empty files, incomplete grids, and malformed trailing
lines. Keep private cases and raw results in the example's ignored `data/` and `results/` folders.

## Test storage contracts, not implementations

Memory and SQLite stores run shared contract cases for materialization, idempotent append, tail
conflict, producer fencing, observation offsets, export, and corruption. Adapters advertising
`ConversationStore.checkpoints` additionally run `conversationCheckpointConformanceCases`. A memory fake
must not teach the engine behavior that SQLite rejects.

Durable adapters also add ledger conformance, failpoint, and process-kill crash coverage. See
[Certify storage adapters](./certify-adapters).

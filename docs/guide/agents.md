---
title: Agent definitions
description: Immutable, model-agnostic agent programs with explicit Effect AI Model Bindings.
---

# Agent definitions

<StatusCallout status="available" phase="P1" title="Definitions, Bindings, policies, and type projections are implemented." />

An Agent Definition is immutable program data. It contains Schemas, instructions, a native Effect
AI Toolkit, and finite policy. It does not contain a provider client, database connection, mutable
Conversation, or acquired resource.

```ts
const definition = Agent.define("support-triage", {
  input: SupportRequest,
  output: Resolution,
  instructions,
  toolkit: SupportToolkit,
  policy,
});

const agent = Agent.withModel(definition, ClaudeModel);
```

Only the Binding is runnable. That distinction keeps a Definition inspectable and reusable while
making Model selection explicit.

## Definition contract

```ts
interface Definition<InputSchema, OutputSchema, Instructions, Toolkit> {
  readonly id: AgentId;
  readonly input: InputSchema;
  readonly output: OutputSchema;
  readonly instructions: Instructions;
  readonly toolkit: Toolkit;
  readonly policy: AgentPolicy;
  readonly description?: string;
  readonly metadata?: Readonly<Record<string, string>>;
}
```

Instructions may be static prompt input or an input-dependent value that returns prompt input or an
`Effect`. Effectful instructions can require domain services and fail with domain errors; both are
inferred into the Run.

## Bindings expose Model requirements

`Agent.withModel` accepts an upstream Effect AI `Model`. A Binding does not eagerly acquire the
Model or hide its Layer requirements.

```ts
type BeforeModel = Agent.DefinitionRequirements<typeof definition>;
type AfterModel = Agent.Requirements<typeof agent>;
type Failure = Agent.Failure<typeof agent>;
```

The repository protects these projections with compile-time tests. An unbound Definition is
rejected by `AgentRuntime`.

## Stable identity

The string passed to `Agent.define` is decoded as a branded `AgentId`. It becomes part of
definition digests and durable identity.

During private development, renaming it creates new identity. Stored-data migrations are not yet a
compatibility promise.

## Policy is part of the program

```ts
AgentPolicy.make({
  maxTurns: 12,
  maxToolCalls: 24,
  maxDuration: "5 minutes",
  toolConcurrency: 4,
  repeatedFailureLimit: 3,
  tokenBudget: 80_000,
  costBudgetMicrousd: 2_000_000,
  onExhaustion: "final-answer",
});
```

Turns, Tool Calls, duration, and concurrency are positive finite bounds. Token and cost budgets are
optional because not every Model reports enough usage data to enforce them honestly.

`onExhaustion` selects how Turn, Tool Call, and token exhaustion resolve. The default
`"final-answer"` soft-lands the Run: an over-budget Tool batch is rejected with model-visible
failed results, the model gets one final tool-free opportunity to answer, and the Run completes
with the honest `finishReason: "budget-exhausted"` and an `exhausted` marker naming the
dimension. `"fail"` fails the Run typed instead — the strict rail for pipelines that must never
accept a truncated answer. Duration, cost, and repeated-failure bounds always fail typed.

Bounded Tool results, the run-status message, `contextTokenLimit`, and compaction are covered in
[Context management](/guide/context-management).

## Deliberate absences

Definitions do not use render hooks, module directives, global provider registries, or mutable
Session instances. Dynamic Models, Tools, and context belong at explicit Turn boundaries or in
application Effects before the Binding is created.

This is a core architectural constraint: adding expressiveness must not introduce a second hidden
runtime.

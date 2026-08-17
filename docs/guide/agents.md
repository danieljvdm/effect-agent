---
title: Agent definitions
description: Immutable, model-agnostic agent programs with explicit Effect AI Model Bindings.
---

# Agent definitions

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
interface RunDispositionDeclaration<Output, DispositionSchema> {
  readonly schema: DispositionSchema;
  readonly fromOutput: (output: Output) => unknown;
}

interface Definition<
  InputSchema,
  OutputSchema,
  Instructions,
  Toolkit,
  RunDispositionValue = undefined,
> {
  readonly id: AgentId;
  readonly input: InputSchema;
  readonly output: OutputSchema;
  readonly instructions: Instructions;
  readonly toolkit: Toolkit;
  readonly policy: AgentPolicy;
  readonly runDisposition?: RunDispositionValue;
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

## Declare application completion explicitly

Use an optional run-disposition boundary when a record reader needs an application-owned durable
classification beyond the framework's `completed | failed | aborted` settlement outcome.

```ts
const RunDisposition = Schema.Literal("application-complete");

const definition = Agent.define("support-triage", {
  input: SupportRequest,
  output: Resolution,
  instructions,
  toolkit: SupportToolkit,
  policy,
  runDisposition: {
    schema: RunDisposition,
    fromOutput: (resolution) => resolution.runDisposition,
  },
});
```

The selector sees decoded output and may return `undefined`. Its candidate is untrusted until the
Schema validates and encodes it; invalid selection fails with `AgentRunDispositionError`, which
joins `Agent.Failure` only for Definitions that declare this boundary. An ordinary completed
durable Run persists the encoded value on `SubmissionSettled.runDisposition`, where readers decode
it with the same application Schema. Budget exhaustion, failure, abort, and incomplete recovery
never receive one. A selector exception remains available as the typed error's diagnostic `cause`;
only a fixed, non-sensitive message enters the Run event stream. Do not parse summary prose or
infer finality from successful Tool Calls.

## Stable identity

The string passed to `Agent.define` is decoded as a branded `AgentId`. It becomes part of
definition digests and durable identity.

Renaming it creates new identity; pre-1.0 there is no stored-data migration promise.

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

`onExhaustion` selects how Turn, Tool Call, and token exhaustion resolve — the default
`"final-answer"` soft-lands the Run with one constrained final answer and the honest
`finishReason: "budget-exhausted"`. Every bound, the exhaustion resolutions, and sizing guidance
are covered in [Budgets & bounded autonomy](/concepts/budgets); bounded Tool results, the
run-status message, `contextTokenLimit`, and compaction in
[Context management](/guide/context-management).

## Deliberate absences

Definitions do not use render hooks, module directives, global provider registries, or mutable
Session instances. Dynamic Models, Tools, and context belong at explicit Turn boundaries or in
application Effects before the Binding is created.

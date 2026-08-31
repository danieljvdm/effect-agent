---
title: Agent definitions
description: Immutable agent programs executed with native Effect AI model Layers.
---

# Agent definitions

An Agent Definition is immutable program data. It contains Schemas, instructions, a native Effect
AI Toolkit, and finite policy. It does not contain a provider client, database connection, mutable
Conversation, or acquired resource.

```ts
const definition = Agent.make("support-triage", {
  input: SupportRequest,
  output: Resolution,
  instructions,
  toolkit: SupportToolkit,
  policy,
});

const run = AgentRuntime.run(definition, input).pipe(Effect.provide(ClaudeModel));
```

The Definition stays reusable. The application supplies its model with `Effect.provide`.

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
  InputPrompt = undefined,
> {
  readonly id: AgentId;
  readonly input: InputSchema;
  readonly output: OutputSchema;
  readonly instructions: Instructions;
  readonly inputPrompt?: InputPrompt;
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

## Choose model-visible input

By default, the runtime appends the entire Schema-encoded input as a JSON user message after the
instructions. Set `inputPrompt` to choose what the model receives. The function receives decoded
input and returns native Effect AI `Prompt.RawInput`, directly or through an `Effect`.

```ts
const definition = Agent.make("support-triage", {
  input: Schema.Struct({ question: Schema.String, authorizationToken: Schema.String }),
  output: Resolution,
  instructions: "Answer the customer's question.",
  inputPrompt: ({ question }) =>
    Effect.succeed(Prompt.make([{ role: "user", content: [{ type: "text", text: question }] }])),
  toolkit: SupportToolkit,
  policy,
});
```

Strings become user messages. Native Prompts and message arrays preserve their roles, parts, and
provider options, including multimodal content. Return an empty Prompt or message array to omit the
input message entirely. Projection errors and required services join the Agent's inferred `E` and
`R`, including when it is bound, spawned, or registered with a durable worker.

The runtime decodes input, evaluates instructions, encodes the canonical input, and evaluates
`inputPrompt` before preparing model context. It appends history, instructions, then projected
input. Each Turn applies the context transform before compaction, then adds the output contract
and Run status to the outgoing request. Compaction and its summary calls receive the projected
content. See [Context management](/guide/context-management).

Durable admission retains the complete encoded input in the Submission Ledger and
`UserInputRecorded`. Action-time Tool authorization receives that canonical input. The projection
does not redact storage or change authorization. Instructions also receive decoded input, so they
must avoid copying host-only values into their own messages.

Preparation runs again on each durable Attempt. Once a Turn commits, recovery restores its
recorded projected messages, including the leading messages of a pending Tool batch. A newly
evaluated projection does not replace that committed history. Queued Submissions joined into an
active Run use the same input projection; recovery re-evaluates joins that have not yet committed
model-visible messages. Projection Effects must tolerate re-evaluation and must not assume
exactly-once external execution.

An expected projection failure prevents the next model request and fails the Run. Defects remain
defects, interruption runs Effect finalizers, and the Run deadline also bounds projection. No
failure path falls back to exposing the full input. Canonical admitted input remains available to
the authorized host after failure or interruption. Other model-visible content, such as Tool
results, explicit steering, prior history, and host context transforms, still needs its own
disclosure policy.

## Provide native model services

`AgentRuntime.run`, `stream`, and `start` accept a Definition. They require the native
`LanguageModel.LanguageModel`, `Model.ProviderName`, and `Model.ModelName` services, plus
instruction, Tool, Schema, and runtime dependencies. Upstream model Layers provide all three
model services. Use `Stream.provide` for a stream.

```ts
const program = AgentRuntime.run(definition, input).pipe(
  Effect.provide(ClaudeModel),
  Effect.provide(AppLive),
);

const captured = Effect.gen(function* () {
  const modelLayer = yield* ClaudeModel.captureRequirements;
  return yield* AgentRuntime.run(definition, input).pipe(Effect.provide(modelLayer));
});
```

Ordinary `Layer.mergeAll` and `Layer.provide` composition works without rebuilding a Model.
Capturing requirements moves provider dependencies to the enclosing Effect; it does not remove
them from the application contract. When using `start`, keep the model Layer around the Effect
that starts and awaits the detached Run so its resources outlive the child.

`Agent.withModel(definition, modelLayer)` remains useful when durable registration or delegation
must fix a model for a particular Agent. It accepts a Layer providing all three native model
services, including a captured Model Layer. Bindings remain accepted by the execution operations
and keep their Layer requirements visible. Binding model Layers must have no construction errors;
fallible application setup belongs in the enclosing Layer or Effect.

```ts
type BeforeModel = Agent.DefinitionRequirements<typeof definition>;
type ExecutionRequirements = Agent.Requirements<typeof definition>;
type Failure = Agent.Failure<typeof definition>;
```

The repository protects these projections with compile-time tests.

When selecting between Definitions or Bindings, the returned `E` and `R` include every possible
branch's failures and services. Provide the handlers and Schema services for every branch, or
narrow the selected Agent before executing it.

## Typed and external inputs

The primary operations accept `Agent.EncodedInput<typeof definition>`, the input Schema's
`Encoded` representation. Required fields and field types are checked at the call site. Runtime
decoding still validates refinements and transforms the input before instructions execute.
Instructions and `inputPrompt` receive `Agent.Input<typeof definition>`, the decoded `Type`.

For example, a field declared as `Schema.NumberFromString` accepts a string such as `"2"` at
`run`, while instructions receive the number `2`. To submit an already decoded value, first use
`Schema.encodeEffect(definition.input)` and provide its encoding services.

Use `runUnknown`, `streamUnknown`, or `startUnknown` for external data typed as `unknown`.
They apply the same decoder and fail with `AgentInputError` before instructions or model
execution when input is invalid. Durable Submission payloads enter through this explicit boundary.

## Declare application completion explicitly

Use an optional run-disposition boundary when a record reader needs an application-owned durable
classification beyond the framework's `completed | failed | aborted` settlement outcome.

```ts
const RunDisposition = Schema.Literal("application-complete");

const definition = Agent.make("support-triage", {
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

The string passed to `Agent.make` is decoded as a branded `AgentId`. It becomes part of
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

`onExhaustion` selects how Turn, Tool Call, and token exhaustion resolve. The default
`"final-answer"` soft-lands the Run with one constrained final answer and the honest
`finishReason: "budget-exhausted"`. Every bound, the exhaustion resolutions, and sizing guidance
are covered in [Budgets & bounded autonomy](/concepts/budgets); bounded Tool results, the
run-status message, `contextTokenLimit`, and compaction in
[Context management](/guide/context-management).

## Deliberate absences

Definitions do not use module directives, global provider registries, or mutable
Session instances. Dynamic Models, Tools, and context belong at explicit Turn boundaries or in
application Effects before execution or durable registration.

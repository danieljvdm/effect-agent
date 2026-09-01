---
title: Agent definitions
description: Define agents with schemas, tools, instructions, and run limits.
---

# Agent definitions

An agent definition contains schemas, instructions, a native Effect AI toolkit, and a finite
policy. The application supplies model services and other dependencies when it runs the agent.

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

Definitions contain no provider client, database connection, mutable thread, or acquired
resource. Reuse one definition across many runs.

## Build a definition {#definition-contract}

`Agent.make` requires an ID, input and output schemas, instructions, and a toolkit.
`policy` accepts an `AgentPolicy` or a partial policy declaration. Standalone defaults are
12 turns, 24 tool calls, 5 minutes, and tool concurrency 4. Other defaults follow `AgentPolicy.make`.
Delegated children inherit omitted policy fields from their parent. Supply a partial object
to inherit individual fields; `AgentPolicy.make` fills its own defaults before inheritance.
Definitions retain the explicit fields as `policyOverrides`; `policy` holds their standalone values.
It also accepts `inputPrompt`, `completion`, `runDisposition`, a description, and metadata.

Instructions may be static prompt input or a function of decoded input. That function may return
an `Effect`. Its errors and service requirements become part of the run type.

## Choose model-visible input

The runtime normally sends the complete schema-encoded input as a JSON user message. Use
`inputPrompt` to send a smaller or differently shaped value.

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

`inputPrompt` receives decoded input and returns native Effect AI `Prompt.RawInput`, directly or
through an `Effect`. Strings become user messages. Prompts and message arrays keep their roles,
parts, provider options, and multimodal content. Return an empty Prompt or message array to omit
the input message. Its errors and service requirements join the run's `E` and `R`.

The runtime builds model context in this order: history, instructions, projected input, context
transform, compaction, output contract, and run status. See
[Context management](/guide/context-management).

Projection changes only model-visible input. Durable admission still stores the complete encoded
input, and tool authorization receives it. Keep secrets out of instructions and apply separate
disclosure rules to history, tool results, steering, and host context transforms.

Durable attempts may evaluate the projection again. Committed turns keep their recorded projected
messages. Projection effects must tolerate another evaluation and must not assume exactly-once
external execution.

A projection failure stops the run before the next model request. The runtime never falls back to
the full input. Projection services, errors, interruption, and deadlines follow normal Effect
semantics.

<a id="deliberate-absences"></a>

## Provide native model services

`run`, `stream`, and `start` require `LanguageModel.LanguageModel`, `Model.ProviderName`, and
`Model.ModelName`. Upstream model layers provide all three.

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

Use `Stream.provide` with `stream`. Keep the model layer around both `start` and the detached
run's lifetime.

Pass a model Layer directly to `SubagentRuntime.layer(delegation, model)`. Durable registration
accepts `{ agent: definition, model, definitions: versions }`. `Agent.withModel` remains available
when an application wants a reusable binding. The model Layer must provide all three model
services and have no construction error. Put
fallible setup in the enclosing layer or Effect.

```ts
type BeforeModel = Agent.DefinitionRequirements<typeof definition>;
type ExecutionRequirements = Agent.Requirements<typeof definition>;
type Failure = Agent.Failure<typeof definition>;
```

Selecting between several definitions or bindings produces the union of their errors and
requirements. Provide every branch or narrow the selection before execution.

## Typed and external inputs

The main operations accept `Agent.EncodedInput<typeof definition>`. The runtime decodes that value
before instructions run. For `Schema.NumberFromString`, callers pass a string and instructions
receive a number.

Encode a decoded value with `Schema.encodeEffect(definition.input)`. Use `runUnknown`,
`streamUnknown`, or `startUnknown` for untrusted external data. Invalid input fails with
`AgentInputError` before instructions or model execution.

## Complete through a tool

Set `completion` when a successful tool result should become the agent's output without another
model turn. The projector receives decoded tool parameters and result:

```ts twoslash
import { Agent, AgentPolicy } from "@effect-agent/core";
import { Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

const Answer = Schema.Struct({ answer: Schema.String });
const Complete = Tool.make("complete", { parameters: Answer, success: Schema.Void });
const Tools = Toolkit.make(Complete);

export const Definition = Agent.make("answer-question", {
  input: Schema.Struct({ question: Schema.String }),
  output: Answer,
  instructions: "Answer the question using the complete tool.",
  toolkit: Tools,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  completion: {
    tool: "complete",
    required: true,
    project: ({ parameters }) => parameters,
  },
});

export const ToolsLive = Tools.toLayer({ complete: () => Effect.void });
```

Provide `ToolsLive` with the model and history services when running this definition.
The output schema validates the projected value. With `required: true`, each turn must call a
tool and completion must use the named tool. Without it, valid final assistant JSON may also
complete the run. [Exhaustion policy](../concepts/budgets#exhaustion-final-answer-or-failure)
controls the last available turn.

`completion` decides how a run finishes. `runDisposition` labels its successful output for
durable readers.

## Declare application completion explicitly

Use `runDisposition` when durable readers need an application-defined classification in addition
to the framework settlement outcome.

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

The selector receives decoded output and may return `undefined`. The schema validates and encodes
any returned value. Invalid output fails with `AgentRunDispositionError`.

Only an ordinary completed durable run stores the encoded disposition on
`SubmissionSettled.runDisposition`. Budget exhaustion, failure, abort, and incomplete recovery
store none. Parse it with the same application schema. Do not infer completion from prose or tool
success.

## Stable identity

The string passed to `Agent.make` becomes a branded `AgentId` and contributes to durable identity
and definition digests. Renaming it creates a new identity. Before 1.0, stored data has no
migration promise.

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

Turns, tool calls, duration, and concurrency need positive finite bounds. Token and cost budgets
are optional because some models do not report enough usage data.

The default `onExhaustion: "final-answer"` allows one constrained final answer for turn, tool call,
or token exhaustion. The result uses `finishReason: "budget-exhausted"`. See
[Budgets & bounded autonomy](/concepts/budgets) for all exhaustion rules and
[Context management](/guide/context-management) for tool result bounds, run status, context limits,
and compaction.

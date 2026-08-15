---
title: What is Effect Agent?
description: The product model, current boundary, and reason Effect Agent exists.
---

# What is Effect Agent?

<StatusCallout status="available" phase="P0–P3" title="The typed interpreter and persistent Conversation foundation exist today.">

The workspace is private and pre-release. The current implementation is an ephemeral, bounded
runtime with operational capabilities and persistent Conversation records. Durable accepted work
is the next phase—not a current claim.

</StatusCallout>

## One Agent, end to end

An Effect Agent combines Schema-defined input and output, native Effect AI Tools, an explicit
Model binding, and application Layers. `AgentRuntime.run` interprets that binding as a scoped
Effect program.

```ts [calculator-agent.ts]
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Agent, AgentPolicy, IdGenerator } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import { Config, Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

// 1. Describe what enters and leaves the Agent.
const Question = Schema.Struct({ question: Schema.String });
const Answer = Schema.Struct({ answer: Schema.Number });

// 2. Define a native Effect AI Tool and its handler Layer.
const Add = Tool.make("add", {
  description: "Add two numbers exactly.",
  parameters: Schema.Struct({ left: Schema.Number, right: Schema.Number }),
  success: Schema.Struct({ total: Schema.Number }),
});

const Calculator = Toolkit.make(Add);
const CalculatorLive = Calculator.toLayer({
  add: ({ left, right }) => Effect.succeed({ total: left + right }),
});

// 3. Define finite autonomous behavior, then bind exactly one Model.
const Definition = Agent.define("calculator", {
  input: Question,
  output: Answer,
  instructions: ({ question }) =>
    `Use the calculator Tool to answer: ${question}. Return {"answer": number}.`,
  toolkit: Calculator,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const CalculatorAgent = Agent.withModel(Definition, OpenAiLanguageModel.model("gpt-4.1-mini"));

// 4. Build the application's Effect environment.
const OpenAiLive = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

const AppLive = Layer.mergeAll(CalculatorLive, IdGenerator.layer, OpenAiLive);

// 5. The Agent is still an Effect until the application entrypoint runs it.
const program = AgentRuntime.run(CalculatorAgent, {
  question: "What is 20 + 22?",
}).pipe(
  Effect.tap((result) => Effect.logInfo("answer", result.output)),
  Effect.provide(AppLive),
  Effect.scoped,
);

void Effect.runPromise(program);
```

`Definition` contains the immutable Agent configuration. `CalculatorLive` supplies Tool behavior,
`CalculatorAgent` fixes the Model selection, and `AppLive` provides the runtime dependencies.
`IdGenerator.layer` is the framework's default Web Crypto identity authority; the
[testing guide](./testing) replaces it with deterministic IDs. The result remains an `Effect`
until the application entrypoint executes it.

Effect Agent is an interpreter for autonomous programs in Effect applications. An Agent is an
immutable, schema-defined value. Pair it with an Effect AI Model and the runtime can interpret it as
an `Effect` result or a semantic `Stream`.

```ts
AgentRuntime.run(agent, input);
// Effect<AgentResult<Output>, AgentFailure | DomainFailure, Requirements | Scope>

AgentRuntime.stream(agent, input);
// Stream<RunEvent, AgentFailure | DomainFailure, Requirements>
```

That signature is more than API styling. It means:

- success is Schema-decoded rather than trusted because a model emitted JSON;
- expected AI, Tool, and application failures remain visible in `E`;
- Models, Tool handlers, domain services, clocks, and stores remain visible in `R`;
- interruption closes the Run Scope and finalizes everything it owns.

## The missing layer

Effect AI already owns Models, Language Models, Prompts, Responses, Tools, Toolkits, approval
parts, and provider Layers. Effect Agent uses those values directly.

It adds only the concepts needed to interpret autonomous work:

| Effect AI owns           | Effect Agent adds                         |
| ------------------------ | ----------------------------------------- |
| `Model`, `LanguageModel` | Agent Definition and explicit Binding     |
| `Tool`, `Toolkit`        | bounded Tool-batch scheduling             |
| `Prompt`, `Response`     | multi-Turn state machine                  |
| provider Layers          | Run, Conversation, Submission, Settlement |
| response streams         | stable semantic `RunEvent` stream         |

There is no framework provider registry, second Tool type, callback middleware runtime, or
Promise-first facade.

## Three maturity levels

Understanding the current boundary depends on keeping three related ideas separate.

### Ephemeral execution <StatusBadge status="available" />

A Run executes now, inside one Scope. It supports bounded model Turns, Tool Calls, interruption,
semantic events, steering, follow-up, approval, budgets, context transforms, MCP, and a local
sandbox adapter. If the process disappears, active work disappears with it.

### Persistent Conversations <StatusBadge status="available" />

Canonical Conversation records can survive restart through memory or SQLite adapters. History can
be replayed, projected, exported, checkpointed, and observed from an opaque offset. Persistence
does not mean the runtime has accepted an obligation to finish active work.

### Durable accepted work <StatusBadge status="available" />

The durable runtime first commits a Submission obligation, then returns a Receipt. Attempts may
be replaced after a crash, but exactly one terminal Settlement is eventually recorded. The base
Node/SQLite runtime — fencing, recovery classification, and deterministic failpoints — is
implemented; recovery resumes at Turn boundaries, so
the claim covers safe-to-repeat toolkits. Honest unknown external outcomes, durable Steps, and
replay-safe external mutations are Phase 5 work.

## What the framework optimizes for

Effect Agent is designed for Effect application teams that want autonomous behavior without
adopting a second application runtime. It is especially opinionated about:

- retaining typed domain failures and service requirements;
- replacing live integrations with deterministic Layers in tests;
- bounding autonomy by default;
- preserving one canonical, replayable history;
- refusing to label ambiguous external effects “exactly once.”

It is not currently a hosted control plane, visual builder, turnkey chat product, generic workflow
engine, or secure remote code sandbox.

## Where to go next

- [Build a Definition and Binding](./getting-started)
- [Understand the Effect-native architecture](../concepts/effect-native)
- [See exactly what is implemented](../reference/status)

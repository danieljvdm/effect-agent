---
title: What is Effect Agent?
description: An interpreter for autonomous agents inside Effect applications, including what it adds and what remains in Effect AI.
---

# What is Effect Agent?

**Effect Agent** is an interpreter for autonomous agents inside Effect applications. An Agent is
an immutable, Schema-defined value; pair it with an Effect AI Model and the runtime interprets it
as an `Effect` result or a semantic `Stream`. Failures remain typed in `E`, dependencies remain
visible in `R`, and every resource remains owned by a Scope. This page shows one complete Agent, explains what
the framework adds on top of Effect AI, and names the three persistence levels it supports.

## One Agent, end to end

An Effect Agent combines Schema-defined input and output, native Effect AI Tools, an explicit
Model binding, and application Layers. `AgentRuntime.run` interprets that binding as a scoped
Effect program.

```ts [calculator-agent.ts]
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Agent, AgentPolicy, IdGenerator } from "@effect-agent/core";
import { AgentRuntime, ConversationHistory } from "@effect-agent/engine";
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

const AppLive = Layer.mergeAll(
  CalculatorLive,
  IdGenerator.layer,
  ConversationHistory.layerTransient,
  OpenAiLive,
);

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
until the application entrypoint executes it:

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

## Three levels of persistence

The runtime makes three distinct claims, and keeping them separate is deliberate. All three are
implemented and tested.

**Ephemeral execution.** A Run executes now, inside one Scope. It supports bounded model Turns,
Tool Calls, interruption, semantic events, steering, follow-up, approval, budgets,
[context management](./context-management), MCP, and a local sandbox adapter. If the process
disappears, active work disappears with it.

**Persistent Conversations.** `ConversationHistory` is the history policy required by all
`AgentRuntime` entry points. `PersistentHistory.layer` from `@effect-agent/session/history` retains
successful Runs through a supplied `ConversationStore`; `ConversationHistory.layerTransient`
selects execution without retained history. Load retained history through the service's `load` method.
SQLite and Cloudflare storage survive restart; the memory adapter is process-local. History can
be replayed, projected, exported, and observed. Checkpoint storage is optional and unused by
execution or recovery. An interrupted Run has no completion obligation. See
[Conversations](./conversations#retain-completed-runs) for the two-input SQLite example and
concurrent-writer limits.

**Durable accepted work.** The durable runtime first commits a Submission obligation, then
returns a Receipt. Attempts may be replaced after a crash, but exactly one terminal Settlement is
eventually recorded. Unresolved external Tool effects stop at an explicit Unknown Outcome instead
of replaying, and the same contract runs on Node/SQLite and on Cloudflare Durable Objects. No
level claims exactly-once external side effects. See
[Persistence & durability](../concepts/durability).

## Capabilities and host responsibilities

The [capability inventory](../reference/packages#capability-inventory) distinguishes public APIs,
bundled defaults and adapters, and application-owned implementations. Installing the umbrella
does not install a provider or a durable host. Applications supply Model Layers and credentials,
Tool handlers, authorization, storage, and any external transports they use.

Time-based [Scheduling](./operations#scheduled-input) and durable
[event subscriptions](./operations#event-subscriptions) deliver new input through ordinary
admission after the registering Run has ended. Neither keeps a Run alive or promises one Run per
delivery. Their owner scoping does not establish tenant isolation for Conversation storage; hosts
must enforce the [addressing and access boundary](./operations#authorization-and-isolation).

MCP provides `McpConnector` and `connectMcp` for scoped connection and bounded discovery validation.
The application implements the transport and supplies remote Tool handlers. There is no bundled
stdio or HTTP MCP client adapter.

Runtime Skills, a separate persistent agent memory/state service, SessionStore metadata, and
generic dynamic Turn Plans are unsupported. Conversation history and compaction summaries are
implemented, but neither supplies those missing APIs. [Context management](./context-management)
explains automatic interpreter compaction and the separate, explicitly applied artifact utilities.

## What the framework optimizes for

Effect Agent is designed for Effect application teams that want autonomous behavior without
adopting a second application runtime. It is especially opinionated about:

- retaining typed domain failures and service requirements;
- replacing live integrations with deterministic Layers in tests;
- bounding autonomy by default;
- preserving one canonical, replayable history;
- refusing to label ambiguous external effects "exactly once."

It is not a hosted control plane, visual builder, turnkey chat product, generic workflow engine,
or a general secure remote code sandbox. The public alpha is distributed as npm prereleases on
the `beta` dist-tag, with versions shaped as `X.Y.Z-beta.N`. "Alpha" describes maturity, not an
additional npm channel. This source tree pins Effect and the OpenAI/Anthropic provider packages to
`4.0.0-rc.111`. APIs and stored data may break before 1.0; no compatibility window or migrations are
promised. Incompatible data fails clearly and may need a reset. Follow the
[installation and compatibility guidance](./getting-started#installation-and-compatibility).

## Next steps

- [Getting started](./getting-started) shows how to build a Definition and Binding.
- [Effect-native by construction](../concepts/effect-native) explains how the architecture keeps
  these properties.

---
title: Built on Effect
description: Use Effect schemas, typed errors, service layers, and scoped resources.
---

<a id="effect-native-by-construction"></a>

# Built on Effect

Effect Agent keeps the contracts an Effect application relies on.
These excerpts use the [travel planner from the homepage](/).

<a id="the-architectural-test"></a>

## Return Effects and Streams {#effects-are-the-public-boundary}

Public asynchronous operations return `Effect` or `Stream`. This includes tool handlers,
instructions, approval decisions, stores, and platform capabilities.

```ts twoslash
import { AgentRuntime } from "@effect-agent/engine";
import { Effect, Stream } from "effect";
import { TravelPlanner } from "./planner";
const input = { city: "Lisbon", days: 2 };
// ---cut---
const itinerary = AgentRuntime.run(TravelPlanner, input).pipe(
  Effect.map((result) => result.output.itinerary),
  Effect.timeout("2 minutes"),
);

const eventNames = AgentRuntime.stream(TravelPlanner, input).pipe(
  Stream.map((event) => event._tag),
);
```

Retrying an entire run can repeat external effects. Durable recovery follows recorded tool
outcomes and never automatically replays an uncertain ordinary tool.

## Define data with Schema {#schema-is-canonical}

Effect Schema defines agent input and output, tool parameters and results, commands, events,
persisted records, and transport values. Provider JSON Schema and wire codecs derive from these
definitions. Applications need no parallel schema tree.

```ts twoslash
import { Schema } from "effect";
// ---cut---
const TripRequest = Schema.Struct({
  city: Schema.String,
  days: Schema.Int.check(Schema.isGreaterThan(0)),
});

type TripRequest = typeof TripRequest.Type;

const decodeRequest = Schema.decodeUnknownEffect(TripRequest);
const request = decodeRequest({ city: "Lisbon", days: 2 });
```

Use `TripRequest` as the agent's `input` schema. The same definition supplies the TypeScript
type and rejects invalid values at runtime.

## Provide services through Layers {#layers-preserve-architecture}

Definitions describe behavior and requirements. Layers provide models, toolkits, stores,
sandboxes, clocks, identifiers, authorization, and platform services.

```ts twoslash
import { AgentRuntime } from "@effect-agent/engine";
import { AnthropicLanguageModel } from "@effect/ai-anthropic";
import { Effect, Layer } from "effect";
import { TravelPlanner } from "./planner";
import { AppLive } from "./setup";
const input = { city: "Lisbon", days: 2 };
// ---cut---
const RuntimeLive = AnthropicLanguageModel.model("claude-sonnet-5").pipe(
  Layer.provideMerge(AppLive),
);

const program = AgentRuntime.run(TravelPlanner, input).pipe(Effect.provide(RuntimeLive));
```

The travel planner's `AppLive` supplies the provider client, tool handlers, IDs, and transient
history. `Layer.provideMerge` supplies the model's client dependency and keeps those services
available to the run.

## Scope resources {#scope-is-ownership}

An ephemeral run owns one parent Scope. Model streams, tool fibers, queues, MCP clients, sandbox
processes, event publication, and attached children live beneath it.

`run` completes cleanup before returning. `stream` closes resources when consumption completes,
fails, or is interrupted. `start` requires a caller Scope because execution and replay continue.
Requirements from application code remain visible, including any real `Scope` requirement.

```ts twoslash
import { AgentRuntime } from "@effect-agent/engine";
import { Effect } from "effect";
import { TravelPlanner } from "./planner";
const input = { city: "Lisbon", days: 2 };
// ---cut---
const program = Effect.gen(function* () {
  const handle = yield* AgentRuntime.start(TravelPlanner, input);
  const result = yield* handle.await;
  const events = yield* handle.events;
  return { result, events };
}).pipe(Effect.scoped);
```

The caller's Scope owns the handle and its replay buffer. Leaving it closes both, including
when the program fails or is interrupted.

The runtime creates no daemon fibers. Interrupting the owner stops new work, interrupts children,
and closes resources without reporting false success.

## Use Effect AI directly {#effect-ai-stays-effect-ai}

Effect Agent uses Effect AI's `Tool`, `Toolkit`, `LanguageModel`, `Model`, `Prompt`, and `Response`
directly. Provider integrations remain Effect AI Layers. The engine adds agent loops,
conversation history, and durable execution around those values.

```ts twoslash
import { Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

const SearchActivities = Tool.make("search_activities", {
  description: "Find activities in a city.",
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.Array(Schema.String),
});

const TravelTools = Toolkit.make(SearchActivities);
```

Pass `TravelTools` directly to `Agent.make` as its `toolkit`. Implement handlers with
`TravelTools.toLayer`, using the same Effect AI toolkit.

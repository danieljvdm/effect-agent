# Effect Agent

Build TypeScript agents with [Effect](https://github.com/Effect-TS/effect) and Effect AI.
Define inputs, outputs, and tools with schemas. Effect Agent runs the loop, executes tools,
and validates the result — with typed errors, streaming, and bounded execution.

## Install

```sh
npm install effect-agent@beta
```

Use an [Effect AI provider](examples/providers/README.md) for model access.
Public beta: APIs and stored data may change before 1.0.

## A basic agent

```ts
import { Effect, Schema } from "effect";
import { Agent, AgentRuntime } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";

const planner = Agent.make("travel-planner", {
  input: Schema.Struct({ city: Schema.String, days: Schema.Int }),
  output: Schema.Struct({ itinerary: Schema.Array(Schema.String) }),
  instructions: ({ city, days }) => `Plan ${days} days in ${city}. Suggest one activity per day.`,
  toolkit: Toolkit.empty,
  policy: { maxTurns: 6, maxToolCalls: 10, maxDuration: "2 minutes" },
});

const program = Effect.gen(function* () {
  const result = yield* AgentRuntime.run(planner, { city: "Lisbon", days: 2 });
  yield* Effect.log(result.output.itinerary); // readonly string[]
});
```

The output is schema-validated. Supply your model and runtime services to run it:

<details>
<summary>Run this example with OpenAI</summary>

Save the code above and the setup below as `agent.ts`.

```ts
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { BunRuntime } from "@effect/platform-bun";
import { Config, Layer } from "effect";
import { IdGenerator } from "effect-agent/IdGenerator";
import { ThreadHistory } from "effect-agent/ThreadHistory";
import { FetchHttpClient } from "effect/unstable/http";

const AppLive = Layer.mergeAll(
  OpenAiLanguageModel.model("gpt-4.1-mini"),
  IdGenerator.layer,
  ThreadHistory.layerTransient,
).pipe(
  Layer.provide(OpenAiClient.layerConfig({ apiKey: Config.redacted("OPENAI_API_KEY") })),
  Layer.provide(FetchHttpClient.layer),
);

BunRuntime.runMain(program.pipe(Effect.provide(AppLive)));
```

```sh
export OPENAI_API_KEY="your-api-key"
bun agent.ts
```

</details>

## Give it tools

Use native Effect AI tools with typed parameters, results, and Effect handlers:

```ts
import { Tool } from "effect/unstable/ai";

const SearchActivities = Tool.make("search_activities", {
  description: "Find activities in a city.",
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.Array(Schema.String),
});

const TravelTools = Toolkit.make(SearchActivities);
const TravelToolsLive = TravelTools.toLayer({
  // Sample data; replace with your database or API.
  search_activities: ({ city }) =>
    Effect.succeed(city === "Lisbon" ? ["Riverside walk", "Food market"] : []),
});
```

Define these before `planner`, set its `toolkit` to `TravelTools`, and add `TravelToolsLive`
to `Layer.mergeAll` above.
[More about tools, approvals, and MCP →](docs/guide/tools.md)

## Stream progress

Use the same agent and services to observe text, tool activity, and lifecycle events:

```ts
import { Stream } from "effect";

const streaming = AgentRuntime.stream(planner, { city: "Lisbon", days: 2 }).pipe(
  Stream.runForEach((event) => Effect.log(event._tag)),
  Effect.provide(AppLive),
);

BunRuntime.runMain(streaming);
```

Use this in place of the earlier `BunRuntime.runMain` call.
[More about streaming and interactive input →](docs/guide/run-agents.md)

## More examples

- [Travel planner](docs/snippets/travel-planner/) — complete agent, tools, and provider setup.
- [Subagents](docs/guide/subagents.md), [browser tools](docs/guide/browser.md), and
  [Code Mode](docs/guide/code-mode.md) — delegate research, browse pages, and execute code.
- [Persistent threads](docs/guide/threads.md), [durable execution](docs/concepts/durability.md), and
  [Effect Workflows](docs/guide/workflows.md) — keep history and resume work.
- [Runnable examples](examples/) and the [PR reviewer](packages/pr-review/README.md).

Start with the [getting-started guide](docs/guide/getting-started.md), or explore the
[package map](docs/reference/packages.md#capability-inventory) and
[deployment guide](docs/guide/operations.md#authorization-and-isolation).

## Development

Framework packages live in `packages/*`, and runnable examples live in `examples/*`.
Use Vite+ for repository commands. Bun is the package manager.

```sh
vp install
vp run docs:dev
vp run ready
```

`vp run ready` runs static checks, tests, package builds, and the documentation build with link
validation. Before changing code, read the [toolchain guide](docs/TOOLCHAIN.md),
[glossary](GLOSSARY.md), and [contributor instructions](AGENTS.md).

## Similar projects and inspiration

We took inspiration from [Flue](https://github.com/withastro/flue) and
[Pi](https://github.com/earendil-works/pi) for parts of the agent loop, interaction model, and
durability design.

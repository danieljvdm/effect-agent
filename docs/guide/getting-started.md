---
title: Getting started
description: Install Effect Agent and run your first agent.
---

# Getting started

Build an agent that classifies a bug report and returns a typed result.

## Install {#installation-and-compatibility}

In a TypeScript project with [Bun](https://bun.sh):

```sh
bun add effect-agent@beta
```

Requires `effect@^4.0.0-rc.111` and an Effect AI provider.
For the example below, also install `@effect/ai-openai@4.0.0-rc.111` and `@effect/platform-bun@4.0.0-rc.111`.

## Create an agent

Save as `agent.ts`:

```ts
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { BunRuntime } from "@effect/platform-bun";
import { Config, Console, Effect, Schema } from "effect";
import { Agent, AgentPolicy, AgentRuntime, ConversationHistory, IdGenerator } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

const triage = Agent.make("triage", {
  input: Schema.String,
  output: Schema.Struct({
    severity: Schema.Literals(["low", "medium", "high", "critical"]),
    explanation: Schema.String,
  }),
  instructions: "Classify the bug report by severity. Explain your reasoning in one sentence.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const program = AgentRuntime.run(triage, "All users get a 500 error when signing in.").pipe(
  Effect.tap((result) => Console.log(result.output)),
  Effect.provide(OpenAiLanguageModel.model("gpt-4.1-mini")),
  Effect.provide(OpenAiClient.layerConfig({ apiKey: Config.redacted("OPENAI_API_KEY") })),
  Effect.provide(FetchHttpClient.layer),
  Effect.provide(IdGenerator.layer),
  Effect.provide(ConversationHistory.layerTransient),
);

BunRuntime.runMain(program);
```

The output schema validates the model's answer. The policy limits the run. This example keeps no history.

## Run it

```sh
export OPENAI_API_KEY="your-api-key"
bun agent.ts
```

Example output:

```json
{ "severity": "critical", "explanation": "All users are blocked from signing in." }
```

## Next

[Add tools](./tools), [stream responses](./run-agents), or [save conversation history](./conversations).

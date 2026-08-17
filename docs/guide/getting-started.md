---
title: Getting started
description: Define, bind, and run a bounded Effect-native Agent.
---

# Getting started

An Effect Agent has five pieces: input and output Schemas, instructions, an Effect AI Toolkit, a
finite policy, and an explicit Model Binding. This page builds one from scratch. The packages
publish to npm on the opt-in `beta` dist-tag:

```sh
npm install @effect-agent/core@beta @effect-agent/engine@beta @effect/platform-node effect
```

## 1. Model the boundary

Use Effect Schema for data the application accepts and data the Agent may return.

```ts
import { Schema } from "effect";

class TriageInput extends Schema.Class<TriageInput>("TriageInput")({
  repo: Schema.NonEmptyString,
  issueNumber: Schema.Int,
}) {}

class TriageResult extends Schema.Class<TriageResult>("TriageResult")({
  severity: Schema.Literals(["low", "medium", "high", "critical"]),
  explanation: Schema.NonEmptyString,
}) {}
```

The input is decoded before instructions run. The final output is decoded before the Run can
succeed.

## 2. Define a native Effect AI Tool

Tool handlers can require ordinary application services. Those requirements remain visible in the
eventual Run type.

```ts
import { Context, Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

class IssueUnavailable extends Schema.TaggedError<IssueUnavailable>()("IssueUnavailable", {
  message: Schema.String,
}) {}

class IssueRepo extends Context.Service<
  IssueRepo,
  {
    readonly inspect: (
      repo: string,
      number: number,
    ) => Effect.Effect<{ title: string; sensitive: boolean }, IssueUnavailable>;
  }
>()("IssueRepo") {}

const InspectIssue = Tool.make("inspect_issue", {
  description: "Inspect one issue",
  parameters: Schema.Struct({
    repo: Schema.String,
    number: Schema.Int,
  }),
  success: Schema.Struct({
    title: Schema.String,
    sensitive: Schema.Boolean,
  }),
  failure: IssueUnavailable,
  failureMode: "error",
  dependencies: [IssueRepo],
});

const TriageToolkit = Toolkit.make(InspectIssue);

const TriageToolkitLive = TriageToolkit.toLayer({
  inspect_issue: ({ repo, number }) => Effect.flatMap(IssueRepo, (_) => _.inspect(repo, number)),
});
```

The Definition stays pure. Live clients enter through `TriageToolkitLive` and the `IssueRepo`
Layer supplied by your application.

## 3. Define a finite Agent

```ts
import { Agent, AgentPolicy } from "@effect-agent/core";

const TriageDefinition = Agent.define("triage", {
  input: TriageInput,
  output: TriageResult,
  instructions: ({ repo, issueNumber }) =>
    `Triage ${repo}#${issueNumber}. Escalate sensitive issues.`,
  toolkit: TriageToolkit,
  policy: AgentPolicy.make({
    maxTurns: 6,
    maxToolCalls: 10,
    maxDuration: "2 minutes",
    toolConcurrency: 2,
  }),
});
```

The Agent ID is stable identity, not a display name. Every default policy is finite. Invalid or
unbounded policy values fail at construction.

## 4. Bind one Model

Definitions are model-agnostic. A Model becomes part of the runtime contract only at the explicit
binding seam.

```ts
import { OpenAiLanguageModel } from "@effect/ai-openai";

const Triage = Agent.withModel(TriageDefinition, OpenAiLanguageModel.model("gpt-4.1-mini"));
```

The framework does not choose a Model from ambient configuration. The Effect AI Model's Layer
requirements join the Run's `R`.

## 5. Interpret the Binding

```ts
import { NodeCrypto } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { IdGenerator } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";

const program = AgentRuntime.run(Triage, {
  repo: "Effect-TS/effect",
  issueNumber: 4123,
}).pipe(
  Effect.provide(TriageToolkitLive),
  Effect.provide(IssueRepoLive),
  Effect.provide(IdGenerator.layer.pipe(Layer.provide(NodeCrypto.layer))),
  Effect.provide(OpenAiClientLive),
  Effect.scoped,
);
```

`IssueRepoLive` and `OpenAiClientLive` are application Layers whose exact construction is
intentionally outside the Definition. `IdGenerator.layer` derives the framework identity port from
Effect's platform-neutral `Crypto` service; this Node entrypoint supplies `NodeCrypto.layer`, while
tests replace the whole identity port with a deterministic Layer.

::: tip Deterministic first
The repository's ordinary tests bind the same Definitions to `ScriptedModel`, not a live provider.
The [testing guide](./testing) shows the offline path.
:::

## What the type tells you

<ContractPanel
  success="AgentResult<TriageResult>"
  failure="IssueUnavailable | AiError | decode/policy failures"
  requirements="IssueRepo | Tool handlers | IDs | Model client | Scope"
/>

If you omit a Tool handler Layer or provider client, the Effect cannot run. If a Tool fails with
`IssueUnavailable`, that failure remains in the error channel unless the Tool explicitly opts into
Effect AI's `failureMode: "return"`.

## Next

- [Agent Definitions](./agents) explains inference and immutable identity.
- [Tools & Layers](./tools) explains scheduling and failure behavior.
- [Run & stream](./run-agents) covers all three execution views.

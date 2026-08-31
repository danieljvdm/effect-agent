---
title: Getting started
description: Define, bind, and run a bounded Effect-native Agent.
---

# Getting started

An Effect Agent has five pieces: input and output Schemas, instructions, an Effect AI Toolkit, a
finite policy, and an explicit Model Binding. This page builds one from scratch.

## Installation and compatibility

The public alpha publishes npm prereleases on the opt-in `beta` dist-tag. "Alpha" describes
product maturity; install it through `beta`, with package versions shaped as `X.Y.Z-beta.N`.

For the platform-neutral umbrella and the OpenAI provider used below:

```sh
npm install --save-exact effect-agent@beta effect@4.0.0-rc.111 @effect/ai-openai@4.0.0-rc.111
```

`effect-agent` re-exports core, engine, and capabilities. The examples on this page use their
scoped imports so the owning package is visible. To use those imports directly, declare the
packages you import:

```sh
npm install --save-exact @effect-agent/core@beta @effect-agent/engine@beta effect@4.0.0-rc.111 @effect/ai-openai@4.0.0-rc.111
```

Choose optional packages for the capabilities your application needs:

| Need                             | Install separately                                                                                        |
| -------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Anthropic instead of OpenAI      | `@effect/ai-anthropic@4.0.0-rc.111`                                                                       |
| Persistent history with SQLite   | `@effect-agent/session@beta` and `@effect-agent/storage-sqlite@beta`                                      |
| Node durable accepted work       | `@effect-agent/platform-node@beta`, plus any scoped packages imported directly                            |
| Cloudflare durable accepted work | `@effect-agent/platform-cloudflare@beta` and its `effect-cf` host peer, tested at `0.37.0`                |
| Cloudflare interactive browser   | `@effect-agent/platform-cloudflare@beta` and its optional `@cloudflare/puppeteer` peer, tested at `1.1.0` |
| Trusted local sandbox            | `@effect-agent/sandbox-local@beta`                                                                        |
| Deterministic test Model         | `@effect-agent/testing@beta`                                                                              |

Use the same exact release for all `effect-agent` and `@effect-agent/*` packages. The commands
above resolve `beta` and save exact versions; retain the lockfile. This source tree develops and
tests against `effect`, `@effect/ai-openai`, and `@effect/ai-anthropic` at `4.0.0-rc.111`.
Effect platform, SQL, Atom, and test packages share that development pin;
the contributor compiler plugin `@effect/tsgo` has its own version.
Every public framework package declares `effect` as a required `^4.0.0-rc.111` peer so the
application supplies the shared runtime. This range admits later `4.0.0` release candidates
and stable `4.x` releases. Keep `effect` in your application's dependencies at a version satisfying
both the framework's and your providers' peer ranges. The installation commands above use the
tested development versions. The catalog references used in this repository
are replaced with concrete versions when packages are packed for npm.
The `beta` tag moves, so check the selected release's manifests before upgrading. See the
[provider example](https://github.com/danieljvdm/effect-agent/tree/main/examples/providers)
for direct upstream Model bindings.

Before 1.0, APIs and stored schemas may change incompatibly. No compatibility window or stored-data
migration path is promised. Adapters reject incompatible versions; disposable development data may
be reset. Retaining Conversation history is separate from
[durable accepted work](../concepts/durability).

The umbrella does not acquire provider clients, install Tool handlers, or run a host. Supply those
Layers and credentials in the application. Storage and transport authority also belongs to the
host; read [Authorization and isolation](./operations#authorization-and-isolation) before exposing
Conversation operations to external callers. The [package map](../reference/packages) lists the
available adapters and their limits.

These npm commands are consumer installation examples. Contributors to this repository use
`vp install` and `vp run <task>`; the
[toolchain guide](https://github.com/danieljvdm/effect-agent/blob/main/docs/TOOLCHAIN.md)
defines the Vite+ commands.

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
import { Effect } from "effect";
import { IdGenerator } from "@effect-agent/core";
import { AgentRuntime, ConversationHistory } from "@effect-agent/engine";

const program = AgentRuntime.run(Triage, {
  repo: "Effect-TS/effect",
  issueNumber: 4123,
}).pipe(
  Effect.provide(TriageToolkitLive),
  Effect.provide(IssueRepoLive),
  Effect.provide(IdGenerator.layer),
  Effect.provide(ConversationHistory.layerTransient),
  Effect.provide(OpenAiClientLive),
);
```

`IssueRepoLive` and `OpenAiClientLive` are application Layers whose exact construction is
intentionally outside the Definition. `IdGenerator.layer` is the framework's default identity
authority backed by Web Crypto's `randomUUID`; tests replace it with a deterministic Layer.
`run` closes run-owned resources before returning, so this program needs no extra `Effect.scoped`.
Application Layers close when their enclosing `Effect.provide` finishes. If caller-supplied
operations require `Scope`, that requirement remains visible and must still be provided.

::: tip Deterministic first
The repository's ordinary tests bind the same Definitions to `ScriptedModel`, not a live provider.
The [testing guide](./testing) shows the offline path.
:::

## What the type tells you

<ContractPanel
  success="AgentResult<TriageResult>"
  failure="IssueUnavailable | AiError | decode/policy failures"
  requirements="IssueRepo | Tool handlers | IDs | Model client | ConversationHistory"
/>

If you omit a Tool handler Layer or provider client, the Effect cannot run. If a Tool fails with
`IssueUnavailable`, that failure remains in the error channel unless the Tool explicitly opts into
Effect AI's `failureMode: "return"`.

## Next

- [Agent Definitions](./agents) explains inference and immutable identity.
- [Tools & Layers](./tools) explains scheduling and failure behavior.
- [Run & stream](./run-agents) covers all three execution views.

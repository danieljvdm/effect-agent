# Effect Agent

Effect Agent is a TypeScript agent framework built on Effect and Effect AI. It keeps the parts an
Effect application relies on: typed failures, visible dependencies, Schema boundaries, and scoped
resources. Flue and Pi informed parts of the agent loop, interaction model, and durability design.

The working product thesis is:

> An agent is an immutable, schema-defined program. Running it produces an `Effect`; observing it
> produces a `Stream`; its expected failures remain in `E`; its dependencies remain in `R`; every
> resource belongs to a `Scope`.

## Status

| Item         | Current state                                                             |
| ------------ | ------------------------------------------------------------------------- |
| Distribution | Public beta, published to npm on the `beta` dist-tag                      |
| Effect       | `effect` and OpenAI/Anthropic provider packages at exact `4.0.0-rc.111`   |
| Packages     | `effect-agent` umbrella and optional `@effect-agent/*` packages           |
| Workspace    | Vite+ monorepo with packages in `packages/*` and examples in `examples/*` |
| Platforms    | Node.js with SQLite and Cloudflare Workers with Durable Objects           |

Tests cover the ephemeral interpreter, durable Node runtime (`DN`), Cloudflare runtime (`DC`),
Tool uncertainty, joined input, attached Subagents,
adapter certification, formal models, failpoints, and soak behavior.

`DN` and `DC` use the same coordinator. They provide durable admission, fenced Attempts, one
Settlement per accepted Submission, approval suspension, joined input, and attached child
delegation. Execution is at least once. Neither runtime claims exactly-once external effects.

The work-order examples and `work-order-action/` provide head-bound GitHub dispatch, separate
model/check/publisher jobs, and atomic publication. They remain separate from the read-only PR
reviewer.

Completion is an engineering claim, not a stability claim. Cloudflare durability evidence comes
from workerd/Miniflare; an opt-in temporary deployment proves the Browser Run Worker binding
against Cloudflare's hosted service. The sandbox also exposes bounded page capture, PNG screenshot,
and scoped same-host Markdown crawl ports; Cloudflare supplies native-binding and REST adapters.
Live-model and live-provider suites remain opt-in.

Releases use `X.Y.Z-beta.N` versions on npm's `beta` channel.
APIs and stored data may change incompatibly before 1.0. There is no compatibility window or
stored-data migration promise; incompatible data must fail clearly and may need a reset.
Keep framework packages at one exact release and choose Effect/provider versions that satisfy
their peer ranges. See [installation and compatibility](docs/guide/getting-started.md#installation-and-compatibility).

Normative words such as **MUST**, **SHOULD**, and **MAY** are used in their usual RFC sense.

## Read this first

The documentation site lives in [`docs/`](docs/index.md); run it locally with
`vp run docs:dev`.

1. [Domain glossary](GLOSSARY.md)
2. [Repository toolchain](docs/TOOLCHAIN.md)
3. [Instructions for implementation agents](AGENTS.md)

## Install

The platform-neutral umbrella includes core, engine, and capabilities:

```sh
npm install --save-exact effect-agent@beta
```

Requires `effect@^4.0.0-rc.111` and an Effect AI provider, such as
`@effect/ai-openai@4.0.0-rc.111`. See [installation and compatibility](docs/guide/getting-started.md#installation-and-compatibility).

Provider Layers, credentials, and Tool handlers come from the application. Persistent history,
durable hosts, storage, and sandbox adapters are separate installs. See the
[package map](docs/reference/packages.md#capability-inventory) for implemented APIs, bundled
adapters, host requirements, and unsupported capabilities.

## Architecture decisions

The [concept guides](docs/concepts/effect-native.md) explain these choices:

- build an Effect-native core;
- use Effect AI Tool, Toolkit, LanguageModel, Prompt, Response, and Model directly;
- separate the canonical Thread Log from the operational Submission Ledger;
- represent uncertain external effects instead of blindly replaying them;
- keep framework packages deliberately scoped and runnable consumer benches in leaf `examples/*`
  workspaces;
- use bounded-parallel Tool execution with deterministic result order;
- deliver steering, follow-up, and joined input only at safe Turn seams.

## Architectural summary

```text
Application Agent Definitions
   + native Model Layers
 Effect AI Tool + Toolkit + Model
      Effect + Schema + Layer
                  |
        @effect-agent/core
                  |
        @effect-agent/engine
          /                \
 Thread/Submission  capabilities
          \                /
        Node or Cloudflare host
```

The engine uses Effect AI directly. It owns only the additional agent-loop, Thread,
Submission, recovery, and durability concepts. Provider SDK internals, database drivers,
transports, and platform types must not become canonical domain records.

The filesystem contains only the packages the framework needs today, not every possible future
package. New packages are added only for a genuinely new framework concern agreed with the
repository owner.

## Repository commands

```sh
vp install
vp run docs:dev
vp run ready
```

`vp run ready` runs static checks, tests, package builds, and the documentation build with link
validation. Vite+ is the command authority; Bun is its package manager. See the
[toolchain guide](docs/TOOLCHAIN.md) for version synchronization, installed Effect source,
package creation, hooks, and CI.

## Product boundaries

The beta includes bounded execution, persistent history, durable accepted work, scheduled input,
and event subscriptions. It does not provide hosted execution, a turnkey chat UI, a visual builder,
runtime Skills, separate persistent agent memory/state, arbitrary Thread metadata, or generic dynamic
Turn Plans. Nested Subagent delegation, handoff, detachment, and a marketplace remain unsupported.
MCP exposes a validated connector port whose transport the application implements. The local
sandbox runs trusted code without isolation. See the
[capability inventory](docs/reference/packages.md#capability-inventory) and
[host isolation responsibilities](docs/guide/operations.md#authorization-and-isolation).

Likewise, thread persistence is not the same as durable execution. The framework may only
claim durable execution after it can demonstrate:

- durable admission before acknowledging work;
- exactly one durable terminal settlement for every accepted submission;
- conservative recovery of uncertain external effects;
- fencing of stale workers;
- deterministic crash recovery at every specified failpoint;
- adapter conformance and state-machine verification.

## Reference projects

- [Effect](https://github.com/Effect-TS/effect)
- [Flue](https://github.com/withastro/flue)
- [Pi](https://github.com/earendil-works/pi)
- [Vite+ Effect/Cloudflare template](https://github.com/danieljvdm/vp-effect-cf-template)
- [Contributor agent skills](https://github.com/danieljvdm/agent-skills)

Flue and Pi are source material. Effect and Effect AI are the runtime foundation.

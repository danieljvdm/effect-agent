# Effect Agent

Effect Agent is a TypeScript agent framework built on Effect and Effect AI. It keeps the parts an
Effect application relies on: typed failures, visible dependencies, Schema boundaries, and scoped
resources. Flue and Pi informed parts of the agent loop, interaction model, and durability
specification.

The working product thesis is:

> An agent is an immutable, schema-defined program. Running it produces an `Effect`; observing it
> produces a `Stream`; its expected failures remain in `E`; its dependencies remain in `R`; every
> resource belongs to a `Scope`.

## Status

| Item          | Current state                                                             |
| ------------- | ------------------------------------------------------------------------- |
| Specification | Draft                                                                     |
| Distribution  | Private internal project                                                  |
| Effect        | Effect v4, pinned exactly during pre-1.0 development                      |
| Packages      | `@effect-agent/*`                                                         |
| Workspace     | Vite+ monorepo with packages in `packages/*` and examples in `examples/*` |
| Platforms     | Node.js with SQLite and Cloudflare Workers with Durable Objects           |

The planned build through Phase 7 is implemented. Tests cover the ephemeral interpreter, durable
Node runtime (`DN`), Cloudflare runtime (`DC`), Tool uncertainty, joined input, attached Subagents,
adapter certification, formal models, failpoints, and soak behavior.

`DN` and `DC` use the same coordinator. They provide durable admission, fenced Attempts, one
Settlement per accepted Submission, approval suspension, joined input, and attached child
delegation. Execution is at least once. Neither runtime claims exactly-once external effects.

The work-order examples and `work-order-action/` provide head-bound GitHub dispatch, separate
model/check/publisher jobs, and atomic publication. They remain separate from the read-only PR
reviewer.

Completion is an engineering claim, not a stability claim. Cloudflare evidence comes from
workerd/Miniflare, live-model suites are opt-in, and open-source preparation remains pending.

Normative words such as **MUST**, **SHOULD**, and **MAY** are used in their usual RFC sense.

## Read this first

The documentation site lives in [`docs/`](docs/index.md); run it locally with
`bun run docs:dev`. The specifications below remain the normative design source.

1. [Domain glossary](GLOSSARY.md)
2. [Repository toolchain](docs/TOOLCHAIN.md)
3. [Instructions for implementation agents](AGENTS.md)

## Detailed specifications

| Area                                                    | Document                                                    |
| ------------------------------------------------------- | ----------------------------------------------------------- |
| Authoring, schemas, tools, Layers                       | [Authoring model](docs/spec/authoring.md)                   |
| Turn loop, events, errors, concurrency                  | [Runtime engine](docs/spec/runtime.md)                      |
| Effect AI and model provider integration                | [Model providers](docs/spec/providers.md)                   |
| Sessions, tools, skills, MCP, sandboxes, subagents      | [Capabilities](docs/spec/capabilities.md)                   |
| Declared delegation, child lifecycle, budgets, recovery | [Subagents](docs/spec/subagents.md)                         |
| Accepted work and crash recovery                        | [Durability](docs/spec/durability.md)                       |
| Conversation history, work ledger, and storage adapters | [Persistence](docs/spec/persistence.md)                     |
| Node and Cloudflare hosts                               | [Deployment](docs/spec/deployment.md)                       |
| Security, approvals, tenancy, redaction                 | [Security and operations](docs/spec/security-operations.md) |
| Tests, fault injection, model checking                  | [Verification](docs/spec/testing.md)                        |
| Versioning and compatibility                            | [Compatibility](docs/spec/compatibility.md)                 |
| Pull-request work orders                                | [Work orders](docs/spec/pr-work-orders.md)                  |
| GitHub work-order dispatch and publication              | [Work-order ingress](docs/spec/pr-work-order-ingress.md)    |
| Single-pass pull-request review and GitHub channel      | [PR review](docs/spec/pr-review.md)                         |

## Architecture decisions

The specifications and concept pages record the rationale and invariants for these choices:

- build an Effect-native core;
- use Effect AI Tool, Toolkit, LanguageModel, Prompt, Response, and Model directly;
- separate the canonical Conversation Log from the operational Submission Ledger;
- represent uncertain external effects instead of blindly replaying them;
- keep framework packages deliberately scoped and runnable consumer benches in leaf `examples/*`
  workspaces;
- use bounded-parallel Tool execution with deterministic result order;
- deliver steering, follow-up, and joined input only at safe Turn seams.

## Architectural summary

```text
Application Agent Definitions
   + explicit Model Bindings
 Effect AI Tool + Toolkit + Model
      Effect + Schema + Layer
                  |
        @effect-agent/core
                  |
        @effect-agent/engine
          /                \
 Conversation/Submission  capabilities
          \                /
        Node or Cloudflare host
```

The engine uses Effect AI directly. It owns only the additional agent-loop, Conversation,
Submission, recovery, and durability concepts. Provider SDK internals, database drivers,
transports, and platform types must not become canonical domain records.

The filesystem contains only the packages the framework needs today, not every possible future
package. New packages are added only for a genuinely new framework concern agreed with the
repository owner.

## Repository commands

```sh
bun install
bun run sync:agent-skills
bun run docs:dev
bun run ready
```

`bun run ready` runs the repository checks, package tests, and library builds. See the
[toolchain guide](docs/TOOLCHAIN.md) for version synchronization, the Effect source checkout,
package creation, hooks, and CI.

## Product boundaries

The first release is the bounded Effect-native runtime described by the roadmap. Hosted execution,
channels, a chat UI, a visual builder, an embedded coding sandbox, Subagent extensions beyond the
implemented attached S1/S2 slices (nested delegation, handoff, detachment), and a marketplace
remain outside that first release. Later packages may add them after the core loop is
deterministic, typed, testable, and resource-safe.

Likewise, conversation persistence is not the same as durable execution. The framework may only
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

Flue and Pi are source material. All normative behavior is restated in the Effect-native
specifications. Effect and Effect AI are the runtime foundation.

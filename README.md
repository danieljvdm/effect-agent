# Effect Agent

Effect Agent is an Effect-native framework for building general-purpose autonomous TypeScript
agents. Effect and Effect AI are its implementation foundation. Flue and Pi are attributed
research sources whose agent-loop, interaction, and durability behavior informed parts of the
native specification.

The working product thesis is:

> An agent is an immutable, schema-defined program. Running it produces an `Effect`; observing it
> produces a `Stream`; its expected failures remain in `E`; its dependencies remain in `R`; every
> resource belongs to a `Scope`.

This repository contains the product and technical specification plus the completed Phase 0 design
proof, Phase 1 interpreter, Phase 2 operational local runtime, and Phase 3 persistent Conversation
foundation. The workspace includes schema-first Agent contracts, one shared `run`/`stream`
interpreter, scoped operational capabilities, a narrow local sandbox adapter, replayable canonical
Conversation records with memory and SQLite adapters, deterministic scripted model Layers, the
progressive Travel Planner reference slice, provider-binding compile examples, and a browser test
bench under `examples/demo`.

## Status

- Specification status: **Draft**
- Distribution status: **Private internal project**
- Target Effect line: **Effect v4**, pinned exactly during pre-1.0 development
- Working package scope: `@effect-agent/*`
- Repository shape: **Vite+ monorepo** with framework packages in `packages/*`, leaf consumer
  benches in `examples/*`, and no `apps/`
- Current packages: `core`, `engine`, `capabilities`, `sandbox`, `sandbox-local`, `session`,
  `storage-memory`, `storage-sqlite`, and `testing`
- Current implementation milestone: **Phase 3 complete** (persistent Conversation foundation);
  **Phase 4 next**
- Target platforms: Node.js/SQLite and Cloudflare Workers/Durable Objects
- First runtime: bounded, ephemeral multi-Run Conversations with safe-seam input, approval,
  context, budget, MCP, and sandbox capabilities
- Persistent foundation: versioned canonical records, pure replay/checkpoints, definition digests,
  opaque resumable offsets, export, and memory/SQLite Conversation Store Layers
- Durable runtime: planned, but it must pass the durability gates before the product may claim
  durable execution

Normative words such as **MUST**, **SHOULD**, and **MAY** are used in their usual RFC sense.
Decisions labeled **Proposed** are recommendations, not settled owner decisions.

## Read this first

The curated, future-facing documentation site lives in [`docs/`](docs/index.md). Run it locally
with `bun run docs:dev`; its guides distinguish implemented APIs from next, planned, and proposed
interfaces. The specifications below remain the normative design source.

1. [Product specification](docs/PRODUCT.md)
2. [Domain language](CONTEXT.md)
3. [Technical architecture](docs/ARCHITECTURE.md)
4. [Owner decision register](docs/DECISIONS.md)
5. [Implementation roadmap](docs/ROADMAP.md)
6. [Repository toolchain](docs/TOOLCHAIN.md)
7. [Requirements and traceability index](docs/REQUIREMENTS.md)
8. [Instructions for implementation agents](AGENTS.md)
9. [AI-driven project execution guide](docs/guides/project-execution.md)
10. [Target API examples](docs/guides/examples.md)
11. [Progressive Travel Planner reference application](docs/guides/travel-planner.md)
12. [Reference-project analysis](docs/REFERENCE-ANALYSIS.md)
13. [Phase 1 evidence](docs/PHASE-1-EVIDENCE.md)
14. [Phase 2 evidence](docs/PHASE-2-EVIDENCE.md)
15. [Phase 3 evidence](docs/PHASE-3-EVIDENCE.md)

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

## Architecture decisions

The [architecture decision records](docs/adr/README.md) explain the highest-impact
choices, rationale, and invariants:

- build an Effect-native core;
- use Effect AI Tool, Toolkit, LanguageModel, Prompt, Response, and Model directly;
- separate the canonical Conversation Log from the operational Submission Ledger;
- represent uncertain external effects instead of blindly replaying them;
- keep framework packages phase-gated and runnable consumer benches in leaf `examples/*`
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

The filesystem reflects the active roadmap phase rather than every possible future package. New
packages are added only when their phase begins.

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
channels, a chat UI, a visual builder, an embedded coding sandbox, durable Subagents, and a
marketplace remain outside that first release. Later packages may add them after the core loop is
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

Flue and Pi are source material documented in
[REFERENCE-ANALYSIS.md](docs/REFERENCE-ANALYSIS.md). All normative behavior is restated in the
Effect-native specifications. Their pinned source snapshots live in the shallow Git submodules at
`repos/flue` and `repos/pi`; the matching Effect source lives at `repos/effect`. Effect and Effect
AI are the runtime foundation.

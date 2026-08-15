# Effect Agent

Effect Agent is an Effect-native framework for building general-purpose autonomous TypeScript
agents. Effect and Effect AI are its implementation foundation. Flue and Pi are attributed
research sources whose agent-loop, interaction, and durability behavior informed parts of the
native specification.

The working product thesis is:

> An agent is an immutable, schema-defined program. Running it produces an `Effect`; observing it
> produces a `Stream`; its expected failures remain in `E`; its dependencies remain in `R`; every
> resource belongs to a `Scope`.

This repository contains the product and technical specification plus every completed roadmap
phase: the Phase 0 design proof, Phase 1 interpreter, Phase 2 operational local runtime, Phase 3
persistent Conversation foundation, Phase 4 durable Node/SQLite runtime, Phase 5 durable Tools
and joined input, Phase 6 Cloudflare Durable Object runtime, and Phase 7 internal hardening
(adapter certification, TLA+ model checking, administrative operations, threat model and
security review, chaos/soak evidence, and three internal Agents). The workspace includes
schema-first Agent contracts,
one shared `run`/`stream` interpreter, scoped operational capabilities, a narrow local sandbox
adapter, replayable canonical Conversation records with memory and SQLite adapters, the durable
Submission Ledger, recovery classifier, and `DurableAgentRuntime` coordinator with the Node host
assembly — covering prepared/settled ordinary Tool records, Unknown Outcomes, Durable Steps,
durable approval suspension, joined queued input, and durable attached Subagent delegation — the
Cloudflare assembly running the same coordinator inside one SQLite-backed Durable Object per
Conversation with alarm-driven recovery and cross-Object delegation, deterministic scripted
model Layers, the progressive Travel Planner reference slice, provider-binding compile examples,
and a browser test bench under `examples/demo`.

## Status

- Specification status: **Draft**
- Distribution status: **Private internal project**
- Target Effect line: **Effect v4**, pinned exactly during pre-1.0 development
- Working package scope: `@effect-agent/*`
- Repository shape: **Vite+ monorepo** with framework packages in `packages/*`, leaf consumer
  benches in `examples/*`, and no `apps/`
- Current packages: `effect-agent` (umbrella over the pure surface), `core`, `engine`, `capabilities`, `sandbox`, `sandbox-local`, `session`,
  `storage-memory`, `storage-sqlite`, `storage-cloudflare`, `platform-node`,
  `platform-cloudflare`, `pr-review` (packaged pull-request reviewer with a prebuilt GitHub
  Action at `action/`), and `testing`
- Current implementation milestone: the full build-out is complete — the ephemeral interpreter,
  the durable Node/SQLite runtime (`DN`), durable Tools and joined input, the Cloudflare Durable
  Object runtime (`DC`), internal hardening (certification, formal models, chaos), and both Subagent slices,
  each proven by named test suites in the tree
- Stated plainly: completion is an engineering claim, not a stability one — no hosted-Cloudflare
  evidence is claimed (the `DC` harness is workerd/Miniflare); the opt-in live-model suites are
  implemented and gate-skipped, with live execution a release-lane action; open-source
  preparation remains the deferred next chapter
- Subagents: declared attached delegation Tools are implemented for both slices — ephemeral
  (`E`) delegation with the engine spawner seam and in-memory budget reservations, and durable
  (`DN`) attached children as separately admitted Submissions with requested/started/joined
  canonical records, parent-owned budget reservations, `waitingForChild` suspension and durable
  wakeup, verified Settlement joins, independent parent/child fencing, durable abort
  propagation, and exact-digest Binding resolution under real process-kill tests; `DC` Subagents
  run the same matrix across two Durable Objects under eviction, and no exactly-once child
  external effects are claimed
- Target platforms: Node.js/SQLite and Cloudflare Workers/Durable Objects
- First runtime: bounded, ephemeral multi-Run Conversations with safe-seam input, approval,
  context, budget, MCP, and sandbox capabilities
- Persistent foundation: versioned canonical records, pure replay/checkpoints, definition digests,
  opaque resumable offsets, export, and memory/SQLite Conversation Store Layers
- Durable runtime: class `DN` on the tested Node/SQLite assembly — durable admission and Receipt,
  FIFO Conversation lanes, fenced Attempts with liveness leases, pure recovery classification,
  and exactly one Settlement per accepted Submission; consequential external mutation runs under
  the Phase 5 uncertainty protocol (prepared/settled ordinary Tool records, Unknown Outcomes with
  an audited resolution path, Durable Steps, durable approval suspension, and joined queued
  input); execution stays honestly at-least-once — recovery may re-invoke the model and never
  claims exactly-once external side effects
- Cloudflare runtime: class `DC` on the tested workerd/Miniflare assembly — the same coordinator
  and ports inside one SQLite-backed Durable Object per Conversation, a single multiplexed
  pre-armed alarm so eviction at every failpoint recovers without an incoming request,
  admission limits checked before any ledger row, cross-Object Subagent delegation over a typed
  routed port subset, and Travel Planner canonical outcomes byte-equal to `DN` after the
  documented cross-platform normalization

Normative words such as **MUST**, **SHOULD**, and **MAY** are used in their usual RFC sense.

## Read this first

The curated, future-facing documentation site lives in [`docs/`](docs/index.md). Run it locally
with `bun run docs:dev`; its guides distinguish implemented APIs from next, planned, and proposed
interfaces. The specifications below remain the normative design source.

1. [Product specification](docs/PRODUCT.md)
2. [Domain language](CONTEXT.md)
3. [Technical architecture](docs/ARCHITECTURE.md)
4. [Repository toolchain](docs/TOOLCHAIN.md)
5. [Requirements and traceability index](docs/REQUIREMENTS.md)
6. [Instructions for implementation agents](AGENTS.md)
7. [Target API examples](docs/guides/examples.md)
8. [Progressive Travel Planner reference application](docs/guides/travel-planner.md)

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

The architecture rests on a small set of high-impact choices whose rationale and invariants
are recorded in the specifications and concept pages:

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

Flue and Pi are source material. All normative behavior is restated in the
Effect-native specifications. Their pinned source snapshots live in the shallow Git submodules at
`repos/flue` and `repos/pi`; the matching Effect source lives at `repos/effect`. Effect and Effect
AI are the runtime foundation.

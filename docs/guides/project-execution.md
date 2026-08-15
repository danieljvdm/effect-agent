# AI-driven Project Execution Guide

Status: Draft

This guide turns the specification into a coordinated implementation program. It
supplements the repository's
[AGENTS.md](https://github.com/danieljvdm/effect-agent/blob/main/AGENTS.md), which contains
mandatory repository rules.

## 1. Operating model

Use a dependency-ordered program with parallel work inside each phase:

```text
owner decisions
      ↓
domain Schemas and ports
      ↓
pure state machines ────── scripted adapters
      ↓                         ↓
engine integration and conformance
      ↓
persistence semantics ─── store adapters
      ↓
recovery model ─────────── hosts and operations
      ↓
fault, security, and compatibility gates
```

The project lead owns public coherence. Parallel agents own bounded packages,
adapters, tests, or documents. Shared public Schemas are changed through a designated
domain integrator.

## 2. Work item template

Every work item must state:

```md
# Outcome

Observable behavior delivered.

## In scope

Exact packages and behaviors.

## Out of scope

Adjacent behavior intentionally excluded.

## Requirements

Stable requirement IDs.

## Decision dependencies

Decision IDs and ADRs. Say whether each is Accepted or only Proposed.

## Required skills

Name the synced skill and focused reference files the task must follow.

## Contract

Input, output, errors, environment, events, interruption, ordering, idempotency,
resource lifetime, security, and telemetry.

## Verification

Type, unit, generated, conformance, integration, crash, security, and documentation
evidence as applicable.

## Handoff

Files changed, remaining risks, and newly discovered decisions.
```

An agent must not receive “implement the runtime” as a single task.

## 3. Skill routing

Dev Kit 0.17 moved the general Effect guidance upstream: `node_modules/effect/AGENTS.md` (and the
documents it links) is the canonical Effect reference, with the pinned checkout under
`repos/effect` for source-level questions. The project-local skills in `.agents/skills` are now
focused: a task names only the skill that matches its work; agents do not need to load every
skill for every change.

| Work in scope                                            | Required reference                                                               | Planning consequence                                                                                                       |
| -------------------------------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Any Effect code (version, imports, APIs, Schema, Layers) | `node_modules/effect/AGENTS.md`, then `repos/effect` source for exact signatures | Verify the pinned v4 source before coding; use the current Effect v4 API rather than remembered v3 patterns.               |
| Repository scripts, CLIs, and shell/process automation   | `build-effect-clis/SKILL.md` (+ its `references/`)                               | Use `effect/unstable/cli`, platform services, argument arrays, typed command errors, and a harmless help or dry-run check. |
| Service design or whole-package service audits           | `effect-architecture-audit/SKILL.md`                                             | Audit contracts, accessors, constructors, Layers, configuration, requirements, errors, lifecycle, and test evidence.       |
| Unit, service, runtime, and conformance tests            | `testing/SKILL.md` plus `node_modules/effect/AGENTS.md` testing guidance         | Use Layers and controllable clocks/providers; assert typed exits; never use wall-clock sleeps in green tests.              |
| An HTTP transport boundary                               | `build-effect-apis/SKILL.md` (+ its `references/`)                               | Keep contracts explicit and handlers thin; business orchestration remains in services.                                     |
| Browser client state (examples/demo)                     | `effect-atom-state/SKILL.md`                                                     | Compose client workflows as atoms with reactivity keys; keep the Effect→Promise boundary at the React edge.                |
| Cloudflare adapters (storage/platform-cloudflare)        | `cloudflare/SKILL.md` and `durable-objects/SKILL.md`                             | Follow retrieved platform guidance for Workers, DO storage, alarms, and wrangler-free test wiring.                         |
| Preparing or opening a pull request                      | `open-pull-request/SKILL.md`                                                     | Conventional commits, reviewer-complete descriptions, links to vital code, and concrete evidence.                          |

Review rejects code that technically works but bypasses the applicable skill pattern without a
documented architectural reason. A shared skill is guidance, not a substitute for this project's
stricter durability, security, ordering, and Effect AI requirements.

## 4. Planning granularity

A good task:

- has one primary package owner;
- implements one state transition family or port;
- can be verified without unfinished unrelated packages;
- changes few shared Schemas;
- declares exact dependencies and stubs;
- has a reviewable failure surface.

Examples:

- implement and type-test `Tool.make`;
- build the scripted model event grammar;
- implement the pure Turn reducer;
- add SQLite idempotent append conformance;
- inject crashes around terminal commit;
- author the approval capability threat tests.

## 5. Integration sequence

For each phase:

1. Freeze the relevant proposed decisions or mark experiments explicitly.
2. Add/adjust domain Schemas and fixtures.
3. Add pure transition and policy tests.
4. Implement ports and scripted test doubles.
5. Implement the engine/capability behavior.
6. Add storage/platform adapters and upstream Effect AI provider Models.
7. Run cross-package conformance.
8. Advance the Travel Planner Reference Application through the phase's new behavior, then update
   focused examples and public docs.
9. Produce exit-gate evidence.
10. Conduct a phase review before enabling the next claim.

Adapters should not invent missing domain semantics. They raise an integration
proposal to the domain integrator.

## 6. Branch and ownership rules

The exact version-control workflow is project-local, but these ownership rules are
required:

- one active integrator for `@effect-agent/core` public exports;
- one active integrator for canonical record Schemas;
- one active integrator for durability state transitions;
- storage and platform adapters may proceed independently against frozen service contracts;
- generated fixtures are updated only with their Schema owner;
- changes to shared invariants receive cross-workstream review.

Avoid parallel edits to the same public union. Prefer additive leaf variants followed
by one integration change.

## 7. Agent prompt packet

Every coding agent receives:

- `README.md`;
- `CONTEXT.md`;
- `AGENTS.md`;
- `docs/TOOLCHAIN.md`;
- relevant specification sections;
- related ADRs and decisions;
- current package tree;
- neighboring tests;
- the exact synced skill and focused references selected through section 3;
- exact verification commands;
- explicit non-goals;
- a named reviewer/integrator.

The prompt asks the agent to report ambiguity instead of choosing a new public
semantic silently.

## 8. Review roles

Use independent reviews for high-risk changes:

- **domain reviewer**: vocabulary, Schema, `A/E/R`, dependency direction;
- **runtime reviewer**: state transitions, concurrency, interruption, backpressure;
- **durability reviewer**: atomicity, idempotency, fencing, uncertainty, liveness;
- **security reviewer**: trust boundaries, authority, secrets, redaction;
- **operations reviewer**: diagnostics, reset/restore, overload;
- **DX reviewer**: public examples, inference, error messages.

One reviewer may cover multiple roles for small work. Durable protocol changes
require domain, durability, and verification review.

## 9. Verification evidence packet

A phase exit packet contains:

- exact source commit;
- decision and ADR snapshot;
- requirement coverage report;
- package graph and forbidden-import result;
- applicable skill conformance and any justified exceptions;
- type and test results;
- conformance matrix by adapter;
- skipped/quarantined test list and reasons;
- crash-point results where applicable;
- security findings and ownership;
- benchmark environment/results;
- reset/restore drill result where applicable;
- known limitations and claims allowed for the release.

Release marketing language must not exceed this evidence.

## 10. Escalation triggers

Stop and request owner or integrator input when:

- a task depends on an Open decision;
- preserving typed `E` or `R` appears impossible;
- a provider SDK, database, or platform type would enter canonical domain records;
- code proposes a framework-owned copy of an Effect AI primitive;
- a new durable transition or atomicity boundary is needed;
- a tool recovery policy could duplicate external effects;
- a change weakens tenant or secret boundaries;
- an adapter cannot satisfy conformance;
- a test requires disabling a documented invariant;
- a public term conflicts with `CONTEXT.md`.

Record the question in `docs/DECISIONS.md` if it has project-wide impact.

## 11. First implementation backlog

The first executable batch should be:

1. validate and preserve the existing Vite+/Bun toolchain and package graph enforcement;
2. branded IDs and core error/event Schemas using the schema-first modeling reference;
3. direct Effect AI Tool, Toolkit, Handler Layer composition using the services/Layers reference
   and type tests;
4. `Agent.define`, `Agent.withModel`, policies, and input/output Schema using schema-derived types;
5. scripted Effect AI LanguageModel Layer using explicit Effect service requirements;
6. pure Turn state and named transition functions using `Effect.fn` only where transitions are
   effectful;
7. streaming interpreter with one finite-concurrency Tool batch;
8. Scope/interruption and bounded event delivery tests;
9. opt-in Anthropic and OpenAI Travel Planner examples using Effect AI provider Models;
10. the first two-Turn Travel Planner slice plus one focused secondary Agent as package-local,
    compiling test fixtures.

Do not begin SQLite or durable submission work until the Phase 1 engine invariants
are executable and stable.

## 12. Definition of done

A task is done only when:

- behavior and all expected failure paths are implemented;
- public type inference matches the spec;
- resource lifetimes are tested;
- telemetry and redaction are present;
- relevant requirements have automated evidence;
- docs/examples compile;
- applicable synced skill completion checks pass, or a justified exception is recorded;
- no new undecided project-wide semantic is hidden in code;
- the integrator can explain how the change behaves under interruption and, when
  applicable, process loss.

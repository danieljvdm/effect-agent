# Product specification

Status: **Draft**

## 1. Product thesis

Effect Agent is the missing autonomous-agent interpreter for Effect applications.

Effect applications already have typed failures, dependency requirements, schemas, resource
lifecycles, structured concurrency, streams, tracing, schedules, and test Layers. Existing agent
frameworks commonly erase those properties behind configuration objects, hooks, callbacks,
Promises, mutable session classes, and untyped JSON tool results.

Effect Agent preserves the host application's semantics:

```ts
AgentRuntime.run(agent, input);
// Effect<AgentResult<Output>, AgentFailure | DomainFailure, Requirements>

AgentRuntime.stream(agent, input);
// Stream<RunEvent<Output>, AgentFailure | DomainFailure, Requirements>
```

The initial wedge is bounded ephemeral execution:

> Define and run bounded autonomous agents whose schemas, tools, failures, dependencies,
> cancellation, resources, and semantic events remain typed end-to-end.

The long-term destination adds persistent Conversations and rigorously verified durable execution,
but those are separately gated product capabilities.

## 2. Target users

### Primary

1. **Effect application teams**
   - Reuse existing domain modules and Layers inside agent Tools.
   - Retain typed errors, tracing, test clocks, retries, and resource finalization.
   - Add autonomous behavior without adopting a second application runtime.

2. **Platform and library authors**
   - Publish reusable Agent Definitions whose `E` and `R` communicate the operational contract.
   - Replace providers, handlers, persistence, and hosts with Layers.
   - Certify adapters against shared contract suites.

3. **Backend and developer-tool teams**
   - Graduate from one-shot model calls to bounded tool loops.
   - Stream semantic progress, inspect transcripts, and interrupt safely.
   - Run locally without requiring a hosted control plane.

### Secondary

- Teams evaluating approval workflows and durable execution.
- CLI and coding-tool authors.
- Evaluation and deterministic simulation authors.

### Not initially targeted

- No-code users.
- Consumer chatbot teams seeking a turnkey UI.
- Teams unwilling to adopt Effect.
- Buyers primarily seeking a hosted agent control plane.

## 3. Jobs to be done

| Job                                                   | Product outcome                                             |
| ----------------------------------------------------- | ----------------------------------------------------------- |
| Use application capabilities from an agent            | Tool handlers require normal Effect modules in `R`          |
| Know what can fail                                    | Declared failures remain a closed, inspectable union in `E` |
| Replace infrastructure in tests                       | Providers, handlers, clocks, IDs, and stores are Layers     |
| Observe work without controlling it through callbacks | One scoped semantic `Stream`                                |
| Prevent runaway agents                                | Bounded policies are mandatory by default                   |
| Cancel safely                                         | Fiber interruption propagates through the owned Scope       |
| Resume accepted work after crashes                    | Optional durable runtime with explicit recovery semantics   |
| Audit what happened                                   | Canonical, schema-versioned records and stable identities   |

## 4. Product principles

### 4.1 Effect all the way down

- Public async interfaces MUST return `Effect` or `Stream`.
- Dependencies MUST remain visible in `R`.
- Expected failures MUST remain visible in `E`.
- Resource lifetime MUST be represented by `Scope`.
- Interruption MUST use normal Effect semantics within one Attempt.

Promise and `AbortSignal` interop belongs only at Effect AI/provider or platform boundaries.

### 4.2 Effect Schema is the source of truth

Tool parameters, successes, expected failures, structured final output, transport envelopes, and
persisted records MUST originate from Effect Schema. JSON Schema and wire codecs are derived
representations. Application authors MUST NOT maintain a parallel Valibot, Zod, or provider schema.

### 4.3 Immutable definitions, interpreted execution

Agent, Tool, Toolkit, policy, and Skill definitions are immutable values. Implementations arrive
through Layers. Mutable runtime state is private to a scoped interpreter or reconstructed from
canonical records.

### 4.4 Events observe; they do not steer internals

Subscribers may render, log, meter, persist, or project events. Arbitrary subscriber callbacks
MUST NOT participate in state transitions. Steering, approvals, and follow-ups use explicit
commands and queues.

### 4.5 Bounded autonomy

Every default policy specifies finite maximums for Turns, Tool Calls, duration, and concurrency.
Token and cost budgets SHOULD be supported when the Effect AI Model/provider reports them. Unlimited
execution is an explicit expert opt-in.

Bounded does not mean brittle: by default, Turn and Tool Call exhaustion resolve through one
constrained final-answer opportunity, and the Run completes with the honest
`finishReason: "budget-exhausted"` instead of discarding finished work. The strict
run-fatal resolution remains an explicit policy choice, and duration, token, and cost bounds are
always hard rails.

### 4.6 Provider portability is honest

The framework guarantees common semantic behavior, not byte-identical behavior. Provider-specific
reasoning, caching, continuation IDs, built-in tools, and diagnostics remain capability-qualified
escape hatches.

### 4.7 Durability is a contract

Persisted chat history, a durable queue, and durable execution are distinct. The word "durable" may
only describe a runtime satisfying the Accepted-work Contract and the verification gates.

### 4.8 Secure by construction

The model is untrusted. Tool authorization, approval, tenancy, path/network capability, schema
decoding, resource budgets, and content persistence policies are enforced outside model prose.

## 5. Product capabilities

### P0: Design proof

- Model-agnostic Agent Definitions with explicit Effect AI Model Bindings.
- Agent composition with Effect AI Tool, Toolkit, Model, Prompt, and Response.
- Effect Schema inputs and outputs.
- Effectful instructions.
- Requirements and failures preserved through the full run type.
- One scripted Effect AI LanguageModel Layer.
- One deterministic turn loop.
- Verified interruption and finalization.

### P1: Ephemeral core

- `Agent.define`, `Agent.withModel`, `AgentRuntime.run`, and `AgentRuntime.stream`.
- Finite-concurrency execution of model-requested Effect AI Tools.
- Bounded Stop Policy.
- Text and structured final results.
- Stable semantic live events.
- Direct Effect AI LanguageModel and Model integration.
- OpenAI and Anthropic examples through Effect AI provider Layers.
- Scripted/fake provider testing kit.
- Effect tracing and redaction.

### P2: Operational local agents

- Conversation snapshots and export.
- Steering and follow-up commands at Turn seams.
- Approvals represented as explicit suspension.
- Sequential overrides and deterministic Tool result order.
- Usage and cost budgets.
- Pluggable context transformation and model-context compaction.
- Effect AI MCP and user-supplied Sandbox services.

### P3: Persistent conversations

- Stable identities and versioned Canonical Records.
- Conversation Log and rebuildable projections.
- SQLite reference adapter.
- Resumable observation and reattachment.
- No durable-execution claim yet.

### P4: Durable execution

- Submission Ledger and durable admission.
- Ordered per-Conversation execution and Attempt ownership.
- Recovery classifier and coordinator.
- Abort intent and terminal Settlement.
- Node restart recovery.
- Crash injection, property testing, and model checking gates.

### P5: Durable Tools and joined input

- Interrupted model-response recovery.
- Prepared and settled ordinary Tool records.
- Explicit unknown outcomes for unresolved external effects.
- Named durable Steps and application reconciliation.
- Durable joining at the same Turn boundaries as ephemeral steering and follow-up.
- Durable approval suspension.

### P6: Cloudflare runtime

- SQLite-backed Durable Object storage and coordination.
- Alarm-driven recovery of unsettled work.
- The same durable service contracts and conformance suite as Node/SQLite.
- Platform bindings isolated behind Effect services and Layers.

### P7: Internal hardening

- Adapter certification and destructive crash testing.
- Executable state-machine modeling.
- Administrative recovery diagnostics.
- Security review, threat modeling, chaos tests, and soak tests.
- Validation through real internal Agents.

### Progressive reference application

The [Travel Planner](guides/travel-planner.md) is the product's cumulative Reference Application.
It begins as a two-Turn, read-only itinerary planner backed by a scripted Effect AI model and
deterministic travel data. Each phase keeps that path green while adding the phase's public
behavior: richer Tool scheduling, interactive changes and approval, persistent Conversations,
durable accepted work, honest booking recovery, and equivalent Node/Cloudflare Layer assemblies.

The default profile is fully offline. Live OpenAI or Anthropic Models and selected travel suppliers
are opt-in Layer substitutions used for smoke and internal validation. Reusable contracts and
evidence live in the testing package; `examples/demo` renders the same fixture as a local browser
bench. It is not an `apps/` workspace or hosted product, and its maturity label never exceeds the
adapter and deployment class exercised by its evidence.

Channels, UI, hosted orchestration, and marketplaces remain separate products.

## 6. Core experience

```ts
import { Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import { Agent } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";

class IssueRepo extends Effect.Service<IssueRepo>()("IssueRepo", {
  effect: Effect.succeed({
    get: (repo: string, number: number) =>
      Effect.succeed({ repo, number, title: "Example", sensitive: false }),
  }),
}) {}

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
  failure: Schema.Struct({
    _tag: Schema.Literal("IssueUnavailable"),
    message: Schema.String,
  }),
  failureMode: "error",
  dependencies: [IssueRepo],
});

const TriageTools = Toolkit.make(InspectIssue);

const TriageToolsLive = TriageTools.toLayer({
  inspect_issue: ({ repo, number }) => Effect.flatMap(IssueRepo, (_) => _.get(repo, number)),
});

const TriageDefinition = Agent.define("triage", {
  input: Schema.Struct({
    repo: Schema.String,
    issueNumber: Schema.Int,
  }),
  output: Schema.Struct({
    severity: Schema.Literals(["low", "medium", "high", "critical"]),
    explanation: Schema.String,
  }),
  instructions: ({ repo, issueNumber }) =>
    Effect.succeed(`Triage ${repo}#${issueNumber}. Escalate sensitive issues immediately.`),
  toolkit: TriageTools,
  policy: {
    maxTurns: 12,
    maxToolCalls: 20,
    maxDuration: "10 minutes",
    toolConcurrency: 4,
  },
});

const Triage = Agent.withModel(TriageDefinition, AnthropicClaude);

const result = AgentRuntime.run(Triage, {
  repo: "withastro/astro",
  issueNumber: 123,
}).pipe(Effect.provide(TriageToolsLive), Effect.provide(IssueRepo.Default), Effect.scoped);
```

The exact syntax may evolve. The invariant is that required Tool handlers, domain modules, explicit
model selection, and schema requirements remain visible to the compiler.

## 7. Non-goals for the first stable core

- A Promise-first interface.
- A React-style hook authoring system.
- A generic workflow or DAG engine.
- A provider SDK re-export layer.
- A hosted control plane.
- A visual agent builder.
- Automatic long-term memory or RAG.
- An embedded shell/filesystem coding agent.
- General-purpose sandbox infrastructure.
- Framework-level multi-agent orchestration before one-agent determinism.
- Exactly-once external side effects.
- Automatic provider-level execution of application Tools.
- Release readiness defined by another framework's feature list.

## 8. Success measures

### Type and correctness

- All declared handler requirements remain visible in the run's `R`.
- Expected domain failures are not widened to `unknown` or global `Error`.
- `run` and `stream` pass the same golden semantic traces.
- Finalizers execute when interruption occurs during model streaming, Tool execution, approval
  waiting, and event consumption.
- Every public persisted/wire value has a Schema and fixture.

### Developer experience

- An Effect user can create a typed tool agent from documentation in under 20 minutes.
- A provider is supplied as an Effect AI Model/Layer without a framework adapter.
- A scripted provider can test a multi-Turn agent without network access.
- Type errors name the missing handler or application module rather than emitting unreadable
  generic expansions.

### Product validation

- The Travel Planner grows cumulatively through each phase and validates domain Tools,
  approval-required mutation, persistence, recovery, and platform substitution.
- At least two additional nontrivial internal Agents validate domains and ergonomics that the
  Travel Planner does not cover, including developer tooling.
- Early adopters cite typed requirements, testing, or resource safety as the reason to adopt.
- Provider-specific escape-hatch requests remain exceptional and documented.

### Durability gates

Before durable execution is marketed:

- every crash point has a specified recovery outcome;
- generated state-machine traces preserve safety invariants;
- adapter contract suites pass for the reference durable store;
- kill/restart tests converge without duplicate canonical outcomes;
- old persisted fixtures replay or fail safely under the documented compatibility policy.

## 9. Principal product risks

1. **Effect v4 and Effect AI instability** — use the primitives directly, pin one exact version,
   and treat upgrades as verified repository-wide changes.
2. **Framework breadth** — prioritize the typed interpreter before sandboxes, channels, UI, or
   hosted infrastructure.
3. **Type ergonomics** — preserve information without producing unusable diagnostics.
4. **Durability overclaim** — keep persistent history, recovery, and durable side effects separate.
5. **Provider semantic gaps** — publish a capability matrix and fail early.
6. **Approval and suspension** — do not hide in-memory callbacks behind a durable-sounding
   interface.
7. **Event backpressure** — define buffering and disconnection semantics for every transport.
8. **Security scope** — treat tool execution and sandboxing as authorization products, not model
   features.

## 10. Release posture

The project remains private while the authoring and runtime design is validated. Effect v4 is
pinned exactly. There is no stored-data migration or public compatibility promise during this
period.

The first compelling release should feel unsurprising to an Effect user: immutable values,
Effects, Streams, Schemas, Layers, and normal interruption. That coherence is the differentiator.

# ADR-0002: Use Effect AI primitives directly

- Status: Accepted
- Date: 2026-07-29
- Decision owners: Project owner
- Related decisions: D-002, D-003, D-008, D-012, D-019, D-022

## Context

Effect v4 already provides the central AI building blocks the framework needs:
`Tool`, `Toolkit`, `LanguageModel`, `Prompt`, `Response`, `Chat`, Effect Schema
integration, typed handler failures, request requirements, approval, streaming, and
provider Layers.

Creating framework-owned copies would provide insulation from beta changes, but it
would also make Effect applications translate between two nearly identical systems.
That conflicts with the goal of being Effect-native and interoperable with the wider
Effect ecosystem.

## Decision

Use Effect AI primitives directly in public authoring and runtime APIs.

- Agent Definitions contain Effect AI Toolkits and model requirements.
- Tool handlers use Effect AI's typed failure channel and service requirements.
- Provider implementations are normal Effect AI Layers.
- The agent interpreter consumes Effect AI Prompt and Response values.
- Framework-owned values are limited to concepts Effect AI does not own, such as
  Agent Definition, Run, Conversation, Submission, Settlement, recovery policy, and
  durable records.
- Framework-specific metadata should first use Effect annotations or configuration.
- Generally useful missing behavior should be proposed upstream before creating a
  competing abstraction.

The repository pins one exact Effect v4 version. Upgrades are explicit project work
and must pass type, runtime, provider, and persistence tests.

## Consequences

Positive:

- Effect applications compose without adapters;
- Tool failures and requirements flow through the existing Effect AI types;
- provider Layers work directly;
- improvements can benefit the broader Effect ecosystem;
- less framework code and fewer concepts need long-term maintenance.

Negative:

- beta API changes can require coordinated repository changes;
- framework releases are tied closely to supported Effect versions;
- some durability metadata may require upstream extension or carefully scoped
  annotations;
- supporting several Effect versions at once is not a goal during private
  development.

## Error behavior

Effect AI Tool failure mode defaults to `"error"`, meaning a Tool handler failure
stays in the Effect error channel. This is the framework default.

`failureMode: "return"` remains an explicit application choice when a typed failure
should become a model-visible Tool result. A valid absence is a successful domain
value such as `Option.none`, not a failure.

## Validation

- public examples import Tool, Toolkit, LanguageModel, Prompt, and Response from
  Effect AI;
- Tool handler requirements remain visible in the enclosing Run;
- Tool failures remain typed without framework conversion;
- an Effect AI provider `Model` Layer runs an Agent directly;
- each Effect upgrade passes compile-time examples and the full semantic suite.

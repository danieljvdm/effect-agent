# ADR-0001: Build an Effect-native core

- Status: Accepted
- Date: 2026-07-28
- Decision owners: Project owner
- Related decisions: D-001, D-004

## Context

The source research identified useful agent-loop and durable accepted-work behavior. Those findings
are recorded in [the reference analysis](../REFERENCE-ANALYSIS.md). The product still needs native
control of execution, errors, dependencies, resource ownership, and durability.

Agent instructions, tools, policies, model routing, and capabilities should compose as Effects.
Expected errors should remain in `E`, dependencies in `R`, resource lifetimes in `Scope`, and
events in `Stream`.

## Decision

Build a framework-owned Effect-native authoring model and interpreter.

The first deliverable is a bounded ephemeral interpreter. Persistence and durability
are additive runtimes over the same semantic engine.

## Consequences

Positive:

- the public API reflects Effect semantics directly;
- Effect Schema is authoritative;
- typed errors and requirements are preserved;
- the engine controls scheduling, interruption, and canonical state;
- durability semantics can be designed without translating through another runtime;
- provider and platform choices remain replaceable.

Negative:

- the project must implement and verify its own interpreter;
- durable capabilities arrive later;
- durability work is substantial;
- upstream projects may evolve faster than the native implementation.

## Validation

Before accepting this ADR, the Phase 0 proof must demonstrate:

- inferred tool and instruction requirements appear in Run `R`;
- expected failures appear in Run `E`;
- `run` and `stream` share one interpreter;
- interruption closes every acquired resource;
- a two-turn tool-using example depends only on the declared Effect Agent and Effect AI packages.

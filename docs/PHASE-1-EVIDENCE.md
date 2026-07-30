# Phase 1 evidence

Status: **Implemented and verified**

This packet records the executable evidence for
[Phase 1 — Ephemeral interpreter](ROADMAP.md#phase-1--ephemeral-interpreter). It extends, rather
than rewrites, the historical [Phase 0 evidence](PHASE-0-EVIDENCE.md).

## Delivered `E` contract

- `Agent.define` remains immutable and model-agnostic; only explicit `Agent.withModel` Bindings are
  runnable, preserving native Effect AI Model requirements in `R`.
- The engine reduces native Effect AI Response streams, rejects malformed/truncated Tool work, and
  owns unresolved application Tool handling.
- A complete Tool batch passes preflight before handlers start. Handlers execute as scoped child
  work under finite Effect `Semaphore` permits; results become model-visible in declaration order,
  never as a partial batch.
- Finite turn, Tool-call, and duration bounds prevent a Stop Policy breach from becoming success.
  Stable semantic Run Events and structured telemetry/log annotations expose run and Tool lifecycle
  without a framework logging wrapper; content is excluded from the engine metric/log attributes.
- The scripted LanguageModel and deterministic Layers cover the ordinary, credential-free suite.
  The cumulative Travel Planner uses flight, lodging, and activity Tools with typed unavailable,
  empty-success, and failure cases plus structured final output.

## Provider bindings

`examples/providers` binds the shared Travel Planner Definition directly to the pinned upstream
`@effect/ai-openai` and `@effect/ai-anthropic` Model APIs. These are compile/test-only bindings:
the normal suite makes no provider request and no claim is made that live smoke execution occurred.
Applications electing to invoke them provide their own redacted credentials, upstream client Layer,
and application Tool-handler Layers.

## Requirement and decision evidence

| Concrete executable evidence                                                                               | Requirements / decisions                                                                                  |
| ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `packages/core/test/core.test.ts`; `agent-types.test.ts`                                                   | `AUTH-001`–`AUTH-004`, `AUTH-006`, `AUTH-011`, `AUTH-012`, D-002, D-027                                   |
| `packages/engine/test/agent-runtime.test.ts`                                                               | `AUTH-008`, `RUN-001`–`RUN-009`, `RUN-011`, `MODEL-003`, `MODEL-004`, `MODEL-006`, D-007, D-008, ADR-0005 |
| `packages/testing/test/scripted-model.test.ts`; `travel-planner.test.ts`; `travel-planner.compile.test.ts` | `TEST-002`, `TEST-009`, `TEST-014`, D-026                                                                 |
| `packages/testing/test/toolchain.test.ts`; repository `check` and `build` gates                            | `AUTH-009`, `MODEL-009`, `TEST-010`, `TEST-013`                                                           |
| Engine metric/log definitions and engine verification                                                      | `SEC-008`                                                                                                 |
| `examples/providers/test/profiles.test.ts`                                                                 | `MODEL-008`, `AUTH-012`, D-002, D-027                                                                     |

The primary executable locations are `packages/core/test`, `packages/engine/test`,
`packages/testing/test`, and `examples/providers/test`.

## Verification record

Verified on **2026-07-29** with Bun **1.3.14** and Effect **4.0.0-beta.102**. The verified source
state is the uncommitted Phase 1 working tree based on commit `082d7fa17ca8`; this record does not
claim that the changes were committed.

The verification commands were:

```sh
bun install --frozen-lockfile
bun run sync:effect
bun run ready
```

The frozen install reported no dependency changes, and the Effect source checkout was already at
the catalog pin. `bun run ready` completed formatting, lint, type checks, tests, and builds:

- `packages/core`: 7 tests;
- `packages/engine`: 17 tests;
- `packages/testing`: 17 tests;
- `examples/demo`: 16 tests;
- `examples/providers`: 1 test.

All **58 tests** passed. The three framework packages and both example workspaces built
successfully.

The ordinary gate uses injected Layers and no network. Provider bindings are checked and built
without credentials; any credential-gated live smoke run remains an explicit, opt-in follow-up.

## Claim boundary

Phase 1 proves only class `E` execution: in-memory, process-scoped work with no recovery promise.
It does not claim persisted Conversations, receipts, accepted-work durability, exactly-once external
effects, steering, follow-up, approval, MCP, sandboxing, compaction, or Phase 2 sequential
overrides. Provider SDK values remain outside canonical records.

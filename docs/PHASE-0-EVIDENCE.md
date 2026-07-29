# Phase 0 evidence

Status: **Implemented and verified**

This packet records the executable evidence for
[Phase 0 — Repository and Effect AI design proof](ROADMAP.md#phase-0--repository-and-effect-ai-design-proof).
It does not claim the Phase 1 ephemeral interpreter or any persistent/durable runtime.

## Delivered public contract

- `@effect-agent/core` owns branded identifier Schemas, finite `AgentPolicy`, immutable
  model-agnostic `Agent.define`, explicit `Agent.withModel` Bindings, schema-backed expected
  errors, the semantic `RunEvent` Schema union, and the `IdGenerator` service.
- Agent input/output and expected failures are Schema-first. Instruction, model, Tool handler,
  Schema, and application-service requirements remain visible in `E` and `R`.
- Agent Definitions store Effect AI `Toolkit` values directly and remain model-agnostic. Agent
  Bindings pair a Definition with an Effect AI `Model`. The framework defines no competing Tool,
  Toolkit, Model, LanguageModel, Prompt, or Response primitive.
- `@effect-agent/engine` exposes one `AgentRuntime.stream` interpreter. `AgentRuntime.run` folds
  that stream and does not implement another loop.
- The runtime decodes then re-encodes Agent input at the owning Schema boundary, evaluates
  instructions once, constructs an Effect AI Prompt, consumes native Response parts, validates a
  complete Tool result and finish seam, and Schema-decodes final JSON output.
- The runtime accepts only an Agent Binding and provides its native Effect AI Model internally
  while preserving the Model Layer's requirements. Application Tool handlers and their declared
  services remain caller requirements.
- Phase 0 uses Effect AI's native Toolkit resolution with the finite
  `AgentPolicy.toolConcurrency` value. Engine-owned Semaphore scheduling, richer batch preflight,
  retries, budgets, and the full response reducer remain Phase 1 work.

The runtime interfaces are:

```ts
AgentRuntime.stream(agent, input);
// Stream<RunEvent, AgentRuntimeFailure<typeof agent>, AgentRuntimeRequirements<typeof agent>>

AgentRuntime.run(agent, input);
// Effect<AgentResult<Agent.Output<typeof agent>>,
//   AgentRuntimeFailure<typeof agent>,
//   AgentRuntimeRequirements<typeof agent> | Scope>
```

Expected failures include instruction and Tool-declared errors, Effect AI `AiError`,
`AgentInputError`, `AgentOutputError`, `AgentPolicyError`, and `ModelProtocolError`. Defects remain
defects. Missing Tool/application/ID Layers remain visible as missing Effect services rather than
being hidden behind configuration objects.

## Scripted model

`@effect-agent/testing` provides a finite scripted Effect AI `LanguageModel` Layer built with
`LanguageModel.make`. Its schema-backed script grammar supports generate and stream turns, native
encoded Response parts, normalized-request assertions, explicit failure, hanging streams, and
stream finalizer hooks.

The testing utility records normalized Effect AI provider requests without introducing a provider
or model-driver abstraction. Script exhaustion is an `AiError`.

## Travel Planner `E` design proof

The reusable fixture under `packages/testing/src/fixtures/travel-planner` implements the P0
reference slice:

1. decoded `SFO` → `LHR` trip input enters the runtime;
2. scripted Turn 1 declares the native `search_availability` Tool Call;
3. the deterministic catalog Layer returns one read-only quote;
4. the complete Tool result is present in the next Effect AI Prompt;
5. scripted Turn 2 returns JSON;
6. the public `TravelPlan` Schema decodes the itinerary;
7. the result requires review and makes no hold, booking, persistence, or durability claim.

The compile proof checks the exact Run contract:

- `R`: `AvailabilityCatalog | TravelGuidance | Tool.HandlersFor<...> | IdGenerator | Scope`;
- `E`: `AvailabilityUnavailable | GuidanceFailure | AiError | AgentInputError |
AgentOutputError | AgentPolicyError | ModelProtocolError`.

Success-path and interruption tests both prove the acquired catalog and model/Tool resources
finalize when their Scope closes. A length-truncated model response fails with
`ModelProtocolError`; it cannot become a successful Run.

`examples/demo` consumes this same fixture through public package exports. Its TanStack Start UI
uses an Effect Atom action to run the scoped event Stream and presents user input, Tool execution,
semantic events, and Schema-decoded output. It remains an offline test bench, not a durability or
deployment proof.

## Requirement evidence

| Evidence                                 | Requirements / decisions                                         |
| ---------------------------------------- | ---------------------------------------------------------------- |
| Core Schema and inference tests          | `AUTH-001`–`AUTH-004`, `AUTH-006`, `AUTH-009`, D-001–D-003       |
| Shared engine trace and typed failures   | `RUN-001`, `RUN-007`, `RUN-008`, `MODEL-001`, `MODEL-006`        |
| Native finite Toolkit execution          | D-007, ADR-0005; Phase 0 proof only                              |
| Scripted LanguageModel tests             | `TEST-002`, `MODEL-009`                                          |
| Travel Planner runtime and compile tests | `TEST-009`, `TEST-014`, D-026                                    |
| Package/toolchain guards                 | `TEST-010`, `TEST-013`, `COMPAT-001`, `COMPAT-006`, `COMPAT-010` |
| Attributed local source snapshots        | D-021, `COMPAT-002`–`COMPAT-005`                                 |

## Verification record

Verified locally with Bun `1.3.14` and Effect `4.0.0-beta.102`:

```sh
bun install --frozen-lockfile
bun run ready
```

The green suite contains:

- core: 5 tests;
- engine: 5 tests;
- testing and Travel Planner: 13 tests;
- all three package builds and the TanStack Start example build;
- Vite+ formatting, lint, Effect-aware type checks, package graph checks, and script checks.

Static guards also verify that framework packages remain exactly `core`, `engine`, and `testing`;
that `examples/demo` is a leaf consumer; that production packages do not depend on
`@effect-agent/testing`; that workspace Effect dependencies use the root catalog; that the
initialized Effect source tag matches the catalog pin; and that no `apps/`, Wrangler, or Cloudflare
runtime scaffold has entered Phase 0.

## Claim boundary

Phase 0 proves that the public Effect-native design composes and executes. It does not yet claim:

- the complete Phase 1 response protocol reducer or engine-owned Tool scheduler;
- wall-clock Stop Policy enforcement, retries, telemetry, or provider smoke profiles;
- steering, follow-up, approval, MCP, sandbox, or compaction;
- persisted Conversations or accepted-work durability;
- exactly-once external side effects.

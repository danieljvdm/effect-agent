# Progressive Travel Planner Reference Application

Status: Phase 4 durable Node/SQLite runtime implemented; Phase 5 and later increments remain design targets
Owner decision: [D-026](../DECISIONS.md#d-026--progressive-reference-application)

## 1. Purpose

Travel Planner is the cumulative Reference Application for Effect Agent. It gives every roadmap
phase one recognizable application scenario and makes the framework's growth visible without
creating a second demo runtime.

The example is called **Travel Planner** rather than “travel agent” when referring to the
application. Its model-agnostic `travel-planner` Agent Definition is paired with each scripted or
live Effect AI Model through an explicit Agent Binding.

Travel Planner must:

- consume the public framework API like an application would;
- begin with a deterministic, credential-free two-Turn scenario;
- grow cumulatively, retaining all earlier scenarios as later phases add behavior;
- expose typed application services and failures in `R` and `E`;
- use Effect Schema for model, Tool, wire, and persisted values;
- replace model Bindings and travel-service, storage, and platform implementations through Layers;
- make persistence, accepted-work durability, and external side-effect guarantees visibly
  distinct;
- remain executable evidence, not only narrative documentation.

It is not:

- an `apps/` workspace, hosted service, or deployable product shell;
- a reason to create a package before its roadmap phase;
- a wrapper around Effect AI provider Models;
- a promise that airline, hotel, payment, or booking APIs execute exactly once;
- a substitute for focused unit, generated, conformance, fault, or security tests.

## 2. Stable scenario

The initial input describes a trip without requiring passenger identity or payment data:

- origin and destination;
- date window and trip length;
- traveler count;
- budget and currency;
- mobility, lodging, and activity preferences;
- optional constraints such as nonstop travel or refundable inventory.

The structured output contains:

- one or more itinerary options;
- stable quote and inventory references from the travel-service boundary;
- estimated total and currency;
- assumptions and unresolved constraints;
- the approval or follow-up action required before any mutation.

The application grows around these Tool families:

| Tool family                          | Initial behavior                                                  | Risk and scheduling                                                                                     |
| ------------------------------------ | ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Flight, lodging, and activity search | Read-only lookup with typed unavailability and supplier failure   | Bounded parallel execution; results commit in declaration order                                         |
| Itinerary hold                       | Short-lived test mutation introduced to demonstrate approval      | Approval-required and sequential; ephemeral phases make no recovery promise                             |
| Booking confirmation                 | External mutation introduced only with durable Tool recovery work | Prepared/settled record, stable idempotency key where supported, reconciliation or unknown outcome      |
| Cancellation                         | External compensating mutation                                    | Approval-required, sequential, independently reconcilable; never inferred to have rolled back a booking |

Supplier content, quote text, and confirmation data are untrusted. The Agent never receives raw
credentials, payment details, or provider SDK objects. Passenger and loyalty data, when eventually
needed, use opaque handles resolved inside the narrow authorized service.

## 3. Replaceable Layer profiles

“Mocked” and “live” describe application assembly, not different framework APIs.

### Model profiles

- **Scripted model** — the default `@effect-agent/testing` Effect AI `LanguageModel` Layer. It
  emits deterministic Response streams, asserts normalized requests, and covers failures,
  malformed sequences, timeouts, and interruption.
- **Live model** — an upstream Effect AI OpenAI or Anthropic `Model` plus its required client and
  configuration Layers. It is an opt-in smoke profile and never a normal correctness dependency.

The framework does not introduce `MockModelDriver`, `TravelModelProvider`, or a provider registry.
Both profiles bind the same Agent Definition through `Agent.withModel` and meet the same Effect AI
boundary.

`examples/providers` is the Phase 1 compile-only proof for direct OpenAI and Anthropic bindings of
the shared Definition. It has no ordinary live invocation: credentials and client Layers are
application composition, not a normal test dependency.

### Travel-service profiles

- **Deterministic catalog** — fixed, Schema-valid inventory and quotes with controllable latency,
  failures, and resource finalizers.
- **Deterministic booking gateway** — records holds, confirmations, cancellations, idempotency
  keys, and injected uncertain outcomes without contacting an external system.
- **Live supplier** — an application-owned Layer for a selected supplier API. It is opt-in,
  credential-gated, rate-limited, and excluded from ordinary pull-request correctness gates.

Travel services are normal application `Context` services required by Effect AI Tool handlers.
They are not framework provider packages and their SDK values end at the Layer boundary.

### Runtime profiles

- in-memory ephemeral execution (`E`);
- persistent memory or SQLite Conversation execution without accepted-work durability (`P`);
- Node/SQLite durable execution (`DN`);
- Cloudflare Durable Object durable execution (`DC`).

Every test, example command, and evidence packet names its profile. “Durable Travel Planner” is
insufficient without `DN` or `DC` and the tested adapter.

## 4. Phase progression

Each row is cumulative. A phase exit runs that row plus every earlier offline scenario.

P0 through P4 are implemented. P2 retains the ephemeral `E` runtime's operational behavior; P3
adds a separate `P` profile that stores and reconstructs planning history while keeping accepted
work explicitly non-durable. P4 adds the `DN` profile: a durable planning Submission that returns
a Receipt, keeps one FIFO trip lane, survives restart to the same projection, supports durable
abort, and settles exactly once — while its fixture pins `supplierBookingReplaySafe: false`
because replay-safe booking is P5 scope (`packages/testing/src/fixtures/travel-planner/phase4.ts`,
`packages/testing/test/travel-planner-phase4.test.ts`; [Phase 4 evidence](../PHASE-4-EVIDENCE.md)).

| Phase | Maturity                    | Travel Planner increment                                                                                                                                          | Required evidence                                                                                                                                      |
| ----: | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
|    P0 | `E` design proof            | Two-Turn read-only availability lookup and Schema-decoded itinerary using scripted model and deterministic catalog Layers                                         | Public API compile proof; inferred Tool/instruction `E` and `R`; `run`/`stream` trace equality; interruption finalizers                                |
|    P1 | `E` interpreter             | Flight, lodging, and activity Tools; bounded parallel reads; structured final output; typed empty/unavailable/failure cases; live OpenAI and Anthropic assemblies | Deterministic semantic suite for success, malformed output, truncation, timeout, and interruption; opt-in live-model smoke tests                       |
|    P2 | `E` operational             | Date-change steering, constraint follow-up, budget exhaustion, compaction, and approval-gated itinerary hold                                                      | Safe Turn-seam tests; approval prevents handler start; finite concurrency and declaration-order results; no persistence or recovery claim for a hold   |
|    P3 | `P` persistent              | Stored trip Conversation, resumable observation, export, replayable itinerary projection, and current-version redacted fixtures                                   | Full replay equals valid Checkpoint replay; restart reconstruction; unsupported data fails before mutation; explicit non-durable label                 |
|    P4 | `DN` durable planning       | Durable admission of a planning request, Receipt, FIFO trip lane, restart recovery, abort, and one Settlement                                                     | Node process-kill tests across admission, input application, model Turns, terminalization, and reattachment; no replayable booking claim               |
|    P5 | `DN` durable booking        | Approval suspension, prepared/settled booking Tools, named booking Steps, supplier reconciliation, explicit unknown outcomes, and joined traveler changes         | Crash tests before/during/after supplier mutation; confirmed recovery or `UnknownToolOutcome`; no fabricated Tool result; joined input exactly once    |
|    P6 | `DN` + `DC`                 | The same authoring definition and scenarios assembled with Node/SQLite and Cloudflare Layers                                                                      | Shared durability suite; Durable Object eviction and alarm retry; equivalent canonical outcomes; no Cloudflare types in Agent/core/engine              |
|    P7 | certified internal profiles | Live model and selected supplier Layers, operator diagnostics, security cases, chaos, and soak coverage                                                           | Adapter certification; threat model; redacted fixtures/cassettes; explainable recovery; Travel Planner counts as one of at least three internal Agents |

P3 deliberately persists planning history without promising that unobserved work will finish after
a crash. P4 proves durable accepted planning work before P5 introduces external booking recovery.
This ordering keeps a visually compelling example from overstating the framework's guarantees.

## 5. Shared fixture and browser bench

The cumulative consumer fixture belongs in the leaf testing package:

```text
packages/testing/
  src/
    fixtures/
      travel-planner/
        definition.ts
        deterministic-layers.ts
        phase2.ts
        phase3.ts
        phase4.ts
        subagents.ts
        scenarios.ts
        index.ts
  test/
    travel-planner.test.ts
    travel-planner-phase2.test.ts
    travel-planner-phase3.test.ts
    travel-planner-phase4.test.ts
    travel-planner-subagents.test.ts
    travel-planner.compile.test.ts
examples/
  demo/
    src/
      demo/state.ts
      components/
```

The definition imports public `@effect-agent/core` APIs and Effect AI primitives directly. The
testing package may add dependencies on phase packages as those packages are created because it
remains outward of production code; production packages must never import
`@effect-agent/testing`.

`examples/demo` imports the same public Phase 2 fixture exports used by the tests. A server-scoped
runtime assembles the scripted Model, controlled Travel Planner services, ephemeral Conversation,
command queue, approval resolver, budgets, MCP connector, and local sandbox. A demo-local shared
Effect RPC definition returns the operational Stream over framed HTTP/NDJSON; separate unary RPCs
admit commands and approval decisions while the stream remains open.

The TanStack Start app defaults to a separate general-chat surface. Its Simulator tab projects
safe-seam command delivery, Tool declaration/completion order, deterministic commit order,
approval and handler-start state, budget rejection, official versus model-only context, MCP
discovery bounds, local command posture, semantic Run Events, and Schema-decoded output. The
Simulator is deterministic and credential-free.

Focused tests stay with their owner:

- engine state-machine and scheduling tests in `packages/engine`;
- capability behavior in `packages/capabilities`;
- replay and record tests in `packages/session` and storage adapters;
- process-kill and host tests in `packages/platform-node`;
- eviction and alarm tests in `packages/platform-cloudflare`.

Those suites may reproduce the same Travel Planner scenario through public contracts, but shared
domain or record Schemas are not moved outward merely to reduce fixture duplication. One
designated integrator owns changes to public Agent and canonical record Schemas.

No phase adds `apps/travel-planner`, a Worker entrypoint, or production imports from the testing
package. The browser bench remains a private leaf example and does not become a host or deployment
contract.

## 6. Verification profiles

| Profile               |                   Network |                     Ordinary PR gate | Purpose                                                                                           |
| --------------------- | ------------------------: | -----------------------------------: | ------------------------------------------------------------------------------------------------- |
| Offline happy path    |                        No |                                  Yes | Cumulative public API and semantic behavior                                                       |
| Offline failure/fault |                        No |                                  Yes | Typed failures, malformed model responses, interruption, clocks, scheduling, and injected crashes |
| Live model            |                       Yes | No, unless model integration changed | Upstream Effect AI Model compatibility and basic itinerary quality                                |
| Live supplier         |                       Yes |                                   No | Application Layer compatibility, redaction, authorization, and reconciliation drills              |
| `DN` process-kill     |       Local host/database |                 Adapter/release gate | Accepted-work and booking recovery under actual process loss                                      |
| `DC` eviction/alarm   | Miniflare/Workers harness |                 Adapter/release gate | Durable Object storage, eviction, alarms, and platform limits                                     |

Golden data is minimal, versioned, structurally redacted, and annotated with the provider or
supplier adapter version. Semantic assertions remain authoritative. Live fixtures never contain
credentials, passenger identity, loyalty identifiers, payment data, or unrestricted raw payloads.

## 7. Security and durability rules made visible

The example should teach the difficult rules through observable scenarios:

1. Search Tools may execute concurrently, but inventory mutations affecting one itinerary are
   sequential.
2. Authorization and approval are evaluated after Schema decoding and immediately before a
   mutation handler starts.
3. Disconnecting from a `DN` or `DC` observation stream does not abort accepted work.
4. A Receipt proves durable admission and readiness, not successful planning or booking.
5. A persisted Conversation (`P`) is not durable accepted work.
6. A prepared but unsettled ordinary booking call is reconciled, safely retried under a verified
   supplier idempotency contract, or recorded as an unknown outcome.
7. A durable Step records one accepted result exactly once, but its supplier call may execute more
   than once.
8. Supplier text and traveler-provided content cannot grant Tool authority or release secrets.
9. Provider and supplier SDK values never enter canonical records.
10. Every live resource belongs to Scope and finalizes on success, failure, timeout, and
    interruption.

## 8. Traceability

Travel Planner scenarios cite existing normative requirements rather than creating a parallel
travel-specific contract.

| Phase | Primary requirement evidence                                                                                        |
| ----: | ------------------------------------------------------------------------------------------------------------------- |
|    P0 | `AUTH-001`–`AUTH-011`, `RUN-001`, `RUN-007`–`RUN-009`, `MODEL-001`, `MODEL-008`, `TEST-002`, `TEST-009`, `TEST-014` |
|    P1 | `RUN-002`–`RUN-013`, `MODEL-003`–`MODEL-009`, `TEST-002`, `TEST-007`, `TEST-009`, `TEST-014`                        |
|    P2 | `RUN-014`, `RUN-015`, `CAP-001`–`CAP-007`, `CAP-013`, `SEC-004`, `SEC-005`, `SEC-012`, `SEC-013`, `TEST-014`        |
|    P3 | `STORE-001`, `STORE-004`–`STORE-008`, `STORE-011`, `TEST-001`, `TEST-004`, `TEST-014`                               |
|    P4 | `DUR-001`–`DUR-008`, `DUR-011`–`DUR-015`, `DEPLOY-001`–`DEPLOY-009`, `TEST-003`–`TEST-006`, `TEST-012`, `TEST-014`  |
|    P5 | `DUR-009`, `DUR-010`, `DUR-013`, `DUR-016`, `CAP-006`, `OPS-002`, `TEST-005`, `TEST-006`, `TEST-014`                |
|    P6 | `STORE-013`, `DEPLOY-010`, `TEST-004`–`TEST-006`, `TEST-014`                                                        |
|    P7 | `SEC-001`–`SEC-013`, `OPS-001`–`OPS-003`, `TEST-003`–`TEST-014`                                                     |

Every phase work item records the exact scenarios, package slices, decision dependencies, and
verification paths it changes. A passing Travel Planner scenario supports a requirement but never
replaces its focused conformance or adversarial evidence.

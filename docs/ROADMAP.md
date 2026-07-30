# Implementation roadmap

Status: **Draft**  
This roadmap is ordered by what must be proven first, not by calendar estimate.
The repository has no `apps/` workspace. Framework packages are created only when their first
phase begins; runnable consumer benches live under `examples/*` as leaf workspaces.

Implementation work follows the synced Effect skills. Each work item names the focused reference
it needs: schema-first modeling, functions/errors, services/Layers, logging, testing, HTTP
boundaries, or Effect CLI scripting. The
[project execution guide](guides/project-execution.md#3-skill-routing) defines the mapping and
review expectations.

## Implementation status

The roadmap remains normative and **Draft**. This table records implementation evidence; it does
not change the specified scope of later phases.

| Phase  | Implementation status                                       |
| ------ | ----------------------------------------------------------- |
| P0     | Complete — [design proof](PHASE-0-EVIDENCE.md)              |
| P1     | Complete — [ephemeral `E` interpreter](PHASE-1-EVIDENCE.md) |
| P2     | Complete — [operational local runtime](PHASE-2-EVIDENCE.md) |
| P3     | Complete — [persistent Conversations](PHASE-3-EVIDENCE.md)  |
| **P4** | **Next — durable Node/SQLite runtime**                      |
| P5–P7  | Planned — not yet implemented                               |

## Progressive reference application

The [Travel Planner Reference Application](guides/travel-planner.md) is the cumulative integration
thread for the roadmap. Every phase extends the same authoring module and scenario rather than
replacing it with an unrelated demo.

- The ordinary green suite uses a scripted Effect AI `LanguageModel` Layer and deterministic
  travel-service Layers. It requires no credentials or network.
- Selected live AI models and travel suppliers are opt-in Layer substitutions and smoke tests.
- Executable evidence lives in the testing package and focused package tests. `examples/demo`
  renders the same shared fixture as a browser bench without creating an `apps/` workspace or
  production package.
- Each slice names its deployment class and demonstrates only the claims unlocked by that phase.
- The example remains cumulative: a phase exit includes all earlier Travel Planner behavior plus
  the newly introduced scenario.

## Phase 0 — Repository and Effect AI design proof

### Deliverables

- Vite+ `0.2.6` monorepo using Bun `1.3.14`, with phase-gated packages and a leaf example bench.
- Root dependency catalog, frozen lockfile CI, Effect-aware TypeScript, and Vite+ task graph.
- `@effect-agent/core`, `engine`, and `testing` package shells.
- Exact Effect v4 pin and matching local canonical Effect source checkout.
- Project-local `effect-cli` and `effect-patterns` contributor skills synced from
  `@danieljvdm/agent-skills`.
- Agent, policy, ID, error, and Run Event Schemas.
- Schema-derived domain types, branded IDs, and `Schema.TaggedErrorClass` expected failures.
- Direct use of Effect AI Tool, Toolkit, Model, LanguageModel, Prompt, and Response.
- Effect service/Layer proofs that keep runtime dependencies visible in `R`.
- Type proof that Tool handler and instruction requirements propagate into Run `R`.
- Type proof that Effect AI Tool and instruction failures propagate into Run `E`.
- Scripted Effect AI LanguageModel Layer.
- First Travel Planner slice: one two-Turn, read-only availability search producing a
  Schema-decoded itinerary with deterministic model and travel-service Layers.
- TanStack Start browser bench using Effect Atom, Tailwind, shadcn/Base UI, and Vercel AI Elements
  to expose chat input, Tool state, semantic events, and structured output.
- Interruption tests proving all Scope finalizers run.

### Exit gates

- No framework-owned copies of Effect AI primitives.
- Public examples compile against the pinned Effect version.
- `run` and `stream` share one interpreter trace.
- Missing Tool/application Layers produce understandable errors.
- The Travel Planner compiles and runs offline as the public-contract proof, including inferred
  `E`/`R` and Scope finalization.
- Schema-first and service/Layer skill completion checks pass for the first public contracts.
- Reference-project research is captured as attributed source material and translated into native
  Effect Agent requirements.
- `bun run ready` passes from a fresh frozen-lockfile install.
- React stays confined to the leaf example workspace; no `apps/`, Wrangler, hosted, or Cloudflare
  runtime scaffold exists.

## Phase 1 — Ephemeral interpreter

### Deliverables

- Immutable model-agnostic `Agent.define` and explicit `Agent.withModel` Binding.
- Effect Schema Agent input/output.
- Effect AI Toolkits and handler Layers.
- Effect AI Prompt construction and Response stream reduction.
- Engine-controlled Tool boundaries using Effect AI Toolkit handlers.
- Engine-controlled bounded Tool execution using Effect structured concurrency and `Semaphore`
  permits around Effect AI Toolkit handlers.
- Mandatory bounded Stop Policy.
- Stable semantic Run Events.
- Effect spans, metrics, and content redaction.
- Structured Effect logs and annotations with no framework logging wrapper.
- Scripted LanguageModel test DSL.
- Anthropic and OpenAI examples using Effect AI provider Models.
- Travel Planner flight, lodging, and activity lookup Tools, including bounded parallel reads,
  deterministic result ordering, structured final output, typed failures, and opt-in live-model
  assemblies.

### Exit gates

- Traces cover text completion, structured output, Tool success, typed Tool error, empty success,
  malformed parameters, truncation, timeout, and interruption.
- All resources finalize under every exit.
- Tests use injected Layers and controllable Effect time/providers rather than wall-clock sleeps.
- Tool failures remain in `E` by default.
- A Tool Call cannot receive two terminal results.
- A provider SDK type never becomes a canonical record.
- The same Travel Planner semantic suite passes with the scripted model, while credential-gated
  live-provider smoke tests remain outside the ordinary pull-request gate.

## Phase 2 — Operational local runtime

### Deliverables

- `@effect-agent/capabilities`, `sandbox`, and `sandbox-local` packages.
- Ephemeral multi-Run Conversations.
- Steering after a completed response and Tool batch.
- Follow-up input when the Agent would otherwise stop.
- Effect AI approval integration.
- Sequential Run and Tool overrides for the bounded-parallel scheduler.
- Context transforms and model-context compaction.
- Usage, token, cost, and time budgets.
- Effect AI MCP integration.
- User-supplied Sandbox Effect service and local implementation.
- Travel Planner date-change steering, missing-preference follow-up, budget enforcement,
  compaction, and approval-gated itinerary hold using deterministic capability Layers.

### Exit gates

- Slow observers cannot deadlock completion.
- Steering never mutates an in-flight response.
- Follow-up runs only at the documented stop seam.
- Approval prevents handler start.
- Tool concurrency is finite and result ordering deterministic.
- Compaction never erases official history.
- Travel Planner tests prove that traveler changes arrive only at safe Turn seams and that a
  denied or unresolved hold approval never starts the mutation handler.

## Phase 3 — Persistent Conversation foundation

### Deliverables

- `@effect-agent/session`, `storage-memory`, and `storage-sqlite` packages.
- Versioned Canonical Record union.
- Pure Conversation reducer and projections.
- Separate Conversation Store and Submission Store Effect services.
- In-memory reference and Node SQLite implementations.
- Opaque offsets and resumable observation.
- Agent/Tool/Model definition digests.
- Current-version fixtures and a development reset command.
- Persistent Travel Planner Conversations, replayable itinerary projections, reattachment, export,
  and redacted current-version fixtures using memory and SQLite Layers.

### Exit gates

- Full replay and valid Checkpoint replay are equivalent.
- Corrupt or unsupported data fails before mutation.
- Resetting incompatible private-development data is documented.
- Persistence does not yet claim durable accepted work.
- Restarting the persistent Travel Planner reconstructs the same Conversation projection, and its
  documentation labels the example `P` rather than durable.

## Phase 4 — Durable Node/SQLite runtime

### Deliverables

- `@effect-agent/platform-node` package.
- Durable ledger admission and Conversation readiness before Receipt.
- Input application when work is claimed.
- One ordered head per Conversation.
- Attempt identity and recovery classifier.
- Settlement reservation, canonical append, and ledger finalization.
- Durable abort.
- Deterministic failpoints around every transition.
- A `DN` Travel Planner planning Submission that returns a Receipt, survives Node process loss,
  preserves per-trip FIFO order, and converges to one Settlement without yet treating supplier
  booking as safely replayable.

### Exit gates

- Every acknowledged Submission converges to exactly one canonical Settlement.
- A crash after ledger admission completes Conversation readiness.
- Input is applied exactly once before model consumption.
- Canonical settlement repairs an unfinished ledger finalization.
- Later work cannot pass an unsettled head.
- Conflicting idempotency retries fail.
- The `DN` Travel Planner process-kill scenarios cover admission, input application, planning,
  terminalization, reattachment, and abort.

## Phase 5 — Durable Tools and joined input

### Deliverables

- Interrupted Effect AI response convergence.
- Prepared and settled ordinary Tool records.
- Unknown outcomes for unresolved external effects.
- Named durable Steps.
- Application reconciliation hook.
- Claimed `joining` and `joined` queued input.
- Durable approval suspension.
- Approval-gated Travel Planner booking and cancellation scenarios using prepared/settled ordinary
  Tool records, explicit unknown supplier outcomes, named durable Steps with supplier idempotency
  keys, and joined traveler input.

### Exit gates

- Recorded Tool outcomes do not rerun.
- Uncertain ordinary effects do not replay automatically.
- Completed Step results replay without executing.
- External Step side effects remain honestly at least once.
- Joining input reverts before canonical append and reattaches after it.
- Truncated Tool arguments never execute after recovery.
- Travel Planner crash tests never fabricate a booking result: they recover a confirmed supplier
  result, safely repeat under an explicit idempotency contract, or stop at `UnknownToolOutcome`.

## Phase 6 — Cloudflare runtime

### Deliverables

- `@effect-agent/storage-cloudflare` and `platform-cloudflare` packages.
- Cloudflare platform package whose bindings are Effect services/Layers.
- One SQLite-backed Durable Object per Conversation.
- Local Conversation Log and Submission Ledger tables.
- Durable Object alarm service for unsettled work.
- Startup reconciliation before new work.
- R2 artifact service if needed.
- Miniflare/Workers test harness and deployment configuration.
- The same Travel Planner Agent Definition and scenario suite assembled with `DC` Cloudflare Layers,
  with live model Layers still optional and no Worker application scaffold.

### Exit gates

- Eviction at every failpoint recovers without an incoming client request.
- Alarm handlers are idempotent under at-least-once delivery.
- Important state never depends on in-memory Durable Object fields.
- Agent/core/engine packages import no Cloudflare platform types.
- The same durability conformance suite passes for Node and Cloudflare.
- Resource limits are checked before admission.
- Travel Planner produces equivalent canonical outcomes under `DN` and `DC` while eviction and
  alarm retries exercise the Cloudflare-specific recovery path.

## Phase 7 — Internal hardening

### Deliverables

- Adapter certification suite.
- TLA+/PlusCal or Alloy model for ordering, joining, settlement, and uncertainty.
- Administrative explain, verify, retry, and wake operations.
- Security review and threat model.
- Chaos and soak tests.
- Internal usage feedback and API simplification.
- Travel Planner internal use with live model and selected supplier Layers, red-team cases for
  untrusted supplier content and traveler data, and evidence across mocked, `DN`, and `DC`
  assemblies.

### Exit gates

- Safety properties hold under the documented assumptions.
- Security review has no unowned critical finding.
- Operators can explain recovery state without editing storage.
- At least three real internal Agents validate the authoring model.
- Travel Planner is one of those internal Agents and retains a deterministic offline conformance
  profile alongside its live integration profiles.

## Deferred until open-source preparation

- stored-data migrations and compatibility windows;
- retention/deletion policy beyond indefinite internal retention;
- public package names, license, governance, npm publication, and final `dist` export maps;
- PostgreSQL and generic multi-node scheduling;
- durable Subagents and detailed child budgets;
- channel integrations, UI, hosted control plane, and marketplace.

## Workstreams for a large AI project

Parallel work is safe along these boundaries:

1. **Agent/type workstream** — Agent, IDs, policies, type inference around Effect AI.
2. **Engine workstream** — Turn reducer, Effect AI Response handling, Run Events.
3. **Effect AI testing workstream** — scripted LanguageModel and provider smoke tests.
4. **Tool execution workstream** — Toolkit handlers, concurrency, approval, uncertainty.
5. **Conversation workstream** — records, reducers, projections, checkpoints.
6. **Node storage workstream** — SQLite services and conformance.
7. **Recovery workstream** — admission, settlement, joining, crash model.
8. **Cloudflare workstream** — Durable Object Effect services and conformance.
9. **Security/operations workstream** — policy, telemetry, diagnostics.
10. **Examples/documentation workstream** — compiling package-local internal reference Agents.

One domain integrator owns shared Agent and record Schemas. One recovery integrator owns accepted
work invariants. Effect AI gaps are raised upstream rather than solved by silently creating
parallel public primitives.

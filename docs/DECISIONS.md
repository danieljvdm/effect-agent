# Owner decision register

This register contains settled product rules and explicit deferrals. It is not a transcript of the
discussion that produced them. Research attribution and point-in-time observations about other
projects belong in [REFERENCE-ANALYSIS.md](REFERENCE-ANALYSIS.md).

Status values:

- **Accepted** — authoritative for implementation.
- **Accepted by default** — adopted as a roadmap-assigned implementation default and already
  implemented; owner review may still amend it.
- **Deferred** — intentionally postponed until the stated trigger.
- **Proposed** — a future recommendation that still needs owner approval.

Implementation agents must not silently change an accepted decision. Accepted architectural
decisions receive or update an ADR.

## Product and runtime

### D-001 — Product shape

**Status:** Accepted
**Decision:** Build a general-purpose Effect-native agent framework in validated stages. Start with
a narrow typed interpreter, then add persistent Conversations and durable accepted-work execution
without replacing the authoring model or semantic Turn loop.

Record: [ADR-0001](adr/0001-effect-native-core.md)

### D-002 — Relationship to Effect AI

**Status:** Accepted  
**Decision:** Use Effect AI primitives directly in public authoring and runtime APIs. Pin one exact
Effect v4 version at a time and upgrade deliberately.

The framework does not duplicate `Tool`, `Toolkit`, `LanguageModel`, `Prompt`, `Response`, or
`Model`. Generally useful missing capabilities should be contributed upstream.

Record: [ADR-0002](adr/0002-use-effect-ai-primitives.md)

### D-003 — Tool and Toolkit ownership

**Status:** Accepted  
**Decision:** Use Effect AI `Tool` and `Toolkit` directly. Framework scheduling, durability, and
recovery behavior surrounds their handlers without introducing a competing Tool type.

Record: [ADR-0002](adr/0002-use-effect-ai-primitives.md)

### D-004 — Staged durability

**Status:** Accepted  
**Decision:** Ship the ephemeral interpreter first. Add persistence and durable execution as
separate packages and explicitly verified guarantees over the same semantic engine.

Records: [ADR-0001](adr/0001-effect-native-core.md),
[ADR-0003](adr/0003-canonical-log-and-ledger.md)

### D-005 — Durable acceptance boundary

**Status:** Accepted  
**Decision:** `submit` returns a Receipt only after the Submission Ledger entry, Conversation
materialization, durable attachments, and readiness marker are durable. Submitted user input
becomes canonical history when the Submission is claimed for execution, before the model can
consume it.

Record: [ADR-0003](adr/0003-canonical-log-and-ledger.md)

### D-006 — Conversation submission policy

**Status:** Accepted  
**Decision:** Each Conversation has one ordered durable submission queue and at most one active
head by default. An active run may claim a contiguous ready prefix of later input at safe Turn
boundaries. Different Conversations may execute concurrently.

Record: [ADR-0003](adr/0003-canonical-log-and-ledger.md)

### D-007 — Tool scheduling default

**Status:** Accepted  
**Decision:** Execute each validated Tool batch with finite parallelism. The engine uses Effect
structured concurrency and `Semaphore` permits around Effect AI Toolkit handlers. Completion and
progress may be observed in real completion order; results presented to the next model Turn and
committed canonically retain the model's original Tool Call order.

A Run or Tool policy may require sequential execution. Unbounded Tool execution is never the
default.

Record: [ADR-0005](adr/0005-bounded-parallel-tool-scheduling.md)

### D-008 — Tool failure policy

**Status:** Accepted  
**Decision:** A Tool failure remains in the Effect error channel and fails the operation by
default. A legitimate empty result is a successful value such as `Option.none`.

Effect AI's explicit `failureMode: "return"` remains available when an application deliberately
wants a typed failure to become a model-visible Tool result.

Records: [ADR-0002](adr/0002-use-effect-ai-primitives.md),
[ADR-0004](adr/0004-uncertain-external-effects.md)

### D-009 — Ordinary Tool uncertainty

**Status:** Accepted  
**Decision:** If an ordinary Tool may have completed externally but no canonical outcome was
recorded, recovery emits `UnknownToolOutcome` and never automatically replays it.

Record: [ADR-0004](adr/0004-uncertain-external-effects.md)

### D-010 — Durable Step availability

**Status:** Accepted  
**Decision:** Design the durable Step record shape with the durable protocol. Implement `step.do`
only after the base journal and recovery classifier pass their crash tests.

Record: [ADR-0004](adr/0004-uncertain-external-effects.md)

### D-011 — Join, steering, and follow-up behavior

**Status:** Accepted  
**Decision:** Steering never mutates an in-flight model response or skips active Tool Calls. It is
consumed after the current response and complete Tool batch, before the next model request.
Follow-up input is consumed only when the Agent would otherwise stop.

The ephemeral runtime implements these seams with Effect queues. The durable runtime uses claimed
`joining` and `joined` Submission states so the same behavior survives process loss.

Record: [ADR-0008](adr/0008-turn-boundary-input-delivery.md)

### D-012 — Provider Tool execution

**Status:** Accepted  
**Decision:** The engine owns application Tool validation, scheduling, and durable boundaries.
Provider-executed built-in Tools are separate, capability-qualified behavior with explicit recovery
limitations.

Record: [ADR-0002](adr/0002-use-effect-ai-primitives.md)

### D-013 — Child agent budgets

**Status:** Proposed
**Recommendation:** Reopen Subagents in two slices: attached ephemeral delegation immediately
after Phase 3, and durable attached delegation after the Phase 4/5 recovery foundations. A child
receives a finite allocation reserved from its parent's remaining delegation budget; consumption
is charged exactly once to every ancestor, and unused reservation returns exactly once through an
idempotent accounting transition after settlement. The grant is only a ceiling: each protected
child action reauthorizes current policy, and parent approval is not transitive.

The proposed first release is depth one, attached join only, and uses declared Effect AI Tools with
fresh child Conversations. Owner approval and roadmap placement remain required.

Proposed record: [ADR-0010](adr/0010-declared-attached-subagents.md)
Specification: [Subagents](spec/subagents.md)

## Platforms, persistence, and operations

### D-014 — Initial platforms

**Status:** Accepted  
**Decision:** Support Node.js with SQLite and Cloudflare Workers with SQLite-backed Durable
Objects. Do not build PostgreSQL support now.

Platform storage, scheduling, alarms, and host lifecycle are Effect services supplied through
Layers. Agent definitions and the semantic engine remain platform-independent.

### D-015 — Stored schema compatibility

**Status:** Accepted  
**Decision:** Do not implement or promise data migrations during private development. Stored
records remain version-tagged so incompatible data fails clearly; development state may be reset
after a breaking change.

Reopen migrations before external users depend on persisted data.

### D-016 — Canonical retention

**Status:** Deferred  
**Decision for now:** Retain canonical records indefinitely. Reopen deletion and retention policy
before external release or when a concrete legal, privacy, or cost requirement appears.

### D-017 — Reasoning and content persistence

**Status:** Accepted  
**Decision:** Persist model content exposed by Effect AI/provider events, including assistant text,
returned reasoning text, encrypted/signature fields, provider redaction markers, completed Tool
Calls, Tool results, stop reason, usage, and model metadata.

Never request, infer, or store hidden chain-of-thought that the provider does not expose. Partial
streaming Tool argument fragments remain live-only; persist only the completed Tool Call.

### D-018 — Project destination

**Status:** Accepted  
**Decision:** Build and use the project internally first. Prepare it for open source after the
design has been validated.

### D-019 — Effect release line

**Status:** Accepted  
**Decision:** Build against Effect v4, pinned to one exact release at a time. While v4 is beta,
upgrades remain deliberate repository-wide changes gated by the conformance suite.

### D-020 — Canonical storage architecture

**Status:** Accepted  
**Decision:** Maintain an append-only canonical Conversation Log separately from the operational
Submission Ledger.

Conversation history is authoritative for applied inputs and terminal outcomes. The ledger owns
admission and live coordination such as queue order, attempts, ownership, and abort requests. A
terminal ledger row that disagrees with canonical history is repaired from history.

Record: [ADR-0003](adr/0003-canonical-log-and-ledger.md)

### D-029 — Durable runtime placement and leases

**Status:** Accepted by default (adopted as the Phase 4 implementation default; pending owner
review)

**Decision:** The durable coordinator (`DurableAgentRuntime`), recovery classifier, and run
journal live in `@effect-agent/session`, which now depends on `@effect-agent/engine`; platform
packages remain Layer assembly, host gates, and crash harnesses. Ledger claims fence with
atomically bumped producer epochs as the correctness authority, while renewable ownership leases
(default 30 seconds, adapter-configurable) provide liveness only. The SQLite storage version is 2
with an exact-or-zero compatibility check: version-1 development files fail with typed reset
guidance and are never migrated. Canonical model output is recorded per Turn as
`ModelResponseRecorded` carrying the Turn's Schema-encoded Prompt messages; per-delta canonical
records were rejected for volume and remain a Phase 5 consideration.

Record: [ADR-0011](adr/0011-durable-runtime-placement-and-leases.md)
Evidence: [Phase 4 evidence](PHASE-4-EVIDENCE.md)

### D-030 — Durable Tool uncertainty, Steps, and suspension records

**Status:** Accepted by default (adopted as the Phase 5 implementation default; pending owner
review)

**Decision:** Which Tool Calls enter the prepared/settled uncertainty protocol is declared by an
Effect AI execution-class annotation (`readonly` | `idempotent` | `uncertain`) with a fail-closed
`uncertain` default for unannotated Tools; always-prepared was rejected for volume and for
converting free re-runs into Unknown Outcomes. The Durable Step service tag and API
(`step.do(name, OutputSchema, effect)` — the Schema is the canonical codec for the recorded
result) live in `@effect-agent/engine`, provided locally per Tool Call over a session-implemented
`RunStepHook`. Step results persist as settled-success-only `ToolStepSettled` canonical records
in the Conversation Log — no separate Step Store port, no prepared Step records, no recorded Step
failures. A superseding Attempt audits an incomplete prior response with a first-class
`ModelResponseInterrupted` canonical record. Durable approval suspension writes no canonical
suspension record: the canonical `ToolApprovalRequested` is the boundary evidence and suspension
is rebuildable ledger state. Unknown/approval resolution authorization in P5 is service-possession
plus mandatory author/reason audit fields — the same trust boundary as `abort`; the authenticated
operator surface, aging, and alerting are P7 scope.

Record: [ADR-0012](adr/0012-durable-tool-uncertainty-and-steps.md)
Evidence: [Phase 5 evidence](PHASE-5-EVIDENCE.md)

### D-031 — Durable Subagent establishment, waiting suspension, and binding resolution

**Status:** Accepted by default (adopted as the S2 implementation default; pending owner
review — the Subagent capability decision itself, ADR-0010/D-013, remains Proposed)

**Decision:** The durable delegation seam is an engine-owned `RunSubagentHook` contract with a
per-batch `SubagentDurability` service and an explicit ephemeral default; session implements the
hook inside the coordinator and capabilities consumes it, so no package gains a forbidden
dependency edge. A durable parent waits as `waitingForChild` through an additive
`SuspensionReason` member plus one new idempotent cross-lane wake operation
(`recordChildSettled`) — no new submission state. Recovery is primarily idempotent delegation-
handler re-entry via batch resume, with binding-free repair executors only; the encoded child
input rides the canonical `SubagentRequested` record so admission completes without a live
handler, and child identity is derived deterministically from the parent Run and Tool Call pair.
Admission recovery uses a tri-state `resolveAdmission` port operation (`notAdmitted` | `admitted`
| `indeterminate`). Durable recovery resolves Agent Bindings through the host-supplied
`AgentBindingResolver` by stable identity and exact stored digests, fail-closed: an unresolvable
parent-linked child settles the framework `ChildCompatibilityFailure`, a failed durable child
joins as the bounded `SubagentExecutionFailure` (no raw Cause; `mapChildFailure` stays the
ephemeral contract), and a root head is refused typed. Child budget reservations are generic
opaque state-machine rows in the one `SubmissionLedger` port; delegation calls remain ordinary
prepared Tool Calls excluded from `MarkUnknown` by durable evidence plus the core-owned naming
rule. Authorization scope is service possession plus structural Parent-Link/digest verification;
authenticated per-read authorization, aging, and alerting stay P7. Accounting is conservative:
structural dimensions from canonical child evidence, unreported token/cost dimensions consume
their reservation, overruns recorded and never clipped.

Record: [ADR-0013](adr/0013-durable-subagent-establishment.md)
Evidence: [S2 evidence](S2-EVIDENCE.md)

### D-032 — Cloudflare Conversation Objects

**Status:** Accepted by default (adopted as the Phase 6 implementation default; pending owner
review)

**Decision:** Deployment class `DC` runs the unchanged durable coordinator inside one
SQLite-backed Durable Object per Conversation (`namespace.idFromName(conversationId)`): no
worker loop — each ingress event or alarm runs one bounded reconcile-then-drain pass, with the
constructor gate local-only so cross-Object recovery can never deadlock. One multiplexed storage
alarm is the liveness engine under the pre-armed invariant that committed nonterminal work
implies a committed alarm, making eviction recovery request-free; alarm passes are idempotent
under at-least-once delivery. Storage mirrors the Node v4 tables in one fresh migration plus an
`effect_agent_meta` exact-or-fresh version gate and the durable `effect_agent_child_settlements`
cross-store notification marker; transactions use Durable Object storage transactions (the
Node write-contention machinery has no analogue and is absent). Cross-Object port calls travel
as Schema-encoded envelopes over Durable Object JS RPC across a closed route-capable subset,
with adapter-minted routable Submission identities (`{uuidv7}:{conversationId}`) and transport
faults surfacing as `AdmissionIndeterminate`, never absence. Admission limits (queue depth,
input bytes, database size under the 10 GB cap) are checked before `submit`; the ~1.9 MB stored
value bound fails typed and R2 stays deferred with the `AttachmentStore` port. DN ≡ DC is
claimed as byte-equal cross-platform normalized canonical evidence against one committed golden.
The Cloudflare suites run on `@cloudflare/vitest-pool-workers` plus a programmatic Miniflare
restart lane, via direct `vitest run` (the probed `vp test` exception).

Record: [ADR-0014](adr/0014-cloudflare-conversation-objects.md)
Evidence: [Phase 6 evidence](PHASE-6-EVIDENCE.md)

### D-033 — Phase 7 hardening shape

**Status:** Accepted by default (adopted as the Phase 7 implementation default; pending owner
review — Phase 7 completes the roadmap table, so resolving this record, ADRs 0011–0015, and the
still-Proposed ADR-0010 is the owner review that roadmap completion now awaits)

**Decision:** Adapter certification is one three-tier entry point (`certifyDurableAdapters`)
producing a Schema-encoded certificate: the shared conformance arrays verbatim (Tier 1), the
real coordinator swept across every failpoint location and six scenario shapes into the single
shared invariant checker `verifyConversationInvariants` with a fully recomputed digest chain and
a pinned unreachable-location list (Tier 2), and real loss recorded honestly as
exercised / recorded-evidence / not-exercised / not-applicable (Tier 3) — certificates are
committed evidence artifacts, never the assertion source. The formal model is TLA+/PlusCal
checked by TLC on bounded committed instances with negative controls, run manually and on a
schedule but never as a PR gate; its claim is about the protocol design under enumerated
abstraction assumptions, explicitly not about the code, whose claim remains the linked test
corpus. The administrative operations (explain/explainConversation, verify, retry, wake,
scanObligations) are coordinator members over the two storage ports only — identical on DN and
DC — with a scripts/ CLI rather than a new package; explain is provably read-only, verify never
repairs and scopes honestly, retry re-drives exactly the classifier's one decision with the
existing audit record, and obligation scanning is scan-based with host-owned alerting. Admin
authorization is the minimal `OperationAuthorizer` port: possession-default, fail-closed typed
denial before any read or write when a host supplies a real authorizer, mandatory author/reason
on mutating operations. The closing semantic fixes: the model-vindicated
`AwaitParentEstablishment` classifier decision (with the worker-claim gate) closes the
cross-Object establishment race; aborted never-claimed non-head ready Submissions settle
immediately with the gap rule treating them as non-gaps; defects stay defects with
`withTerminalDefectEvent` as the bounded opt-in; `LedgerCapabilities.durability` gains
`durable-cloudflare`; and the shared SQL core is deliberately not extracted (ADR-0014 revisit).

Record: [ADR-0015](adr/0015-hardening-shape.md)
Evidence: [Phase 7 evidence](PHASE-7-EVIDENCE.md)

## Integration and project boundaries

### D-021 — Reference-material role

**Status:** Accepted  
**Decision:** Flue and Pi are attributed source material used to study agent-loop, interaction, and
durability behavior. Their role ends at research. Effect Agent's APIs, runtime, storage protocol,
provider integration, and compatibility contract are defined entirely by this repository's native
Effect specifications. The pinned source snapshots live in the repository at `repos/flue` and
`repos/pi`.

Detailed source observations belong only in
[REFERENCE-ANALYSIS.md](REFERENCE-ANALYSIS.md).

### D-022 — Model integration

**Status:** Accepted  
**Decision:** Effect AI `Model`, `LanguageModel`, `Tool`, `Toolkit`, `Prompt`, `Response`, and
provider Layers are the model integration boundary. Framework code adds only Agent-loop and
durability concepts that Effect AI does not own.

Record: [ADR-0002](adr/0002-use-effect-ai-primitives.md)

### D-023 — Project identity and distribution

**Status:** Accepted (amended 2026-08-14)
**Decision:** The project is private for now. `effect-agent` and `@effect-agent/*` remain working
names. Public naming and governance are deferred until open-source preparation. Two halves were
resolved by owner decision on 2026-08-14: the packages publish to npm on the opt-in **beta
dist-tag** for live integration testing (see the TOOLCHAIN release runbook), and they carry the
**MIT license**.

### D-024 — Repository toolchain and shape

**Status:** Accepted  
**Decision:** Use a Vite+ monorepo based on
`danieljvdm/vp-effect-cf-template`. Use its Bun workspace, exact dependency catalog,
Effect-aware TypeScript patch, and catalog-to-source Effect synchronization. Framework code lives
in phase-gated `packages/*`; runnable consumer benches live in leaf `examples/*` workspaces. Do not
create an `apps/` workspace.

Include `danieljvdm/agent-skills` as contributor tooling and keep it separate from the framework's
runtime Skill abstraction.

Records: [ADR-0006](adr/0006-package-only-vite-plus-monorepo.md),
[ADR-0009](adr/0009-leaf-example-workspaces.md)

### D-025 — Slim toolchain and canonical Effect source

**Status:** Accepted  
**Decision:** Keep only the active Phase 1 framework packages: `core`, `engine`, and `testing`.
Create later packages only when their roadmap phase begins. Shared
TypeScript configuration lives at the repository root.

The framework and contributor tooling resolve one exact Effect runtime. Follow Effect v4 in the
canonical `Effect-TS/effect` repository. Remove direct dependencies and wrapper scripts already
supplied by Vite+ or the root toolchain.

Record: [ADR-0007](adr/0007-slim-toolchain-and-canonical-effect-source.md)

### D-026 — Progressive reference application

**Status:** Accepted  
**Decision:** Maintain one cumulative Travel Planner Reference Application across every roadmap
phase. Its default model and travel-service implementations are deterministic Layers so the full
green suite runs without credentials or network access. Selected live model providers and travel
suppliers may be demonstrated through opt-in Layers and smoke tests.

The executable contracts and reusable deterministic fixtures live in `@effect-agent/testing`.
`examples/demo` is a private leaf consumer that renders the same scenario as a browser test bench;
it does not create an `apps/` workspace or a production dependency on `@effect-agent/testing`.
Each phase extends the same scenario to prove only the maturity claim available in that phase. In
particular, persistent Conversation history does not imply durable accepted work, and external
booking mutations do not imply exactly-once execution.

### D-027 — Agent Definition and Model Binding

**Status:** Accepted

**Decision:** Keep Agent Definitions model-agnostic. `Agent.define` creates the immutable
definition; `Agent.withModel` pairs it with one concrete Effect AI `Model`. The runtime accepts
only this explicit Agent Binding. The Model Layer's requirements remain visible in the Run's `R`.

Do not make `LanguageModel` an ambient runtime requirement merely to support provider substitution.
Applications that select Models dynamically do so with ordinary Effect services before creating
the Binding.

Record: [ADR-0002](adr/0002-use-effect-ai-primitives.md)

### D-028 — Leaf example workspaces

**Status:** Accepted

**Decision:** Keep runnable, local consumer demonstrations under `examples/*`. Each example is a
leaf workspace that may depend on public framework packages and `@effect-agent/testing`; no
framework package may import it. Examples must use the real public runtime path, remain included
in the root check/test/build gates, and avoid implying deployment or durability guarantees.

The Phase 0 demo uses TanStack Start, Effect Atom, Tailwind CSS, shadcn/ui on Base UI, and Vercel AI
Elements-style components. Its scripted Model remains the default so ordinary verification
requires no credentials or network.

Record: [ADR-0009](adr/0009-leaf-example-workspaces.md)

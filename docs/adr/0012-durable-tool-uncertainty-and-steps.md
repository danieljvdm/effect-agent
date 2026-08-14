# ADR-0012: Durable Tool uncertainty protocol, Steps, and suspension records

- Status: Accepted by default
- Status note (2026-08-13): adopted as the Phase 5 implementation default by the integrated P5
  plan (decision points 2, 3, 4, 7, 10, and 11) and implemented as such
  ([Phase 5 evidence](../PHASE-5-EVIDENCE.md)); owner review may still amend it
- Date: 2026-08-13
- Decision owners: Project owner
- Related decisions: D-004, D-008, D-009, D-010, D-020, D-029, D-030
- Builds on: [ADR-0004](0004-uncertain-external-effects.md),
  [ADR-0011](0011-durable-runtime-placement-and-leases.md)

## Context

Phase 5 turns ADR-0004's uncertainty stance into executable protocol: prepared/settled records
for consequential ordinary Tools, Unknown Outcomes with an authorized resolution path, Durable
Steps, durable approval suspension, and joined queued input. Six shape-level choices recur in any
re-implementation and need a durable record:

1. which Tool Calls enter the prepared/settled protocol;
2. where the Durable Step service lives;
3. what record family persists Step results;
4. what the Step API looks like;
5. how an interrupted model response is audited;
6. what record family persists approval suspension.

## Decision

### Prepared-record scope: execution-class annotation, fail-closed `uncertain` (plan point 7)

Tools declare an execution class through the Effect AI annotation
`Tool.annotate(ToolExecutionClass, "readonly" | "idempotent" | "uncertain")` defined in
`@effect-agent/engine`:

- `readonly` opts out of preparation entirely — a crash between start and settlement is a free
  re-run, preserving the exact P4 crash-matrix behavior for search-style Tools;
- `idempotent` writes prepared/settled records and lets recovery re-execute without
  reconciliation proof — the annotation IS the declared external idempotency contract, carries no
  key, and leaves key derivation to the handler;
- `uncertain` — the default for unannotated Tools — writes prepared/settled records and requires
  reconciliation proof or an Unknown Outcome.

The default is fail-closed per ADR-0004: the framework never infers a safer class from a Tool's
shape. The visible consequence is a deliberate migration: the P4 fixtures annotate their search
Tools `readonly` to keep prior canonical histories byte-stable.

**Rejected — always-prepared:** writing prepared records for every Tool Call was rejected for
record volume and because it silently converts every P4 crash row into an Unknown Outcome,
destroying the free-re-run property of read-only Tools.

**Rejected — schema-shape inference:** classifying by parameter/success schema shape (for
example "no mutation verbs") guesses about external semantics, violating the fail-closed rule.

### Durable Step placement: engine-owned service over a session hook (plan point 2)

The `DurableStep` service tag and API live in `@effect-agent/engine`; the persistence lives in
`@effect-agent/session` behind the dependency-neutral `RunStepHook`
(`lookup`/`commit`, keyed by `{toolCallId, stepName}`). The engine provides the service locally
to each Tool Call's handler stream, bound to that call's identity — the same local-provision
template as `RunEventSink` and `AgentSpawner`. Declaring the tag in
`Tool.make({ dependencies: [DurableStep] })` is what makes a Tool a Durable Tool; the engine
never satisfies it from an application Layer. Without a durable runtime the engine provides an
ephemeral pass-through that executes each Step once and records nothing — the durable claim
attaches to the runtime, not the Tool.

**Rejected — session-owned tag:** session cannot scope a service per `ToolCallId` without engine
cooperation, and handlers would import session, inverting the package dependency direction
(`engine ← session`).

### Step record family: settled-success-only canonical records (plan point 3)

Step results persist as `ToolStepSettled` canonical records in the Conversation Log (record and
batch identity `step:{runId}:{toolCallId}:{stepName}`), not in a separate Step Store port.
Persistence §2.5's "Step Store" is satisfied as a logical record family over the existing log.
Reusing the log inherits batch idempotency and epoch fencing — the fenced winner's record commits
and a racing loser replays it, which is exactly durability §11's racing-writers rule — and keeps
one recovery truth (DUR-015). Only success is recorded.

**Rejected — physical Step Store port:** a second durable store splits recovery truth across
stores and duplicates the fencing machinery; revisit only if P6 Durable Object locality demands
it.

**Rejected — recorded Step failures:** a recorded failure would replay a transient supplier
outage forever; failures fail the step call into the handler's error channel and re-entry
re-executes.

**Rejected — prepared Step records:** under an at-least-once body contract "may have executed"
is the normal case, so a prepared marker adds no recovery information.

### Step API shape: `step.do(name, OutputSchema, effect)` (plan point 4)

The API matches the future-docs sketch. The Schema argument is load-bearing, not decoration: it
is the canonical codec for the recorded result — commit encodes through it, replay decodes
through it, and a recorded result that no longer decodes is a typed
`recorded-result-invalid` conflict rather than silently accepted data (STORE-006).

**Rejected — schema-less `step.do(name, effect)`:** the recorded value would be unvalidated JSON
whose meaning drifts with code changes, exactly the class of bug the schema-first rule exists to
prevent.

### Interruption audit: first-class `ModelResponseInterrupted` (plan point 10)

When a superseding Attempt resumes a Run whose prior owner left no complete canonical Turn, it
appends a first-class `ModelResponseInterrupted` audit record
(`interrupted:{runId}:{supersededEpoch}`) before re-invoking the model. This satisfies durability
§9 ("the attempt is recorded as interrupted") and makes duplicate provider cost observable in
canonical history. The record is prompt-transparent.

**Rejected — overloading `RepairAnnotated`:** that record is reserved for executed recovery
repairs and is not emitted for deferred resumes, so interruption evidence would be structurally
absent exactly where it matters most (a worker resuming without a recovery pass).

### Suspension record family: no canonical "suspended" record (plan point 11)

Durable approval suspension writes no canonical suspension record. The canonical
`ToolApprovalRequested` is the boundary evidence — durability §8 names "waiting for explicit
approval, if the approval request is canonical" as a safe resumption boundary — and suspension
itself is operational ledger state (`suspended`, with its reason), rebuildable from history. The
resuming Attempt appends the canonical `ToolApprovalDecided` before honoring any decision.

**Rejected — canonical `RunSuspended`/`RunResumed` pair:** suspension is scheduling state, not
conversation history; a canonical pair would have to be repaired whenever the ledger and log
disagree, creating a second truth for the same fact (violates DUR-015's single-authority rule).

## Consequences

Positive:

- consequential external mutation is now covered by an executable honesty protocol: recovered
  supplier truth, provably-safe repetition, or a visible Unknown Outcome — never a fabricated
  result;
- Durable Tools run unchanged on the ephemeral runtime with honest (weaker) semantics;
- canonical history remains the single recovery truth for Tools, Steps, approvals, and
  interruptions;
- P4 histories replay byte-identically (`readonly` annotations, no-tool Turns keep the single
  batch).

Negative:

- Tool authors must annotate execution classes or accept the `uncertain` default's operational
  cost (blocked lanes awaiting resolution);
- Step authors own deterministic naming and external idempotency keys;
- unknown-outcome lanes require an authorized resolution dependency before a deployment can
  claim durable liveness (DUR-017) — the operator surface, aging, and alerting are Phase 7 scope.

## Validation

- Engine seam ordering, pass-through Steps, replay-without-execution, duplicate-name conflicts,
  and truncated-argument rejection: `packages/engine/test/durable-tool-seam.test.ts`.
- Coordinator uncertainty protocol, reconciliation, unknown resolution, and Step replay under
  failpoint kills: `packages/testing/test/durable-tools.test.ts`.
- Durable approval suspension and decision delivery: `packages/testing/test/durable-approval.test.ts`.
- Pure classifier coverage of every new crash row: `packages/session/test/recovery-classifier.test.ts`.
- Reference-application slice with a real supplier desk and reconciler:
  `packages/testing/test/travel-planner-phase5.test.ts`.
- Real process-kill coverage: `packages/platform-node/test/crash/crash.test.ts`.

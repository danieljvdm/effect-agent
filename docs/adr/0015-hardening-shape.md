# ADR-0015: Phase 7 hardening shape — certification tiers, formal-model scope, admin authorization, and the closing semantic fixes

- Status: Accepted by default
- Status note (2026-08-13): adopted as the Phase 7 implementation default by the adopted P7 plan
  and implemented as such ([Phase 7 evidence](../PHASE-7-EVIDENCE.md)); owner review may still
  amend it. Phase 7 completes the roadmap table, so this record — like ADRs 0011–0014 — is part
  of the open governance backlog the owner resolves at roadmap completion.
- Date: 2026-08-13
- Decision owners: Project owner
- Related decisions: D-029, D-030, D-031, D-032, D-033
- Builds on: [ADR-0003](0003-canonical-log-and-ledger.md),
  [ADR-0004](0004-uncertain-external-effects.md),
  [ADR-0011](0011-durable-runtime-placement-and-leases.md),
  [ADR-0012](0012-durable-tool-uncertainty-and-steps.md),
  [ADR-0013](0013-durable-subagent-establishment.md),
  [ADR-0014](0014-cloudflare-conversation-objects.md)

## Context

Phase 7 is internal hardening: it must turn the P0–P6/S1/S2 protocol into _demonstrated_
properties — certification for third-party adapters, a formal model of the durable protocol, an
operator surface, an adversarial security review, chaos/soak evidence — and close the semantic
loose ends the earlier phases parked, without a breaking port redesign, without new canonical
payload tags unless a fix demands one, and without a storage-version bump. The shape-level
choices below recur in any re-implementation and need a durable record. Everything here is
implemented; the evidence map is [PHASE-7-EVIDENCE.md](../PHASE-7-EVIDENCE.md).

## Decision

### D-P7-1 — Adapter certification: one entry point, three tiers, honest statuses

`certifyDurableAdapters` (in `@effect-agent/testing`) runs against a candidate
`SubmissionLedger`/`ConversationStore` Layer pair and produces one Schema-encoded
`CertificationReport` (schemas in `@effect-agent/session/certification.ts`). Tier 1 runs the two
shared conformance case arrays verbatim (32 ledger + 8 store). Tier 2 assembles the real durable
coordinator over the candidate adapters and arms every `DurableRuntimeFailpointLocation` (28)
one-shot across six scenario shapes, asserting classifiable state and re-drive convergence
through public levers only into the shared `verifyConversationInvariants` with every Submission
settled and the digest chain fully recomputed (append-time producer-identity capture). The three
locations the six shapes cannot reach are pinned as an exact list the runners assert against —
scoped coverage can be stated but never silently grow. Tier 3 (real loss) is honest by
construction: `exercised` / `recorded-evidence` / `not-exercised` / `not-applicable`; `ok`
covers executed checks only. Certificates are committed under `docs/certification/` as
deterministic evidence artifacts; the runner tests own the semantic assertions (testing.md §12).

**Rejected — per-adapter bespoke certification suites:** duplicates drift back in exactly where
conformance was invented to prevent it.
**Rejected — treating the committed JSON as the assertion source:** golden data never replaces
semantic assertions; a certificate that could pass while the runner fails would be theater.
**Rejected — requiring a live crash lever from every adapter:** an honest `recorded-evidence`
citation of a committed real-loss suite is strictly better than a synthetic lever bolted on for
the certificate's sake; `not-exercised` stays visible instead of faked.

### D-P7-2 — Formal model: TLA+/PlusCal, bounded instances, correspondence — never a PR gate

The protocol's essence is temporal (crash/recovery interleavings, epoch supersession,
fairness-conditioned liveness), so TLA+ with PlusCal was chosen over Alloy. Two specifications
(`formal/DurableSubmission.tla`, `formal/SubagentEstablishment.tla`) are checked by TLC over six
committed `.cfg` instances including two negative controls that must fail (fencing disabled;
the establishment race). The claim is precisely scoped: the _protocol design_ satisfies the
stated properties on bounded instances under the abstraction assumptions enumerated in
`formal/CORRESPONDENCE.md` §3; the claim about the _code_ remains the executable test corpus,
linked action-by-action and invariant-by-invariant. TLC needs a JVM, so `bun run formal:check`
and the scheduled/dispatchable GitHub Actions workflow run outside `bun run ready` and the PR
gate, and the committed run results live in `formal/EVIDENCE.md`.

**Rejected — Alloy:** Alloy 6's temporal support is a poor fit for fairness-conditioned
liveness, and the team's artifacts (crash matrix, classifier) are already trace-shaped.
**Rejected — a PR-gating JVM step:** Bun-only CI stays Bun-only; a weekly/dispatch lane plus the
committed evidence is the honest cadence for a design-level check.
**Rejected — claiming the model verifies the implementation:** stated as a non-claim in the
model, the correspondence document, and the evidence.

### D-P7-3 — Administrative operations: coordinator members over the two ports, a scripts/ CLI, scan-based obligations

The five operations (`explain`/`explainConversation`, `verify`, `retry`, `wake`,
`scanObligations`) are additive `DurableAgentRuntime` members implemented over
`SubmissionLedger` + `ConversationStore` only — one implementation, identical on DN and DC; pure
report logic lives in `packages/session/src/admin.ts`. `explain` is provably read-only (tested
byte-identical durable state) and renders every classifier decision from a static meaning table;
`verify` never repairs and reports typed per-check results, honestly `skipped` where the port
cannot supply producer identity; `retry` re-drives exactly the classifier's one decision with
the existing `RepairAnnotated` audit and typed refusals; `scanObligations` is scan-based —
never a daemon — ages from timestamps that already exist (no storage-version bump), and hosts
own the alert loop (OPS-001/OPS-002). The CLI is a repository script (`scripts/durable-admin.ts`)
per D-024/D-025 — no new package before a phase requires one; DC deployments reach the same
operations through their Worker via the Schema-encoded Conversation Object entry points.

**Rejected — platform-specific admin implementations:** would fork operator semantics exactly
where DN ≡ DC equivalence is the product claim.
**Rejected — a framework-owned aging/alerting daemon:** rule 4 (no daemon fibers) and the
host-owned telemetry boundary; the framework provides the measurement, hosts own the loop.
**Rejected — an `@effect-agent/admin` package:** a root script satisfies the deliverable;
packaging is revisited at open-source preparation.

### D-P7-4 — Authorization: a minimal `OperationAuthorizer` port, possession default, fail-closed otherwise

A `Context.Reference` port (`authorize(request) → allow | deny`) is consulted by `observe`, all
admin operations, `resolveUnknown`, and `resolveApproval`. The default preserves the pre-P7
service-possession behavior, so nothing breaks for existing assemblies; a non-default Layer is
enforced fail-closed — typed `OperationDenied` **before any read or write**. Mutating operations
keep mandatory `author`/`reason` (SEC-011). The framework deliberately ships no identity system:
hosts authenticate and supply the authorizer. Two honesty records bound the stance: the cross-DO
`portCall` channel remains a namespace-binding-trust system channel (FINDINGS SEC-P7-002,
accepted risk), and the DC admin entry points run the possession default until a DC authorizer
config lever is added (FINDINGS SEC-P7-003, deferred with the hosted-platform work).

**Rejected — inventing a framework Principal/identity system:** hosts own authentication
(SEC-001); the framework enforces decisions, not identity.
**Rejected — per-request authorization on the cross-DO port:** would break the S2 cross-Object
delegation protocol; the namespace binding is the documented trust boundary.

### D-P7-5 — The closing semantic fixes (model-checked first where the model applies)

1. **`AwaitParentEstablishment`** closes the cross-Object establishment race: a parent-linked
   `admitted` Submission without canonical lineage defers — through the classifier **and**
   through the worker claim path (the claim head rule legally grants an admitted head, so both
   entrances enforce the same deferral) — and the parent's idempotent establishment completes
   it. TLC demonstrated the race reachable under the old discipline and eliminated under the new
   one _before_ implementation; the cross-DO pin is "a child never runs a Turn before its
   lineage record is canonical". The `RecoveryDecision` union is now 29 tags.
2. **Aborted non-head ready Submissions settle immediately** through the recovery/maintenance
   pass. Settlement order of never-run work is not execution order (DUR-004 bounds execution;
   DUR-012 permits settling inactive accepted work); adapters authorize exactly the aborted
   outcome by the durable abort intent, and the contiguous-joining-prefix gap rule treats
   aborted-settled rows as non-gaps in all three adapters. The shared ledger conformance suite
   grew to 32 cases; DN, DC, and a process-kill crash row pin the behavior.
3. **Defects stay defects by default.** The engine never converts a defect into a typed event;
   `withTerminalDefectEvent` is the exported, documented, bounded opt-in boundary helper
   (runtime.md §10), adopted by the demo.
4. **`LedgerCapabilities.durability` gained `"durable-cloudflare"`** so the DO adapter stops
   reporting `"durable-node"`; no caller branches on the literal.
5. **No shared SQL core was extracted** (the ADR-0014 D-P6-4 revisit): no third SQL adapter
   appeared, extraction would churn two landed adapters for zero behavior, and the shared
   conformance suites remain the anti-drift guard. Revisit if a third SQL adapter is built.

**Rejected — repairing the establishment race in the coordinator's tolerance paths only:**
the stale-tail refresh made it survivable, not ordered; hygiene belongs in the classifier where
it is explainable.
**Rejected — converting defects to typed `RunFailed` events by default:** violates "don't widen
errors" and would launder invariant violations into expected failures.
**Rejected — a storage-version bump for obligation aging:** every needed timestamp already
exists on the snapshots and canonical records.

## Consequences

Positive:

- a third-party adapter has one documented, executable path to a certificate whose scope
  statements cannot silently drift;
- the durable protocol's safety story is layered — model-checked design, certified adapters,
  seeded chaos, real-loss matrices — all converging on **one** shared invariant checker, so the
  claims cannot fork;
- operators get explain/verify/retry/wake/obligations with identical semantics on both
  deployment classes, and the authorization seam exists without a framework identity system;
- the last known protocol races and honesty gaps (establishment ordering, queued-abort latency,
  defect terminalization, the durability label) are closed with pinned evidence.

Negative:

- the formal claim is bounded-instance and design-level; nobody should read it as code
  verification (stated everywhere, but the distinction demands reader attention);
- certification Tier 2 runs 168 coordinator assemblies per adapter — minutes of test time per
  runner, the price of sweeping every failpoint;
- the possession-default authorizer means tenant isolation on admin surfaces is only as real as
  the host-supplied Layer, and the DC entry points cannot install one yet (recorded findings);
- the single test lane means soak/crash cadence is paid on every full test run until a release
  lane exists (recorded honestly in the operations guide).

## Validation

- Certification: `packages/testing/test/certification-memory.test.ts`,
  `certification-sqlite.test.ts`, `packages/storage-cloudflare/test/certification.test.ts`;
  committed certificates under `docs/certification/`.
- Formal: `formal/EVIDENCE.md` (TLC run results), `formal/CORRESPONDENCE.md`,
  `scripts/formal-check.ts`.
- Admin + authorization: `packages/testing/test/admin-operations.test.ts`,
  `packages/platform-cloudflare/test/admin-encoded.test.ts`,
  `packages/testing/test/security/redteam-idor-sweep.test.ts`.
- Semantic fixes: `packages/session/test/recovery-classifier.test.ts`,
  `packages/testing/test/durable-runtime.test.ts`,
  `packages/platform-cloudflare/test/eviction.test.ts` and `subagents-cross-do.test.ts`,
  `packages/platform-node/test/crash/crash.test.ts`, `packages/engine/test/agent-runtime.test.ts`.
- See [Phase 7 evidence](../PHASE-7-EVIDENCE.md) for the full gate-by-gate map.

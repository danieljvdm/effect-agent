# ADR-0019: Budget soft landing, delegation containment, and budget extension

- Status: Accepted (owner-directed, 2026-08-15; S1 engine soft landing assigned and implemented,
  S2 delegation containment and S3 budget extension assigned as follow-on slices)
- Status note (2026-08-15): S2 implemented. Mechanics delta from decision item 6: Effect AI's
  native `failureMode: "return"` converts EVERY handler failure into a result (`Toolkit.handle`'s
  `Stream.catch` has no passthrough), which would encode the engine-owned `ToolCallWaiting`
  suspension signal as data and silently orphan a durable child. The implemented containment
  therefore keeps the underlying Tool on Effect AI `failureMode: "error"`, widens the Tool
  success Schema to the union of the declared success and the contained failure family, and
  catches exactly that family in the delegation handler — the contract of item 6 (contained
  members model-visible, engine signals stay in the error channel) holds unchanged, and the
  durable settlement join records the contained failure with the non-failure polarity the live
  batch continues with (SUB-019 coherence). SUB-033 pins the requirement; the pr-review
  shadow-Tool workaround is retired and both reviewer profiles adopt the S1 final-answer default.
- Status note (2026-08-15, later): S3 implemented (RUN-021, SUB-034). `RunOptions` gains
  tightening-only `toolCallAllowance`/`turnAllowance` (effective limit =
  `min(policy, max(1, floor(allowance)))`, the `onExhaustion` resolution keys off the effective
  limits); `Subagent.define` gains `toolCallAllowance: { default, fromParameters }` clamped
  fail-closed to the delegation's per-invocation reservation slice; `projectResult` receives the
  bounded `SubagentResultContext` with the honest `budgetExhausted` marker (from the ephemeral
  child result, or the child Settlement's durable marker carried through
  `ChildEstablishSettled.finishReason`). One scope delta from decision item 7: the allowance
  reaches only EPHEMERAL children — a durable child's lane owns no per-run options channel, and
  making the reservation slice binding on the child would contradict §7's never-clipped overrun
  model, so durable allowance threading is deferred to its own proposal. The extension flow
  (fresh re-delegation with a raised allowance and partial findings forwarded through
  `prepareInput`) works on both paths today because the exhausted marker travels durably.
- Status note (2026-08-15, hardening after #50's autoreviewer): `define` is overloaded so the
  resolved Tool channels follow the `failureMode` VALUE (return mode cannot be claimed through a
  type argument while the runtime builds the error-mode Tool); containment classifies genuine
  engine signals through a module-private provenance wrapper applied at the only operations that
  can produce them, so an author-declared failure using the exported signal classes is contained
  as data rather than rethrown as a suspension; each delegation exposes its canonical
  `containedFailure` schema so consumers (pr-review coverage) can never diverge from the runtime
  family; and the contained durable join has failpoint recovery coverage at all three
  `subagent:after-join*` boundaries. One S2 adoption reverted deliberately: the pr-review CHILD
  reviewer returns to `onExhaustion: "fail"` — a review is a coverage claim, and schema-valid
  findings from a child whose reads were rejected would launder budget exhaustion into coverage;
  host-owned read-provenance is the recorded future work, and containment keeps the typed
  exhaustion non-fatal to the run.
- Related decisions: [D-007](../DECISIONS.md#d-007--tool-scheduling-default),
  [D-008](../DECISIONS.md#d-008--tool-failure-policy),
  [D-013](../DECISIONS.md#d-013--child-agent-budgets),
  [D-035](../DECISIONS.md#d-035--code-mode-capability),
  [D-037](../DECISIONS.md#d-037--budget-soft-landing-delegation-containment-and-budget-extension)

## Context

A research-shaped agent on a grep-and-read cadence spends one Tool Call per observation, and the
model cannot see the policy counter, so any `maxToolCalls`/`maxTurns` bound is an invisible cliff:
the Run that crosses it fails typed after doing real work and delivers nothing. Under attached
delegation the damage compounds — `Subagent.define` fixes `failureMode: "error"`, so one exhausted
scout child fails its parent Tool Call and detonates the whole parent Run
(`SubagentExecutionFailure: "Agent exceeded its 16 Tool Call limit"`). Both halves are observed in
production consumers: the pr-review fan-out reviewer records the containment gap as FRICTION #7
and routinely returns "unit unreviewed: AgentPolicyError" for large units, and the kommunikasie
deployment shipped budget raises plus prompt discipline as mitigation — which cannot bound the
tail, because the model is asked to respect a limit it cannot count against.

Prompt discipline treats a mechanism gap as a prompting problem. The mechanism gap is that the
engine conflates two different things: the _bound_ (work that would exceed the limit must never
start — correct and kept) and the _resolution_ (what happens to the Run once the bound binds —
previously always fatal). The Code Mode broker already models the distinction mid-pass (RUN-017:
exhaustion becomes the inner call's outcome, not a Run failure), and the subagent specification
already anticipates a granting flow ("No retry without a new budget decision", spec §15).

## Decision

1. **`AgentPolicy.onExhaustion: "final-answer" | "fail"`, default `"final-answer"`.** The knob
   selects the resolution for `maxTurns` and `maxToolCalls` exhaustion only. Duration, token,
   cost, repeated-failure, and hierarchical budget-hook bounds remain unconditional hard rails,
   so a soft landing can never loop or spend unboundedly.

2. **Over-budget batches settle synthetically (RUN-018).** At the Turn seam, an over-budget
   declared batch never executes a handler and is never durably declared: the engine settles
   every open application call in the trace with the encoded `AgentPolicyError` as a
   model-visible failed result and emits `ToolCallFailed` without `ToolCallStarted` (the
   approval-denied precedent). The trace then flows through the ordinary
   `toolBatchContinuation`, so the tool message, history advancement, steering seam, and — under
   a durable coordinator — the single-batch canonical Turn commit
   (`ModelResponseRecorded` + `ToolCallSettled` per call, no `ToolCallPrepared`) are all reused
   unchanged. `commitResponse` is deliberately skipped for the rejected Turn so recovery replays
   it like any no-tool Turn and a resumed Attempt cannot re-execute rejected work.

3. **Synthetic settlements are exempt from repeated-failure folding.** No handler ran; a
   rejected batch of N calls must not trip `repeatedFailureLimit`. The trace entries carry a
   `budgetRejected` flag that `turnToolFailures` skips (neither advancing nor resetting the
   counter).

4. **Exhausted Turns forbid tool use (RUN-018/RUN-019/RUN-020).** Once exhaustion is derivable
   from committed state (`turn > maxTurns` or declared-plus-programmatic calls exceeding
   `maxToolCalls`), every subsequent model request carries Effect AI `toolChoice: "none"`. Turn
   exhaustion admits exactly one grace Turn past `maxTurns` (`turn > maxTurns` can only be
   `maxTurns + 1`). A model that declares a Tool Call under the constraint fails the Run typed
   (`ModelProtocolError`) — fail-closed, no rejection loops. Both derivations are pure functions
   of replayed state; no new counters or records exist.

5. **Honest settlement (RUN-011).** A Run that settles under the final-answer constraint
   completes with `finishReason: "budget-exhausted"` — never `"model-stop"` — on the live
   `RunCompleted` event, the reduced `AgentResult`, and, durably, on the `SubmissionSettled`
   record via an additive `optionalKey` field written only for this case (absent for every
   ordinary settlement, keeping existing histories and committed goldens byte-stable at
   `schemaVersion: 1`). The `SettlementOutcome` union is unchanged: a soft-landed Run _is_
   `completed`.

6. **S2 (assigned): delegation containment.** `Subagent.define` gains
   `failureMode?: "error" | "return"` (default `"error"`). Under `"return"`, the declared child
   failure and the framework failure family become model-visible failed results; the
   engine-signal members `ToolCallWaiting` and `SubagentDurabilityError` always stay in the
   error channel so durable suspension semantics survive. This promotes the pr-review shadow-Tool
   workaround (FRICTION #7) to a first-party option sanctioned by spec §4.2.

7. **S3 (assigned): budget extension by fresh re-delegation.** `RunOptions` gains a
   tightening-only per-run Tool Call allowance (`min(policy bound, allowance)`), and
   `Subagent.define` gains an opt-in per-invocation allowance parameter clamped fail-closed to a
   configured ceiling at or below the child Definition's policy. A child that soft-lands returns
   its partial output with the exhausted marker; the orchestrator grants more budget by invoking
   the delegation again with a raised allowance and the partial findings passed forward through
   `prepareInput`. The reservation ledger sees an ordinary new invocation; conservation
   accounting, `SubagentReservationConflict` semantics, and the D-013 ceiling chain are
   untouched.

## Consequences

- The runtime specification's Stop Policy paragraph now separates the bound from its resolution;
  RUN-011 names the honest finish reason, and RUN-018/RUN-019/RUN-020 pin the soft-landing
  semantics. ADR-0017's Turn-boundary-accounting consequence carries a dated amendment note.
- `RunCompleted.finishReason` (core events), the engine `AgentResult`, `SubagentCompleted`, and
  the durable `SubmissionSettled` record widen additively; no schema version changes.
- Existing consumers inherit the `"final-answer"` default on upgrade: Turn/Tool-Call budget
  deaths become honest completions. Strict pipelines opt back into fatality with
  `onExhaustion: "fail"` (the pr-review package pins `"fail"` until its S2/S3 rework).
- The durable batch-resume seam keeps its per-Attempt Tool Call accounting; the runtime spec now
  documents that asymmetry explicitly. Cumulative cross-Attempt accounting would need persisted
  counters and stays out of scope.
- The grace Turn means `RunCompleted.turns` may exceed `maxTurns` by exactly one.

## Rejected alternatives

- **Keep run-fatal exhaustion and rely on prompt discipline.** The model cannot observe the
  counter, so prompting shrinks the average but cannot bound the tail; production evidence
  (kommunikasie, pr-review fan-out) shows the cliff keeps detonating finished work.
- **Execute the leading portion of an over-budget batch.** Partial batch execution breaks Tool
  Batch atomicity (RUN-005: the next model request sees a complete batch or none) and makes the
  cut point provider-order-dependent.
- **Fail-only default with opt-in soft landing.** Rejected by owner decision: the framework's
  product stance is that an agent should deliver what it has rather than die; strict pipelines
  are the exception and take the explicit `"fail"` pin.
- **Mid-flight reservation top-up.** A same-key reservation with a larger allocation is a
  conservation violation by design (`SubagentReservationConflict`); reopening §7's equations for
  in-place growth buys nothing over fresh re-delegation, which already conserves and audits.
- **Child Conversation reuse for the extension.** Rejected scope in ADR-0010 and spec §17
  ("follow-up submissions to an existing child Conversation" is its own future proposal);
  fresh re-delegation with forwarded partial findings needs no durable machinery.
- **Widening `SettlementOutcome` with an `exhausted` member.** A soft-landed Run is a completed
  Run; a fourth outcome would fracture every settlement consumer for what is an annotation, not
  an outcome.

## Validation

- Engine suite (`packages/engine/test/agent-runtime.test.ts`): RUN-018 synthetic settlement
  (zero handler starts, `ToolCallFailed` without `ToolCallStarted`, `toolChoice: "none"` on the
  following request, terminal `budget-exhausted`), repeated-failure exemption, exact-cap
  behavior preservation, resume-path rejection with recorded results kept verbatim, RUN-019
  single grace Turn and its absence of a second, RUN-020 fail-closed declaration; the prior
  fatality tests re-pinned under `onExhaustion: "fail"`.
- Broker suite keeps RUN-017 Turn-seam fatality under an explicit `"fail"` pin.
- Durable suite (`packages/testing/test/durable-runtime.test.ts`): an uncertain-class
  over-budget batch settles as one single-batch canonical Turn with no `ToolCallPrepared`, the
  Submission settles `completed` with the durable `finishReason`, and the canonical journal
  alone rebuilds the model-visible prompt including the rejected batch.
- The committed Travel Planner goldens are unchanged (DN and DC suites re-verify), proving the
  additive schema surface leaves non-exhausting histories byte-stable.

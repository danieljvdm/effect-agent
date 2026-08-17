---
title: Certify storage adapters
description: Run the three-tier certification suite against a candidate SubmissionLedger and ConversationStore pair.
---

# Certify storage adapters

A storage adapter cannot be called compatible because it type-checks. Certification is one entry
point that runs three tiers against a candidate
`SubmissionLedger`/`ConversationStore` Layer pair and produces one Schema-encoded certificate:

| Tier                            | Claim                                                                                    | How it is discharged                                                                                                                                                                                                                                                                                                          |
| ------------------------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1 — port contract**           | Both ports honor every documented contract case                                          | The two shared conformance case arrays (`submissionLedgerConformanceCases` and `conversationStoreConformanceCases`), run verbatim                                                                                                                                                                                             |
| **2 — coordinator convergence** | Reached coordinator boundaries converge over this candidate adapter pair in this harness | Every literal in `DurableRuntimeFailpointLocation` is armed one-shot across the scenario shapes declared by `CERTIFICATION_SCENARIOS`; a row is fault evidence only when the failpoint fires and the re-drive converges to `verifyConversationInvariants` with every Submission settled and the digest chain fully recomputed |
| **3 — real loss**               | The adapter survives ACTUAL loss (process kill, eviction), not only in-process faults    | An adapter-supplied crash lever executed in this run, or citations of committed real-loss suites — recorded honestly, never silently claimed                                                                                                                                                                                  |

## Run the certification

```ts
import { Effect } from "effect";
import { certifyDurableAdapters } from "@effect-agent/testing";

const certificate = Effect.gen(function* () {
  return yield* certifyDurableAdapters({
    adapter: { name: "@your-org/storage-yours" },
    submissionLedger: yourLedgerLayer,
    conversationStore: yourStoreLayer,
    tierThreeEvidence: ["path/to/your/real-loss.test.ts"],
  });
});
```

Requirements on the calling environment:

- **`Crypto.Crypto`** must be provided (`NodeCrypto.layer` on Node, `BrowserCrypto.layer` in
  workerd) — the invariant checker recomputes real digests.
- **A TestClock** must be active (`@effect/vitest`'s `it.effect`, or a manual
  `TestClock.layer()` root in workerd). Tier 1 drives lease expiry through virtual time, and
  every Tier-2 re-drive round advances the clock past the ownership lease — the
  adapter-neutral reclaim lever after a mid-Attempt fault. If your ledger is configured with a
  non-default lease, pass `ownershipLeaseDuration`.
- **One connection root for both ports** when your adapters must share one (the session
  "same file" rule): pass the SAME combined Layer instance for both fields — Layer
  memoization builds it once. All three shipped runners do this for their SQL adapters.

The result is a `CertificationReport` (defined in `@effect-agent/session`): adapter identity
with the ledger's own `durability` claim, per-case Tier-1 results, per-cell Tier-2 sweep rows,
and the Tier-3 record. `ok` is true exactly when every **executed** check passed; honest scope
statements (`not-triggered`, `recorded-evidence`, `not-exercised`, `not-applicable`) are never
silent passes and never failures.

## What Tier 2 asserts, exactly

Each cell in the Cartesian product of `CERTIFICATION_SCENARIOS` and
`DurableRuntimeFailpointLocation.literals` arms ONE coordinator failpoint one-shot, submits
fresh work on its own Conversation lane, drives the lane(s) with
`processConversationResolved`, and then re-drives to convergence using only public levers:
`runRecovery`, worker re-drives, and the authorized unblocking operations chosen from
`explainConversation` evidence (`resolveUnknown` with `SafeToRetry`, `resolveApproval` with
`approved`). A cell reports:

- `converged` — the fault fired and the lane(s) settled with verified invariants;
- `not-triggered` — the armed location is not on that scenario's coordinator path (the clean
  run still settled and verified); this is recorded scope, not fault coverage;
- `failed` — anything else, with a bounded detail.

The three committed certificates currently record the same reached matrix. The “not triggered”
count is part of the result, not a pass over that boundary:

| Scenario         | Reached (`converged` with `failpointFired: true`)                                                                                                                                                                                                                                                                                                                                                                          | Not triggered |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------: |
| `plain`          | `submit:after-admit`, `submit:after-materialize`, `claim:after-claim`, `input:after-canonical-append`, `turn:after-canonical-append`, `terminalize:after-reserve`, `terminalize:after-canonical-append`                                                                                                                                                                                                                    |            23 |
| `uncertain-tool` | the `plain` locations, plus `turn:after-response-append`, `turn:after-results-append`, `tools:after-prepared-append`                                                                                                                                                                                                                                                                                                       |            20 |
| `durable-steps`  | the `uncertain-tool` locations, plus `step:after-step-append`                                                                                                                                                                                                                                                                                                                                                              |            19 |
| `approval`       | the `uncertain-tool` locations, plus `approval:after-request-append`, `approval:after-suspend`                                                                                                                                                                                                                                                                                                                             |            18 |
| `join`           | the `plain` locations, plus `join:after-claim`, `join:after-canonical-append`                                                                                                                                                                                                                                                                                                                                              |            21 |
| `delegation`     | the `plain` locations; `turn:after-response-append`; `tools:after-prepared-append`; and `subagent:after-reserve`, `subagent:after-request-append`, `subagent:after-admit`, `subagent:after-child-ready`, `subagent:after-start-append`, `subagent:after-child-attach`, `subagent:after-sibling-settle`, `subagent:after-suspend`, `subagent:after-join-append`, `subagent:after-release-pending`, `subagent:after-release` |            10 |

That is 69 reached cells and 111 `not-triggered` cells out of 180 armed cells. A location may be
reached in one scenario and not triggered in another. A candidate certificate therefore proves
only its rows with `status: "converged"` and `failpointFired: true`; it does not turn an armed but
unreached row into generic durable-boundary coverage.

The invariant verification is the same shared checker the admin `verify` operation uses —
with one upgrade: the runner captures each batch's producer identity at append time, so the
`digest-chain` check is FULLY recomputed from `EMPTY_TAIL_DIGEST` instead of reported
`skipped` (the ConversationStore port deliberately does not export producer identity).

The locations that never fire in any row of the current scenario matrix are declared by
`TIER2_UNREACHED_LOCATIONS`: `abort:after-intent`, `compaction:after-canonical-append`,
`resolve:after-intent`, and `subagent:after-child-abort-intent` sit on operator, compaction, or
abort paths the shapes do not take.
The runners assert the observed never-fired set equals exactly this documented list, so scoped
coverage cannot silently change. Dedicated in-process suites and crash matrices exercise those
paths separately; Tier 2 itself makes no fault-convergence claim for these four locations.

## Tier 3 honestly

- `@effect-agent/storage-memory` — `not-applicable`: the reference adapter declares
  `non-durable` state; there is no real loss to exercise.
- `@effect-agent/storage-sqlite` — `recorded-evidence`: the process-kill crash matrix
  (`packages/platform-node/test/crash/crash.test.ts`,
  `crash-subagents.test.ts`) kills real workers over these Layers (TEST-005).
- `@effect-agent/storage-cloudflare` — `recorded-evidence`: the `ctx.abort()` eviction matrix,
  the cross-DO subagent matrix, and the Miniflare restart lane
  (`packages/platform-cloudflare/test/eviction.test.ts`, `subagents-cross-do.test.ts`,
  `restart/travel-planner-restart.test.ts`).
- A third-party adapter passes `crashLever` (an Effect that kills/evicts/reopens around a
  designated row subset and reports per-row results) to earn `exercised` — or its certificate
  says `not-exercised`, visibly.

## The shipped certificates

The repository certifies its own three adapter pairs and commits the resulting JSON certificates
under `docs/certification/`. `storage-memory` reports `non-durable`. `storage-sqlite` reports the
adapter capability `durable-node`; its certificate plus the cited process-kill host suites are
evidence for the repository's tested `DN` Node/SQLite assembly. `storage-cloudflare` reports
`durable-cloudflare`; its workerd certificate plus the cited eviction/restart suites are evidence
for the repository's tested `DC` SQLite-backed Durable Object assembly. An adapter capability or
Tier-2 certificate alone does not confer a generic `DN` or `DC` deployment-class claim on another
host assembly.

Reports are deterministic (scripted model, virtual clock, fixed lane names), so a diff in a
regenerated certificate is a real behavior change. Use those runners as worked examples when
certifying your own adapter, then satisfy the named deployment-class requirements and real-loss
evidence for the exact assembly you ship.

---
title: Certify storage adapters
description: Run the three-tier certification suite against a candidate SubmissionLedger and ConversationStore pair.
---

# Certify storage adapters

A storage adapter cannot be called compatible because it type-checks. Certification is one entry
point that runs three tiers against a candidate
`SubmissionLedger`/`ConversationStore` Layer pair and produces one Schema-encoded certificate:

| Tier                           | Claim                                                                                              | How it is discharged                                                                                                                                                                                                                                                                                    |
| ------------------------------ | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Port contract**           | Both ports honor every documented contract case                                                    | Run the two shared conformance case arrays verbatim: 32 `submissionLedgerConformanceCases` and 8 `conversationStoreConformanceCases`                                                                                                                                                                    |
| **2. Coordinator convergence** | The durable coordinator over the candidate adapters survives each durable failpoint                | Arm all 28 `DurableRuntimeFailpointLocation` values once across six scenarios: plain, uncertain Tool, Durable Steps, approval, join, and delegation. After each fault, classify the state and converge to `verifyConversationInvariants` with every Submission settled and the digest chain recomputed. |
| **3. Process or runtime loss** | The adapter survives process termination or eviction, beyond failures contained within one process | Execute an adapter-supplied crash mechanism during the run or cite committed real-loss suites. The certificate records which evidence it used.                                                                                                                                                          |

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

- **Provide `Crypto.Crypto`.** Use `NodeCrypto.layer` on Node or `BrowserCrypto.layer` in workerd.
  The invariant checker recomputes real digests.
- **A TestClock** must be active (`@effect/vitest`'s `it.effect`, or a manual
  `TestClock.layer()` root in workerd). Tier 1 drives lease expiry through virtual time, and
  every Tier 2 re-drive round advances the clock past the ownership lease. This lets any adapter
  reclaim ownership after a mid-Attempt fault. If your ledger is configured with a
  non-default lease, pass `ownershipLeaseDuration`.
- **Use one connection root for both ports** when your adapters must share one. Pass the same
  combined Layer instance for both fields so Layer
  memoization builds it once. All three shipped runners do this for their SQL adapters.

The result is a `CertificationReport` (defined in `@effect-agent/session/testing`): adapter identity
with the ledger's own `durability` claim, per-case Tier-1 results, per-cell Tier-2 sweep rows,
and the Tier-3 record. `ok` is true exactly when every executed check passed. Scope
statuses (`not-triggered`, `recorded-evidence`, `not-exercised`, `not-applicable`) are never
silent passes and never failures.

## What Tier 2 asserts, exactly

Each of the 168 sweep cells (6 scenarios × 28 locations) arms one coordinator failpoint
one-shot, submits fresh work on its own Conversation lane, drives the lane(s) with
`processConversationResolved`, and then re-drives to convergence using only public levers:
`runRecovery`, worker re-drives, and the authorized unblocking operations chosen from
`explainConversation` evidence (`resolveUnknown` with `SafeToRetry`, `resolveApproval` with
`approved`). A cell reports:

- `converged` means the fault fired and the lanes settled with verified invariants;
- `not-triggered` means the armed location is not on that scenario's coordinator path. The clean
  run still settled and verified. This records scope, not fault coverage;
- `failed` covers every other outcome and includes bounded detail.

The invariant verification uses the same shared checker as the admin `verify` operation. The
runner also captures each batch's producer identity at append time, so the
`digest-chain` check is recomputed from `EMPTY_TAIL_DIGEST` instead of reported
`skipped`. The ConversationStore port does not export producer identity.

Three locations never fire in these six scenarios: `abort:after-intent`, `resolve:after-intent`,
and `subagent:after-child-abort-intent` sit on operator/abort paths the shapes do not take.
The runners assert the observed never-fired set equals exactly this documented list, so scoped
coverage cannot silently grow; those locations are pinned by dedicated in-process suites and the
crash matrices.

## Tier 3 evidence

- `@effect-agent/storage-memory` reports `not-applicable`. The reference adapter declares
  `non-durable` state; there is no real loss to exercise.
- `@effect-agent/storage-sqlite` reports `recorded-evidence` for the process-kill crash matrix in
  `packages/platform-node/test/crash/crash.test.ts` and `crash-subagents.test.ts`. The tests kill
  real workers over these Layers under TEST-005.
- `@effect-agent/storage-cloudflare` reports `recorded-evidence` for the `ctx.abort()` eviction matrix,
  the cross-DO subagent matrix, and the Miniflare restart lane
  (`packages/platform-cloudflare/test/eviction.test.ts`, `subagents-cross-do.test.ts`,
  `restart/travel-planner-restart.test.ts`).
- A third-party adapter passes `crashLever` (an Effect that kills/evicts/reopens around a
  designated row subset and reports per-row results) to earn `exercised`. Otherwise its
  certificate says `not-exercised`.

## Shipped adapter tests

The repository runs the same certification suite against its memory, SQLite, and Cloudflare
adapters. Set `EFFECT_AGENT_CERTIFICATION_OUT` when running a Node certification test to write a
Schema-encoded report locally. The Cloudflare runner can print its report from workerd by enabling
`PRINT_REPORT` in its test file.

Import `CertificationReport`, `certifyPorts`, and the shared conformance case arrays from
`@effect-agent/session/testing`. The `@effect-agent/session` root contains only production
schemas, ports, invariant verification, replay, and runtime APIs.

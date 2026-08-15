---
title: Certify storage adapters
description: Run the three-tier certification suite against a candidate SubmissionLedger and ConversationStore pair.
---

# Certify storage adapters

<StatusCallout status="available" phase="P7" title="The certification entry point, the three shipped adapter certificates, and this guide exist today." />

A storage adapter cannot be called compatible because it type-checks (TEST-004, STORE-010).
Certification is one entry point that runs three tiers against a candidate
`SubmissionLedger`/`ConversationStore` Layer pair and produces one Schema-encoded certificate:

| Tier                            | Claim                                                                                 | How it is discharged                                                                                                                                                                                                                                                                                                                       |
| ------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **1 — port contract**           | Both ports honor every documented contract case                                       | The two shared conformance case arrays (`submissionLedgerConformanceCases`, 32 cases; `conversationStoreConformanceCases`, 8 cases), run verbatim                                                                                                                                                                                          |
| **2 — coordinator convergence** | The durable coordinator over YOUR adapters survives a fault at every durable boundary | Every `DurableRuntimeFailpointLocation` (28) is armed one-shot across six scenario shapes (plain, uncertain-tool, durable-steps, approval, join, delegation); after the fault the state must classify and the re-drive must converge to `verifyConversationInvariants` with every Submission settled and the digest chain fully recomputed |
| **3 — real loss**               | The adapter survives ACTUAL loss (process kill, eviction), not only in-process faults | An adapter-supplied crash lever executed in this run, or citations of committed real-loss suites — recorded honestly, never silently claimed                                                                                                                                                                                               |

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
  every Tier-2 re-drive round advances the clock past the D5 ownership lease — the
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

Each of the 168 sweep cells (6 scenarios × 28 locations) arms ONE coordinator failpoint
one-shot, submits fresh work on its own Conversation lane, drives the lane(s) with
`processConversationResolved`, and then re-drives to convergence using only public levers:
`runRecovery`, worker re-drives, and the authorized unblocking operations chosen from
`explainConversation` evidence (`resolveUnknown` with `SafeToRetry`, `resolveApproval` with
`approved`). A cell reports:

- `converged` — the fault fired and the lane(s) settled with verified invariants;
- `not-triggered` — the armed location is not on that scenario's coordinator path (the clean
  run still settled and verified); this is recorded scope, not fault coverage;
- `failed` — anything else, with a bounded detail.

The invariant verification is the same shared checker the admin `verify` operation uses —
with one upgrade: the runner captures each batch's producer identity at append time, so the
`digest-chain` check is FULLY recomputed from `EMPTY_TAIL_DIGEST` instead of reported
`skipped` (the ConversationStore port deliberately does not export producer identity).

Three locations never fire in these six shapes — `abort:after-intent`, `resolve:after-intent`,
and `subagent:after-child-abort-intent` sit on operator/abort paths the shapes do not take.
The runners assert the observed never-fired set equals exactly this documented list
(`TIER2_UNREACHED_LOCATIONS`), so scoped coverage cannot silently grow; those locations are
pinned by the P5/S2 in-process suites and the crash matrices.

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

## The shipped runners and committed certificates

| Adapter                             | Runner                                                   | Certificate                                  |
| ----------------------------------- | -------------------------------------------------------- | -------------------------------------------- |
| storage-memory (reference)          | `packages/testing/test/certification-memory.test.ts`     | `docs/certification/storage-memory.json`     |
| storage-sqlite (DN)                 | `packages/testing/test/certification-sqlite.test.ts`     | `docs/certification/storage-sqlite.json`     |
| storage-cloudflare (DC, in-workerd) | `packages/storage-cloudflare/test/certification.test.ts` | `docs/certification/storage-cloudflare.json` |

The memory and SQLite runners live in `packages/testing` (not beside their adapters) because
vp's task graph rejects the dev-edge cycle `storage-* → testing → storage-*` — the same
constraint that placed `durable-runtime.test.ts` there. The Cloudflare runner runs the
identical entry point inside workerd against a real SQLite-backed Durable Object (STORE-013).

The committed JSON certificates are evidence artifacts; the runner tests carry the semantic
assertions (testing.md §12 — golden data never replaces assertions). To regenerate after an
intentional protocol or adapter change:

```sh
# Node adapters: writes docs/certification/storage-{memory,sqlite}.json
EFFECT_AGENT_CERTIFICATION_OUT="$PWD/docs/certification" \
  bunx vitest run packages/testing/test/certification-memory.test.ts \
                  packages/testing/test/certification-sqlite.test.ts

# Cloudflare: workerd cannot write files — flip PRINT_REPORT to true in
# packages/storage-cloudflare/test/certification.test.ts, run with
# `bunx vitest run --disable-console-intercept test/certification.test.ts`,
# copy the logged JSON to docs/certification/storage-cloudflare.json, flip it back.
```

Reports are deterministic (scripted model, virtual clock, fixed lane names), so a diff in a
regenerated certificate is a real behavior change.

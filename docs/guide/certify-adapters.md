---
title: Certify storage adapters
description: Certify a SubmissionLedger and ThreadStore pair at three failure levels.
---

# Certify storage adapters

`certifyDurableAdapters` tests a candidate `SubmissionLedger` and `ThreadStore` pair and
returns one schema-encoded report.

| Level                      | Evidence                                                                           |
| -------------------------- | ---------------------------------------------------------------------------------- |
| 1. Port contract           | Runs all shared ledger and thread store conformance cases                          |
| 2. Coordinator convergence | Injects each coordinator failpoint into six durable scenarios and drives recovery  |
| 3. Runtime loss            | Exercises process termination or eviction, or records the committed suites that do |

## Implement the store contract {#store-contract}

A `ThreadStore` materializes a thread, appends fenced batches, reads or observes
records, exports history, and inspects the tail. Appends must be atomic, digest-bound, idempotent by
batch ID, checked against the expected tail, and fenced by producer epoch. Reads decode stored
values through schemas.

Checkpoint support is optional and used only by explicit projection consumers. Retained history
and durable recovery do not read it. An adapter with checkpoints must also run the checkpoint
conformance suite.

Implement `SubmissionLedger.readAbortIntent` as a strongly consistent read of one submission's
abort intent. The runtime polls this method during execution, so its work must stay independent
of other submissions, approvals, and attached children. Unknown submissions fail with `LedgerError`.
The read grants no ownership, and `canonicalRecordId` must come from canonical history.

```ts
import type { SubmissionId } from "@effect-agent/core/Identifiers";
import { AbortIntentRequest, SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import { Effect } from "effect";

const readAbort = Effect.fn(function* (submissionId: SubmissionId) {
  const ledger = yield* SubmissionLedger;
  return yield* ledger.readAbortIntent(AbortIntentRequest.make({ submissionId }));
});
```

Cloudflare keeps this read local to the submission's owning Durable Object. The abort command
still becomes canonical under the append gate before the runtime interrupts execution.

## Run the certification

```ts
import { Effect } from "effect";
import { certifyDurableAdapters } from "@effect-agent/testing/Certification";

const certificate = Effect.gen(function* () {
  return yield* certifyDurableAdapters({
    adapter: { name: "@your-org/storage-yours" },
    submissionLedger: yourLedgerLayer,
    threadStore: yourStoreLayer,
    tierThreeEvidence: ["path/to/your/real-loss.test.ts"],
  });
});
```

Provide `Crypto.Crypto` with `NodeCrypto.layer` on Node or `BrowserCrypto.layer` in workerd. Run
with a `TestClock` through `@effect/vitest` or a manual `TestClock.layer()` root. Tier 1 advances
leases, and tier 2 advances past the ownership lease during recovery. Pass
`ownershipLeaseDuration` when the ledger uses a different lease.

If both ports share a connection, pass the same combined layer instance to both fields. Layer
memoization will acquire it once.

The returned `CertificationReport` includes adapter identity, the ledger durability claim, each
tier 1 result, each tier 2 cell, and tier 3 evidence. `ok` is true only when every executed check
passes. Statuses such as `not-triggered`, `recorded-evidence`, `not-exercised`, and
`not-applicable` describe scope. They count as neither a pass nor a failure.

Use `fullyCertified` when a gate requires complete durable-adapter certification in this run.
It requires `ok: true`, a durable adapter, and at least one passing real-loss case from `crashLever`.
All lever cases must belong to the `real-loss` suite. An empty lever reports `not-exercised`.

| Tier 3 status                              | `ok` when executed checks pass | `fullyCertified` |
| ------------------------------------------ | ------------------------------ | ---------------- |
| `exercised` with passing real-loss cases   | `true`                         | `true`           |
| `recorded-evidence`                        | `true`                         | `false`          |
| `not-exercised`                            | `true`                         | `false`          |
| `not-applicable` for a non-durable adapter | `true`                         | `false`          |

Any failed executed check makes both fields `false`. Recorded citations are external evidence;
the runner does not execute or verify those suites. Non-durable adapters can pass conformance
without earning durable certification. Reports use the `effect-agent/certification@2` format.

## Interpret tier 2 results {#what-tier-2-asserts-exactly}

Tier 2 arms every coordinator failpoint across six scenarios. Each cell uses fresh
thread state, injects one failpoint, and drives recovery through public operations. The
runner resolves unknown outcomes as `SafeToRetry` and approvals as `approved` only when
`explainThread` authorizes that action.

Each cell reports:

- `converged` when the failpoint fired and recovery settled with verified invariants;
- `not-triggered` when the scenario never reached that location and the clean run still verified;
- `failed` for any other result, with bounded diagnostic detail.

`not-triggered` records the tested scope. It makes no fault-survival claim. The runner also checks
the expected set of locations that these six scenarios never reach. Dedicated suites and crash
matrices cover those operator and abort paths.

The final invariant check recomputes the digest chain from `EMPTY_TAIL_DIGEST` and uses the same
checker as the administrative `verify` operation.

## Provide tier 3 evidence {#tier-3-evidence}

- `@effect-agent/storage-memory` reports `not-applicable` because it declares non-durable state.
- `@effect-agent/storage-sqlite` records the platform Node process-kill suites.
- `@effect-agent/storage-cloudflare` records Durable Object eviction, cross-object subagent, and
  Miniflare restart suites.
- A third-party adapter may pass `crashLever` to kill or evict its runtime and reopen storage for
  selected rows. Successful rows report `exercised`. Without that lever or committed evidence, the
  report says `not-exercised`.

<a id="shipped-adapter-tests"></a>

Import `CertificationReport` and `certifyPorts` from
`@effect-agent/thread/testing/Certification`. The shared conformance cases live in
`@effect-agent/thread/testing/ThreadStoreConformance` and
`@effect-agent/thread/testing/SubmissionLedgerConformance`. Production schemas, ports,
replay, verification, and runtime APIs have their own public thread modules.

## Certify subscription stores {#subscription-stores}

An adapter that implements `SubscriptionStore` must also run
`subscriptionStoreConformanceCases` from `@effect-agent/thread/testing/SubscriptionStoreConformance`. Give each case a fresh
partition. The cases cover intake cutoffs, deduplication, once selection, capacity, cancellation,
prepared recovery, catch-up, scan cursors, and replay after limits tighten.

The thread and submission certificate does not include these cases. Add restart or eviction
tests around intake, partial fanout, selection, preparation, admission, and receipt persistence.

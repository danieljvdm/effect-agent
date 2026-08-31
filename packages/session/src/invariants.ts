import type { Crypto } from "effect";
import { Effect, Schema } from "effect";

import { IntegrityCheck, IntegrityReport, type IntegrityCheckName } from "./admin.ts";
import { digestCanonicalBatch, EMPTY_TAIL_DIGEST } from "./digest.ts";
import {
  submissionInputRecordId,
  submissionSettlementRecordId,
  type SubmissionSnapshot,
} from "./ledger.ts";
import {
  CanonicalBatch,
  CanonicalRecordEnvelope,
  type BatchId,
  type ProducerId,
  type RecordEnvelope,
} from "./records.ts";
import type { ConversationCheckpoint, ConversationExport } from "./store.ts";

/**
 * The production conversation invariant checker, shared by the durable coordinator, admin
 * verification, and development harnesses. Keeping it independent of certification and
 * conformance modules prevents ordinary runtime imports from loading test-only dependencies.
 *
 * `verifyConversationInvariants` — hoisted from the DC eviction harness's `assertConvergence`
 * so the admin `verify` operation, the adapter certification tiers (WP2), and the chaos/soak
 * properties (WP4) all assert one set of claims.
 */

/**
 * Input of one invariant verification, assembled entirely from port reads (or from a captured
 * copy under test):
 *
 * - `export` — the Conversation's canonical export (`ConversationStore.export`).
 * - `submissions` — every known Submission row of the lane (ledger scan + lookups). Order is
 *   irrelevant; the checker sorts by `queueSequence`.
 * - `batchProducers` — OPTIONAL per-batch producer identity directory. The digest chain hashes
 *   each batch's Schema-encoded `CanonicalBatch` — including its `producerId`, which the
 *   `ConversationStore` port deliberately does not export — so full chain recomputation needs
 *   this directory (capture it at append time, or from adapter-private storage). Without it the
 *   `digest-chain` and `checkpoint-binding` checks report `skipped` with the honest reason;
 *   adapter-level `verifyOnOpen` remains the storage-side full audit.
 * - `checkpoint` — the stored checkpoint to bind against the recomputed chain, when one exists.
 * - `checkpointsSupported` — whether the caller inspected a supporting adapter. Without this
 *   evidence or a supplied checkpoint, checkpoint verification is skipped.
 * - `requireAllSettled` — convergence mode (certification Tier 2, chaos, soak): additionally
 *   require every known Submission to be settled.
 */
export interface ConversationInvariantInput {
  readonly export: ConversationExport;
  readonly submissions: ReadonlyArray<SubmissionSnapshot>;
  readonly batchProducers?: ReadonlyMap<BatchId, ProducerId> | undefined;
  readonly checkpoint?: ConversationCheckpoint | undefined;
  /** Set only after checking the adapter's optional checkpoint capability. */
  readonly checkpointsSupported?: boolean | undefined;
  readonly requireAllSettled?: boolean | undefined;
}

const encodeEnvelope = Schema.encodeEffect(CanonicalRecordEnvelope);
const decodeEnvelope = Schema.decodeUnknownEffect(CanonicalRecordEnvelope);

const check = (
  name: IntegrityCheckName,
  status: "passed" | "failed" | "skipped",
  detail?: string,
): IntegrityCheck =>
  IntegrityCheck.make({
    name,
    status,
    ...(detail === undefined ? {} : { detail: detail.slice(0, 4_096) }),
  });

interface BatchRun {
  readonly batchId: BatchId;
  readonly records: Array<RecordEnvelope>;
  readonly lastSequence: number;
}

/** Group the export's records into their atomic append runs (batches append contiguously). */
const batchRunsOf = (records: ReadonlyArray<CanonicalRecordEnvelope>): Array<BatchRun> => {
  const runs: Array<BatchRun> = [];
  for (const envelope of records) {
    const current = runs.at(-1);
    if (current !== undefined && current.batchId === envelope.batchId) {
      current.records.push(envelope.record);
      runs[runs.length - 1] = { ...current, lastSequence: envelope.sequence };
      continue;
    }
    runs.push({
      batchId: envelope.batchId,
      records: [envelope.record],
      lastSequence: envelope.sequence,
    });
  }
  return runs;
};

/**
 * The shared conversation invariant checker (plan §1/§3): the DC harness `assertConvergence`
 * claims, verbatim, as typed per-check results over a canonical export plus the lane's ledger
 * rows — read-only, never a repair:
 *
 * 1. `schema-round-trip` — every envelope re-encodes and re-decodes through its canonical
 *    Schema (a corrupted copy fails typed instead of decoding incorrectly).
 * 2. `record-identity` — no canonical record identity appears twice (DUR-007 dedupe held).
 * 3. `sequence-contiguity` — gap-free sequences from 1 through the exported tail.
 * 4. `digest-chain` — the tail digest recomputes from `EMPTY_TAIL_DIGEST` through every batch
 *    (requires the per-batch producer directory; skipped honestly otherwise).
 * 5. `fifo-input-order` / `fifo-settlement-order` — canonical input and settlement records
 *    follow the admitted queue order (DUR-004). Aborted settlements of never-run work (no
 *    canonical `input:{sid}` record) are exempt from the settlement comparison: P7 §7(c)
 *    settles them immediately without waiting for the head, and DUR-004 bounds execution
 *    order, which never-run work has none of.
 * 6. `terminal-uniqueness` — at most one canonical terminal record per Submission, exactly one
 *    for every settled ledger row (DUR-002).
 * 7. `ledger-canonical-agreement` — a settled ledger row has a canonical settlement record and
 *    the outcomes agree; canonical history remains the authority (DUR-015).
 * 8. `checkpoint-binding` — a stored checkpoint's digest matches the recomputed chain at its
 *    sequence (needs the chain; bounds are checked regardless).
 * 9. `all-settled` — convergence mode only: every known Submission settled.
 *
 * Requires only `Crypto.Crypto` (digest recomputation); it performs no port access, so the
 * admin operation, certification runners, and chaos properties feed it whatever copies they
 * hold.
 */
export const verifyConversationInvariants = Effect.fn("Session.verifyConversationInvariants")(
  function* (
    input: ConversationInvariantInput,
  ): Effect.fn.Return<IntegrityReport, never, Crypto.Crypto> {
    const exported = input.export;
    const records = exported.records;
    const checks: Array<IntegrityCheck> = [];

    // 1. schema-round-trip
    {
      let failure: string | undefined;
      for (const envelope of records) {
        const roundTrip = yield* encodeEnvelope(envelope).pipe(
          Effect.flatMap(decodeEnvelope),
          Effect.exit,
        );
        if (roundTrip._tag === "Failure") {
          failure = `record ${envelope.record.recordId} does not round-trip its Schema`;
          break;
        }
      }
      checks.push(
        failure === undefined
          ? check("schema-round-trip", "passed")
          : check("schema-round-trip", "failed", failure),
      );
    }

    // 2. record-identity
    {
      const seen = new Set<string>();
      const duplicates = new Set<string>();
      for (const envelope of records) {
        if (seen.has(envelope.record.recordId)) duplicates.add(envelope.record.recordId);
        seen.add(envelope.record.recordId);
      }
      checks.push(
        duplicates.size === 0
          ? check("record-identity", "passed")
          : check(
              "record-identity",
              "failed",
              `duplicated canonical record identities: ${[...duplicates].join(", ")}`,
            ),
      );
    }

    // 3. sequence-contiguity
    {
      let contiguityFailure: string | undefined;
      for (const [index, envelope] of records.entries()) {
        if (envelope.sequence !== index + 1) {
          contiguityFailure = `sequence ${envelope.sequence} at position ${index} (expected ${index + 1})`;
          break;
        }
      }
      if (contiguityFailure === undefined && records.length !== exported.tailSequence) {
        contiguityFailure = `exported tail sequence ${exported.tailSequence} disagrees with ${records.length} records`;
      }
      checks.push(
        contiguityFailure === undefined
          ? check("sequence-contiguity", "passed")
          : check("sequence-contiguity", "failed", contiguityFailure),
      );
    }

    // 4. digest-chain (+ collect per-sequence chain digests for checkpoint binding)
    const chainDigestAtSequence = new Map<number, string>();
    if (input.batchProducers === undefined) {
      checks.push(
        check(
          "digest-chain",
          "skipped",
          "the ConversationStore port does not export per-batch producer identity; supply batchProducers (captured at append time) to recompute the chain — adapter-level verifyOnOpen performs the storage-side audit",
        ),
      );
    } else {
      let chain = EMPTY_TAIL_DIGEST;
      let chainFailure: string | undefined;
      for (const run of batchRunsOf(records)) {
        const producerId = input.batchProducers.get(run.batchId);
        if (producerId === undefined) {
          chainFailure = `no producer identity supplied for batch ${run.batchId}`;
          break;
        }
        const [first, ...rest] = run.records;
        if (first === undefined) {
          chainFailure = `batch ${run.batchId} grouped zero records`;
          break;
        }
        const digest = yield* digestCanonicalBatch(
          chain,
          CanonicalBatch.make({ batchId: run.batchId, producerId, records: [first, ...rest] }),
        ).pipe(Effect.exit);
        if (digest._tag === "Failure") {
          chainFailure = `batch ${run.batchId} could not be re-digested`;
          break;
        }
        chain = digest.value;
        chainDigestAtSequence.set(run.lastSequence, chain);
      }
      if (chainFailure === undefined && chain !== exported.tailDigest) {
        chainFailure = `recomputed tail digest ${chain} disagrees with exported tail digest ${exported.tailDigest}`;
      }
      checks.push(
        chainFailure === undefined
          ? check("digest-chain", "passed")
          : check("digest-chain", "failed", chainFailure),
      );
    }

    // Shared FIFO/terminal machinery
    const ordered = [...input.submissions].sort(
      (left, right) => left.queueSequence - right.queueSequence,
    );
    const recordIds = records.map((envelope) => envelope.record.recordId);

    // 5a. fifo-input-order
    {
      const expectedInputs = ordered.map((row) => submissionInputRecordId(row.submissionId));
      const expectedSet = new Set<string>(expectedInputs);
      const inputOrder = recordIds.filter((recordId) => expectedSet.has(recordId));
      const expectedPresent = expectedInputs.filter((expected) => inputOrder.includes(expected));
      const matches =
        inputOrder.length === expectedPresent.length &&
        inputOrder.every((recordId, index) => recordId === expectedPresent[index]);
      checks.push(
        matches
          ? check("fifo-input-order", "passed")
          : check(
              "fifo-input-order",
              "failed",
              `canonical input order [${inputOrder.join(", ")}] violates queue order`,
            ),
      );
    }

    // 5b. fifo-settlement-order — P7 §7(c) exemption: an ABORTED settlement for never-run work
    // (no canonical `input:{sid}` record) settles immediately by design, without waiting to
    // head the lane, so it is excluded from the FIFO comparison. DUR-004 bounds EXECUTION
    // order; the exempted rows provably never executed.
    {
      const recordIdSet = new Set<string>(recordIds);
      const abortedNeverRun = new Set<string>();
      for (const envelope of records) {
        const payload = envelope.record.payload;
        if (
          payload._tag === "SubmissionSettled" &&
          payload.outcome === "aborted" &&
          !recordIdSet.has(submissionInputRecordId(payload.submissionId))
        ) {
          abortedNeverRun.add(payload.submissionId);
        }
      }
      const expectedSettlements = ordered
        .filter((row) => !abortedNeverRun.has(row.submissionId))
        .map((row) => submissionSettlementRecordId(row.submissionId));
      const expectedSet = new Set<string>(expectedSettlements);
      const settlementOrder = recordIds.filter((recordId) => expectedSet.has(recordId));
      const expectedPresent = expectedSettlements.filter((expected) =>
        settlementOrder.includes(expected),
      );
      const matches =
        settlementOrder.length === expectedPresent.length &&
        settlementOrder.every((recordId, index) => recordId === expectedPresent[index]);
      checks.push(
        matches
          ? check("fifo-settlement-order", "passed")
          : check(
              "fifo-settlement-order",
              "failed",
              `canonical settlement order [${settlementOrder.join(", ")}] violates queue order`,
            ),
      );
    }

    // 6 + 7. terminal-uniqueness and ledger-canonical-agreement
    {
      const settlementsBySubmission = new Map<string, Array<CanonicalRecordEnvelope>>();
      for (const envelope of records) {
        if (envelope.record.payload._tag !== "SubmissionSettled") continue;
        const submissionId = envelope.record.payload.submissionId;
        const existing = settlementsBySubmission.get(submissionId) ?? [];
        existing.push(envelope);
        settlementsBySubmission.set(submissionId, existing);
      }
      let uniquenessFailure: string | undefined;
      let agreementFailure: string | undefined;
      for (const [submissionId, settlements] of settlementsBySubmission) {
        if (settlements.length > 1) {
          uniquenessFailure = `submission ${submissionId} has ${settlements.length} canonical terminal records`;
        }
      }
      for (const row of ordered) {
        const settlements = settlementsBySubmission.get(row.submissionId) ?? [];
        const canonical = settlements[0]?.record.payload;
        if (row.state === "settled") {
          if (settlements.length !== 1) {
            uniquenessFailure ??= `settled submission ${row.submissionId} has ${settlements.length} canonical terminal records (expected exactly 1)`;
          }
          if (
            canonical !== undefined &&
            canonical._tag === "SubmissionSettled" &&
            row.settledOutcome !== undefined &&
            canonical.outcome !== row.settledOutcome
          ) {
            agreementFailure = `submission ${row.submissionId}: ledger outcome ${row.settledOutcome} disagrees with canonical outcome ${canonical.outcome}`;
          }
          if (settlements.length === 0) {
            agreementFailure ??= `settled submission ${row.submissionId} has no canonical settlement record (DUR-015: history is the authority)`;
          }
        }
      }
      checks.push(
        uniquenessFailure === undefined
          ? check("terminal-uniqueness", "passed")
          : check("terminal-uniqueness", "failed", uniquenessFailure),
      );
      checks.push(
        agreementFailure === undefined
          ? check("ledger-canonical-agreement", "passed")
          : check("ledger-canonical-agreement", "failed", agreementFailure),
      );
    }

    // 8. checkpoint-binding
    if (input.checkpoint === undefined && input.checkpointsSupported !== true) {
      checks.push(check("checkpoint-binding", "skipped", "checkpoint support was not supplied"));
    } else if (input.checkpoint === undefined) {
      checks.push(check("checkpoint-binding", "passed", "no stored checkpoint"));
    } else if (input.checkpoint.throughSequence > exported.tailSequence) {
      checks.push(
        check(
          "checkpoint-binding",
          "failed",
          `checkpoint through-sequence ${input.checkpoint.throughSequence} is ahead of the tail ${exported.tailSequence}`,
        ),
      );
    } else if (input.batchProducers === undefined) {
      checks.push(
        check(
          "checkpoint-binding",
          "skipped",
          "digest binding needs the recomputed chain; supply batchProducers (the adapter rejects mismatched checkpoints on save/load regardless)",
        ),
      );
    } else {
      const bound = chainDigestAtSequence.get(input.checkpoint.throughSequence);
      checks.push(
        bound === input.checkpoint.tailDigest
          ? check("checkpoint-binding", "passed")
          : check(
              "checkpoint-binding",
              "failed",
              `checkpoint digest ${input.checkpoint.tailDigest} does not match the recomputed chain at sequence ${input.checkpoint.throughSequence}`,
            ),
      );
    }

    // 9. all-settled (convergence mode)
    if (input.requireAllSettled === true) {
      const nonterminal = ordered.filter((row) => row.state !== "settled");
      checks.push(
        nonterminal.length === 0 && ordered.length > 0
          ? check("all-settled", "passed")
          : check(
              "all-settled",
              "failed",
              ordered.length === 0
                ? "no Submissions are known to the lane"
                : `nonterminal submissions: ${nonterminal
                    .map((row) => `${row.submissionId}(${row.state})`)
                    .join(", ")}`,
            ),
      );
    }

    return IntegrityReport.make({
      conversationId: exported.conversationId,
      tailSequence: exported.tailSequence,
      recordCount: records.length,
      submissionCount: ordered.length,
      checks,
      ok: checks.every((result) => result.status !== "failed"),
    });
  },
);

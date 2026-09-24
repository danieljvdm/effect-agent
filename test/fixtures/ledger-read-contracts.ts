import { Cause, DateTime, Effect, Exit, Option, Schema, Stream } from "effect";
import { digestJson } from "effect-agent/digest";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  SubmissionSettledRecord,
  type SettlementOutcome,
} from "effect-agent/records";
import {
  AbortCommand,
  AdmissionRequest,
  ClaimRequest,
  MarkReadyRequest,
  OwnershipToken,
  SettlementConflict,
  SettlementFinalization,
  SettlementReservation,
  SubmissionLedger,
  SubmissionLookupById,
  submissionSettlementId,
  submissionSettlementRecordId,
  type SubmissionSnapshot,
} from "effect-agent/submission-ledger";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { CurrentTransformer } from "effect/unstable/sql/Statement";
import { expect } from "vite-plus/test";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const producerId = ProducerId.make("ledger-read-test");
const deploymentId = DeploymentId.make("ledger-read-test");

export const admitReadFixture = Effect.fn("LedgerReadFixture.admit")(function* (
  threadId: string,
  key: string,
) {
  const ledger = yield* SubmissionLedger;

  return yield* ledger.admit(
    yield* Schema.decodeEffect(AdmissionRequest)({
      threadId,
      principal: "ledger-read-test",
      idempotencyKey: key,
      agentId: "ledger-read-test",
      agentDigests: definitions,
      deploymentId,
      inputPayload: null,
      inputDigest: digest,
    }),
  );
});

const reserve = Effect.fn("LedgerReadFixture.reserve")(function* (
  submission: Pick<SubmissionSnapshot, "submissionId" | "receiptId">,
  ownershipToken: OwnershipToken,
  outcome: SettlementOutcome,
) {
  const ledger = yield* SubmissionLedger;
  const settlementId = submissionSettlementId(submission.submissionId);

  const payload = yield* Schema.decodeEffect(SubmissionSettledRecord)({
    _tag: "SubmissionSettled",
    submissionId: submission.submissionId,
    settlementId,
    receiptId: submission.receiptId,
    outcome,
    ...(outcome === "failed"
      ? { result: { errorTag: "FixtureFailure", message: "Fixture failed" } }
      : {}),
  });

  const record = RecordEnvelope.make({
    recordId: submissionSettlementRecordId(submission.submissionId),
    family: "thread",
    schemaVersion: 1,
    createdAt: DateTime.makeUnsafe(1),
    deploymentId,
    payload,
  });

  const recordDigest = yield* digestJson(yield* Schema.encodeEffect(RecordEnvelope)(record));

  yield* ledger.reserveSettlement(
    SettlementReservation.make({
      submissionId: submission.submissionId,
      ownershipToken,
      settlementId,
      outcome,
      record,
      recordDigest,
    }),
  );

  return SettlementFinalization.make({ submissionId: submission.submissionId, settlementId });
});

/** Adapter fixture: reservation precedes finalization; runtime canonical append is outside this suite. */
export const reserveReadFixture = Effect.fn("LedgerReadFixture.prepare")(function* (
  key: string,
  outcome: SettlementOutcome = "completed",
) {
  const ledger = yield* SubmissionLedger;
  const admitted = yield* admitReadFixture(key, key);

  yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));

  const claim = yield* ledger.claim(
    ClaimRequest.make({ threadId: AdmissionRequest.fields.threadId.make(key), producerId }),
  );

  if (Option.isNone(claim)) return yield* Effect.die("Fixture lane did not become claimable");

  return yield* reserve(admitted, claim.value.ownershipToken, outcome);
});

const abortQueued = Effect.fn("LedgerReadFixture.abortQueued")(function* (submissionId: string) {
  const ledger = yield* SubmissionLedger;

  const submission = yield* ledger.lookup(
    SubmissionLookupById.make({
      submissionId: SubmissionLookupById.fields.submissionId.make(submissionId),
    }),
  );

  if (Option.isNone(submission)) return yield* Effect.die("Missing scan fixture submission");
  yield* ledger.requestAbort(
    AbortCommand.make({
      submissionId: submission.value.submissionId,
      author: "ledger-read-test",
      reason: "queued abort",
    }),
  );

  return yield* ledger.finalizeSettlement(
    yield* reserve(submission.value, OwnershipToken.make("unused-queued-abort"), "aborted"),
  );
});

/** Synthetic scan rows isolate retained ledger growth; they do not represent a canonical settlement protocol. */
const seedScan = Effect.fn("LedgerReadFixture.seedScan")(function* (count: number) {
  const sql = yield* SqlClient.SqlClient;

  const encodedDefinitions = yield* Schema.encodeEffect(Schema.fromJsonString(DefinitionDigests))(
    definitions,
  );

  yield* sql`
    WITH RECURSIVE positions(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM positions WHERE n + 1 < ${count})
    INSERT INTO effect_agent_submissions (
      submission_id, thread_id, queue_sequence, principal, idempotency_key,
      agent_id, agent_digests_json, deployment_id, input_json, input_digest,
      receipt_id, state, settled_outcome, created_at, ready_at
    )
    SELECT 'scan-' || n, 'scan-lane', n + 1, 'ledger-read-test', 'scan-' || n,
      'ledger-read-test', ${encodedDefinitions}, ${deploymentId}, 'null', ${digest},
      'receipt-scan-' || n,
      'ready',
      NULL,
      '1970-01-01T00:00:00.001Z', '1970-01-01T00:00:00.001Z'
    FROM positions
  `;
});

export const ledgerReadCases = [
  {
    name: "keeps cursor order during settlement and observes earlier admissions on the next scan",
    run: Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;

      yield* seedScan(770);
      let count = 0;
      let earlier: string | undefined;
      let later: string | undefined;

      const result = yield* Stream.runCollect(
        ledger.scanNonterminal.pipe(
          Stream.tap(() =>
            Effect.gen(function* () {
              count++;
              if (count !== 256) return;
              yield* abortQueued("scan-0");
              yield* abortQueued("scan-514");
              earlier = (yield* admitReadFixture("a-before-cursor", "earlier")).submissionId;
              later = (yield* admitReadFixture("z-after-cursor", "later")).submissionId;
            }),
          ),
        ),
      );

      expect(result.map(({ submissionId }) => submissionId)).toEqual([
        ...Array.from({ length: 770 }, (_, n) => `scan-${n}`).filter((id) => id !== "scan-514"),
        later,
      ]);
      const next = yield* Stream.runCollect(ledger.scanNonterminal);

      expect(next.map(({ submissionId }) => submissionId)).toEqual([
        earlier,
        ...Array.from({ length: 770 }, (_, n) => `scan-${n}`).filter(
          (id) => id !== "scan-0" && id !== "scan-514",
        ),
        later,
      ]);
    }),
  },
  ...(["failed"] as const).map((outcome) => ({
    name: `replays ${outcome} settlement with unchanged timestamp and diagnostics`,
    run: Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const request = yield* reserveReadFixture(`settlement-${outcome}`, outcome);
      const active = yield* ledger.finalizeSettlement(request);

      expect(active.outcome).toBe(outcome);
      for (let i = 0; i < 1; i++) {
        const replay = yield* ledger.finalizeSettlement(request);

        expect(replay).toEqual(active);
      }
      expect(active.failure).toEqual(
        outcome === "failed"
          ? { errorTag: "FixtureFailure", message: "Fixture failed" }
          : undefined,
      );

      const conflict = yield* ledger
        .finalizeSettlement(
          SettlementFinalization.make({
            ...request,
            settlementId: SettlementFinalization.fields.settlementId.make("conflicting-settlement"),
          }),
        )
        .pipe(Effect.result);

      expect(conflict).toMatchObject({
        _tag: "Failure",
        failure: SettlementConflict.make({
          submissionId: request.submissionId,
          existingOutcome: outcome,
        }),
      });
    }),
  })),
  ...(["missing"] as const).map((corruption) => ({
    name: `preserves typed ${corruption} failure without repairing settled storage`,
    run: Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const sql = yield* SqlClient.SqlClient;
      const request = yield* reserveReadFixture(`corrupt-${corruption}`);

      yield* ledger.finalizeSettlement(request);
      yield* sql`DELETE FROM effect_agent_settlement_reservations WHERE submission_id=${request.submissionId}`;

      const before =
        yield* sql`SELECT * FROM effect_agent_settlement_reservations WHERE submission_id=${request.submissionId}`;

      const result = yield* ledger.finalizeSettlement(request).pipe(Effect.result);

      expect(result).toMatchObject({ _tag: "Failure", failure: { _tag: "LedgerError" } });

      expect(
        yield* sql`SELECT * FROM effect_agent_settlement_reservations WHERE submission_id=${request.submissionId}`,
      ).toEqual(before);
      expect(
        yield* sql`SELECT state FROM effect_agent_submissions WHERE submission_id=${request.submissionId}`,
      ).toEqual([{ state: "settled" }]);
    }),
  })),
  ...(["interruption"] as const).map((mode) => ({
    name: `releases a ${mode} during the settled read and permits later mutations`,
    run: Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const request = yield* reserveReadFixture(`cleanup-${mode}`);
      const settled = yield* ledger.finalizeSettlement(request);

      const result = yield* ledger.finalizeSettlement(request).pipe(
        Effect.provideService(CurrentTransformer, () => Effect.interrupt),
        Effect.exit,
      );

      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        expect(Cause.hasInterrupts(result.cause)).toBe(true);
      }
      expect(yield* ledger.finalizeSettlement(request)).toEqual(settled);
      yield* admitReadFixture(`after-${mode}`, "after");
    }),
  })),
];

import { digestJson } from "@effect-agent/thread/Digest";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  SubmissionSettledRecord,
  type SettlementOutcome,
} from "@effect-agent/thread/Records";
import {
  AbortCommand,
  AdmissionRequest,
  ClaimRequest,
  LedgerError,
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
} from "@effect-agent/thread/SubmissionLedger";
import { Cause, DateTime, Effect, Exit, Option, Schema, Stream } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { CurrentTransformer, type Statement } from "effect/unstable/sql/Statement";
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
    yield* Schema.decodeUnknownEffect(AdmissionRequest)({
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

  const payload = yield* Schema.decodeUnknownEffect(SubmissionSettledRecord)({
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
export const seedScan = Effect.fn("LedgerReadFixture.seedScan")(function* (
  count: number,
  settledPrefix: number = 0,
  gaps = false,
) {
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
      CASE WHEN n < ${settledPrefix} OR (${gaps ? 1 : 0} = 1 AND n % 7 = 0) THEN 'settled' ELSE 'ready' END,
      CASE WHEN n < ${settledPrefix} OR (${gaps ? 1 : 0} = 1 AND n % 7 = 0) THEN 'completed' ELSE NULL END,
      '1970-01-01T00:00:00.001Z', '1970-01-01T00:00:00.001Z'
    FROM positions
  `;
});

type CompiledQuery = ReturnType<Statement<unknown>["compile"]>;

export const observeQueries = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const queries: Array<CompiledQuery> = [];

    const result = yield* effect.pipe(
      Effect.provideService(CurrentTransformer, (statement) =>
        Effect.sync(() => {
          queries.push(statement.compile());

          return statement;
        }),
      ),
    );

    return { result, queries };
  });

const assertIndexedScan = Effect.fn("LedgerReadFixture.assertIndexedScan")(function* (
  queries: ReadonlyArray<CompiledQuery>,
) {
  const sql = yield* SqlClient.SqlClient;

  for (const [index, [query, parameters]] of queries.entries()) {
    const plan = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, parameters);
    const accesses = plan.filter(({ detail }) => /\b(?:SCAN|SEARCH)\b/.test(detail));

    // A constant statement count can still hide a full-ledger scan or a correlated scan.
    // Each page walks only the outstanding-obligation index, and later pages seek past
    // the cursor rather than revisiting earlier rows through OFFSET pagination.
    expect(accesses.length).toBeGreaterThan(0);
    for (const { detail } of accesses) {
      expect(detail, "Nonterminal scans must exclude settled rows at the index").toContain(
        "effect_agent_submissions_nonterminal",
      );
      if (index > 0) {
        expect(detail, "Later ledger pages must seek from their cursor").toContain("SEARCH");
        expect(detail, "A Thread-only seek can rescan its earlier queue pages").toContain(
          "queue_sequence",
        );
      }
    }
    expect(plan.some(({ detail }) => detail.includes("TEMP B-TREE"))).toBe(false);
    expect(query).toMatch(/\bLIMIT \?\s*$/);
    expect(parameters.at(-1)).toBeGreaterThan(0);
    expect(parameters.at(-1)).toBeLessThanOrEqual(256);
  }
});

export const ledgerReadCases = [
  {
    name: "scans more than two pages in FIFO order through a partial index and cursor seeks",
    run: Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;

      yield* seedScan(900, 0, true);
      const { result, queries } = yield* observeQueries(Stream.runCollect(ledger.scanNonterminal));
      const expected = Array.from({ length: 900 }, (_, n) => n).filter((n) => n % 7 !== 0);

      expect(result.map(({ submissionId }) => submissionId)).toEqual(
        expected.map((n) => `scan-${n}`),
      );
      expect(result.map(({ queueSequence }) => queueSequence)).toEqual(expected.map((n) => n + 1));
      expect(queries).toHaveLength(4);
      yield* assertIndexedScan(queries);
    }),
  },
  // Empty, short, and exact multiple-page scans distinguish fixed/indexed work from
  // the allowed linear work in *unfinished* rows. Retained settlements add no pages.
  ...[0, 2_048, 8_192].flatMap((settled) =>
    [0, 16, 768].map((unfinished) => ({
      name: `bounds the scan to ${unfinished} unfinished rows with ${settled} settled rows`,
      run: Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        if (settled + unfinished > 0) yield* seedScan(settled + unfinished, settled);

        const { result, queries } = yield* observeQueries(
          Stream.runCollect(ledger.scanNonterminal),
        );

        expect(result.map(({ submissionId }) => submissionId)).toEqual(
          Array.from({ length: unfinished }, (_, n) => `scan-${settled + n}`),
        );
        // A full final page may need one empty read to discover the end of the stream.
        expect(queries.length).toBeLessThanOrEqual(Math.floor(unfinished / 256) + 1);
        expect(queries.length).toBeGreaterThan(0);
        yield* assertIndexedScan(queries);
      }),
    })),
  ),
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
  ...(["completed", "failed", "aborted"] as const).map((outcome) => ({
    name: `replays ${outcome} settlement in one read with unchanged timestamp and diagnostics`,
    run: Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const request = yield* reserveReadFixture(`settlement-${outcome}`, outcome);
      const active = yield* observeQueries(ledger.finalizeSettlement(request));

      // One probe, the two original reads and three original mutations; BEGIN/COMMIT are driver-owned.
      expect(active.queries).toHaveLength(6);
      expect(active.result.outcome).toBe(outcome);
      for (let i = 0; i < 3; i++) {
        const replay = yield* observeQueries(ledger.finalizeSettlement(request));

        expect(replay.result).toEqual(active.result);
        expect(replay.queries).toHaveLength(1);
        expect(replay.queries[0]?.[0].trimStart().startsWith("SELECT")).toBe(true);
      }
      expect(active.result.failure).toEqual(
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
  ...(["record", "timestamp", "failure", "missing"] as const).map((corruption) => ({
    name: `preserves typed ${corruption} failure without repairing settled storage`,
    run: Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const sql = yield* SqlClient.SqlClient;
      const request = yield* reserveReadFixture(`corrupt-${corruption}`);

      yield* ledger.finalizeSettlement(request);
      switch (corruption) {
        case "record":
          yield* sql`UPDATE effect_agent_settlement_reservations SET record_json='{}' WHERE submission_id=${request.submissionId}`;
          break;
        case "timestamp":
          yield* sql`UPDATE effect_agent_settlement_reservations SET finalized_at=NULL WHERE submission_id=${request.submissionId}`;
          break;
        case "failure":
          yield* sql`UPDATE effect_agent_settlement_reservations SET outcome='failed' WHERE submission_id=${request.submissionId}`;
          break;
        case "missing":
          yield* sql`DELETE FROM effect_agent_settlement_reservations WHERE submission_id=${request.submissionId}`;
          break;
      }

      const before =
        yield* sql`SELECT * FROM effect_agent_settlement_reservations WHERE submission_id=${request.submissionId}`;

      const result = yield* ledger.finalizeSettlement(request).pipe(Effect.result);

      expect(result).toMatchObject({ _tag: "Failure", failure: { _tag: "LedgerError" } });
      if (result._tag === "Failure") {
        expect(Schema.is(LedgerError)(result.failure)).toBe(true);
        if (corruption !== "missing")
          expect(result.failure).toMatchObject({
            cause: { _tag: expect.stringMatching(/StorageCorruptionError$/) },
          });
      }
      expect(
        yield* sql`SELECT * FROM effect_agent_settlement_reservations WHERE submission_id=${request.submissionId}`,
      ).toEqual(before);
      expect(
        yield* sql`SELECT state FROM effect_agent_submissions WHERE submission_id=${request.submissionId}`,
      ).toEqual([{ state: "settled" }]);
    }),
  })),
  ...(["failure", "defect", "interruption"] as const).map((mode) => ({
    name: `releases a ${mode} during the settled read and permits later mutations`,
    run: Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const sql = yield* SqlClient.SqlClient;
      const request = yield* reserveReadFixture(`cleanup-${mode}`);
      const settled = yield* ledger.finalizeSettlement(request);

      const result = yield* ledger.finalizeSettlement(request).pipe(
        Effect.provideService(CurrentTransformer, () =>
          mode === "failure"
            ? Effect.succeed(sql`SELECT * FROM effect_agent_missing_read_fixture`)
            : mode === "defect"
              ? Effect.die("read defect")
              : Effect.interrupt,
        ),
        Effect.exit,
      );

      expect(Exit.isFailure(result)).toBe(true);
      if (Exit.isFailure(result)) {
        if (mode === "failure") expect(Cause.hasFails(result.cause)).toBe(true);
        if (mode === "defect") expect(Cause.hasDies(result.cause)).toBe(true);
        if (mode === "interruption") expect(Cause.hasInterrupts(result.cause)).toBe(true);
      }
      expect(yield* ledger.finalizeSettlement(request)).toEqual(settled);
      yield* admitReadFixture(`after-${mode}`, "after");
    }),
  })),
];

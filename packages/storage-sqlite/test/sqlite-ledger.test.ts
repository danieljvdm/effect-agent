import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Crypto,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  PlatformError,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  AbortCommand,
  AdmissionRequest,
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  ClaimRequest,
  ConversationMaterialization,
  ConversationStore,
  ConversationTailRequest,
  DefinitionDigests,
  DeploymentId,
  Digest,
  digestJson,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  FenceRejected,
  IdempotencyKey,
  LedgerError,
  MarkInputAppliedRequest,
  MarkReadyRequest,
  OwnershipLost,
  Principal,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RenewOwnershipRequest,
  ReleaseOwnershipRequest,
  SettlementFinalization,
  SettlementReservation,
  SubmissionLedger,
  SubmissionLookupByKey,
  SubmissionSettled,
  submissionInputRecordId,
  submissionLedgerConformanceCases,
  submissionSettlementId,
  submissionSettlementRecordId,
  UserInputRecorded,
  type AdmissionResult,
  type AppendResult,
  type OwnershipToken,
  type PersistedJson,
  type SettlementOutcome,
} from "@effect-agent/session";
import {
  conversationStoreLayer,
  ledgerLayer,
  storageConfigLayer,
  SqliteStorageCompatibilityError,
  SqliteStorageConfig,
  SqliteStorageFailpoint,
  SqliteStorageFailpointError,
  SqliteWriteContention,
  submissionLedgerLayer,
  type SqliteStorageFailpointLocation,
  type SqliteStorageInitializationError,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;
type SubmissionLedgerLayerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof submissionLedgerLayer>,
    SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
  >
>;
type SubmissionLedgerLayerErrorProof = Assert<
  Equal<Layer.Error<typeof submissionLedgerLayer>, SqliteStorageInitializationError>
>;

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const conversation = (value: string) =>
  id(ConversationMaterialization.fields.conversationId, value);
const epoch = (value: number) => Schema.decodeSync(ProducerEpoch)(value);
const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const TEST_PRINCIPAL = id(Principal, "principal-sqlite-ledger");
const TEST_PRODUCER = id(ProducerId, "producer-sqlite-ledger");
const OTHER_PRODUCER = id(ProducerId, "producer-sqlite-ledger-other");
const TEST_AGENT = id(AdmissionRequest.fields.agentId, "agent-sqlite-ledger");
const TEST_DEPLOYMENT = id(DeploymentId, "deployment-sqlite-ledger");
const TEST_DEFINITION_DIGEST = Schema.decodeSync(Digest)("a".repeat(64));
const TEST_DIGESTS = DefinitionDigests.make({
  agent: TEST_DEFINITION_DIGEST,
  model: TEST_DEFINITION_DIGEST,
  tools: TEST_DEFINITION_DIGEST,
});

const admission = Effect.fn("SqliteLedgerTest.admission")(function* (
  conversationId: string,
  idempotencyKey: string,
  input: PersistedJson,
) {
  const inputDigest = yield* digestJson(input);
  return AdmissionRequest.make({
    conversationId: conversation(conversationId),
    principal: TEST_PRINCIPAL,
    idempotencyKey: id(IdempotencyKey, idempotencyKey),
    agentId: TEST_AGENT,
    agentDigests: TEST_DIGESTS,
    deploymentId: TEST_DEPLOYMENT,
    inputPayload: input,
    inputDigest,
  });
});

const settlementReservation = Effect.fn("SqliteLedgerTest.settlementReservation")(function* (
  admitted: AdmissionResult,
  ownershipToken: OwnershipToken,
  outcome: SettlementOutcome,
) {
  const settlementId = submissionSettlementId(admitted.submissionId);
  const record = RecordEnvelope.make({
    recordId: submissionSettlementRecordId(admitted.submissionId),
    family: "conversation",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload: SubmissionSettled.make({
      submissionId: admitted.submissionId,
      settlementId,
      receiptId: admitted.receiptId,
      outcome,
    }),
  });
  const encoded = yield* Schema.encodeEffect(RecordEnvelope)(record).pipe(Effect.orDie);
  const recordDigest = yield* digestJson(encoded);
  return SettlementReservation.make({
    submissionId: admitted.submissionId,
    ownershipToken,
    settlementId,
    outcome,
    record,
    recordDigest,
  });
});

const inputRecord = (recordId: string, input: string): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/RecordId")),
      recordId,
    ),
    family: "conversation",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload: UserInputRecorded.make({
      submissionId: id(UserInputRecorded.fields.submissionId, "submission-epoch-append"),
      kind: "user",
      input,
    }),
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/BatchId")), batchId),
    producerId: TEST_PRODUCER,
    records,
  });

const append = (
  store: ConversationStore["Service"],
  conversationId: string,
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = {
    lastSequence: sequence(0),
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpoch: ProducerEpoch = epoch(1),
) =>
  store.append(
    FencedAppendRequest.make({
      conversationId: conversation(conversationId),
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch,
    }),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-storage-sqlite-ledger-",
      });
      return yield* use(`${directory}/ledger.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const withLedger = <A, E>(
  filename: string,
  effect: Effect.Effect<A, E, SubmissionLedger | Crypto.Crypto>,
) => Effect.provide(effect, [ledgerLayer({ filename }), NodeCrypto.layer]);

const withSql = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, SqliteClient.layer({ filename }));

/** ConversationStore and SubmissionLedger sharing one SqlClient over one database file. */
const combinedLayer = (filename: string) =>
  Layer.mergeAll(conversationStoreLayer, submissionLedgerLayer).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        storageConfigLayer({ filename, observationPollInterval: 1 }),
        SqliteStorageFailpoint.layer,
        SqliteClient.layer({ filename }),
        NodeCrypto.layer,
      ),
    ),
  );

describe("SqliteSubmissionLedger", () => {
  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((filename) => withLedger(filename, conformanceCase.run)),
      );
    }
  });

  it("keeps configuration, failpoint, SQL, and Crypto authority in the named Layer input", () => {
    const requirementsProof: SubmissionLedgerLayerRequirementsProof = true;
    const errorProof: SubmissionLedgerLayerErrorProof = true;

    expect(requirementsProof).toBe(true);
    expect(errorProof).toBe(true);
  });

  it.effect("persists admissions durably across process-style reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const first = yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const capabilities = yield* ledger.capabilities;
            expect(capabilities.durability).toBe("durable-node");
            const admitted = yield* ledger.admit(
              yield* admission("conversation-reopen", "reopen-key", { city: "Kyoto" }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
            return admitted;
          }),
        );

        yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const replayed = yield* ledger.admit(
              yield* admission("conversation-reopen", "reopen-key", { city: "Kyoto" }),
            );
            expect(replayed.replayed).toBe(true);
            expect(replayed.submissionId).toBe(first.submissionId);
            expect(replayed.receiptId).toBe(first.receiptId);

            const byKey = yield* ledger.lookup(
              SubmissionLookupByKey.make({
                conversationId: conversation("conversation-reopen"),
                principal: TEST_PRINCIPAL,
                idempotencyKey: id(IdempotencyKey, "reopen-key"),
              }),
            );
            expect(Option.isSome(byKey)).toBe(true);
            if (Option.isSome(byKey)) {
              expect(byKey.value.state).toBe("ready");
            }

            const nonterminal = yield* ledger.scanNonterminal.pipe(Stream.runCollect);
            expect(nonterminal.map((snapshot) => snapshot.submissionId)).toEqual([
              first.submissionId,
            ]);
          }),
        );
      }),
    ),
  );

  it.effect("bumps the conversation producer epoch atomically with a claim", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        const ledger = yield* SubmissionLedger;
        const conversationId = "conversation-epoch";

        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId: conversation(conversationId),
            producerEpoch: epoch(1),
          }),
        );
        const first = yield* append(
          store,
          conversationId,
          batch("epoch-batch-1", [inputRecord("epoch-record-1", "before claim")]),
        );

        const admitted = yield* ledger.admit(
          yield* admission(conversationId, "epoch-key", { work: "epoch" }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
        const claim = yield* ledger.claim(
          ClaimRequest.make({
            conversationId: conversation(conversationId),
            producerId: TEST_PRODUCER,
          }),
        );
        expect(Option.isSome(claim)).toBe(true);
        if (Option.isNone(claim)) return;
        expect(claim.value.producerEpoch).toBe(2);

        // The pre-claim epoch is fenced out of canonical appends in the same consistency
        // domain the claim mutated.
        const stale = yield* append(
          store,
          conversationId,
          batch("epoch-batch-2", [inputRecord("epoch-record-2", "stale append")]),
          first,
          epoch(1),
        ).pipe(Effect.exit);
        expect(Exit.isFailure(stale)).toBe(true);
        if (Exit.isFailure(stale)) {
          const error = Cause.squash(stale.cause);
          expect(error).toBeInstanceOf(FenceRejected);
          if (error instanceof FenceRejected) {
            expect(error.actualEpoch).toBe(2);
            expect(error.attemptedEpoch).toBe(1);
          }
        }

        const fenced = yield* append(
          store,
          conversationId,
          batch("epoch-batch-2", [inputRecord("epoch-record-2", "stale append")]),
          first,
          claim.value.producerEpoch,
        );
        expect(fenced.firstSequence).toBe(first.lastSequence + 1);
      }).pipe(Effect.provide(combinedLayer(filename))),
    ),
  );

  it.effect("creates the conversation fencing row when claiming pre-materialization work", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        const ledger = yield* SubmissionLedger;
        const conversationId = "conversation-unmaterialized";

        const admitted = yield* ledger.admit(
          yield* admission(conversationId, "unmaterialized-key", { work: "recover" }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
        const claim = yield* ledger.claim(
          ClaimRequest.make({
            conversationId: conversation(conversationId),
            producerId: TEST_PRODUCER,
          }),
        );
        expect(Option.isSome(claim)).toBe(true);
        if (Option.isNone(claim)) return;
        expect(claim.value.producerEpoch).toBe(1);

        // Recovery re-materializes idempotently at the claim's epoch.
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId: conversation(conversationId),
            producerEpoch: claim.value.producerEpoch,
          }),
        );
        const tail = yield* store.inspectTail(
          ConversationTailRequest.make({ conversationId: conversation(conversationId) }),
        );
        expect(tail.producerEpoch).toBe(claim.value.producerEpoch);
        expect(tail.tailSequence).toBe(0);
        expect(tail.tailDigest).toBe(EMPTY_TAIL_DIGEST);
      }).pipe(Effect.provide(combinedLayer(filename))),
    ),
  );

  it.effect("classifies cross-connection write contention as retryable typed contention", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.admit(yield* admission("conversation-busy", "busy-key-1", { step: 1 }));
          }),
        );

        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            // Hold the write lock on a separate connection, as a transiently coexisting
            // producer would.
            yield* sql`BEGIN IMMEDIATE`;
            const contended = yield* Effect.provide(
              Effect.gen(function* () {
                const ledger = yield* SubmissionLedger;
                return yield* ledger.admit(
                  yield* admission("conversation-busy", "busy-key-2", { step: 2 }),
                );
              }),
              [ledgerLayer({ filename, busyTimeout: 0 }), NodeCrypto.layer],
            ).pipe(Effect.exit);
            yield* sql`ROLLBACK`;

            expect(Exit.isFailure(contended)).toBe(true);
            if (Exit.isFailure(contended)) {
              const error = Cause.squash(contended.cause);
              expect(error).toBeInstanceOf(LedgerError);
              if (error instanceof LedgerError) {
                expect(error.cause).toBeInstanceOf(SqliteWriteContention);
              }
            }
          }),
        );

        // Once the competing writer releases the lock, the identical admission commits.
        const recovered = yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.admit(
              yield* admission("conversation-busy", "busy-key-2", { step: 2 }),
            );
          }),
        );
        expect(recovered.replayed).toBe(false);
      }),
    ),
  );

  it.effect("rejects a v1 file exactly with reset guidance and still rejects newer versions", () =>
    Effect.forEach([1, 3, 99], (storedVersion) =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          yield* withSql(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;
              yield* sql.unsafe(`PRAGMA user_version = ${storedVersion}`);
            }),
          );

          const opened = yield* withLedger(filename, SubmissionLedger).pipe(Effect.exit);
          expect(Exit.isFailure(opened)).toBe(true);
          if (Exit.isFailure(opened)) {
            const error = Cause.squash(opened.cause);
            expect(error).toBeInstanceOf(SqliteStorageCompatibilityError);
            if (error instanceof SqliteStorageCompatibilityError) {
              expect(error.actualVersion).toBe(storedVersion);
              expect(error.supportedVersion).toBe(2);
              expect(error.message).toContain("Reset the database file explicitly");
            }
          }

          // Failing closed must not mutate the incompatible file.
          const tables = yield* withSql(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;
              return yield* sql<Record<string, unknown>>`
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name LIKE 'effect_agent_%'
              `;
            }),
          );
          expect(tables).toEqual([]);
        }),
      ),
    ),
  );

  it.effect("leaves a recovery-classifiable state at every ledger failpoint", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const active = yield* Ref.make<SqliteStorageFailpointLocation | undefined>(undefined);
        const select = (location: SqliteStorageFailpointLocation | undefined) =>
          Ref.set(active, location);
        const failingLedger = <A, E>(
          effect: Effect.Effect<A, E, SubmissionLedger | Crypto.Crypto>,
        ) =>
          Effect.provide(effect, [
            ledgerLayer({
              filename,
              failpoint: (location) =>
                Ref.get(active).pipe(
                  Effect.flatMap((selected) =>
                    selected === location
                      ? Effect.fail(SqliteStorageFailpointError.make({ location }))
                      : Effect.void,
                  ),
                ),
            }),
            NodeCrypto.layer,
          ]);
        const expectInjectedFailure = <A>(
          exit: Exit.Exit<A, unknown>,
          location: SqliteStorageFailpointLocation,
        ) => {
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause);
            expect(error).toBeInstanceOf(LedgerError);
            if (error instanceof LedgerError) {
              expect(error.cause).toBeInstanceOf(SqliteStorageFailpointError);
              if (error.cause instanceof SqliteStorageFailpointError) {
                expect(error.cause.location).toBe(location);
              }
            }
          }
        };
        const submissionStates = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, state, receipt_id, input_applied_record_id
              FROM effect_agent_submissions
              ORDER BY conversation_id, queue_sequence
            `;
          }),
        );
        const ownershipRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, attempt_id, ownership_token, producer_epoch, lease_expires_at
              FROM effect_agent_submission_ownership
            `;
          }),
        );
        const attemptRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT attempt_id, submission_id, producer_epoch
              FROM effect_agent_attempts
              ORDER BY producer_epoch
            `;
          }),
        );
        const reservationRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, settlement_id, outcome, finalized_at
              FROM effect_agent_settlement_reservations
            `;
          }),
        );
        const abortRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, reason, requested_at
              FROM effect_agent_abort_intents
            `;
          }),
        );
        const conversationRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT conversation_id, producer_epoch
              FROM effect_agent_conversations
            `;
          }),
        );

        const lane = "conversation-failpoints";
        const admitOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.admit(yield* admission(lane, "failpoint-key", { work: "fail" }));
          }),
        );

        // admit: before → nothing durable; after → row durable, retry replays the same identity.
        yield* select("ledger:admit:before");
        expectInjectedFailure(yield* admitOnce.pipe(Effect.exit), "ledger:admit:before");
        expect(yield* submissionStates).toEqual([]);
        yield* select("ledger:admit:after");
        expectInjectedFailure(yield* admitOnce.pipe(Effect.exit), "ledger:admit:after");
        const admittedRows = yield* submissionStates;
        expect(admittedRows).toHaveLength(1);
        expect(admittedRows[0]?.state).toBe("admitted");
        yield* select(undefined);
        const admitted = yield* admitOnce;
        expect(admitted.replayed).toBe(true);
        expect(admitted.submissionId).toBe(admittedRows[0]?.submission_id);
        expect(admitted.receiptId).toBe(admittedRows[0]?.receipt_id);

        // markReady: before → still admitted; after → ready durable, retry is a no-op.
        const markReadyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          }),
        );
        yield* select("ledger:mark-ready:before");
        expectInjectedFailure(yield* markReadyOnce.pipe(Effect.exit), "ledger:mark-ready:before");
        expect((yield* submissionStates)[0]?.state).toBe("admitted");
        yield* select("ledger:mark-ready:after");
        expectInjectedFailure(yield* markReadyOnce.pipe(Effect.exit), "ledger:mark-ready:after");
        expect((yield* submissionStates)[0]?.state).toBe("ready");
        yield* select(undefined);
        yield* markReadyOnce;

        // claim: before → no ownership, no conversation row, no epoch consumed; after → the
        // claim is durable (ownership + audit + epoch bump) even though the caller never saw it.
        const claimOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.claim(
              ClaimRequest.make({ conversationId: conversation(lane), producerId: TEST_PRODUCER }),
            );
          }),
        );
        yield* select("ledger:claim:before");
        expectInjectedFailure(yield* claimOnce.pipe(Effect.exit), "ledger:claim:before");
        expect(yield* ownershipRows).toEqual([]);
        expect(yield* conversationRows).toEqual([]);
        yield* select("ledger:claim:after");
        expectInjectedFailure(yield* claimOnce.pipe(Effect.exit), "ledger:claim:after");
        const orphanedOwnership = yield* ownershipRows;
        expect(orphanedOwnership).toHaveLength(1);
        expect(orphanedOwnership[0]?.producer_epoch).toBe(1);
        expect(yield* conversationRows).toEqual([{ conversation_id: lane, producer_epoch: 1 }]);
        expect((yield* submissionStates)[0]?.state).toBe("running");
        expect(yield* attemptRows).toHaveLength(1);
        // The orphaned lease blocks until expiry; a later Attempt reclaims at a higher epoch.
        yield* select(undefined);
        expect(Option.isNone(yield* claimOnce)).toBe(true);
        yield* TestClock.adjust(30_001);
        const claim = yield* claimOnce;
        expect(Option.isSome(claim)).toBe(true);
        if (Option.isNone(claim)) return;
        expect(claim.value.producerEpoch).toBe(2);
        expect(yield* attemptRows).toHaveLength(2);

        // markInputApplied: before → no marker; after → marker durable, retry is a no-op.
        const markInputOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.markInputApplied(
              MarkInputAppliedRequest.make({
                submissionId: admitted.submissionId,
                ownershipToken: claim.value.ownershipToken,
                recordId: submissionInputRecordId(admitted.submissionId),
                sequence: sequence(1),
              }),
            );
          }),
        );
        yield* select("ledger:mark-input-applied:before");
        expectInjectedFailure(
          yield* markInputOnce.pipe(Effect.exit),
          "ledger:mark-input-applied:before",
        );
        expect((yield* submissionStates)[0]?.input_applied_record_id).toBeNull();
        yield* select("ledger:mark-input-applied:after");
        expectInjectedFailure(
          yield* markInputOnce.pipe(Effect.exit),
          "ledger:mark-input-applied:after",
        );
        expect((yield* submissionStates)[0]?.input_applied_record_id).toBe(
          submissionInputRecordId(admitted.submissionId),
        );
        expect((yield* submissionStates)[0]?.state).toBe("input-applied");
        yield* select(undefined);
        yield* markInputOnce;

        // renew: before → lease unchanged; after → extension durable.
        const renewOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.renewOwnership(
              RenewOwnershipRequest.make({
                submissionId: admitted.submissionId,
                ownershipToken: claim.value.ownershipToken,
              }),
            );
          }),
        );
        const leaseBeforeRenew = (yield* ownershipRows)[0]?.lease_expires_at;
        yield* TestClock.adjust(1_000);
        yield* select("ledger:renew:before");
        expectInjectedFailure(yield* renewOnce.pipe(Effect.exit), "ledger:renew:before");
        expect((yield* ownershipRows)[0]?.lease_expires_at).toBe(leaseBeforeRenew);
        yield* select("ledger:renew:after");
        expectInjectedFailure(yield* renewOnce.pipe(Effect.exit), "ledger:renew:after");
        const leaseAfterRenew = (yield* ownershipRows)[0]?.lease_expires_at;
        expect(leaseAfterRenew).not.toBe(leaseBeforeRenew);
        yield* select(undefined);
        yield* renewOnce;

        // reserveSettlement: before → no reservation, submission nonterminal; after → the
        // reservation row is durable and the submission is terminalizing but NOT settled,
        // exactly the state a recovery pass classifies as append-then-finalize.
        const reservation = yield* settlementReservation(
          admitted,
          claim.value.ownershipToken,
          "completed",
        ).pipe(Effect.provide(NodeCrypto.layer));
        const reserveOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.reserveSettlement(reservation);
          }),
        );
        yield* select("ledger:reserve-settlement:before");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:reserve-settlement:before",
        );
        expect(yield* reservationRows).toEqual([]);
        expect((yield* submissionStates)[0]?.state).toBe("input-applied");
        yield* select("ledger:reserve-settlement:after");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:reserve-settlement:after",
        );
        const reservedRows = yield* reservationRows;
        expect(reservedRows).toHaveLength(1);
        expect(reservedRows[0]?.finalized_at).toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("terminalizing");
        yield* select(undefined);
        const replayedReservation = yield* reserveOnce;
        expect(replayedReservation.replayed).toBe(true);

        // finalizeSettlement: before → reservation unfinalized, still terminalizing; after →
        // settled durably; the retry replays the recorded Settlement.
        const finalizeOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: admitted.submissionId,
                settlementId: reservation.settlementId,
              }),
            );
          }),
        );
        yield* select("ledger:finalize-settlement:before");
        expectInjectedFailure(
          yield* finalizeOnce.pipe(Effect.exit),
          "ledger:finalize-settlement:before",
        );
        expect((yield* reservationRows)[0]?.finalized_at).toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("terminalizing");
        yield* select("ledger:finalize-settlement:after");
        expectInjectedFailure(
          yield* finalizeOnce.pipe(Effect.exit),
          "ledger:finalize-settlement:after",
        );
        expect((yield* reservationRows)[0]?.finalized_at).not.toBeNull();
        expect((yield* submissionStates)[0]?.state).toBe("settled");
        expect(yield* ownershipRows).toEqual([]);
        yield* select(undefined);
        const settlement = yield* finalizeOnce;
        expect(settlement.outcome).toBe("completed");

        // requestAbort: before → no intent; after → intent durable, retry returns it unchanged.
        const abortLane = "conversation-failpoints-abort";
        yield* select(undefined);
        const abortAdmitted = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const result = yield* ledger.admit(
              yield* admission(abortLane, "failpoint-abort-key", { work: "abort" }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: result.submissionId }));
            return result;
          }),
        );
        const abortOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.requestAbort(
              AbortCommand.make({
                submissionId: abortAdmitted.submissionId,
                author: "failpoint-operator",
                reason: "failpoint abort",
              }),
            );
          }),
        );
        yield* select("ledger:request-abort:before");
        expectInjectedFailure(yield* abortOnce.pipe(Effect.exit), "ledger:request-abort:before");
        expect(yield* abortRows).toEqual([]);
        yield* select("ledger:request-abort:after");
        expectInjectedFailure(yield* abortOnce.pipe(Effect.exit), "ledger:request-abort:after");
        const abortIntents = yield* abortRows;
        expect(abortIntents).toHaveLength(1);
        yield* select(undefined);
        const intent = yield* abortOnce;
        expect(intent.reason).toBe("failpoint abort");

        // release: before → ownership retained; after → ownership released durably, the retry
        // observes OwnershipLost exactly as a recovering caller would.
        const abortClaim = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation(abortLane),
                producerId: OTHER_PRODUCER,
              }),
            );
          }),
        );
        expect(Option.isSome(abortClaim)).toBe(true);
        if (Option.isNone(abortClaim)) return;
        const releaseOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: abortAdmitted.submissionId,
                ownershipToken: abortClaim.value.ownershipToken,
              }),
            );
          }),
        );
        yield* select("ledger:release:before");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:release:before");
        expect(yield* ownershipRows).toHaveLength(1);
        yield* select("ledger:release:after");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:release:after");
        expect(yield* ownershipRows).toEqual([]);
        expect(
          (yield* submissionStates).find((row) => row.submission_id === abortAdmitted.submissionId)
            ?.state,
        ).toBe("ready");
        yield* select(undefined);
        const retriedRelease = yield* releaseOnce.pipe(Effect.exit);
        expect(Exit.isFailure(retriedRelease)).toBe(true);
        if (Exit.isFailure(retriedRelease)) {
          expect(Cause.squash(retriedRelease.cause)).toBeInstanceOf(OwnershipLost);
        }
      }),
    ),
  );
});

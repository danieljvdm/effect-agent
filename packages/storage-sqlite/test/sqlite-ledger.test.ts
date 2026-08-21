import {
  AbortCommand,
  AdmissionRequest,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
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
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  ParentLinkage,
  Principal,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecoverySnapshotRequest,
  ReleaseChildBudgetRequest,
  RenewOwnershipRequest,
  ReleaseOwnershipRequest,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  RevertJoiningRequest,
  SettlementFinalization,
  SettlementReservation,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  SubmissionSettled,
  SubmissionSettledRecord,
  SuspendRequest,
  UnknownResolutionCommand,
  WaitingChild,
  WaitingForChildSuspension,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementRecordId,
  UserInputRecorded,
  type AdmissionResult,
  type AppendResult,
  type OwnershipToken,
  type PersistedJson,
  type SettlementOutcome,
} from "@effect-agent/session";
import { submissionLedgerConformanceCases } from "@effect-agent/session/testing";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import type { Crypto, PlatformError } from "effect";
import {
  Cause,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  type SqliteStorageConfig,
  conversationStoreLayer,
  ledgerLayer,
  storageConfigLayer,
  SqliteStorageCompatibilityError,
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

const toolCall = (value: string) => id(ApprovalDecisionCommand.fields.toolCallId, value);
const isFenceRejected = Schema.is(FenceRejected);
const isLedgerError = Schema.is(LedgerError);
const isSqliteStorageCompatibilityError = Schema.is(SqliteStorageCompatibilityError);
const isSqliteStorageFailpointError = Schema.is(SqliteStorageFailpointError);

const TEST_PRINCIPAL = id(Principal, "principal-sqlite-ledger");
const TEST_PRODUCER = id(ProducerId, "producer-sqlite-ledger");
const OTHER_PRODUCER = id(ProducerId, "producer-sqlite-ledger-other");
const TEST_AGENT = id(AdmissionRequest.fields.agentId, "agent-sqlite-ledger");
const TEST_DEPLOYMENT = id(DeploymentId, "deployment-sqlite-ledger");
const TEST_DEFINITION_DIGEST = Schema.decodeSync(Digest)("a".repeat(64));
const S2_REOPEN_RESERVATION = Schema.decodeSync(ChildReservationId)(
  "child-reservation:run-s2:call-s2",
);
const S2_FAILPOINT_RESERVATION = Schema.decodeSync(ChildReservationId)(
  "child-reservation:run-s2fp:call-1",
);
const TEST_DIGESTS = DefinitionDigests.make({
  agent: TEST_DEFINITION_DIGEST,
  model: TEST_DEFINITION_DIGEST,
  tools: TEST_DEFINITION_DIGEST,
});

const admission = Effect.fn("SqliteLedgerTest.admission")(function* (
  conversationId: string,
  idempotencyKey: string,
  input: PersistedJson,
  parentLinkage?: ParentLinkage,
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
    ...(parentLinkage === undefined ? {} : { parentLinkage }),
  });
});

const settlementReservation = Effect.fn("SqliteLedgerTest.settlementReservation")(function* (
  admitted: AdmissionResult,
  ownershipToken: OwnershipToken,
  outcome: SettlementOutcome,
) {
  const settlementId = submissionSettlementId(admitted.submissionId);
  const payload = yield* Schema.decodeUnknownEffect(SubmissionSettledRecord)(
    SubmissionSettled.make({
      submissionId: admitted.submissionId,
      settlementId,
      receiptId: admitted.receiptId,
      outcome,
      ...(outcome === "failed"
        ? {
            result: {
              errorTag: "SqliteLedgerTestFailure",
              message: "The SQLite ledger test Submission failed",
            },
          }
        : {}),
    }),
  ).pipe(Effect.orDie);
  const record = RecordEnvelope.make({
    recordId: submissionSettlementRecordId(admitted.submissionId),
    family: "conversation",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload,
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

  it.effect("validates convenience-layer configuration before constructing the ledger", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const opened = yield* SubmissionLedger.pipe(
          Effect.provide(ledgerLayer({ filename, observationPollInterval: -1 })),
          Effect.exit,
        );

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          expect(Cause.squash(opened.cause)).toHaveProperty("_tag", "SqliteStorageError");
        }
        const databaseExists = yield* FileSystem.FileSystem.use((fs) => fs.exists(filename)).pipe(
          Effect.provide(NodeFileSystem.layer),
        );
        expect(databaseExists).toBe(false);
      }),
    ),
  );

  it.effect("treats reordered persisted JSON as an idempotent replay", () =>
    withTemporaryDatabase((filename) =>
      withLedger(
        filename,
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const lane = conversation("conversation-semantic-json");
          const admitted = yield* ledger.admit(
            yield* admission("conversation-semantic-json", "semantic-json-key", {
              work: "semantic JSON",
            }),
          );
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          const claim = yield* ledger.claim(
            ClaimRequest.make({ conversationId: lane, producerId: TEST_PRODUCER }),
          );
          if (Option.isNone(claim)) return yield* Effect.die("missing semantic JSON claim");

          const allocation = { turns: 4, toolCalls: 8 };
          const reorderedAllocation = { toolCalls: 8, turns: 4 };
          const allocationDigest = yield* digestJson(allocation);
          expect(yield* digestJson(reorderedAllocation)).toBe(allocationDigest);
          const reservationId = id(ChildReservationId, "child-reservation:sqlite-semantic-json");
          const reservationFields = {
            reservationId,
            parentSubmissionId: admitted.submissionId,
            parentToolCallId: toolCall("call-sqlite-semantic-json"),
            ownershipToken: claim.value.ownershipToken,
            allocationDigest,
          };
          yield* ledger.reserveChildBudget(
            ChildBudgetReservationRequest.make({ ...reservationFields, allocation }),
          );
          const replayed = yield* ledger.reserveChildBudget(
            ChildBudgetReservationRequest.make({
              ...reservationFields,
              allocation: reorderedAllocation,
            }),
          );
          expect(replayed.replayed).toBe(true);

          const accounting = {
            consumed: { turns: 1, toolCalls: 2 },
            released: { turns: 3, toolCalls: 6 },
          };
          yield* ledger.beginChildBudgetRelease(
            BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
          );
          const replayedFreeze = yield* ledger.beginChildBudgetRelease(
            BeginChildBudgetReleaseRequest.make({
              reservationId,
              accounting: {
                released: { toolCalls: 6, turns: 3 },
                consumed: { toolCalls: 2, turns: 1 },
              },
            }),
          );
          expect(replayedFreeze.status).toBe("releasePending");

          const resolutionCall = toolCall("call-sqlite-semantic-resolution");
          yield* ledger.markUnknown(
            MarkUnknownRequest.make({
              submissionId: admitted.submissionId,
              toolCallIds: [resolutionCall],
              reason: "semantic JSON replay",
            }),
          );
          const firstResolution = yield* ledger.recordUnknownResolution(
            UnknownResolutionCommand.make({
              submissionId: admitted.submissionId,
              toolCallId: resolutionCall,
              author: "sqlite-ledger-test",
              reason: "supplier answered",
              resolution: ResolutionCompletedWithResult.make({
                result: { bookingRef: "booking-1", details: { city: "Kyoto", nights: 2 } },
                isFailure: false,
              }),
            }),
          );
          const replayedResolution = yield* ledger.recordUnknownResolution(
            UnknownResolutionCommand.make({
              submissionId: admitted.submissionId,
              toolCallId: resolutionCall,
              author: "sqlite-ledger-test-replay",
              reason: "same supplier answer",
              resolution: ResolutionCompletedWithResult.make({
                isFailure: false,
                result: { details: { nights: 2, city: "Kyoto" }, bookingRef: "booking-1" },
              }),
            }),
          );
          expect(replayedResolution.resolvedAt).toEqual(firstResolution.resolvedAt);
        }),
      ),
    ),
  );

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
          if (isFenceRejected(error)) {
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
              if (isLedgerError(error)) {
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

  it.effect(
    "rejects v1-v3 files exactly with reset guidance and still rejects newer versions",
    () =>
      Effect.forEach([1, 2, 3, 5, 99], (storedVersion) =>
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
              if (isSqliteStorageCompatibilityError(error)) {
                expect(error.actualVersion).toBe(storedVersion);
                expect(error.supportedVersion).toBe(4);
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
            if (isLedgerError(error)) {
              expect(error.cause).toBeInstanceOf(SqliteStorageFailpointError);
              if (isSqliteStorageFailpointError(error.cause)) {
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

  it.effect("persists joins, suspensions, approvals, and unknown resolutions across reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const seeded = yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            // Joined queued input.
            const host = yield* ledger.admit(
              yield* admission("conversation-reopen-join", "reopen-host-key", { work: "host" }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: host.submissionId }));
            const queued = yield* ledger.admit(
              yield* admission("conversation-reopen-join", "reopen-queued-key", { queued: 2 }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: queued.submissionId }));
            const hostClaim = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation("conversation-reopen-join"),
                producerId: TEST_PRODUCER,
              }),
            );
            expect(Option.isSome(hostClaim)).toBe(true);
            if (Option.isNone(hostClaim)) return yield* Effect.die("missing host claim");
            const claims = yield* ledger.claimJoining(
              ClaimJoiningRequest.make({
                conversationId: conversation("conversation-reopen-join"),
                hostSubmissionId: host.submissionId,
                ownershipToken: hostClaim.value.ownershipToken,
                maxCount: 4,
              }),
            );
            expect(claims.map((claim) => claim.submissionId)).toEqual([queued.submissionId]);
            yield* ledger.markJoined(
              MarkJoinedRequest.make({
                submissionId: queued.submissionId,
                ownershipToken: hostClaim.value.ownershipToken,
                recordId: submissionInputRecordId(queued.submissionId),
                sequence: sequence(3),
              }),
            );

            // Durable approval suspension with one of two calls decided.
            const gated = yield* ledger.admit(
              yield* admission("conversation-reopen-suspend", "reopen-gated-key", {
                work: "gated",
              }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: gated.submissionId }));
            const gatedClaim = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation("conversation-reopen-suspend"),
                producerId: TEST_PRODUCER,
              }),
            );
            expect(Option.isSome(gatedClaim)).toBe(true);
            if (Option.isNone(gatedClaim)) return yield* Effect.die("missing gated claim");
            yield* ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: gated.submissionId,
                toolCallId: toolCall("call-reopen-s1"),
                decision: "approved",
                resolver: "reopen-approver",
                reason: "first of two calls",
              }),
            );
            const suspended = yield* ledger.suspend(
              SuspendRequest.make({
                submissionId: gated.submissionId,
                ownershipToken: gatedClaim.value.ownershipToken,
                reason: ApprovalPendingSuspension.make({
                  toolCallIds: [toolCall("call-reopen-s1"), toolCall("call-reopen-s2")],
                }),
              }),
            );
            expect(suspended).toBe("suspended");

            // Unknown Outcome with one of two calls resolved.
            const uncertain = yield* ledger.admit(
              yield* admission("conversation-reopen-unknown", "reopen-uncertain-key", {
                work: "uncertain",
              }),
            );
            yield* ledger.markReady(
              MarkReadyRequest.make({ submissionId: uncertain.submissionId }),
            );
            yield* ledger.markUnknown(
              MarkUnknownRequest.make({
                submissionId: uncertain.submissionId,
                toolCallIds: [toolCall("call-reopen-u1"), toolCall("call-reopen-u2")],
                reason: "the worker died during two supplier calls",
              }),
            );
            yield* ledger.recordUnknownResolution(
              UnknownResolutionCommand.make({
                submissionId: uncertain.submissionId,
                toolCallId: toolCall("call-reopen-u1"),
                author: "reopen-operator",
                reason: "supplier store shows the booking",
                resolution: ResolutionCompletedWithResult.make({
                  result: { bookingRef: "booking-reopen-1" },
                  isFailure: false,
                }),
              }),
            );

            return {
              host: host.submissionId,
              queued: queued.submissionId,
              gated: gated.submissionId,
              uncertain: uncertain.submissionId,
            };
          }),
        );

        // A fresh process reads every Phase 5 marker back through the storage schemas.
        yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const hostSnapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: seeded.host }),
            );
            expect(hostSnapshot.joins).toHaveLength(1);
            expect(hostSnapshot.joins[0]?.submissionId).toBe(seeded.queued);
            expect(hostSnapshot.joins[0]?.state).toBe("joined");
            expect(hostSnapshot.joins[0]?.hostSubmissionId).toBe(seeded.host);

            const queuedSnapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: seeded.queued }),
            );
            expect(queuedSnapshot.submission.state).toBe("joined");
            expect(queuedSnapshot.hostSubmissionId).toBe(seeded.host);
            expect(queuedSnapshot.inputApplied?.recordId).toBe(
              submissionInputRecordId(seeded.queued),
            );
            expect(queuedSnapshot.inputApplied?.sequence).toBe(3);

            const gatedSnapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: seeded.gated }),
            );
            expect(gatedSnapshot.submission.state).toBe("suspended");
            expect(gatedSnapshot.suspension?.reason._tag).toBe("ApprovalPending");
            if (gatedSnapshot.suspension?.reason._tag === "ApprovalPending") {
              expect(gatedSnapshot.suspension.reason.toolCallIds).toEqual([
                toolCall("call-reopen-s1"),
                toolCall("call-reopen-s2"),
              ]);
            }
            expect(gatedSnapshot.approvalDecisions).toHaveLength(1);
            expect(gatedSnapshot.approvalDecisions[0]?.toolCallId).toBe(toolCall("call-reopen-s1"));
            expect(gatedSnapshot.approvalDecisions[0]?.decision).toBe("approved");
            const blockedSuspended = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation("conversation-reopen-suspend"),
                producerId: OTHER_PRODUCER,
              }),
            );
            expect(Option.isNone(blockedSuspended)).toBe(true);

            const uncertainSnapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: seeded.uncertain }),
            );
            expect(uncertainSnapshot.submission.state).toBe("unknown");
            expect(uncertainSnapshot.unknownResolutions).toHaveLength(1);
            const resolution = uncertainSnapshot.unknownResolutions[0]?.resolution;
            expect(resolution?._tag).toBe("CompletedWithResult");
            if (resolution?._tag === "CompletedWithResult") {
              expect(resolution.result).toEqual({ bookingRef: "booking-reopen-1" });
              expect(resolution.isFailure).toBe(false);
            }

            // The durable intents keep working after reopen: covering decisions and
            // resolutions wake the respective lanes.
            yield* ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: seeded.gated,
                toolCallId: toolCall("call-reopen-s2"),
                decision: "denied",
                resolver: "reopen-approver",
                reason: "second of two calls",
              }),
            );
            const wokenGated = yield* ledger.lookup(
              SubmissionLookupById.make({ submissionId: seeded.gated }),
            );
            expect(Option.isSome(wokenGated)).toBe(true);
            if (Option.isSome(wokenGated)) expect(wokenGated.value.state).toBe("input-applied");

            yield* ledger.recordUnknownResolution(
              UnknownResolutionCommand.make({
                submissionId: seeded.uncertain,
                toolCallId: toolCall("call-reopen-u2"),
                author: "reopen-operator",
                reason: "supplier store shows nothing",
                resolution: ResolutionNeverHappened.make(),
              }),
            );
            const wokenUncertain = yield* ledger.lookup(
              SubmissionLookupById.make({ submissionId: seeded.uncertain }),
            );
            expect(Option.isSome(wokenUncertain)).toBe(true);
            if (Option.isSome(wokenUncertain)) {
              expect(wokenUncertain.value.state).toBe("input-applied");
            }
          }),
        );
      }),
    ),
  );

  it.effect("leaves a recovery-classifiable state at every Phase 5 ledger failpoint", () =>
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
            if (isLedgerError(error)) {
              expect(error.cause).toBeInstanceOf(SqliteStorageFailpointError);
              if (isSqliteStorageFailpointError(error.cause)) {
                expect(error.cause.location).toBe(location);
              }
            }
          }
        };
        const submissionMarkers = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT
                submission_id,
                state,
                joined_host_submission_id,
                input_applied_record_id,
                suspended_reason_json,
                unknown_reason,
                unknown_tool_call_ids_json
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
              SELECT submission_id, ownership_token
              FROM effect_agent_submission_ownership
            `;
          }),
        );
        const approvalRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, tool_call_id, decision, decided_at
              FROM effect_agent_approval_decisions
              ORDER BY tool_call_id
            `;
          }),
        );
        const resolutionRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT submission_id, tool_call_id, resolution_json
              FROM effect_agent_unknown_resolutions
              ORDER BY tool_call_id
            `;
          }),
        );
        const markerFor = (rows: ReadonlyArray<Record<string, unknown>>, submissionId: string) =>
          rows.find((row) => row.submission_id === submissionId);

        const lane = "conversation-p5-failpoints";
        const { host, hostClaim, queued, queuedSecond } = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const host = yield* ledger.admit(
              yield* admission(lane, "p5-host-key", { work: "host" }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: host.submissionId }));
            const queued = yield* ledger.admit(
              yield* admission(lane, "p5-queued-key", { queued: 2 }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: queued.submissionId }));
            const queuedSecond = yield* ledger.admit(
              yield* admission(lane, "p5-queued-second-key", { queued: 3 }),
            );
            yield* ledger.markReady(
              MarkReadyRequest.make({ submissionId: queuedSecond.submissionId }),
            );
            const claim = yield* ledger.claim(
              ClaimRequest.make({ conversationId: conversation(lane), producerId: TEST_PRODUCER }),
            );
            if (Option.isNone(claim)) return yield* Effect.die("missing host claim");
            return { host, hostClaim: claim.value, queued, queuedSecond };
          }),
        );

        // claimJoining: before → nothing claimed; after → the joining transition and host
        // linkage are durable even though the caller never saw the claims (recovery sees a
        // joining Submission without canonical input → RevertJoining).
        const claimJoiningOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.claimJoining(
              ClaimJoiningRequest.make({
                conversationId: conversation(lane),
                hostSubmissionId: host.submissionId,
                ownershipToken: hostClaim.ownershipToken,
                maxCount: 1,
              }),
            );
          }),
        );
        yield* select("ledger:claim-joining:before");
        expectInjectedFailure(
          yield* claimJoiningOnce.pipe(Effect.exit),
          "ledger:claim-joining:before",
        );
        expect(markerFor(yield* submissionMarkers, queued.submissionId)?.state).toBe("ready");
        yield* select("ledger:claim-joining:after");
        expectInjectedFailure(
          yield* claimJoiningOnce.pipe(Effect.exit),
          "ledger:claim-joining:after",
        );
        const joiningMarker = markerFor(yield* submissionMarkers, queued.submissionId);
        expect(joiningMarker?.state).toBe("joining");
        expect(joiningMarker?.joined_host_submission_id).toBe(host.submissionId);
        expect(joiningMarker?.input_applied_record_id).toBeNull();
        yield* select(undefined);
        const secondClaims = yield* claimJoiningOnce;
        expect(secondClaims.map((claim) => claim.submissionId)).toEqual([
          queuedSecond.submissionId,
        ]);

        // markJoined: before → still joining without a marker; after → joined durably;
        // the retry is an idempotent no-op.
        const markJoinedOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.markJoined(
              MarkJoinedRequest.make({
                submissionId: queued.submissionId,
                ownershipToken: hostClaim.ownershipToken,
                recordId: submissionInputRecordId(queued.submissionId),
                sequence: sequence(2),
              }),
            );
          }),
        );
        yield* select("ledger:mark-joined:before");
        expectInjectedFailure(yield* markJoinedOnce.pipe(Effect.exit), "ledger:mark-joined:before");
        expect(
          markerFor(yield* submissionMarkers, queued.submissionId)?.input_applied_record_id,
        ).toBeNull();
        yield* select("ledger:mark-joined:after");
        expectInjectedFailure(yield* markJoinedOnce.pipe(Effect.exit), "ledger:mark-joined:after");
        const joinedMarker = markerFor(yield* submissionMarkers, queued.submissionId);
        expect(joinedMarker?.state).toBe("joined");
        expect(joinedMarker?.input_applied_record_id).toBe(
          submissionInputRecordId(queued.submissionId),
        );
        yield* select(undefined);
        yield* markJoinedOnce;

        // revertJoining: before → still joining; after → ready with the linkage cleared;
        // the retry is an idempotent no-op.
        const revertOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.revertJoining(
              RevertJoiningRequest.make({ submissionId: queuedSecond.submissionId }),
            );
          }),
        );
        yield* select("ledger:revert-joining:before");
        expectInjectedFailure(yield* revertOnce.pipe(Effect.exit), "ledger:revert-joining:before");
        expect(markerFor(yield* submissionMarkers, queuedSecond.submissionId)?.state).toBe(
          "joining",
        );
        yield* select("ledger:revert-joining:after");
        expectInjectedFailure(yield* revertOnce.pipe(Effect.exit), "ledger:revert-joining:after");
        const revertedMarker = markerFor(yield* submissionMarkers, queuedSecond.submissionId);
        expect(revertedMarker?.state).toBe("ready");
        expect(revertedMarker?.joined_host_submission_id).toBeNull();
        yield* select(undefined);
        yield* revertOnce;

        // recordApprovalDecision: before → no intent; after → intent durable; the retry
        // replays the recorded intent unchanged.
        const decideOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: host.submissionId,
                toolCallId: toolCall("call-fp-a"),
                decision: "approved",
                resolver: "failpoint-approver",
                reason: "failpoint decision",
              }),
            );
          }),
        );
        yield* select("ledger:approval-decision:before");
        expectInjectedFailure(
          yield* decideOnce.pipe(Effect.exit),
          "ledger:approval-decision:before",
        );
        expect(yield* approvalRows).toEqual([]);
        yield* select("ledger:approval-decision:after");
        expectInjectedFailure(
          yield* decideOnce.pipe(Effect.exit),
          "ledger:approval-decision:after",
        );
        const decidedRows = yield* approvalRows;
        expect(decidedRows).toHaveLength(1);
        yield* select(undefined);
        const replayedIntent = yield* decideOnce;
        expect(replayedIntent.decision).toBe("approved");
        expect(yield* approvalRows).toHaveLength(1);

        // suspend: before → ownership retained, no suspension; after → suspended durably
        // with the ownership period ended; the retry observes OwnershipLost exactly as a
        // recovering caller would.
        const suspendOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.suspend(
              SuspendRequest.make({
                submissionId: host.submissionId,
                ownershipToken: hostClaim.ownershipToken,
                reason: ApprovalPendingSuspension.make({ toolCallIds: [toolCall("call-fp-b")] }),
              }),
            );
          }),
        );
        yield* select("ledger:suspend:before");
        expectInjectedFailure(yield* suspendOnce.pipe(Effect.exit), "ledger:suspend:before");
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("running");
        expect(yield* ownershipRows).toHaveLength(1);
        yield* select("ledger:suspend:after");
        expectInjectedFailure(yield* suspendOnce.pipe(Effect.exit), "ledger:suspend:after");
        const suspendedMarker = markerFor(yield* submissionMarkers, host.submissionId);
        expect(suspendedMarker?.state).toBe("suspended");
        expect(suspendedMarker?.suspended_reason_json).not.toBeNull();
        expect(yield* ownershipRows).toEqual([]);
        yield* select(undefined);
        const retriedSuspend = yield* suspendOnce.pipe(Effect.exit);
        expect(Exit.isFailure(retriedSuspend)).toBe(true);
        if (Exit.isFailure(retriedSuspend)) {
          expect(Cause.squash(retriedSuspend.cause)).toBeInstanceOf(OwnershipLost);
        }

        // Wake the lane and reclaim it for the unknown-outcome failpoints.
        yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.recordApprovalDecision(
              ApprovalDecisionCommand.make({
                submissionId: host.submissionId,
                toolCallId: toolCall("call-fp-b"),
                decision: "approved",
                resolver: "failpoint-approver",
                reason: "wake the suspended lane",
              }),
            );
          }),
        );
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("input-applied");

        // markUnknown: before → state unchanged; after → the unknown mark is durable; the
        // retry is an idempotent no-op.
        const markUnknownOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            yield* ledger.markUnknown(
              MarkUnknownRequest.make({
                submissionId: host.submissionId,
                toolCallIds: [toolCall("call-fp-c"), toolCall("call-fp-d")],
                reason: "failpoint uncertainty",
              }),
            );
          }),
        );
        yield* select("ledger:mark-unknown:before");
        expectInjectedFailure(
          yield* markUnknownOnce.pipe(Effect.exit),
          "ledger:mark-unknown:before",
        );
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.unknown_reason).toBeNull();
        yield* select("ledger:mark-unknown:after");
        expectInjectedFailure(
          yield* markUnknownOnce.pipe(Effect.exit),
          "ledger:mark-unknown:after",
        );
        const unknownMarker = markerFor(yield* submissionMarkers, host.submissionId);
        expect(unknownMarker?.state).toBe("unknown");
        expect(unknownMarker?.unknown_reason).toBe("failpoint uncertainty");
        expect(unknownMarker?.unknown_tool_call_ids_json).not.toBeNull();
        yield* select(undefined);
        yield* markUnknownOnce;

        // recordUnknownResolution: before → no intent; after → the intent is durable while
        // the lane stays blocked; the covering resolution's wake transition commits
        // atomically with its intent.
        const resolveOnce = (call: string, resolution: "never" | "completed") =>
          failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;
              return yield* ledger.recordUnknownResolution(
                UnknownResolutionCommand.make({
                  submissionId: host.submissionId,
                  toolCallId: toolCall(call),
                  author: "failpoint-operator",
                  reason: "failpoint resolution",
                  resolution:
                    resolution === "never"
                      ? ResolutionNeverHappened.make()
                      : ResolutionCompletedWithResult.make({
                          result: { bookingRef: "booking-fp-1" },
                          isFailure: false,
                        }),
                }),
              );
            }),
          );
        yield* select("ledger:unknown-resolution:before");
        expectInjectedFailure(
          yield* resolveOnce("call-fp-c", "never").pipe(Effect.exit),
          "ledger:unknown-resolution:before",
        );
        expect(yield* resolutionRows).toEqual([]);
        yield* select("ledger:unknown-resolution:after");
        expectInjectedFailure(
          yield* resolveOnce("call-fp-c", "never").pipe(Effect.exit),
          "ledger:unknown-resolution:after",
        );
        expect(yield* resolutionRows).toHaveLength(1);
        expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe("unknown");
        yield* select(undefined);
        yield* resolveOnce("call-fp-c", "never");

        yield* select("ledger:unknown-resolution:after");
        expectInjectedFailure(
          yield* resolveOnce("call-fp-d", "completed").pipe(Effect.exit),
          "ledger:unknown-resolution:after",
        );
        // The covering resolution and its wake transition are one atomic durable step.
        expect(yield* resolutionRows).toHaveLength(2);
        const wokenMarker = markerFor(yield* submissionMarkers, host.submissionId);
        expect(wokenMarker?.state).toBe("input-applied");
        expect(wokenMarker?.unknown_reason).toBeNull();
        expect(wokenMarker?.unknown_tool_call_ids_json).toBeNull();
        yield* select(undefined);
        const replayedResolution = yield* resolveOnce("call-fp-d", "completed");
        expect(replayedResolution.resolution._tag).toBe("CompletedWithResult");
        expect(yield* resolutionRows).toHaveLength(2);
      }),
    ),
  );

  it.effect("persists child reservations and parent linkage across reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const parentLane = "conversation-s2-reopen-parent";
        const childLane = "conversation-s2-reopen-child";
        const reservationId = S2_REOPEN_RESERVATION;
        const delegationCall = toolCall("call-s2");

        const seeded = yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const parent = yield* ledger.admit(
              yield* admission(parentLane, "s2-parent-key", { work: "parent" }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: parent.submissionId }));
            const parentClaim = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation(parentLane),
                producerId: TEST_PRODUCER,
              }),
            );
            if (Option.isNone(parentClaim)) return yield* Effect.die("missing parent claim");

            const child = yield* ledger.admit(
              yield* admission(
                childLane,
                "s2-child-key",
                { task: "research" },
                ParentLinkage.make({
                  parentSubmissionId: parent.submissionId,
                  parentToolCallId: delegationCall,
                }),
              ),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: child.submissionId }));

            const allocation = { turns: 2, toolCalls: 4 };
            yield* ledger.reserveChildBudget(
              ChildBudgetReservationRequest.make({
                reservationId,
                parentSubmissionId: parent.submissionId,
                parentToolCallId: delegationCall,
                ownershipToken: parentClaim.value.ownershipToken,
                allocation,
                allocationDigest: yield* digestJson(allocation),
              }),
            );
            yield* ledger.attachChildToReservation(
              AttachChildToReservationRequest.make({
                reservationId,
                ownershipToken: parentClaim.value.ownershipToken,
                childSubmissionId: child.submissionId,
              }),
            );
            const suspended = yield* ledger.suspend(
              SuspendRequest.make({
                submissionId: parent.submissionId,
                ownershipToken: parentClaim.value.ownershipToken,
                reason: WaitingForChildSuspension.make({
                  children: [
                    WaitingChild.make({
                      toolCallId: delegationCall,
                      childSubmissionId: child.submissionId,
                    }),
                  ],
                }),
              }),
            );
            expect(suspended).toBe("suspended");
            return { parent, child };
          }),
        );
        if (seeded === undefined) return;

        // A fresh process reads every S2 marker back through the storage schemas, and the
        // durable wake still works after reopen.
        yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const parentSnapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: seeded.parent.submissionId }),
            );
            expect(parentSnapshot.submission.state).toBe("suspended");
            expect(parentSnapshot.suspension?.reason._tag).toBe("WaitingForChild");
            if (parentSnapshot.suspension?.reason._tag === "WaitingForChild") {
              expect(parentSnapshot.suspension.reason.children).toHaveLength(1);
              expect(parentSnapshot.suspension.reason.children[0]?.childSubmissionId).toBe(
                seeded.child.submissionId,
              );
            }
            expect(parentSnapshot.childReservations).toHaveLength(1);
            expect(parentSnapshot.childReservations[0]?.reservationId).toBe(reservationId);
            expect(parentSnapshot.childReservations[0]?.status).toBe("reserved");
            expect(parentSnapshot.childReservations[0]?.childSubmissionId).toBe(
              seeded.child.submissionId,
            );
            expect(parentSnapshot.childReservations[0]?.allocation).toEqual({
              turns: 2,
              toolCalls: 4,
            });
            expect(parentSnapshot.childAttachments).toHaveLength(1);
            expect(parentSnapshot.childAttachments[0]?.toolCallId).toBe(delegationCall);
            expect(parentSnapshot.childAttachments[0]?.childState).toBe("ready");

            const childSnapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: seeded.child.submissionId }),
            );
            expect(childSnapshot.parentLinkage?.parentSubmissionId).toBe(
              seeded.parent.submissionId,
            );
            expect(childSnapshot.parentLinkage?.parentToolCallId).toBe(delegationCall);

            const resolved = yield* ledger.resolveAdmission(
              SubmissionLookupByKey.make({
                conversationId: conversation(childLane),
                principal: TEST_PRINCIPAL,
                idempotencyKey: id(IdempotencyKey, "s2-child-key"),
              }),
            );
            expect(resolved._tag).toBe("Admitted");
            if (resolved._tag === "Admitted") {
              expect(resolved.submission.submissionId).toBe(seeded.child.submissionId);
              expect(resolved.submission.receiptId).toBe(seeded.child.receiptId);
            }

            // The suspended parent stays dormant; the child settles on its own lane and the
            // recorded settlement durably wakes the parent (spec §12 step 10).
            const blocked = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation(parentLane),
                producerId: OTHER_PRODUCER,
              }),
            );
            expect(Option.isNone(blocked)).toBe(true);
            const childClaim = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation(childLane),
                producerId: TEST_PRODUCER,
              }),
            );
            expect(Option.isSome(childClaim)).toBe(true);
            if (Option.isNone(childClaim)) return;
            const reservation = yield* settlementReservation(
              seeded.child,
              childClaim.value.ownershipToken,
              "completed",
            );
            yield* ledger.reserveSettlement(reservation);
            yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: seeded.child.submissionId,
                settlementId: reservation.settlementId,
              }),
            );
            const woken = yield* ledger.recordChildSettled(
              ChildSettledNotification.make({
                parentSubmissionId: seeded.parent.submissionId,
                childSubmissionId: seeded.child.submissionId,
              }),
            );
            expect(woken).toBe("woken");
            const parentClaim = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation(parentLane),
                producerId: OTHER_PRODUCER,
              }),
            );
            expect(Option.isSome(parentClaim)).toBe(true);

            // The reservation accounting decision freezes and releases exactly once after
            // reopen (spec §12 join step 6).
            const accounting = { consumed: { turns: 1 }, released: { turns: 1 } };
            const frozen = yield* ledger.beginChildBudgetRelease(
              BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
            );
            expect(frozen.status).toBe("releasePending");
            const released = yield* ledger.releaseChildBudget(
              ReleaseChildBudgetRequest.make({ reservationId }),
            );
            expect(released.status).toBe("released");
            expect(released.accounting).toEqual(accounting);
          }),
        );
      }),
    ),
  );

  it.effect("leaves a recovery-classifiable state at every S2 ledger failpoint", () =>
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
            if (isLedgerError(error)) {
              expect(error.cause).toBeInstanceOf(SqliteStorageFailpointError);
              if (isSqliteStorageFailpointError(error.cause)) {
                expect(error.cause.location).toBe(location);
              }
            }
          }
        };
        const reservationRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT
                reservation_id,
                status,
                child_submission_id,
                accounting_json,
                release_began_at,
                released_at
              FROM effect_agent_child_reservations
            `;
          }),
        );
        const parentMarkers = (submissionId: string) =>
          withSql(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;
              return yield* sql<Record<string, unknown>>`
                SELECT state, suspended_reason_json
                FROM effect_agent_submissions
                WHERE submission_id = ${submissionId}
              `;
            }),
          );

        const parentLane = "conversation-s2-failpoints";
        const childLane = "conversation-s2-failpoints-child";
        const reservationId = S2_FAILPOINT_RESERVATION;
        const delegationCall = toolCall("call-s2-fp");
        const { child, parent, parentClaim } = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const parent = yield* ledger.admit(
              yield* admission(parentLane, "s2-fp-parent-key", { work: "parent" }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: parent.submissionId }));
            const child = yield* ledger.admit(
              yield* admission(childLane, "s2-fp-child-key", { task: "child" }),
            );
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: child.submissionId }));
            const claim = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation(parentLane),
                producerId: TEST_PRODUCER,
              }),
            );
            if (Option.isNone(claim)) return yield* Effect.die("missing parent claim");
            return { parent, child, parentClaim: claim.value };
          }),
        );

        // reserveChildBudget: before → no row, nothing to repair; after → the reservation is
        // durable ('reserved') even though the caller never saw it; the retry replays it.
        const allocation = { turns: 2 };
        const allocationDigest = yield* digestJson(allocation).pipe(
          Effect.provide(NodeCrypto.layer),
        );
        const reserveOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.reserveChildBudget(
              ChildBudgetReservationRequest.make({
                reservationId,
                parentSubmissionId: parent.submissionId,
                parentToolCallId: delegationCall,
                ownershipToken: parentClaim.ownershipToken,
                allocation,
                allocationDigest,
              }),
            );
          }),
        );
        yield* select("ledger:child-reservation:before");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:child-reservation:before",
        );
        expect(yield* reservationRows).toEqual([]);
        yield* select("ledger:child-reservation:after");
        expectInjectedFailure(
          yield* reserveOnce.pipe(Effect.exit),
          "ledger:child-reservation:after",
        );
        const reservedRows = yield* reservationRows;
        expect(reservedRows).toHaveLength(1);
        expect(reservedRows[0]?.status).toBe("reserved");
        expect(reservedRows[0]?.child_submission_id).toBeNull();
        yield* select(undefined);
        const replayedReserve = yield* reserveOnce;
        expect(replayedReserve.replayed).toBe(true);

        // attachChildToReservation: before → no child recorded; after → the attachment is
        // durable; the retry is an idempotent no-op.
        const attachOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.attachChildToReservation(
              AttachChildToReservationRequest.make({
                reservationId,
                ownershipToken: parentClaim.ownershipToken,
                childSubmissionId: child.submissionId,
              }),
            );
          }),
        );
        yield* select("ledger:child-attach:before");
        expectInjectedFailure(yield* attachOnce.pipe(Effect.exit), "ledger:child-attach:before");
        expect((yield* reservationRows)[0]?.child_submission_id).toBeNull();
        yield* select("ledger:child-attach:after");
        expectInjectedFailure(yield* attachOnce.pipe(Effect.exit), "ledger:child-attach:after");
        expect((yield* reservationRows)[0]?.child_submission_id).toBe(child.submissionId);
        yield* select(undefined);
        const replayedAttach = yield* attachOnce;
        expect(replayedAttach.childSubmissionId).toBe(child.submissionId);

        // beginChildBudgetRelease: before → status reserved with no frozen accounting; after →
        // releasePending with the frozen decision; the retry is an idempotent no-op.
        const accounting = { consumed: { turns: 1 }, released: { turns: 1 } };
        const beginOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.beginChildBudgetRelease(
              BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
            );
          }),
        );
        yield* select("ledger:child-release-pending:before");
        expectInjectedFailure(
          yield* beginOnce.pipe(Effect.exit),
          "ledger:child-release-pending:before",
        );
        expect((yield* reservationRows)[0]?.status).toBe("reserved");
        expect((yield* reservationRows)[0]?.accounting_json).toBeNull();
        yield* select("ledger:child-release-pending:after");
        expectInjectedFailure(
          yield* beginOnce.pipe(Effect.exit),
          "ledger:child-release-pending:after",
        );
        const frozenRows = yield* reservationRows;
        expect(frozenRows[0]?.status).toBe("releasePending");
        expect(frozenRows[0]?.accounting_json).not.toBeNull();
        yield* select(undefined);
        const replayedBegin = yield* beginOnce;
        expect(replayedBegin.status).toBe("releasePending");

        // releaseChildBudget: before → still releasePending; after → released durably; the
        // retry replays the released row: the unused allocation never returns twice.
        const releaseOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.releaseChildBudget(
              ReleaseChildBudgetRequest.make({ reservationId }),
            );
          }),
        );
        yield* select("ledger:child-release:before");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:child-release:before");
        expect((yield* reservationRows)[0]?.status).toBe("releasePending");
        expect((yield* reservationRows)[0]?.released_at).toBeNull();
        yield* select("ledger:child-release:after");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:child-release:after");
        expect((yield* reservationRows)[0]?.status).toBe("released");
        expect((yield* reservationRows)[0]?.released_at).not.toBeNull();
        yield* select(undefined);
        const replayedRelease = yield* releaseOnce;
        expect(replayedRelease.status).toBe("released");

        // recordChildSettled: before → the parent stays suspended; after → the wake transition
        // is durable even though the caller never saw it; the retry answers not-waiting.
        yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const suspended = yield* ledger.suspend(
              SuspendRequest.make({
                submissionId: parent.submissionId,
                ownershipToken: parentClaim.ownershipToken,
                reason: WaitingForChildSuspension.make({
                  children: [
                    WaitingChild.make({
                      toolCallId: delegationCall,
                      childSubmissionId: child.submissionId,
                    }),
                  ],
                }),
              }),
            );
            expect(suspended).toBe("suspended");
            const childClaim = yield* ledger.claim(
              ClaimRequest.make({
                conversationId: conversation(childLane),
                producerId: TEST_PRODUCER,
              }),
            );
            if (Option.isNone(childClaim)) return yield* Effect.die("missing child claim");
            const reservation = yield* settlementReservation(
              child,
              childClaim.value.ownershipToken,
              "completed",
            );
            yield* ledger.reserveSettlement(reservation);
            yield* ledger.finalizeSettlement(
              SettlementFinalization.make({
                submissionId: child.submissionId,
                settlementId: reservation.settlementId,
              }),
            );
          }),
        );
        const notifyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            return yield* ledger.recordChildSettled(
              ChildSettledNotification.make({
                parentSubmissionId: parent.submissionId,
                childSubmissionId: child.submissionId,
              }),
            );
          }),
        );
        yield* select("ledger:child-settled:before");
        expectInjectedFailure(yield* notifyOnce.pipe(Effect.exit), "ledger:child-settled:before");
        expect((yield* parentMarkers(parent.submissionId))[0]?.state).toBe("suspended");
        yield* select("ledger:child-settled:after");
        expectInjectedFailure(yield* notifyOnce.pipe(Effect.exit), "ledger:child-settled:after");
        const wokenMarkers = yield* parentMarkers(parent.submissionId);
        expect(wokenMarkers[0]?.state).toBe("input-applied");
        expect(wokenMarkers[0]?.suspended_reason_json).toBeNull();
        yield* select(undefined);
        const replayedNotification = yield* notifyOnce;
        expect(replayedNotification).toBe("not-waiting");
      }),
    ),
  );
});

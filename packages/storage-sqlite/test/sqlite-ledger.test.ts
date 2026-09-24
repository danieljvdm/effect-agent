import {
  SqliteStorageCompatibilityError,
  SqliteStorageFailpointError,
  SqliteWriteContention,
  type SqliteStorageFailpointLocation,
} from "@effect-agent/storage-sqlite/sqlite-storage-error";
import { SqliteStorageFailpoint } from "@effect-agent/storage-sqlite/sqlite-storage-failpoint";
import { CurrentSqliteStorageVersion } from "@effect-agent/storage-sqlite/sqlite-storage-version";
import {
  ledgerLayer,
  submissionLedgerLayer,
} from "@effect-agent/storage-sqlite/sqlite-submission-ledger";
import {
  threadStoreLayer,
  storageConfigLayer,
} from "@effect-agent/storage-sqlite/sqlite-thread-store";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import type { Crypto, PlatformError } from "effect";
import {
  Cause,
  DateTime,
  Deferred,
  Effect,
  Exit,
  FileSystem,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { digestJson, EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  SubmissionSettled,
  SubmissionSettledRecord,
  UserInputRecorded,
  type PersistedJson,
  type SettlementOutcome,
} from "effect-agent/records";
import {
  AdmissionPolicyError,
  SubmissionAdmissionFence,
  AbortCommand,
  AdmissionRequest,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  ClaimJoiningRequest,
  ClaimRequest,
  IdempotencyKey,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  Principal,
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
  SuspendRequest,
  UnknownResolutionCommand,
  WaitingChild,
  WaitingForChildSuspension,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementRecordId,
  type AdmissionResult,
  type ParentLinkage,
  type OwnershipToken,
} from "effect-agent/submission-ledger";
import { submissionLedgerConformanceCases } from "effect-agent/testing/submission-ledger-conformance";
import {
  ThreadMaterialization,
  ThreadStore,
  FencedAppendRequest,
  FenceRejected,
  type AppendResult,
} from "effect-agent/thread-store";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { CurrentTransformer } from "effect/unstable/sql/Statement";

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const thread = (value: string) => id(ThreadMaterialization.fields.threadId, value);
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

const S2_FAILPOINT_RESERVATION = Schema.decodeSync(ChildReservationId)(
  "child-reservation:run-s2fp:call-1",
);

const TEST_DIGESTS = DefinitionDigests.make({
  agent: TEST_DEFINITION_DIGEST,
  model: TEST_DEFINITION_DIGEST,
  tools: TEST_DEFINITION_DIGEST,
});

const admission = Effect.fn("SqliteLedgerTest.admission")(function* (
  threadId: string,
  idempotencyKey: string,
  input: PersistedJson,
  parentLinkage?: ParentLinkage,
) {
  const inputDigest = yield* digestJson(input);

  return AdmissionRequest.make({
    threadId: thread(threadId),
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

  const payload = yield* Schema.decodeEffect(SubmissionSettledRecord)(
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
    family: "thread",
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
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/RecordId")),
      recordId,
    ),
    family: "thread",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload: UserInputRecorded.make({
      submissionId: id(SubmissionId, "submission-epoch-append"),
      kind: "user",
      input,
    }),
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/BatchId")), batchId),
    producerId: TEST_PRODUCER,
    records,
  });

const append = (
  store: ThreadStore["Service"],
  threadId: string,
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = {
    lastSequence: sequence(0),
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpoch: ProducerEpoch = epoch(1),
) =>
  store.append(
    FencedAppendRequest.make({
      threadId: thread(threadId),
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

/** ThreadStore and SubmissionLedger sharing one SqlClient over one database file. */
const combinedLayer = (filename: string) =>
  Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
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
  it.effect(
    "fences local policy in the admission transaction and replays before mutable checks",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const sql = yield* SqlClientService.SqlClient;

          yield* sql`CREATE TABLE host_admission_policy (revision TEXT NOT NULL, observations INTEGER NOT NULL)`;
          yield* sql`INSERT INTO host_admission_policy VALUES ('1', 0)`;
          let unavailable = false;

          const fence = Layer.succeed(SubmissionAdmissionFence)({
            check: (request) =>
              Effect.gen(function* () {
                if (unavailable)
                  return yield* AdmissionPolicyError.make({
                    reason: "unavailable",
                    code: "host-policy",
                  });
                yield* sql`UPDATE host_admission_policy SET observations=observations+1`.pipe(
                  Effect.mapError(() =>
                    AdmissionPolicyError.make({ reason: "unavailable", code: "host-policy" }),
                  ),
                );

                const rows = yield* sql<{
                  revision: string;
                }>`SELECT revision FROM host_admission_policy`.pipe(
                  Effect.mapError(() =>
                    AdmissionPolicyError.make({ reason: "unavailable", code: "host-policy" }),
                  ),
                );

                if (request.admissionFence?.revision !== rows[0]?.revision)
                  return yield* AdmissionPolicyError.make({
                    reason: "refused",
                    code: "stale-revision",
                  });
              }),
          });

          yield* Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            const original = AdmissionRequest.make({
              ...(yield* admission("policy-transaction", "first", {})),
              admissionGroup: "entity",
              admissionFence: { policyId: "host", key: "entity", revision: "1" },
            });

            const first = yield* ledger.admit(original);

            yield* sql`UPDATE host_admission_policy SET revision='2'`;
            expect((yield* ledger.admit(original)).submissionId).toBe(first.submissionId);

            const fresh = AdmissionRequest.make({
              ...original,
              idempotencyKey: id(IdempotencyKey, "fresh"),
            });

            expect(yield* ledger.admit(fresh).pipe(Effect.flip)).toMatchObject({
              reason: "refused",
              code: "stale-revision",
            });
            // The policy callback's SQL and the admission have one rollback boundary.
            expect(yield* sql`SELECT observations FROM host_admission_policy`).toEqual([
              { observations: 1 },
            ]);
            expect((yield* ledger.resolveAdmission(SubmissionLookupByKey.make(fresh)))._tag).toBe(
              "NotAdmitted",
            );
            unavailable = true;
            expect(yield* ledger.admit(fresh).pipe(Effect.flip)).toMatchObject({
              reason: "unavailable",
            });
            expect((yield* ledger.admit(original)).replayed).toBe(true);
          }).pipe(
            Effect.provide(
              submissionLedgerLayer.pipe(
                Layer.provide(
                  Layer.mergeAll(
                    fence,
                    Layer.succeed(SqlClientService.SqlClient)(sql),
                    storageConfigLayer({ filename }),
                    SqliteStorageFailpoint.layer,
                    NodeCrypto.layer,
                  ),
                ),
              ),
            ),
          );
        }).pipe(Effect.provide([SqliteClient.layer({ filename }), NodeCrypto.layer])),
      ),
  );

  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((filename) => withLedger(filename, conformanceCase.run)),
      );
    }
  });

  it.effect(
    "discovers control state independently of poisoned payloads and rejects invalid control identities",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const sql = yield* SqlClientService.SqlClient;
          const request = yield* admission("retained-worker", "original", { work: "retained" });
          const retained = yield* ledger.admit(request);

          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: retained.submissionId }));
          yield* sql`UPDATE effect_agent_submissions
          SET input_json = '{', worker_admission_json = '{'
          WHERE submission_id = ${retained.submissionId}`;
          const work = yield* Stream.runCollect(ledger.scanNonterminal);

          expect(work).toEqual([
            expect.objectContaining({
              submissionId: retained.submissionId,
              receiptId: retained.receiptId,
              threadId: request.threadId,
              principal: request.principal,
              idempotencyKey: request.idempotencyKey,
              deploymentId: request.deploymentId,
              queueSequence: retained.queueSequence,
              state: "ready",
            }),
          ]);
          expect(work[0]).not.toHaveProperty("inputPayload");
          expect(
            yield* ledger
              .lookup(
                SubmissionLookupById.make({
                  submissionId: retained.submissionId,
                }),
              )
              .pipe(Effect.flip),
          ).toMatchObject({ _tag: "LedgerError" });
          expect(
            yield* sql`SELECT input_json, worker_admission_json, receipt_id, state
          FROM effect_agent_submissions WHERE submission_id = ${retained.submissionId}`,
          ).toEqual([
            {
              input_json: "{",
              worker_admission_json: "{",
              receipt_id: retained.receiptId,
              state: "ready",
            },
          ]);

          yield* sql`UPDATE effect_agent_submissions SET receipt_id = ''
          WHERE submission_id = ${retained.submissionId}`;
          expect(yield* Stream.runCollect(ledger.scanNonterminal).pipe(Effect.flip)).toMatchObject({
            _tag: "LedgerError",
            operation: "ledger scan nonterminal",
          });
          expect(
            yield* sql`SELECT state FROM effect_agent_submissions
          WHERE submission_id = ${retained.submissionId}`,
          ).toEqual([{ state: "ready" }]);
        }).pipe(Effect.provide(combinedLayer(filename))),
      ),
  );

  it.effect("bumps the thread producer epoch atomically with a claim", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const ledger = yield* SubmissionLedger;
        const threadId = "thread-epoch";

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId: thread(threadId),
            producerEpoch: epoch(1),
          }),
        );

        const first = yield* append(
          store,
          threadId,
          batch("epoch-batch-1", [inputRecord("epoch-record-1", "before claim")]),
        );

        const admitted = yield* ledger.admit(
          yield* admission(threadId, "epoch-key", { work: "epoch" }),
        );

        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));

        const claim = yield* ledger.claim(
          ClaimRequest.make({
            threadId: thread(threadId),
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
          threadId,
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
          threadId,
          batch("epoch-batch-2", [inputRecord("epoch-record-2", "stale append")]),
          first,
          claim.value.producerEpoch,
        );

        expect(fenced.firstSequence).toBe(first.lastSequence + 1);
      }).pipe(Effect.provide(combinedLayer(filename))),
    ),
  );

  it.effect("preserves a later writer when unknown work is resolved after reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const lane = "thread-unknown-writer-reopen";
        const request = ClaimRequest.make({ threadId: thread(lane), producerId: TEST_PRODUCER });

        const before = yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const older = yield* ledger.admit(yield* admission(lane, "older", { work: "older" }));
            const later = yield* ledger.admit(yield* admission(lane, "later", { work: "later" }));

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: older.submissionId }));
            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: later.submissionId }));
            const first = yield* ledger.claim(request);

            if (Option.isNone(first)) return yield* Effect.die("missing older claim");
            yield* ledger.markUnknown(
              MarkUnknownRequest.make({
                submissionId: older.submissionId,
                toolCallIds: [toolCall("unknown-reopen-call")],
                reason: "ordinary Tool outcome is uncertain",
              }),
            );
            yield* TestClock.adjust("30 seconds");
            const second = yield* ledger.claim(request);

            if (Option.isNone(second)) return yield* Effect.die("missing later claim");
            expect(second.value.submissionId).toBe(later.submissionId);

            return { older, first: first.value, second: second.value };
          }),
        );

        yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.recordUnknownResolution(
              UnknownResolutionCommand.make({
                submissionId: before.older.submissionId,
                toolCallId: toolCall("unknown-reopen-call"),
                author: "operator",
                reason: "external service confirmed no effect",
                resolution: ResolutionNeverHappened.make(),
              }),
            );

            const later = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: before.second.submissionId }),
            );

            expect(later.ownership?.producerEpoch).toBe(before.second.producerEpoch);
            expect(Option.isNone(yield* ledger.claim(request))).toBe(true);
            expect(
              yield* ledger
                .renewOwnership(
                  RenewOwnershipRequest.make({
                    submissionId: before.older.submissionId,
                    ownershipToken: before.first.ownershipToken,
                  }),
                )
                .pipe(Effect.flip),
            ).toMatchObject({ _tag: "OwnershipLost", actualEpoch: before.second.producerEpoch });

            const renewal = yield* ledger.renewOwnership(
              RenewOwnershipRequest.make({
                submissionId: before.second.submissionId,
                ownershipToken: before.second.ownershipToken,
              }),
            );

            expect(renewal.ownershipToken).toBe(before.second.ownershipToken);
            yield* ledger.releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: before.second.submissionId,
                ownershipToken: renewal.ownershipToken,
              }),
            );
            const resumed = yield* ledger.claim(request);

            expect(Option.isSome(resumed)).toBe(true);
            if (Option.isSome(resumed)) {
              expect(resumed.value.submissionId).toBe(before.older.submissionId);
              expect(resumed.value.producerEpoch).toBeGreaterThan(before.second.producerEpoch);
            }
          }),
        );
      }),
    ),
  );

  it.effect("reads committed recovery state while a separate connection holds the write lock", () =>
    withTemporaryDatabase((filename) =>
      withLedger(
        filename,
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const admitted = yield* ledger.admit(yield* admission("recovery-reader", "input", {}));
          const request = RecoverySnapshotRequest.make({ submissionId: admitted.submissionId });

          yield* withSql(
            filename,
            Effect.scoped(
              Effect.gen(function* () {
                const sql = yield* SqlClientService.SqlClient;

                // Explicit SQL keeps the writer's transaction out of Effect's ambient
                // transaction context: the ledger must reserve its own reader connection.
                yield* Effect.acquireRelease(sql`BEGIN IMMEDIATE`, () =>
                  sql`ROLLBACK`.pipe(Effect.orDie),
                );
                yield* sql`
                  UPDATE effect_agent_submissions
                  SET state = 'ready', ready_at = '1970-01-01T00:00:00.001Z'
                  WHERE submission_id = ${admitted.submissionId}
                `;

                const snapshot = yield* ledger.loadRecoverySnapshot(request);

                expect(snapshot.submission.state).toBe("admitted");
                expect(snapshot.submission.readyAt).toBeUndefined();
              }),
            ),
          );

          // The completed read must release its transaction and connection reservation.
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          expect((yield* ledger.loadRecoverySnapshot(request)).submission.state).toBe("ready");
        }),
      ),
    ),
  );

  it.effect("releases recovery read transactions after typed failure and interruption", () =>
    withTemporaryDatabase((filename) =>
      withLedger(
        filename,
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const admitted = yield* ledger.admit(yield* admission("recovery-cleanup", "input", {}));
          const request = RecoverySnapshotRequest.make({ submissionId: admitted.submissionId });

          const missing = yield* ledger
            .loadRecoverySnapshot(
              RecoverySnapshotRequest.make({
                submissionId: id(RecoverySnapshotRequest.fields.submissionId, "missing-submission"),
              }),
            )
            .pipe(Effect.exit);

          expect(Exit.isFailure(missing)).toBe(true);
          if (Exit.isFailure(missing))
            expect(Cause.squash(missing.cause)).toBeInstanceOf(LedgerError);
          expect((yield* ledger.loadRecoverySnapshot(request)).submission.state).toBe("admitted");

          const paused = yield* Deferred.make<void>();
          const queries = yield* Ref.make(0);

          const reader = yield* ledger.loadRecoverySnapshot(request).pipe(
            Effect.provideService(CurrentTransformer, (statement) =>
              Ref.updateAndGet(queries, (count) => count + 1).pipe(
                Effect.flatMap((count) =>
                  count === 2
                    ? Deferred.succeed(paused, undefined).pipe(Effect.andThen(Effect.never))
                    : Effect.succeed(statement),
                ),
              ),
            ),
            Effect.forkChild,
          );

          yield* Deferred.await(paused);
          yield* Fiber.interrupt(reader);
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          expect((yield* ledger.loadRecoverySnapshot(request)).submission.state).toBe("ready");
        }),
      ),
    ),
  );

  it.effect("classifies cross-connection write contention as retryable typed contention", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withLedger(
          filename,
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.admit(yield* admission("thread-busy", "busy-key-1", { step: 1 }));
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
                  yield* admission("thread-busy", "busy-key-2", { step: 2 }),
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

            return yield* ledger.admit(yield* admission("thread-busy", "busy-key-2", { step: 2 }));
          }),
        );

        expect(recovered.replayed).toBe(false);
      }),
    ),
  );

  it.effect("rejects unsupported older and newer versions with preservation guidance", () =>
    Effect.forEach([1, CurrentSqliteStorageVersion + 1], (storedVersion) =>
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
              expect(error.supportedVersion).toBe(CurrentSqliteStorageVersion);
              expect(error.message).toContain("Keep the original file");
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

  it.effect("recovers ledger identity and ownership after lost acknowledgements", () =>
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
              ORDER BY thread_id, queue_sequence
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

        const threadRows = withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT thread_id, producer_epoch
              FROM effect_agent_threads
            `;
          }),
        );

        const lane = "thread-failpoints";

        const admitOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.admit(yield* admission(lane, "failpoint-key", { work: "fail" }));
          }),
        );

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

        const markReadyOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          }),
        );

        yield* select(undefined);
        yield* markReadyOnce;

        const claimOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.claim(
              ClaimRequest.make({ threadId: thread(lane), producerId: TEST_PRODUCER }),
            );
          }),
        );

        yield* select("ledger:claim:after");
        expectInjectedFailure(yield* claimOnce.pipe(Effect.exit), "ledger:claim:after");
        const orphanedOwnership = yield* ownershipRows;

        expect(orphanedOwnership).toHaveLength(1);
        expect(orphanedOwnership[0]?.producer_epoch).toBe(1);
        expect(yield* threadRows).toEqual([{ thread_id: lane, producer_epoch: 1 }]);
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

        yield* select(undefined);
        yield* markInputOnce;

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

        const abortLane = "thread-failpoints-abort";

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

        yield* select("ledger:request-abort:after");
        expectInjectedFailure(yield* abortOnce.pipe(Effect.exit), "ledger:request-abort:after");
        const abortIntents = yield* abortRows;

        expect(abortIntents).toHaveLength(1);
        yield* select(undefined);
        const intent = yield* abortOnce;

        expect(intent.reason).toBe("failpoint abort");

        const abortClaim = yield* failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.claim(
              ClaimRequest.make({
                threadId: thread(abortLane),
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

  it.effect(
    "preserves join, approval and unknown-operation evidence after lost acknowledgements",
    () =>
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
              ORDER BY thread_id, queue_sequence
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

          const lane = "thread-p5-failpoints";

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
                ClaimRequest.make({ threadId: thread(lane), producerId: TEST_PRODUCER }),
              );

              if (Option.isNone(claim)) return yield* Effect.die("missing host claim");

              return { host, hostClaim: claim.value, queued, queuedSecond };
            }),
          );

          const claimJoiningOnce = failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              return yield* ledger.claimJoining(
                ClaimJoiningRequest.make({
                  threadId: thread(lane),
                  hostSubmissionId: host.submissionId,
                  ownershipToken: hostClaim.ownershipToken,
                  maxCount: 1,
                }),
              );
            }),
          );

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

          yield* select(undefined);
          yield* markJoinedOnce;

          const revertOnce = failingLedger(
            Effect.gen(function* () {
              const ledger = yield* SubmissionLedger;

              yield* ledger.revertJoining(
                RevertJoiningRequest.make({ submissionId: queuedSecond.submissionId }),
              );
            }),
          );

          yield* select(undefined);
          yield* revertOnce;

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
          expect(markerFor(yield* submissionMarkers, host.submissionId)?.state).toBe(
            "input-applied",
          );

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

  it.effect("preserves child accounting and wake identity after lost acknowledgements", () =>
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

        const parentLane = "thread-s2-failpoints";
        const childLane = "thread-s2-failpoints-child";
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
                threadId: thread(parentLane),
                producerId: TEST_PRODUCER,
              }),
            );

            if (Option.isNone(claim)) return yield* Effect.die("missing parent claim");

            return { parent, child, parentClaim: claim.value };
          }),
        );

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

        yield* select(undefined);
        const replayedAttach = yield* attachOnce;

        expect(replayedAttach.childSubmissionId).toBe(child.submissionId);

        const accounting = { consumed: { turns: 1 }, released: { turns: 1 } };

        const beginOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.beginChildBudgetRelease(
              BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
            );
          }),
        );

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

        const releaseOnce = failingLedger(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;

            return yield* ledger.releaseChildBudget(
              ReleaseChildBudgetRequest.make({ reservationId }),
            );
          }),
        );

        yield* select("ledger:child-release:after");
        expectInjectedFailure(yield* releaseOnce.pipe(Effect.exit), "ledger:child-release:after");
        expect((yield* reservationRows)[0]?.status).toBe("released");
        expect((yield* reservationRows)[0]?.released_at).not.toBeNull();
        yield* select(undefined);
        const replayedRelease = yield* releaseOnce;

        expect(replayedRelease.status).toBe("released");

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
                threadId: thread(childLane),
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
import { SubmissionId } from "effect-agent/identifiers";

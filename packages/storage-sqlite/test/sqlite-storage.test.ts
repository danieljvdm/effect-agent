import {
  SqliteStorageConfig,
  SqliteStorageConfigValue,
} from "@effect-agent/storage-sqlite/sqlite-storage-config";
import {
  SqliteStorageFailpointError,
  type SqliteStorageFailpointLocation,
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteWriteContention,
} from "@effect-agent/storage-sqlite/sqlite-storage-error";
import { ledgerLayer } from "@effect-agent/storage-sqlite/sqlite-submission-ledger";
import { threadStoreLayer, layer } from "@effect-agent/storage-sqlite/sqlite-thread-store";
import { SqliteStorageFailpointTestControl } from "@effect-agent/storage-sqlite/testing/sqlite-storage-failpoint-testing";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, describe, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import {
  DateTime,
  Cause,
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
import { EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  ProducerEpoch,
  RunCompleted,
  UserInputRecorded,
  type CanonicalRecordPayload,
} from "effect-agent/records";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "effect-agent/testing/thread-store-conformance";
import {
  ThreadCheckpoint,
  ThreadTailRequest,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadObservation,
  ThreadRead,
  ThreadStore,
  ThreadStoreError,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  SaveRecoveryCheckpointRequest,
  type AppendResult,
} from "effect-agent/thread-store";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { seedCheckpoint, assertCheckpoint } from "../../../test/fixtures/checkpoints.ts";
import { snapshotStore } from "../../../test/fixtures/storage-upgrade.ts";

const threadId = Schema.decodeSync(ThreadMaterialization.fields.threadId)("thread-sqlite-1");
const secondThreadId = Schema.decodeSync(ThreadMaterialization.fields.threadId)("thread-sqlite-2");
const runId = Schema.decodeSync(RunCompleted.fields.runId)("run-sqlite-1");
const submissionId = Schema.decodeSync(SubmissionId)("submission-sqlite-1");

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
const epoch = (value: number) => Schema.decodeSync(ProducerEpoch)(value);

const isThreadStoreError = Schema.is(ThreadStoreError);

const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const canonicalRecord = (recordId: string, payload: CanonicalRecordPayload): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/RecordId")),
      recordId,
    ),
    family: "thread",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/DeploymentId")),
      "deployment-sqlite",
    ),
    payload,
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/BatchId")), batchId),
    producerId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/ProducerId")),
      "producer-sqlite",
    ),
    records,
  });

const inputRecord = (recordId: string, input: string): CanonicalRecord =>
  canonicalRecord(
    recordId,
    UserInputRecorded.make({
      submissionId,
      kind: "user",
      runId,
      input,
    }),
  );

const append = (
  store: ThreadStore["Service"],
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = {
    lastSequence: sequence(0),
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpoch: ProducerEpoch = epoch(1),
) =>
  store.append(
    FencedAppendRequest.make({
      threadId,
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch,
    }),
  );

const withStorage = <A, E>(filename: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(effect, layer({ filename, observationPollInterval: 1 }));

const withSql = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, SqliteClient.layer({ filename }));

const explicitTestStorageLayer = (filename: string) =>
  threadStoreLayer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Layer.succeed(SqliteStorageConfig)(
          SqliteStorageConfigValue.make({
            observationPollInterval: 1,
            busyTimeout: 5_000,
            ownershipLeaseDuration: 30_000,
            verifyOnOpen: false,
          }),
        ),
        SqliteStorageFailpointTestControl.layer,
        SqliteClient.layer({ filename }),
        NodeCrypto.layer,
      ),
    ),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-storage-sqlite-",
      });

      return yield* use(`${directory}/thread.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SqliteThreadStore", () => {
  it.effect("rejects canonical records beyond the captured export tail", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }));
        yield* store.append(
          FencedAppendRequest.make({
            threadId,
            producerEpoch: epoch(1),
            expectedTailSequence: sequence(0),
            expectedTailDigest: EMPTY_TAIL_DIGEST,
            batch: batch("bad-export-tail", [inputRecord("bad-export-tail", "Kyoto")]),
          }),
        );
        yield* withSql(
          filename,
          Effect.flatMap(
            SqlClientService.SqlClient,
            (sql) =>
              sql`UPDATE effect_agent_threads SET tail_sequence=0 WHERE thread_id=${threadId}`,
          ),
        );
        expect(
          yield* store.export(ThreadExportRequest.make({ threadId })).pipe(Effect.flip),
        ).toMatchObject({
          _tag: "ThreadStoreError",
          message: expect.stringContaining("beyond the captured thread tail"),
        });
      }).pipe(Effect.provide(layer({ filename }))),
    ),
  );

  it.effect("isolates, fences and replaces one durable recovery checkpoint across reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const checkpoint = yield* Effect.gen(function* () {
          const store = yield* ThreadStore;

          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );

          const empty = ThreadCheckpoint.make({
            schemaVersion: 1,
            threadId,
            throughSequence: sequence(0),
            tailDigest: EMPTY_TAIL_DIGEST,
            engineVersion: "recovery-test",
            agentDefinitionDigest: EMPTY_TAIL_DIGEST,
            modelDigest: EMPTY_TAIL_DIGEST,
            toolDigest: EMPTY_TAIL_DIGEST,
            state: {},
            createdAt: at(2),
          });

          yield* store.checkpoints!.save(SaveCheckpointRequest.make({ checkpoint: empty }));
          yield* store.recoveryCheckpoints!.save(
            SaveRecoveryCheckpointRequest.make({ checkpoint: empty, producerEpoch: epoch(1) }),
          );

          const appended = yield* store.append(
            FencedAppendRequest.make({
              threadId,
              producerEpoch: epoch(1),
              expectedTailSequence: sequence(0),
              expectedTailDigest: EMPTY_TAIL_DIGEST,
              batch: batch("recovery-batch", [
                inputRecord("recovery-1", "Kyoto"),
                inputRecord("recovery-2", "Nara"),
              ]),
            }),
          );

          const checkpoint = ThreadCheckpoint.make({
            ...empty,
            throughSequence: appended.lastSequence,
            tailDigest: appended.tailDigest,
            state: { version: 1 },
          });

          const save = (checkpoint: ThreadCheckpoint, producerEpoch = epoch(1)) =>
            store.recoveryCheckpoints!.save(
              SaveRecoveryCheckpointRequest.make({ checkpoint, producerEpoch }),
            );

          yield* save(checkpoint);
          yield* save(empty);
          expect(
            yield* store.recoveryCheckpoints!.load(LoadCheckpointRequest.make({ threadId })),
          ).toEqual(Option.some(checkpoint));
          expect(
            Option.isNone(
              yield* store.recoveryCheckpoints!.load(
                LoadCheckpointRequest.make({ threadId, atOrBeforeSequence: sequence(0) }),
              ),
            ),
          ).toBe(true);
          expect(
            yield* save(
              ThreadCheckpoint.make({ ...checkpoint, throughSequence: sequence(1) }),
            ).pipe(Effect.flip),
          ).toMatchObject({ _tag: "CheckpointRejected", reason: "digest-mismatch" });
          expect(
            yield* save(
              ThreadCheckpoint.make({ ...checkpoint, throughSequence: sequence(3) }),
            ).pipe(Effect.flip),
          ).toMatchObject({ _tag: "CheckpointRejected", reason: "ahead-of-tail" });
          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(2) }),
          );
          expect(yield* save(checkpoint).pipe(Effect.flip)).toMatchObject({
            _tag: "FenceRejected",
            actualEpoch: 2,
            attemptedEpoch: 1,
          });

          const replacement = ThreadCheckpoint.make({
            ...checkpoint,
            state: { version: 2 },
            createdAt: at(3),
          });

          yield* save(replacement, epoch(2));
          expect(yield* store.checkpoints!.load(LoadCheckpointRequest.make({ threadId }))).toEqual(
            Option.some(empty),
          );
          expect(
            (yield* store.export(ThreadExportRequest.make({ threadId }))).records,
          ).toHaveLength(2);

          return replacement;
        }).pipe(Effect.provide(layer({ filename })));

        const restored = yield* ThreadStore.pipe(
          Effect.flatMap((store) =>
            store.recoveryCheckpoints!.load(
              LoadCheckpointRequest.make({ threadId: checkpoint.threadId }),
            ),
          ),
          Effect.provide(layer({ filename })),
        );

        expect(restored).toEqual(Option.some(checkpoint));

        const rows = yield* withSql(
          filename,
          Effect.flatMap(
            SqlClientService.SqlClient,
            (sql) => sql`SELECT COUNT(*) AS count FROM effect_agent_recovery_checkpoints`,
          ),
        );

        expect(rows).toEqual([{ count: 1 }]);
      }),
    ),
  );

  for (const corruption of ["json", "thread"]) {
    it.effect(`rejects ${corruption} recovery cache corruption and permits same-tail repair`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const store = yield* ThreadStore;

          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );

          const checkpoint = ThreadCheckpoint.make({
            schemaVersion: 1,
            threadId,
            throughSequence: sequence(0),
            tailDigest: EMPTY_TAIL_DIGEST,
            engineVersion: "recovery-test",
            agentDefinitionDigest: EMPTY_TAIL_DIGEST,
            modelDigest: EMPTY_TAIL_DIGEST,
            toolDigest: EMPTY_TAIL_DIGEST,
            state: {},
            createdAt: at(2),
          });

          const save = store.recoveryCheckpoints!.save(
            SaveRecoveryCheckpointRequest.make({ checkpoint, producerEpoch: epoch(1) }),
          );

          yield* save;
          const encoded = yield* Schema.encodeEffect(ThreadCheckpoint)(checkpoint);

          const json =
            corruption === "json"
              ? "{"
              : JSON.stringify({
                  ...encoded,
                  ...(corruption === "thread" ? { threadId: "foreign" } : {}),
                });

          const storedSequence = 0;

          yield* withSql(
            filename,
            Effect.flatMap(
              SqlClientService.SqlClient,
              (sql) =>
                sql`UPDATE effect_agent_recovery_checkpoints SET checkpoint_json=${json}, through_sequence=${storedSequence} WHERE thread_id=${threadId}`,
            ),
          );
          expect(
            yield* store
              .recoveryCheckpoints!.load(LoadCheckpointRequest.make({ threadId }))
              .pipe(Effect.flip),
          ).toMatchObject({ _tag: "CheckpointRejected", reason: "corrupt" });
          yield* save;
          expect(
            yield* store.recoveryCheckpoints!.load(LoadCheckpointRequest.make({ threadId })),
          ).toEqual(Option.some(checkpoint));
          expect(
            (yield* store.inspectTail(ThreadTailRequest.make({ threadId }))).tailSequence,
          ).toBe(0);
        }).pipe(Effect.provide(layer({ filename }))),
      ),
    );
  }

  for (const [location, mode] of [
    ["save-recovery-checkpoint:before", "failure"],
    ["save-recovery-checkpoint:after", "interrupt"],
  ] as const) {
    it.effect(`reopens safely after ${mode} at ${location}`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const save = Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );

            const checkpoint = ThreadCheckpoint.make({
              schemaVersion: 1,
              threadId,
              throughSequence: sequence(0),
              tailDigest: EMPTY_TAIL_DIGEST,
              engineVersion: "recovery-test",
              agentDefinitionDigest: EMPTY_TAIL_DIGEST,
              modelDigest: EMPTY_TAIL_DIGEST,
              toolDigest: EMPTY_TAIL_DIGEST,
              state: {},
              createdAt: at(2),
            });

            yield* store.recoveryCheckpoints!.save(
              SaveRecoveryCheckpointRequest.make({ checkpoint, producerEpoch: epoch(1) }),
            );
          });

          const entered = yield* Deferred.make<void>();

          const failed = yield* Effect.scoped(
            Effect.gen(function* () {
              const fiber = yield* save.pipe(
                Effect.provide(
                  layer({
                    filename,
                    failpoint: (point) =>
                      point !== location
                        ? Effect.void
                        : Deferred.succeed(entered, undefined).pipe(
                            Effect.andThen(
                              mode === "failure"
                                ? SqliteStorageFailpointError.make({ location })
                                : Effect.interrupt,
                            ),
                          ),
                  }),
                ),
                Effect.timeout("1 second"),
                Effect.forkChild,
              );

              yield* Deferred.await(entered);

              return yield* Fiber.await(fiber);
            }),
          );

          expect(Exit.isFailure(failed)).toBe(true);
          yield* Effect.gen(function* () {
            const store = yield* ThreadStore;

            expect(
              Option.isSome(
                yield* store.recoveryCheckpoints!.load(LoadCheckpointRequest.make({ threadId })),
              ),
            ).toBe(location === "save-recovery-checkpoint:after");
            expect(
              (yield* store.inspectTail(ThreadTailRequest.make({ threadId }))).tailSequence,
            ).toBe(0);
          }).pipe(Effect.provide(layer({ filename })));
        }),
      ),
    );
  }

  for (const historical of [false, true]) {
    it.effect(
      `reopens ${historical ? "historical" : "metadata-free"} checkpoints without rewriting storage`,
      () =>
        withTemporaryDatabase((filename) =>
          Effect.gen(function* () {
            yield* seedCheckpoint(historical).pipe(
              Effect.provide(
                Layer.mergeAll(layer({ filename }), ledgerLayer({ filename }), NodeCrypto.layer),
              ),
            );
            const before = yield* snapshotStore;
            const sql = yield* SqlClientService.SqlClient;
            const version = yield* sql`PRAGMA user_version`;

            for (const verifyOnOpen of [true]) {
              yield* Effect.gen(function* () {
                yield* ThreadStore;
                expect(yield* snapshotStore).toEqual(before);
                yield* assertCheckpoint(historical);
                expect(yield* snapshotStore).toEqual(before);
                expect(yield* sql`PRAGMA user_version`).toEqual(version);
              }).pipe(Effect.provide(layer({ filename, verifyOnOpen })));
            }
          }).pipe(Effect.provide(SqliteClient.layer({ filename }))),
        ),
    );
  }

  for (const corruption of ["thread"]) {
    it.effect(`rejects checkpoint ${corruption} metadata that disagrees with its row`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const store = yield* ThreadStore;
          const sql = yield* SqlClientService.SqlClient;

          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );

          yield* append(
            store,
            batch("checkpoint-metadata", [inputRecord("checkpoint-metadata-input", "Kyoto")]),
          );

          const checkpoint = ThreadCheckpoint.make({
            schemaVersion: 1,
            threadId,
            throughSequence: sequence(0),
            tailDigest: EMPTY_TAIL_DIGEST,
            state: {},
            createdAt: at(2),
          });

          yield* store.checkpoints!.save(SaveCheckpointRequest.make({ checkpoint }));

          const corrupted = ThreadCheckpoint.make({
            ...checkpoint,
            ...(corruption === "thread" ? { threadId: secondThreadId } : {}),
          });

          const corruptedJson = JSON.stringify(
            yield* Schema.encodeEffect(ThreadCheckpoint)(corrupted),
          );

          const rowDigest = EMPTY_TAIL_DIGEST;

          yield* sql`
            UPDATE effect_agent_checkpoints
            SET checkpoint_json = ${corruptedJson}, tail_digest = ${rowDigest}
            WHERE thread_id = ${threadId} AND through_sequence = 0
          `;

          const loaded = yield* store
            .checkpoints!.load(
              LoadCheckpointRequest.make({ threadId, atOrBeforeSequence: sequence(0) }),
            )
            .pipe(Effect.exit);

          expect(Exit.isFailure(loaded)).toBe(true);
          if (Exit.isFailure(loaded)) {
            const error = Cause.squash(loaded.cause);

            expect(error).toBeInstanceOf(ThreadStoreError);
            if (isThreadStoreError(error)) expect(error.operation).toBe("load checkpoint");
          }
        }).pipe(Effect.provide(explicitTestStorageLayer(filename))),
      ),
    );
  }

  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of [
      ...threadStoreConformanceCases,
      ...threadCheckpointConformanceCases,
    ]) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((filename) => withStorage(filename, conformanceCase.run)),
      );
    }
  });

  it.effect("rejects an unsupported storage version before creating canonical tables", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`PRAGMA user_version = 99`;
          }),
        );

        const opened = yield* withStorage(filename, ThreadStore).pipe(Effect.exit);

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          expect(Cause.squash(opened.cause)).toBeInstanceOf(SqliteStorageCompatibilityError);
        }

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
  );

  it.effect("fails clearly on corrupt current-version rows without mutating the log", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );
            yield* append(store, batch("corrupt-1", [inputRecord("corrupt-record-1", "Osaka")]));
          }),
        );
        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`
              UPDATE effect_agent_canonical_records
              SET record_json = '{"schemaVersion":2}'
              WHERE thread_id = ${threadId}
                AND sequence = 1
            `;
          }),
        );

        // The opt-in integrity scan refuses to open the corrupt database.
        const verified = yield* ThreadStore.pipe(
          Effect.provide(layer({ filename, observationPollInterval: 1, verifyOnOpen: true })),
          Effect.exit,
        );

        expect(Exit.isFailure(verified)).toBe(true);
        if (Exit.isFailure(verified)) {
          expect(Cause.squash(verified.cause)).toBeInstanceOf(SqliteStorageCorruptionError);
        }

        // The default lazy open succeeds; the corrupt row fails clearly at first decode.
        const lazyRead = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            return yield* store
              .read(ThreadRead.make({ threadId, limit: 1_024 }))
              .pipe(Stream.runCollect);
          }),
        ).pipe(Effect.exit);

        expect(Exit.isFailure(lazyRead)).toBe(true);
        if (Exit.isFailure(lazyRead)) {
          const error = Cause.squash(lazyRead.cause);

          expect(error).toBeInstanceOf(ThreadStoreError);
          if (isThreadStoreError(error)) {
            expect(error.operation).toBe("decode canonical record");
          }
        }

        const rows = yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            return yield* sql<Record<string, unknown>>`
              SELECT sequence, record_json
              FROM effect_agent_canonical_records
              WHERE thread_id = ${threadId}
            `;
          }),
        );

        expect(rows).toEqual([{ sequence: 1, record_json: '{"schemaVersion":2}' }]);
      }),
    ),
  );

  it.effect("exports one transactionally consistent snapshot during a concurrent append", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const first = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );

            return yield* append(
              store,
              batch("snapshot-1", [inputRecord("snapshot-record-1", "before")]),
            );
          }),
        );

        const exportStarted = yield* Deferred.make<void>();
        const releaseExport = yield* Deferred.make<void>();

        const exportFiber = yield* Effect.provide(
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            return yield* store.export(ThreadExportRequest.make({ threadId }));
          }),
          layer({
            filename,
            observationPollInterval: 1,
            failpoint: (location) =>
              location === "export:after-thread-read"
                ? Deferred.succeed(exportStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseExport)),
                    Effect.asVoid,
                  )
                : Effect.void,
          }),
        ).pipe(Effect.forkChild);

        yield* Deferred.await(exportStarted);
        yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* append(
              store,
              batch("snapshot-2", [inputRecord("snapshot-record-2", "after")]),
              first,
            );
          }),
        );
        yield* Deferred.succeed(releaseExport, undefined);

        const snapshot = yield* Fiber.join(exportFiber);

        expect(snapshot.tailSequence).toBe(first.lastSequence);
        expect(snapshot.tailDigest).toBe(first.tailDigest);
        expect(snapshot.records).toHaveLength(1);

        const current = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            return yield* store.export(ThreadExportRequest.make({ threadId }));
          }),
        );

        expect(current.records).toHaveLength(2);
      }),
    ),
  );

  it.effect("wakes a live observer whose poll found nothing once a new batch commits", () =>
    withTemporaryDatabase((filename) =>
      withStorage(
        filename,
        Effect.gen(function* () {
          const store = yield* ThreadStore;

          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );

          const observerFiber = yield* store
            .observe(ThreadObservation.make({ threadId }))
            .pipe(Stream.take(1), Stream.runCollect, Effect.forkChild);

          // Let the observer reach its first empty poll before anything is committed.
          yield* Effect.yieldNow;

          yield* append(store, batch("live-1", [inputRecord("live-record-1", "first")]));

          // Drive the TestClock until the sleeping poll wakes and sees the new batch. The
          // adjust loop yields each step, so the observer always progresses deterministically.
          const observed = yield* Fiber.join(observerFiber).pipe(
            Effect.raceFirst(
              TestClock.adjust(1).pipe(Effect.andThen(Effect.yieldNow), Effect.forever),
            ),
          );

          expect(observed.map((record) => record.record.recordId)).toEqual(["live-record-1"]);
        }),
      ),
    ),
  );

  it.effect("classifies cross-connection write contention as retryable typed contention", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const first = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );

            return yield* append(store, batch("busy-1", [inputRecord("busy-record-1", "before")]));
          }),
        );

        const contendedBatch = batch("busy-2", [inputRecord("busy-record-2", "after")]);

        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            // Hold the write lock on a separate connection, as a transiently coexisting
            // producer would.
            yield* sql`BEGIN IMMEDIATE`;

            const contended = yield* Effect.provide(
              Effect.gen(function* () {
                const store = yield* ThreadStore;

                return yield* append(store, contendedBatch, first);
              }),
              layer({ filename, observationPollInterval: 1, busyTimeout: 0 }),
            ).pipe(Effect.exit);

            yield* sql`ROLLBACK`;

            expect(Exit.isFailure(contended)).toBe(true);
            if (Exit.isFailure(contended)) {
              const error = Cause.squash(contended.cause);

              expect(error).toBeInstanceOf(ThreadStoreError);
              if (isThreadStoreError(error)) {
                expect(error.cause).toBeInstanceOf(SqliteWriteContention);
              }
            }
          }),
        );

        // Once the competing writer releases the lock, the identical append commits.
        const recovered = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            return yield* append(store, contendedBatch, first);
          }),
        );

        expect(recovered.replayed).toBe(false);
        expect(recovered.firstSequence).toBe(first.lastSequence + 1);
      }),
    ),
  );

  it.effect(
    "rolls back partial appends and replays a committed append after acknowledgement loss",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const active = yield* Ref.make<SqliteStorageFailpointLocation | undefined>(undefined);

          const withFailpoints = <A, E>(effect: Effect.Effect<A, E, ThreadStore>) =>
            Effect.provide(
              effect,
              layer({
                filename,
                observationPollInterval: 1,
                failpoint: (location) =>
                  Ref.get(active).pipe(
                    Effect.flatMap((selected) =>
                      selected === location
                        ? Effect.fail(SqliteStorageFailpointError.make({ location }))
                        : Effect.void,
                    ),
                  ),
              }),
            );

          const select = (location: SqliteStorageFailpointLocation | undefined) =>
            Ref.set(active, location);

          yield* withFailpoints(
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              yield* store.materialize(
                ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
              );
            }),
          );

          const firstBatch = batch("failpoint-append", [
            inputRecord("failpoint-record", "Sapporo"),
          ]);

          yield* Effect.forEach(
            [
              "append:after-batch-insert",
              "append:after-record-insert",
              "append:after-tail-update",
            ] as const,
            (location) =>
              Effect.gen(function* () {
                yield* select(location);

                const exit = yield* withFailpoints(
                  Effect.gen(function* () {
                    const store = yield* ThreadStore;

                    yield* append(store, firstBatch);
                  }),
                ).pipe(Effect.exit);

                expect(Exit.isFailure(exit)).toBe(true);
                if (Exit.isFailure(exit)) {
                  const error = Cause.squash(exit.cause);

                  expect(error).toBeInstanceOf(ThreadStoreError);
                  if (isThreadStoreError(error)) {
                    expect(error.operation).toBe("append canonical batch");
                    expect(error.message).toContain(location);
                  }
                }
                yield* select(undefined);

                const exported = yield* withFailpoints(
                  Effect.gen(function* () {
                    const store = yield* ThreadStore;

                    return yield* store.export(ThreadExportRequest.make({ threadId }));
                  }),
                );

                expect(exported.records).toEqual([]);
                expect(exported.tailSequence).toBe(0);
                expect(exported.tailDigest).toBe(EMPTY_TAIL_DIGEST);
              }),
          );

          yield* select("append:after");
          expect(
            Exit.isFailure(
              yield* withFailpoints(
                Effect.gen(function* () {
                  const store = yield* ThreadStore;

                  yield* append(store, firstBatch);
                }),
              ).pipe(Effect.exit),
            ),
          ).toBe(true);
          yield* select(undefined);

          const recoveredAppend = yield* withFailpoints(
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              return yield* append(store, firstBatch);
            }),
          );

          expect(recoveredAppend.replayed).toBe(true);
        }),
      ),
  );
});
import { SubmissionId } from "effect-agent/identifiers";

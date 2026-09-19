import {
  PostgresStorageConfig,
  PostgresStorageConfigValue,
} from "@effect-agent/storage-postgres/postgres-storage-config";
import {
  PostgresStorageCompatibilityError,
  PostgresStorageCorruptionError,
  PostgresStorageFailpointError,
  type PostgresStorageFailpointLocation,
  PostgresWriteContention,
} from "@effect-agent/storage-postgres/postgres-storage-error";
import * as PostgresThreadStore from "@effect-agent/storage-postgres/postgres-thread-store";
import { PostgresStorageFailpointTestControl } from "@effect-agent/storage-postgres/testing/postgres-storage-failpoint-testing";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  DateTime,
  Effect,
  Exit,
  Layer,
  Option,
  Redacted,
  Ref,
  Schema,
  Stream,
} from "effect";
import { EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { SubmissionId } from "effect-agent/identifiers";
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
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  ThreadCheckpoint,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadRead,
  ThreadStore,
  ThreadStoreError,
  type AppendResult,
} from "effect-agent/thread-store";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  clientLayer,
  singleConnectionServices,
  whileHoldingWriterLock,
  withTemporaryDatabase,
} from "./harness.ts";

const threadId = Schema.decodeSync(ThreadMaterialization.fields.threadId)("thread-postgres-1");
const runId = Schema.decodeSync(RunCompleted.fields.runId)("run-postgres-1");
const submissionId = Schema.decodeSync(SubmissionId)("submission-postgres-1");

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
const epoch = (value: number) => Schema.decodeSync(ProducerEpoch)(value);
const isThreadStoreError = Schema.is(ThreadStoreError);
const isCompatibilityError = Schema.is(PostgresStorageCompatibilityError);

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
      "deployment-postgres",
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
      "producer-postgres",
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

const withStorage = <A, E>(url: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(
    effect,
    PostgresThreadStore.layer({ client: { url: Redacted.make(url) }, observationPollInterval: 1 }),
  );

const withVerifiedStorage = <A, E>(url: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(
    effect,
    PostgresThreadStore.layer({
      client: { url: Redacted.make(url) },
      observationPollInterval: 1,
      verifyOnOpen: true,
    }),
  );

const withSql = <A, E>(url: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, clientLayer(url));

const storageTables = (url: string) =>
  withSql(
    url,
    Effect.gen(function* () {
      const sql = yield* SqlClientService.SqlClient;

      const rows = yield* sql<Record<string, unknown>>`
        SELECT c.relname AS name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND starts_with(c.relname, 'effect_agent_')
        ORDER BY c.relname
      `;

      return rows.map((row) => row.name);
    }),
  );

const explicitTestStorageLayer = (url: string) =>
  PostgresThreadStore.layerWithServices.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Layer.succeed(PostgresStorageConfig)(
          PostgresStorageConfigValue.make({
            observationPollInterval: 1,
            lockTimeout: 5_000,
            ownershipLeaseDuration: 30_000,
            verifyOnOpen: false,
            schema: "public",
          }),
        ),
        PostgresStorageFailpointTestControl.layer,
        clientLayer(url),
        NodeCrypto.layer,
      ),
    ),
  );

const singleConnectionStore = (url: string, lockTimeout: number) =>
  PostgresThreadStore.layerWithServices.pipe(
    Layer.provide(singleConnectionServices(url, lockTimeout)),
  );

describe("PostgresThreadStore faults", () => {
  it.effect("supports explicit configuration and controllable failpoint services", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const config = yield* PostgresStorageConfig;
        const failpoints = yield* PostgresStorageFailpointTestControl;
        const store = yield* ThreadStore;

        expect(config).toMatchObject({
          observationPollInterval: 1,
          lockTimeout: 5_000,
          verifyOnOpen: false,
        });
        yield* failpoints.setHandler((location) =>
          location === "materialize:before"
            ? Effect.fail(PostgresStorageFailpointError.make({ location }))
            : Effect.void,
        );

        const injected = yield* store
          .materialize(
            ThreadMaterialization.make({
              threadId,
              producerEpoch: epoch(1),
            }),
          )
          .pipe(Effect.exit);

        expect(Exit.isFailure(injected)).toBe(true);

        yield* failpoints.clear;
        yield* store.materialize(
          ThreadMaterialization.make({
            threadId,
            producerEpoch: epoch(1),
          }),
        );
      }).pipe(Effect.provide(explicitTestStorageLayer(url))),
    ),
  );

  it.effect("rejects an unsupported storage version without touching canonical tables", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* withStorage(url, ThreadStore);

        const tablesBefore = yield* storageTables(url);

        yield* withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`UPDATE effect_agent_storage_version SET version = 999`;
          }),
        );

        const opened = yield* withStorage(url, ThreadStore).pipe(Effect.exit);

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          const error = Cause.squash(opened.cause);

          expect(error).toBeInstanceOf(PostgresStorageCompatibilityError);
          if (isCompatibilityError(error)) {
            expect(error.actualVersion).toBe(999);
          }
        }

        expect(yield* storageTables(url)).toEqual(tablesBefore);
        expect(
          yield* withSql(
            url,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql<Record<string, unknown>>`
                SELECT version FROM effect_agent_storage_version
              `;
            }),
          ),
        ).toEqual([{ version: 999 }]);
      }),
    ),
  );

  it.effect("refuses an unversioned foreign table without creating the rest of the schema", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* withSql(
          url,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;

            yield* sql`CREATE TABLE effect_agent_threads (thread_id TEXT PRIMARY KEY NOT NULL)`;
          }),
        );

        const opened = yield* withStorage(url, ThreadStore).pipe(Effect.exit);

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          const error = Cause.squash(opened.cause);

          expect(error).toBeInstanceOf(PostgresStorageCompatibilityError);
          if (isCompatibilityError(error)) {
            expect(error.actualVersion).toBe(0);
          }
        }

        expect(yield* storageTables(url)).toEqual(["effect_agent_threads"]);
      }),
    ),
  );

  it.effect("fails clearly on corrupt current-version rows without mutating the log", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        yield* withStorage(
          url,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );
            yield* append(store, batch("corrupt-1", [inputRecord("corrupt-record-1", "Osaka")]));
          }),
        );
        yield* withSql(
          url,
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
        const verified = yield* withVerifiedStorage(url, ThreadStore).pipe(Effect.exit);

        expect(Exit.isFailure(verified)).toBe(true);
        if (Exit.isFailure(verified)) {
          expect(Cause.squash(verified.cause)).toBeInstanceOf(PostgresStorageCorruptionError);
        }

        // The default lazy open succeeds; the corrupt row fails clearly at first decode.
        const lazyRead = yield* withStorage(
          url,
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
          url,
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

  it.effect("classifies cross-connection write contention as retryable typed contention", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const first = yield* withStorage(
          url,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            yield* store.materialize(
              ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
            );

            return yield* append(store, batch("busy-1", [inputRecord("busy-record-1", "before")]));
          }),
        );

        const contendedBatch = batch("busy-2", [inputRecord("busy-record-2", "after")]);

        // Opening a store takes the writer lock itself, so the competing producer must arrive
        // after the store is open. `lock_timeout = 0` disables the bound in Postgres, so the
        // shortest bounded wait is used instead.
        // The retry runs on the SAME store over its single pooled connection: a timed-out lock
        // wait aborts the open transaction, and only a rollback makes that connection reusable.
        const { contended, retried } = yield* Effect.gen(function* () {
          const store = yield* ThreadStore;

          const contended = yield* whileHoldingWriterLock(
            url,
            append(store, contendedBatch, first).pipe(Effect.exit),
          );

          return { contended, retried: yield* append(store, contendedBatch, first) };
        }).pipe(Effect.provide(singleConnectionStore(url, 50)));

        expect(Exit.isFailure(contended)).toBe(true);
        if (Exit.isFailure(contended)) {
          const error = Cause.squash(contended.cause);

          expect(error).toBeInstanceOf(ThreadStoreError);
          if (isThreadStoreError(error)) {
            expect(error.cause).toBeInstanceOf(PostgresWriteContention);
          }
        }
        expect(retried.replayed).toBe(false);
        expect(retried.firstSequence).toBe(first.lastSequence + 1);

        // A fresh store then sees the committed batch as an idempotent replay.
        const replayed = yield* withStorage(
          url,
          Effect.gen(function* () {
            const store = yield* ThreadStore;

            return yield* append(store, contendedBatch, first);
          }),
        );

        expect(replayed.replayed).toBe(true);
      }),
    ),
  );

  it.effect("exposes deterministic before/after mutation failpoints with recoverable reopen", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const active = yield* Ref.make<PostgresStorageFailpointLocation | undefined>(undefined);

        const withFailpoints = <A, E>(effect: Effect.Effect<A, E, ThreadStore>) =>
          Effect.provide(
            effect,
            PostgresThreadStore.layer({
              client: { url: Redacted.make(url) },
              observationPollInterval: 1,
              failpoint: (location) =>
                Ref.get(active).pipe(
                  Effect.flatMap((selected) =>
                    selected === location
                      ? Effect.fail(PostgresStorageFailpointError.make({ location }))
                      : Effect.void,
                  ),
                ),
            }),
          );

        const select = (location: PostgresStorageFailpointLocation | undefined) =>
          Ref.set(active, location);

        yield* select("materialize:before");
        expect(
          Exit.isFailure(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ThreadStore;

                yield* store.materialize(
                  ThreadMaterialization.make({
                    threadId,
                    producerEpoch: epoch(1),
                  }),
                );
              }),
            ).pipe(Effect.exit),
          ),
        ).toBe(true);

        yield* select(undefined);
        expect(
          Exit.isFailure(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ThreadStore;

                yield* store.export(ThreadExportRequest.make({ threadId }));
              }),
            ).pipe(Effect.exit),
          ),
        ).toBe(true);
        yield* select("materialize:after");
        expect(
          Exit.isFailure(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ThreadStore;

                yield* store.materialize(
                  ThreadMaterialization.make({
                    threadId,
                    producerEpoch: epoch(1),
                  }),
                );
              }),
            ).pipe(Effect.exit),
          ),
        ).toBe(true);
        yield* select(undefined);
        expect(
          (yield* withFailpoints(
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              return yield* store.export(ThreadExportRequest.make({ threadId }));
            }),
          )).records,
        ).toEqual([]);

        const firstBatch = batch("failpoint-append", [inputRecord("failpoint-record", "Sapporo")]);

        yield* select("append:before");
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
        expect(
          (yield* withFailpoints(
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              return yield* store.export(ThreadExportRequest.make({ threadId }));
            }),
          )).records,
        ).toEqual([]);

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

        const checkpoint = ThreadCheckpoint.make({
          schemaVersion: 1,
          threadId,
          throughSequence: recoveredAppend.lastSequence,
          tailDigest: recoveredAppend.tailDigest,
          state: { destination: "Sapporo" },
          createdAt: at(3),
        });

        const save = Effect.gen(function* () {
          const store = yield* ThreadStore;

          yield* store.checkpoints!.save(SaveCheckpointRequest.make({ checkpoint }));
        });

        yield* select("save-checkpoint:before");
        expect(Exit.isFailure(yield* withFailpoints(save).pipe(Effect.exit))).toBe(true);
        yield* select(undefined);
        expect(
          Option.isNone(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ThreadStore;

                return yield* store.checkpoints!.load(LoadCheckpointRequest.make({ threadId }));
              }),
            ),
          ),
        ).toBe(true);

        yield* select("save-checkpoint:after");
        expect(Exit.isFailure(yield* withFailpoints(save).pipe(Effect.exit))).toBe(true);
        yield* select(undefined);
        expect(
          Option.isSome(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ThreadStore;

                return yield* store.checkpoints!.load(LoadCheckpointRequest.make({ threadId }));
              }),
            ),
          ),
        ).toBe(true);
        yield* withFailpoints(save);
      }),
    ),
  );
});

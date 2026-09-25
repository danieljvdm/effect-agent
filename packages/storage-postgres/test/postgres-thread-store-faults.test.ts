import {
  PostgresStorageCompatibilityError,
  PostgresStorageCorruptionError,
  PostgresStorageFailpointError,
  type PostgresStorageFailpointLocation,
  PostgresWriteContention,
} from "@effect-agent/storage-postgres/postgres-storage-error";
import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
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
  ToolCallPrepared,
  ToolCallSettled,
  UserInputRecorded,
  type CanonicalRecordPayload,
} from "effect-agent/records";
import {
  type SelectedThreadRead,
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
import * as Statement from "effect/unstable/sql/Statement";

import { WRITER_LOCK_KEY } from "../src/internal/postgres-storage.ts";
import {
  clientLayer,
  storage as makeStorage,
  singleConnectionStorage,
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
  Effect.provide(effect, makeStorage(url).threadStore);

const withVerifiedStorage = <A, E>(url: string, effect: Effect.Effect<A, E, ThreadStore>) =>
  Effect.provide(
    effect,
    makeStorage(url, {
      observationPollInterval: 1,
      verifyOnOpen: true,
    }).threadStore,
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

const singleConnectionStore = (url: string, lockTimeout: number) =>
  singleConnectionStorage(url, lockTimeout).threadStore;

describe("PostgresThreadStore faults", () => {
  it.live(
    "keeps outstanding records in the captured snapshot while another client settles them",
    () =>
      withTemporaryDatabase((url) =>
        withStorage(
          url,
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              yield* store.materialize(
                ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
              );
              const toolCallId = id(ToolCallPrepared.fields.toolCallId, "snapshot-call");

              const prepared = canonicalRecord(
                "snapshot-prepared",
                ToolCallPrepared.make({
                  runId,
                  turnId: id(ToolCallPrepared.fields.turnId, "snapshot-turn"),
                  turn: 1,
                  toolCallId,
                  toolName: "write",
                  parameters: { original: true },
                  parametersDigest: EMPTY_TAIL_DIGEST,
                  executionKind: "orchestration",
                  executionClass: "uncertain",
                }),
              );

              const tail = yield* append(store, batch("snapshot-prepared", [prepared]));
              const paused = yield* Deferred.make<void>();
              const resume = yield* Deferred.make<void>();

              const reader = yield* store
                .read({
                  threadId,
                  page: { limit: 10 },
                  selection: {
                    _tag: "Outstanding",
                    expectedTailSequence: tail.lastSequence,
                    expectedTailDigest: tail.tailDigest,
                  },
                } satisfies SelectedThreadRead)
                .pipe(
                  Stream.runCollect,
                  Effect.provideService(Statement.CurrentTransformer, (statement) =>
                    statement.compile()[0].includes("AND outstanding <> 0")
                      ? Deferred.succeed(paused, undefined).pipe(
                          Effect.andThen(Deferred.await(resume)),
                          Effect.as(statement),
                        )
                      : Effect.succeed(statement),
                  ),
                  Effect.forkScoped,
                );

              // Pause after tail validation but before the mutable outstanding index is queried.
              yield* Deferred.await(paused).pipe(Effect.timeout("2 seconds"));

              const settled = yield* withStorage(
                url,
                Effect.gen(function* () {
                  const writer = yield* ThreadStore;

                  return yield* append(
                    writer,
                    batch("snapshot-settled", [
                      canonicalRecord(
                        "snapshot-settled",
                        ToolCallSettled.make({
                          runId,
                          toolCallId,
                          toolName: "write",
                          result: { receipt: "supplier-receipt" },
                          isFailure: false,
                        }),
                      ),
                    ]),
                    tail,
                  );
                }),
              );

              yield* Deferred.succeed(resume, undefined);
              expect((yield* Fiber.join(reader)).map((envelope) => envelope.record)).toEqual([
                prepared,
              ]);

              const current = yield* store
                .read({
                  threadId,
                  page: { limit: 10 },
                  selection: {
                    _tag: "Outstanding",
                    expectedTailSequence: settled.lastSequence,
                    expectedTailDigest: settled.tailDigest,
                  },
                } satisfies SelectedThreadRead)
                .pipe(Stream.runCollect);

              expect(current).toEqual([]);
            }),
          ),
        ),
      ),
  );

  it.effect(
    "preserves arbitrary JSON strings through append, native lookup, replay and reopen",
    () =>
      withTemporaryDatabase((url) => {
        const text = "nul:\u0000 lone:\ud800 slash:\\u0000 emoji:😀";
        const unusualRunId = id(RunCompleted.fields.runId, "run-\u0000-\ud800");

        const record = canonicalRecord(
          "unicode-record",
          UserInputRecorded.make({
            submissionId,
            kind: "user",
            runId: unusualRunId,
            input: { text },
          }),
        );

        const canonicalBatch = batch("unicode-batch", [record]);

        return Effect.gen(function* () {
          yield* withStorage(
            url,
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              yield* store.materialize(
                ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
              );
              const appended = yield* append(store, canonicalBatch);

              expect(appended.replayed).toBe(false);
              expect((yield* append(store, canonicalBatch)).replayed).toBe(true);
            }),
          );
          yield* withVerifiedStorage(
            url,
            Effect.gen(function* () {
              const store = yield* ThreadStore;

              const selected = yield* store
                .read({
                  threadId,
                  selection: { _tag: "RunInput", runId: unusualRunId },
                  page: { limit: 2 },
                } satisfies SelectedThreadRead)
                .pipe(Stream.runCollect);

              expect(selected.map((envelope) => envelope.record)).toEqual([record]);
              const exported = yield* store.export(ThreadExportRequest.make({ threadId }));

              expect(exported.records.map((envelope) => envelope.record)).toEqual([record]);
            }),
          );
        });
      }),
  );

  // Regression introduced in 6df51d36: UTF-8 replacement must not alias another thread's identity.
  it.effect("rejects malformed thread IDs without reading or fencing a valid Unicode thread", () =>
    withTemporaryDatabase((url) =>
      withStorage(
        url,
        Effect.gen(function* () {
          const store = yield* ThreadStore;
          const validId = id(ThreadMaterialization.fields.threadId, "thread-😀-\ufffd");
          const first = inputRecord("unicode-identity-first", "private input");
          const second = inputRecord("unicode-identity-second", "valid owner input");

          yield* store.materialize(
            ThreadMaterialization.make({ threadId: validId, producerEpoch: epoch(1) }),
          );

          const tail = yield* store.append(
            FencedAppendRequest.make({
              threadId: validId,
              batch: batch("unicode-identity-first", [first]),
              expectedTailSequence: sequence(0),
              expectedTailDigest: EMPTY_TAIL_DIGEST,
              producerEpoch: epoch(1),
            }),
          );

          const rejected = [];

          for (const malformed of ["thread-😀-\ud800", "thread-😀-\udc00"]) {
            const malformedId = id(ThreadMaterialization.fields.threadId, malformed);

            const mutation = yield* store
              .materialize(
                ThreadMaterialization.make({ threadId: malformedId, producerEpoch: epoch(2) }),
              )
              .pipe(Effect.exit);

            const read = yield* store
              .read(ThreadRead.make({ threadId: malformedId, limit: 10 }))
              .pipe(Stream.runCollect, Effect.exit);

            rejected.push({
              mutation:
                Exit.isFailure(mutation) && isThreadStoreError(Cause.squash(mutation.cause)),
              read: Exit.isFailure(read) && isThreadStoreError(Cause.squash(read.cause)),
            });
          }

          const validAppend = yield* store
            .append(
              FencedAppendRequest.make({
                threadId: validId,
                batch: batch("unicode-identity-second", [second]),
                expectedTailSequence: tail.lastSequence,
                expectedTailDigest: tail.tailDigest,
                producerEpoch: epoch(1),
              }),
            )
            .pipe(Effect.exit);

          const exported = yield* store.export(ThreadExportRequest.make({ threadId: validId }));

          expect({
            rejected,
            validAppend: Exit.isSuccess(validAppend),
            threadId: exported.threadId,
            records: exported.records.map((envelope) => envelope.record),
          }).toEqual({
            rejected: [
              { mutation: true, read: true },
              { mutation: true, read: true },
            ],
            validAppend: true,
            threadId: validId,
            records: [first, second],
          });
        }),
      ),
    ),
  );

  it.live(
    "interrupts a blocked writer before its lock timeout and reuses the rolled-back connection",
    () =>
      withTemporaryDatabase((url) =>
        Effect.gen(function* () {
          const store = yield* ThreadStore;
          const writerSql = yield* SqlClientService.SqlClient;

          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );
          const [{ pid }] = yield* writerSql<{ pid: number }>`SELECT pg_backend_pid() AS pid`;
          const pending = batch("cancelled-writer", [inputRecord("cancelled-record", "retry")]);

          const cancellation = yield* Effect.scoped(
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;
              const blocker = yield* sql.reserve;

              return yield* Effect.acquireUseRelease(
                blocker.executeUnprepared("BEGIN", [], undefined),
                () =>
                  Effect.gen(function* () {
                    yield* blocker.executeUnprepared(
                      `SELECT pg_advisory_xact_lock(${WRITER_LOCK_KEY})`,
                      [],
                      undefined,
                    );
                    const writer = yield* append(store, pending).pipe(Effect.forkScoped);

                    yield* Effect.gen(function* () {
                      while (true) {
                        const waiting = yield* blocker.executeUnprepared(
                          "SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND NOT granted",
                          [pid],
                          undefined,
                        );

                        if (waiting.length > 0) break;
                        yield* Effect.sleep("10 millis");
                      }
                    }).pipe(Effect.timeout("2 seconds"));

                    return yield* Fiber.interrupt(writer).pipe(
                      Effect.timeout("2 seconds"),
                      Effect.exit,
                    );
                  }),
                () => blocker.executeUnprepared("ROLLBACK", [], undefined).pipe(Effect.orDie),
              );
            }),
          ).pipe(Effect.provide(clientLayer(url)));

          expect(Exit.isSuccess(cancellation)).toBe(true);
          const appended = yield* append(store, pending);

          expect(appended.replayed).toBe(false);
          expect(appended.firstSequence).toBe(1);
        }).pipe(
          Effect.provide(
            (() => {
              const storage = singleConnectionStorage(url, 30_000);

              return Layer.mergeAll(storage.threadStore, storage.clientLayer);
            })(),
          ),
        ),
      ),
  );

  it.effect("captures an injected failpoint handler that can change after construction", () =>
    withTemporaryDatabase((url) =>
      Effect.gen(function* () {
        const active = yield* Ref.make(false);

        const storage = makeStorage(url, {
          failpoint: (location) =>
            Ref.get(active).pipe(
              Effect.flatMap((enabled) =>
                enabled && location === "materialize:before"
                  ? Effect.fail(PostgresStorageFailpointError.make({ location }))
                  : Effect.void,
              ),
            ),
        });

        yield* Effect.gen(function* () {
          const store = yield* ThreadStore;

          yield* Ref.set(active, true);

          const injected = yield* store
            .materialize(ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }))
            .pipe(Effect.exit);

          expect(Exit.isFailure(injected)).toBe(true);
          yield* Ref.set(active, false);
          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );
        }).pipe(Effect.provide(storage.threadStore));
      }),
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
        ).toEqual([{ version: 999n }]);
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

        expect(rows).toEqual([{ sequence: 1n, record_json: '{"schemaVersion":2}' }]);
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
            makeStorage(url, {
              observationPollInterval: 1,
              failpoint: (location) =>
                Ref.get(active).pipe(
                  Effect.flatMap((selected) =>
                    selected === location
                      ? Effect.fail(PostgresStorageFailpointError.make({ location }))
                      : Effect.void,
                  ),
                ),
            }).threadStore,
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

import { type DoStorageConfig } from "@effect-agent/storage-cloudflare/DoStorageConfig";
import {
  DoStorageCompatibilityError,
  DoStorageError,
  DoStorageFailpointError,
  DoValueBoundExceeded,
} from "@effect-agent/storage-cloudflare/DoStorageError";
import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/DoStorageFailpoint";
import { CurrentDoStorageVersion } from "@effect-agent/storage-cloudflare/DoStorageVersion";
import { ledgerLayer } from "@effect-agent/storage-cloudflare/DoSubmissionLedger";
import {
  threadStoreLayer,
  layer,
  storageConfigLayer,
  type DoStorageInitializationError,
} from "@effect-agent/storage-cloudflare/DoThreadStore";
import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import { CanonicalBatch, CanonicalRecord, UserInputRecorded } from "@effect-agent/thread/Records";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "@effect-agent/thread/testing/ThreadStoreConformance";
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
} from "@effect-agent/thread/ThreadStore";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import type { Crypto } from "effect";
import {
  Cause,
  Deferred,
  Fiber,
  Effect,
  Exit,
  Layer,
  Option,
  Schema,
  Stream,
  Tracer,
} from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import { seedCheckpoint, assertCheckpoint } from "../../../test/fixtures/checkpoints.ts";
import { snapshotStore } from "../../../test/fixtures/storage-upgrade.ts";
import {
  thread,
  epoch,
  id,
  at,
  sequence,
  TEST_DEPLOYMENT,
  TEST_PRODUCER,
  withThreadStorage,
} from "./harness.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;
type ThreadStoreLayerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof threadStoreLayer>,
    DoStorageConfig | DoStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
  >
>;
type ThreadStoreLayerErrorProof = Assert<
  Equal<Layer.Error<typeof threadStoreLayer>, DoStorageInitializationError>
>;
const isThreadStoreError = Schema.is(ThreadStoreError);
const isDoStorageCompatibilityError = Schema.is(DoStorageCompatibilityError);
const isDoStorageError = Schema.is(DoStorageError);
const isDoValueBoundExceeded = Schema.is(DoValueBoundExceeded);

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
      submissionId: id(SubmissionId, "submission-do-store"),
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

let recoveryCase = 0;

describe("DoThreadStore", () => {
  it("rejects canonical records beyond the captured export tail", () =>
    withThreadStorage("bad-export-tail", (storage) =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const threadId = thread("bad-export-tail");

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
        yield* Effect.sync(() =>
          storage.sql.exec(
            "UPDATE effect_agent_threads SET tail_sequence=0 WHERE thread_id=?",
            threadId,
          ),
        );
        expect(
          yield* store.export(ThreadExportRequest.make({ threadId })).pipe(Effect.flip),
        ).toMatchObject({
          _tag: "ThreadStoreError",
          message: expect.stringContaining("beyond the captured thread tail"),
        });
      }).pipe(Effect.provide(layer({ storage }))),
    ));

  it("isolates, fences and replaces one durable recovery checkpoint across reopen", () =>
    withThreadStorage(`recovery-store:${++recoveryCase}`, (storage) =>
      Effect.gen(function* () {
        const checkpoint = yield* Effect.gen(function* () {
          const store = yield* ThreadStore;
          const threadId = thread("recovery-store");

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
        }).pipe(Effect.provide(layer({ storage })));

        const restored = yield* ThreadStore.pipe(
          Effect.flatMap((store) =>
            store.recoveryCheckpoints!.load(
              LoadCheckpointRequest.make({ threadId: checkpoint.threadId }),
            ),
          ),
          Effect.provide(layer({ storage })),
        );

        expect(restored).toEqual(Option.some(checkpoint));

        const rows = yield* Effect.sync(() =>
          storage.sql
            .exec("SELECT COUNT(*) AS count FROM effect_agent_recovery_checkpoints")
            .toArray(),
        );

        expect(rows).toEqual([{ count: 1 }]);
      }),
    ));

  for (const corruption of ["json", "version", "thread", "sequence", "digest", "row"] as const) {
    it(`rejects ${corruption} recovery cache corruption and permits same-tail repair`, () =>
      withThreadStorage(`recovery-store:${++recoveryCase}`, (storage) =>
        Effect.gen(function* () {
          const store = yield* ThreadStore;
          const threadId = thread("recovery-store");

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
                  ...(corruption === "version" ? { schemaVersion: 99 } : {}),
                  ...(corruption === "thread" ? { threadId: "foreign" } : {}),
                  ...(corruption === "sequence" ? { throughSequence: 1 } : {}),
                  ...(corruption === "digest" ? { tailDigest: "a".repeat(64) } : {}),
                });

          const storedSequence = corruption === "row" ? -1 : 0;

          yield* Effect.sync(() =>
            storage.sql.exec(
              "UPDATE effect_agent_recovery_checkpoints SET checkpoint_json=?, through_sequence=? WHERE thread_id=?",
              json,
              storedSequence,
              threadId,
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
        }).pipe(Effect.provide(layer({ storage }))),
      ));
  }

  for (const location of [
    "save-recovery-checkpoint:before",
    "save-recovery-checkpoint:after",
  ] as const) {
    for (const mode of ["failure", "defect", "interrupt", "timeout"] as const) {
      it(`reopens safely after ${mode} at ${location}`, () =>
        withThreadStorage(`recovery-store:${++recoveryCase}`, (storage) =>
          Effect.gen(function* () {
            const threadId = thread("recovery-store");

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
                      storage,
                      failpoint: (point) =>
                        point !== location
                          ? Effect.void
                          : Deferred.succeed(entered, undefined).pipe(
                              Effect.andThen(
                                mode === "failure"
                                  ? DoStorageFailpointError.make({ location })
                                  : mode === "defect"
                                    ? Effect.die("injected checkpoint defect")
                                    : mode === "interrupt"
                                      ? Effect.interrupt
                                      : Effect.never,
                              ),
                            ),
                    }),
                  ),
                  Effect.timeout("1 second"),
                  Effect.forkChild,
                );

                yield* Deferred.await(entered);
                if (mode === "timeout") yield* TestClock.adjust("1 second");

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
            }).pipe(Effect.provide(layer({ storage })));
          }),
        ));
    }
  }

  for (const historical of [false, true]) {
    it(`reopens ${historical ? "historical" : "metadata-free"} checkpoints without rewriting storage`, () =>
      withThreadStorage(`checkpoint-roundtrip:${historical}`, (storage) =>
        Effect.gen(function* () {
          yield* seedCheckpoint(historical).pipe(
            Effect.provide(
              Layer.mergeAll(layer({ storage }), ledgerLayer({ storage }), BrowserCrypto.layer),
            ),
          );
          const before = yield* snapshotStore;

          for (const verifyOnOpen of [false, true]) {
            yield* Effect.gen(function* () {
              yield* ThreadStore;
              expect(yield* snapshotStore).toEqual(before);
              yield* assertCheckpoint(historical);
              expect(yield* snapshotStore).toEqual(before);
            }).pipe(Effect.provide(layer({ storage, verifyOnOpen })));
          }
        }).pipe(Effect.provide(SqliteClient.layer({ storage }))),
      ));
  }

  for (const corruption of ["thread", "sequence", "digest"] as const) {
    it(`rejects checkpoint ${corruption} metadata that disagrees with its row`, () =>
      withThreadStorage(`checkpoint-metadata:${corruption}`, (storage) =>
        Effect.gen(function* () {
          const store = yield* ThreadStore;
          const threadId = thread("checkpoint-metadata");

          yield* store.materialize(
            ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }),
          );

          const appended = yield* store.append(
            FencedAppendRequest.make({
              threadId,
              batch: batch("checkpoint-metadata", [
                inputRecord("checkpoint-metadata-input", "Kyoto"),
              ]),
              expectedTailSequence: sequence(0),
              expectedTailDigest: EMPTY_TAIL_DIGEST,
              producerEpoch: epoch(1),
            }),
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
            ...(corruption === "thread" ? { threadId: thread("other-checkpoint-thread") } : {}),
            ...(corruption === "sequence"
              ? { throughSequence: appended.lastSequence, tailDigest: appended.tailDigest }
              : {}),
          });

          const corruptedJson = JSON.stringify(
            yield* Schema.encodeEffect(ThreadCheckpoint)(corrupted),
          );

          const rowDigest = corruption === "digest" ? appended.tailDigest : EMPTY_TAIL_DIGEST;

          yield* Effect.sync(() =>
            storage.sql.exec(
              "UPDATE effect_agent_checkpoints SET checkpoint_json = ?, tail_digest = ? WHERE thread_id = ? AND through_sequence = 0",
              corruptedJson,
              rowDigest,
              threadId,
            ),
          );

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
        }).pipe(Effect.provide(layer({ storage }))),
      ));
  }

  // The SAME adapter-neutral contract suite the Node/SQLite and in-memory adapters run,
  // executed in-workerd against a real SQLite-backed Durable Object's storage. One Durable
  // Object per case: the 0.21.x pool shares storage across tests within a run.
  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of [
      ...threadStoreConformanceCases,
      ...threadCheckpointConformanceCases,
    ]) {
      it(conformanceCase.name, () => {
        const spanNames: Array<string> = [];

        const tracer = Tracer.make({
          span(options) {
            spanNames.push(options.name);

            return new Tracer.NativeSpan(options);
          },
        });

        return withThreadStorage(`wp1-store:${conformanceCase.name}`, (storage) =>
          conformanceCase.run.pipe(
            Effect.provide(layer({ storage, observationPollInterval: 1 })),
            Effect.provideService(Tracer.Tracer, tracer),
            Effect.withTracerEnabled(true),
            Effect.tap(() =>
              Effect.sync(() => {
                expect(spanNames).toContain("sql.execute");
                for (const helper of [
                  "DoJournal.decodeRows",
                  "DoJournal.decodeSingleRow",
                  "DoThreadStore.makeOffset",
                  "DoThreadStore.parseOffset",
                  "DoThreadStore.encodeCanonicalRecord",
                  "DoThreadStore.encodeCanonicalBatch",
                  "DoThreadStore.encodeCheckpoint",
                  "DoThreadStore.decodeEnvelope",
                  "DoThreadStore.decodeCheckpoint",
                  "DoThreadStore.hitFailpoint",
                ]) {
                  expect(spanNames).not.toContain(helper);
                }
              }),
            ),
          ),
        );
      });
    }
  });

  it("keeps configuration, failpoint, SQL, and Crypto authority in the named Layer input", () => {
    const requirementsProof: ThreadStoreLayerRequirementsProof = true;
    const errorProof: ThreadStoreLayerErrorProof = true;

    expect(requirementsProof).toBe(true);
    expect(errorProof).toBe(true);
  });

  it("streams large reads from their captured membership and resumes observation through later appends", () =>
    withThreadStorage("wp1-store-large-read-snapshot", (storage) =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const threadId = thread("thread-large-read-snapshot");
        let tail = { lastSequence: sequence(0), tailDigest: EMPTY_TAIL_DIGEST };

        yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch: epoch(1) }));

        const appendInput = Effect.fn("DoThreadStoreTest.appendLargeInput")(function* (
          name: string,
          input: string,
        ) {
          tail = yield* store.append(
            FencedAppendRequest.make({
              threadId,
              batch: batch(`large-batch-${name}`, [inputRecord(`large-record-${name}`, input)]),
              expectedTailSequence: tail.lastSequence,
              expectedTailDigest: tail.tailDigest,
              producerEpoch: epoch(1),
            }),
          );
        });

        const input = "x".repeat(900_000);

        for (let index = 1; index <= 9; index++) yield* appendInput(String(index), input);

        const records = yield* store.read(ThreadRead.make({ threadId, limit: 1_024 })).pipe(
          Stream.mapEffect(
            Effect.fn(function* (record) {
              if (record.sequence === 1) yield* appendInput("during-read", "later");

              return { sequence: record.sequence, offset: record.offset };
            }),
          ),
          Stream.runCollect,
        );

        expect(records.map((record) => record.sequence)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);

        const limited = yield* store
          .read(ThreadRead.make({ threadId, afterSequence: sequence(2), limit: 5 }))
          .pipe(
            Stream.map((record) => record.sequence),
            Stream.runCollect,
          );

        expect(limited).toEqual([3, 4, 5, 6, 7]);

        const observed = yield* store
          .observe(ThreadObservation.make({ threadId, afterOffset: records[3].offset }))
          .pipe(
            Stream.mapEffect(
              Effect.fn(function* (record) {
                if (record.sequence === 5) yield* appendInput("during-observe", "later again");

                return record.sequence;
              }),
            ),
            Stream.take(7),
            Stream.runCollect,
          );

        expect(observed).toEqual([5, 6, 7, 8, 9, 10, 11]);

        const changed = yield* store.read(ThreadRead.make({ threadId, limit: 9 })).pipe(
          Stream.tap((record) =>
            Effect.sync(() => {
              if (record.sequence === 1) {
                storage.sql.exec(
                  "DELETE FROM effect_agent_canonical_records WHERE thread_id = ? AND sequence = 9",
                  threadId,
                );
              }
            }),
          ),
          Stream.runDrain,
          Effect.flip,
        );

        expect(changed).toMatchObject({
          _tag: "ThreadStoreError",
          cause: { _tag: "DoStorageCorruptionError" },
        });
      }).pipe(Effect.provide(layer({ storage }))),
    ));

  it("validates convenience-layer configuration before initializing storage", () =>
    withThreadStorage("wp1-store-invalid-config", (storage) =>
      Effect.gen(function* () {
        const opened = yield* ThreadStore.pipe(
          Effect.provide(layer({ storage, observationPollInterval: -1 })),
          Effect.exit,
        );

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          const failure = Cause.findErrorOption(opened.cause);

          expect(Option.isSome(failure)).toBe(true);
          if (Option.isSome(failure)) {
            expect(isDoStorageError(failure.value)).toBe(true);
            if (isDoStorageError(failure.value)) {
              expect(failure.value.operation).toBe("configure Durable Object storage");
            }
          }
        }

        const tables = storage.sql
          .exec<{ name: string }>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'effect_agent_%'",
          )
          .toArray();

        expect(tables).toEqual([]);
      }),
    ));

  it("rejects an unsupported storage version without mutating its tables", () =>
    withThreadStorage("wp1-store-unsupported-version", (storage) =>
      Effect.gen(function* () {
        const previousVersion = 1;

        storage.sql.exec(`
          CREATE TABLE effect_agent_meta (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
          );
          INSERT INTO effect_agent_meta (key, value)
          VALUES ('storage_version', '${previousVersion}');
          CREATE TABLE effect_agent_threads (
            thread_id TEXT PRIMARY KEY NOT NULL
          );
          INSERT INTO effect_agent_threads (thread_id)
          VALUES ('retained-thread');
        `);

        const opened = yield* ThreadStore.pipe(
          Effect.provide(layer({ storage, observationPollInterval: 1 })),
          Effect.exit,
        );

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          const failure = Cause.findErrorOption(opened.cause);

          expect(Option.isSome(failure)).toBe(true);
          if (Option.isSome(failure)) {
            expect(isDoStorageCompatibilityError(failure.value)).toBe(true);
            if (isDoStorageCompatibilityError(failure.value)) {
              expect(failure.value.actualVersion).toBe(previousVersion);
              expect(failure.value.supportedVersion).toBe(CurrentDoStorageVersion);
              expect(failure.value.message).toContain("Keep the original store");
            }
          }
        }

        const rows = storage.sql
          .exec<{ thread_id: string }>("SELECT thread_id FROM effect_agent_threads")
          .toArray();

        expect(rows).toEqual([{ thread_id: "retained-thread" }]);
      }),
    ));

  it("refuses an over-bound canonical append typed before any write", () =>
    withThreadStorage("wp1-store-value-bound", (storage) =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const sql = yield* SqlClientService.SqlClient;
        const threadId = "thread-value-bound";

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId: thread(threadId),
            producerEpoch: epoch(1),
          }),
        );

        // 2,048 bytes of record content against a 1,024-byte configured bound (the platform
        // analogue is ~1.9 MB under the 2 MB per-value limit; a small bound keeps the test
        // payload honest without allocating megabytes inside workerd).
        const exit = yield* store
          .append(
            FencedAppendRequest.make({
              threadId: thread(threadId),
              batch: batch("value-bound-batch", [
                inputRecord("value-bound-record", "x".repeat(2_048)),
              ]),
              expectedTailSequence: sequence(0),
              expectedTailDigest: EMPTY_TAIL_DIGEST,
              producerEpoch: epoch(1),
            }),
          )
          .pipe(Effect.exit);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause);

          expect(error).toBeInstanceOf(ThreadStoreError);
          if (isThreadStoreError(error)) {
            expect(error.cause).toBeInstanceOf(DoValueBoundExceeded);
            if (isDoValueBoundExceeded(error.cause)) {
              expect(error.cause.maxBytes).toBe(1_024);
              expect(error.cause.actualBytes).toBeGreaterThan(1_024);
              expect(error.cause.message).toContain("R2");
            }
          }
        }

        // Nothing was written: the refusal happened BEFORE any durable mutation.
        const batchRows = yield* sql<Record<string, unknown>>`
          SELECT batch_id FROM effect_agent_canonical_batches
        `;

        expect(batchRows).toEqual([]);

        const recordRows = yield* sql<Record<string, unknown>>`
          SELECT record_id FROM effect_agent_canonical_records
        `;

        expect(recordRows).toEqual([]);
      }).pipe(
        Effect.provide(
          threadStoreLayer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                storageConfigLayer({ storage, maxStoredValueBytes: 1_024 }),
                DoStorageFailpoint.layer,
                SqliteClient.layer({ storage }),
                BrowserCrypto.layer,
              ),
            ),
          ),
        ),
      ),
    ));
});
import { SubmissionId } from "@effect-agent/core/Identifiers";

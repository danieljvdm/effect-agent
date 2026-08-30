import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  ConversationCheckpoint,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  LoadCheckpointRequest,
  ObservationOffset,
  ProducerEpoch,
  RunCompleted,
  SaveCheckpointRequest,
  UserInputRecorded,
  type AppendResult,
  type CanonicalRecordPayload,
} from "@effect-agent/session";
import { conversationStoreConformanceCases } from "@effect-agent/session/testing";
import { SqliteStorageFailpointTestControl } from "@effect-agent/storage-sqlite/testing";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, describe, it } from "@effect/vitest";
import type { Crypto, PlatformError } from "effect";
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
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import type { SqliteStorageFailpoint } from "../src/index.ts";
import {
  conversationStoreLayer,
  layer,
  observationOffsetAt,
  SqliteStorageConfig,
  SqliteStorageConfigValue,
  SqliteStorageFailpointError,
  type SqliteStorageFailpointLocation,
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  type SqliteStorageInitializationError,
  SqliteStorageError,
  SqliteWriteContention,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;
type ConversationStoreLayerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof conversationStoreLayer>,
    SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
  >
>;
type ConversationStoreLayerErrorProof = Assert<
  Equal<Layer.Error<typeof conversationStoreLayer>, SqliteStorageInitializationError>
>;

const conversationId = Schema.decodeSync(ConversationMaterialization.fields.conversationId)(
  "conversation-sqlite-1",
);
const secondConversationId = Schema.decodeSync(ConversationMaterialization.fields.conversationId)(
  "conversation-sqlite-2",
);
const runId = Schema.decodeSync(RunCompleted.fields.runId)("run-sqlite-1");
const submissionId = Schema.decodeSync(UserInputRecorded.fields.submissionId)(
  "submission-sqlite-1",
);

const id = <A>(schema: Schema.Codec<A, string>, value: string): A =>
  Schema.decodeSync(schema)(value);

const sequence = (value: number) => Schema.decodeSync(CanonicalSequence)(value);
const epoch = (value: number) => Schema.decodeSync(ProducerEpoch)(value);
const isSqliteStorageError = Schema.is(SqliteStorageError);
const isConversationStoreError = Schema.is(ConversationStoreError);

const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const canonicalRecord = (recordId: string, payload: CanonicalRecordPayload): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/RecordId")),
      recordId,
    ),
    family: "conversation",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/DeploymentId")),
      "deployment-sqlite",
    ),
    payload,
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/BatchId")), batchId),
    producerId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/ProducerId")),
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
  store: ConversationStore["Service"],
  canonicalBatch: CanonicalBatch,
  tail: Pick<AppendResult, "lastSequence" | "tailDigest"> = {
    lastSequence: sequence(0),
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpoch: ProducerEpoch = epoch(1),
) =>
  store.append(
    FencedAppendRequest.make({
      conversationId,
      batch: canonicalBatch,
      expectedTailSequence: tail.lastSequence,
      expectedTailDigest: tail.tailDigest,
      producerEpoch,
    }),
  );

const withStorage = <A, E>(filename: string, effect: Effect.Effect<A, E, ConversationStore>) =>
  Effect.provide(effect, layer({ filename, observationPollInterval: 1 }));

const withVerifiedStorage = <A, E>(
  filename: string,
  effect: Effect.Effect<A, E, ConversationStore>,
) => Effect.provide(effect, layer({ filename, observationPollInterval: 1, verifyOnOpen: true }));

const withSql = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, SqliteClient.layer({ filename }));

const explicitTestStorageLayer = (filename: string) =>
  conversationStoreLayer.pipe(
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
      return yield* use(`${directory}/conversation.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SqliteConversationStore", () => {
  describe("shared ConversationStore conformance", () => {
    for (const conformanceCase of conversationStoreConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((filename) => withStorage(filename, conformanceCase.run)),
      );
    }
  });

  it("keeps configuration and failpoint authority in the named Layer input", () => {
    const requirementsProof: ConversationStoreLayerRequirementsProof = true;
    const errorProof: ConversationStoreLayerErrorProof = true;

    expect(requirementsProof).toBe(true);
    expect(errorProof).toBe(true);
  });

  it.effect("supports explicit configuration and controllable failpoint services", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const config = yield* SqliteStorageConfig;
        const failpoints = yield* SqliteStorageFailpointTestControl;
        const store = yield* ConversationStore;

        expect(config).toMatchObject({
          observationPollInterval: 1,
          busyTimeout: 5_000,
          verifyOnOpen: false,
        });
        yield* failpoints.setHandler((location) =>
          location === "materialize:before"
            ? Effect.fail(SqliteStorageFailpointError.make({ location }))
            : Effect.void,
        );
        const injected = yield* store
          .materialize(
            ConversationMaterialization.make({
              conversationId,
              producerEpoch: epoch(1),
            }),
          )
          .pipe(Effect.exit);
        expect(Exit.isFailure(injected)).toBe(true);

        yield* failpoints.clear;
        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId,
            producerEpoch: epoch(1),
          }),
        );
      }).pipe(Effect.provide(explicitTestStorageLayer(filename))),
    ),
  );

  it.effect("validates convenience-layer configuration before constructing the store", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const opened = yield* ConversationStore.pipe(
          Effect.provide(layer({ filename, observationPollInterval: -1 })),
          Effect.exit,
        );

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          const error = Cause.squash(opened.cause);
          expect(error).toBeInstanceOf(SqliteStorageError);
          if (isSqliteStorageError(error)) {
            expect(error.operation).toBe("configure SQLite storage");
            expect(error.cause).toBeDefined();
          }
        }
        const databaseExists = yield* FileSystem.FileSystem.use((fs) => fs.exists(filename)).pipe(
          Effect.provide(NodeFileSystem.layer),
        );
        expect(databaseExists).toBe(false);
      }),
    ),
  );

  it.effect(
    "persists replay, export, checkpoints, and non-durable capabilities across reopen",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const first = yield* withStorage(
            filename,
            Effect.gen(function* () {
              const store = yield* ConversationStore;
              yield* store.materialize(
                ConversationMaterialization.make({ conversationId, producerEpoch: epoch(1) }),
              );
              const appended = yield* append(
                store,
                batch("persist-1", [inputRecord("persist-record-1", "Kyoto")]),
              );
              const checkpoint = ConversationCheckpoint.make({
                schemaVersion: 1,
                conversationId,
                throughSequence: appended.lastSequence,
                tailDigest: appended.tailDigest,
                engineVersion: "phase-3",
                agentDefinitionDigest: appended.tailDigest,
                modelDigest: appended.tailDigest,
                toolDigest: appended.tailDigest,
                state: { destination: "Kyoto" },
                createdAt: at(2),
              });
              yield* store.saveCheckpoint(SaveCheckpointRequest.make({ checkpoint }));
              return appended;
            }),
          );

          yield* withStorage(
            filename,
            Effect.gen(function* () {
              const store = yield* ConversationStore;
              const exported = yield* store.export(
                ConversationExportRequest.make({ conversationId }),
              );
              const checkpoint = yield* store.loadCheckpoint(
                LoadCheckpointRequest.make({ conversationId }),
              );

              expect(exported.records).toHaveLength(1);
              expect(exported.tailSequence).toBe(first.lastSequence);
              expect(exported.tailDigest).toBe(first.tailDigest);
              expect(Option.isSome(checkpoint)).toBe(true);
              const reread = yield* store
                .read(ConversationRead.make({ conversationId, limit: 1_024 }))
                .pipe(Stream.runCollect);
              expect(reread).toHaveLength(1);
              const reobserved = yield* store
                .observe(ConversationObservation.make({ conversationId }))
                .pipe(Stream.take(1), Stream.runCollect);
              expect(reobserved).toHaveLength(1);
            }),
          );
        }),
      ),
  );

  it.effect("resumes observation from the opaque offset returned by the prior record", () =>
    withTemporaryDatabase((filename) =>
      withStorage(
        filename,
        Effect.gen(function* () {
          const store = yield* ConversationStore;
          yield* store.materialize(
            ConversationMaterialization.make({ conversationId, producerEpoch: epoch(1) }),
          );
          const first = yield* append(
            store,
            batch("observe-1", [inputRecord("observe-record-1", "first")]),
          );
          yield* append(
            store,
            batch("observe-2", [inputRecord("observe-record-2", "second")]),
            first,
          );
          const existing = yield* store
            .read(ConversationRead.make({ conversationId, limit: 1_024 }))
            .pipe(Stream.runCollect);
          const [firstExisting, secondExisting] = existing;
          if (firstExisting === undefined || secondExisting === undefined) {
            return yield* Effect.die(
              new Error("Expected both canonical records before resuming observation"),
            );
          }
          const resumed = yield* store
            .observe(
              ConversationObservation.make({
                conversationId,
                afterOffset: firstExisting.offset,
              }),
            )
            .pipe(Stream.take(1), Stream.runCollect);
          expect(resumed.map((record) => record.record.recordId)).toEqual([
            secondExisting.record.recordId,
          ]);

          yield* store.materialize(
            ConversationMaterialization.make({
              conversationId: secondConversationId,
              producerEpoch: epoch(1),
            }),
          );
          yield* store.append(
            FencedAppendRequest.make({
              conversationId: secondConversationId,
              batch: batch("observe-foreign", [inputRecord("observe-foreign-record", "foreign")]),
              expectedTailSequence: sequence(0),
              expectedTailDigest: EMPTY_TAIL_DIGEST,
              producerEpoch: epoch(1),
            }),
          );
          const foreign = yield* store
            .read(
              ConversationRead.make({
                conversationId: secondConversationId,
                limit: 1,
              }),
            )
            .pipe(Stream.runCollect);
          const [foreignRecord] = foreign;
          if (foreignRecord === undefined) {
            return yield* Effect.die(
              new Error("Expected the foreign Conversation to contain one canonical record"),
            );
          }
          const foreignExit = yield* store
            .observe(
              ConversationObservation.make({
                conversationId,
                afterOffset: foreignRecord.offset,
              }),
            )
            .pipe(Stream.take(1), Stream.runCollect, Effect.exit);
          expect(Exit.isFailure(foreignExit)).toBe(true);

          const malformedOffset =
            yield* Schema.decodeUnknownEffect(ObservationOffset)("foreign-adapter:1");
          const malformedExit = yield* store
            .observe(
              ConversationObservation.make({
                conversationId,
                afterOffset: malformedOffset,
              }),
            )
            .pipe(Stream.take(1), Stream.runCollect, Effect.exit);
          expect(Exit.isFailure(malformedExit)).toBe(true);
        }),
      ),
    ),
  );

  it.effect("rejects invalid canonical sequences when constructing observation offsets", () =>
    Effect.forEach([-1, 1.5, Number.NaN], (sequence) =>
      Effect.gen(function* () {
        const exit = yield* observationOffsetAt(conversationId, sequence).pipe(Effect.exit);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause);
          expect(error).toBeInstanceOf(ConversationStoreError);
          if (isConversationStoreError(error)) {
            expect(error.operation).toBe("encode observation offset");
          }
        }
      }),
    ),
  );

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

        const opened = yield* withStorage(filename, ConversationStore).pipe(Effect.exit);
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
            const store = yield* ConversationStore;
            yield* store.materialize(
              ConversationMaterialization.make({ conversationId, producerEpoch: epoch(1) }),
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
              WHERE conversation_id = ${conversationId}
                AND sequence = 1
            `;
          }),
        );

        // The opt-in integrity scan refuses to open the corrupt database.
        const verified = yield* ConversationStore.pipe(
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
            const store = yield* ConversationStore;
            return yield* store
              .read(ConversationRead.make({ conversationId, limit: 1_024 }))
              .pipe(Stream.runCollect);
          }),
        ).pipe(Effect.exit);
        expect(Exit.isFailure(lazyRead)).toBe(true);
        if (Exit.isFailure(lazyRead)) {
          const error = Cause.squash(lazyRead.cause);
          expect(error).toBeInstanceOf(ConversationStoreError);
          if (isConversationStoreError(error)) {
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
              WHERE conversation_id = ${conversationId}
            `;
          }),
        );
        expect(rows).toEqual([{ sequence: 1, record_json: '{"schemaVersion":2}' }]);
      }),
    ),
  );

  it.effect("reconstructs the same export after a second persisted append and reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const expected = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ConversationStore;
            yield* store.materialize(
              ConversationMaterialization.make({ conversationId, producerEpoch: epoch(1) }),
            );
            const first = yield* append(
              store,
              batch("restart-1", [inputRecord("restart-record-1", "Nara")]),
            );
            const completed = canonicalRecord(
              "restart-record-2",
              RunCompleted.make({ runId, output: { itinerary: "Nara" } }),
            );
            yield* append(store, batch("restart-2", [completed]), first);
            return yield* store.export(ConversationExportRequest.make({ conversationId }));
          }),
        );

        // Reopen with the opt-in integrity scan to keep its healthy-database path covered.
        const reopened = yield* withVerifiedStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ConversationStore;
            return yield* store.export(ConversationExportRequest.make({ conversationId }));
          }),
        );
        expect(reopened).toEqual(expected);
      }),
    ),
  );

  it.effect("exports one transactionally consistent snapshot during a concurrent append", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const first = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ConversationStore;
            yield* store.materialize(
              ConversationMaterialization.make({ conversationId, producerEpoch: epoch(1) }),
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
            const store = yield* ConversationStore;
            return yield* store.export(ConversationExportRequest.make({ conversationId }));
          }),
          layer({
            filename,
            observationPollInterval: 1,
            failpoint: (location) =>
              location === "export:after-conversation-read"
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
            const store = yield* ConversationStore;
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
            const store = yield* ConversationStore;
            return yield* store.export(ConversationExportRequest.make({ conversationId }));
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
          const store = yield* ConversationStore;
          yield* store.materialize(
            ConversationMaterialization.make({ conversationId, producerEpoch: epoch(1) }),
          );

          const observerFiber = yield* store
            .observe(ConversationObservation.make({ conversationId }))
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
            const store = yield* ConversationStore;
            yield* store.materialize(
              ConversationMaterialization.make({ conversationId, producerEpoch: epoch(1) }),
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
                const store = yield* ConversationStore;
                return yield* append(store, contendedBatch, first);
              }),
              layer({ filename, observationPollInterval: 1, busyTimeout: 0 }),
            ).pipe(Effect.exit);
            yield* sql`ROLLBACK`;

            expect(Exit.isFailure(contended)).toBe(true);
            if (Exit.isFailure(contended)) {
              const error = Cause.squash(contended.cause);
              expect(error).toBeInstanceOf(ConversationStoreError);
              if (isConversationStoreError(error)) {
                expect(error.cause).toBeInstanceOf(SqliteWriteContention);
              }
            }
          }),
        );

        // Once the competing writer releases the lock, the identical append commits.
        const recovered = yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ConversationStore;
            return yield* append(store, contendedBatch, first);
          }),
        );
        expect(recovered.replayed).toBe(false);
        expect(recovered.firstSequence).toBe(first.lastSequence + 1);
      }),
    ),
  );

  it.effect("exposes deterministic before/after mutation failpoints with recoverable reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const active = yield* Ref.make<SqliteStorageFailpointLocation | undefined>(undefined);
        const withFailpoints = <A, E>(effect: Effect.Effect<A, E, ConversationStore>) =>
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

        yield* select("materialize:before");
        expect(
          Exit.isFailure(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ConversationStore;
                yield* store.materialize(
                  ConversationMaterialization.make({
                    conversationId,
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
                const store = yield* ConversationStore;
                yield* store.export(ConversationExportRequest.make({ conversationId }));
              }),
            ).pipe(Effect.exit),
          ),
        ).toBe(true);
        yield* select("materialize:after");
        expect(
          Exit.isFailure(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ConversationStore;
                yield* store.materialize(
                  ConversationMaterialization.make({
                    conversationId,
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
              const store = yield* ConversationStore;
              return yield* store.export(ConversationExportRequest.make({ conversationId }));
            }),
          )).records,
        ).toEqual([]);

        const firstBatch = batch("failpoint-append", [inputRecord("failpoint-record", "Sapporo")]);
        yield* select("append:before");
        expect(
          Exit.isFailure(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ConversationStore;
                yield* append(store, firstBatch);
              }),
            ).pipe(Effect.exit),
          ),
        ).toBe(true);
        yield* select(undefined);
        expect(
          (yield* withFailpoints(
            Effect.gen(function* () {
              const store = yield* ConversationStore;
              return yield* store.export(ConversationExportRequest.make({ conversationId }));
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
                  const store = yield* ConversationStore;
                  yield* append(store, firstBatch);
                }),
              ).pipe(Effect.exit);
              expect(Exit.isFailure(exit)).toBe(true);
              if (Exit.isFailure(exit)) {
                const error = Cause.squash(exit.cause);
                expect(error).toBeInstanceOf(ConversationStoreError);
                if (isConversationStoreError(error)) {
                  expect(error.operation).toBe("append canonical batch");
                  expect(error.message).toContain(location);
                }
              }
              yield* select(undefined);
              const exported = yield* withFailpoints(
                Effect.gen(function* () {
                  const store = yield* ConversationStore;
                  return yield* store.export(ConversationExportRequest.make({ conversationId }));
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
                const store = yield* ConversationStore;
                yield* append(store, firstBatch);
              }),
            ).pipe(Effect.exit),
          ),
        ).toBe(true);
        yield* select(undefined);
        const recoveredAppend = yield* withFailpoints(
          Effect.gen(function* () {
            const store = yield* ConversationStore;
            return yield* append(store, firstBatch);
          }),
        );
        expect(recoveredAppend.replayed).toBe(true);

        const checkpoint = ConversationCheckpoint.make({
          schemaVersion: 1,
          conversationId,
          throughSequence: recoveredAppend.lastSequence,
          tailDigest: recoveredAppend.tailDigest,
          engineVersion: "phase-3",
          agentDefinitionDigest: recoveredAppend.tailDigest,
          modelDigest: recoveredAppend.tailDigest,
          toolDigest: recoveredAppend.tailDigest,
          state: { destination: "Sapporo" },
          createdAt: at(3),
        });
        const save = Effect.gen(function* () {
          const store = yield* ConversationStore;
          yield* store.saveCheckpoint(SaveCheckpointRequest.make({ checkpoint }));
        });

        yield* select("save-checkpoint:before");
        expect(Exit.isFailure(yield* withFailpoints(save).pipe(Effect.exit))).toBe(true);
        yield* select(undefined);
        expect(
          Option.isNone(
            yield* withFailpoints(
              Effect.gen(function* () {
                const store = yield* ConversationStore;
                return yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId }));
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
                const store = yield* ConversationStore;
                return yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId }));
              }),
            ),
          ),
        ).toBe(true);
        yield* withFailpoints(save);
      }),
    ),
  );
});

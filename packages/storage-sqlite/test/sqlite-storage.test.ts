import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, describe, it } from "@effect/vitest";
import {
  DateTime,
  Cause,
  Effect,
  Exit,
  FileSystem,
  Option,
  PlatformError,
  Ref,
  Schema,
  Stream,
} from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  CanonicalBatch,
  CanonicalRecord,
  ConversationCheckpoint,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  LoadCheckpointRequest,
  ObservationOffset,
  RunCompleted,
  SaveCheckpointRequest,
  SubmissionStore,
  UserInputRecorded,
  type AppendResult,
  type CanonicalRecordPayload,
} from "@effect-agent/session";
import {
  layer,
  SqliteStorageFailpointError,
  type SqliteStorageFailpointLocation,
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
} from "../src/index.ts";

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
    lastSequence: 0,
    tailDigest: EMPTY_TAIL_DIGEST,
  },
  producerEpoch = 1,
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

const inspectConversationStoreConformance = Effect.gen(function* () {
  const store = yield* ConversationStore;
  const read = yield* store
    .read(ConversationRead.make({ conversationId, limit: 1_024 }))
    .pipe(Stream.runCollect);
  const observed = yield* store
    .observe(ConversationObservation.make({ conversationId }))
    .pipe(Stream.take(read.length), Stream.runCollect);
  const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
  const checkpoint = yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId }));
  return {
    readCount: read.length,
    observedCount: observed.length,
    exportCount: exported.records.length,
    hasCheckpoint: Option.isSome(checkpoint),
  };
});

const withStorage = <A, E>(
  filename: string,
  effect: Effect.Effect<A, E, ConversationStore | SubmissionStore>,
) => Effect.provide(effect, layer({ filename, observationPollInterval: 1 }));

const withSql = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, SqliteClient.layer({ filename }));

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
                ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
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
              const submissions = yield* SubmissionStore;
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
              expect(yield* inspectConversationStoreConformance).toEqual({
                readCount: 1,
                observedCount: 1,
                exportCount: 1,
                hasCheckpoint: true,
              });
              expect(yield* submissions.capabilities).toEqual({
                durability: "non-durable",
                acceptsDurableWork: false,
              });
              expect(Option.isNone(yield* submissions.inspect(submissionId))).toBe(true);
            }),
          );
        }),
      ),
  );

  it.effect("atomically replays identical batches and rejects conflicts and fenced producers", () =>
    withTemporaryDatabase((filename) =>
      withStorage(
        filename,
        Effect.gen(function* () {
          const store = yield* ConversationStore;
          yield* store.materialize(
            ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
          );
          const firstBatch = batch("atomic-1", [inputRecord("atomic-record-1", "Lisbon")]);
          const first = yield* append(store, firstBatch);
          expect((yield* append(store, firstBatch)).replayed).toBe(true);

          const conflicting = yield* append(
            store,
            batch("atomic-1", [inputRecord("atomic-record-2", "Porto")]),
          ).pipe(Effect.exit);
          expect(Exit.isFailure(conflicting)).toBe(true);

          const duplicate = inputRecord("duplicate-record", "duplicate");
          const duplicateExit = yield* append(
            store,
            batch("atomic-duplicate", [duplicate, duplicate]),
            first,
          ).pipe(Effect.exit);
          expect(Exit.isFailure(duplicateExit)).toBe(true);

          yield* store.materialize(
            ConversationMaterialization.make({ conversationId, producerEpoch: 2 }),
          );
          const fenced = yield* append(
            store,
            batch("atomic-2", [inputRecord("atomic-record-3", "Coimbra")]),
            first,
            1,
          ).pipe(Effect.exit);
          expect(Exit.isFailure(fenced)).toBe(true);

          const records = yield* store
            .read(ConversationRead.make({ conversationId, limit: 1_024 }))
            .pipe(Stream.runCollect);
          expect(records).toHaveLength(1);
        }),
      ),
    ),
  );

  it.effect("resumes observation from the opaque offset returned by the prior record", () =>
    withTemporaryDatabase((filename) =>
      withStorage(
        filename,
        Effect.gen(function* () {
          const store = yield* ConversationStore;
          yield* store.materialize(
            ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
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
          const resumed = yield* store
            .observe(
              ConversationObservation.make({
                conversationId,
                afterOffset: existing[0]!.offset,
              }),
            )
            .pipe(Stream.take(1), Stream.runCollect);
          expect(resumed.map((record) => record.record.recordId)).toEqual([
            existing[1]!.record.recordId,
          ]);

          yield* store.materialize(
            ConversationMaterialization.make({
              conversationId: secondConversationId,
              producerEpoch: 1,
            }),
          );
          yield* store.append(
            FencedAppendRequest.make({
              conversationId: secondConversationId,
              batch: batch("observe-foreign", [inputRecord("observe-foreign-record", "foreign")]),
              expectedTailSequence: 0,
              expectedTailDigest: EMPTY_TAIL_DIGEST,
              producerEpoch: 1,
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
          const foreignExit = yield* store
            .observe(
              ConversationObservation.make({
                conversationId,
                afterOffset: foreign[0]!.offset,
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

  it.effect("fails startup on corrupt current-version rows without mutating the log", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withStorage(
          filename,
          Effect.gen(function* () {
            const store = yield* ConversationStore;
            yield* store.materialize(
              ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
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

        const reopened = yield* withStorage(filename, ConversationStore).pipe(Effect.exit);
        expect(Exit.isFailure(reopened)).toBe(true);
        if (Exit.isFailure(reopened)) {
          expect(Cause.squash(reopened.cause)).toBeInstanceOf(SqliteStorageCorruptionError);
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
              ConversationMaterialization.make({ conversationId, producerEpoch: 1 }),
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

        const reopened = yield* withStorage(
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

  it.effect("exposes deterministic before/after mutation failpoints with recoverable reopen", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const active = yield* Ref.make<SqliteStorageFailpointLocation | undefined>(undefined);
        const withFailpoints = <A, E>(
          effect: Effect.Effect<A, E, ConversationStore | SubmissionStore>,
        ) =>
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
                    producerEpoch: 1,
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
                    producerEpoch: 1,
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

import { activityProcessorStoreLayer } from "@effect-agent/storage-sqlite/sqlite-activity-store";
import { layer as sqliteThreadStoreLayer } from "@effect-agent/storage-sqlite/sqlite-thread-store";
import { type PreparedActivity } from "@effect-agent/thread/activity-store";
import { processCommittedActivity } from "@effect-agent/thread/committed-activity";
import { type CanonicalRecordEnvelope } from "@effect-agent/thread/records";
import { memoryStoreLayerWithFailpoints } from "@effect-agent/thread/sql-memory-store";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Config, Console, Effect, Layer, Ref, Schema } from "effect";
import {
  MemoryMutationFailpoint,
  MemoryWrite,
  MemoryWriter,
  type MemoryMutationPoint,
} from "effect-agent/memory-store";

import {
  ActivityMemoryOutput,
  DanStatement,
  DIVERGENT_TEXT,
  MEMORY_SCOPE,
  MemoryActivityMarker,
  MemoryActivityWorkerConfig,
  MemoryActivityWorkerResult,
  activityKey,
  memoryKey,
} from "./memory-activity-fixtures.ts";

const decodeConfig = Effect.fn("MemoryActivityWorker.decodeConfig")(function* () {
  const database = yield* Config.string("EFFECT_AGENT_MEMORY_ACTIVITY_DB");
  const mode = yield* Config.string("EFFECT_AGENT_MEMORY_ACTIVITY_MODE");

  return yield* Schema.decodeUnknownEffect(MemoryActivityWorkerConfig)({ database, mode });
});

const encodedLine = <A, I>(schema: Schema.Codec<A, I, never>, value: A) =>
  Schema.encodeEffect(Schema.fromJsonString(schema))(value);

const encodedMarker = Schema.encodeSync(Schema.fromJsonString(MemoryActivityMarker))({
  _tag: "MemoryActivityMarker",
  point: "memory:change:after",
});

const extractionOutput = (record: CanonicalRecordEnvelope, divergent: boolean) => {
  if (record.record.payload._tag !== "UserInputRecorded") {
    return Schema.decodeEffect(ActivityMemoryOutput)({ _tag: "Skip" });
  }

  return Schema.decodeUnknownEffect(DanStatement)(record.record.payload.input).pipe(
    Effect.flatMap((statement) =>
      Schema.decodeEffect(ActivityMemoryOutput)({
        _tag: "Remember",
        key: memoryKey,
        locator: statement.locator,
        content: {
          text: divergent ? DIVERGENT_TEXT : statement.text,
          attributions: [
            {
              originId: record.record.recordId,
              speaker: statement.speaker,
              observers: [statement.observer],
              locator: statement.locator,
              activityAt: statement.activityAt,
              interpretation: statement.interpretation,
            },
          ],
          metadata: {
            sourceThreadId: record.threadId,
            sourceSequence: record.sequence,
            sourceRecordId: record.record.recordId,
            sourceSchemaVersion: record.record.schemaVersion,
          },
          recordedAt: statement.recordedAt,
          extractedAt: 3_000,
        },
        scopes: [MEMORY_SCOPE],
      }),
    ),
  );
};

const applyPrepared = Effect.fn("MemoryActivityWorker.applyPrepared")(function* (
  work: PreparedActivity,
) {
  const output = yield* Schema.decodeUnknownEffect(ActivityMemoryOutput)(work.output);

  if (output._tag === "Skip") return;
  const writer = yield* MemoryWriter;

  yield* writer.change(
    yield* Schema.decodeEffect(MemoryWrite.Wire)({
      _tag: "Put",
      key: output.key,
      operationId: work.workId,
      expectedRevision: null,
      locator: output.locator,
      content: {
        ...output.content,
        metadata: {
          ...output.content.metadata,
          sourceRecordDigest: work.recordDigest,
          sourceWorkId: work.workId,
        },
      },
      scopes: output.scopes,
    }),
  );
});

/** Fixed child workflow. The entrypoint owns only NodeRuntime process handling. */
export const memoryActivityWorker = Effect.gen(function* () {
  const config = yield* decodeConfig();
  const appliedWorkIds = yield* Ref.make<ReadonlyArray<PreparedActivity["workId"]>>([]);
  const extractedUserRecords = yield* Ref.make(0);

  const failpoint = MemoryMutationFailpoint.of({
    hit: (point: MemoryMutationPoint) =>
      config.mode === "crash-after-apply" && point === "memory:change:after"
        ? Effect.gen(function* () {
            yield* Console.log(encodedMarker);

            return yield* Effect.never;
          })
        : Effect.void,
  });

  const sql = SqliteClient.layer({ filename: config.database, busyTimeout: 5_000 });

  const adapters = Layer.mergeAll(
    activityProcessorStoreLayer.pipe(Layer.provide(sql)),
    memoryStoreLayerWithFailpoints.pipe(
      Layer.provide(Layer.merge(sql, Layer.succeed(MemoryMutationFailpoint, failpoint))),
    ),
    sqliteThreadStoreLayer({ filename: config.database, busyTimeout: 5_000 }),
    NodeCrypto.layer,
  );

  const pass = yield* processCommittedActivity({
    key: activityKey,
    owner: config.mode === "crash-after-apply" ? "first-process" : "second-process",
    limits: { maxRecords: 16, pageSize: 4, timeoutMillis: 1_000, leaseMillis: 2_200 },
    extract: (record) =>
      extractionOutput(record, config.mode === "recover-divergent").pipe(
        Effect.tap(() =>
          record.record.payload._tag === "UserInputRecorded"
            ? Ref.update(extractedUserRecords, (count) => count + 1)
            : Effect.void,
        ),
        Effect.flatMap(Schema.encodeEffect(ActivityMemoryOutput)),
      ),
    apply: (work) =>
      applyPrepared(work).pipe(
        Effect.tap(() => Ref.update(appliedWorkIds, (ids) => [...ids, work.workId])),
      ),
  }).pipe(Effect.provide(adapters));

  yield* Console.log(
    yield* encodedLine(MemoryActivityWorkerResult, {
      _tag: "MemoryActivityWorkerResult",
      pass,
      appliedWorkIds: yield* Ref.get(appliedWorkIds),
      extractedUserRecords: yield* Ref.get(extractedUserRecords),
    }),
  );
});

import { ThreadId } from "@effect-agent/core/Identifiers";
import { digestJson, EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import { CanonicalBatch, CanonicalSequence, ProducerEpoch } from "@effect-agent/thread/Records";
import { AdmissionRequest, SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import {
  ThreadProjection,
  replayThread,
  replayThreadFromCheckpoint,
} from "@effect-agent/thread/ThreadProjection";
import {
  ThreadCheckpoint,
  ThreadExportRequest,
  ThreadMaterialization,
  ThreadRead,
  ThreadStore,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
} from "@effect-agent/thread/ThreadStore";
import { DateTime, Effect, Option, Schema, Stream } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { expect } from "vite-plus/test";

// Frozen bytes with required compatibility metadata, independent of the current encoder.
export const historicalCheckpointJson =
  '{"schemaVersion":1,"threadId":"checkpoint-history","throughSequence":0,"tailDigest":"0000000000000000000000000000000000000000000000000000000000000000","engineVersion":"historical-engine","agentDefinitionDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","modelDigest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","toolDigest":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","state":{"historical":true},"createdAt":"2026-09-01T00:00:00.000Z"}';

const threadId = Schema.decodeSync(ThreadId)("checkpoint-history");
const zero = Schema.decodeSync(CanonicalSequence)(0);
const epoch = Schema.decodeSync(ProducerEpoch)(1);

export const seedCheckpoint = Effect.fn("CheckpointFixture.seed")(function* (historical: boolean) {
  const store = yield* ThreadStore;
  const sql = yield* SqlClient.SqlClient;
  const checkpoints = store.checkpoints;

  expect(checkpoints).toBeDefined();
  if (checkpoints === undefined) return;

  yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch: epoch }));
  const ledger = yield* SubmissionLedger;

  const pending = yield* Schema.decodeUnknownEffect(AdmissionRequest)({
    threadId,
    principal: "checkpoint-principal",
    idempotencyKey: "checkpoint-pending",
    agentId: "checkpoint-agent",
    deploymentId: "checkpoint-deployment",
    agentDigests: { agent: EMPTY_TAIL_DIGEST, model: EMPTY_TAIL_DIGEST, tools: EMPTY_TAIL_DIGEST },
    inputPayload: "pending input",
    inputDigest: yield* digestJson("pending input"),
  });

  yield* ledger.admit(pending);
  let tail = { lastSequence: zero, tailDigest: EMPTY_TAIL_DIGEST };

  for (const index of [1, 2]) {
    const batch = yield* Schema.decodeUnknownEffect(CanonicalBatch)({
      batchId: `checkpoint-batch-${index}`,
      producerId: "checkpoint-producer",
      records: [
        {
          recordId: `checkpoint-record-${index}`,
          family: "thread",
          schemaVersion: 1,
          createdAt: "2026-09-01T00:00:00.000Z",
          deploymentId: "checkpoint-deployment",
          payload: {
            _tag: "UserInputRecorded",
            submissionId: `checkpoint-submission-${index}`,
            kind: "user",
            input: `input-${index}`,
          },
        },
      ],
    });

    tail = yield* store.append(
      FencedAppendRequest.make({
        threadId,
        batch,
        expectedTailSequence: tail.lastSequence,
        expectedTailDigest: tail.tailDigest,
        producerEpoch: epoch,
      }),
    );
    if (index === 1 && !historical) {
      const records = yield* store
        .read(ThreadRead.make({ threadId, limit: 1024 }))
        .pipe(Stream.runCollect);

      const checkpoint = ThreadCheckpoint.make({
        schemaVersion: 1,
        threadId,
        throughSequence: tail.lastSequence,
        tailDigest: tail.tailDigest,
        state: yield* Schema.encodeEffect(ThreadProjection)(
          replayThread(threadId, records, tail.tailDigest),
        ),
        createdAt: DateTime.makeUnsafe("2026-09-01T00:00:00.000Z"),
      });

      yield* checkpoints.save(SaveCheckpointRequest.make({ checkpoint }));
    }
  }
  if (historical) {
    yield* sql`INSERT INTO effect_agent_checkpoints (thread_id, through_sequence, tail_digest, checkpoint_json)
      VALUES (${threadId}, 0, ${EMPTY_TAIL_DIGEST}, ${historicalCheckpointJson})`;
  }
});

export const assertCheckpoint = Effect.fn("CheckpointFixture.assert")(function* (
  historical: boolean,
) {
  const store = yield* ThreadStore;
  const sql = yield* SqlClient.SqlClient;
  const checkpoints = store.checkpoints;

  expect(checkpoints).toBeDefined();
  if (checkpoints === undefined) return;
  const loaded = yield* checkpoints.load(LoadCheckpointRequest.make({ threadId }));

  expect(Option.isSome(loaded)).toBe(true);
  if (Option.isNone(loaded)) return;
  const checkpoint = loaded.value;
  const encoded = yield* Schema.encodeEffect(ThreadCheckpoint)(checkpoint);

  yield* checkpoints.save(SaveCheckpointRequest.make({ checkpoint }));

  const stateConflict = yield* checkpoints
    .save(
      SaveCheckpointRequest.make({
        checkpoint: ThreadCheckpoint.make({ ...checkpoint, state: { changed: true } }),
      }),
    )
    .pipe(Effect.flip);

  expect(stateConflict).toMatchObject({ _tag: "CheckpointRejected", reason: "digest-mismatch" });
  if (historical) {
    expect(JSON.stringify(encoded)).toBe(historicalCheckpointJson);

    const withoutMetadata = ThreadCheckpoint.make({
      schemaVersion: checkpoint.schemaVersion,
      threadId: checkpoint.threadId,
      throughSequence: checkpoint.throughSequence,
      tailDigest: checkpoint.tailDigest,
      state: checkpoint.state,
      createdAt: checkpoint.createdAt,
    });

    const removalConflict = yield* checkpoints
      .save(SaveCheckpointRequest.make({ checkpoint: withoutMetadata }))
      .pipe(Effect.flip);

    expect(removalConflict).toMatchObject({
      _tag: "CheckpointRejected",
      reason: "digest-mismatch",
    });
    for (const field of [
      "engineVersion",
      "agentDefinitionDigest",
      "modelDigest",
      "toolDigest",
    ] as const) {
      const changed = ThreadCheckpoint.make({
        ...checkpoint,
        [field]: field === "engineVersion" ? "changed-engine" : EMPTY_TAIL_DIGEST,
      });

      const failure = yield* checkpoints
        .save(SaveCheckpointRequest.make({ checkpoint: changed }))
        .pipe(Effect.flip);

      expect(failure).toMatchObject({ _tag: "CheckpointRejected", reason: "digest-mismatch" });
    }

    const rows = yield* sql<{
      checkpoint_json: string;
    }>`SELECT checkpoint_json FROM effect_agent_checkpoints WHERE thread_id = ${threadId}`;

    expect(rows[0].checkpoint_json).toBe(historicalCheckpointJson);
  } else {
    expect(Object.keys(encoded)).toEqual([
      "schemaVersion",
      "threadId",
      "throughSequence",
      "tailDigest",
      "state",
      "createdAt",
    ]);
    const projection = yield* Schema.decodeUnknownEffect(ThreadProjection)(checkpoint.state);

    const suffix = yield* store
      .read(ThreadRead.make({ threadId, afterSequence: checkpoint.throughSequence, limit: 1024 }))
      .pipe(Stream.runCollect);

    const full = yield* store.export(ThreadExportRequest.make({ threadId }));

    expect(suffix).toHaveLength(1);
    expect(replayThreadFromCheckpoint(projection, suffix, full.tailDigest)).toEqual(
      replayThread(threadId, full.records, full.tailDigest),
    );
  }
});

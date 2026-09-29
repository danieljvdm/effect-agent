import { DateTime, Effect, Schema } from "effect";

import type { AgentId, ThreadId } from "../../core/Identifiers.ts";
import { WorkerError } from "../../core/Worker.ts";
import { DurableRuntimeFailpoint } from "../DurableFailpoint.ts";
import {
  BatchId,
  CanonicalBatch,
  RecordEnvelope,
  RecordId,
  ThreadCreated,
  WorkerOrigin,
  WorkerOriginRecorded,
  type DefinitionDigests,
  type DeploymentId,
  type ProducerId,
} from "../Records.ts";
import { workerOriginRecordId } from "../RunJournal.ts";
import type {
  AppendConflict,
  FenceRejected,
  ThreadNotMaterialized,
  ThreadStoreError,
} from "../ThreadStore.ts";
import {
  FencedAppendRequest,
  ThreadIdentityRequest,
  ThreadTailRequest,
  ThreadStore,
} from "../ThreadStore.ts";
import { WakeScheduler } from "../WakeScheduler.ts";
import { definitionDigestsEqual } from "./agent-registration.ts";

interface InitializationConfig {
  readonly producerId: ProducerId;
  readonly deploymentId: DeploymentId;
}

/** Stable identities retained across admission retries and recovery. */
export const threadCreatedBatchId = (threadId: ThreadId): BatchId =>
  Schema.decodeSync(BatchId)(`thread-created:${threadId}`);

export const threadCreatedRecordId = (threadId: ThreadId): RecordId =>
  Schema.decodeSync(RecordId)(`thread-created:${threadId}`);

/**
 * Coordinator invariant: every Thread's first canonical record is `ThreadCreated`,
 * so `tailSequence >= 1` is the deterministic already-created check. A lost race (conflict or
 * fence) is verified against that invariant instead of being trusted blindly. An admitted
 * child can be claimed before it is runnable; that advances the shared storage fence without
 * appending anything. Re-read and retry that initialization race with the current fence.
 */
export const ensureThreadCreated = Effect.fn("DurableAgentRuntime.ensureThreadCreated")(
  function* (
    config: InitializationConfig,
    threadId: ThreadId,
    agentId: AgentId,
    definitions: DefinitionDigests,
  ): Effect.fn.Return<
    void,
    ThreadStoreError | ThreadNotMaterialized | AppendConflict | FenceRejected,
    ThreadStore | WakeScheduler
  > {
    const store = yield* ThreadStore;
    const wake = yield* WakeScheduler;
    const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

    if (tail.tailSequence > 0) return;

    const record = yield* Effect.map(DateTime.now, (createdAt) =>
      RecordEnvelope.make({
        recordId: threadCreatedRecordId(threadId),
        family: "thread",
        schemaVersion: 1,
        createdAt,
        deploymentId: config.deploymentId,
        payload: ThreadCreated.make({ agentId, definitions }),
      }),
    );

    yield* store
      .append(
        FencedAppendRequest.make({
          threadId,
          batch: CanonicalBatch.make({
            batchId: threadCreatedBatchId(threadId),
            producerId: config.producerId,
            records: [record],
          }),
          expectedTailSequence: tail.tailSequence,
          expectedTailDigest: tail.tailDigest,
          producerEpoch: tail.producerEpoch,
        }),
      )
      .pipe(
        Effect.catchTag(["AppendConflict", "FenceRejected"], (error) =>
          store
            .inspectTail(ThreadTailRequest.make({ threadId }))
            .pipe(
              Effect.flatMap((current) =>
                current.tailSequence > 0 ? Effect.void : Effect.fail(error),
              ),
            ),
        ),
        Effect.andThen(wake.notify(threadId)),
        Effect.asVoid,
      );
  },
  (effect) =>
    effect.pipe(
      Effect.retry({
        times: 7,
        while: (error) => error._tag === "AppendConflict" || error._tag === "FenceRejected",
      }),
    ),
);

const sameOrigin = Schema.toEquivalence(WorkerOrigin);

export const ensureWorkerOrigin = Effect.fn("WorkerHost.ensureOrigin")(
  function* (config: InitializationConfig, origin: WorkerOrigin) {
    const store = yield* ThreadStore;
    const failpoint = yield* DurableRuntimeFailpoint;

    for (let attempt = 0; attempt < 16; attempt++) {
      const current = yield* store.readIdentity(
        ThreadIdentityRequest.make({ threadId: origin.worker.threadId }),
      );

      const existing = current.records.find(
        ({ record }) => record.payload._tag === "WorkerOriginRecorded",
      )?.record.payload;

      if (existing?._tag === "WorkerOriginRecorded") {
        if (!sameOrigin(existing.origin, origin))
          return yield* WorkerError.make({ operation: "start", reason: "worker-mismatch" });

        return;
      }
      const first = current.records[0]?.record.payload;

      if (
        first?._tag !== "ThreadCreated" ||
        first.agentId !== origin.worker.targetAgentId ||
        !definitionDigestsEqual(first.definitions, origin.targetDigests) ||
        current.records.some(({ record }) => record.payload._tag === "SubagentLineageRecorded")
      )
        return yield* WorkerError.make({ operation: "start", reason: "worker-mismatch" });
      const id = workerOriginRecordId(origin.worker.threadId);

      yield* failpoint.hit("worker:before-origin-append");

      const appended = yield* store
        .append(
          FencedAppendRequest.make({
            threadId: origin.worker.threadId,
            producerEpoch: current.producerEpoch,
            expectedTailSequence: current.tailSequence,
            expectedTailDigest: current.tailDigest,
            batch: CanonicalBatch.make({
              batchId: Schema.decodeSync(BatchId)(id),
              producerId: config.producerId,
              records: [
                RecordEnvelope.make({
                  recordId: id,
                  family: "thread",
                  schemaVersion: 1,
                  createdAt: DateTime.makeUnsafe(origin.createdAtMillis),
                  deploymentId: config.deploymentId,
                  payload: WorkerOriginRecorded.make({ origin }),
                }),
              ],
            }),
          }),
        )
        .pipe(
          Effect.as(true),
          Effect.catchTag(["AppendConflict", "FenceRejected"], () => Effect.succeed(false)),
        );

      if (appended) {
        yield* failpoint.hit("worker:after-origin-append");

        return;
      }
    }

    return yield* WorkerError.make({ operation: "start", reason: "storage" });
  },
  Effect.mapError((cause) =>
    cause._tag === "WorkerError"
      ? cause
      : WorkerError.make({ operation: "start", reason: "storage", cause }),
  ),
);

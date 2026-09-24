import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Context, DateTime, Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import { EMPTY_TAIL_DIGEST, digestJson } from "effect-agent/digest";
import { DurableAgentRuntime, DurableRuntimeConfig } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "effect-agent/durable-failpoint";
import { AgentId, ThreadId } from "effect-agent/identifiers";
import type { WorkerOrigin } from "effect-agent/records";
import {
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  ThreadCreated,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerEpoch,
  ProducerId,
  RepairAnnotated,
  UserInputRecorded,
  WorkerOriginRecorded,
} from "effect-agent/records";
import { runIdForSubmission } from "effect-agent/run-journal";
import {
  AbortCommand,
  AdmissionRequest,
  IdempotencyKey,
  MarkReadyRequest,
  Principal,
  SubmissionLedger,
  submissionInputBatchId,
  submissionInputRecordId,
} from "effect-agent/submission-ledger";
import {
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadStore,
  ThreadTailRequest,
  FencedAppendRequest,
} from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";

class RecoveryReadProbe extends Context.Service<
  RecoveryReadProbe,
  {
    readonly failReadAfter: (sequence: CanonicalSequence) => Effect.Effect<void>;
  }
>()("@effect-agent/storage-memory/test/RecoveryReadProbe") {}

const countingThreadStoreLayer = Layer.effectContext(
  Effect.gen(function* () {
    const store = yield* ThreadStore;
    const failingAfter = yield* Ref.make<Option.Option<CanonicalSequence>>(Option.none());

    const counted = ThreadStore.of({
      readIdentity: store.readIdentity,
      materialize: store.materialize,
      append: store.append,
      read: (request) =>
        Stream.unwrap(
          Effect.gen(function* () {
            if ("selection" in request) return store.read(request);
            const failure = yield* Ref.get(failingAfter);

            if (Option.isSome(failure) && request.afterSequence === failure.value) {
              yield* Ref.set(failingAfter, Option.none());

              return Stream.fail(ThreadNotMaterialized.make({ threadId: request.threadId }));
            }

            return store.read(request);
          }),
        ),
      observe: store.observe,
      export: store.export,
      inspectTail: store.inspectTail,
      checkpoints: store.checkpoints,
    });

    return Context.make(ThreadStore, counted).pipe(
      Context.add(
        RecoveryReadProbe,
        RecoveryReadProbe.of({
          failReadAfter: (sequence) => Ref.set(failingAfter, Option.some(sequence)),
        }),
      ),
    );
  }),
).pipe(Layer.provide(MemoryThreadStoreLive));

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeAgentId = Schema.decodeSync(AgentId);
const decodeDeploymentId = Schema.decodeSync(DeploymentId);
const decodeDigest = Schema.decodeSync(Digest);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodePrincipal = Schema.decodeSync(Principal);
const decodeProducerEpoch = Schema.decodeSync(ProducerEpoch);
const decodeProducerId = Schema.decodeSync(ProducerId);
const decodeBatchId = Schema.decodeSync(CanonicalBatch.fields.batchId);
const decodeRecordId = Schema.decodeSync(CanonicalRecord.fields.recordId);

const THREAD_ID = decodeThreadId("thread-recovery-history-bound");
const AGENT_ID = decodeAgentId("agent-recovery-history-bound");
const DEPLOYMENT_ID = decodeDeploymentId("deployment-recovery-history-bound");
const PRODUCER_ID = decodeProducerId("producer-recovery-history-bound");
const PRINCIPAL = decodePrincipal("principal-recovery-history-bound");
const FIRST_EPOCH = decodeProducerEpoch(1);
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const DIGEST = decodeDigest("a".repeat(64));
const DEFINITIONS = DefinitionDigests.make({ agent: DIGEST, model: DIGEST, tools: DIGEST });
const HISTORY_RECORDS = 2_050;
const HISTORY_TAIL = Schema.decodeSync(CanonicalSequence)(HISTORY_RECORDS);

const runtimeLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      countingThreadStoreLayer,
      MemorySubmissionLedgerLive,
      WakeScheduler.layerNoop,
      DurableRuntimeFailpoint.layer,
      DurableRuntimeConfig.layer({
        deploymentId: DEPLOYMENT_ID,
        producerId: PRODUCER_ID,
      }),
      ToolReconciler.uncertain,
    ).pipe(Layer.provideMerge(NodeCrypto.layer)),
  ),
);

const seedHistory = Effect.fn("RecoveryHistoryTest.seedHistory")(function* (
  workerOrigin?: WorkerOrigin,
) {
  const store = yield* ThreadStore;

  yield* store.materialize(
    ThreadMaterialization.make({
      threadId: THREAD_ID,
      producerEpoch: FIRST_EPOCH,
    }),
  );

  let tailSequence = ZERO_SEQUENCE;
  let tailDigest = EMPTY_TAIL_DIGEST;

  for (let start = 0; start < HISTORY_RECORDS; start += 256) {
    const size = Math.min(256, HISTORY_RECORDS - start);

    const records = Array.from({ length: size }, (_, offset) => {
      const sequence = start + offset;

      return CanonicalRecord.make({
        recordId: decodeRecordId(`history-seed:${sequence}`),
        family: "thread",
        schemaVersion: 1,
        createdAt: DateTime.toUtc(DateTime.makeUnsafe(sequence + 1)),
        deploymentId: DEPLOYMENT_ID,
        payload:
          sequence === 0
            ? ThreadCreated.make({ agentId: AGENT_ID, definitions: DEFINITIONS })
            : sequence === 1 && workerOrigin !== undefined
              ? WorkerOriginRecorded.make({ origin: workerOrigin })
              : RepairAnnotated.make({ reason: "history seed", details: { sequence } }),
      });
    });

    const [first, ...rest] = records;

    if (first === undefined) {
      return yield* Effect.die(new Error("The history seed must produce a non-empty batch"));
    }

    const appended = yield* store.append(
      FencedAppendRequest.make({
        threadId: THREAD_ID,
        batch: CanonicalBatch.make({
          batchId: decodeBatchId(`history-seed:${start}`),
          producerId: PRODUCER_ID,
          records: [first, ...rest],
        }),
        expectedTailSequence: tailSequence,
        expectedTailDigest: tailDigest,
        producerEpoch: FIRST_EPOCH,
      }),
    );

    tailSequence = appended.lastSequence;
    tailDigest = appended.tailDigest;
  }
});

describe("DurableAgentRuntime recovery history", () => {
  it.effect("STORE-015 issue #96: normalizes disappearance during suffix refresh", () =>
    Effect.gen(function* () {
      yield* seedHistory();
      const ledger = yield* SubmissionLedger;
      const runtime = yield* DurableAgentRuntime;
      const probe = yield* RecoveryReadProbe;
      const store = yield* ThreadStore;
      let prefixTail = HISTORY_TAIL;

      for (let index = 0; index < 2; index++) {
        const input = { work: `suffix-race-${index}` };
        const inputDigest = yield* digestJson(input);

        const admitted = yield* ledger.admit(
          AdmissionRequest.make({
            threadId: THREAD_ID,
            principal: PRINCIPAL,
            idempotencyKey: decodeIdempotencyKey(`recovery-suffix-race-${index}`),
            agentId: AGENT_ID,
            agentDigests: DEFINITIONS,
            deploymentId: DEPLOYMENT_ID,
            inputPayload: input,
            inputDigest,
          }),
        );

        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
        if (index === 0) {
          yield* ledger.requestAbort(
            AbortCommand.make({
              submissionId: admitted.submissionId,
              author: "issue-96-test",
              reason: "exercise suffix refresh after a repaired predecessor",
            }),
          );
        } else {
          // A lost input marker requires suffix repair; untouched ready input does not.
          const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId: THREAD_ID }));

          const appended = yield* store.append(
            FencedAppendRequest.make({
              threadId: THREAD_ID,
              expectedTailSequence: tail.tailSequence,
              expectedTailDigest: tail.tailDigest,
              producerEpoch: tail.producerEpoch,
              batch: CanonicalBatch.make({
                batchId: submissionInputBatchId(admitted.submissionId),
                producerId: PRODUCER_ID,
                records: [
                  CanonicalRecord.make({
                    recordId: submissionInputRecordId(admitted.submissionId),
                    family: "thread",
                    schemaVersion: 1,
                    createdAt: DateTime.toUtc(DateTime.makeUnsafe(HISTORY_RECORDS + 1)),
                    deploymentId: DEPLOYMENT_ID,
                    payload: UserInputRecorded.make({
                      submissionId: admitted.submissionId,
                      kind: "user",
                      runId: runIdForSubmission(admitted.submissionId),
                      input,
                    }),
                  }),
                ],
              }),
            }),
          );

          prefixTail = appended.lastSequence;
        }
      }

      yield* probe.failReadAfter(prefixTail);
      const result = yield* runtime.runRecovery();

      expect(result.reports).toEqual([]);
      expect(result.blocked).toMatchObject([
        {
          threadId: THREAD_ID,
          failure: { errorTag: "ThreadStoreError", operation: "read recovery history" },
        },
      ]);
      expect(result.blocked).toHaveLength(1);
    }).pipe(Effect.provide(runtimeLayer)),
  );
});

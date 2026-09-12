import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { AgentId, DelegationId, ThreadId } from "@effect-agent/core/Identifiers";
import {
  SubagentDelegationCaps,
  SubagentGrant,
  SubagentReservationAmounts,
} from "@effect-agent/core/SubagentContract";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { EMPTY_TAIL_DIGEST, digestJson } from "@effect-agent/thread/Digest";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpoint } from "@effect-agent/thread/DurableFailpoint";
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
  WorkerAdmission,
  WorkerOrigin,
  WorkerOriginRecorded,
} from "@effect-agent/thread/Records";
import { type RecoveryDecision } from "@effect-agent/thread/Recovery";
import { runIdForSubmission } from "@effect-agent/thread/RunJournal";
import {
  AbortCommand,
  AdmissionRequest,
  IdempotencyKey,
  MarkReadyRequest,
  Principal,
  SubmissionLedger,
  submissionInputBatchId,
  submissionInputRecordId,
} from "@effect-agent/thread/SubmissionLedger";
import { type ThreadRead } from "@effect-agent/thread/ThreadStore";
import {
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadStore,
  ThreadTailRequest,
  FencedAppendRequest,
} from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Context, DateTime, Effect, Layer, Option, Ref, Schema, Stream } from "effect";

class RecoveryReadProbe extends Context.Service<
  RecoveryReadProbe,
  {
    readonly failReadAfter: (sequence: CanonicalSequence) => Effect.Effect<void>;
    readonly requests: Effect.Effect<ReadonlyArray<ThreadRead>>;
    readonly exportedThreads: Effect.Effect<ReadonlyArray<ThreadId>>;
    readonly reset: Effect.Effect<void>;
  }
>()("@effect-agent/storage-memory/test/RecoveryReadProbe") {}

const countingThreadStoreLayer = Layer.effectContext(
  Effect.gen(function* () {
    const store = yield* ThreadStore;
    const requests = yield* Ref.make<ReadonlyArray<ThreadRead>>([]);
    const exportedThreads = yield* Ref.make<ReadonlyArray<ThreadId>>([]);
    const failingAfter = yield* Ref.make<Option.Option<CanonicalSequence>>(Option.none());

    const counted = ThreadStore.of({
      materialize: store.materialize,
      append: store.append,
      read: (request) =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* Ref.update(requests, (current) => [...current, request]);
            const failure = yield* Ref.get(failingAfter);

            if (Option.isSome(failure) && request.afterSequence === failure.value) {
              yield* Ref.set(failingAfter, Option.none());

              return Stream.fail(ThreadNotMaterialized.make({ threadId: request.threadId }));
            }

            return store.read(request);
          }),
        ),
      observe: store.observe,
      export: (request) =>
        Ref.update(exportedThreads, (current) => [...current, request.threadId]).pipe(
          Effect.andThen(store.export(request)),
        ),
      inspectTail: store.inspectTail,
      checkpoints: store.checkpoints,
    });

    return Context.make(ThreadStore, counted).pipe(
      Context.add(
        RecoveryReadProbe,
        RecoveryReadProbe.of({
          failReadAfter: (sequence) => Ref.set(failingAfter, Option.some(sequence)),
          requests: Ref.get(requests),
          exportedThreads: Ref.get(exportedThreads),
          reset: Ref.set(requests, []).pipe(Effect.andThen(Ref.set(exportedThreads, []))),
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
  it.effect(
    "repairs standard worker updates once per recovery Thread, including direct recovery",
    () =>
      Effect.gen(function* () {
        const firstMessageId = decodeIdempotencyKey("worker-recovery-0");

        const origin = WorkerOrigin.make({
          worker: {
            schemaVersion: 1,
            threadId: THREAD_ID,
            targetAgentId: AGENT_ID,
            delegationId: Schema.decodeSync(DelegationId)("reporting-worker"),
          },
          source: {
            _tag: "programmatic",
            threadId: decodeThreadId("worker-recovery-source"),
            agentId: AGENT_ID,
          },
          targetDigests: DEFINITIONS,
          policy: AgentPolicy.resolve(),
          budget: {
            caps: SubagentDelegationCaps.make({
              maxConcurrentChildren: 1,
              maxTotalChildInvocations: 4,
            }),
            allocation: SubagentReservationAmounts.make({
              turns: 12,
              toolCalls: 24,
              durationMillis: 300_000,
              inputTokens: 0,
              outputTokens: 0,
              costMicrousd: 0,
              resultBytes: 1_000,
            }),
          },
          grant: SubagentGrant.make({ maxDepth: 1, allowedToolNames: [] }),
          depth: 1,
          firstMessageId,
          createdAtMillis: 1,
          expiresAtMillis: 1_000_000,
          reporting: { mode: "standard", sourceDigests: DEFINITIONS },
        });

        yield* seedHistory(origin);
        const ledger = yield* SubmissionLedger;
        const runtime = yield* DurableAgentRuntime;
        const probe = yield* RecoveryReadProbe;

        for (let index = 0; index < 4; index++) {
          const input = { work: `worker-submission-${index}` };
          const messageId = decodeIdempotencyKey(`worker-recovery-${index}`);

          const admitted = yield* ledger.admit(
            AdmissionRequest.make({
              threadId: THREAD_ID,
              principal: PRINCIPAL,
              idempotencyKey: messageId,
              agentId: AGENT_ID,
              agentDigests: DEFINITIONS,
              deploymentId: DEPLOYMENT_ID,
              inputPayload: input,
              inputDigest: yield* digestJson(input),
              workerAdmission: WorkerAdmission.make({
                origin,
                messageId,
                parameters: input,
                createdAtMillis: index + 1,
              }),
            }),
          );

          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
        }
        yield* probe.reset;
        const reports = yield* runtime.runRecovery;

        expect(reports.map((report) => report.disposition)).toEqual([
          "deferred",
          "deferred",
          "deferred",
          "deferred",
        ]);
        expect(yield* probe.exportedThreads).toEqual([THREAD_ID]);
        const first = reports[0];

        if (first === undefined) return yield* Effect.die("Expected a pending worker submission");
        yield* probe.reset;
        expect((yield* runtime.recoverSubmission(first.submissionId)).disposition).toBe("deferred");
        expect(yield* probe.exportedThreads).toEqual([THREAD_ID]);
      }).pipe(Effect.provide(runtimeLayer)),
  );

  it.effect("STORE-015 issue #96: reads canonical pages once for mixed recovery decisions", () =>
    Effect.gen(function* () {
      yield* seedHistory();
      const ledger = yield* SubmissionLedger;
      const runtime = yield* DurableAgentRuntime;
      const probe = yield* RecoveryReadProbe;
      const admitted = [];

      for (let index = 0; index < 4; index++) {
        const input = { work: `submission-${index}` };
        const inputDigest = yield* digestJson(input);

        admitted.push(
          yield* ledger.admit(
            AdmissionRequest.make({
              threadId: THREAD_ID,
              principal: PRINCIPAL,
              idempotencyKey: decodeIdempotencyKey(`recovery-history-${index}`),
              agentId: AGENT_ID,
              agentDigests: DEFINITIONS,
              deploymentId: DEPLOYMENT_ID,
              inputPayload: input,
              inputDigest,
            }),
          ),
        );
      }
      const second = admitted[1];
      const third = admitted[2];
      const fourth = admitted[3];

      if (second === undefined || third === undefined || fourth === undefined) {
        return yield* Effect.die(new Error("The recovery fixture must admit four Submissions"));
      }
      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: second.submissionId }));
      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: third.submissionId }));
      yield* ledger.requestAbort(
        AbortCommand.make({
          submissionId: third.submissionId,
          author: "issue-96-test",
          reason: "exercise a mixed queued-abort recovery decision",
        }),
      );
      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: fourth.submissionId }));

      yield* probe.reset;
      const reports = yield* runtime.runRecovery;

      expect(reports.map((report) => report.decision._tag)).toEqual([
        "RepairReadiness",
        "ApplyInput",
        "SettleAborted",
        "ApplyInput",
      ] satisfies ReadonlyArray<RecoveryDecision["_tag"]>);
      expect(reports.map((report) => report.disposition)).toEqual([
        "repaired",
        "deferred",
        "repaired",
        "deferred",
      ]);

      const requests = yield* probe.requests;

      expect(
        requests.map((request) => ({
          afterSequence: request.afterSequence,
          limit: request.limit,
        })),
      ).toEqual([
        { afterSequence: undefined, limit: 1_024 },
        { afterSequence: 1_024, limit: 1_024 },
        { afterSequence: 2_048, limit: 2 },
      ]);
    }).pipe(Effect.provide(runtimeLayer)),
  );

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

      yield* probe.reset;
      yield* probe.failReadAfter(prefixTail);
      const failure = yield* runtime.runRecovery.pipe(Effect.flip);

      expect(failure).toMatchObject({
        _tag: "ThreadStoreError",
        operation: "read recovery history",
      });

      const requests = (yield* probe.requests).map((request) => ({
        afterSequence: request.afterSequence,
        limit: request.limit,
      }));

      expect(requests.slice(0, 3)).toEqual([
        { afterSequence: undefined, limit: 1_024 },
        { afterSequence: 1_024, limit: 1_024 },
        { afterSequence: 2_048, limit: 3 },
      ]);
      expect(requests.at(-1)).toMatchObject({
        afterSequence: prefixTail,
      });
    }).pipe(Effect.provide(runtimeLayer)),
  );
});

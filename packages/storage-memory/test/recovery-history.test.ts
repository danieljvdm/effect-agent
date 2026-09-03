import { AgentId, ThreadId } from "@effect-agent/core/Identifiers";
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
} from "@effect-agent/thread/Records";
import { type RecoveryDecision } from "@effect-agent/thread/Recovery";
import {
  AbortCommand,
  AdmissionRequest,
  IdempotencyKey,
  MarkReadyRequest,
  Principal,
  SubmissionLedger,
} from "@effect-agent/thread/SubmissionLedger";
import { type ThreadRead } from "@effect-agent/thread/ThreadStore";
import {
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadStore,
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
    readonly reset: Effect.Effect<void>;
  }
>()("@effect-agent/storage-memory/test/RecoveryReadProbe") {}

const countingThreadStoreLayer = Layer.effectContext(
  Effect.gen(function* () {
    const store = yield* ThreadStore;
    const requests = yield* Ref.make<ReadonlyArray<ThreadRead>>([]);
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
      export: store.export,
      inspectTail: store.inspectTail,
      checkpoints: store.checkpoints,
    });

    return Context.make(ThreadStore, counted).pipe(
      Context.add(
        RecoveryReadProbe,
        RecoveryReadProbe.of({
          failReadAfter: (sequence) => Ref.set(failingAfter, Option.some(sequence)),
          requests: Ref.get(requests),
          reset: Ref.set(requests, []),
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

const seedHistory = Effect.fn("RecoveryHistoryTest.seedHistory")(function* () {
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
        }
      }

      yield* probe.reset;
      yield* probe.failReadAfter(HISTORY_TAIL);
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
        { afterSequence: 2_048, limit: 2 },
      ]);
      expect(requests.at(-1)).toMatchObject({
        afterSequence: HISTORY_TAIL,
      });
    }).pipe(Effect.provide(runtimeLayer)),
  );
});

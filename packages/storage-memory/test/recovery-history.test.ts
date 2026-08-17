import { AgentId, ConversationId } from "@effect-agent/core";
import {
  AbortCommand,
  AdmissionRequest,
  CanonicalBatch,
  CanonicalRecord,
  CanonicalSequence,
  ConversationCreated,
  ConversationMaterialization,
  ConversationRead,
  ConversationStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  IdempotencyKey,
  MarkReadyRequest,
  Principal,
  ProducerEpoch,
  ProducerId,
  RecoveryDecision,
  RepairAnnotated,
  SubmissionLedger,
  ToolReconciler,
  WakeScheduler,
  digestJson,
} from "@effect-agent/session";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Context, DateTime, Effect, Layer, Ref, Schema, Stream } from "effect";

import { MemoryConversationStoreLive, MemorySubmissionLedgerLive } from "../src/index.ts";

class RecoveryReadProbe extends Context.Service<
  RecoveryReadProbe,
  {
    readonly requests: Effect.Effect<ReadonlyArray<ConversationRead>>;
    readonly reset: Effect.Effect<void>;
  }
>()("@effect-agent/storage-memory/test/RecoveryReadProbe") {}

const countingConversationStoreLayer = Layer.effectContext(
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    const requests = yield* Ref.make<ReadonlyArray<ConversationRead>>([]);
    const counted = ConversationStore.of({
      materialize: store.materialize,
      append: store.append,
      read: (request) =>
        Stream.unwrap(
          Ref.update(requests, (current) => [...current, request]).pipe(
            Effect.as(store.read(request)),
          ),
        ),
      observe: store.observe,
      export: store.export,
      inspectTail: store.inspectTail,
      saveCheckpoint: store.saveCheckpoint,
      loadCheckpoint: store.loadCheckpoint,
    });
    return Context.make(ConversationStore, counted).pipe(
      Context.add(
        RecoveryReadProbe,
        RecoveryReadProbe.of({
          requests: Ref.get(requests),
          reset: Ref.set(requests, []),
        }),
      ),
    );
  }),
).pipe(Layer.provide(MemoryConversationStoreLive));

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeAgentId = Schema.decodeSync(AgentId);
const decodeDeploymentId = Schema.decodeSync(DeploymentId);
const decodeDigest = Schema.decodeSync(Digest);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodePrincipal = Schema.decodeSync(Principal);
const decodeProducerEpoch = Schema.decodeSync(ProducerEpoch);
const decodeProducerId = Schema.decodeSync(ProducerId);
const decodeBatchId = Schema.decodeSync(CanonicalBatch.fields.batchId);
const decodeRecordId = Schema.decodeSync(CanonicalRecord.fields.recordId);

const CONVERSATION_ID = decodeConversationId("conversation-recovery-history-bound");
const AGENT_ID = decodeAgentId("agent-recovery-history-bound");
const DEPLOYMENT_ID = decodeDeploymentId("deployment-recovery-history-bound");
const PRODUCER_ID = decodeProducerId("producer-recovery-history-bound");
const PRINCIPAL = decodePrincipal("principal-recovery-history-bound");
const FIRST_EPOCH = decodeProducerEpoch(1);
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
const DIGEST = decodeDigest("a".repeat(64));
const DEFINITIONS = DefinitionDigests.make({ agent: DIGEST, model: DIGEST, tools: DIGEST });
const HISTORY_RECORDS = 2_050;

const runtimeLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      countingConversationStoreLayer,
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
  const store = yield* ConversationStore;
  yield* store.materialize(
    ConversationMaterialization.make({
      conversationId: CONVERSATION_ID,
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
        family: "conversation",
        schemaVersion: 1,
        createdAt: DateTime.toUtc(DateTime.makeUnsafe(sequence + 1)),
        deploymentId: DEPLOYMENT_ID,
        payload:
          sequence === 0
            ? ConversationCreated.make({ agentId: AGENT_ID, definitions: DEFINITIONS })
            : RepairAnnotated.make({ reason: "history seed", details: { sequence } }),
      });
    });
    const [first, ...rest] = records;
    if (first === undefined) {
      return yield* Effect.die(new Error("The history seed must produce a non-empty batch"));
    }
    const appended = yield* store.append(
      FencedAppendRequest.make({
        conversationId: CONVERSATION_ID,
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
              conversationId: CONVERSATION_ID,
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
});

import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpoint } from "@effect-agent/thread/DurableFailpoint";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
  ThreadCreated,
} from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import {
  FencedAppendRequest,
  ThreadMaterialization,
  ThreadStore,
} from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Array, DateTime, Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const definition = Agent.make("history-cost", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const response: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const base = Layer.mergeAll(
  MemoryThreadStoreLive,
  MemorySubmissionLedgerLive,
  WakeScheduler.layerNoop,
  ToolReconciler.uncertain,
  DurableRuntimeFailpoint.layer,
  RunToolAuthorization.allowAll,
  DurableRuntimeConfig.layer({
    deploymentId: Schema.decodeSync(DeploymentId)("history-cost"),
    producerId: Schema.decodeSync(ProducerId)("history-cost"),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

const measure = Effect.fn("RuntimeHistoryCost.measure")(function* (historySize: number) {
  const store = yield* ThreadStore;
  const threadId = Schema.decodeSync(ThreadId)(`history-cost-${historySize}`);
  const producerEpoch = Schema.decodeSync(ProducerEpoch)(0);
  const createdAt = yield* DateTime.now;
  let tailSequence = Schema.decodeSync(CanonicalSequence)(0);
  let tailDigest = EMPTY_TAIL_DIGEST;

  yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch }));
  for (let start = 0; start < historySize; start += 256) {
    const records = Array.makeBy(Math.min(256, historySize - start), (offset) =>
      RecordEnvelope.make({
        recordId: Schema.decodeSync(RecordId)(`history-seed:${start + offset}`),
        family: "thread",
        schemaVersion: 1,
        deploymentId: Schema.decodeSync(DeploymentId)("history-cost"),
        createdAt,
        payload:
          start + offset === 0
            ? ThreadCreated.make({ agentId: definition.id, definitions })
            : RepairAnnotated.make({ reason: "history-cost", details: { text: "x".repeat(128) } }),
      }),
    );

    const appended = yield* store.append(
      FencedAppendRequest.make({
        threadId,
        producerEpoch,
        expectedTailSequence: tailSequence,
        expectedTailDigest: tailDigest,
        batch: CanonicalBatch.make({
          batchId: Schema.decodeSync(BatchId)(`history-seed:${start}`),
          producerId: Schema.decodeSync(ProducerId)("history-cost"),
          records,
        }),
      }),
    );

    tailSequence = appended.lastSequence;
    tailDigest = appended.tailDigest;
  }
  let returnedRecords = 0;
  let measured = false;

  const counted = ThreadStore.of({
    ...store,
    read: (request) =>
      store.read(request).pipe(
        Stream.tap(() =>
          Effect.sync(() => {
            if (!measured) returnedRecords++;
          }),
        ),
      ),
  });

  const model = Model.make(
    "scripted",
    "history-cost",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () =>
          Stream.fromEffect(
            Effect.sync(() => {
              measured = true;
            }),
          ).pipe(Stream.flatMap(() => Stream.fromIterable(response))),
      }),
    ),
  );

  const agent = Agent.withModel(definition, model);

  const runtime = yield* DurableAgentRuntime.pipe(
    Effect.provide(DurableAgentRuntime.layer),
    Effect.provideService(ThreadStore, counted),
  );

  yield* runtime.submit(agent, "measure", {
    threadId,
    principal: Schema.decodeSync(Principal)("history-cost"),
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("history-cost"),
    definitions,
  });
  yield* runtime.processThread(agent, threadId);

  return { returnedRecords, measured };
});

it.live("bounds startup history reads while retaining the complete fixed prefix", () =>
  Effect.gen(function* () {
    for (const historySize of [1, 11, 2051]) {
      const measurement = yield* measure(historySize).pipe(Effect.provide(base));

      expect(measurement.measured).toBe(true);
      expect(measurement.returnedRecords).toBeLessThanOrEqual(3 * historySize + 6);
    }
  }),
);

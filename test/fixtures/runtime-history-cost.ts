import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { RunId, ThreadId } from "@effect-agent/core/Identifiers";
import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  CompactionCreated,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ModelCompleted,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
  ThreadCreated,
} from "@effect-agent/thread/Records";
import {
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  SubmissionLedger,
} from "@effect-agent/thread/SubmissionLedger";
import {
  FencedAppendRequest,
  ThreadMaterialization,
  ThreadStore,
  ThreadStoreError,
  ThreadTailRequest,
  type ThreadRead,
} from "@effect-agent/thread/ThreadStore";
import { Array, DateTime, Effect, Exit, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, Toolkit, type Response } from "effect/unstable/ai";
import { expect } from "vite-plus/test";

const digest = Schema.decodeSync(Digest)("a".repeat(64));

export const historyDefinitions = DefinitionDigests.make({
  agent: digest,
  model: digest,
  tools: digest,
});

export const historyDefinition = Agent.make("history-cost", {
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

export const historyResponse: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

export type ReadFault = "gap" | "short" | "failure" | "defect" | "interruption";
export type ReadPhase = "prefix" | "suffix" | "fold";

/** The same retained-history fixture serves the memory and real SQLite work-budget gates. */
export const seedHistory = Effect.fn("RuntimeHistoryCost.seed")(function* (
  historySize: number,
  withCompaction = false,
) {
  const store = yield* ThreadStore;
  const threadId = Schema.decodeSync(ThreadId)(`history-cost-${historySize}`);
  const producerEpoch = Schema.decodeSync(ProducerEpoch)(0);
  const createdAt = yield* DateTime.now;
  let tailSequence = Schema.decodeSync(CanonicalSequence)(0);
  let tailDigest = EMPTY_TAIL_DIGEST;
  const retainedInputs: Array<string> = [];

  yield* store.materialize(ThreadMaterialization.make({ threadId, producerEpoch }));
  for (let start = 0; start < historySize; start += 256) {
    const records = Array.makeBy(Math.min(256, historySize - start), (offset) => {
      const position = start + offset;
      const input = `retained input ${position}`;
      const compaction = withCompaction && position === 128;
      const retained = position > 0 && position % 4 === 0 && !compaction;

      if (retained) retainedInputs.push(input);

      return RecordEnvelope.make({
        recordId: Schema.decodeSync(RecordId)(`history-seed:${start + offset}`),
        family: "thread",
        schemaVersion: 1,
        deploymentId: Schema.decodeSync(DeploymentId)("history-cost"),
        createdAt,
        payload:
          start + offset === 0
            ? ThreadCreated.make({ agentId: historyDefinition.id, definitions: historyDefinitions })
            : compaction
              ? CompactionCreated.make({
                  runId: RunId.make("prior-compaction"),
                  turn: 1,
                  kind: "summarize",
                  coversThrough: Schema.decodeSync(CanonicalSequence)(0),
                  summary: "Invalid zero coverage must not hide retained input",
                })
              : retained
                ? ModelCompleted.make({
                    runId: RunId.make(`retained:${position}`),
                    output: "retained answer",
                    messages: Schema.decodeUnknownSync(PersistedJson)(
                      Schema.encodeSync(Prompt.Prompt)(
                        Prompt.make([
                          { role: "user", content: input },
                          { role: "assistant", content: "retained answer" },
                        ]),
                      ),
                    ),
                  })
                : RepairAnnotated.make({
                    reason: "history-cost",
                    details: { text: "x".repeat(128) },
                  }),
      });
    });

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

  return { historySize, threadId, createdAt, retainedInputs };
});

export const measureHistory = Effect.fn("RuntimeHistoryCost.measure")(function* (
  seed: Effect.Success<ReturnType<typeof seedHistory>>,
  fault?: { readonly kind: ReadFault; readonly phase: ReadPhase },
  raceAppend = false,
) {
  const store = yield* ThreadStore;
  const { historySize, threadId, createdAt, retainedInputs } = seed;
  let returnedRecords = 0;
  let totalReturnedRecords = 0;
  let modelCalls = 0;
  let modelFinalizers = 0;
  let measured = false;

  let openedPages = 0;
  let closedPages = 0;
  let prefixTraversals = 0;
  let injected = false;
  let raced = false;
  const requests: Array<ThreadRead> = [];

  const counted = ThreadStore.of({
    ...store,
    read: (request) =>
      Stream.suspend(() => {
        openedPages++;
        requests.push(request);
        if (request.afterSequence === undefined) prefixTraversals++;

        const inject =
          !injected &&
          fault !== undefined &&
          (fault.phase === "suffix"
            ? request.afterSequence === historySize
            : request.afterSequence === 1_024 &&
              prefixTraversals === (fault.phase === "prefix" ? 1 : 2));

        if (inject) {
          injected = true;
          switch (fault.kind) {
            case "gap":
              return store.read(request).pipe(Stream.drop(1));
            case "short":
              return Stream.empty;
            case "failure":
              return Stream.fail(
                ThreadStoreError.make({ operation: "history test", message: "read unavailable" }),
              );
            case "defect":
              return Stream.die("read defect");
            case "interruption":
              return Stream.fromEffect(Effect.interrupt);
          }
        }

        if (raceAppend && !raced && request.afterSequence === 1_024) {
          raced = true;

          return Stream.unwrap(
            Effect.gen(function* () {
              const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
              const input = "retained input racing append";

              retainedInputs.push(input);
              yield* store
                .append(
                  FencedAppendRequest.make({
                    threadId,
                    producerEpoch: tail.producerEpoch,
                    expectedTailSequence: tail.tailSequence,
                    expectedTailDigest: tail.tailDigest,
                    batch: CanonicalBatch.make({
                      batchId: BatchId.make("racing-history"),
                      producerId: ProducerId.make("history-cost"),
                      records: [
                        RecordEnvelope.make({
                          recordId: RecordId.make("racing-history"),
                          family: "thread",
                          schemaVersion: 1,
                          createdAt,
                          deploymentId: DeploymentId.make("history-cost"),
                          payload: ModelCompleted.make({
                            runId: RunId.make("racing-history"),
                            output: "racing answer",
                            messages: Schema.decodeUnknownSync(PersistedJson)(
                              Schema.encodeSync(Prompt.Prompt)(
                                Prompt.make([
                                  { role: "user", content: input },
                                  { role: "assistant", content: "racing answer" },
                                ]),
                              ),
                            ),
                          }),
                        }),
                      ],
                    }),
                  }),
                )
                .pipe(Effect.orDie);

              return store.read(request);
            }),
          );
        }

        return store.read(request);
      }).pipe(
        Stream.tap(() =>
          Effect.sync(() => {
            totalReturnedRecords++;
            if (!measured) returnedRecords++;
          }),
        ),
        Stream.ensuring(
          Effect.sync(() => {
            closedPages++;
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
        streamText: (request) =>
          Stream.fromEffect(
            Effect.sync(() => {
              measured = true;
              modelCalls++;

              const userTexts = request.prompt.content.flatMap((message) =>
                message.role === "user"
                  ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
                  : [],
              );

              expect(userTexts.filter((text) => text.startsWith("retained input "))).toEqual(
                retainedInputs,
              );
            }),
          ).pipe(
            Stream.flatMap(() => Stream.fromIterable(historyResponse)),
            Stream.ensuring(
              Effect.sync(() => {
                modelFinalizers++;
              }),
            ),
          ),
      }),
    ),
  );

  const agent = Agent.withModel(historyDefinition, model);

  const runtime = yield* DurableAgentRuntime.pipe(
    // A Node host may already have acquired this Layer. Its memoized coordinator would
    // retain the unobserved store, so construct a fresh one over the counting decorator.
    Effect.provide(Layer.fresh(DurableAgentRuntime.layer)),
    Effect.provideService(ThreadStore, counted),
  );

  const receipt = yield* runtime.submit(agent, "measure", {
    threadId,
    principal: Schema.decodeSync(Principal)("history-cost"),
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("history-cost"),
    definitions: historyDefinitions,
  });

  const exit = yield* runtime.processThread(agent, threadId).pipe(Effect.exit);

  if (Exit.isSuccess(exit)) {
    const settled = yield* runtime.awaitSettlement(receipt);

    expect(exit.value).toEqual([settled]);
  }
  const ledger = yield* SubmissionLedger;

  const snapshot = yield* ledger.loadRecoverySnapshot(
    RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
  );

  return {
    returnedRecords,
    totalReturnedRecords,
    modelCalls,
    modelFinalizers,
    measured,
    exit,
    openedPages,
    closedPages,
    requests,
    injected,
    raced,
    snapshot,
  };
});

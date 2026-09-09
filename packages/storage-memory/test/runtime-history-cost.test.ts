import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { RunId, ThreadId } from "@effect-agent/core/Identifiers";
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
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Array, Cause, DateTime, Effect, Exit, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, Toolkit, type Response } from "effect/unstable/ai";

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

type ReadFault = "gap" | "short" | "failure" | "defect" | "interruption";
type ReadPhase = "prefix" | "suffix" | "fold";

const measure = Effect.fn("RuntimeHistoryCost.measure")(function* (
  historySize: number,
  fault?: { readonly kind: ReadFault; readonly phase: ReadPhase },
  raceAppend = false,
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
            ? ThreadCreated.make({ agentId: definition.id, definitions })
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
  let returnedRecords = 0;
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

              const userTexts = request.prompt.content.flatMap((message) =>
                message.role === "user"
                  ? message.content.flatMap((part) => (part.type === "text" ? [part.text] : []))
                  : [],
              );

              expect(userTexts.filter((text) => text.startsWith("retained input "))).toEqual(
                retainedInputs,
              );
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

  const receipt = yield* runtime.submit(agent, "measure", {
    threadId,
    principal: Schema.decodeSync(Principal)("history-cost"),
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("history-cost"),
    definitions,
  });

  const exit = yield* runtime.processThread(agent, threadId).pipe(Effect.exit);
  const ledger = yield* SubmissionLedger;

  const snapshot = yield* ledger.loadRecoverySnapshot(
    RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
  );

  return {
    returnedRecords,
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

it.live("bounds startup history reads while retaining the complete fixed prefix", () =>
  Effect.gen(function* () {
    for (const historySize of [1, 11, 2051]) {
      const measurement = yield* measure(historySize).pipe(Effect.provide(base));

      expect(Exit.isSuccess(measurement.exit)).toBe(true);
      expect(measurement.measured).toBe(true);
      expect(measurement.openedPages).toBe(measurement.closedPages);
      expect(measurement.snapshot.ownership).toBeUndefined();
      expect(measurement.returnedRecords).toBeLessThanOrEqual(2 * historySize + 6);
    }
  }),
);

it.live.each([
  { kind: "gap", phase: "prefix" },
  { kind: "short", phase: "prefix" },
  { kind: "failure", phase: "prefix" },
  { kind: "defect", phase: "prefix" },
  { kind: "interruption", phase: "prefix" },
  { kind: "gap", phase: "suffix" },
  { kind: "failure", phase: "suffix" },
  { kind: "interruption", phase: "suffix" },
  { kind: "gap", phase: "fold" },
  { kind: "short", phase: "fold" },
] satisfies ReadonlyArray<{ readonly kind: ReadFault; readonly phase: ReadPhase }>)(
  "releases startup reads and ownership after $phase $kind",
  (fault) =>
    Effect.gen(function* () {
      const result = yield* measure(1_025, fault).pipe(Effect.provide(base));

      expect(result.injected).toBe(true);
      expect(result.measured).toBe(false);
      expect(Exit.isFailure(result.exit)).toBe(true);
      expect(result.openedPages).toBe(result.closedPages);
      expect(result.snapshot.ownership).toBeUndefined();
      expect(result.snapshot.reservation).toBeUndefined();
      expect(result.requests.every((request) => request.limit <= 1_024)).toBe(true);
      if (Exit.isFailure(result.exit) && ["gap", "short", "failure"].includes(fault.kind))
        expect(Cause.hasFails(result.exit.cause)).toBe(true);
    }),
);

it.live("captures the initial tail and incorporates racing appends through a later suffix", () =>
  Effect.gen(function* () {
    const result = yield* measure(1_025, undefined, true).pipe(Effect.provide(base));

    expect(result.raced).toBe(true);
    expect(result.measured).toBe(true);
    expect(Exit.isSuccess(result.exit)).toBe(true);
    expect(
      result.requests.slice(0, 2).map(({ afterSequence, limit }) => [afterSequence ?? 0, limit]),
    ).toEqual([
      [0, 1_024],
      [1_024, 1],
    ]);
    expect(result.returnedRecords).toBeLessThanOrEqual(2 * 1_026 + 6);
    expect(result.openedPages).toBe(result.closedPages);
    expect(result.snapshot.ownership).toBeUndefined();
  }),
);

it.live("falls back to ordinary journal scans when any compaction metadata is present", () =>
  Effect.gen(function* () {
    const result = yield* measure(1_025, undefined, false, true).pipe(Effect.provide(base));

    expect(result.measured).toBe(true);
    expect(Exit.isSuccess(result.exit)).toBe(true);
    expect(result.returnedRecords).toBeGreaterThanOrEqual(3 * 1_025);
    expect(result.returnedRecords).toBeLessThanOrEqual(3 * 1_025 + 6);
    expect(result.openedPages).toBe(result.closedPages);
    expect(result.snapshot.ownership).toBeUndefined();
  }),
);

import {
  Agent,
  AgentPolicy,
  CompactionPolicy,
  ThreadId,
  ReceiptId,
  SubmissionId,
  ToolCallId,
  type IdGenerator,
} from "@effect-agent/core";
import {
  COMPACTION_SUMMARY_PREFIX,
  ContextCompactor,
  RunContextPreparation,
  RunToolAuthorization,
  estimatePromptTokens,
  ToolExecutionClass,
  ToolBroker,
} from "@effect-agent/engine";
import { MemoryThreadStoreLive, MemorySubmissionLedgerLive } from "@effect-agent/storage-memory";
import {
  AbortCommand,
  ApprovalDecisionCommand,
  BatchId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ThreadRead,
  ThreadStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpointError,
  IdempotencyKey,
  ModelResponseRecorded,
  ObservationOffset,
  PersistedJson,
  Principal,
  ProducerId,
  QueueSequence,
  Receipt,
  RecordEnvelope,
  RecoverySnapshotRequest,
  RunCompleted,
  Settlement,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  ToolReconciler,
  WakeScheduler,
  makeWakeSubscriptionHub,
  modelResponseRecordId,
  projectRunJournal,
  promptFromCanonicalRecords,
  replayThread,
  recoveryRepairRecordId,
  runCompletedRecordId,
  runIdForSubmission,
  runStartedRecordId,
  submissionInputRecordId,
  submissionSettlementRecordId,
  toolCallSettledRecordId,
  turnCanonicalBatch,
  turnIdForRun,
  type DurableRuntimeFailpointLocation,
  type DurableSubmitOptions,
  type AdmissionConflict,
  type FenceRejected,
  type SettlementConflict,
} from "@effect-agent/thread";
import { DurableRuntimeFailpointTestControl } from "@effect-agent/thread/testing";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import {
  AiError,
  LanguageModel,
  Model,
  Prompt,
  Tool,
  Toolkit,
  type Response,
} from "effect/unstable/ai";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-durable");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);

const submitOptions = (threadId: string, idempotencyKey: string): DurableSubmitOptions => ({
  threadId: decodeThreadId(threadId),
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: "search-1",
    name: "search",
    params: { query: "sea" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

const approvalCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: "book-progress-1",
    name: "book_progress",
    params: { ref: "#94" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/**
 * Scripted model whose call counter and captured request prompts live OUTSIDE the Model Layer,
 * so they survive Layer rebuilds across Attempts (each Attempt provides the Model afresh).
 */
const makeScriptedModel = (script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts: Array<Prompt.Prompt> = [];
    const model = Model.make(
      "scripted",
      "durable-test",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) =>
            Stream.unwrap(
              Ref.getAndUpdate(calls, (call) => call + 1).pipe(
                Effect.map((call) => {
                  prompts.push(request.prompt);
                  return Stream.fromIterable(script(call));
                }),
              ),
            ),
        }),
      ),
    );
    return { model, prompts };
  });

const plannerDefinition = Agent.make("durable-planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ question }) => `Answer ${question} as JSON.`,
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const RunDisposition = Schema.Literal("application-complete");
const dispositionDefinition = Agent.make("durable-run-disposition", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({
    answer: Schema.String,
    runDisposition: Schema.optionalKey(Schema.String),
  }),
  instructions: "Answer as JSON and declare application completion explicitly.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  runDisposition: {
    schema: RunDisposition,
    fromOutput: (output) => output.runDisposition,
  },
});

// `readonly` keeps the P4 canonical record shape byte-stable (plan §4.3): an unannotated tool
// fails closed to `uncertain` and gains `ToolCallPrepared` records under the P5 split commits.
const Search = Tool.make("search", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ available: Schema.Boolean }),
}).annotate(ToolExecutionClass, "readonly");
const searchTools = Toolkit.make(Search);
const searchDefinition = Agent.make("durable-search", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Search before answering.",
  toolkit: searchTools,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});
const searchToolLayer = searchTools.toLayer({
  search: () => Effect.succeed({ available: true }),
});

const BookProgress = Tool.make("book_progress", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});
const progressApprovalTools = Toolkit.make(BookProgress);
const progressApprovalDefinition = Agent.make("durable-progress-approval", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book only after approval.",
  toolkit: progressApprovalTools,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});
const progressApprovalToolLayer = progressApprovalTools.toLayer({
  book_progress: () => Effect.succeed({ confirmation: "confirmed-#94" }),
});

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-durable"),
  producerId: Schema.decodeSync(ProducerId)("producer-durable"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

const baseLayer = Layer.mergeAll(
  RunToolAuthorization.allowAll,
  MemorySubmissionLedgerLive,
  MemoryThreadStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layer.pipe(Layer.provideMerge(baseLayer));

const AnswerCompletionOutput = Schema.Struct({ answer: Schema.String });
const DeliveryCompletionOutput = Schema.Struct({
  messageId: Schema.String,
  delivered: Schema.Boolean,
});
const isAnswerCompletionOutput = Schema.is(AnswerCompletionOutput);
const isDeliveryCompletionOutput = Schema.is(DeliveryCompletionOutput);

const corruptCompletionOutput = (output: Schema.Json): Schema.Json => {
  if (isAnswerCompletionOutput(output)) return { answer: "hostile replacement" };
  if (isDeliveryCompletionOutput(output)) {
    return { messageId: "hostile-replacement", delivered: output.delivered };
  }
  return output;
};

const corruptCompletionEnvelope = (envelope: CanonicalRecordEnvelope): CanonicalRecordEnvelope => {
  const payload = envelope.record.payload;
  if (payload._tag !== "RunCompleted") return envelope;
  return CanonicalRecordEnvelope.make({
    ...envelope,
    record: RecordEnvelope.make({
      ...envelope.record,
      payload: RunCompleted.make({
        runId: payload.runId,
        output: corruptCompletionOutput(payload.output),
        ...(payload.runDisposition === undefined ? {} : { runDisposition: payload.runDisposition }),
        ...(payload.finishReason === undefined ? {} : { finishReason: payload.finishReason }),
        ...(payload.exhausted === undefined ? {} : { exhausted: payload.exhausted }),
      }),
    }),
  });
};

const corruptedCompletionStoreLayer = Layer.effect(
  ThreadStore,
  Effect.gen(function* () {
    const inner = yield* ThreadStore;
    return ThreadStore.of({
      ...inner,
      read: (request) => inner.read(request).pipe(Stream.map(corruptCompletionEnvelope)),
    });
  }),
).pipe(Layer.provide(MemoryThreadStoreLive));

const corruptedCompletionBaseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  corruptedCompletionStoreLayer,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const corruptedCompletionTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(corruptedCompletionBaseLayer),
);

const injectProviderExecutedCall = (messages: Schema.Json): Schema.Json => {
  const prompt = Schema.decodeUnknownSync(Prompt.Prompt)(messages);
  return Schema.decodeUnknownSync(PersistedJson)(
    Schema.encodeSync(Prompt.Prompt)(
      Prompt.fromMessages([
        ...prompt.content,
        Prompt.makeMessage("assistant", {
          content: [
            Prompt.makePart("tool-call", {
              id: "hostile-provider-call",
              name: "HostedSearch",
              params: { query: "hidden" },
              providerExecuted: true,
            }),
          ],
        }),
      ]),
    ),
  );
};

const injectProviderCallEnvelope = (envelope: CanonicalRecordEnvelope): CanonicalRecordEnvelope => {
  const payload = envelope.record.payload;
  if (payload._tag !== "ModelResponseRecorded") return envelope;
  return CanonicalRecordEnvelope.make({
    ...envelope,
    record: RecordEnvelope.make({
      ...envelope.record,
      payload: ModelResponseRecorded.make({
        runId: payload.runId,
        turnId: payload.turnId,
        turn: payload.turn,
        messages: injectProviderExecutedCall(payload.messages),
        messagesDigest: payload.messagesDigest,
        ...(payload.runScopedPrefixLength === undefined
          ? {}
          : { runScopedPrefixLength: payload.runScopedPrefixLength }),
        ...(payload.modelUsage === undefined ? {} : { modelUsage: payload.modelUsage }),
        ...(payload.inputTokens === undefined ? {} : { inputTokens: payload.inputTokens }),
        ...(payload.outputTokens === undefined ? {} : { outputTokens: payload.outputTokens }),
        ...(payload.costMicrousd === undefined ? {} : { costMicrousd: payload.costMicrousd }),
      }),
    }),
  });
};

const injectedProviderCallStoreLayer = Layer.effect(
  ThreadStore,
  Effect.gen(function* () {
    const inner = yield* ThreadStore;
    return ThreadStore.of({
      ...inner,
      read: (request) => inner.read(request).pipe(Stream.map(injectProviderCallEnvelope)),
    });
  }),
).pipe(Layer.provide(MemoryThreadStoreLive));

const injectedProviderCallTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      MemorySubmissionLedgerLive,
      injectedProviderCallStoreLayer,
      WakeScheduler.layerNoop,
      DurableRuntimeFailpointTestControl.layer,
      ToolReconciler.uncertain,
      configLayer,
    ).pipe(Layer.provideMerge(NodeCrypto.layer)),
  ),
);

const corruptRunDispositionEnvelope = (
  envelope: CanonicalRecordEnvelope,
): CanonicalRecordEnvelope => {
  const payload = envelope.record.payload;
  if (payload._tag !== "RunCompleted" || payload.runDisposition === undefined) return envelope;
  return CanonicalRecordEnvelope.make({
    ...envelope,
    record: RecordEnvelope.make({
      ...envelope.record,
      payload: RunCompleted.make({
        runId: payload.runId,
        output: payload.output,
        runDisposition: "hostile-replacement",
        ...(payload.finishReason === undefined ? {} : { finishReason: payload.finishReason }),
        ...(payload.exhausted === undefined ? {} : { exhausted: payload.exhausted }),
      }),
    }),
  });
};

const corruptedRunDispositionStoreLayer = Layer.effect(
  ThreadStore,
  Effect.gen(function* () {
    const inner = yield* ThreadStore;
    return ThreadStore.of({
      ...inner,
      read: (request) => inner.read(request).pipe(Stream.map(corruptRunDispositionEnvelope)),
    });
  }),
).pipe(Layer.provide(MemoryThreadStoreLive));

const corruptedRunDispositionTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      MemorySubmissionLedgerLive,
      corruptedRunDispositionStoreLayer,
      WakeScheduler.layerNoop,
      DurableRuntimeFailpointTestControl.layer,
      ToolReconciler.uncertain,
      configLayer,
    ).pipe(Layer.provideMerge(NodeCrypto.layer)),
  ),
);

const pricedConfigLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-priced"),
  producerId: Schema.decodeSync(ProducerId)("producer-priced"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
  estimateCostMicrousd: () =>
    Effect.succeed({
      costMicrousd: 1_200,
      serviceTier: "priority",
      pricingVersion: "prices-2026-08-24",
    }),
});

const pricedTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      MemorySubmissionLedgerLive,
      MemoryThreadStoreLive,
      WakeScheduler.layerNoop,
      DurableRuntimeFailpointTestControl.layer,
      ToolReconciler.uncertain,
      pricedConfigLayer,
    ).pipe(Layer.provideMerge(NodeCrypto.layer)),
  ),
);

const untrustedSettlementLedgerLayer = Layer.effect(
  SubmissionLedger,
  Effect.gen(function* () {
    const inner = yield* SubmissionLedger;
    return SubmissionLedger.of({
      ...inner,
      finalizeSettlement: (request) =>
        inner.finalizeSettlement(request).pipe(
          Effect.map((settlement) =>
            Settlement.make({
              submissionId: settlement.submissionId,
              settlementId: settlement.settlementId,
              receiptId: settlement.receiptId,
              outcome: settlement.outcome,
              runDisposition: "unverified-ledger-disposition",
              ...(settlement.failure === undefined ? {} : { failure: settlement.failure }),
              settledAt: settlement.settledAt,
            }),
          ),
        ),
    });
  }),
).pipe(Layer.provide(MemorySubmissionLedgerLive));

const untrustedSettlementBaseLayer = Layer.mergeAll(
  untrustedSettlementLedgerLayer,
  MemoryThreadStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const untrustedSettlementTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(untrustedSettlementBaseLayer),
);

class ProgressWaitTestControl extends Context.Service<
  ProgressWaitTestControl,
  {
    readonly scheduler: WakeScheduler["Service"];
    readonly active: Ref.Ref<number>;
    readonly parking: Ref.Ref<number>;
    readonly reads: Ref.Ref<ReadonlyMap<ThreadId, number>>;
    readonly subscribeGate: Ref.Ref<Option.Option<Deferred.Deferred<void>>>;
    readonly parkGate: Ref.Ref<Option.Option<Deferred.Deferred<void>>>;
  }
>()("@effect-agent/testing/ProgressWaitTestControl") {}

const progressWaitControlLayer = Layer.effect(
  ProgressWaitTestControl,
  Effect.gen(function* () {
    const hub = yield* makeWakeSubscriptionHub;
    const active = yield* Ref.make(0);
    const parking = yield* Ref.make(0);
    const reads = yield* Ref.make<ReadonlyMap<ThreadId, number>>(new Map());
    const subscribeGate = yield* Ref.make<Option.Option<Deferred.Deferred<void>>>(Option.none());
    const parkGate = yield* Ref.make<Option.Option<Deferred.Deferred<void>>>(Option.none());

    const scheduler = WakeScheduler.of({
      notify: hub.notify,
      wakes: Stream.never,
      subscribe: (threadId) =>
        Effect.gen(function* () {
          const wait = yield* hub.subscribe(threadId);
          yield* Ref.update(active, (count) => count + 1);
          yield* Effect.addFinalizer(() => Ref.update(active, (count) => count - 1));
          const beforeCheck = yield* Ref.get(subscribeGate);
          if (Option.isSome(beforeCheck)) yield* Deferred.await(beforeCheck.value);
          return wait;
        }).pipe(
          Effect.map((wait) =>
            Effect.gen(function* () {
              yield* Ref.update(parking, (count) => count + 1);
              const beforePark = yield* Ref.get(parkGate);
              if (Option.isSome(beforePark)) yield* Deferred.await(beforePark.value);
              yield* wait;
            }),
          ),
        ),
    });

    return ProgressWaitTestControl.of({
      scheduler,
      active,
      parking,
      reads,
      subscribeGate,
      parkGate,
    });
  }),
);

const progressWaitSchedulerLayer = Layer.effect(
  WakeScheduler,
  Effect.map(ProgressWaitTestControl, (control) => control.scheduler),
);

const progressWaitStoreLayer = Layer.effect(
  ThreadStore,
  Effect.gen(function* () {
    const inner = yield* ThreadStore;
    const control = yield* ProgressWaitTestControl;
    return ThreadStore.of({
      ...inner,
      read: (request) =>
        Stream.unwrap(
          Ref.update(control.reads, (current) => {
            const next = new Map(current);
            next.set(request.threadId, (current.get(request.threadId) ?? 0) + 1);
            return next;
          }).pipe(Effect.as(inner.read(request))),
        ),
    });
  }),
).pipe(Layer.provide(MemoryThreadStoreLive));

const progressWaitAdapters = Layer.merge(progressWaitSchedulerLayer, progressWaitStoreLayer).pipe(
  Layer.provideMerge(progressWaitControlLayer),
);

const progressWaitBaseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  progressWaitAdapters,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const progressWaitTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(progressWaitBaseLayer),
);

const waitForAtLeast = (ref: Ref.Ref<number>, expected: number): Effect.Effect<void> =>
  Effect.gen(function* () {
    while ((yield* Ref.get(ref)) < expected) yield* Effect.yieldNow;
  });

const readCount = (control: ProgressWaitTestControl["Service"], threadId: ThreadId) =>
  Ref.get(control.reads).pipe(Effect.map((counts) => counts.get(threadId) ?? 0));

const readLog = (threadId: string) =>
  Effect.gen(function* () {
    const store = yield* ThreadStore;
    return yield* Stream.runCollect(
      store.read(
        ThreadRead.make({
          threadId: decodeThreadId(threadId),
          limit: 1_024,
        }),
      ),
    );
  });

const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

const lookupState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value.state;
  });

const armFailpoint = (location: DurableRuntimeFailpointLocation) =>
  Effect.gen(function* () {
    const control = yield* DurableRuntimeFailpointTestControl;
    yield* control.setHandler((hitLocation) =>
      hitLocation === location
        ? Effect.fail(DurableRuntimeFailpointError.make({ location: hitLocation }))
        : Effect.void,
    );
  });

const clearFailpoint = Effect.gen(function* () {
  const control = yield* DurableRuntimeFailpointTestControl;
  yield* control.clear;
});

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  const error: unknown = failure.value;
  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

layer(progressWaitTestLayer)("#94 DurableAgentRuntime progress waits", (it) => {
  it.effect("wakes promptly when the terminal settlement append commits", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const failpoints = yield* DurableRuntimeFailpointTestControl;
      const control = yield* ProgressWaitTestControl;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"settled"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const threadId = decodeThreadId("thread-progress-settlement");
      yield* runtime.submit(
        agent,
        { question: "wake on settlement" },
        submitOptions(threadId, "progress-settlement-1"),
      );

      const reserved = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* failpoints.setHandler((location) =>
        location === "terminalize:after-reserve"
          ? Deferred.succeed(reserved, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void,
      );
      yield* Effect.gen(function* () {
        const processing = yield* Effect.forkChild(runtime.processThread(agent, threadId));
        yield* Deferred.await(reserved);

        const beforeSettlement = yield* readLog(threadId);
        expect(logTags(beforeSettlement)).not.toContain("SubmissionSettled");
        const cursor = beforeSettlement.at(-1)?.sequence;
        expect(cursor).toBeDefined();
        if (cursor === undefined) return;

        const waiting = yield* Effect.forkChild(runtime.awaitProgress(threadId, cursor));
        yield* waitForAtLeast(control.active, 1);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(waiting);
        yield* Fiber.join(processing);

        const afterSettlement = yield* readLog(threadId);
        expect(afterSettlement.at(-1)?.record.payload._tag).toBe("SubmissionSettled");
        expect(yield* Ref.get(control.active)).toBe(0);
      }).pipe(Effect.ensuring(failpoints.clear));
    }),
  );

  it.effect("wakes after approval request and decision records commit", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const control = yield* ProgressWaitTestControl;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0 ? approvalCallParts : finalParts('{"answer":"approved"}'),
      );
      const agent = Agent.withModel(progressApprovalDefinition, scripted.model);
      const threadId = decodeThreadId("thread-progress-approval");
      const receipt = yield* runtime.submit(
        agent,
        { question: "wake at both approval boundaries" },
        submitOptions(threadId, "progress-approval-1"),
      );
      const submitted = yield* readLog(threadId);
      const submittedCursor = submitted.at(-1)?.sequence;
      expect(submittedCursor).toBeDefined();
      if (submittedCursor === undefined) return;

      const awaitingRequest = yield* Effect.forkChild(
        runtime.awaitProgress(threadId, submittedCursor),
      );
      yield* waitForAtLeast(control.active, 1);
      yield* runtime.processThread(agent, threadId).pipe(Effect.provide(progressApprovalToolLayer));
      yield* Fiber.join(awaitingRequest);
      const requested = yield* readLog(threadId);
      expect(
        requested.some(
          (record) =>
            record.sequence > submittedCursor &&
            record.record.payload._tag === "ToolApprovalRequested",
        ),
      ).toBe(true);
      const requestCursor = requested.at(-1)?.sequence;
      expect(requestCursor).toBeDefined();
      if (requestCursor === undefined) return;

      const parksBeforeDecision = yield* Ref.get(control.parking);
      const awaitingDecision = yield* Effect.forkChild(
        Effect.gen(function* () {
          let cursor = requestCursor;
          for (;;) {
            const records = yield* readLog(threadId);
            if (
              records.some(
                (record) =>
                  record.sequence > requestCursor &&
                  record.record.payload._tag === "ToolApprovalDecided",
              )
            ) {
              return;
            }
            const last = records.at(-1);
            if (last !== undefined && last.sequence > cursor) cursor = last.sequence;
            yield* runtime.awaitProgress(threadId, cursor);
          }
        }),
      );
      yield* waitForAtLeast(control.parking, parksBeforeDecision + 1);
      yield* runtime.resolveApproval(
        ApprovalDecisionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: Schema.decodeSync(ToolCallId)("book-progress-1"),
          decision: "approved",
          resolver: "#94-runtime-test",
          reason: "exercise the public approval progress edge",
        }),
      );
      // Resolution first commits a ledger intent and emits a permitted false-positive hint.
      // The caller re-reads, parks again, and the resumed Attempt appends the canonical decision.
      yield* waitForAtLeast(control.parking, parksBeforeDecision + 2);
      yield* runtime.processThread(agent, threadId).pipe(Effect.provide(progressApprovalToolLayer));
      yield* Fiber.join(awaitingDecision);
      const decided = yield* readLog(threadId);
      expect(
        decided.some(
          (record) =>
            record.sequence > requestCursor && record.record.payload._tag === "ToolApprovalDecided",
        ),
      ).toBe(true);
      expect(yield* Ref.get(control.active)).toBe(0);
    }),
  );

  it.effect("closes the subscribe/check and check/park lost-wakeup windows", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scheduler = yield* WakeScheduler;
      const control = yield* ProgressWaitTestControl;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"never"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const threadId = decodeThreadId("thread-progress-races");
      const receipt = yield* runtime.submit(
        agent,
        { question: "race the wait" },
        submitOptions(threadId, "progress-races-1"),
      );
      const initial = yield* readLog(threadId);
      const initialCursor = initial.at(-1)?.sequence;
      expect(initialCursor).toBeDefined();
      if (initialCursor === undefined) return;

      // Force append+notify after registration but before the authoritative check.
      const beforeCheck = yield* Deferred.make<void>();
      yield* Ref.set(control.subscribeGate, Option.some(beforeCheck));
      const subscribedRace = yield* Effect.forkChild(
        runtime.awaitProgress(threadId, initialCursor),
      );
      yield* waitForAtLeast(control.active, 1);
      yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "force #94 subscribe/check race",
        }),
      );
      yield* Deferred.succeed(beforeCheck, undefined);
      yield* Fiber.join(subscribedRace);
      expect(yield* Ref.get(control.active)).toBe(0);

      const afterAbort = yield* readLog(threadId);
      const abortCursor = afterAbort.at(-1)?.sequence;
      expect(abortCursor).toBeDefined();
      if (abortCursor === undefined) return;

      // Force a hint after the empty canonical check but before the returned wait Effect parks.
      yield* Ref.set(control.subscribeGate, Option.none());
      const beforePark = yield* Deferred.make<void>();
      yield* Ref.set(control.parkGate, Option.some(beforePark));
      const readsBeforeParkRace = yield* readCount(control, threadId);
      const parksBeforeParkRace = yield* Ref.get(control.parking);
      const parkedRace = yield* Effect.forkChild(runtime.awaitProgress(threadId, abortCursor));
      yield* waitForAtLeast(control.parking, parksBeforeParkRace + 1);
      yield* scheduler.notify(threadId);
      yield* Deferred.succeed(beforePark, undefined);
      yield* Fiber.join(parkedRace);
      expect(yield* readCount(control, threadId)).toBe(readsBeforeParkRace + 1);
      expect(yield* Ref.get(control.active)).toBe(0);

      // The second wake was deliberately a false positive: storage, not the hint, is truth.
      const final = yield* readLog(threadId);
      expect(final.at(-1)?.sequence).toBe(abortCursor);
    }),
  );

  it.effect(
    "broadcasts to concurrent waiters, stays O(1), and cleans up cancellation/timeout",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scheduler = yield* WakeScheduler;
        const control = yield* ProgressWaitTestControl;
        const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"never"}'));
        const agent = Agent.withModel(plannerDefinition, scripted.model);
        const threadId = decodeThreadId("thread-progress-many");
        const unrelatedId = decodeThreadId("thread-progress-unrelated");

        yield* runtime.submit(
          agent,
          { question: "many waiters" },
          submitOptions(threadId, "progress-many-1"),
        );
        yield* runtime.submit(
          agent,
          { question: "unrelated waiter" },
          submitOptions(unrelatedId, "progress-unrelated-1"),
        );
        const records = yield* readLog(threadId);
        const unrelatedRecords = yield* readLog(unrelatedId);
        const cursor = records.at(-1)?.sequence;
        const unrelatedCursor = unrelatedRecords.at(-1)?.sequence;
        expect(cursor).toBeDefined();
        expect(unrelatedCursor).toBeDefined();
        if (cursor === undefined || unrelatedCursor === undefined) return;

        const readsBefore = yield* readCount(control, threadId);
        const unrelatedReadsBefore = yield* readCount(control, unrelatedId);
        const first = yield* Effect.forkChild(runtime.awaitProgress(threadId, cursor));
        const second = yield* Effect.forkChild(runtime.awaitProgress(threadId, cursor));
        const unrelated = yield* Effect.forkChild(
          runtime.awaitProgress(unrelatedId, unrelatedCursor),
        );
        yield* waitForAtLeast(control.active, 3);
        yield* waitForAtLeast(control.parking, 3);
        expect(yield* readCount(control, threadId)).toBe(readsBefore + 2);
        expect(yield* readCount(control, unrelatedId)).toBe(unrelatedReadsBefore + 1);

        yield* TestClock.adjust(Duration.seconds(10));
        expect(yield* readCount(control, threadId)).toBe(readsBefore + 2);
        expect(yield* readCount(control, unrelatedId)).toBe(unrelatedReadsBefore + 1);

        yield* scheduler.notify(threadId);
        yield* Fiber.join(first);
        yield* Fiber.join(second);
        expect(unrelated.pollUnsafe()).toBeUndefined();
        expect(yield* Ref.get(control.active)).toBe(1);

        yield* Fiber.interrupt(unrelated);
        expect(yield* Ref.get(control.active)).toBe(0);

        const timed = yield* Effect.forkChild(
          runtime.awaitProgress(threadId, cursor).pipe(Effect.timeoutOption(Duration.seconds(3))),
        );
        yield* waitForAtLeast(control.active, 1);
        yield* TestClock.adjust(Duration.seconds(3));
        const timedResult = yield* Fiber.join(timed);
        expect(Option.isNone(timedResult)).toBe(true);
        expect(yield* Ref.get(control.active)).toBe(0);
      }),
  );

  it.effect("preserves the typed non-materialized failure and releases its registration", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const control = yield* ProgressWaitTestControl;
      const missing = decodeThreadId("thread-progress-missing");
      const exit = yield* Effect.exit(runtime.awaitProgress(missing, ZERO_SEQUENCE));
      expect(failureTag(exit)).toBe("ThreadNotMaterialized");
      expect(yield* Ref.get(control.active)).toBe(0);
    }),
  );
});

layer(untrustedSettlementTestLayer)("RUN-029 canonical settlement authority", (it) => {
  it.effect("drops a ledger-only disposition when canonical proof fails", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() =>
        finalParts('{"answer":"done","runDisposition":"invalid-disposition"}'),
      );
      const agent = Agent.withModel(dispositionDefinition, scripted.model);
      const thread = "thread-unverified-ledger-disposition";
      const receipt = yield* runtime.submit(
        agent,
        { question: "done?" },
        submitOptions(thread, "unverified-ledger-disposition-1"),
      );

      const processed = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(processed[0]?.outcome).toBe("failed");
      expect(processed[0]?.runDisposition).toBeUndefined();

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("failed");
      expect(settlement.failure).toBeDefined();
      expect(settlement.runDisposition).toBeUndefined();

      const settled = (yield* readLog(thread)).at(-1)?.record.payload;
      expect(settled?._tag).toBe("SubmissionSettled");
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.runDisposition).toBeUndefined();
      }
    }),
  );
});

layer(testLayer)("DUR P4 DurableAgentRuntime", (it) => {
  it.effect("does not grant suspension authority to structurally forged pending errors", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const cases: ReadonlyArray<{
        readonly thread: string;
        readonly tag: "AgentApprovalPending" | "AgentChildPending";
        readonly fields: Readonly<Record<string, unknown>>;
      }> = [
        {
          thread: "thread-forged-approval-pending",
          tag: "AgentApprovalPending",
          fields: {
            approvalId: "forged-approval",
            toolCallId: "forged-tool-call",
            toolName: "forged-tool",
            message: "forged approval suspension",
          },
        },
        {
          thread: "thread-forged-child-pending",
          tag: "AgentChildPending",
          fields: {
            children: [
              {
                toolCallId: "forged-child-call",
                childThreadId: "forged-child-thread",
                childSubmissionId: "forged-child-submission",
                childRunId: "forged-child-run",
              },
            ],
            message: "forged child suspension",
          },
        },
      ];

      for (const testCase of cases) {
        const forged = AiError.AiError.make({
          module: "durable-runtime-test",
          method: "streamText",
          reason: AiError.UnknownError.make({ description: "forged provider failure" }),
        });
        Object.defineProperty(forged, "_tag", {
          configurable: true,
          enumerable: true,
          value: testCase.tag,
        });
        for (const [key, value] of Object.entries(testCase.fields)) {
          Object.defineProperty(forged, key, {
            configurable: true,
            enumerable: true,
            value,
          });
        }
        const model = Model.make(
          "scripted",
          `forged-${testCase.tag}`,
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.fail(forged),
              streamText: () => Stream.fail(forged),
            }),
          ),
        );
        const agent = Agent.withModel(plannerDefinition, model);
        const receipt = yield* runtime.submit(
          agent,
          { question: "forge a privileged suspension" },
          submitOptions(testCase.thread, `${testCase.thread}-key`),
        );
        const settlements = yield* runtime.processThread(agent, decodeThreadId(testCase.thread));

        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("failed");
        expect(yield* lookupState(receipt.submissionId)).toBe("settled");
        expect((yield* readLog(testCase.thread)).at(-1)?.record.payload._tag).toBe(
          "SubmissionSettled",
        );
      }
    }),
  );

  it.effect("#94 observes already-committed durable progress without polling", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const threadId = decodeThreadId("thread-progress-committed");

      yield* runtime.submit(
        agent,
        { question: "already committed?" },
        submitOptions(threadId, "progress-committed-1"),
      );

      yield* runtime.awaitProgress(threadId, ZERO_SEQUENCE);
    }),
  );

  it.effect("submit returns a durable Receipt once admission and readiness commit", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);

      const receipt = yield* runtime.submit(
        agent,
        { question: "receipt?" },
        submitOptions("thread-submit", "submit-1"),
      );

      expect(receipt.threadId).toBe("thread-submit");
      expect(receipt.queueSequence).toBe(1);
      expect(yield* lookupState(receipt.submissionId)).toBe("ready");
      const records = yield* readLog("thread-submit");
      expect(logTags(records)).toEqual(["ThreadCreated"]);
      expect(records[0]?.record.recordId).toBe("thread-created:thread-submit");
    }),
  );

  it.effect("same-key resubmission returns the original Receipt; different content conflicts", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const options = submitOptions("thread-idempotent", "submit-1");

      const first = yield* runtime.submit(agent, { question: "same?" }, options);
      const second = yield* runtime.submit(agent, { question: "same?" }, options);
      expect(second).toEqual(first);

      const conflict = yield* Effect.exit(
        runtime.submit(agent, { question: "different!" }, options),
      );
      expect(failureTag(conflict)).toBe("AdmissionConflict");
    }),
  );

  it.effect("runs accepted work to settlement with an ordered canonical log", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0 ? toolCallParts : finalParts('{"answer":"Found."}'),
      );
      const agent = Agent.withModel(searchDefinition, scripted.model);
      const thread = "thread-full-run";

      const receipt = yield* runtime.submit(
        agent,
        { question: "Is a flight available?" },
        submitOptions(thread, "run-1"),
      );
      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(searchToolLayer));

      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(settlements[0]?.receiptId).toBe(receipt.receiptId);

      const submissionId = receipt.submissionId;
      const runId = runIdForSubmission(submissionId);
      const records = yield* readLog(thread);
      expect(logTags(records)).toEqual([
        "ThreadCreated",
        "UserInputRecorded",
        "RunStarted",
        "ModelResponseRecorded",
        "ToolCallSettled",
        "ModelResponseRecorded",
        "RunCompleted",
        "SubmissionSettled",
      ]);
      expect(records.map((envelope) => envelope.record.recordId)).toEqual([
        `thread-created:${thread}`,
        submissionInputRecordId(submissionId),
        runStartedRecordId(runId),
        modelResponseRecordId(runId, 1),
        `tool-settled:${runId}:1:search-1`,
        modelResponseRecordId(runId, 2),
        runCompletedRecordId(runId),
        submissionSettlementRecordId(submissionId),
      ]);

      const settled = records.at(-1)?.record.payload;
      expect(settled?._tag).toBe("SubmissionSettled");
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.outcome).toBe("completed");
        expect(settled.runId).toBe(runId);
        expect(settled.result).toEqual({ answer: "Found." });
      }

      // A later Run sees Thread history without replaying this Run's
      // evaluated instruction/input prefix.
      const prompt = yield* promptFromCanonicalRecords(records);
      expect(prompt.content.map((message) => message.role)).toEqual([
        "assistant",
        "tool",
        "assistant",
      ]);

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement).toEqual(settlements[0]);
    }),
  );

  it.effect(
    "RUN-029 persists and replays an ordinary application run disposition across terminalization crashes",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scenarios = [
          {
            location: undefined,
            thread: "thread-run-disposition",
            key: "run-disposition-1",
          },
          {
            location: "terminalize:after-reserve" as const,
            thread: "thread-run-disposition-reserve",
            key: "run-disposition-reserve-1",
          },
          {
            location: "terminalize:after-canonical-append" as const,
            thread: "thread-run-disposition-append",
            key: "run-disposition-append-1",
          },
        ];

        for (const scenario of scenarios) {
          const scripted = yield* makeScriptedModel(() =>
            finalParts('{"answer":"done","runDisposition":"application-complete"}'),
          );
          const agent = Agent.withModel(dispositionDefinition, scripted.model);
          const threadId = decodeThreadId(scenario.thread);
          const receipt = yield* runtime.submit(
            agent,
            { question: "done?" },
            submitOptions(scenario.thread, scenario.key),
          );

          if (scenario.location === undefined) {
            yield* runtime.processThread(agent, threadId);
          } else {
            yield* armFailpoint(scenario.location);
            const killed = yield* Effect.exit(runtime.processThread(agent, threadId));
            expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
            yield* clearFailpoint;
            yield* runtime.runRecovery;
          }

          const settlement = yield* runtime.awaitSettlement(receipt);
          expect(settlement.outcome).toBe("completed");
          expect(settlement.runDisposition).toBe("application-complete");
          const records = yield* readLog(scenario.thread);
          const projection = replayThread(threadId, records);
          expect(projection.settlements).toHaveLength(1);
          const disposition = projection.settlements[0]?.runDisposition;
          const decodedDisposition = yield* Schema.decodeUnknownEffect(RunDisposition)(disposition);
          expect(decodedDisposition).toBe("application-complete");
        }
      }),
  );

  it.effect("recovers a canonically completed no-tool Turn without another model call", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? finalParts('{"answer":"Committed once."}')
          : finalParts('{"answer":"Wrong duplicate call."}'),
      );
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-final-turn-recovery";
      const receipt = yield* runtime.submit(
        agent,
        { question: "complete once?" },
        submitOptions(thread, "final-turn-recovery-1"),
      );

      yield* armFailpoint("turn:after-canonical-append");
      const crashed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      expect(scripted.prompts).toHaveLength(1);
      yield* clearFailpoint;

      const settled = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settled).toHaveLength(1);
      expect(settled[0]?.outcome).toBe("completed");
      expect(yield* runtime.awaitSettlement(receipt)).toEqual(settled[0]);
      expect(scripted.prompts).toHaveLength(1);

      const payloads = (yield* readLog(thread)).map((envelope) => envelope.record.payload);
      expect(payloads.filter((payload) => payload._tag === "ModelResponseRecorded")).toHaveLength(
        1,
      );
      expect(payloads.filter((payload) => payload._tag === "RunCompleted")).toHaveLength(1);
      expect(payloads.at(-1)).toMatchObject({
        _tag: "SubmissionSettled",
        outcome: "completed",
        result: { answer: "Committed once." },
      });
    }),
  );

  it.effect(
    "RUN-032 recovers a canonically settled completion Tool without another model call or side effect",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const PostMessage = Tool.make("post_message", {
          parameters: Schema.Struct({ message: Schema.String }),
          success: Schema.Struct({ messageId: Schema.String }),
        });
        const tools = Toolkit.make(PostMessage);
        const definition = Agent.make("durable-terminal-delivery", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ messageId: Schema.String, delivered: Schema.Boolean }),
          instructions: "Deliver through post_message.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
          completion: {
            tool: "post_message",
            project: ({ result }) => ({ messageId: result.messageId, delivered: true }),
          },
        });
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? [
                {
                  type: "tool-call",
                  id: "delivery-1",
                  name: "post_message",
                  params: { message: "Delivered once" },
                  providerExecuted: false,
                },
                { type: "finish", reason: "tool-calls", usage },
              ]
            : finalParts('{"messageId":"wrong-second-call","delivered":false}'),
        );
        const handlerCalls = yield* Ref.make(0);
        const toolLayer = tools.toLayer({
          post_message: () =>
            Ref.updateAndGet(handlerCalls, (count) => count + 1).pipe(
              Effect.as({ messageId: "message-1" }),
            ),
        });
        const agent = Agent.withModel(definition, scripted.model);
        const thread = "thread-terminal-delivery-recovery";

        yield* runtime.submit(
          agent,
          { question: "deliver?" },
          submitOptions(thread, "terminal-delivery-1"),
        );
        yield* armFailpoint("turn:after-results-append");
        const crashed = yield* Effect.exit(
          runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
        );
        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(handlerCalls)).toBe(1);
        expect(scripted.prompts).toHaveLength(1);
        yield* clearFailpoint;

        const settled = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(toolLayer));
        expect(settled).toHaveLength(1);
        expect(settled[0]?.outcome).toBe("completed");
        expect(yield* Ref.get(handlerCalls)).toBe(1);
        expect(scripted.prompts).toHaveLength(1);
        const payloads = (yield* readLog(thread)).map((envelope) => envelope.record.payload);
        expect(payloads.filter((payload) => payload._tag === "ModelResponseRecorded")).toHaveLength(
          1,
        );
        expect(payloads.at(-1)).toMatchObject({
          _tag: "SubmissionSettled",
          outcome: "completed",
          result: { messageId: "message-1", delivered: true },
        });
      }),
  );

  it.effect(
    "RUN-032 resumes an over-token completion delivery under final-answer mode after the response commit",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const PostMessage = Tool.make("post_message", {
          parameters: Schema.Struct({ message: Schema.String }),
          success: Schema.Struct({ messageId: Schema.String }),
        });
        const tools = Toolkit.make(PostMessage);
        const definition = Agent.make("durable-over-token-delivery", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ messageId: Schema.String, delivered: Schema.Boolean }),
          instructions: "Deliver through post_message.",
          toolkit: tools,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            tokenBudget: 1_000,
            completionReserveTokens: 100,
          }),
          completion: {
            tool: "post_message",
            project: ({ result }) => ({ messageId: result.messageId, delivered: true }),
          },
        });
        const scripted = yield* makeScriptedModel(() => [
          {
            type: "tool-call",
            id: "over-token-delivery-1",
            name: "post_message",
            params: { message: "Delivered after recovery" },
            providerExecuted: false,
          },
          {
            type: "finish",
            reason: "tool-calls",
            usage: {
              inputTokens: { total: 900 },
              outputTokens: { total: 200 },
            },
          },
        ]);
        const handlerCalls = yield* Ref.make(0);
        const toolLayer = tools.toLayer({
          post_message: () =>
            Ref.updateAndGet(handlerCalls, (count) => count + 1).pipe(
              Effect.as({ messageId: "recovered-message-1" }),
            ),
        });
        const agent = Agent.withModel(definition, scripted.model);
        const thread = "thread-over-token-terminal-delivery";

        yield* runtime.submit(
          agent,
          { question: "deliver?" },
          submitOptions(thread, "over-token-terminal-delivery-1"),
        );
        yield* armFailpoint("turn:after-response-append");
        const crashed = yield* Effect.exit(
          runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
        );
        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(handlerCalls)).toBe(0);
        expect(scripted.prompts).toHaveLength(1);
        yield* clearFailpoint;

        const settled = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(toolLayer));
        expect(settled).toHaveLength(1);
        expect(settled[0]?.outcome).toBe("completed");
        expect(yield* Ref.get(handlerCalls)).toBe(1);
        expect(scripted.prompts).toHaveLength(1);
        const payloads = (yield* readLog(thread)).map((envelope) => envelope.record.payload);
        expect(payloads.at(-1)).toMatchObject({
          _tag: "SubmissionSettled",
          outcome: "completed",
          finishReason: "budget-exhausted",
          exhausted: "tokens",
          result: { messageId: "recovered-message-1", delivered: true },
        });
      }),
  );

  it.effect("RUN-029 invalid disposition selection settles failed without disposition", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const secret = "sensitive-run-disposition-must-not-enter-history";
      const scripted = yield* makeScriptedModel(() =>
        finalParts(
          '{"answer":"done","runDisposition":"sensitive-run-disposition-must-not-enter-history"}',
        ),
      );
      const agent = Agent.withModel(dispositionDefinition, scripted.model);
      const thread = "thread-run-disposition-invalid";

      const receipt = yield* runtime.submit(
        agent,
        { question: "done?" },
        submitOptions(thread, "run-disposition-invalid-1"),
      );
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));

      expect(settlements[0]?.outcome).toBe("failed");
      expect(settlements[0]?.runDisposition).toBeUndefined();
      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("failed");
      expect(settlement.runDisposition).toBeUndefined();
      const settled = (yield* readLog(thread)).at(-1)?.record.payload;
      expect(settled?._tag).toBe("SubmissionSettled");
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.outcome).toBe("failed");
        expect(settled.result).toMatchObject({
          errorTag: "AgentRunDispositionError",
          message: "Run disposition failed Schema encoding",
        });
        expect(settled.runDisposition).toBeUndefined();
        expect(JSON.stringify(settled)).not.toContain(secret);
      }
    }),
  );

  it.effect(
    "RUN-018 a budget-exhausted Run settles the Submission completed with canonical synthetic Tool settlements",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        // Deliberately unannotated: an uncertain-class tool normally gains
        // `ToolCallPrepared` records under the P5 split commits — a rejected
        // batch must never durably declare, so none may appear here.
        const Probe = Tool.make("probe", {
          parameters: Schema.Struct({ query: Schema.String }),
          success: Schema.Struct({ available: Schema.Boolean }),
        });
        const probeTools = Toolkit.make(Probe);
        const definition = Agent.make("durable-soft-landing", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({
            answer: Schema.String,
            runDisposition: Schema.optionalKey(Schema.String),
          }),
          instructions: "Probe before answering.",
          toolkit: probeTools,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
          runDisposition: {
            schema: RunDisposition,
            fromOutput: (output) => output.runDisposition,
          },
        });
        const handlerStarts = yield* Ref.make(0);
        const probeToolLayer = probeTools.toLayer({
          probe: () =>
            Ref.update(handlerStarts, (count) => count + 1).pipe(Effect.as({ available: true })),
        });
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? [
                {
                  type: "tool-call",
                  id: "probe-1",
                  name: "probe",
                  params: { query: "a" },
                  providerExecuted: false,
                },
                {
                  type: "tool-call",
                  id: "probe-2",
                  name: "probe",
                  params: { query: "b" },
                  providerExecuted: false,
                },
                { type: "finish", reason: "tool-calls", usage },
              ]
            : finalParts(
                '{"answer":"partial, budget exhausted","runDisposition":"application-complete"}',
              ),
        );
        const agent = Agent.withModel(definition, scripted.model);
        const thread = "thread-soft-landing";

        const receipt = yield* runtime.submit(
          agent,
          { question: "Everything about the package?" },
          submitOptions(thread, "soft-landing-1"),
        );
        const settlements = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(probeToolLayer));

        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("completed");
        expect(settlements[0]?.runDisposition).toBeUndefined();
        const settlement = yield* runtime.awaitSettlement(receipt);
        expect(settlement.outcome).toBe("completed");
        expect(settlement.runDisposition).toBeUndefined();
        expect(yield* Ref.get(handlerStarts)).toBe(0);

        const submissionId = receipt.submissionId;
        const runId = runIdForSubmission(submissionId);
        const records = yield* readLog(thread);
        // The rejected Turn commits as ONE single-batch canonical Turn — no
        // `ToolCallPrepared`, no split response batch — and the Submission
        // settles completed with the honest durable finishReason (RUN-011).
        expect(logTags(records)).toEqual([
          "ThreadCreated",
          "UserInputRecorded",
          "RunStarted",
          "RunPolicyUsageReserved",
          "ModelResponseRecorded",
          "ToolCallSettled",
          "ToolCallSettled",
          "ModelResponseRecorded",
          "RunCompleted",
          "SubmissionSettled",
        ]);
        // Exact synthetic settlements: identities in declaration order, each
        // carrying the encoded policy failure — replay correlation for the
        // model-declared calls, not just an isFailure bit.
        expect(
          records
            .filter(({ record }) => record.payload._tag !== "RunPolicyUsageReserved")
            .map((envelope) => envelope.record.recordId)
            .slice(3, 6),
        ).toEqual([
          modelResponseRecordId(runId, 1),
          toolCallSettledRecordId(runId, 1, Schema.decodeSync(ToolCallId)("probe-1")),
          toolCallSettledRecordId(runId, 1, Schema.decodeSync(ToolCallId)("probe-2")),
        ]);
        const settledCalls = records
          .map((envelope) => envelope.record.payload)
          .filter((payload) => payload._tag === "ToolCallSettled");
        expect(
          settledCalls.map((payload) => ({
            toolCallId: payload.toolCallId,
            toolName: payload.toolName,
            isFailure: payload.isFailure,
          })),
        ).toEqual([
          { toolCallId: "probe-1", toolName: "probe", isFailure: true },
          { toolCallId: "probe-2", toolName: "probe", isFailure: true },
        ]);
        for (const payload of settledCalls) {
          expect(payload.result).toMatchObject({
            _tag: "AgentPolicyError",
            limit: "tool-calls",
          });
        }
        const settled = records.at(-1)?.record.payload;
        expect(settled?._tag).toBe("SubmissionSettled");
        if (settled?._tag === "SubmissionSettled") {
          expect(settled.outcome).toBe("completed");
          expect(settled.runId).toBe(runId);
          expect(settled.finishReason).toBe("budget-exhausted");
          expect(settled.exhausted).toBe("tool-calls");
          expect(settled.policyLimit).toBeUndefined();
          expect(settled.runDisposition).toBeUndefined();
          expect(settled.result).toEqual({
            answer: "partial, budget exhausted",
            runDisposition: "application-complete",
          });
        }

        // A later Run sees the rejected batch as Thread history but
        // not this Run's evaluated instruction/input prefix.
        const prompt = yield* promptFromCanonicalRecords(records);
        expect(prompt.content.map((message) => message.role)).toEqual([
          "assistant",
          "tool",
          "assistant",
        ]);
        const toolMessage = prompt.content.find((message) => message.role === "tool");
        const toolParts = (toolMessage?.content ?? []).flatMap((part) =>
          part.type === "tool-result" ? [{ id: part.id, isFailure: part.isFailure ?? false }] : [],
        );
        expect(toolParts).toEqual([
          { id: "probe-1", isFailure: true },
          { id: "probe-2", isFailure: true },
        ]);
      }),
  );

  it.effect(
    "RUN-018 recovery preserves the budget-exhausted finishReason and exhausted dimension across both terminalize failpoints",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scenarios = [
          {
            location: "terminalize:after-reserve" as const,
            thread: "thread-soft-landing-reserve",
            key: "soft-landing-reserve-1",
          },
          {
            location: "terminalize:after-canonical-append" as const,
            thread: "thread-soft-landing-append",
            key: "soft-landing-append-1",
          },
        ];
        for (const scenario of scenarios) {
          const Probe = Tool.make("probe", {
            parameters: Schema.Struct({ query: Schema.String }),
            success: Schema.Struct({ available: Schema.Boolean }),
          });
          const probeTools = Toolkit.make(Probe);
          const definition = Agent.make("durable-soft-landing-recovery", {
            input: Schema.Struct({ question: Schema.String }),
            output: Schema.Struct({
              answer: Schema.String,
              runDisposition: Schema.optionalKey(Schema.String),
            }),
            instructions: "Probe before answering.",
            toolkit: probeTools,
            policy: AgentPolicy.make({
              maxTurns: 5,
              maxToolCalls: 1,
              maxDuration: "30 seconds",
              toolConcurrency: 1,
            }),
            runDisposition: {
              schema: RunDisposition,
              fromOutput: (output) => output.runDisposition,
            },
          });
          const probeToolLayer = probeTools.toLayer({
            probe: () => Effect.succeed({ available: true }),
          });
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? [
                  {
                    type: "tool-call",
                    id: "probe-1",
                    name: "probe",
                    params: { query: "a" },
                    providerExecuted: false,
                  },
                  {
                    type: "tool-call",
                    id: "probe-2",
                    name: "probe",
                    params: { query: "b" },
                    providerExecuted: false,
                  },
                  { type: "finish", reason: "tool-calls", usage },
                ]
              : finalParts(
                  '{"answer":"recovered partial","runDisposition":"application-complete"}',
                ),
          );
          const agent = Agent.withModel(definition, scripted.model);

          const receipt = yield* runtime.submit(
            agent,
            { question: "Everything?" },
            submitOptions(scenario.thread, scenario.key),
          );
          yield* armFailpoint(scenario.location);
          const killed = yield* Effect.exit(
            runtime
              .processThread(agent, decodeThreadId(scenario.thread))
              .pipe(Effect.provide(probeToolLayer)),
          );
          expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          yield* runtime.runRecovery;
          const settlement = yield* runtime.awaitSettlement(receipt);
          expect(settlement.outcome).toBe("completed");
          expect(settlement.runDisposition).toBeUndefined();
          const records = yield* readLog(scenario.thread);
          const settledRecords = records
            .map((envelope) => envelope.record.payload)
            .filter((payload) => payload._tag === "SubmissionSettled");
          expect(settledRecords).toHaveLength(1);
          expect(settledRecords[0]).toMatchObject({
            outcome: "completed",
            finishReason: "budget-exhausted",
            exhausted: "tool-calls",
          });
          expect(settledRecords[0]?.runDisposition).toBeUndefined();
        }
      }),
  );

  it.effect("awaitSettlement waits on the poll fallback until the lane settles", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"waited"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-await";

      const receipt = yield* runtime.submit(
        agent,
        { question: "when?" },
        submitOptions(thread, "await-1"),
      );
      const waiter = yield* Effect.forkChild(runtime.awaitSettlement(receipt));
      yield* runtime.processThread(agent, decodeThreadId(thread));
      yield* TestClock.adjust(Duration.millis(150));
      const settlement = yield* Fiber.join(waiter);
      expect(settlement.outcome).toBe("completed");
      expect(settlement.receiptId).toBe(receipt.receiptId);
    }),
  );

  it.effect("keeps one Thread lane FIFO: contiguous queued work joins the active Run", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"next"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-fifo";

      const first = yield* runtime.submit(
        agent,
        { question: "first" },
        submitOptions(thread, "fifo-1"),
      );
      const second = yield* runtime.submit(
        agent,
        { question: "second" },
        submitOptions(thread, "fifo-2"),
      );
      expect(first.queueSequence).toBe(1);
      expect(second.queueSequence).toBe(2);

      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      // P5 (plan §2.5): the active host Run claims the contiguous ready prefix, so the second
      // Submission JOINS the first Run instead of waiting for its own claim — one head
      // settlement, and the joined Submission settles with the host in admitted FIFO order.
      expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
        first.submissionId,
      ]);
      const joinedSettlement = yield* runtime.awaitSettlement(second);
      expect(joinedSettlement.outcome).toBe("completed");

      const records = yield* readLog(thread);
      const settledIds = records
        .filter((envelope) => envelope.record.payload._tag === "SubmissionSettled")
        .map((envelope) => envelope.record.recordId);
      expect(settledIds).toEqual([
        submissionSettlementRecordId(first.submissionId),
        submissionSettlementRecordId(second.submissionId),
      ]);
      const firstInputSequence = records.find(
        (envelope) => envelope.record.recordId === submissionInputRecordId(first.submissionId),
      )?.sequence;
      const secondInputSequence = records.find(
        (envelope) => envelope.record.recordId === submissionInputRecordId(second.submissionId),
      )?.sequence;
      expect(firstInputSequence).toBeDefined();
      expect(secondInputSequence).toBeDefined();
      expect(Number(firstInputSequence)).toBeLessThan(Number(secondInputSequence));
    }),
  );

  it.effect("aborts ready, unclaimed work through the recovery pass without an Attempt", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"never"}'));
      const agent = Agent.withModel(dispositionDefinition, scripted.model);
      const thread = "thread-abort-ready";

      const receipt = yield* runtime.submit(
        agent,
        { question: "abort me" },
        submitOptions(thread, "abort-ready-1"),
      );
      const intent = yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "user cancelled",
        }),
      );
      expect(intent.submissionId).toBe(receipt.submissionId);

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("SettleAborted");
      expect(report?.disposition).toBe("repaired");

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("aborted");
      expect(settlement.runDisposition).toBeUndefined();

      const records = yield* readLog(thread);
      const tags = logTags(records);
      expect(tags).toContain("AbortRequested");
      expect(tags.indexOf("AbortRequested")).toBeLessThan(tags.indexOf("SubmissionSettled"));
      const settled = records.find(
        (envelope) => envelope.record.payload._tag === "SubmissionSettled",
      )?.record.payload;
      expect(settled?._tag).toBe("SubmissionSettled");
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.runDisposition).toBeUndefined();
      }
      // No model ran and no input became canonical for the aborted, never-claimed head.
      expect(tags).not.toContain("ModelResponseRecorded");
      // DUR-013: the executed decision left a deterministic audit record.
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        recoveryRepairRecordId(receipt.submissionId, "SettleAborted"),
      );

      // Repeating the same abort command is idempotent only until settlement (DUR-012).
      const conflict = yield* Effect.exit(
        runtime.abort(
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "operator",
            reason: "user cancelled",
          }),
        ),
      );
      expect(failureTag(conflict)).toBe("SettlementConflict");
    }),
  );

  it.effect("an aborted non-head ready submission settles without waiting for the head", () =>
    Effect.gen(function* () {
      // P7 §7(c): settlement order of never-run work is not execution order — the recovery
      // pass settles an aborted, never-claimed, non-head `ready` Submission immediately, and
      // the contiguous joining prefix later walks over the aborted-settled row as a non-gap
      // (DUR-004 bounds execution order; DUR-012 permits settling inactive accepted work).
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"headline"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-abort-non-head";

      const head = yield* runtime.submit(
        agent,
        { question: "head" },
        submitOptions(thread, "non-head-1"),
      );
      const second = yield* runtime.submit(
        agent,
        { question: "cancel me" },
        submitOptions(thread, "non-head-2"),
      );
      const third = yield* runtime.submit(
        agent,
        { question: "after the gap" },
        submitOptions(thread, "non-head-3"),
      );
      yield* runtime.abort(
        AbortCommand.make({
          submissionId: second.submissionId,
          author: "operator",
          reason: "cancelled while queued",
        }),
      );

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === second.submissionId);
      expect(report?.decision._tag).toBe("SettleAborted");
      expect(report?.disposition).toBe("repaired");

      // The aborted non-head settled immediately — the HEAD is still unsettled.
      const settled = yield* runtime.awaitSettlement(second);
      expect(settled.outcome).toBe("aborted");
      const ledger = yield* SubmissionLedger;
      const headRow = yield* ledger.lookup(
        SubmissionLookupById.make({ submissionId: head.submissionId }),
      );
      expect(Option.isSome(headRow) && headRow.value.state !== "settled").toBe(true);

      const midRecords = yield* readLog(thread);
      const midTags = logTags(midRecords);
      // Never claimed: no model ran and no canonical input exists for the aborted row, and
      // its abort record precedes its settlement (durability §13).
      expect(midRecords.map((envelope) => envelope.record.recordId)).not.toContain(
        submissionInputRecordId(second.submissionId),
      );
      expect(midTags.indexOf("AbortRequested")).toBeGreaterThanOrEqual(0);
      expect(midRecords.map((envelope) => envelope.record.recordId)).toContain(
        recoveryRepairRecordId(second.submissionId, "SettleAborted"),
      );

      // The head then runs normally and the THIRD Submission joins its Run across the
      // aborted-settled row — the joining-prefix gap rule treats it as a non-gap.
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements.map((settlement) => settlement.submissionId)).toEqual([head.submissionId]);
      const headSettled = yield* runtime.awaitSettlement(head);
      expect(headSettled.outcome).toBe("completed");
      const thirdSettled = yield* runtime.awaitSettlement(third);
      expect(thirdSettled.outcome).toBe("completed");

      // Canonical order: the aborted settlement precedes the head's settlement — that is the
      // documented §7(c) exemption, and the shared invariant checker accepts it.
      const records = yield* readLog(thread);
      const recordIds = records.map((envelope) => envelope.record.recordId);
      expect(recordIds.indexOf(submissionSettlementRecordId(second.submissionId))).toBeLessThan(
        recordIds.indexOf(submissionSettlementRecordId(head.submissionId)),
      );
      const integrity = yield* runtime.verify(decodeThreadId(thread));
      expect(integrity.ok).toBe(true);
      expect(integrity.checks.find((check) => check.name === "fifo-settlement-order")?.status).toBe(
        "passed",
      );
    }),
  );

  it.effect("aborts a running Submission canonically before interrupting its Run fiber", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const started = yield* Deferred.make<void>();
      const latch = yield* Deferred.make<void>();
      const model = Model.make(
        "scripted",
        "durable-blocked",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.fromEffect(
                Deferred.succeed(started, void 0).pipe(Effect.andThen(Deferred.await(latch))),
              ).pipe(Stream.flatMap(() => Stream.fromIterable(finalParts('{"answer":"never"}')))),
          }),
        ),
      );
      const agent = Agent.withModel(plannerDefinition, model);
      const thread = "thread-abort-running";

      const receipt = yield* runtime.submit(
        agent,
        { question: "block" },
        submitOptions(thread, "abort-running-1"),
      );
      const worker = yield* Effect.forkChild(runtime.processThread(agent, decodeThreadId(thread)));
      yield* Deferred.await(started);

      yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "stop the run",
        }),
      );
      yield* TestClock.adjust(Duration.millis(150));

      const settlements = yield* Fiber.join(worker);
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("aborted");
      expect(yield* Deferred.isDone(latch)).toBe(false);

      const records = yield* readLog(thread);
      const tags = logTags(records);
      expect(tags.indexOf("AbortRequested")).toBeGreaterThanOrEqual(0);
      expect(tags.indexOf("AbortRequested")).toBeLessThan(tags.indexOf("SubmissionSettled"));
      const settled = records.at(-1)?.record.payload;
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.outcome).toBe("aborted");
      }
    }),
  );

  it.effect("fences a superseded Attempt out of canonical history", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const started = yield* Deferred.make<void>();
      const latch = yield* Deferred.make<void>();
      const calls = yield* Ref.make(0);
      const model = Model.make(
        "scripted",
        "durable-fencing",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(calls, (call) => call + 1).pipe(
                  Effect.map((call) =>
                    call === 0
                      ? Stream.fromEffect(
                          Deferred.succeed(started, void 0).pipe(
                            Effect.andThen(Deferred.await(latch)),
                          ),
                        ).pipe(
                          Stream.flatMap(() =>
                            Stream.fromIterable(finalParts('{"answer":"stale"}')),
                          ),
                        )
                      : Stream.fromIterable(finalParts('{"answer":"fresh"}')),
                  ),
                ),
              ),
          }),
        ),
      );
      const agent = Agent.withModel(plannerDefinition, model);
      const thread = "thread-fencing";

      const receipt = yield* runtime.submit(
        agent,
        { question: "who owns the lane?" },
        submitOptions(thread, "fencing-1"),
      );

      const staleWorker = yield* Effect.forkChild(
        runtime.processThread(agent, decodeThreadId(thread)),
      );
      yield* Deferred.await(started);

      // A second Attempt (same producer restarting) supersedes the first: higher epoch.
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");

      // Unblock the stale Attempt: its canonical append must be fenced, not committed.
      yield* Deferred.succeed(latch, void 0);
      const staleExit = yield* Fiber.await(staleWorker);
      expect(["FenceRejected", "OwnershipLost"]).toContain(failureTag(staleExit));

      const records = yield* readLog(thread);
      const runId = runIdForSubmission(receipt.submissionId);
      const turnRecords = records.filter(
        (envelope) => envelope.record.recordId === modelResponseRecordId(runId, 1),
      );
      expect(turnRecords).toHaveLength(1);
      const firstTurn = turnRecords[0]?.record.payload;
      if (firstTurn?._tag === "ModelResponseRecorded") {
        const messages = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(firstTurn.messages);
        const text = messages.content
          .filter((message) => message.role === "assistant")
          .flatMap((message) => message.content)
          .filter((part) => part.type === "text")
          .map((part) => part.text)
          .join("");
        expect(text).toBe('{"answer":"fresh"}');
      }
      const settledRecords = logTags(records).filter((tag) => tag === "SubmissionSettled");
      expect(settledRecords).toHaveLength(1);
    }),
  );

  it.effect("a failpoint-interrupted submit resumes to the same Receipt", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"resume"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-submit-resume";
      const options = submitOptions(thread, "resume-1");

      yield* armFailpoint("submit:after-admit");
      const interrupted = yield* Effect.exit(runtime.submit(agent, { question: "kill" }, options));
      expect(failureTag(interrupted)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const admitted = yield* ledger.lookup(
        SubmissionLookupByKey.make({
          threadId: options.threadId,
          principal: options.principal,
          idempotencyKey: options.idempotencyKey,
        }),
      );
      expect(Option.isSome(admitted)).toBe(true);
      const resumed = yield* runtime.submit(agent, { question: "kill" }, options);
      if (Option.isSome(admitted)) {
        expect(admitted.value.state).toBe("admitted");
        expect(resumed.submissionId).toBe(admitted.value.submissionId);
        expect(resumed.receiptId).toBe(admitted.value.receiptId);
      }
      expect(yield* lookupState(resumed.submissionId)).toBe("ready");

      // Same idempotency key resumes to the same Receipt after the materialize-boundary kill too.
      yield* armFailpoint("submit:after-materialize");
      const options2 = submitOptions(thread, "resume-2");
      const killed = yield* Effect.exit(runtime.submit(agent, { question: "kill" }, options2));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      const receipt2 = yield* runtime.submit(agent, { question: "kill" }, options2);
      const resubmitted = yield* runtime.submit(agent, { question: "kill" }, options2);
      expect(resubmitted).toEqual(receipt2);
      expect(yield* lookupState(receipt2.submissionId)).toBe("ready");
    }),
  );

  it.effect(
    "recovery completes materialization and repairs readiness (kill submit boundaries)",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
        const agent = Agent.withModel(plannerDefinition, scripted.model);

        // Kill after admission: nothing durable beyond the ledger row.
        yield* armFailpoint("submit:after-admit");
        const killedAdmit = yield* Effect.exit(
          runtime.submit(
            agent,
            { question: "recover me" },
            submitOptions("thread-recover-admit", "recover-1"),
          ),
        );
        expect(failureTag(killedAdmit)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const reports = yield* runtime.runRecovery;
        const admitReport = reports.find((entry) => entry.threadId === "thread-recover-admit");
        expect(admitReport?.decision._tag).toBe("CompleteMaterialization");
        expect(admitReport?.disposition).toBe("repaired");
        if (admitReport !== undefined) {
          expect(yield* lookupState(admitReport.submissionId)).toBe("ready");
        }
        expect(logTags(yield* readLog("thread-recover-admit"))).toContain("ThreadCreated");

        // Kill after materialization: only the readiness marker is missing.
        yield* armFailpoint("submit:after-materialize");
        const killedReady = yield* Effect.exit(
          runtime.submit(
            agent,
            { question: "recover me" },
            submitOptions("thread-recover-ready", "recover-2"),
          ),
        );
        expect(failureTag(killedReady)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const readinessReports = yield* runtime.runRecovery;
        const repaired = readinessReports.find(
          (report) => report.threadId === "thread-recover-ready",
        );
        expect(repaired?.decision._tag).toBe("RepairReadiness");
        expect(repaired?.disposition).toBe("repaired");
        if (repaired !== undefined) {
          expect(yield* lookupState(repaired.submissionId)).toBe("ready");
        }
      }),
  );

  it.effect("recovery re-applies input after a claim-boundary kill (exactly one record)", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"applied"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-recover-claim";

      const receipt = yield* runtime.submit(
        agent,
        { question: "apply once" },
        submitOptions(thread, "claim-kill-1"),
      );
      yield* armFailpoint("claim:after-claim");
      const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("ApplyInput");
      expect(report?.disposition).toBe("repaired");

      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
      );
      expect(snapshot.inputApplied?.recordId).toBe(submissionInputRecordId(receipt.submissionId));

      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements[0]?.outcome).toBe("completed");
      const inputRecords = (yield* readLog(thread)).filter(
        (envelope) => envelope.record.recordId === submissionInputRecordId(receipt.submissionId),
      );
      expect(inputRecords).toHaveLength(1);
    }),
  );

  it.effect("recovery repairs a lost input marker from canonical history (DUR-015)", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"repaired"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-recover-marker";

      const receipt = yield* runtime.submit(
        agent,
        { question: "mark me" },
        submitOptions(thread, "marker-1"),
      );
      yield* armFailpoint("input:after-canonical-append");
      const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("RepairInputMarker");
      expect(report?.disposition).toBe("repaired");

      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements[0]?.outcome).toBe("completed");
      const inputRecords = (yield* readLog(thread)).filter(
        (envelope) => envelope.record.recordId === submissionInputRecordId(receipt.submissionId),
      );
      expect(inputRecords).toHaveLength(1);
    }),
  );

  it.effect("recovery appends a reserved-but-unappended settlement exactly as reserved", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"reserved"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-recover-reserved";

      const receipt = yield* runtime.submit(
        agent,
        { question: "reserve" },
        submitOptions(thread, "reserved-1"),
      );
      yield* armFailpoint("terminalize:after-reserve");
      const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("AppendReservedSettlement");
      expect(report?.disposition).toBe("repaired");

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("completed");
      const records = yield* readLog(thread);
      const settledRecords = records.filter(
        (envelope) => envelope.record.payload._tag === "SubmissionSettled",
      );
      expect(settledRecords).toHaveLength(1);
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        recoveryRepairRecordId(receipt.submissionId, "AppendReservedSettlement"),
      );
    }),
  );

  it.effect("recovery finalizes the ledger from history without rewriting the record", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"finalized"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-recover-finalize";

      const receipt = yield* runtime.submit(
        agent,
        { question: "finalize" },
        submitOptions(thread, "finalize-1"),
      );
      yield* armFailpoint("terminalize:after-canonical-append");
      const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // The canonical settlement exists while the ledger row is still nonterminal.
      const beforeRecords = yield* readLog(thread);
      expect(logTags(beforeRecords)).toContain("SubmissionSettled");
      expect(yield* lookupState(receipt.submissionId)).not.toBe("settled");

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("FinalizeLedgerFromHistory");
      expect(report?.disposition).toBe("repaired");

      expect(yield* lookupState(receipt.submissionId)).toBe("settled");
      const afterRecords = yield* readLog(thread);
      const settledRecords = afterRecords.filter(
        (envelope) => envelope.record.payload._tag === "SubmissionSettled",
      );
      expect(settledRecords).toHaveLength(1);
      expect(settledRecords[0]).toEqual(
        beforeRecords.find((envelope) => envelope.record.payload._tag === "SubmissionSettled"),
      );
    }),
  );

  it.effect("a new Attempt resumes from the last committed Turn boundary", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0 ? toolCallParts : finalParts('{"answer":"Resumed."}'),
      );
      const agent = Agent.withModel(searchDefinition, scripted.model);
      const thread = "thread-resume-turn";

      const receipt = yield* runtime.submit(
        agent,
        { question: "resume?" },
        submitOptions(thread, "resume-turn-1"),
      );
      // P5 split commits: the readonly tool Turn commits its response batch at the finish part
      // and its results batch at the next TurnStarted seam — kill right after the results append
      // so Turn 1 is fully canonical and the run is not settled (the P4 boundary, same shape).
      yield* armFailpoint("turn:after-results-append");
      const killed = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(searchToolLayer)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // Turn 1 (model response + settled Tool batch) is canonical; the run is not settled.
      const runId = runIdForSubmission(receipt.submissionId);
      const committed = yield* readLog(thread);
      expect(logTags(committed)).toEqual([
        "ThreadCreated",
        "UserInputRecorded",
        "RunStarted",
        "ModelResponseRecorded",
        "ToolCallSettled",
      ]);

      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(searchToolLayer));
      expect(settlements[0]?.outcome).toBe("completed");

      // The resumed Attempt's model request saw the canonical prompt, tool result included,
      // without re-appended instructions or input. The second system message is the
      // request-time model-visible output contract (RUN-028) and the trailing user
      // message is the derived run-status line (RUN-024) — neither is canonical.
      expect(scripted.prompts).toHaveLength(2);
      const resumedPrompt = scripted.prompts[1];
      expect(resumedPrompt?.content.map((message) => message.role)).toEqual([
        "system",
        "system",
        "user",
        "assistant",
        "tool",
        "user",
      ]);

      const records = yield* readLog(thread);
      expect(
        records.filter((envelope) => envelope.record.recordId === modelResponseRecordId(runId, 1)),
      ).toHaveLength(1);
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        modelResponseRecordId(runId, 2),
      );
      const settled = records.at(-1)?.record.payload;
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.result).toEqual({ answer: "Resumed." });
      }
    }),
  );

  const typeProofReceipt = Receipt.make({
    receiptId: Schema.decodeSync(ReceiptId)("receipt-types"),
    submissionId: Schema.decodeSync(SubmissionId)("submission-types"),
    threadId: decodeThreadId("thread-types"),
    queueSequence: Schema.decodeSync(QueueSequence)(1),
  });

  it.effect("keeps failure and requirement channels typed (E/R proofs)", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"typed"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);

      const submitProgram = runtime.submit(
        agent,
        { question: "typed" },
        submitOptions("thread-types", "types-1"),
      );
      type SubmitError = Effect.Error<typeof submitProgram>;
      const submitHasAdmissionConflict: AdmissionConflict extends SubmitError ? true : false = true;
      const submitHasFailpoint: DurableRuntimeFailpointError extends SubmitError ? true : false =
        true;

      const awaitProgram = runtime.awaitSettlement(typeProofReceipt);
      type AwaitError = Effect.Error<typeof awaitProgram>;
      type AwaitSuccess = Effect.Success<typeof awaitProgram>;
      const awaitHasConflict: SettlementConflict extends AwaitError ? true : false = true;
      const awaitReturnsSettlement: AwaitSuccess extends Settlement ? true : false = true;

      const workerProgram = runtime.processThread(
        Agent.withModel(searchDefinition, scripted.model),
        decodeThreadId("thread-types"),
      );
      type WorkerError = Effect.Error<typeof workerProgram>;
      type WorkerServices = Effect.Services<typeof workerProgram>;
      const workerHasFence: FenceRejected extends WorkerError ? true : false = true;
      const workerProvidesIdGenerator: IdGenerator extends WorkerServices ? true : false = false;

      expect(submitHasAdmissionConflict).toBe(true);
      expect(submitHasFailpoint).toBe(true);
      expect(awaitHasConflict).toBe(true);
      expect(awaitReturnsSettlement).toBe(true);
      expect(workerHasFence).toBe(true);
      expect(workerProvidesIdGenerator).toBe(false);
    }),
  );

  describe("run journal projections", () => {
    const runId = runIdForSubmission(Schema.decodeSync(SubmissionId)("submission-journal"));
    const otherRunId = runIdForSubmission(Schema.decodeSync(SubmissionId)("submission-other"));
    const callOneId = Schema.decodeSync(ToolCallId)("call-1");
    const producerId = Schema.decodeSync(ProducerId)("producer-journal");
    const deploymentId = Schema.decodeSync(DeploymentId)("deployment-journal");
    const createdAt = DateTime.toUtc(DateTime.makeUnsafe(1_000));

    const turnOneAppended: ReadonlyArray<Prompt.Message> = [
      Prompt.makeMessage("system", { content: "Answer as JSON." }),
      Prompt.makeMessage("user", {
        content: [Prompt.makePart("text", { text: '{"question":"resume?"}' })],
      }),
      Prompt.makeMessage("assistant", {
        content: [
          Prompt.makePart("tool-call", {
            id: "call-1",
            name: "search",
            params: { query: "sea" },
            providerExecuted: false,
          }),
        ],
      }),
      Prompt.makeMessage("tool", {
        content: [
          Prompt.makePart("tool-result", {
            id: "call-1",
            name: "search",
            result: { available: true },
            isFailure: false,
            providerExecuted: false,
          }),
        ],
      }),
    ];
    const turnTwoAppended: ReadonlyArray<Prompt.Message> = [
      Prompt.makeMessage("assistant", {
        content: [Prompt.makePart("text", { text: '{"answer":"Found."}' })],
      }),
    ];

    const envelopeAt = (sequence: number, record: CanonicalRecordEnvelope["record"]) =>
      CanonicalRecordEnvelope.make({
        threadId: decodeThreadId("thread-journal"),
        batchId: Schema.decodeSync(BatchId)(`batch-journal-${sequence}`),
        sequence: Schema.decodeSync(CanonicalSequence)(sequence),
        offset: Schema.decodeSync(ObservationOffset)(`memory:${sequence}`),
        record,
      });

    it.effect("folds Turn seams into batches and replays them to the same prompt", () =>
      Effect.gen(function* () {
        const batchOne = yield* turnCanonicalBatch({
          runId,
          turn: 1,
          turnId: turnIdForRun(runId, 1),
          appended: turnOneAppended,
          producerId,
          deploymentId,
          createdAt,
        });
        expect(batchOne.records.map((record) => record.recordId)).toEqual([
          modelResponseRecordId(runId, 1),
          `tool-settled:${runId}:1:call-1`,
        ]);
        expect(batchOne.records[0]?.payload._tag).toBe("ModelResponseRecorded");
        expect(batchOne.records[1]?.payload._tag).toBe("ToolCallSettled");

        const batchTwo = yield* turnCanonicalBatch({
          runId,
          turn: 2,
          turnId: turnIdForRun(runId, 2),
          appended: turnTwoAppended,
          producerId,
          deploymentId,
          createdAt,
        });

        const journalRecords = [
          ...batchOne.records.map((record, index) => envelopeAt(index + 1, record)),
          ...batchTwo.records.map((record, index) =>
            envelopeAt(batchOne.records.length + index + 1, record),
          ),
        ];

        const prompt = yield* promptFromCanonicalRecords(journalRecords);
        expect(prompt.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
          "tool",
          "assistant",
        ]);
        const toolMessage = prompt.content.find((message) => message.role === "tool");
        expect(
          toolMessage?.content.filter((part) => part.type === "tool-result").map((part) => part.id),
        ).toEqual(["call-1"]);

        const projection = yield* projectRunJournal(journalRecords, runId);
        expect(projection.committedTurns).toBe(2);
        expect(projection.historyBefore.content).toHaveLength(0);

        // Replay equivalence: projecting the same canonical records is deterministic.
        const replayed = yield* projectRunJournal(journalRecords, runId);
        const encodedFirst = yield* Schema.encodeEffect(Prompt.Prompt)(projection.prompt);
        const encodedReplayed = yield* Schema.encodeEffect(Prompt.Prompt)(replayed.prompt);
        expect(encodedReplayed).toEqual(encodedFirst);

        // Another Run's projection treats these records as prior history.
        const other = yield* projectRunJournal(journalRecords, otherRunId);
        expect(other.committedTurns).toBe(0);
        expect(other.historyBefore.content).toHaveLength(prompt.content.length);
      }),
    );

    it.effect("rejects a Turn with no model-visible messages", () =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          turnCanonicalBatch({
            runId,
            turn: 1,
            turnId: turnIdForRun(runId, 1),
            appended: [],
            producerId,
            deploymentId,
            createdAt,
          }),
        );
        expect(failureTag(exit)).toBe("RunJournalError");
      }),
    );

    it.effect("keeps tool-settled record identities per (run, turn, toolCallId)", () =>
      Effect.gen(function* () {
        const batch = yield* turnCanonicalBatch({
          runId,
          turn: 3,
          turnId: turnIdForRun(runId, 3),
          appended: turnOneAppended,
          producerId,
          deploymentId,
          createdAt,
        });
        const toolRecord = batch.records[1];
        expect(toolRecord?.recordId).toBe(toolCallSettledRecordId(runId, 3, callOneId));
        if (toolRecord?.payload._tag === "ToolCallSettled") {
          expect(toolRecord.payload.result).toEqual({ available: true });
          expect(toolRecord.payload.isFailure).toBe(false);
        }
      }),
    );
  });
});

layer(corruptedCompletionTestLayer)("RUN-032 recovered completion validation", (it) => {
  it.effect("rejects a no-Tool marker whose output disagrees with its canonical response", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"canonical"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const thread = "thread-hostile-no-tool-completion";

      yield* runtime.submit(
        agent,
        { question: "validate recovery" },
        submitOptions(thread, "hostile-no-tool-completion-1"),
      );
      yield* armFailpoint("turn:after-canonical-append");
      const crashed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const recovered = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(recovered)).toBe("RunJournalError");
      expect(scripted.prompts).toHaveLength(1);
    }),
  );

  it.effect("rejects a completion-Tool marker that disagrees with its canonical result", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const PostMessage = Tool.make("post_message", {
        parameters: Schema.Struct({ message: Schema.String }),
        success: Schema.Struct({ messageId: Schema.String }),
      });
      const tools = Toolkit.make(PostMessage);
      const definition = Agent.make("hostile-terminal-delivery", {
        input: Schema.Struct({ question: Schema.String }),
        output: DeliveryCompletionOutput,
        instructions: "Deliver through post_message.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
        completion: {
          tool: "post_message",
          project: ({ result }) => ({ messageId: result.messageId, delivered: true }),
        },
      });
      const scripted = yield* makeScriptedModel(() => [
        {
          type: "tool-call",
          id: "hostile-delivery-1",
          name: "post_message",
          params: { message: "canonical delivery" },
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const handlerCalls = yield* Ref.make(0);
      const toolLayer = tools.toLayer({
        post_message: () =>
          Ref.updateAndGet(handlerCalls, (count) => count + 1).pipe(
            Effect.as({ messageId: "canonical-message" }),
          ),
      });
      const agent = Agent.withModel(definition, scripted.model);
      const thread = "thread-hostile-tool-completion";

      yield* runtime.submit(
        agent,
        { question: "validate delivery recovery" },
        submitOptions(thread, "hostile-tool-completion-1"),
      );
      yield* armFailpoint("turn:after-results-append");
      const crashed = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
      );
      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      expect(yield* Ref.get(handlerCalls)).toBe(1);
      yield* clearFailpoint;

      const recovered = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
      );
      expect(failureTag(recovered)).toBe("RunJournalError");
      expect(yield* Ref.get(handlerCalls)).toBe(1);
      expect(scripted.prompts).toHaveLength(1);
    }),
  );
});

layer(injectedProviderCallTestLayer)("RUN-032 recovered completion singleton validation", (it) => {
  it.effect("rejects a completion marker mixed with a provider-executed Tool Call", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const PostMessage = Tool.make("post_message", {
        parameters: Schema.Struct({ message: Schema.String }),
        success: Schema.Struct({ messageId: Schema.String }),
      });
      const tools = Toolkit.make(PostMessage);
      const definition = Agent.make("hostile-mixed-terminal-delivery", {
        input: Schema.Struct({ question: Schema.String }),
        output: DeliveryCompletionOutput,
        instructions: "Deliver through post_message.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
        completion: {
          tool: "post_message",
          project: ({ result }) => ({ messageId: result.messageId, delivered: true }),
        },
      });
      const scripted = yield* makeScriptedModel(() => [
        {
          type: "tool-call",
          id: "mixed-delivery-1",
          name: "post_message",
          params: { message: "canonical delivery" },
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);
      const handlerCalls = yield* Ref.make(0);
      const toolLayer = tools.toLayer({
        post_message: () =>
          Ref.updateAndGet(handlerCalls, (count) => count + 1).pipe(
            Effect.as({ messageId: "canonical-message" }),
          ),
      });
      const agent = Agent.withModel(definition, scripted.model);
      const thread = "thread-hostile-mixed-tool-completion";

      yield* runtime.submit(
        agent,
        { question: "validate singleton recovery" },
        submitOptions(thread, "hostile-mixed-tool-completion-1"),
      );
      yield* armFailpoint("turn:after-results-append");
      const crashed = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
      );
      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      expect(yield* Ref.get(handlerCalls)).toBe(1);
      yield* clearFailpoint;

      const recovered = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
      );
      expect(failureTag(recovered)).toBe("RunJournalError");
      expect(yield* Ref.get(handlerCalls)).toBe(1);
      expect(scripted.prompts).toHaveLength(1);
    }),
  );
});

layer(corruptedRunDispositionTestLayer)("RUN-029 recovered run disposition validation", (it) => {
  it.effect("rejects a marker whose disposition disagrees with its reconstructed output", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() =>
        finalParts('{"answer":"done","runDisposition":"application-complete"}'),
      );
      const agent = Agent.withModel(dispositionDefinition, scripted.model);
      const thread = "thread-hostile-run-disposition";

      yield* runtime.submit(
        agent,
        { question: "validate disposition recovery" },
        submitOptions(thread, "hostile-run-disposition-1"),
      );
      yield* armFailpoint("turn:after-canonical-append");
      const crashed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const recovered = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(recovered)).toBe("RunJournalError");
      expect(scripted.prompts).toHaveLength(1);
    }),
  );
});

layer(testLayer)("RUN-026 durable compaction and usage re-seed", (it) => {
  it.effect(
    "mixed pending batches restore provider failures in declaration order exactly once",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        for (const providerFirst of [true, false]) {
          yield* clearFailpoint;
          const hosted = Tool.providerDefined({
            id: "test.hosted_probe",
            customName: "HostedProbe",
            providerName: "hosted_probe",
            parameters: Schema.Struct({}),
            success: Schema.String,
          })(undefined);
          const probe = Tool.make("probe", {
            parameters: Schema.Struct({}),
            success: Schema.String,
            failure: Schema.String,
            failureMode: "return",
          });
          const toolkit = Toolkit.make(hosted, probe);
          const definition = Agent.make("mixed-resume-policy", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Probe.",
            toolkit,
            policy: AgentPolicy.make({
              maxTurns: 5,
              maxToolCalls: 4,
              maxDuration: "30 seconds",
              toolConcurrency: 1,
              repeatedFailureLimit: 2,
              onExhaustion: "fail",
              toolResultBounds: { maxBytes: 512 },
            }),
          });
          const providerCall: Response.StreamPartEncoded = {
            type: "tool-call",
            id: "hosted-1",
            name: "HostedProbe",
            params: {},
            providerExecuted: true,
          };
          const appCall: Response.StreamPartEncoded = {
            type: "tool-call",
            id: "app-1",
            name: "probe",
            params: {},
            providerExecuted: false,
          };
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? [
                  ...(providerFirst ? [providerCall, appCall] : [appCall, providerCall]),
                  {
                    type: "tool-result",
                    id: "hosted-1",
                    name: "HostedProbe",
                    result: "provider outcome".repeat(100),
                    isFailure: providerFirst,
                    providerExecuted: true,
                  },
                  { type: "finish", reason: "tool-calls", usage },
                ]
              : call === 1
                ? [
                    {
                      type: "tool-call",
                      id: "app-2",
                      name: "probe",
                      params: {},
                      providerExecuted: false,
                    },
                    { type: "finish", reason: "tool-calls", usage },
                  ]
                : finalParts('"done"'),
          );
          const agent = Agent.withModel(definition, scripted.model);
          const starts = yield* Ref.make(0);
          const handlers = toolkit.toLayer({
            probe: () =>
              Ref.update(starts, (n) => n + 1).pipe(Effect.andThen(Effect.fail("failed"))),
          });
          const receipt = yield* runtime.submit(
            agent,
            "probe",
            submitOptions(`mixed-provider-${providerFirst}`, "mixed"),
          );
          const run = runtime.processThread(agent, receipt.threadId).pipe(Effect.provide(handlers));
          yield* armFailpoint("turn:after-response-append");
          expect(failureTag(yield* Effect.exit(run))).toBe("DurableRuntimeFailpointError");
          expect(yield* Ref.get(starts)).toBe(0);
          const before = yield* readLog(receipt.threadId);
          const response = before.find(
            ({ record }) => record.payload._tag === "ModelResponseRecorded",
          )?.record.payload;
          if (response?._tag !== "ModelResponseRecorded")
            throw new Error("Expected canonical response");
          const recordedPrompt = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(
            response.messages,
          );
          expect(
            recordedPrompt.content.flatMap((message) =>
              message.role === "assistant"
                ? message.content.filter((part) => part.type === "tool-result")
                : [],
            ),
          ).toMatchObject([{ id: "hosted-1", isFailure: providerFirst }]);
          expect(
            (yield* projectRunJournal(before, runIdForSubmission(receipt.submissionId)))
              .policyUsage,
          ).toMatchObject({ toolCalls: 2, consecutiveToolFailures: 0 });
          yield* clearFailpoint;
          expect((yield* run)[0]?.outcome).toBe(providerFirst ? "failed" : "completed");
          expect(scripted.prompts).toHaveLength(providerFirst ? 1 : 3);
          expect(yield* Ref.get(starts)).toBe(providerFirst ? 1 : 2);
          const journal = yield* projectRunJournal(
            yield* readLog(receipt.threadId),
            runIdForSubmission(receipt.submissionId),
          );
          expect(journal.policyUsage.consecutiveToolFailures).toBe(providerFirst ? 0 : 1);
        }
      }),
  );
  it.effect("programmatic reservations survive loss before the inner Handler starts", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      for (const location of [
        "policy:before-reservation-append",
        "policy:after-reservation-append",
      ] as const) {
        yield* clearFailpoint;
        const inner = Toolkit.make(
          Tool.make("query", { parameters: Schema.Struct({}), success: Schema.String }),
        );
        const outer = Toolkit.make(
          Tool.make("orchestrate", { parameters: Schema.Struct({}), success: Schema.String })
            .addDependency(ToolBroker)
            .annotate(ToolExecutionClass, "idempotent"),
        );
        const definition = Agent.make("reservation-recovery", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Query.",
          toolkit: outer,
          policy: AgentPolicy.make({
            maxTurns: 5,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            onExhaustion: "fail",
          }),
        });
        const executions = yield* Ref.make(0);
        const handlers = outer
          .toLayer(
            Effect.gen(function* () {
              const innerTools = yield* inner;
              return {
                orchestrate: () =>
                  Effect.gen(function* () {
                    const broker = yield* ToolBroker;
                    const pass = yield* broker
                      .openPass(innerTools, { maxResultBytes: 1024 })
                      .pipe(Effect.orDie);
                    yield* pass.invoke({ toolName: "query", encodedArguments: {} });
                    return "done";
                  }),
              };
            }),
          )
          .pipe(
            Layer.provide(
              inner.toLayer({
                query: () => Ref.update(executions, (n) => n + 1).pipe(Effect.as("ok")),
              }),
            ),
          );
        const scripted = yield* makeScriptedModel(() => [
          {
            type: "tool-call",
            id: "outer",
            name: "orchestrate",
            params: {},
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage },
        ]);
        const agent = Agent.withModel(definition, scripted.model);
        const receipt = yield* runtime.submit(
          agent,
          "query",
          submitOptions(`programmatic-${location}`, "reservation"),
        );
        const run = runtime.processThread(agent, receipt.threadId).pipe(Effect.provide(handlers));
        yield* armFailpoint(location);
        expect(failureTag(yield* Effect.exit(run))).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(executions)).toBe(0);
        yield* clearFailpoint;
        expect((yield* run)[0]?.outcome).toBe("failed");
        expect(yield* Ref.get(executions)).toBe(
          location === "policy:before-reservation-append" ? 1 : 0,
        );
        expect(scripted.prompts).toHaveLength(2);
        const journal = yield* projectRunJournal(
          yield* readLog(receipt.threadId),
          runIdForSubmission(receipt.submissionId),
        );
        expect(journal.policyUsage.programmaticToolCalls).toBe(1);
      }
    }),
  );

  it.effect("a durably reserved grace finalization is not granted to the replacement Attempt", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      yield* clearFailpoint;
      const definition = Agent.make("grace-recovery", {
        input: searchDefinition.input,
        output: searchDefinition.output,
        instructions: "Search.",
        toolkit: searchDefinition.toolkit,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 10,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const scripted = yield* makeScriptedModel((call) =>
        call === 0 ? toolCallParts : finalParts('{"answer":"grace"}'),
      );
      const agent = Agent.withModel(definition, scripted.model);
      const receipt = yield* runtime.submit(
        agent,
        { question: "search" },
        submitOptions("grace-reservation-recovery", "grace"),
      );
      const run = runtime
        .processThread(agent, receipt.threadId)
        .pipe(Effect.provide(searchToolLayer));
      yield* armFailpoint("policy:after-reservation-append");
      expect(failureTag(yield* Effect.exit(run))).toBe("DurableRuntimeFailpointError");
      expect(scripted.prompts).toHaveLength(1);
      yield* clearFailpoint;
      expect((yield* run)[0]?.outcome).toBe("failed");
      expect(scripted.prompts).toHaveLength(1);
      const journal = yield* projectRunJournal(
        yield* readLog(receipt.threadId),
        runIdForSubmission(receipt.submissionId),
      );
      expect(journal.policyUsage.finalizationUsed).toBe(true);
    }),
  );
  it.effect("replacement Attempts preserve the original Turn, Tool and failure limits", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      for (const limit of ["turns", "tool-calls", "repeated-failures"] as const) {
        yield* clearFailpoint;
        const tool = Tool.make("probe", {
          parameters: Schema.Struct({}),
          success: Schema.String,
          failure: Schema.String,
          failureMode: "return",
        });
        const toolkit = Toolkit.make(tool);
        const definition = Agent.make(`resume-${limit}`, {
          input: Schema.String,
          output: Schema.String,
          instructions: "Keep probing.",
          toolkit,
          policy: AgentPolicy.make({
            maxTurns: limit === "turns" ? 3 : 10,
            maxToolCalls: limit === "tool-calls" ? 2 : 10,
            repeatedFailureLimit: limit === "repeated-failures" ? 2 : 0,
            onExhaustion: "fail",
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const scripted = yield* makeScriptedModel((call) => [
          {
            type: "tool-call",
            id: `probe-${call}`,
            name: "probe",
            params: {},
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage },
        ]);
        const agent = Agent.withModel(definition, scripted.model);
        const executions = yield* Ref.make(0);
        const handlers = toolkit.toLayer({
          probe: () =>
            Ref.update(executions, (n) => n + 1).pipe(
              Effect.andThen(
                limit === "repeated-failures" ? Effect.fail("failed") : Effect.succeed("ok"),
              ),
            ),
        });
        const receipt = yield* runtime.submit(
          agent,
          "probe",
          submitOptions(`limits-${limit}`, "limits"),
        );
        const run = runtime.processThread(agent, receipt.threadId).pipe(Effect.provide(handlers));
        yield* armFailpoint("turn:after-results-append");
        for (let attempt = 0; attempt < (limit === "repeated-failures" ? 1 : 2); attempt++) {
          const exit = yield* Effect.exit(run);
          expect(exit._tag, `${limit} Attempt ${attempt}: ${JSON.stringify(exit)}`).toBe("Failure");
          expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
        }
        yield* clearFailpoint;
        const settlements = yield* run;
        expect(settlements[0]?.outcome).toBe("failed");
        expect(yield* Ref.get(executions)).toBe(2);
        expect(scripted.prompts.length).toBe(limit === "repeated-failures" ? 2 : 3);
        expect((yield* readLog(receipt.threadId)).at(-1)?.record.payload).toMatchObject({
          policyLimit: limit,
        });
      }
    }),
  );

  const usageOf = (input: number, output: number) => ({
    inputTokens: { total: input },
    outputTokens: { total: output },
  });

  const finalPartsWithUsage = (
    text: string,
    used: ReturnType<typeof usageOf>,
  ): ReadonlyArray<Response.StreamPartEncoded> => [
    { type: "text-start", id: "answer" },
    { type: "text-delta", id: "answer", delta: text },
    { type: "text-end", id: "answer" },
    { type: "finish", reason: "stop", usage: used },
  ];

  const toolCallPartsWithUsage = (
    used: ReturnType<typeof usageOf>,
  ): ReadonlyArray<Response.StreamPartEncoded> => [
    {
      type: "tool-call",
      id: "search-1",
      name: "search",
      params: { query: "sea" },
      providerExecuted: false,
    },
    { type: "finish", reason: "tool-calls", usage: used },
  ];

  const promptTexts = (prompt: Prompt.Prompt): string =>
    prompt.content
      .map((message) =>
        typeof message.content === "string"
          ? message.content
          : message.content
              .map((part) => ("text" in part && typeof part.text === "string" ? part.text : ""))
              .join(""),
      )
      .join("\n");

  it.effect(
    "RUN-026: compaction commits one canonical record across a failpoint re-drive and later Runs fold it",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const thread = "thread-compaction";

        // Submission 1: an ordinary tool Run leaves prior-Run records to cover.
        const first = yield* makeScriptedModel((call) =>
          call === 0
            ? toolCallParts
            : finalParts(JSON.stringify({ answer: `Found the sea. ${"PAD".repeat(1_000)}` })),
        );
        yield* runtime.submit(
          Agent.withModel(searchDefinition, first.model),
          { question: "Is a flight available?" },
          submitOptions(thread, "compaction-1"),
        );
        const firstSettled = yield* runtime
          .processThread(Agent.withModel(searchDefinition, first.model), decodeThreadId(thread))
          .pipe(Effect.provide(searchToolLayer));
        expect(firstSettled[0]?.outcome).toBe("completed");

        // Submission 2: a compacting agent whose estimated context exceeds the limit
        // at Turn 1, forcing summarize; the summarizer response is model call 0 of
        // each Attempt, the final answer the call after it.
        const compactingDefinition = Agent.make("durable-compactor", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Answer from what is known.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 500,
            compaction: CompactionPolicy.make({ keepRecentTokens: 10, mode: "summarize" }),
          }),
        });
        const second = yield* makeScriptedModel((call) =>
          call === 0
            ? finalParts("Goal: prior run booked the flight")
            : finalParts('{"answer":"compacted"}'),
        );
        const compactor = Agent.withModel(compactingDefinition, second.model);
        yield* runtime.submit(
          compactor,
          { question: "what happened?" },
          submitOptions(thread, "compaction-2"),
        );

        // Crash immediately AFTER the compaction record commits, BEFORE the
        // Turn's model call: the re-driven Attempt must project the compacted
        // prompt and must NOT append a duplicate record.
        yield* armFailpoint("compaction:after-canonical-append");
        const crashed = yield* Effect.exit(
          runtime.processThread(compactor, decodeThreadId(thread)),
        );
        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        // The hook failure is typed AND ordering holds: the summarizer call
        // ran, but no compacted Turn request started before a successful
        // record commit.
        expect(second.prompts).toHaveLength(1);
        yield* clearFailpoint;

        const settled = yield* runtime.processThread(compactor, decodeThreadId(thread));
        expect(settled).toHaveLength(1);
        expect(settled[0]?.outcome).toBe("completed");

        const records = yield* readLog(thread);
        const compactions = records.filter(
          (envelope) => envelope.record.payload._tag === "CompactionCreated",
        );
        expect(compactions).toHaveLength(1);
        const payload = compactions[0]?.record.payload;
        if (payload === undefined || payload._tag !== "CompactionCreated") {
          throw new Error("expected a CompactionCreated record");
        }
        expect(payload.kind).toBe("summarize");
        expect(payload.summary).toBe("Goal: prior run booked the flight");
        expect(payload.coversThrough).toBeLessThan(compactions[0]?.sequence ?? 0);

        // The settled Run's final model call saw the compacted view.
        const lastPrompt = second.prompts.at(-1);
        if (lastPrompt === undefined) throw new Error("expected captured prompts");
        expect(promptTexts(lastPrompt)).toContain(COMPACTION_SUMMARY_PREFIX);
        // The canonical cutoff is the actual covered prefix, so recovery does not summarize it again.
        expect(second.prompts).toHaveLength(2);
      }),
  );

  it.effect(
    "commits only a custom strategy's covered prefix and recovers it before calling the Run Model",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const thread = "custom-compaction-coverage";
        const prior = yield* makeScriptedModel((call) =>
          finalParts(
            JSON.stringify({
              answer: call === 0 ? `FIRST ${"x".repeat(5_000)}` : "SECOND KEEP",
            }),
          ),
        );
        const original = Agent.withModel(plannerDefinition, prior.model);
        for (const key of ["first", "second"]) {
          yield* runtime.submit(original, { question: key }, submitOptions(thread, key));
          yield* runtime.processThread(original, decodeThreadId(thread));
        }
        const definition = Agent.make("custom-coverage", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Answer from retained history.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 1_000,
            compaction: CompactionPolicy.make({ mode: "summarize", keepRecentTokens: 1 }),
          }),
        });
        const model = yield* makeScriptedModel(() => finalParts('{"answer":"retained"}'));
        const agent = Agent.withModel(definition, model.model);
        yield* runtime.submit(agent, { question: "continue" }, submitOptions(thread, "third"));
        yield* armFailpoint("compaction:after-canonical-append");
        const interrupted = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.exit);
        expect(failureTag(interrupted)).toBe("DurableRuntimeFailpointError");
        expect(model.prompts).toHaveLength(0);
        yield* clearFailpoint;
        const settled = yield* runtime.processThread(agent, decodeThreadId(thread));
        expect(settled[0]?.outcome).toBe("completed");
        expect(model.prompts).toHaveLength(1);
        expect(promptTexts(model.prompts[0] ?? Prompt.empty)).toContain("SECOND KEEP");
        expect(promptTexts(model.prompts[0] ?? Prompt.empty)).toContain(
          "custom first-only summary",
        );
        const records = yield* readLog(thread);
        const responses = records.filter(
          (entry) => entry.record.payload._tag === "ModelResponseRecorded",
        );
        const compactions = records.filter(
          (entry) => entry.record.payload._tag === "CompactionCreated",
        );
        expect(compactions).toHaveLength(1);
        const payload = compactions[0]?.record.payload;
        if (payload?._tag !== "CompactionCreated") return yield* Effect.die("missing compaction");
        expect(payload.coversThrough).toBe(responses[0]?.sequence);
        const replay = yield* promptFromCanonicalRecords(records);
        expect(promptTexts(replay)).toContain("SECOND KEEP");
        expect(promptTexts(replay)).not.toContain("FIRST");
      }).pipe(
        Effect.provide(
          Layer.fresh(DurableAgentRuntime.layerWithServices).pipe(
            Layer.provide(
              Layer.succeed(RunContextPreparation, {
                compactor: ContextCompactor.of({
                  estimate: estimatePromptTokens,
                  compact: () =>
                    Stream.succeed({
                      kind: "summarize",
                      through: 1,
                      summary: "custom first-only summary",
                    }),
                }),
              }),
            ),
            Layer.provideMerge(baseLayer),
          ),
        ),
      ),
  );

  for (const scenario of [
    "no prior records",
    "transformed prefix",
    "partially mapped prefix",
    "oversized summary",
    "summary at canonical capacity",
    "replacement after recovery",
  ] as const) {
    it.effect(`durable compaction validates persistence before continuing: ${scenario}`, () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const thread = `compaction-persistence-${scenario.replaceAll(" ", "-")}`;
        const summary =
          scenario === "oversized summary"
            ? "é".repeat(65_536) + "s"
            : scenario === "summary at canonical capacity"
              ? "é".repeat(65_536)
              : "custom summary";
        if (scenario !== "no prior records") {
          const prior = yield* makeScriptedModel(() => finalParts('{"answer":"ORIGINAL HISTORY"}'));
          const initial = Agent.withModel(plannerDefinition, prior.model);
          yield* runtime.submit(initial, { question: "seed" }, submitOptions(thread, "seed"));
          const settled = yield* runtime.processThread(initial, decodeThreadId(thread));
          expect(settled[0]?.outcome).toBe("completed");
        }
        const definition = Agent.make("persistence-compactor", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer from retained history.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 500,
            runStatus: "off",
          }),
        });
        const model = yield* makeScriptedModel(() => finalParts('"done"'));
        const agent = Agent.withModel(definition, model.model);
        const settled = yield* Effect.gen(function* () {
          const custom = yield* DurableAgentRuntime;
          const receipt = yield* custom.submit(agent, "continue", submitOptions(thread, "compact"));
          if (scenario === "replacement after recovery") {
            yield* armFailpoint("compaction:after-canonical-append");
            const crashed = yield* custom
              .processThread(agent, receipt.threadId)
              .pipe(Effect.ensuring(clearFailpoint), Effect.exit);
            expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
            expect(model.prompts).toHaveLength(0);
          }
          return yield* custom.processThread(agent, receipt.threadId);
        }).pipe(
          Effect.provide(
            Layer.fresh(DurableAgentRuntime.layerWithServices).pipe(
              Layer.provide(
                Layer.succeed(RunContextPreparation, {
                  compactor: ContextCompactor.of({
                    estimate: (messages) =>
                      (scenario === "replacement after recovery" &&
                        promptTexts(Prompt.fromMessages(messages)).includes(
                          COMPACTION_SUMMARY_PREFIX,
                        )) ||
                      messages.some((message) => message.role === "assistant")
                        ? 1_000
                        : 10,
                    compact: ({ source }) =>
                      Stream.succeed({
                        kind: "summarize",
                        through: scenario === "partially mapped prefix" ? 2 : 1,
                        summary:
                          scenario === "replacement after recovery" &&
                          promptTexts(source).includes(COMPACTION_SUMMARY_PREFIX)
                            ? "uncommitted replacement"
                            : summary,
                      }),
                  }),
                  ...(scenario === "no prior records" ||
                  scenario === "transformed prefix" ||
                  scenario === "partially mapped prefix"
                    ? {
                        hook: {
                          prepare: ({ source }) =>
                            Effect.succeed({
                              prompt: Prompt.fromMessages(
                                scenario === "partially mapped prefix"
                                  ? [
                                      ...source.content.slice(0, 1),
                                      Prompt.assistantMessage({
                                        content: [Prompt.textPart({ text: "UNPERSISTED" })],
                                      }),
                                      ...source.content.slice(1),
                                    ]
                                  : [
                                      Prompt.assistantMessage({
                                        content: [Prompt.textPart({ text: "UNPERSISTED" })],
                                      }),
                                      ...source.content,
                                    ],
                              ),
                            }),
                        },
                      }
                    : {}),
                }),
              ),
              Layer.provideMerge(baseLayer),
            ),
          ),
        );
        const records = yield* readLog(thread);
        const compactions = records.flatMap(({ record }) =>
          record.payload._tag === "CompactionCreated" ? [record.payload] : [],
        );
        const replay = yield* promptFromCanonicalRecords(records);
        if (scenario === "summary at canonical capacity") {
          expect(settled[0]?.outcome).toBe("completed");
          expect(model.prompts).toHaveLength(1);
          expect(compactions).toHaveLength(1);
          expect(compactions[0]?.summary).toBe(summary);
          expect(promptTexts(model.prompts[0] ?? Prompt.empty)).toContain(
            COMPACTION_SUMMARY_PREFIX + summary,
          );
          expect(promptTexts(replay)).toContain(COMPACTION_SUMMARY_PREFIX + summary);
        } else {
          expect(settled[0]?.outcome).toBe("failed");
          expect(records.at(-1)?.record.payload).toMatchObject({
            _tag: "SubmissionSettled",
            outcome: "failed",
            result: { errorTag: "CompactionError" },
          });
          expect(model.prompts).toHaveLength(0);
          if (scenario === "replacement after recovery") {
            expect(compactions).toHaveLength(1);
            expect(compactions[0]?.summary).toBe(summary);
            expect(promptTexts(replay)).toContain(COMPACTION_SUMMARY_PREFIX + summary);
            expect(promptTexts(replay)).not.toContain("uncommitted replacement");
          } else {
            expect(compactions).toEqual([]);
            expect(promptTexts(replay)).not.toContain(COMPACTION_SUMMARY_PREFIX);
            if (scenario !== "no prior records")
              expect(promptTexts(replay)).toContain("ORIGINAL HISTORY");
          }
        }
      }),
    );
  }

  it.effect(
    "invalid compaction summaries retain charged usage without becoming recovery state",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const thread = "invalid-compaction-summary";
        const first = yield* makeScriptedModel(() =>
          finalParts(JSON.stringify({ answer: "PADDING".repeat(1_000) })),
        );
        const initial = Agent.withModel(plannerDefinition, first.model);
        yield* runtime.submit(
          initial,
          { question: "prepare history" },
          submitOptions(thread, "initial"),
        );
        yield* runtime.processThread(initial, decodeThreadId(thread));
        const definition = Agent.make("invalid-summary", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 500,
            compaction: CompactionPolicy.make({ keepRecentTokens: 10, mode: "summarize" }),
          }),
        });
        const model = yield* makeScriptedModel(() => finalPartsWithUsage("   \n", usageOf(17, 9)));
        const agent = Agent.withModel(definition, model.model);
        const receipt = yield* runtime.submit(agent, "summarize", submitOptions(thread, "invalid"));
        const settlement = (yield* runtime.processThread(agent, receipt.threadId))[0];
        expect(settlement?.outcome).toBe("failed");
        expect(settlement?.usageSummary).toMatchObject({
          modelCalls: 1,
          inputTokens: { total: 17 },
          outputTokens: { total: 9 },
        });
        expect(model.prompts).toHaveLength(1);
        const records = yield* readLog(thread);
        expect(records.at(-1)?.record.payload).toMatchObject({
          _tag: "SubmissionSettled",
          outcome: "failed",
          result: { errorTag: "ModelProtocolError" },
        });
        expect(records.filter(({ record }) => record.payload._tag === "CompactionCreated")).toEqual(
          [],
        );
        const prompt = yield* promptFromCanonicalRecords(records);
        expect(promptTexts(prompt)).not.toContain(COMPACTION_SUMMARY_PREFIX);
        expect(promptTexts(prompt)).toContain("PADDING");
      }),
  );

  it.effect(
    "RUN-026: the durable cut never covers the threshold-crossing Turn (whole-Turn retention)",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const thread = "thread-compaction-cut";

        // Prior run: two tool Turns then a final answer. The second Turn's
        // settled result is huge, so a keepRecentTokens of 400 lands the
        // reverse-scan threshold ON that settled record (mid-Turn).
        const Probe = Tool.make("probe", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        });
        const probeTools = Toolkit.make(Probe);
        const probeDefinition = Agent.make("durable-probe", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Probe twice, then answer.",
          toolkit: probeTools,
          policy: AgentPolicy.make({
            maxTurns: 4,
            maxToolCalls: 3,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const probeCall = (id: string): ReadonlyArray<Response.StreamPartEncoded> => [
          { type: "tool-call", id, name: "probe", params: {}, providerExecuted: false },
          { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
        ];
        const calls = yield* Ref.make(0);
        const probeLayer = probeTools.toLayer({
          probe: () =>
            Ref.getAndUpdate(calls, (count) => count + 1).pipe(
              Effect.map((count) => (count === 0 ? "OLD".repeat(3_000) : "PAD".repeat(2_000))),
            ),
        });
        const prior = yield* makeScriptedModel((call) =>
          call === 0
            ? probeCall("probe-1")
            : call === 1
              ? probeCall("probe-2")
              : finalParts('{"answer":"probed"}'),
        );
        yield* runtime.submit(
          Agent.withModel(probeDefinition, prior.model),
          { question: "sizes?" },
          submitOptions(thread, "cut-1"),
        );
        const priorSettled = yield* runtime
          .processThread(Agent.withModel(probeDefinition, prior.model), decodeThreadId(thread))
          .pipe(Effect.provide(probeLayer));
        expect(priorSettled[0]?.outcome).toBe("completed");

        const compactingDefinition = Agent.make("durable-cut-compactor", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Answer from what is known.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 3_000,
            compaction: CompactionPolicy.make({ keepRecentTokens: 400, mode: "summarize" }),
          }),
        });
        const second = yield* makeScriptedModel((call) =>
          call === 0 ? finalParts("Goal: sizes probed") : finalParts('{"answer":"kept"}'),
        );
        const compactor = Agent.withModel(compactingDefinition, second.model);
        yield* runtime.submit(
          compactor,
          { question: "what happened?" },
          submitOptions(thread, "cut-2"),
        );
        const settled = yield* runtime.processThread(compactor, decodeThreadId(thread));
        expect(settled[0]?.outcome).toBe("completed");

        const records = yield* readLog(thread);
        const responses = records.filter(
          (envelope) => envelope.record.payload._tag === "ModelResponseRecorded",
        );
        const settleds = records.filter(
          (envelope) => envelope.record.payload._tag === "ToolCallSettled",
        );
        const compactions = records.filter(
          (envelope) => envelope.record.payload._tag === "CompactionCreated",
        );
        expect(responses.length).toBeGreaterThanOrEqual(3);
        expect(settleds).toHaveLength(2);
        expect(compactions).toHaveLength(1);
        const payload = compactions[0]?.record.payload;
        if (payload === undefined || payload._tag !== "CompactionCreated") {
          throw new Error("expected a CompactionCreated record");
        }
        // The threshold lands on the huge second settled (mid Turn 2), so the
        // covered prefix must end just BEFORE Turn 2's response: the whole
        // threshold-crossing Turn stays retained.
        expect(payload.coversThrough).toBe(settleds[0]?.sequence ?? -1);
        expect(payload.coversThrough).toBeLessThan(responses[1]?.sequence ?? -1);
      }),
  );

  it.effect("RUN-023: the compaction summarizer's usage joins the Turn's canonical record", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const thread = "thread-summarizer-usage";

      // Submission 1 leaves prior-Run records for the compactor to cover.
      const first = yield* makeScriptedModel((call) =>
        call === 0
          ? toolCallParts
          : finalParts(JSON.stringify({ answer: `Found the sea. ${"PAD".repeat(1_000)}` })),
      );
      yield* runtime.submit(
        Agent.withModel(searchDefinition, first.model),
        { question: "Is a flight available?" },
        submitOptions(thread, "summarizer-usage-1"),
      );
      const firstSettled = yield* runtime
        .processThread(Agent.withModel(searchDefinition, first.model), decodeThreadId(thread))
        .pipe(Effect.provide(searchToolLayer));
      expect(firstSettled[0]?.outcome).toBe("completed");

      const compactingDefinition = Agent.make("durable-usage-compactor", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer from what is known.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          contextTokenLimit: 500,
          compaction: CompactionPolicy.make({ keepRecentTokens: 10, mode: "summarize" }),
        }),
      });
      // Call 0 is the pre-Turn summarizer, call 1 the Turn's real response;
      // the Turn's canonical record must carry BOTH calls' usage so a later
      // Attempt re-seeds the summarizer's spend too.
      const second = yield* makeScriptedModel((call) =>
        call === 0
          ? finalPartsWithUsage("Goal: metered summary", usageOf(30, 10))
          : finalPartsWithUsage('{"answer":"metered"}', usageOf(20, 5)),
      );
      const compactor = Agent.withModel(compactingDefinition, second.model);
      yield* runtime.submit(
        compactor,
        { question: "what happened?" },
        submitOptions(thread, "summarizer-usage-2"),
      );
      const settled = yield* runtime.processThread(compactor, decodeThreadId(thread));
      expect(settled).toHaveLength(1);
      expect(settled[0]?.outcome).toBe("completed");

      const records = yield* readLog(thread);
      // The compactor's Turn is the newest response record in the log.
      const response = [...records]
        .reverse()
        .find((envelope) => envelope.record.payload._tag === "ModelResponseRecorded");
      const payload = response?.record.payload;
      if (payload === undefined || payload._tag !== "ModelResponseRecorded") {
        throw new Error("expected the compactor Turn's response record");
      }
      expect(payload.inputTokens).toBe(50);
      expect(payload.outputTokens).toBe(15);
    }),
  );

  it.effect("RUN-023: a resumed Attempt re-seeds committed usage into the token budget", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const thread = "thread-reseed";
      const reseedDefinition = Agent.make("durable-reseed", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Search before answering.",
        toolkit: searchTools,
        policy: AgentPolicy.make({
          maxTurns: 3,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
          tokenBudget: 1_000,
          onExhaustion: "fail",
        }),
      });
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolCallPartsWithUsage(usageOf(900, 50))
          : finalPartsWithUsage('{"answer":"cheap"}', usageOf(200, 50)),
      );
      const agent = Agent.withModel(reseedDefinition, scripted.model);
      yield* runtime.submit(agent, { question: "reseed?" }, submitOptions(thread, "reseed-1"));

      // Crash after the Turn-1 response commit (usage already staged into the
      // canonical record), leaving a declared pending Tool batch to resume.
      yield* armFailpoint("turn:after-response-append");
      const crashed = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(searchToolLayer)),
      );
      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const settled = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(searchToolLayer));
      // 950 committed tokens re-seed the resumed Attempt. The completion
      // reserve is already unavailable, so recovery fails before another
      // unconstrained model call; without re-seeding it would continue.
      expect(settled).toHaveLength(1);
      expect(settled[0]?.outcome).toBe("failed");

      const records = yield* readLog(thread);
      const response = records.find(
        (envelope) =>
          envelope.record.payload._tag === "ModelResponseRecorded" &&
          envelope.record.payload.turn === 1,
      );
      const payload = response?.record.payload;
      if (payload === undefined || payload._tag !== "ModelResponseRecorded") {
        throw new Error("expected the Turn 1 response record");
      }
      expect(payload.inputTokens).toBe(900);
      expect(payload.outputTokens).toBe(50);
    }),
  );
});

layer(testLayer)("RUN-030 canonical duration", (it) => {
  it.effect("refuses executed history whose original start evidence is missing", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const store = yield* ThreadStore;
      const scripted = yield* makeScriptedModel(() => toolCallParts);
      const agent = Agent.withModel(searchDefinition, scripted.model);
      const receipt = yield* runtime.submit(
        agent,
        { question: "missing clock" },
        submitOptions("missing-run-start", "start"),
      );
      yield* armFailpoint("turn:after-response-append");
      expect(
        failureTag(
          yield* Effect.exit(
            runtime.processThread(agent, receipt.threadId).pipe(Effect.provide(searchToolLayer)),
          ),
        ),
      ).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      const withoutStart = ThreadStore.of({
        ...store,
        read: (request) =>
          store
            .read(request)
            .pipe(Stream.filter(({ record }) => record.payload._tag !== "RunStarted")),
      });
      const recovered = yield* Effect.exit(
        Effect.flatMap(DurableAgentRuntime, (fresh) =>
          fresh.processThread(agent, receipt.threadId),
        ).pipe(
          Effect.provide(Layer.fresh(DurableAgentRuntime.layer)),
          Effect.provideService(ThreadStore, withoutStart),
          Effect.provide(searchToolLayer),
        ),
      );
      expect(failureTag(recovered)).toBe("RunJournalError");
      expect(scripted.prompts).toHaveLength(1);
    }),
  );

  it.effect("starts the clock once across both Run-start append failpoints", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      for (const location of ["run:before-start-append", "run:after-start-append"] as const) {
        const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"done"}'));
        const agent = Agent.withModel(plannerDefinition, scripted.model);
        const thread = `thread-${location}`;
        const receipt = yield* runtime.submit(
          agent,
          { question: "clock" },
          submitOptions(thread, location),
        );
        yield* armFailpoint(location);
        const crashed = yield* Effect.exit(runtime.processThread(agent, receipt.threadId));
        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        expect(scripted.prompts).toHaveLength(0);
        yield* TestClock.adjust(Duration.seconds(31));
        const settlements = yield* runtime.processThread(agent, receipt.threadId);
        const alreadyStarted = location === "run:after-start-append";
        expect(settlements.map((entry) => entry.outcome)).toEqual([
          alreadyStarted ? "failed" : "completed",
        ]);
        expect(scripted.prompts).toHaveLength(alreadyStarted ? 0 : 1);
        const records = yield* readLog(thread);
        const starts = records.filter(({ record }) => record.payload._tag === "RunStarted");
        expect(starts).toHaveLength(1);
        expect(starts[0]?.record.payload).toMatchObject({
          runId: runIdForSubmission(receipt.submissionId),
          maxDurationMillis: 30_000,
        });
        expect(yield* runtime.processThread(agent, receipt.threadId)).toEqual([]);
      }
    }),
  );

  it.effect(
    "rejects a changed duration after process loss without moving the canonical deadline",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scripted = yield* makeScriptedModel((call) =>
          call === 0 ? toolCallParts : finalParts('{"answer":"done"}'),
        );
        const agent = Agent.withModel(searchDefinition, scripted.model);
        const thread = "thread-replacement-duration";
        const receipt = yield* runtime.submit(
          agent,
          { question: "recover" },
          submitOptions(thread, "duration"),
        );
        yield* armFailpoint("turn:after-response-append");
        const crashed = yield* Effect.exit(
          runtime.processThread(agent, receipt.threadId).pipe(Effect.provide(searchToolLayer)),
        );
        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        const before = yield* readLog(thread);
        const start = before.find(({ record }) => record.payload._tag === "RunStarted");
        yield* TestClock.adjust(Duration.seconds(31));
        const wider = Agent.withModel(
          Agent.make(searchDefinition.id, {
            input: searchDefinition.input,
            output: searchDefinition.output,
            instructions: searchDefinition.instructions,
            toolkit: searchDefinition.toolkit,
            policy: AgentPolicy.make({ ...searchDefinition.policy, maxDuration: "5 minutes" }),
          }),
          scripted.model,
        );
        const rejected = yield* Effect.exit(
          runtime.processThread(wider, receipt.threadId).pipe(Effect.provide(searchToolLayer)),
        );
        expect(failureTag(rejected)).toBe("RunJournalError");
        expect(scripted.prompts).toHaveLength(1);
        const after = yield* readLog(thread);
        expect(after.find(({ record }) => record.payload._tag === "RunStarted")).toEqual(start);
        const settlements = yield* runtime
          .processThread(agent, receipt.threadId)
          .pipe(Effect.provide(searchToolLayer));
        expect(settlements.map((entry) => entry.outcome)).toEqual(["failed"]);
        expect(scripted.prompts).toHaveLength(1);
        const settlement = (yield* readLog(thread)).at(-1)?.record.payload;
        expect(settlement).toMatchObject({ _tag: "SubmissionSettled", policyLimit: "duration" });
      }),
  );

  it.effect(
    "binding-free input recovery does not start a Run or spend its duration allowance",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"fresh"}'));
        const agent = Agent.withModel(plannerDefinition, scripted.model);
        const thread = "thread-input-before-start";
        const receipt = yield* runtime.submit(
          agent,
          { question: "queued" },
          submitOptions(thread, "queued"),
        );
        yield* armFailpoint("claim:after-claim");
        yield* Effect.exit(runtime.processThread(agent, receipt.threadId));
        yield* clearFailpoint;
        yield* runtime.runRecovery;
        expect(logTags(yield* readLog(thread))).not.toContain("RunStarted");
        yield* TestClock.adjust(Duration.minutes(5));
        expect(
          (yield* runtime.processThread(agent, receipt.threadId)).map((entry) => entry.outcome),
        ).toEqual(["completed"]);
        expect(scripted.prompts).toHaveLength(1);
      }),
  );
});

layer(testLayer)("RUN-011 durable typed budget settlement", (it) => {
  const usageOf = (input: number, output: number) => ({
    inputTokens: { total: input },
    outputTokens: { total: output },
  });

  const finalPartsWithUsage = (
    text: string,
    used: ReturnType<typeof usageOf>,
  ): ReadonlyArray<Response.StreamPartEncoded> => [
    { type: "text-start", id: "answer" },
    { type: "text-delta", id: "answer", delta: text },
    { type: "text-end", id: "answer" },
    { type: "finish", reason: "stop", usage: used },
  ];

  const lastSettlement = (records: ReadonlyArray<CanonicalRecordEnvelope>) => {
    const payload = records.at(-1)?.record.payload;
    if (payload?._tag !== "SubmissionSettled") {
      throw new Error("expected the canonical settlement to close the log");
    }
    return payload;
  };

  const policyOf = (overrides?: Partial<Parameters<typeof AgentPolicy.make>[0]>) =>
    AgentPolicy.make({
      maxTurns: 3,
      maxToolCalls: 2,
      maxDuration: "30 seconds",
      toolConcurrency: 1,
      ...overrides,
    });

  it.effect('RUN-011: a Turn-exhausted soft landing settles with exhausted: "turns"', () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const definition = Agent.make("durable-turns-landing", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Search before answering.",
        toolkit: searchTools,
        policy: policyOf({ maxTurns: 1 }),
      });
      // Turn 1 declares the permitted batch; the grace Turn past `maxTurns`
      // delivers the constrained final answer (RUN-019).
      const scripted = yield* makeScriptedModel((call) =>
        call === 0 ? toolCallParts : finalParts('{"answer":"turn-bound partial"}'),
      );
      const agent = Agent.withModel(definition, scripted.model);
      const thread = "thread-turns-landing";

      yield* runtime.submit(agent, { question: "turns?" }, submitOptions(thread, "turns-1"));
      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(searchToolLayer));
      expect(settlements[0]?.outcome).toBe("completed");

      const settled = lastSettlement(yield* readLog(thread));
      expect(settled.outcome).toBe("completed");
      expect(settled.finishReason).toBe("budget-exhausted");
      expect(settled.exhausted).toBe("turns");
      expect(settled.policyLimit).toBeUndefined();
      expect(settled.result).toEqual({ answer: "turn-bound partial" });
    }),
  );

  it.effect('RUN-011: a token-exhausted soft landing settles with exhausted: "tokens"', () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const definition = Agent.make("durable-tokens-landing", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer immediately.",
        toolkit: Toolkit.empty,
        policy: policyOf({ tokenBudget: 100 }),
      });
      // A token-breaching stop response that already decodes settles directly
      // (RUN-025): 90 + 20 tokens against the 100-token budget.
      const scripted = yield* makeScriptedModel(() =>
        finalPartsWithUsage('{"answer":"token-bound partial"}', usageOf(90, 20)),
      );
      const agent = Agent.withModel(definition, scripted.model);
      const thread = "thread-tokens-landing";

      yield* runtime.submit(agent, { question: "tokens?" }, submitOptions(thread, "tokens-1"));
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements[0]?.outcome).toBe("completed");

      const settled = lastSettlement(yield* readLog(thread));
      expect(settled.outcome).toBe("completed");
      expect(settled.finishReason).toBe("budget-exhausted");
      expect(settled.exhausted).toBe("tokens");
      expect(settled.policyLimit).toBeUndefined();
      expect(settled.result).toEqual({ answer: "token-bound partial" });
    }),
  );

  it.effect("RUN-011: a fail-mode token breach settles failed with the typed policyLimit", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const definition = Agent.make("durable-tokens-rail", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer immediately.",
        toolkit: Toolkit.empty,
        policy: policyOf({ tokenBudget: 100, onExhaustion: "fail" }),
      });
      const scripted = yield* makeScriptedModel(() =>
        finalPartsWithUsage('{"answer":"expensive"}', usageOf(90, 20)),
      );
      const agent = Agent.withModel(definition, scripted.model);
      const thread = "thread-tokens-rail";

      yield* runtime.submit(agent, { question: "tokens?" }, submitOptions(thread, "tokens-rail-1"));
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements[0]?.outcome).toBe("failed");

      const settled = lastSettlement(yield* readLog(thread));
      expect(settled.outcome).toBe("failed");
      expect(settled.policyLimit).toBe("tokens");
      expect(settled.finishReason).toBeUndefined();
      expect(settled.exhausted).toBeUndefined();
      expect(settled.result).toMatchObject({ errorTag: "AgentPolicyError" });
    }),
  );

  it.effect("RUN-011: a hard cost rail settles failed with the typed policyLimit", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const definition = Agent.make("durable-cost-rail", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer immediately.",
        toolkit: Toolkit.empty,
        // The durable runtime provides no cost estimator, so a configured cost
        // budget fails the Run typed on the first usage consumption — the hard
        // cost rail regardless of `onExhaustion` (runtime spec §3).
        policy: policyOf({ costBudgetMicrousd: 1_000 }),
      });
      const scripted = yield* makeScriptedModel(() =>
        finalPartsWithUsage('{"answer":"priced"}', usageOf(10, 10)),
      );
      const agent = Agent.withModel(definition, scripted.model);
      const thread = "thread-cost-rail";

      yield* runtime.submit(agent, { question: "cost?" }, submitOptions(thread, "cost-1"));
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements[0]?.outcome).toBe("failed");

      const settled = lastSettlement(yield* readLog(thread));
      expect(settled.outcome).toBe("failed");
      expect(settled.policyLimit).toBe("cost");
      expect(settled.result).toMatchObject({ errorTag: "AgentPolicyError" });
    }),
  );

  it.effect("RUN-011: a duration-exhausted Run settles failed with the typed policyLimit", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const definition = Agent.make("durable-duration-rail", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer immediately.",
        toolkit: Toolkit.empty,
        policy: policyOf({ maxDuration: "5 seconds" }),
      });
      // A model that never finishes streaming: only the duration rail can end
      // this Run.
      const hangingModel = Model.make(
        "scripted",
        "durable-test",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => Stream.never,
          }),
        ),
      );
      const agent = Agent.withModel(definition, hangingModel);
      const thread = "thread-duration-rail";

      yield* runtime.submit(agent, { question: "slow?" }, submitOptions(thread, "duration-1"));
      const worker = yield* Effect.forkChild(runtime.processThread(agent, decodeThreadId(thread)));
      yield* TestClock.adjust(Duration.seconds(6));
      const settlements = yield* Fiber.join(worker);
      expect(settlements[0]?.outcome).toBe("failed");

      const settled = lastSettlement(yield* readLog(thread));
      expect(settled.outcome).toBe("failed");
      expect(settled.policyLimit).toBe("duration");
      expect(settled.result).toMatchObject({ errorTag: "AgentPolicyError" });
    }),
  );

  it.effect(
    "RUN-011: recovery preserves the typed policyLimit across both terminalize failpoints",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scenarios = [
          {
            location: "terminalize:after-reserve" as const,
            thread: "thread-policy-reserve",
            key: "policy-reserve-1",
          },
          {
            location: "terminalize:after-canonical-append" as const,
            thread: "thread-policy-append",
            key: "policy-append-1",
          },
        ];
        for (const scenario of scenarios) {
          const definition = Agent.make("durable-policy-recovery", {
            input: Schema.Struct({ question: Schema.String }),
            output: Schema.Struct({ answer: Schema.String }),
            instructions: "Answer immediately.",
            toolkit: Toolkit.empty,
            policy: policyOf({ tokenBudget: 100, onExhaustion: "fail" }),
          });
          const scripted = yield* makeScriptedModel(() =>
            finalPartsWithUsage('{"answer":"expensive"}', usageOf(90, 20)),
          );
          const agent = Agent.withModel(definition, scripted.model);

          const receipt = yield* runtime.submit(
            agent,
            { question: "tokens?" },
            submitOptions(scenario.thread, scenario.key),
          );
          yield* armFailpoint(scenario.location);
          const killed = yield* Effect.exit(
            runtime.processThread(agent, decodeThreadId(scenario.thread)),
          );
          expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          yield* runtime.runRecovery;
          const settlement = yield* runtime.awaitSettlement(receipt);
          expect(settlement.outcome).toBe("failed");
          const records = yield* readLog(scenario.thread);
          const settledRecords = records
            .map((envelope) => envelope.record.payload)
            .filter((payload) => payload._tag === "SubmissionSettled");
          expect(settledRecords).toHaveLength(1);
          expect(settledRecords[0]).toMatchObject({
            outcome: "failed",
            policyLimit: "tokens",
            result: { errorTag: "AgentPolicyError" },
          });
        }
      }),
  );
});

layer(pricedTestLayer)("RUN-035 durable cost accounting", (it) => {
  it.effect(
    "persists cache splits and pricing identity, enforces cost, and settles with usage",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const definition = Agent.make("durable-priced-usage", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Answer immediately.",
          toolkit: Toolkit.empty,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            costBudgetMicrousd: 1_000,
          }),
        });
        const meteredUsage = {
          inputTokens: { total: 10, uncached: 7, cacheRead: 2, cacheWrite: 1 },
          outputTokens: { total: 10, text: 6, reasoning: 4 },
        };
        const scripted = yield* makeScriptedModel(() => [
          { type: "text-start", id: "answer" },
          { type: "text-delta", id: "answer", delta: '{"answer":"priced"}' },
          { type: "text-end", id: "answer" },
          { type: "finish", reason: "stop", usage: meteredUsage },
        ]);
        const agent = Agent.withModel(definition, scripted.model);
        const thread = "thread-priced-usage";

        yield* runtime.submit(
          agent,
          { question: "cost?" },
          submitOptions(thread, "priced-usage-1"),
        );
        const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
        const settlement = settlements[0];
        expect(settlement?.outcome).toBe("failed");
        expect(settlement?.usageSummary).toMatchObject({
          modelCalls: 1,
          inputTokens: meteredUsage.inputTokens,
          outputTokens: meteredUsage.outputTokens,
          costMicrousd: 1_200,
          byModel: [
            {
              provider: "scripted",
              model: "durable-test",
              serviceTier: "priority",
              pricingVersion: "prices-2026-08-24",
              modelCalls: 1,
            },
          ],
        });

        const payloads = (yield* readLog(thread)).map((envelope) => envelope.record.payload);
        const response = payloads.find((payload) => payload._tag === "ModelResponseRecorded");
        expect(response?.modelUsage).toEqual([
          {
            provider: "scripted",
            model: "durable-test",
            serviceTier: "priority",
            pricingVersion: "prices-2026-08-24",
            inputTokens: meteredUsage.inputTokens,
            outputTokens: meteredUsage.outputTokens,
            costMicrousd: 1_200,
          },
        ]);
        const settled = payloads.at(-1);
        expect(settled).toMatchObject({
          _tag: "SubmissionSettled",
          outcome: "failed",
          policyLimit: "cost",
          usageSummary: settlement?.usageSummary,
        });
      }),
  );
});

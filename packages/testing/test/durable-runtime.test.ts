import type { IdGenerator } from "@effect-agent/core";
import {
  Agent,
  AgentPolicy,
  CompactionPolicy,
  ConversationId,
  ReceiptId,
  SubmissionId,
  ToolCallId,
  type IdGenerator,
} from "@effect-agent/core";
import { COMPACTION_SUMMARY_PREFIX, ToolExecutionClass } from "@effect-agent/engine";
import type { AdmissionConflict, FenceRejected, SettlementConflict } from "@effect-agent/session";
import {
  AbortCommand,
  ApprovalDecisionCommand,
  BatchId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ConversationRead,
  ConversationStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointTestControl,
  IdempotencyKey,
  ObservationOffset,
  Principal,
  ProducerId,
  QueueSequence,
  Receipt,
  RecoverySnapshotRequest,
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
  replayConversation,
  recoveryRepairRecordId,
  runIdForSubmission,
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
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
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
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);

const submitOptions = (conversationId: string, idempotencyKey: string): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversationId),
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

const plannerDefinition = Agent.define("durable-planner", {
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
const dispositionDefinition = Agent.define("durable-run-disposition", {
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
const searchDefinition = Agent.define("durable-search", {
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
const progressApprovalDefinition = Agent.define("durable-progress-approval", {
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
  MemorySubmissionLedgerLive,
  MemoryConversationStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layerTest,
  ToolReconciler.uncertain,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layer.pipe(Layer.provideMerge(baseLayer));

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
      MemoryConversationStoreLive,
      WakeScheduler.layerNoop,
      DurableRuntimeFailpoint.layerTest,
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
  MemoryConversationStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layerTest,
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
    readonly reads: Ref.Ref<ReadonlyMap<ConversationId, number>>;
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
    const reads = yield* Ref.make<ReadonlyMap<ConversationId, number>>(new Map());
    const subscribeGate = yield* Ref.make<Option.Option<Deferred.Deferred<void>>>(Option.none());
    const parkGate = yield* Ref.make<Option.Option<Deferred.Deferred<void>>>(Option.none());

    const scheduler = WakeScheduler.of({
      notify: hub.notify,
      wakes: Stream.never,
      subscribe: (conversationId) =>
        Effect.gen(function* () {
          const wait = yield* hub.subscribe(conversationId);
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
  ConversationStore,
  Effect.gen(function* () {
    const inner = yield* ConversationStore;
    const control = yield* ProgressWaitTestControl;
    return ConversationStore.of({
      ...inner,
      read: (request) =>
        Stream.unwrap(
          Ref.update(control.reads, (current) => {
            const next = new Map(current);
            next.set(request.conversationId, (current.get(request.conversationId) ?? 0) + 1);
            return next;
          }).pipe(Effect.as(inner.read(request))),
        ),
    });
  }),
).pipe(Layer.provide(MemoryConversationStoreLive));

const progressWaitAdapters = Layer.merge(progressWaitSchedulerLayer, progressWaitStoreLayer).pipe(
  Layer.provideMerge(progressWaitControlLayer),
);

const progressWaitBaseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  progressWaitAdapters,
  DurableRuntimeFailpoint.layerTest,
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

const readCount = (control: ProgressWaitTestControl["Service"], conversationId: ConversationId) =>
  Ref.get(control.reads).pipe(Effect.map((counts) => counts.get(conversationId) ?? 0));

const readLog = (conversationId: string) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(
        ConversationRead.make({
          conversationId: decodeConversationId(conversationId),
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
      const conversationId = decodeConversationId("conversation-progress-settlement");
      yield* runtime.submit(
        agent,
        { question: "wake on settlement" },
        submitOptions(conversationId, "progress-settlement-1"),
      );

      const reserved = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* failpoints.setHandler((location) =>
        location === "terminalize:after-reserve"
          ? Deferred.succeed(reserved, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void,
      );
      yield* Effect.gen(function* () {
        const processing = yield* Effect.forkChild(
          runtime.processConversation(agent, conversationId),
        );
        yield* Deferred.await(reserved);

        const beforeSettlement = yield* readLog(conversationId);
        expect(logTags(beforeSettlement)).not.toContain("SubmissionSettled");
        const cursor = beforeSettlement.at(-1)?.sequence;
        expect(cursor).toBeDefined();
        if (cursor === undefined) return;

        const waiting = yield* Effect.forkChild(runtime.awaitProgress(conversationId, cursor));
        yield* waitForAtLeast(control.active, 1);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(waiting);
        yield* Fiber.join(processing);

        const afterSettlement = yield* readLog(conversationId);
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
      const conversationId = decodeConversationId("conversation-progress-approval");
      const receipt = yield* runtime.submit(
        agent,
        { question: "wake at both approval boundaries" },
        submitOptions(conversationId, "progress-approval-1"),
      );
      const submitted = yield* readLog(conversationId);
      const submittedCursor = submitted.at(-1)?.sequence;
      expect(submittedCursor).toBeDefined();
      if (submittedCursor === undefined) return;

      const awaitingRequest = yield* Effect.forkChild(
        runtime.awaitProgress(conversationId, submittedCursor),
      );
      yield* waitForAtLeast(control.active, 1);
      yield* runtime
        .processConversation(agent, conversationId)
        .pipe(Effect.provide(progressApprovalToolLayer));
      yield* Fiber.join(awaitingRequest);
      const requested = yield* readLog(conversationId);
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
            const records = yield* readLog(conversationId);
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
            yield* runtime.awaitProgress(conversationId, cursor);
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
      yield* runtime
        .processConversation(agent, conversationId)
        .pipe(Effect.provide(progressApprovalToolLayer));
      yield* Fiber.join(awaitingDecision);
      const decided = yield* readLog(conversationId);
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
      const conversationId = decodeConversationId("conversation-progress-races");
      const receipt = yield* runtime.submit(
        agent,
        { question: "race the wait" },
        submitOptions(conversationId, "progress-races-1"),
      );
      const initial = yield* readLog(conversationId);
      const initialCursor = initial.at(-1)?.sequence;
      expect(initialCursor).toBeDefined();
      if (initialCursor === undefined) return;

      // Force append+notify after registration but before the authoritative check.
      const beforeCheck = yield* Deferred.make<void>();
      yield* Ref.set(control.subscribeGate, Option.some(beforeCheck));
      const subscribedRace = yield* Effect.forkChild(
        runtime.awaitProgress(conversationId, initialCursor),
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

      const afterAbort = yield* readLog(conversationId);
      const abortCursor = afterAbort.at(-1)?.sequence;
      expect(abortCursor).toBeDefined();
      if (abortCursor === undefined) return;

      // Force a hint after the empty canonical check but before the returned wait Effect parks.
      yield* Ref.set(control.subscribeGate, Option.none());
      const beforePark = yield* Deferred.make<void>();
      yield* Ref.set(control.parkGate, Option.some(beforePark));
      const readsBeforeParkRace = yield* readCount(control, conversationId);
      const parksBeforeParkRace = yield* Ref.get(control.parking);
      const parkedRace = yield* Effect.forkChild(
        runtime.awaitProgress(conversationId, abortCursor),
      );
      yield* waitForAtLeast(control.parking, parksBeforeParkRace + 1);
      yield* scheduler.notify(conversationId);
      yield* Deferred.succeed(beforePark, undefined);
      yield* Fiber.join(parkedRace);
      expect(yield* readCount(control, conversationId)).toBe(readsBeforeParkRace + 1);
      expect(yield* Ref.get(control.active)).toBe(0);

      // The second wake was deliberately a false positive: storage, not the hint, is truth.
      const final = yield* readLog(conversationId);
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
        const conversationId = decodeConversationId("conversation-progress-many");
        const unrelatedId = decodeConversationId("conversation-progress-unrelated");

        yield* runtime.submit(
          agent,
          { question: "many waiters" },
          submitOptions(conversationId, "progress-many-1"),
        );
        yield* runtime.submit(
          agent,
          { question: "unrelated waiter" },
          submitOptions(unrelatedId, "progress-unrelated-1"),
        );
        const records = yield* readLog(conversationId);
        const unrelatedRecords = yield* readLog(unrelatedId);
        const cursor = records.at(-1)?.sequence;
        const unrelatedCursor = unrelatedRecords.at(-1)?.sequence;
        expect(cursor).toBeDefined();
        expect(unrelatedCursor).toBeDefined();
        if (cursor === undefined || unrelatedCursor === undefined) return;

        const readsBefore = yield* readCount(control, conversationId);
        const unrelatedReadsBefore = yield* readCount(control, unrelatedId);
        const first = yield* Effect.forkChild(runtime.awaitProgress(conversationId, cursor));
        const second = yield* Effect.forkChild(runtime.awaitProgress(conversationId, cursor));
        const unrelated = yield* Effect.forkChild(
          runtime.awaitProgress(unrelatedId, unrelatedCursor),
        );
        yield* waitForAtLeast(control.active, 3);
        yield* waitForAtLeast(control.parking, 3);
        expect(yield* readCount(control, conversationId)).toBe(readsBefore + 2);
        expect(yield* readCount(control, unrelatedId)).toBe(unrelatedReadsBefore + 1);

        yield* TestClock.adjust(Duration.seconds(10));
        expect(yield* readCount(control, conversationId)).toBe(readsBefore + 2);
        expect(yield* readCount(control, unrelatedId)).toBe(unrelatedReadsBefore + 1);

        yield* scheduler.notify(conversationId);
        yield* Fiber.join(first);
        yield* Fiber.join(second);
        expect(unrelated.pollUnsafe()).toBeUndefined();
        expect(yield* Ref.get(control.active)).toBe(1);

        yield* Fiber.interrupt(unrelated);
        expect(yield* Ref.get(control.active)).toBe(0);

        const timed = yield* Effect.forkChild(
          runtime
            .awaitProgress(conversationId, cursor)
            .pipe(Effect.timeoutOption(Duration.seconds(3))),
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
      const missing = decodeConversationId("conversation-progress-missing");
      const exit = yield* Effect.exit(runtime.awaitProgress(missing, ZERO_SEQUENCE));
      expect(failureTag(exit)).toBe("ConversationNotMaterialized");
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
      const conversation = "conversation-unverified-ledger-disposition";
      const receipt = yield* runtime.submit(
        agent,
        { question: "done?" },
        submitOptions(conversation, "unverified-ledger-disposition-1"),
      );

      const processed = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(processed[0]?.outcome).toBe("failed");
      expect(processed[0]?.runDisposition).toBeUndefined();

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("failed");
      expect(settlement.failure).toBeDefined();
      expect(settlement.runDisposition).toBeUndefined();

      const settled = (yield* readLog(conversation)).at(-1)?.record.payload;
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
        readonly conversation: string;
        readonly tag: "AgentApprovalPending" | "AgentChildPending";
        readonly fields: Readonly<Record<string, unknown>>;
      }> = [
        {
          conversation: "conversation-forged-approval-pending",
          tag: "AgentApprovalPending",
          fields: {
            approvalId: "forged-approval",
            toolCallId: "forged-tool-call",
            toolName: "forged-tool",
            message: "forged approval suspension",
          },
        },
        {
          conversation: "conversation-forged-child-pending",
          tag: "AgentChildPending",
          fields: {
            children: [
              {
                toolCallId: "forged-child-call",
                childConversationId: "forged-child-conversation",
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
          submitOptions(testCase.conversation, `${testCase.conversation}-key`),
        );
        const settlements = yield* runtime.processConversation(
          agent,
          decodeConversationId(testCase.conversation),
        );

        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("failed");
        expect(yield* lookupState(receipt.submissionId)).toBe("settled");
        expect((yield* readLog(testCase.conversation)).at(-1)?.record.payload._tag).toBe(
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
      const conversationId = decodeConversationId("conversation-progress-committed");

      yield* runtime.submit(
        agent,
        { question: "already committed?" },
        submitOptions(conversationId, "progress-committed-1"),
      );

      yield* runtime.awaitProgress(conversationId, ZERO_SEQUENCE);
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
        submitOptions("conversation-submit", "submit-1"),
      );

      expect(receipt.conversationId).toBe("conversation-submit");
      expect(receipt.queueSequence).toBe(1);
      expect(yield* lookupState(receipt.submissionId)).toBe("ready");
      const records = yield* readLog("conversation-submit");
      expect(logTags(records)).toEqual(["ConversationCreated"]);
      expect(records[0]?.record.recordId).toBe("conversation-created:conversation-submit");
    }),
  );

  it.effect("same-key resubmission returns the original Receipt; different content conflicts", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const options = submitOptions("conversation-idempotent", "submit-1");

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
      const conversation = "conversation-full-run";

      const receipt = yield* runtime.submit(
        agent,
        { question: "Is a flight available?" },
        submitOptions(conversation, "run-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(searchToolLayer));

      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(settlements[0]?.receiptId).toBe(receipt.receiptId);

      const submissionId = receipt.submissionId;
      const runId = runIdForSubmission(submissionId);
      const records = yield* readLog(conversation);
      expect(logTags(records)).toEqual([
        "ConversationCreated",
        "UserInputRecorded",
        "ModelResponseRecorded",
        "ToolCallSettled",
        "ModelResponseRecorded",
        "SubmissionSettled",
      ]);
      expect(records.map((envelope) => envelope.record.recordId)).toEqual([
        `conversation-created:${conversation}`,
        submissionInputRecordId(submissionId),
        modelResponseRecordId(runId, 1),
        `tool-settled:${runId}:1:search-1`,
        modelResponseRecordId(runId, 2),
        submissionSettlementRecordId(submissionId),
      ]);

      const settled = records.at(-1)?.record.payload;
      expect(settled?._tag).toBe("SubmissionSettled");
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.outcome).toBe("completed");
        expect(settled.runId).toBe(runId);
        expect(settled.result).toEqual({ answer: "Found." });
      }

      // A later Run sees conversational history without replaying this Run's
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
            conversation: "conversation-run-disposition",
            key: "run-disposition-1",
          },
          {
            location: "terminalize:after-reserve" as const,
            conversation: "conversation-run-disposition-reserve",
            key: "run-disposition-reserve-1",
          },
          {
            location: "terminalize:after-canonical-append" as const,
            conversation: "conversation-run-disposition-append",
            key: "run-disposition-append-1",
          },
        ];

        for (const scenario of scenarios) {
          const scripted = yield* makeScriptedModel(() =>
            finalParts('{"answer":"done","runDisposition":"application-complete"}'),
          );
          const agent = Agent.withModel(dispositionDefinition, scripted.model);
          const conversationId = decodeConversationId(scenario.conversation);
          const receipt = yield* runtime.submit(
            agent,
            { question: "done?" },
            submitOptions(scenario.conversation, scenario.key),
          );

          if (scenario.location === undefined) {
            yield* runtime.processConversation(agent, conversationId);
          } else {
            yield* armFailpoint(scenario.location);
            const killed = yield* Effect.exit(runtime.processConversation(agent, conversationId));
            expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
            yield* clearFailpoint;
            yield* runtime.runRecovery;
          }

          const settlement = yield* runtime.awaitSettlement(receipt);
          expect(settlement.outcome).toBe("completed");
          expect(settlement.runDisposition).toBe("application-complete");
          const records = yield* readLog(scenario.conversation);
          const projection = replayConversation(conversationId, records);
          expect(projection.settlements).toHaveLength(1);
          const disposition = projection.settlements[0]?.runDisposition;
          const decodedDisposition = yield* Schema.decodeUnknownEffect(RunDisposition)(disposition);
          expect(decodedDisposition).toBe("application-complete");
        }
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
        const definition = Agent.define("durable-terminal-delivery", {
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
        const conversation = "conversation-terminal-delivery-recovery";

        yield* runtime.submit(
          agent,
          { question: "deliver?" },
          submitOptions(conversation, "terminal-delivery-1"),
        );
        yield* armFailpoint("turn:after-results-append");
        const crashed = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(toolLayer)),
        );
        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(handlerCalls)).toBe(1);
        expect(scripted.prompts).toHaveLength(1);
        yield* clearFailpoint;

        const settled = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(toolLayer));
        expect(settled).toHaveLength(1);
        expect(settled[0]?.outcome).toBe("completed");
        expect(yield* Ref.get(handlerCalls)).toBe(1);
        expect(scripted.prompts).toHaveLength(1);
        const payloads = (yield* readLog(conversation)).map((envelope) => envelope.record.payload);
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
    "RUN-032 resumes an over-token completion delivery under fail mode after the response commit",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const PostMessage = Tool.make("post_message", {
          parameters: Schema.Struct({ message: Schema.String }),
          success: Schema.Struct({ messageId: Schema.String }),
        });
        const tools = Toolkit.make(PostMessage);
        const definition = Agent.define("durable-over-token-delivery", {
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
            onExhaustion: "fail",
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
        const conversation = "conversation-over-token-terminal-delivery";

        yield* runtime.submit(
          agent,
          { question: "deliver?" },
          submitOptions(conversation, "over-token-terminal-delivery-1"),
        );
        yield* armFailpoint("turn:after-response-append");
        const crashed = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(toolLayer)),
        );
        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(handlerCalls)).toBe(0);
        expect(scripted.prompts).toHaveLength(1);
        yield* clearFailpoint;

        const settled = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(toolLayer));
        expect(settled).toHaveLength(1);
        expect(settled[0]?.outcome).toBe("completed");
        expect(yield* Ref.get(handlerCalls)).toBe(1);
        expect(scripted.prompts).toHaveLength(1);
        const payloads = (yield* readLog(conversation)).map((envelope) => envelope.record.payload);
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
      const conversation = "conversation-run-disposition-invalid";

      const receipt = yield* runtime.submit(
        agent,
        { question: "done?" },
        submitOptions(conversation, "run-disposition-invalid-1"),
      );
      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );

      expect(settlements[0]?.outcome).toBe("failed");
      expect(settlements[0]?.runDisposition).toBeUndefined();
      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("failed");
      expect(settlement.runDisposition).toBeUndefined();
      const settled = (yield* readLog(conversation)).at(-1)?.record.payload;
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
        const definition = Agent.define("durable-soft-landing", {
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
        const conversation = "conversation-soft-landing";

        const receipt = yield* runtime.submit(
          agent,
          { question: "Everything about the package?" },
          submitOptions(conversation, "soft-landing-1"),
        );
        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
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
        const records = yield* readLog(conversation);
        // The rejected Turn commits as ONE single-batch canonical Turn — no
        // `ToolCallPrepared`, no split response batch — and the Submission
        // settles completed with the honest durable finishReason (RUN-011).
        expect(logTags(records)).toEqual([
          "ConversationCreated",
          "UserInputRecorded",
          "ModelResponseRecorded",
          "ToolCallSettled",
          "ToolCallSettled",
          "ModelResponseRecorded",
          "SubmissionSettled",
        ]);
        // Exact synthetic settlements: identities in declaration order, each
        // carrying the encoded policy failure — replay correlation for the
        // model-declared calls, not just an isFailure bit.
        expect(records.map((envelope) => envelope.record.recordId).slice(2, 5)).toEqual([
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

        // A later Run sees the rejected batch as conversational history but
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
            conversation: "conversation-soft-landing-reserve",
            key: "soft-landing-reserve-1",
          },
          {
            location: "terminalize:after-canonical-append" as const,
            conversation: "conversation-soft-landing-append",
            key: "soft-landing-append-1",
          },
        ];
        for (const scenario of scenarios) {
          const Probe = Tool.make("probe", {
            parameters: Schema.Struct({ query: Schema.String }),
            success: Schema.Struct({ available: Schema.Boolean }),
          });
          const probeTools = Toolkit.make(Probe);
          const definition = Agent.define("durable-soft-landing-recovery", {
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
            submitOptions(scenario.conversation, scenario.key),
          );
          yield* armFailpoint(scenario.location);
          const killed = yield* Effect.exit(
            runtime
              .processConversation(agent, decodeConversationId(scenario.conversation))
              .pipe(Effect.provide(probeToolLayer)),
          );
          expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          yield* runtime.runRecovery;
          const settlement = yield* runtime.awaitSettlement(receipt);
          expect(settlement.outcome).toBe("completed");
          expect(settlement.runDisposition).toBeUndefined();
          const records = yield* readLog(scenario.conversation);
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
      const conversation = "conversation-await";

      const receipt = yield* runtime.submit(
        agent,
        { question: "when?" },
        submitOptions(conversation, "await-1"),
      );
      const waiter = yield* Effect.forkChild(runtime.awaitSettlement(receipt));
      yield* runtime.processConversation(agent, decodeConversationId(conversation));
      yield* TestClock.adjust(Duration.millis(150));
      const settlement = yield* Fiber.join(waiter);
      expect(settlement.outcome).toBe("completed");
      expect(settlement.receiptId).toBe(receipt.receiptId);
    }),
  );

  it.effect("keeps one Conversation lane FIFO: contiguous queued work joins the active Run", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"next"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-fifo";

      const first = yield* runtime.submit(
        agent,
        { question: "first" },
        submitOptions(conversation, "fifo-1"),
      );
      const second = yield* runtime.submit(
        agent,
        { question: "second" },
        submitOptions(conversation, "fifo-2"),
      );
      expect(first.queueSequence).toBe(1);
      expect(second.queueSequence).toBe(2);

      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      // P5 (plan §2.5): the active host Run claims the contiguous ready prefix, so the second
      // Submission JOINS the first Run instead of waiting for its own claim — one head
      // settlement, and the joined Submission settles with the host in admitted FIFO order.
      expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
        first.submissionId,
      ]);
      const joinedSettlement = yield* runtime.awaitSettlement(second);
      expect(joinedSettlement.outcome).toBe("completed");

      const records = yield* readLog(conversation);
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
      const conversation = "conversation-abort-ready";

      const receipt = yield* runtime.submit(
        agent,
        { question: "abort me" },
        submitOptions(conversation, "abort-ready-1"),
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

      const records = yield* readLog(conversation);
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
      const conversation = "conversation-abort-non-head";

      const head = yield* runtime.submit(
        agent,
        { question: "head" },
        submitOptions(conversation, "non-head-1"),
      );
      const second = yield* runtime.submit(
        agent,
        { question: "cancel me" },
        submitOptions(conversation, "non-head-2"),
      );
      const third = yield* runtime.submit(
        agent,
        { question: "after the gap" },
        submitOptions(conversation, "non-head-3"),
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

      const midRecords = yield* readLog(conversation);
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
      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements.map((settlement) => settlement.submissionId)).toEqual([head.submissionId]);
      const headSettled = yield* runtime.awaitSettlement(head);
      expect(headSettled.outcome).toBe("completed");
      const thirdSettled = yield* runtime.awaitSettlement(third);
      expect(thirdSettled.outcome).toBe("completed");

      // Canonical order: the aborted settlement precedes the head's settlement — that is the
      // documented §7(c) exemption, and the shared invariant checker accepts it.
      const records = yield* readLog(conversation);
      const recordIds = records.map((envelope) => envelope.record.recordId);
      expect(recordIds.indexOf(submissionSettlementRecordId(second.submissionId))).toBeLessThan(
        recordIds.indexOf(submissionSettlementRecordId(head.submissionId)),
      );
      const integrity = yield* runtime.verify(decodeConversationId(conversation));
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
      const conversation = "conversation-abort-running";

      const receipt = yield* runtime.submit(
        agent,
        { question: "block" },
        submitOptions(conversation, "abort-running-1"),
      );
      const worker = yield* Effect.forkChild(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
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

      const records = yield* readLog(conversation);
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
      const conversation = "conversation-fencing";

      const receipt = yield* runtime.submit(
        agent,
        { question: "who owns the lane?" },
        submitOptions(conversation, "fencing-1"),
      );

      const staleWorker = yield* Effect.forkChild(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      yield* Deferred.await(started);

      // A second Attempt (same producer restarting) supersedes the first: higher epoch.
      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");

      // Unblock the stale Attempt: its canonical append must be fenced, not committed.
      yield* Deferred.succeed(latch, void 0);
      const staleExit = yield* Fiber.await(staleWorker);
      expect(failureTag(staleExit)).toBe("FenceRejected");

      const records = yield* readLog(conversation);
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
      const conversation = "conversation-submit-resume";
      const options = submitOptions(conversation, "resume-1");

      yield* armFailpoint("submit:after-admit");
      const interrupted = yield* Effect.exit(runtime.submit(agent, { question: "kill" }, options));
      expect(failureTag(interrupted)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const admitted = yield* ledger.lookup(
        SubmissionLookupByKey.make({
          conversationId: options.conversationId,
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
      const options2 = submitOptions(conversation, "resume-2");
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
            submitOptions("conversation-recover-admit", "recover-1"),
          ),
        );
        expect(failureTag(killedAdmit)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const reports = yield* runtime.runRecovery;
        const admitReport = reports.find(
          (entry) => entry.conversationId === "conversation-recover-admit",
        );
        expect(admitReport?.decision._tag).toBe("CompleteMaterialization");
        expect(admitReport?.disposition).toBe("repaired");
        if (admitReport !== undefined) {
          expect(yield* lookupState(admitReport.submissionId)).toBe("ready");
        }
        expect(logTags(yield* readLog("conversation-recover-admit"))).toContain(
          "ConversationCreated",
        );

        // Kill after materialization: only the readiness marker is missing.
        yield* armFailpoint("submit:after-materialize");
        const killedReady = yield* Effect.exit(
          runtime.submit(
            agent,
            { question: "recover me" },
            submitOptions("conversation-recover-ready", "recover-2"),
          ),
        );
        expect(failureTag(killedReady)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const readinessReports = yield* runtime.runRecovery;
        const repaired = readinessReports.find(
          (report) => report.conversationId === "conversation-recover-ready",
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
      const conversation = "conversation-recover-claim";

      const receipt = yield* runtime.submit(
        agent,
        { question: "apply once" },
        submitOptions(conversation, "claim-kill-1"),
      );
      yield* armFailpoint("claim:after-claim");
      const killed = yield* Effect.exit(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
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

      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements[0]?.outcome).toBe("completed");
      const inputRecords = (yield* readLog(conversation)).filter(
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
      const conversation = "conversation-recover-marker";

      const receipt = yield* runtime.submit(
        agent,
        { question: "mark me" },
        submitOptions(conversation, "marker-1"),
      );
      yield* armFailpoint("input:after-canonical-append");
      const killed = yield* Effect.exit(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("RepairInputMarker");
      expect(report?.disposition).toBe("repaired");

      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements[0]?.outcome).toBe("completed");
      const inputRecords = (yield* readLog(conversation)).filter(
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
      const conversation = "conversation-recover-reserved";

      const receipt = yield* runtime.submit(
        agent,
        { question: "reserve" },
        submitOptions(conversation, "reserved-1"),
      );
      yield* armFailpoint("terminalize:after-reserve");
      const killed = yield* Effect.exit(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("AppendReservedSettlement");
      expect(report?.disposition).toBe("repaired");

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("completed");
      const records = yield* readLog(conversation);
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
      const conversation = "conversation-recover-finalize";

      const receipt = yield* runtime.submit(
        agent,
        { question: "finalize" },
        submitOptions(conversation, "finalize-1"),
      );
      yield* armFailpoint("terminalize:after-canonical-append");
      const killed = yield* Effect.exit(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // The canonical settlement exists while the ledger row is still nonterminal.
      const beforeRecords = yield* readLog(conversation);
      expect(logTags(beforeRecords)).toContain("SubmissionSettled");
      expect(yield* lookupState(receipt.submissionId)).not.toBe("settled");

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("FinalizeLedgerFromHistory");
      expect(report?.disposition).toBe("repaired");

      expect(yield* lookupState(receipt.submissionId)).toBe("settled");
      const afterRecords = yield* readLog(conversation);
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
      const conversation = "conversation-resume-turn";

      const receipt = yield* runtime.submit(
        agent,
        { question: "resume?" },
        submitOptions(conversation, "resume-turn-1"),
      );
      // P5 split commits: the readonly tool Turn commits its response batch at the finish part
      // and its results batch at the next TurnStarted seam — kill right after the results append
      // so Turn 1 is fully canonical and the run is not settled (the P4 boundary, same shape).
      yield* armFailpoint("turn:after-results-append");
      const killed = yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(searchToolLayer)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // Turn 1 (model response + settled Tool batch) is canonical; the run is not settled.
      const runId = runIdForSubmission(receipt.submissionId);
      const committed = yield* readLog(conversation);
      expect(logTags(committed)).toEqual([
        "ConversationCreated",
        "UserInputRecorded",
        "ModelResponseRecorded",
        "ToolCallSettled",
      ]);

      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
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

      const records = yield* readLog(conversation);
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
    conversationId: decodeConversationId("conversation-types"),
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
        submitOptions("conversation-types", "types-1"),
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

      const workerProgram = runtime.processConversation(
        Agent.withModel(searchDefinition, scripted.model),
        decodeConversationId("conversation-types"),
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
        conversationId: decodeConversationId("conversation-journal"),
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

layer(testLayer)("RUN-026 durable compaction and usage re-seed", (it) => {
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
        const conversation = "conversation-compaction";

        // Submission 1: an ordinary tool Run leaves prior-Run records to cover.
        const first = yield* makeScriptedModel((call) =>
          call === 0
            ? toolCallParts
            : finalParts(JSON.stringify({ answer: `Found the sea. ${"PAD".repeat(1_000)}` })),
        );
        yield* runtime.submit(
          Agent.withModel(searchDefinition, first.model),
          { question: "Is a flight available?" },
          submitOptions(conversation, "compaction-1"),
        );
        const firstSettled = yield* runtime
          .processConversation(
            Agent.withModel(searchDefinition, first.model),
            decodeConversationId(conversation),
          )
          .pipe(Effect.provide(searchToolLayer));
        expect(firstSettled[0]?.outcome).toBe("completed");

        // Submission 2: a compacting agent whose estimated context exceeds the limit
        // at Turn 1, forcing summarize; the summarizer response is model call 0 of
        // each Attempt, the final answer the call after it.
        const compactingDefinition = Agent.define("durable-compactor", {
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
          call === 0 || call === 1
            ? finalParts("Goal: prior run booked the flight")
            : finalParts('{"answer":"compacted"}'),
        );
        const compactor = Agent.withModel(compactingDefinition, second.model);
        yield* runtime.submit(
          compactor,
          { question: "what happened?" },
          submitOptions(conversation, "compaction-2"),
        );

        // Crash immediately AFTER the compaction record commits, BEFORE the
        // Turn's model call: the re-driven Attempt must project the compacted
        // prompt and must NOT append a duplicate record.
        yield* armFailpoint("compaction:after-canonical-append");
        const crashed = yield* Effect.exit(
          runtime.processConversation(compactor, decodeConversationId(conversation)),
        );
        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        // The hook failure is typed AND ordering holds: the summarizer call
        // ran, but no compacted Turn request started before a successful
        // record commit.
        expect(second.prompts).toHaveLength(1);
        yield* clearFailpoint;

        const settled = yield* runtime.processConversation(
          compactor,
          decodeConversationId(conversation),
        );
        expect(settled).toHaveLength(1);
        expect(settled[0]?.outcome).toBe("completed");

        const records = yield* readLog(conversation);
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
      }),
  );

  it.effect(
    "RUN-026: the durable cut never covers the threshold-crossing Turn (whole-Turn retention)",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const conversation = "conversation-compaction-cut";

        // Prior run: two tool Turns then a final answer. The second Turn's
        // settled result is huge, so a keepRecentTokens of 400 lands the
        // reverse-scan threshold ON that settled record (mid-Turn).
        const Probe = Tool.make("probe", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        });
        const probeTools = Toolkit.make(Probe);
        const probeDefinition = Agent.define("durable-probe", {
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
          submitOptions(conversation, "cut-1"),
        );
        const priorSettled = yield* runtime
          .processConversation(
            Agent.withModel(probeDefinition, prior.model),
            decodeConversationId(conversation),
          )
          .pipe(Effect.provide(probeLayer));
        expect(priorSettled[0]?.outcome).toBe("completed");

        const compactingDefinition = Agent.define("durable-cut-compactor", {
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
          submitOptions(conversation, "cut-2"),
        );
        const settled = yield* runtime.processConversation(
          compactor,
          decodeConversationId(conversation),
        );
        expect(settled[0]?.outcome).toBe("completed");

        const records = yield* readLog(conversation);
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
      const conversation = "conversation-summarizer-usage";

      // Submission 1 leaves prior-Run records for the compactor to cover.
      const first = yield* makeScriptedModel((call) =>
        call === 0
          ? toolCallParts
          : finalParts(JSON.stringify({ answer: `Found the sea. ${"PAD".repeat(1_000)}` })),
      );
      yield* runtime.submit(
        Agent.withModel(searchDefinition, first.model),
        { question: "Is a flight available?" },
        submitOptions(conversation, "summarizer-usage-1"),
      );
      const firstSettled = yield* runtime
        .processConversation(
          Agent.withModel(searchDefinition, first.model),
          decodeConversationId(conversation),
        )
        .pipe(Effect.provide(searchToolLayer));
      expect(firstSettled[0]?.outcome).toBe("completed");

      const compactingDefinition = Agent.define("durable-usage-compactor", {
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
        submitOptions(conversation, "summarizer-usage-2"),
      );
      const settled = yield* runtime.processConversation(
        compactor,
        decodeConversationId(conversation),
      );
      expect(settled).toHaveLength(1);
      expect(settled[0]?.outcome).toBe("completed");

      const records = yield* readLog(conversation);
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
      const conversation = "conversation-reseed";
      const reseedDefinition = Agent.define("durable-reseed", {
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
      yield* runtime.submit(
        agent,
        { question: "reseed?" },
        submitOptions(conversation, "reseed-1"),
      );

      // Crash after the Turn-1 response commit (usage already staged into the
      // canonical record), leaving a declared pending Tool batch to resume.
      yield* armFailpoint("turn:after-response-append");
      const crashed = yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(searchToolLayer)),
      );
      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const settled = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(searchToolLayer));
      // 950 committed tokens re-seed the resumed Attempt. The completion
      // reserve is already unavailable, so recovery fails before another
      // unconstrained model call; without re-seeding it would continue.
      expect(settled).toHaveLength(1);
      expect(settled[0]?.outcome).toBe("failed");

      const records = yield* readLog(conversation);
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
      const definition = Agent.define("durable-turns-landing", {
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
      const conversation = "conversation-turns-landing";

      yield* runtime.submit(agent, { question: "turns?" }, submitOptions(conversation, "turns-1"));
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(searchToolLayer));
      expect(settlements[0]?.outcome).toBe("completed");

      const settled = lastSettlement(yield* readLog(conversation));
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
      const definition = Agent.define("durable-tokens-landing", {
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
      const conversation = "conversation-tokens-landing";

      yield* runtime.submit(
        agent,
        { question: "tokens?" },
        submitOptions(conversation, "tokens-1"),
      );
      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements[0]?.outcome).toBe("completed");

      const settled = lastSettlement(yield* readLog(conversation));
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
      const definition = Agent.define("durable-tokens-rail", {
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
      const conversation = "conversation-tokens-rail";

      yield* runtime.submit(
        agent,
        { question: "tokens?" },
        submitOptions(conversation, "tokens-rail-1"),
      );
      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements[0]?.outcome).toBe("failed");

      const settled = lastSettlement(yield* readLog(conversation));
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
      const definition = Agent.define("durable-cost-rail", {
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
      const conversation = "conversation-cost-rail";

      yield* runtime.submit(agent, { question: "cost?" }, submitOptions(conversation, "cost-1"));
      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements[0]?.outcome).toBe("failed");

      const settled = lastSettlement(yield* readLog(conversation));
      expect(settled.outcome).toBe("failed");
      expect(settled.policyLimit).toBe("cost");
      expect(settled.result).toMatchObject({ errorTag: "AgentPolicyError" });
    }),
  );

  it.effect("RUN-011: a duration-exhausted Run settles failed with the typed policyLimit", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const definition = Agent.define("durable-duration-rail", {
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
      const conversation = "conversation-duration-rail";

      yield* runtime.submit(
        agent,
        { question: "slow?" },
        submitOptions(conversation, "duration-1"),
      );
      const worker = yield* Effect.forkChild(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      yield* TestClock.adjust(Duration.seconds(6));
      const settlements = yield* Fiber.join(worker);
      expect(settlements[0]?.outcome).toBe("failed");

      const settled = lastSettlement(yield* readLog(conversation));
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
            conversation: "conversation-policy-reserve",
            key: "policy-reserve-1",
          },
          {
            location: "terminalize:after-canonical-append" as const,
            conversation: "conversation-policy-append",
            key: "policy-append-1",
          },
        ];
        for (const scenario of scenarios) {
          const definition = Agent.define("durable-policy-recovery", {
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
            submitOptions(scenario.conversation, scenario.key),
          );
          yield* armFailpoint(scenario.location);
          const killed = yield* Effect.exit(
            runtime.processConversation(agent, decodeConversationId(scenario.conversation)),
          );
          expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          yield* runtime.runRecovery;
          const settlement = yield* runtime.awaitSettlement(receipt);
          expect(settlement.outcome).toBe("failed");
          const records = yield* readLog(scenario.conversation);
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
        const definition = Agent.define("durable-priced-usage", {
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
        const conversation = "conversation-priced-usage";

        yield* runtime.submit(
          agent,
          { question: "cost?" },
          submitOptions(conversation, "priced-usage-1"),
        );
        const settlements = yield* runtime.processConversation(
          agent,
          decodeConversationId(conversation),
        );
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

        const payloads = (yield* readLog(conversation)).map((envelope) => envelope.record.payload);
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

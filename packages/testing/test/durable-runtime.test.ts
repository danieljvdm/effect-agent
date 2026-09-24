import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
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
import { RetryCommand } from "effect-agent/admin";
import * as Agent from "effect-agent/agent";
import { AgentPolicy, CompactionPolicy } from "effect-agent/agent-policy";
import {
  compileRegistrations,
  DurableWorkerBinding,
  type ResolvedBinding,
} from "effect-agent/agent-registration";
import {
  COMPACTION_SUMMARY_PREFIX,
  CONTEXT_ROLLOVER_PREFIX,
  estimatePromptTokens,
} from "effect-agent/compaction";
import { ContextCompactor } from "effect-agent/context-compactor";
import { ContextWindow } from "effect-agent/context-window";
import {
  type Receipt,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  type DurableSubmitOptions,
} from "effect-agent/durable-agent-runtime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "effect-agent/durable-failpoint";
import { ToolExecutionClass } from "effect-agent/durable-step";
import type { SubmissionId } from "effect-agent/identifiers";
import { ThreadId, ToolCallId } from "effect-agent/identifiers";
import * as Output from "effect-agent/output";
import {
  CanonicalRecordEnvelope,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  RunCompleted,
} from "effect-agent/records";
import {
  projectRunJournal,
  promptFromCanonicalRecords,
  runIdForSubmission,
} from "effect-agent/run-journal";
import { RunContextPreparation, RunToolAuthorization } from "effect-agent/run-options";
import {
  AbortCommand,
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  ResolutionCompletedWithResult,
  ResolutionSafeToRetry,
  UnknownResolutionCommand,
  SubmissionLedger,
  SubmissionScheduling,
  SubmissionLookupById,
} from "effect-agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "effect-agent/testing/durable-failpoint-test-control";
import { ThreadRead, ThreadStore, ThreadTailRequest } from "effect-agent/thread-store";
import { ToolBroker } from "effect-agent/tool-broker";
import {
  ReconciliationCompleted,
  ReconciliationNeverStarted,
  ReconciliationSafeToRetry,
  ToolReconciler,
} from "effect-agent/tool-reconciler";
import { WakeScheduler, makeWakeSubscriptionHub } from "effect-agent/wake-scheduler";
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

const makeReceiptCompletionFixture = (needsApproval = false) => {
  const Create = Tool.make("create", {
    parameters: Schema.Struct({ name: Schema.String }),
    success: Schema.Struct({
      name: Schema.String,
      href: Schema.String,
      complete: Schema.Boolean,
    }),
    needsApproval,
  });

  const Respond = Tool.make("respond", {
    parameters: Schema.Struct({ answer: Schema.String }),
    success: Schema.Struct({ answer: Schema.String }),
  });

  const tools = Toolkit.make(Create, Respond);

  const definition = Agent.make("durable-receipt-completion", {
    input: Schema.Struct({ question: Schema.String }),
    output: Schema.Struct({ answer: Schema.String }),
    instructions: "Use create only when creation satisfies the whole request; otherwise respond.",
    toolkit: tools,
    policy: AgentPolicy.make({
      maxTurns: 3,
      maxToolCalls: 2,
      maxDuration: "30 seconds",
      toolConcurrency: 1,
    }),
    completion: {
      tool: "respond",
      required: true,
      project: ({ result }) => result,
    },
    completionFromTools: [
      {
        tool: "create",
        project: ({ result }) =>
          result.complete
            ? Option.some({ answer: `Created ${result.name}: ${result.href}` })
            : Option.none(),
      },
    ],
  });

  return { definition, tools };
};

const receiptCreateParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: "create-1",
    name: "create",
    params: { name: "requested name" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

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

const isAnswerCompletionOutput = Schema.is(AnswerCompletionOutput);

const corruptCompletionOutput = (output: Schema.Json): Schema.Json => {
  if (isAnswerCompletionOutput(output)) return { answer: "hostile replacement" };

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
        ...payload,
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

class ProgressWaitTestControl extends Context.Service<
  ProgressWaitTestControl,
  {
    readonly scheduler: WakeScheduler["Service"];
    readonly active: Ref.Ref<number>;
    readonly parking: Ref.Ref<number>;
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
      subscribeGate,
      parkGate,
    });
  }),
);

const progressWaitSchedulerLayer = Layer.effect(
  WakeScheduler,
  Effect.map(ProgressWaitTestControl, (control) => control.scheduler),
);

const progressWaitAdapters = Layer.merge(progressWaitSchedulerLayer, MemoryThreadStoreLive).pipe(
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
  if (Exit.isSuccess(exit))
    throw new Error(`Expected the Effect to fail, received ${JSON.stringify(exit.value)}`);
  const failure = Cause.findErrorOption(exit.cause);

  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  const error: unknown = failure.value;

  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

layer(progressWaitTestLayer)("#94 DurableAgentRuntime progress waits", (it) => {
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
      const parksBeforeParkRace = yield* Ref.get(control.parking);
      const parkedRace = yield* Effect.forkChild(runtime.awaitProgress(threadId, abortCursor));

      yield* waitForAtLeast(control.parking, parksBeforeParkRace + 1);
      yield* scheduler.notify(threadId);
      yield* Deferred.succeed(beforePark, undefined);
      yield* Fiber.join(parkedRace);
      expect(yield* Ref.get(control.active)).toBe(0);

      // The second wake was deliberately a false positive: storage, not the hint, is truth.
      const final = yield* readLog(threadId);

      expect(final.at(-1)?.sequence).toBe(abortCursor);
    }),
  );

  it.effect("broadcasts to concurrent waiters and cleans up cancellation and timeout", () =>
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
      const first = yield* Effect.forkChild(runtime.awaitProgress(threadId, cursor));
      const second = yield* Effect.forkChild(runtime.awaitProgress(threadId, cursor));

      const unrelated = yield* Effect.forkChild(
        runtime.awaitProgress(unrelatedId, unrelatedCursor),
      );

      yield* waitForAtLeast(control.active, 3);
      yield* waitForAtLeast(control.parking, 3);

      yield* TestClock.adjust(Duration.seconds(10));

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

  it.effect("#509 retains the complete conversation across consecutive durable Runs", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel((call) => finalParts(String(call + 1)));

      const agent = Agent.withModel(
        Agent.make("durable-conversation", {
          input: Schema.String,
          inputPrompt: (message) => message,
          output: Output.text(Schema.String),
          instructions: Prompt.empty,
          toolkit: Toolkit.empty,
          policy: plannerDefinition.policy,
        }),
        scripted.model,
      );

      const threadId = decodeThreadId("thread-conversation");

      for (const [index, input] of ["Say 1", "Say 2", "Say 3"].entries()) {
        yield* runtime.submit(agent, input, submitOptions(threadId, `message-${index + 1}`));
        const settlements = yield* runtime.processThread(agent, threadId);

        expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      }

      const conversations = scripted.prompts.map((prompt) =>
        prompt.content
          .filter((message) => message.role !== "system")
          .map((message) => ({
            role: message.role,
            text:
              typeof message.content === "string"
                ? message.content
                : message.content
                    .filter((part) => part.type === "text")
                    .map((part) => part.text)
                    .join(""),
          })),
      );

      expect(conversations).toEqual([
        [{ role: "user", text: "Say 1" }],
        [
          { role: "user", text: "Say 1" },
          { role: "assistant", text: "1" },
          { role: "user", text: "Say 2" },
        ],
        [
          { role: "user", text: "Say 1" },
          { role: "assistant", text: "1" },
          { role: "user", text: "Say 2" },
          { role: "assistant", text: "2" },
          { role: "user", text: "Say 3" },
        ],
      ]);
    }),
  );

  it.effect("recovers canonical receipt completion without repeating the action or model", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const { definition, tools } = makeReceiptCompletionFixture();
      const scripted = yield* makeScriptedModel(() => receiptCreateParts);
      const createCalls = yield* Ref.make(0);
      const respondCalls = yield* Ref.make(0);

      const toolLayer = tools.toLayer({
        create: () =>
          Ref.update(createCalls, (count) => count + 1).pipe(
            Effect.as({ name: "Committed name", href: "/projects/created-1", complete: true }),
          ),
        respond: (parameters) =>
          Ref.update(respondCalls, (count) => count + 1).pipe(Effect.as(parameters)),
      });

      const agent = Agent.withModel(definition, scripted.model);
      const thread = "thread-receipt-completion-recovery";
      const options = submitOptions(thread, "receipt-completion-recovery-1");
      const input = { question: "Create requested name." };
      const receipt = yield* runtime.submit(agent, input, options);

      yield* armFailpoint("turn:after-results-append");

      const crashed = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
      );

      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      expect(yield* Ref.get(createCalls)).toBe(1);
      expect(scripted.prompts).toHaveLength(1);
      yield* clearFailpoint;

      const settled = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(toolLayer));

      expect(settled).toHaveLength(1);
      expect(settled[0]?.outcome).toBe("completed");
      expect(yield* runtime.awaitSettlement(receipt)).toEqual(settled[0]);
      const repeated = yield* runtime.submit(agent, input, options);

      expect(repeated.submissionId).toBe(receipt.submissionId);
      expect(
        yield* runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
      ).toHaveLength(0);
      expect(yield* Ref.get(createCalls)).toBe(1);
      expect(yield* Ref.get(respondCalls)).toBe(0);
      expect(scripted.prompts).toHaveLength(1);
      const payloads = (yield* readLog(thread)).map((envelope) => envelope.record.payload);

      expect(payloads.filter((payload) => payload._tag === "ModelResponseRecorded")).toHaveLength(
        1,
      );
      expect(payloads.filter((payload) => payload._tag === "ToolCallSettled")).toHaveLength(1);
      expect(payloads.filter((payload) => payload._tag === "RunCompleted")).toHaveLength(1);
      expect(payloads.filter((payload) => payload._tag === "SubmissionSettled")).toHaveLength(1);
      expect(payloads.at(-1)).toMatchObject({
        _tag: "SubmissionSettled",
        result: { answer: "Created Committed name: /projects/created-1" },
      });
    }),
  );

  it.effect(
    "receipt completion projects a recovered external result without another model call",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const fixture = makeReceiptCompletionFixture();

        const scripted = yield* makeScriptedModel(() => [
          {
            type: "tool-call",
            id: "hosted-1",
            name: "respond",
            params: { answer: "found" },
            providerExecuted: true,
          },
          {
            type: "tool-result",
            id: "hosted-1",
            name: "respond",
            result: { answer: "found" },
            isFailure: false,
            providerExecuted: true,
          },
          ...receiptCreateParts,
        ]);

        const agent = Agent.withModel(fixture.definition, scripted.model);
        const thread = "thread-receipt-recovered-result";
        const starts = yield* Ref.make(0);

        const tools = fixture.tools.toLayer({
          create: () =>
            Ref.update(starts, (count) => count + 1).pipe(
              Effect.as({ name: "Project", href: "/project/1", complete: true }),
            ),
          respond: () => Effect.die("Recorded provider work must not be replayed"),
        });

        const receipt = yield* runtime.submit(
          agent,
          { question: "create" },
          submitOptions(thread, "recovered-result"),
        );

        yield* armFailpoint("tools:after-prepared-append");

        const crashed = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(tools), Effect.exit);

        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        yield* runtime.runRecovery();
        expect(yield* lookupState(receipt.submissionId)).toBe("unknown");
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: Schema.decodeSync(ToolCallId)("create-1"),
            author: "operator",
            reason: "The authoritative product receipt confirms committed creation",
            resolution: ResolutionCompletedWithResult.make({
              result: { name: "Project", href: "/project/1", complete: true },
              isFailure: false,
            }),
          }),
        );
        yield* armFailpoint("turn:after-results-append");

        const afterProjection = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(tools), Effect.exit);

        expect(failureTag(afterProjection)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const settled = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(tools));

        expect(settled[0]?.outcome).toBe("completed");
        expect(yield* Ref.get(starts)).toBe(0);
        expect(scripted.prompts).toHaveLength(1);
        const records = yield* readLog(thread);

        expect(
          records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
        ).toHaveLength(1);
        expect(records.filter(({ record }) => record.payload._tag === "RunCompleted")).toHaveLength(
          1,
        );
        expect(records.at(-1)?.record.payload).toMatchObject({
          _tag: "SubmissionSettled",
          result: { answer: "Created Project: /project/1" },
        });
      }),
  );

  {
    const scenario = {
      name: "corrected result",
      location: "turn:after-results-append",
      repeated: false,
    } as const;

    it.effect(
      `mixed completion recovery retains hosted results and preserves ${scenario.name} without duplicate effects`,
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          const Search = Tool.make("search", {
            parameters: Schema.Struct({}),
            success: Schema.String,
          });

          const Deliver = Tool.make("deliver", {
            parameters: Schema.Struct({ message: Schema.String }),
            success: Schema.String,
          });

          const tools = Toolkit.make(Search, Deliver);

          const definition = Agent.make("durable-completion-correction", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Research, then deliver alone.",
            toolkit: tools,
            completion: { tool: "deliver", required: true, project: ({ result }) => result },
            policy: {
              maxTurns: 5,
              maxToolCalls: 8,
              maxDuration: "30 seconds",
              repeatedFailureLimit: 3,
            },
          });

          const scripted = yield* makeScriptedModel((turn) =>
            turn === 0 || scenario.repeated
              ? [
                  ...(turn === 0
                    ? ([
                        {
                          type: "tool-call",
                          id: `hosted-${turn}`,
                          name: "search",
                          params: {},
                          providerExecuted: true,
                        },
                        {
                          type: "tool-result",
                          id: `hosted-${turn}`,
                          name: "search",
                          result: "provider evidence",
                          isFailure: false,
                          providerExecuted: true,
                        },
                      ] as const)
                    : []),
                  {
                    type: "tool-call",
                    id: `premature-${turn}`,
                    name: "deliver",
                    params: { message: "premature" },
                    providerExecuted: false,
                  },
                  {
                    type: "tool-call",
                    id: `rejected-${turn}`,
                    name: "search",
                    params: {},
                    providerExecuted: false,
                  },
                  {
                    type: "finish",
                    reason: "tool-calls",
                    usage: { inputTokens: { total: 10 }, outputTokens: { total: 5 } },
                  },
                ]
              : turn === 1
                ? [
                    {
                      type: "tool-call",
                      id: "research",
                      name: "search",
                      params: {},
                      providerExecuted: false,
                    },
                    {
                      type: "finish",
                      reason: "tool-calls",
                      usage: { inputTokens: { total: 20 }, outputTokens: { total: 10 } },
                    },
                  ]
                : [
                    {
                      type: "tool-call",
                      id: "final",
                      name: "deliver",
                      params: { message: "researched" },
                      providerExecuted: false,
                    },
                    {
                      type: "finish",
                      reason: "tool-calls",
                      usage: { inputTokens: { total: 30 }, outputTokens: { total: 15 } },
                    },
                  ],
          );

          const starts: Array<string> = [];

          const toolLayer = tools.toLayer({
            search: () =>
              Effect.sync(() => {
                starts.push("search");

                return "found";
              }),
            deliver: ({ message }) =>
              Effect.sync(() => {
                starts.push(message);

                return message;
              }),
          });

          const agent = Agent.withModel(definition, scripted.model);
          const thread = `correction-${scenario.name}`;

          const receipt = yield* runtime.submit(
            agent,
            "travel",
            submitOptions(thread, "correction"),
          );

          yield* armFailpoint(scenario.location);

          const crashed = yield* runtime
            .processThread(agent, decodeThreadId(thread))
            .pipe(Effect.provide(toolLayer), Effect.exit);

          expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          const before = (yield* readLog(thread)).map((envelope) => envelope.record.payload);

          expect(
            before.filter((payload) => payload._tag === "ToolCallSettled").slice(0, 2),
          ).toMatchObject([
            {
              toolCallId: "premature-0",
              toolName: "deliver",
              isFailure: true,
              result: { _tag: "ModelProtocolError" },
            },
            {
              toolCallId: "rejected-0",
              toolName: "search",
              isFailure: true,
              result: { _tag: "ModelProtocolError" },
            },
          ]);
          expect(
            before.some(
              (payload) =>
                payload._tag === "ToolCallPrepared" &&
                (payload.toolCallId === "premature-0" || payload.toolCallId === "rejected-0"),
            ),
          ).toBe(false);
          expect(before.some((payload) => payload._tag === "RunCompleted")).toBe(false);
          expect(starts).toEqual(["search"]);

          yield* runtime
            .processThread(agent, decodeThreadId(thread))
            .pipe(Effect.provide(toolLayer));
          const settlement = yield* runtime.awaitSettlement(receipt);

          expect(settlement.outcome).toBe("completed");
          expect(starts).toEqual(["search", "researched"]);
          expect(scripted.prompts).toHaveLength(3);
          const correctionPrompt = JSON.stringify(scripted.prompts[1]);

          expect(correctionPrompt).toContain("none of its application tools ran");
          expect(correctionPrompt).toContain("provider evidence");
          expect(correctionPrompt).toContain("premature-0");
          const after = (yield* readLog(thread)).map((envelope) => envelope.record.payload);

          expect(after.filter((payload) => payload._tag === "RunCompleted")).toHaveLength(1);
          expect(after.filter((payload) => payload._tag === "SubmissionSettled")).toHaveLength(1);
          expect(after.filter((payload) => payload._tag === "ModelResponseRecorded")).toHaveLength(
            3,
          );
          expect(after.find((payload) => payload._tag === "SubmissionSettled")).toMatchObject({
            result: "researched",
          });
        }),
    );
  }
});

layer(corruptedCompletionTestLayer)("RUN-032 recovered completion validation", (it) => {
  it.effect("rejects a recovered receipt completion that disagrees with the canonical result", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const { definition, tools } = makeReceiptCompletionFixture();
      const scripted = yield* makeScriptedModel(() => receiptCreateParts);
      const createCalls = yield* Ref.make(0);
      const respondCalls = yield* Ref.make(0);

      const toolLayer = tools.toLayer({
        create: () =>
          Ref.update(createCalls, (count) => count + 1).pipe(
            Effect.as({ name: "Canonical name", href: "/projects/canonical-1", complete: true }),
          ),
        respond: (parameters) =>
          Ref.update(respondCalls, (count) => count + 1).pipe(Effect.as(parameters)),
      });

      const agent = Agent.withModel(definition, scripted.model);
      const thread = "thread-hostile-receipt-completion";

      yield* runtime.submit(
        agent,
        { question: "Create a project." },
        submitOptions(thread, "hostile-receipt-completion-1"),
      );
      yield* armFailpoint("turn:after-results-append");

      const crashed = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
      );

      expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const recovered = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
      );

      expect(failureTag(recovered)).toBe("RunJournalError");
      expect(yield* Ref.get(createCalls)).toBe(1);
      expect(yield* Ref.get(respondCalls)).toBe(0);
      expect(scripted.prompts).toHaveLength(1);
    }),
  );
});

layer(testLayer)("RUN-026 durable compaction and usage re-seed", (it) => {
  it.effect("programmatic reservations survive loss before the inner Handler starts", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;

      {
        const location = "policy:after-reservation-append" as const;

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
        expect(yield* Ref.get(executions)).toBe(0);
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
  it.effect("replacement Attempts preserve the original Tool call limit", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;

      {
        const limit = "tool-calls" as const;

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
            maxTurns: 10,
            maxToolCalls: 2,
            repeatedFailureLimit: 0,
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
            Ref.update(executions, (n) => n + 1).pipe(Effect.andThen(Effect.succeed("ok"))),
        });

        const receipt = yield* runtime.submit(
          agent,
          "probe",
          submitOptions(`limits-${limit}`, "limits"),
        );

        const run = runtime.processThread(agent, receipt.threadId).pipe(Effect.provide(handlers));

        yield* armFailpoint("turn:after-results-append");
        for (let attempt = 0; attempt < 2; attempt++) {
          const exit = yield* Effect.exit(run);

          expect(exit._tag, `${limit} Attempt ${attempt}: ${JSON.stringify(exit)}`).toBe("Failure");
          expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
        }
        yield* clearFailpoint;
        const settlements = yield* run;

        expect(settlements[0]?.outcome).toBe("failed");
        expect(yield* Ref.get(executions)).toBe(2);
        expect(scripted.prompts.length).toBe(3);
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
    "automatic rollover commits complete first-Run Tool results before recovery and preserves their original evidence",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        const toolkit = Toolkit.make(
          Tool.make("large_result", {
            parameters: Schema.Struct({}),
            success: Schema.String,
            dependencies: [ContextWindow],
          }).annotate(ToolExecutionClass, "readonly"),
        );

        const definition = Agent.make("durable-automatic-window", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Complete the original objective.",
          toolkit,
          policy: AgentPolicy.make({
            maxTurns: 3,
            maxToolCalls: 2,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
            contextTokenLimit: 1_500,
          }),
        });

        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? [
                {
                  type: "tool-call",
                  id: "large-result-call",
                  name: "large_result",
                  params: {},
                  providerExecuted: false,
                },
                { type: "finish", reason: "tool-calls", usage: usageOf(100, 10) },
              ]
            : finalPartsWithUsage('"done"', usageOf(50, 5)),
        );

        const agent = Agent.withModel(definition, scripted.model);
        const evidence = `ORIGINAL-LARGE-EVIDENCE ${"x".repeat(24_000)}`;
        const executions = yield* Ref.make(0);
        const entered = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const windowEstimates: Array<number> = [];

        const handlers = toolkit.toLayer({
          large_result: () =>
            Effect.gen(function* () {
              yield* Ref.update(executions, (count) => count + 1);
              const window = yield* ContextWindow;

              windowEstimates.push((yield* window.status).estimatedTokens);
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(released);

              return evidence;
            }),
        });

        const receipt = yield* runtime.submit(
          agent,
          "Finish this exact request",
          submitOptions("automatic-window", "first"),
        );

        const process = runtime.processThread(agent, receipt.threadId).pipe(
          Effect.provide(handlers),
          Effect.provideService(ContextCompactor, {
            estimate: estimatePromptTokens,
            compact: () => Stream.die("Worker compactor must not replace the host strategy"),
          }),
          Effect.provideService(RunContextPreparation, {
            hook: {
              prepare: () => Effect.die("Worker preparation must not replace the host hook"),
            },
          }),
        );

        yield* armFailpoint("turn:after-response-append");
        const declared = yield* process.pipe(Effect.exit, Effect.ensuring(clearFailpoint));

        expect(failureTag(declared)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(executions)).toBe(0);
        yield* armFailpoint("compaction:after-canonical-append");

        const running = yield* process.pipe(
          Effect.exit,
          Effect.ensuring(clearFailpoint),
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        expect(windowEstimates).toEqual([110]);
        yield* runtime.submit(
          agent,
          "STEERING-MUST-SURVIVE: include the latest correction exactly.",
          submitOptions("automatic-window", "steering"),
        );
        yield* Deferred.succeed(released, undefined);
        const interrupted = yield* Fiber.join(running);

        expect(failureTag(interrupted)).toBe("DurableRuntimeFailpointError");
        expect(scripted.prompts).toHaveLength(1);
        const before = yield* readLog(receipt.threadId);
        const rollover = before.find(({ record }) => record.payload._tag === "CompactionCreated");
        const result = before.find(({ record }) => record.payload._tag === "ToolCallSettled");

        expect(rollover?.record.payload).toMatchObject({
          kind: "rollover",
          coversThrough: result?.sequence,
        });
        expect(result?.record.payload).toMatchObject({ result: evidence });

        const settlements = yield* process;

        expect(settlements[0]?.outcome).toBe("completed");
        expect(settlements[0]?.usageSummary).toMatchObject({
          modelCalls: 2,
          inputTokens: { total: 150 },
          outputTokens: { total: 15 },
        });
        expect(yield* Ref.get(executions)).toBe(1);
        expect(scripted.prompts).toHaveLength(2);
        expect(
          scripted.prompts.every((prompt) => promptTexts(prompt).includes("HOST-REFERENCE")),
        ).toBe(true);
        expect(promptTexts(scripted.prompts[1] ?? Prompt.empty)).toContain(
          "Finish this exact request",
        );
        expect(promptTexts(scripted.prompts[1] ?? Prompt.empty)).toContain(CONTEXT_ROLLOVER_PREFIX);
        expect(promptTexts(scripted.prompts[1] ?? Prompt.empty)).toContain(
          "STEERING-MUST-SURVIVE: include the latest correction exactly.",
        );
        expect(
          estimatePromptTokens((scripted.prompts[1] ?? Prompt.empty).content),
        ).toBeLessThanOrEqual(1_500);
        const records = yield* readLog(receipt.threadId);

        expect(
          records.filter(({ record }) => record.payload._tag === "CompactionCreated"),
        ).toHaveLength(1);
        expect(JSON.stringify(records)).toContain(evidence);
        expect(JSON.stringify(records)).not.toContain("HOST-REFERENCE");
      }).pipe(
        Effect.provide(
          Layer.fresh(DurableAgentRuntime.layerWithServices).pipe(
            Layer.provide(ContextCompactor.layerRollover),
            Layer.provide(
              Layer.succeed(RunContextPreparation, {
                hook: { prepare: ({ source }) => Effect.succeed({ prompt: source }) },
                transientContext: { load: () => Effect.succeed(Prompt.make("HOST-REFERENCE")) },
              }),
            ),
            Layer.provideMerge(baseLayer),
          ),
        ),
      ),
  );

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
    "re-reads transient references after durable compaction recovery without persisting them",
    () => {
      let reference = "EXTERNAL-REVISION-ONE";
      let reads = 0;

      const preparation = Layer.merge(
        Layer.succeed(RunContextPreparation, {
          transientContext: {
            load: () =>
              Effect.sync(() => {
                reads += 1;

                return Prompt.make([
                  { role: "user", content: `Untrusted reference: ${reference}` },
                ]);
              }),
          },
        }),
        Layer.succeed(ContextCompactor, {
          estimate: estimatePromptTokens,
          compact: ({ source }) => {
            expect(promptTexts(source)).not.toContain("EXTERNAL-REVISION");

            return Stream.succeed({
              kind: "summarize",
              through: source.content.findIndex((message) => message.role === "assistant") + 1,
              summary: "Prior recorded discussion",
            });
          },
        }),
      );

      return Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const thread = "transient-compaction-recovery";

        const first = yield* makeScriptedModel(() =>
          finalParts(JSON.stringify({ answer: "HISTORY ".repeat(800) })),
        );

        const initial = Agent.withModel(plannerDefinition, first.model);

        yield* runtime.submit(initial, { question: "seed" }, submitOptions(thread, "first"));
        expect((yield* runtime.processThread(initial, decodeThreadId(thread)))[0]?.outcome).toBe(
          "completed",
        );

        const definition = Agent.make("transient-durable-compactor", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Use attributed reference material as evidence.",
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

        const second = yield* makeScriptedModel(() => finalParts('"done"'));
        const agent = Agent.withModel(definition, second.model);

        yield* runtime.submit(agent, "continue", submitOptions(thread, "second"));
        yield* armFailpoint("compaction:after-canonical-append");

        const crashed = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.exit, Effect.ensuring(clearFailpoint));

        expect(failureTag(crashed)).toBe("DurableRuntimeFailpointError");
        expect(second.prompts).toHaveLength(0);
        expect(reads).toBe(1);
        reference = "EXTERNAL-REVISION-TWO";
        expect((yield* runtime.processThread(agent, decodeThreadId(thread)))[0]?.outcome).toBe(
          "completed",
        );
        expect(reads).toBe(2);
        expect(promptTexts(second.prompts[0] ?? Prompt.empty)).toContain("EXTERNAL-REVISION-TWO");
        expect(promptTexts(second.prompts[0] ?? Prompt.empty)).not.toContain(
          "EXTERNAL-REVISION-ONE",
        );
        const records = yield* readLog(thread);

        expect(JSON.stringify(records)).not.toContain("EXTERNAL-REVISION");
        expect(
          records.filter(({ record }) => record.payload._tag === "CompactionCreated"),
        ).toHaveLength(1);
        expect(promptTexts(yield* promptFromCanonicalRecords(records))).not.toContain(
          "EXTERNAL-REVISION",
        );
      }).pipe(
        Effect.provide(
          Layer.fresh(DurableAgentRuntime.layerWithServices).pipe(
            Layer.provide(preparation),
            Layer.provideMerge(baseLayer),
          ),
        ),
      );
    },
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

layer(testLayer)("RUN-030 durable execution duration", (it) => {
  {
    const expired = true as const;

    it.effect(
      "yields before the next provider call, releases resources, and resumes after downtime",
      () =>
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const acquired = yield* Ref.make(0);
          const released = yield* Ref.make(0);
          const toolCalls = yield* Ref.make(0);
          const preparations = yield* Ref.make(0);
          const thread = `host-yield-${expired}`;

          const scripted = yield* makeScriptedModel((call) =>
            call === 0 ? toolCallParts : finalParts('{"answer":"resumed"}'),
          );

          const model = Layer.merge(
            scripted.model,
            Layer.effectDiscard(
              Effect.acquireRelease(
                Ref.update(acquired, (count) => count + 1),
                () => Ref.update(released, (count) => count + 1),
              ),
            ),
          );

          const yieldingTools = Toolkit.make(Search.annotate(ToolExecutionClass, "uncertain"));

          const definition = Agent.make("host-yield-search", {
            input: searchDefinition.input,
            output: searchDefinition.output,
            instructions: "Search before answering.",
            toolkit: yieldingTools,
            policy: searchDefinition.policy,
          });

          const agent = Agent.withModel(definition, model);

          const binding = yield* DurableWorkerBinding.make(agent, DIGESTS).pipe(
            Effect.provide(
              yieldingTools.toLayer({
                search: () =>
                  Ref.update(toolCalls, (count) => count + 1).pipe(
                    Effect.andThen(TestClock.adjust(Duration.seconds(1))),
                    Effect.as({ available: true }),
                  ),
              }),
            ),
          );

          yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;

            const receipt = yield* runtime.submit(
              agent,
              { question: "yield" },
              submitOptions(thread, "one"),
            );

            const now = yield* DateTime.now;
            const yieldAfter = DateTime.toUtc(DateTime.makeUnsafe(DateTime.toEpochMillis(now) + 1));
            const first = yield* runtime.processThreadHead(receipt.threadId, { yieldAfter });

            expect(Option.isNone(first)).toBe(true);
            expect(scripted.prompts).toHaveLength(1);
            expect(yield* Ref.get(preparations)).toBe(1);
            expect(yield* Ref.get(acquired)).toBe(1);
            expect(yield* Ref.get(released)).toBe(1);

            const pending = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
            );

            expect(pending.ownership).toBeUndefined();
            expect(pending.submission.state).not.toBe("settled");
            const before = yield* readLog(thread);

            expect(
              before.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
            ).toHaveLength(1);
            const start = before.find(({ record }) => record.payload._tag === "RunStarted");

            yield* TestClock.adjust(Duration.seconds(30));
            const second = yield* runtime.processThreadHead(receipt.threadId);

            expect(Option.isSome(second) && second.value.outcome).toBe("completed");
            expect(scripted.prompts).toHaveLength(2);
            expect(yield* Ref.get(acquired)).toBe(2);
            expect(yield* Ref.get(released)).toBe(2);
            expect(yield* Ref.get(toolCalls)).toBe(1);
            const after = yield* readLog(thread);

            expect(after.filter(({ record }) => record.payload._tag === "RunStarted")).toEqual([
              start,
            ]);
            expect(
              after.filter(({ record }) => record.payload._tag === "ToolCallPrepared"),
            ).toHaveLength(1);
            expect(
              after.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
            ).toHaveLength(1);
          }).pipe(
            Effect.provide(DurableAgentRuntime.layerWithBindings([binding])),
            Effect.provideService(RunContextPreparation, {
              hook: {
                prepare: ({ source }) =>
                  Ref.update(preparations, (count) => count + 1).pipe(
                    Effect.as({ prompt: source }),
                  ),
              },
            }),
          );
        }),
    );
  }
});

layer(testLayer)("deployment continuity", (it) => {
  it.effect(
    "explains and retries unsupported SafeToRetry without touching a later live owner's canonical tail",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const modelCalls = yield* Ref.make(0);
        const handlerCalls = yield* Ref.make(0);

        const model = Model.make(
          "scripted",
          "unsupported-retry-admin",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Effect.gen(function* () {
                    const call = yield* Ref.getAndUpdate(modelCalls, (count) => count + 1);

                    if (call === 0) return Stream.fromIterable(toolCallParts);
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);

                    return Stream.fromIterable(finalParts('{"answer":"later request completed"}'));
                  }),
                ),
            }),
          ),
        );

        const originalTools = Toolkit.make(Search.annotate(ToolExecutionClass, "uncertain"));
        const originalDefinition = { ...searchDefinition, toolkit: originalTools };
        const currentDefinition = { ...searchDefinition, toolkit: Toolkit.empty };

        const originalBindings = yield* compileRegistrations([
          {
            agent: Agent.withModel(originalDefinition, model),
            definitions: { agent: "original", model: "scripted", tools: ["search"] },
            continuity: { versions: { tools: { search: "v1" } } },
          },
        ]).pipe(
          Effect.provide(
            originalTools.toLayer({
              search: () =>
                Ref.update(handlerCalls, (count) => count + 1).pipe(Effect.as({ available: true })),
            }),
          ),
        );

        const currentBindings = yield* compileRegistrations([
          {
            agent: Agent.withModel(currentDefinition, model),
            definitions: { agent: "current", model: "scripted", tools: [] },
          },
        ]);

        const original = yield* Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          const receipt = yield* runtime.submitRegistered(
            { definition: originalDefinition },
            { question: "original uncertain operation" },
            submitOptions("unsupported-retry-admin", "original"),
          );

          yield* armFailpoint("tools:after-prepared-append");
          expect(failureTag(yield* Effect.exit(runtime.processThreadHead(receipt.threadId)))).toBe(
            "DurableRuntimeFailpointError",
          );
          yield* clearFailpoint;

          return receipt;
        }).pipe(Effect.provide(DurableAgentRuntime.layerWithBindings(originalBindings)));

        const runtime = yield* DurableAgentRuntime.pipe(
          Effect.provide(DurableAgentRuntime.layerWithBindings(currentBindings)),
        );

        yield* runtime.runRecovery();
        expect(yield* lookupState(original.submissionId)).toBe("unknown");
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: original.submissionId,
            toolCallId: Schema.decodeSync(ToolCallId)("search-1"),
            author: "operator",
            reason: "Only the original supplier operation may repeat",
            resolution: ResolutionSafeToRetry.make({}),
          }),
        );
        expect(Option.isNone(yield* runtime.processThreadHead(original.threadId))).toBe(true);
        expect(yield* lookupState(original.submissionId)).toBe("unknown");

        const later = yield* runtime.submitRegistered(
          { definition: currentDefinition },
          { question: "answer this later question" },
          submitOptions(original.threadId, "later"),
        );

        const worker = yield* Effect.forkChild(runtime.processThreadHead(original.threadId));

        yield* Deferred.await(entered);
        const store = yield* ThreadStore;
        const ledger = yield* SubmissionLedger;

        const tailBefore = yield* store.inspectTail(
          ThreadTailRequest.make({ threadId: original.threadId }),
        );

        const ownerBefore = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: later.submissionId }),
        );

        expect(ownerBefore.ownership).toBeDefined();
        const explanation = yield* runtime.explain(original.submissionId);

        expect({
          decision: explanation.decision._tag,
          disposition: explanation.disposition,
        }).toEqual({
          decision: "ApplyUnknownResolutions",
          disposition: "unknown",
        });
        expect(explanation.evidence.unknownCalls).toMatchObject([
          { toolCallId: "search-1", resolved: false },
        ]);
        expect(explanation.evidence.pendingOperations).toMatchObject([
          { toolCallId: "search-1", parameters: { query: "sea" } },
        ]);

        const retried = yield* runtime.retry(
          RetryCommand.make({
            submissionId: original.submissionId,
            author: "operator",
            reason: "Inspect the still unsupported original operation",
          }),
        );

        expect(retried.disposition).toBe("unknown");
        expect(
          yield* store.inspectTail(ThreadTailRequest.make({ threadId: original.threadId })),
        ).toEqual(tailBefore);
        expect(
          (yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: later.submissionId }),
          )).ownership,
        ).toEqual(ownerBefore.ownership);
        expect(yield* lookupState(original.submissionId)).toBe("unknown");
        yield* Deferred.succeed(release, undefined);
        const completed = yield* Fiber.join(worker);

        expect(Option.isSome(completed) && completed.value).toMatchObject({
          submissionId: later.submissionId,
          outcome: "completed",
        });
        expect(yield* Ref.get(handlerCalls)).toBe(0);
        expect(yield* Ref.get(modelCalls)).toBe(2);
        expect(
          (yield* readLog(original.threadId)).filter(
            ({ record }) => record.payload._tag === "ToolCallSettled",
          ),
        ).toEqual([]);
      }),
  );

  for (const proof of ["NeverStarted", "SafeToRetry"] as const) {
    it.effect(`${proof} cannot execute a prepared operation with changed semantics`, () =>
      Effect.gen(function* () {
        const starts = yield* Ref.make(0);
        const tools = Toolkit.make(Search.annotate(ToolExecutionClass, "uncertain"));
        const definition = { ...searchDefinition, toolkit: tools };

        const scripted = yield* makeScriptedModel((call) =>
          call === 0 ? toolCallParts : finalParts('{"answer":"retired safely"}'),
        );

        const handlers = tools.toLayer({
          search: () =>
            Ref.update(starts, (count) => count + 1).pipe(Effect.as({ available: true })),
        });

        const originalBindings = yield* compileRegistrations([
          {
            agent: Agent.withModel(definition, scripted.model),
            definitions: { agent: "original", model: "scripted", tools: ["search"] },
            continuity: { versions: { tools: { search: "v1" } } },
          },
        ]).pipe(Effect.provide(handlers));

        const currentBindings = yield* compileRegistrations([
          {
            agent: Agent.withModel(definition, scripted.model),
            definitions: { agent: "current", model: "scripted", tools: ["search"] },
            continuity: { versions: { tools: { search: "v2" } } },
          },
        ]).pipe(Effect.provide(handlers));

        const receipt = yield* Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          const receipt = yield* runtime.submitRegistered(
            { definition },
            { question: "original operation" },
            submitOptions(`continuity-proof-${proof}`, "original"),
          );

          yield* armFailpoint("tools:after-prepared-append");
          expect(failureTag(yield* Effect.exit(runtime.processThreadHead(receipt.threadId)))).toBe(
            "DurableRuntimeFailpointError",
          );
          yield* clearFailpoint;

          return receipt;
        }).pipe(Effect.provide(DurableAgentRuntime.layerWithBindings(originalBindings)));

        const retained = yield* readLog(receipt.threadId);
        const reviewed: Array<unknown> = [];

        const currentLayer = DurableAgentRuntime.layerWithBindings(currentBindings).pipe(
          Layer.provide(
            Layer.succeed(ToolReconciler)({
              reconcile: (evidence) => {
                reviewed.push(evidence);

                return Effect.succeed(
                  proof === "NeverStarted"
                    ? ReconciliationNeverStarted.make({})
                    : ReconciliationSafeToRetry.make({}),
                );
              },
            }),
          ),
        );

        const outcome = yield* DurableAgentRuntime.use((runtime) =>
          runtime.processThreadHead(receipt.threadId),
        ).pipe(Effect.provide(currentLayer));

        expect(yield* Ref.get(starts)).toBe(0);
        expect(reviewed).toMatchObject([
          {
            submissionId: receipt.submissionId,
            toolCallId: "search-1",
            parameters: { query: "sea" },
          },
        ]);
        const after = yield* readLog(receipt.threadId);

        expect(after.slice(0, retained.length)).toEqual(retained);
        const payloads = after.map(({ record }) => record.payload);

        if (proof === "NeverStarted") {
          expect(Option.isSome(outcome) && outcome.value.outcome).toBe("completed");
          expect(payloads.filter((payload) => payload._tag === "ToolCallSettled")).toMatchObject([
            {
              toolCallId: "search-1",
              result: { _tag: "ToolUnavailable", execution: "not-executed" },
            },
          ]);
          expect(scripted.prompts).toHaveLength(2);
        } else {
          expect(Option.isNone(outcome)).toBe(true);
          expect(yield* lookupState(receipt.submissionId)).toBe("unknown");
          expect(payloads.filter((payload) => payload._tag === "ToolCallUnknown")).toMatchObject([
            { toolCallId: "search-1" },
          ]);
          expect(payloads.filter((payload) => payload._tag === "ToolCallSettled")).toEqual([]);
          expect(scripted.prompts).toHaveLength(1);
        }
      }),
    );
  }

  it.effect(
    "recovers a committed external result after its handler has been removed without repeating the effect",
    () =>
      Effect.gen(function* () {
        const committed = yield* Deferred.make<void>();
        const effects = yield* Ref.make(0);
        const finalized = yield* Ref.make(0);
        const tools = Toolkit.make(Search.annotate(ToolExecutionClass, "uncertain"));
        const originalDefinition = { ...searchDefinition, toolkit: tools };
        const currentDefinition = { ...searchDefinition, toolkit: Toolkit.empty };

        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolCallParts
            : finalParts('{"answer":"supplier confirmed the original effect"}'),
        );

        const originalBindings = yield* compileRegistrations([
          {
            agent: Agent.withModel(originalDefinition, scripted.model),
            definitions: { agent: "original", model: "scripted", tools: ["search"] },
          },
        ]).pipe(
          Effect.provide(
            tools.toLayer({
              search: () =>
                Ref.update(effects, (count) => count + 1).pipe(
                  Effect.andThen(Deferred.succeed(committed, undefined)),
                  Effect.andThen(Effect.never),
                  Effect.ensuring(Ref.update(finalized, (count) => count + 1)),
                ),
            }),
          ),
        );

        const currentBindings = yield* compileRegistrations([
          {
            agent: Agent.withModel(currentDefinition, scripted.model),
            definitions: { agent: "current", model: "scripted", tools: [] },
          },
        ]);

        const receipt = yield* Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          const receipt = yield* runtime.submitRegistered(
            { definition: originalDefinition },
            { question: "keep the supplier receipt" },
            submitOptions("continuity-committed-effect", "original"),
          );

          const worker = yield* Effect.forkChild(runtime.processThreadHead(receipt.threadId));

          yield* Deferred.await(committed);
          yield* Fiber.interrupt(worker);

          return receipt;
        }).pipe(Effect.provide(DurableAgentRuntime.layerWithBindings(originalBindings)));

        expect(yield* Ref.get(effects)).toBe(1);
        expect(yield* Ref.get(finalized)).toBe(1);
        const retained = yield* readLog(receipt.threadId);

        expect(retained.filter(({ record }) => record.payload._tag === "ToolCallSettled")).toEqual(
          [],
        );

        const currentLayer = DurableAgentRuntime.layerWithBindings(currentBindings).pipe(
          Layer.provide(
            Layer.succeed(ToolReconciler)({
              reconcile: (evidence) => {
                expect(evidence).toMatchObject({
                  submissionId: receipt.submissionId,
                  runId: runIdForSubmission(receipt.submissionId),
                  toolCallId: "search-1",
                  parameters: { query: "sea" },
                });

                return Effect.succeed(
                  ReconciliationCompleted.make({
                    result: { available: true, supplierReceipt: "external-1" },
                    isFailure: false,
                  }),
                );
              },
            }),
          ),
        );

        const settled = yield* DurableAgentRuntime.use((runtime) =>
          runtime.processThreadHead(receipt.threadId),
        ).pipe(Effect.provide(currentLayer));

        expect(Option.isSome(settled) && settled.value).toMatchObject({
          submissionId: receipt.submissionId,
          outcome: "completed",
        });
        expect(yield* Ref.get(effects)).toBe(1);
        expect(scripted.prompts).toHaveLength(2);
        expect(JSON.stringify(scripted.prompts[1])).toContain("external-1");
        const after = yield* readLog(receipt.threadId);

        expect(after.slice(0, retained.length)).toEqual(retained);
        expect(
          after
            .filter(({ record }) => record.payload._tag === "ToolCallSettled")
            .map(({ record }) => record.payload),
        ).toMatchObject([
          {
            toolCallId: "search-1",
            isFailure: false,
            result: { available: true, supplierReceipt: "external-1" },
          },
        ]);
      }),
  );
});

// Native turn-boundary yielding retained the active FIFO head:
// https://github.com/danieljvdm/effect-agent/commit/2259fc05eec3bfac2a92a8d055953f3482e54735
layer(testLayer)("independent input scheduling", (it) => {
  {
    const origin = "human" as const;

    for (const mode of ["stop", "interrupted-claim"] as const)
      it.effect(`handles ${mode} at a complete boundary after ${origin} work`, () =>
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const control = yield* DurableRuntimeFailpointTestControl;
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let inspections = 0;
          let openAttempts = 0;
          let activeSubmission: SubmissionId | undefined;

          const turns: Array<{
            question: string;
            submissionId: SubmissionId | undefined;
            at: number;
          }> = [];

          const authorizations: Array<{ runId: string; question: string }> = [];
          const thread = `handoff-${origin}-${mode}`;
          const initialPrincipal = Principal.make("human-one");
          const input = Schema.Struct({ question: Schema.String });

          const definition = Agent.make("main-separate-replies", {
            input,
            output: Schema.Struct({ answer: Schema.String }),
            instructions: ({ question }) => question,
            toolkit: searchTools,
            policy: { maxTurns: 5, maxToolCalls: 4, maxDuration: "30 seconds" },
          });

          const scripted = Model.make(
            "scripted",
            "input-handoff",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: (request) =>
                  Stream.unwrap(
                    Effect.gen(function* () {
                      const text = request.prompt.content
                        .filter((message) => message.role === "system")
                        .map((message) => JSON.stringify(message.content))
                        .join(" ");

                      const question = text.includes("correction-two")
                        ? "correction-two"
                        : text.includes("correction-one")
                          ? "correction-one"
                          : "old-work";

                      // Interleaved Runs must not replace this Run's latest accepted input:
                      // https://github.com/danieljvdm/effect-agent/commit/8fc53ad9eb6b110ca6faaaebbb6dbba08e3c292f
                      expect(
                        JSON.stringify(
                          request.prompt.content
                            .filter((message) => message.role === "user")
                            .at(-1),
                        ),
                      ).toContain(question);

                      turns.push({
                        question,
                        submissionId: activeSubmission,
                        at: DateTime.toEpochMillis(yield* DateTime.now),
                      });
                      expect(openAttempts).toBe(1);

                      return Stream.fromIterable<Response.StreamPartEncoded>(
                        question === "old-work" && inspections < 3
                          ? [
                              {
                                type: "tool-call",
                                id: `inspect-${inspections}`,
                                name: "search",
                                params: { query: question },
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : finalParts(JSON.stringify({ answer: question })),
                      );
                    }),
                  ),
              }),
            ),
          );

          const agent = Agent.withModel(definition, scripted);

          const binding = yield* DurableWorkerBinding.make(agent, DIGESTS).pipe(
            Effect.provide(
              searchTools.toLayer({
                search: () =>
                  Effect.gen(function* () {
                    inspections++;
                    if (inspections === 1) {
                      yield* Deferred.succeed(entered, undefined);
                      yield* Deferred.await(release);
                    }
                    yield* TestClock.adjust(100);

                    return { available: true };
                  }),
              }),
            ),
          );

          const tracked: ResolvedBinding = {
            ...binding,
            attempt: (driver, threadId, claim) =>
              Effect.acquireUseRelease(
                Effect.sync(() => {
                  expect(openAttempts).toBe(0);
                  openAttempts++;
                  activeSubmission = claim.submissionId;
                }),
                () => binding.attempt(driver, threadId, claim),
                () =>
                  Effect.sync(() => {
                    openAttempts--;
                    activeSubmission = undefined;
                  }),
              ),
          };

          const runtimeLayer = DurableAgentRuntime.layerWithBindings([tracked]).pipe(
            Layer.provide(
              Layer.succeed(SubmissionScheduling, {
                yieldTo: ({ next }) => Effect.succeed(next.principal === "human-one"),
              }),
            ),
            Layer.provide(
              Layer.succeed(SubmissionLedger, {
                ...ledger,
                claimJoining: () => Effect.succeed([]),
              }),
            ),
            Layer.provide(
              Layer.succeed(RunToolAuthorization, {
                authorize: (request) =>
                  Effect.sync(() => {
                    const admitted = Schema.decodeUnknownSync(input)(request.input);

                    authorizations.push({ runId: request.runId, question: admitted.question });
                    if (activeSubmission === undefined)
                      throw new Error("Missing active submission");
                    expect(request.runId).toBe(runIdForSubmission(activeSubmission));

                    return { _tag: "allowed" as const };
                  }),
              }),
            ),
          );

          let first: Receipt;
          let second: Receipt;
          let third: Receipt;
          let admittedAt = 0;

          yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;

            first = yield* runtime.submit(
              agent,
              { question: "old-work" },
              {
                ...submitOptions(thread, "first"),
                principal: initialPrincipal,
              },
            );

            const worker = yield* Effect.forkChild(
              Effect.exit(runtime.processThreadHead(first.threadId)),
            );

            yield* Deferred.await(entered);

            const secondOptions = {
              ...submitOptions(thread, "second"),
              principal: Principal.make("human-one"),
            };

            second = yield* runtime.submit(agent, { question: "correction-one" }, secondOptions);
            admittedAt = DateTime.toEpochMillis(yield* DateTime.now);
            third = yield* runtime.submit(
              agent,
              { question: "correction-two" },
              {
                ...submitOptions(thread, "third"),
                principal: Principal.make("human-one"),
              },
            );
            expect(
              yield* runtime.submit(agent, { question: "correction-one" }, secondOptions),
            ).toEqual(second);
            expect(turns).toHaveLength(1);
            if (mode === "interrupted-claim") yield* armFailpoint("claim:after-claim");
            yield* Deferred.succeed(release, undefined);
            const result = yield* Fiber.join(worker);

            expect(openAttempts).toBe(0);
            if (mode === "interrupted-claim") {
              expect(failureTag(result)).toBe("DurableRuntimeFailpointError");
              yield* control.clear;
            } else {
              expect(Exit.isSuccess(result)).toBe(true);
              expect(turns.map(({ question }) => question)).toEqual(["old-work", "correction-one"]);
              if (mode === "stop")
                yield* runtime.abort(
                  AbortCommand.make({
                    submissionId: first.submissionId,
                    author: "human-one",
                    reason: "stop old work",
                  }),
                );
            }
          }).pipe(Effect.provide(runtimeLayer));
          // Rebuild the runtime after the handoff/claim interruption; every input and receipt
          // comes from the original ledger, with no retained scheduling hint or ambient authority.
          yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;

            yield* runtime.processThreadResolved(first.threadId);
            const records = yield* readLog(thread);

            expect(
              records
                .filter(({ record }) => record.payload._tag === "UserInputRecorded")
                .map(({ record }) =>
                  record.payload._tag === "UserInputRecorded"
                    ? record.payload.submissionId
                    : undefined,
                ),
            ).toEqual([first.submissionId, second.submissionId, third.submissionId]);
            expect(
              records.filter(({ record }) => record.payload._tag === "RunStarted"),
            ).toHaveLength(3);
            expect(
              records.filter(({ record }) => record.payload._tag === "SubmissionSettled"),
            ).toHaveLength(3);
            for (const [receipt, question] of [
              [first, "old-work"],
              [second, "correction-one"],
              [third, "correction-two"],
            ] as const) {
              const snapshot = yield* ledger.loadRecoverySnapshot(
                RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
              );

              expect(snapshot.ownership).toBeUndefined();
              expect(snapshot.submission.receiptId).toBe(receipt.receiptId);
              expect(snapshot.submission.state).toBe("settled");
              expect(snapshot.submission.principal).toBe(
                receipt === first ? initialPrincipal : "human-one",
              );
              const settled = yield* runtime.awaitSettlement(receipt);

              expect(settled.outcome).toBe(
                mode === "stop" && receipt === first ? "aborted" : "completed",
              );
              if (settled.outcome === "completed") {
                const completion = records.find(
                  ({ record }) =>
                    record.payload._tag === "RunCompleted" &&
                    record.payload.runId === runIdForSubmission(receipt.submissionId),
                );

                expect(completion?.record.payload).toMatchObject({ output: { answer: question } });
              }
            }

            const correction = turns.find(
              ({ submissionId }) => submissionId === second.submissionId,
            );

            expect(correction?.at).toBe(admittedAt + 100);
            expect(turns.slice(0, 3).map(({ question }) => question)).toEqual([
              "old-work",
              "correction-one",
              "correction-two",
            ]);
            expect(inspections).toBe(mode === "stop" ? 1 : 3);
            expect(authorizations).toEqual(
              Array.from({ length: inspections }, () => ({
                runId: runIdForSubmission(first.submissionId),
                question: "old-work",
              })),
            );
            expect(openAttempts).toBe(0);
          }).pipe(Effect.provide(runtimeLayer));
        }),
      );
  }
});

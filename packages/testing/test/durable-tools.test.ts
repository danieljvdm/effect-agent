import { Agent, AgentPolicy, ConversationId, SubmissionId, ToolCallId } from "@effect-agent/core";
import {
  DurableStep,
  DurableStepError,
  RunContextPreparation,
  ToolExecutionClass,
  toolFailureObserverLayer,
  type ToolFailureObservation,
  type RunToolAuthorizationDecision,
  type RunToolAuthorizationRequest,
} from "@effect-agent/engine";
import {
  type CanonicalRecordEnvelope,
  AbortCommand,
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
  Principal,
  ProducerId,
  ReconciliationCompleted,
  ReconciliationSafeToRetry,
  ReconciliationUncertain,
  ResolutionAbortSubmission,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  SubmissionLedger,
  SubmissionLookupById,
  ToolReconciler,
  UnknownResolutionCommand,
  modelResponseInterruptedRecordId,
  modelResponseRecordId,
  promptFromCanonicalRecords,
  runIdForSubmission,
  toolCallPreparedRecordId,
  toolStepSettledRecordId,
  type DurableRuntimeFailpointLocation,
  type DurableSubmitOptions,
  type PreparedToolCallEvidence,
  type ReconciliationDecision,
  type SettlementConflict,
  type UnknownResolutionConflict,
  WakeScheduler,
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import {
  type Prompt,
  LanguageModel,
  Model,
  Tool,
  Toolkit,
  type Response,
} from "effect/unstable/ai";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-durable-tools");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodeSubmissionId = Schema.decodeSync(SubmissionId);

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

const toolCall = (id: string, name: string, params: unknown): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
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
      "durable-tools-test",
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

const policy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 4,
  maxDuration: "30 seconds",
  toolConcurrency: 2,
});

/** Unannotated → fail-closed `uncertain`: enters the prepared/settled protocol. */
const Book = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});
const bookTools = Toolkit.make(Book);
const bookDefinition = Agent.define("durable-book", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book it.",
  toolkit: bookTools,
  policy,
});

/** The declared external idempotency contract: recovery may re-execute without proof. */
const BookIdempotent = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
}).annotate(ToolExecutionClass, "idempotent");
const bookIdempotentTools = Toolkit.make(BookIdempotent);
const bookIdempotentDefinition = Agent.define("durable-book-idempotent", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book it idempotently.",
  toolkit: bookIdempotentTools,
  policy,
});

/** Readonly search: no external mutation, never enters the prepared/settled protocol. */
const Search = Tool.make("search", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ available: Schema.Boolean }),
}).annotate(ToolExecutionClass, "readonly");
const searchTools = Toolkit.make(Search);
const searchDefinition = Agent.define("durable-tools-search", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Search before answering.",
  toolkit: searchTools,
  policy,
});
const searchToolLayer = searchTools.toLayer({
  search: () => Effect.succeed({ available: true }),
});

/** Mixed batch: one readonly + one uncertain application call in a single Turn. */
const mixedTools = Toolkit.make(Search, Book);
const mixedDefinition = Agent.define("durable-tools-mixed", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Search, then book.",
  toolkit: mixedTools,
  policy,
});

/** Durable Tool: declaring `DurableStep` as a dependency is what makes it durable. */
const Itinerary = Tool.make("itinerary", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ state: Schema.String }),
  failure: DurableStepError,
  dependencies: [DurableStep],
});
const itineraryTools = Toolkit.make(Itinerary);
const itineraryDefinition = Agent.define("durable-itinerary", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Reserve the itinerary.",
  toolkit: itineraryTools,
  policy,
});

/** Per-ref supplier call counters that survive Tool-Layer rebuilds across Attempts. */
const makeBookDesk = (tools: typeof bookTools | typeof bookIdempotentTools) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make<ReadonlyMap<string, number>>(new Map());
    const toolLayer = tools.toLayer({
      book: ({ ref }) =>
        Ref.update(calls, (current) => new Map(current).set(ref, (current.get(ref) ?? 0) + 1)).pipe(
          Effect.as({ confirmation: `confirmed-${ref}` }),
        ),
    });
    const count = (ref: string) => Ref.get(calls).pipe(Effect.map((m) => m.get(ref) ?? 0));
    return { toolLayer, count };
  });

const makeItineraryDesk = Effect.gen(function* () {
  const entries = yield* Ref.make(0);
  const flightRuns = yield* Ref.make(0);
  const lodgingRuns = yield* Ref.make(0);
  const toolLayer = itineraryTools.toLayer({
    itinerary: ({ ref }) =>
      Effect.gen(function* () {
        yield* Ref.update(entries, (n) => n + 1);
        const step = yield* DurableStep;
        const flight = yield* step.do(
          "reserve-flight",
          Schema.String,
          Ref.update(flightRuns, (n) => n + 1).pipe(Effect.as(`flight-${ref}`)),
        );
        const lodging = yield* step.do(
          "reserve-lodging",
          Schema.String,
          Ref.update(lodgingRuns, (n) => n + 1).pipe(Effect.as(`lodging-${ref}`)),
        );
        return { state: `${flight}+${lodging}` };
      }),
  });
  return {
    toolLayer,
    entries: Ref.get(entries),
    flightRuns: Ref.get(flightRuns),
    lodgingRuns: Ref.get(lodgingRuns),
  };
});

/** Test control replacing the reconciliation policy per test (default: fail-closed Uncertain). */
class ReconcilerTestControl extends Context.Service<
  ReconcilerTestControl,
  {
    readonly set: (
      decide: (evidence: PreparedToolCallEvidence) => ReconciliationDecision,
    ) => Effect.Effect<void>;
    readonly reset: Effect.Effect<void>;
    readonly consultations: Effect.Effect<number>;
  }
>()("@effect-agent/testing/ReconcilerTestControl") {}

const uncertainDefault = (): ReconciliationDecision =>
  ReconciliationUncertain.make({ reason: "test default: no proof either way" });

const reconcilerTestLayer = Layer.effectContext(
  Effect.gen(function* () {
    const handler =
      yield* Ref.make<(evidence: PreparedToolCallEvidence) => ReconciliationDecision>(
        uncertainDefault,
      );
    const consulted = yield* Ref.make(0);
    return Context.make(
      ToolReconciler,
      ToolReconciler.of({
        reconcile: (evidence) =>
          Ref.update(consulted, (n) => n + 1).pipe(
            Effect.andThen(Ref.get(handler)),
            Effect.map((decide) => decide(evidence)),
          ),
      }),
    ).pipe(
      Context.add(
        ReconcilerTestControl,
        ReconcilerTestControl.of({
          set: (decide) => Ref.set(handler, decide),
          reset: Ref.set(handler, uncertainDefault).pipe(Effect.andThen(Ref.set(consulted, 0))),
          consultations: Ref.get(consulted),
        }),
      ),
    );
  }),
);

/** Host action-authorization control (default: allow) captured by the existing run-context hook. */
class ToolAuthorizationTestControl extends Context.Service<
  ToolAuthorizationTestControl,
  {
    readonly set: (
      decide: (request: RunToolAuthorizationRequest) => RunToolAuthorizationDecision,
    ) => Effect.Effect<void>;
    readonly reset: Effect.Effect<void>;
    readonly requests: Effect.Effect<ReadonlyArray<RunToolAuthorizationRequest>>;
  }
>()("@effect-agent/testing/ToolAuthorizationTestControl") {}

const allowToolExecution = (): RunToolAuthorizationDecision => ({ _tag: "allowed" });

const toolAuthorizationTestLayer = Layer.effectContext(
  Effect.gen(function* () {
    const policy =
      yield* Ref.make<(request: RunToolAuthorizationRequest) => RunToolAuthorizationDecision>(
        allowToolExecution,
      );
    const requests = yield* Ref.make<ReadonlyArray<RunToolAuthorizationRequest>>([]);
    return Context.make(
      RunContextPreparation,
      RunContextPreparation.of({
        toolAuthorization: {
          authorize: (request) =>
            Ref.update(requests, (all) => [...all, request]).pipe(
              Effect.andThen(Ref.get(policy)),
              Effect.map((decide) => decide(request)),
            ),
        },
      }),
    ).pipe(
      Context.add(
        ToolAuthorizationTestControl,
        ToolAuthorizationTestControl.of({
          set: (decide) => Ref.set(policy, decide),
          reset: Ref.set(policy, allowToolExecution).pipe(Effect.andThen(Ref.set(requests, []))),
          requests: Ref.get(requests),
        }),
      ),
    );
  }),
);

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-durable-tools"),
  producerId: Schema.decodeSync(ProducerId)("producer-durable-tools"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryConversationStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layerTest,
  reconcilerTestLayer,
  toolAuthorizationTestLayer,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layerWithContext.pipe(Layer.provideMerge(baseLayer));

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

const resetReconciler = Effect.gen(function* () {
  const control = yield* ReconcilerTestControl;
  yield* control.reset;
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

layer(testLayer)("DUR P5 durable Tools (prepared/settled, reconciliation, unknown)", (it) => {
  it.effect("an ordinary delegate_export is never replayed after its external effect", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      yield* clearFailpoint;
      const runtime = yield* DurableAgentRuntime;
      const tool = Tool.make("delegate_export", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const toolkit = Toolkit.make(tool);
      const definition = Agent.define("ordinary-export", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Export.",
        toolkit,
        policy,
      });
      const scripted = yield* makeScriptedModel(() =>
        toolTurn(toolCall("export-1", "delegate_export", {})),
      );
      const agent = Agent.withModel(definition, scripted.model);
      const starts = yield* Ref.make(0);
      const acted = yield* Deferred.make<void>();
      const handlers = toolkit.toLayer({
        delegate_export: () =>
          Ref.update(starts, (n) => n + 1).pipe(
            Effect.andThen(Deferred.succeed(acted, undefined)),
            Effect.andThen(Effect.never),
          ),
      });
      // Lose ownership after the external action, before the result batch can become canonical.
      const receipt = yield* runtime.submit(
        agent,
        "export",
        submitOptions("ordinary-delegate-export", "export"),
      );
      const attempt = yield* Effect.forkChild(
        runtime.processConversation(agent, receipt.conversationId).pipe(Effect.provide(handlers)),
      );
      yield* Deferred.await(acted);
      yield* Fiber.interrupt(attempt);
      expect(yield* Ref.get(starts)).toBe(1);
      yield* runtime.runRecovery;
      expect(
        yield* runtime
          .processConversation(agent, receipt.conversationId)
          .pipe(Effect.provide(handlers)),
      ).toEqual([]);
      expect(yield* Ref.get(starts)).toBe(1);
      expect(yield* lookupState(receipt.submissionId)).toBe("unknown");
      const records = yield* readLog(receipt.conversationId);
      expect(
        records.find(({ record }) => record.payload._tag === "ToolCallPrepared")?.record.payload,
      ).toMatchObject({ executionKind: "ordinary" });
      expect(logTags(records)).toContain("ToolCallUnknown");
      expect(logTags(records)).not.toContain("SubagentRequested");
    }),
  );
  it.effect(
    "RUN-036 captures the host observer for fresh and replacement Attempts and skips settled replay",
    () => {
      const observations: Array<ToolFailureObservation> = [];
      const ambient: Array<ToolFailureObservation> = [];
      const observerLayer = toolFailureObserverLayer({
        observe: (observation) =>
          Effect.sync(() => {
            observations.push(observation);
          }),
      });
      const ambientLayer = toolFailureObserverLayer({
        observe: (observation) =>
          Effect.sync(() => {
            ambient.push(observation);
          }),
      });
      const Failed = Tool.make("failed", {
        parameters: Schema.Struct({ ref: Schema.String }),
        success: Schema.String,
        failure: Schema.Struct({ _tag: Schema.Literal("LookupFailure"), message: Schema.String }),
        failureMode: "return",
      });
      const tools = Toolkit.make(Failed);
      const definition = Agent.define("durable-observed-failure", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Use the lookup, then answer.",
        toolkit: tools,
        policy,
      });
      const starts: Array<string> = [];
      const handlers = tools.toLayer({
        failed: ({ ref }) =>
          Effect.sync(() => {
            starts.push(ref);
          }).pipe(
            Effect.andThen(
              Effect.fail({ _tag: "LookupFailure" as const, message: "lookup unavailable" }),
            ),
          ),
      });
      return Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const freshModel = yield* makeScriptedModel((n) =>
          n === 0
            ? toolTurn(toolCall("fresh", "failed", { ref: "fresh" }))
            : finalParts('{"answer":"fallback"}'),
        );
        const fresh = Agent.withModel(definition, freshModel.model);
        yield* runtime.submit(
          fresh,
          { question: "lookup" },
          submitOptions("observer-fresh", "fresh"),
        );
        const completed = yield* runtime
          .processConversation(fresh, decodeConversationId("observer-fresh"))
          .pipe(Effect.provide(Layer.merge(handlers, ambientLayer)));
        expect(completed[0]?.outcome).toBe("completed");
        expect(
          observations.map((value) =>
            value._tag === "ModelToolFailure" ? value.toolCallId : undefined,
          ),
        ).toEqual(["fresh"]);

        const replacementModel = yield* makeScriptedModel((n) =>
          n === 0
            ? toolTurn(
                toolCall("settled", "failed", { ref: "settled" }),
                toolCall("open", "failed", { ref: "open" }),
              )
            : finalParts('{"answer":"recovered"}'),
        );
        const replacement = Agent.withModel(definition, replacementModel.model);
        const conversationId = decodeConversationId("observer-replacement");
        const receipt = yield* runtime.submit(
          replacement,
          { question: "lookup both" },
          submitOptions(conversationId, "replacement"),
        );
        yield* armFailpoint("tools:after-prepared-append");
        const interrupted = yield* runtime
          .processConversation(replacement, conversationId)
          .pipe(Effect.provide(Layer.merge(handlers, ambientLayer)), Effect.exit);
        expect(failureTag(interrupted)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        yield* runtime.runRecovery;
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("settled"),
            author: "operator",
            reason: "Recovered external failure",
            resolution: ResolutionCompletedWithResult.make({
              result: { _tag: "LookupFailure", message: "already settled" },
              isFailure: true,
            }),
          }),
        );
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("open"),
            author: "operator",
            reason: "No Handler started",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
        const resumed = yield* runtime
          .processConversation(replacement, conversationId)
          .pipe(Effect.provide(Layer.merge(handlers, ambientLayer)));
        expect(resumed[0]?.outcome).toBe("completed");
        expect(starts).toEqual(["fresh", "open"]);
        expect(
          observations.map((value) =>
            value._tag === "ModelToolFailure" ? value.toolCallId : undefined,
          ),
        ).toEqual(["fresh", "open"]);
        expect(ambient).toEqual([]);
        expect(
          (yield* readLog(conversationId)).filter(
            (envelope) => envelope.record.payload._tag === "ToolCallSettled",
          ),
        ).toHaveLength(2);
      }).pipe(
        Effect.provide(
          DurableAgentRuntime.layerWithContext.pipe(
            Layer.provideMerge(baseLayer),
            Layer.provide(observerLayer),
          ),
          { local: true },
        ),
      );
    },
  );

  it.effect("RUN-036 captured absence ignores an ambient worker observer", () =>
    Effect.gen(function* () {
      const observations: Array<ToolFailureObservation> = [];
      const Failed = Tool.make("failed", {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: Schema.String,
        failureMode: "return",
      });
      const tools = Toolkit.make(Failed);
      const scripted = yield* makeScriptedModel((n) =>
        n === 0 ? toolTurn(toolCall("failed", "failed", {})) : finalParts('{"answer":"fallback"}'),
      );
      const agent = Agent.withModel(
        Agent.define("observer-absent", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Try the lookup.",
          toolkit: tools,
          policy,
        }),
        scripted.model,
      );
      const runtime = yield* DurableAgentRuntime;
      yield* runtime.submit(
        agent,
        { question: "lookup" },
        submitOptions("observer-absent", "absent"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId("observer-absent"))
        .pipe(
          Effect.provide(
            Layer.merge(
              tools.toLayer({ failed: () => Effect.fail("unavailable") }),
              toolFailureObserverLayer({
                observe: (observation) =>
                  Effect.sync(() => {
                    observations.push(observation);
                  }),
              }),
            ),
          ),
        );
      expect(settlements[0]?.outcome).toBe("completed");
      expect(observations).toEqual([]);
    }),
  );

  it.effect("splits a tool Turn into response, prepared, and results commits", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk(bookTools);
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-1" }))
          : finalParts('{"answer":"booked"}'),
      );
      const agent = Agent.withModel(bookDefinition, scripted.model);
      const conversation = "conversation-split-commits";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "split-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-1")).toBe(1);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      expect(logTags(records)).toEqual([
        "ConversationCreated",
        "UserInputRecorded",
        "RunStarted",
        "ModelResponseRecorded",
        "ToolCallPrepared",
        "ToolCallSettled",
        "ModelResponseRecorded",
        "RunCompleted",
        "SubmissionSettled",
      ]);
      const byId = new Map(
        records.map((envelope) => [envelope.record.recordId as string, envelope]),
      );
      // Stable record identities across the split commits.
      expect(byId.has(`model-response:${runId}:1`)).toBe(true);
      expect(byId.has(`tool-prepared:${runId}:1:book-1`)).toBe(true);
      expect(byId.has(`tool-settled:${runId}:1:book-1`)).toBe(true);
      // Stable batch identities: response, prepared, and results commit separately; the no-tool
      // final Turn keeps the P4 single-batch identity.
      expect(byId.get(`model-response:${runId}:1`)?.batchId).toBe(`turn-response:${runId}:1`);
      expect(byId.get(`tool-prepared:${runId}:1:book-1`)?.batchId).toBe(`turn-prepared:${runId}:1`);
      expect(byId.get(`tool-settled:${runId}:1:book-1`)?.batchId).toBe(`turn-results:${runId}:1`);
      expect(byId.get(`model-response:${runId}:2`)?.batchId).toBe(`turn:${runId}:2`);
      const prepared = byId.get(`tool-prepared:${runId}:1:book-1`)?.record.payload;
      if (prepared?._tag === "ToolCallPrepared") {
        expect(prepared.parameters).toEqual({ ref: "r-1" });
        expect(prepared.toolName).toBe("book");
      }

      // The canonical journal rebuilds the next-Run prompt without replaying the prior Run's
      // instruction and wake prefix.
      const prompt = yield* promptFromCanonicalRecords(records);
      expect(prompt.content.map((message) => message.role)).toEqual([
        "assistant",
        "tool",
        "assistant",
      ]);
    }),
  );

  it.effect("readonly toolkits never produce prepared records (P4 parity)", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("search-1", "search", { query: "sea" }))
          : finalParts('{"answer":"found"}'),
      );
      const agent = Agent.withModel(searchDefinition, scripted.model);
      const conversation = "conversation-readonly-parity";

      const receipt = yield* runtime.submit(
        agent,
        { question: "find it" },
        submitOptions(conversation, "readonly-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(searchToolLayer));
      expect(settlements[0]?.outcome).toBe("completed");

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      expect(logTags(records)).toEqual([
        "ConversationCreated",
        "UserInputRecorded",
        "RunStarted",
        "ModelResponseRecorded",
        "ToolCallSettled",
        "ModelResponseRecorded",
        "RunCompleted",
        "SubmissionSettled",
      ]);
      // The response/results split still applies (application calls exist), only preparation
      // is skipped for the readonly class.
      const settled = records.find(
        (envelope) => envelope.record.payload._tag === "ToolCallSettled",
      );
      expect(settled?.batchId).toBe(`turn-results:${runId}:1`);
    }),
  );

  it.effect("a mixed batch prepares only the non-readonly calls", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk(bookTools);
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(
              toolCall("search-1", "search", { query: "sea" }),
              toolCall("book-1", "book", { ref: "r-mixed" }),
            )
          : finalParts('{"answer":"mixed"}'),
      );
      const agent = Agent.withModel(mixedDefinition, scripted.model);
      const conversation = "conversation-mixed-batch";

      const receipt = yield* runtime.submit(
        agent,
        { question: "search and book" },
        submitOptions(conversation, "mixed-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(Layer.mergeAll(searchToolLayer, desk.toolLayer)));
      expect(settlements[0]?.outcome).toBe("completed");

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      const preparedIds = records
        .filter((envelope) => envelope.record.payload._tag === "ToolCallPrepared")
        .map((envelope) => envelope.record.recordId);
      expect(preparedIds).toEqual([`tool-prepared:${runId}:1:book-1`]);
      const settledBatchIds = records
        .filter((envelope) => envelope.record.payload._tag === "ToolCallSettled")
        .map((envelope) => envelope.batchId);
      expect(settledBatchIds).toEqual([`turn-results:${runId}:1`, `turn-results:${runId}:1`]);
    }),
  );

  it.effect(
    "resumes a declared batch after a response-boundary kill without re-invoking the model",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookTools);
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-resume" }))
            : finalParts('{"answer":"resumed"}'),
        );
        const agent = Agent.withModel(bookDefinition, scripted.model);
        const conversation = "conversation-response-kill";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(conversation, "response-kill-1"),
        );
        yield* armFailpoint("turn:after-response-append");
        const killed = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(desk.toolLayer)),
        );
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        // The provably-safe window (durability §15): response canonical, nothing prepared,
        // nothing executed.
        const runId = runIdForSubmission(receipt.submissionId);
        const committed = yield* readLog(conversation);
        expect(logTags(committed)).toEqual([
          "ConversationCreated",
          "UserInputRecorded",
          "RunStarted",
          "ModelResponseRecorded",
        ]);
        expect(yield* desk.count("r-resume")).toBe(0);

        const reports = yield* runtime.runRecovery;
        const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
        expect(report?.decision._tag).toBe("ResumePendingToolBatch");
        expect(report?.disposition).toBe("deferred");

        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer));
        expect(settlements[0]?.outcome).toBe("completed");

        // No model re-invocation for the declared Turn: one call declared it, one call answered
        // the next Turn — and exactly one ModelResponseRecorded exists for Turn 1.
        expect(scripted.prompts).toHaveLength(2);
        expect(yield* desk.count("r-resume")).toBe(1);
        const records = yield* readLog(conversation);
        expect(
          records.filter(
            (envelope) => envelope.record.recordId === modelResponseRecordId(runId, 1),
          ),
        ).toHaveLength(1);
        // The resumed batch replayed the prepared commit before executing.
        expect(records.map((envelope) => envelope.record.recordId)).toContain(
          toolCallPreparedRecordId(runId, 1, decodeToolCallId("book-1")),
        );
        // A batch resume never re-invokes the model for the pending Turn, so no interruption
        // audit is recorded for it.
        expect(records.map((envelope) => envelope.record.recordId)).not.toContain(
          modelResponseInterruptedRecordId(runId, 1),
        );
      }),
  );

  it.effect(
    "a resumed declared batch re-delivers the pending Turn's leading messages to the next model request",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookIdempotentTools);
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-leading" }))
            : finalParts('{"answer":"resumed"}'),
        );
        const agent = Agent.withModel(bookIdempotentDefinition, scripted.model);
        const conversation = "conversation-resume-leading";

        yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(conversation, "leading-1"),
        );
        yield* armFailpoint("tools:after-prepared-append");
        const killed = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(desk.toolLayer)),
        );
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer));
        expect(settlements[0]?.outcome).toBe("completed");
        expect(scripted.prompts).toHaveLength(2);

        // WP1 `resume.leadingMessages` (task #12): the pending Turn's canonical response record
        // carries the Turn-1 evaluated instructions + input BEFORE its assistant tool-call
        // message; the resumed Attempt's canonical prompt boundary excludes the pending Turn
        // entirely, so without the threaded leading messages the next model request would open
        // with a bare assistant message and no instructions or user input at all.
        const resumedRequest = scripted.prompts[1];
        expect(resumedRequest).toBeDefined();
        const roles = (resumedRequest?.content ?? []).map((message) => message.role);
        expect(roles[0]).toBe("system");
        const userIndex = roles.indexOf("user");
        const assistantIndex = roles.indexOf("assistant");
        expect(userIndex).toBeGreaterThanOrEqual(0);
        expect(assistantIndex).toBeGreaterThan(userIndex);
        expect(roles).toContain("tool");
      }),
  );

  it.effect(
    "RUN-031 preserves canonical authority for a later-Turn restart and durable resume",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const authorization = yield* ToolAuthorizationTestControl;
        yield* authorization.reset;
        const runtime = yield* DurableAgentRuntime;
        const handlerWrites = yield* Ref.make<ReadonlyArray<string>>([]);
        const toolLayer = bookIdempotentTools.toLayer({
          book: ({ ref }) =>
            Ref.update(handlerWrites, (writes) => [...writes, ref]).pipe(
              Effect.as({ confirmation: `confirmed-${ref}` }),
            ),
        });
        const nonIdempotentWake = Schema.String.pipe(
          Schema.decode({
            decode: SchemaGetter.transform((value) => value),
            encode: SchemaGetter.transform((value) => `admitted:${value}`),
          }),
        );
        const definition = Agent.define("durable-book-idempotent-canonical-authority", {
          input: Schema.Struct({ question: Schema.String, wake: nonIdempotentWake }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Book it idempotently.",
          toolkit: bookIdempotentTools,
          policy,
        });
        const scripted = yield* makeScriptedModel((call) => {
          switch (call) {
            case 0:
              return toolTurn(toolCall("book-582-turn-1", "book", { ref: "r-turn-1" }));
            case 1:
              return toolTurn(toolCall("book-582-turn-2", "book", { ref: "r-authorized" }));
            default:
              return finalParts('{"answer":"resumed"}');
          }
        });
        const agent = Agent.withModel(definition, scripted.model);
        const conversation = "conversation-tool-authorization-resume";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it", wake: "wake-582" },
          submitOptions(conversation, "tool-authorization-resume-1"),
        );

        // Commit Turn 1 and interrupt after its results boundary. No batch remains pending, so the
        // replacement engine really restarts its local Turn counter at 1.
        yield* armFailpoint("turn:after-results-append");
        const turnOneInterrupted = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(toolLayer)),
        );
        expect(failureTag(turnOneInterrupted)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(handlerWrites)).toEqual(["r-turn-1"]);
        expect(yield* authorization.requests).toHaveLength(1);
        yield* clearFailpoint;

        // The replacement declares canonical Turn 2 from engine-local Turn 1, authorizes it, and
        // dies after preparation. Its following Attempt resumes that exact durable batch.
        yield* armFailpoint("tools:after-prepared-append");
        const turnTwoInterrupted = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(toolLayer)),
        );
        expect(failureTag(turnTwoInterrupted)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(handlerWrites)).toEqual(["r-turn-1"]);
        expect(yield* authorization.requests).toHaveLength(2);
        yield* clearFailpoint;

        const runId = runIdForSubmission(receipt.submissionId);
        const targetPreparedId = toolCallPreparedRecordId(
          runId,
          2,
          decodeToolCallId("book-582-turn-2"),
        );
        expect(
          (yield* readLog(conversation)).filter(
            (envelope) => envelope.record.recordId === targetPreparedId,
          ),
        ).toHaveLength(1);
        yield* authorization.set(() => ({
          _tag: "denied",
          reason: "the originating task-message wake was superseded before resume",
        }));

        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(toolLayer));
        expect(settlements).toHaveLength(1);
        expect(settlements[0]).toMatchObject({
          outcome: "failed",
          failure: {
            errorTag: "AgentToolAuthorizationDenied",
            message: "the originating task-message wake was superseded before resume",
          },
        });
        expect(yield* Ref.get(handlerWrites)).toEqual(["r-turn-1"]);

        const requests = yield* authorization.requests;
        expect(requests).toHaveLength(3);
        expect(requests.map((request) => request.turn)).toEqual([1, 2, 2]);
        const freshTurnTwo = requests[1];
        const resumedTurnTwo = requests[2];
        expect(freshTurnTwo).toBeDefined();
        expect(resumedTurnTwo).toEqual(freshTurnTwo);
        expect(freshTurnTwo).toMatchObject({
          conversationId: conversation,
          turn: 2,
          input: { question: "book it", wake: "admitted:wake-582" },
          call: {
            toolCallId: "book-582-turn-2",
            toolName: "book",
            parameters: { ref: "r-authorized" },
            executionClass: "idempotent",
          },
        });
        expect(freshTurnTwo?.runId).toBeDefined();
        expect(freshTurnTwo?.turnId).toBeDefined();
        expect(scripted.prompts).toHaveLength(2);

        const records = yield* readLog(conversation);
        expect(
          records.filter((envelope) => envelope.record.recordId === targetPreparedId),
        ).toHaveLength(1);
        expect(
          records.filter((envelope) => envelope.record.payload._tag === "SubmissionSettled"),
        ).toHaveLength(1);

        const replay = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(toolLayer));
        expect(replay).toEqual([]);
        expect(yield* Ref.get(handlerWrites)).toEqual(["r-turn-1"]);
        expect(yield* authorization.requests).toHaveLength(3);
        yield* authorization.reset;
      }),
  );

  it.effect("settles a denied action failed without preparation, handler writes, or retry", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const authorization = yield* ToolAuthorizationTestControl;
      yield* authorization.reset;
      yield* authorization.set(() => ({
        _tag: "denied",
        reason: "the originating task-message wake was superseded",
      }));
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk(bookTools);
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-denied-582", "book", { ref: "r-denied" }))
          : finalParts('{"answer":"unreachable"}'),
      );
      const agent = Agent.withModel(bookDefinition, scripted.model);
      const conversation = "conversation-tool-authorization-denied";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "tool-authorization-denied-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements).toHaveLength(1);
      expect(settlements[0]).toMatchObject({
        outcome: "failed",
        failure: {
          errorTag: "AgentToolAuthorizationDenied",
          message: "the originating task-message wake was superseded",
        },
      });
      expect(yield* desk.count("r-denied")).toBe(0);

      const requests = yield* authorization.requests;
      expect(requests).toHaveLength(1);
      expect(requests[0]?.call).toEqual({
        toolCallId: "book-denied-582",
        toolName: "book",
        parameters: { ref: "r-denied" },
        executionClass: "uncertain",
        executionKind: "ordinary",
      });
      const records = yield* readLog(conversation);
      expect(logTags(records)).toEqual([
        "ConversationCreated",
        "UserInputRecorded",
        "RunStarted",
        "ModelResponseRecorded",
        "SubmissionSettled",
      ]);
      expect(records.some((envelope) => envelope.record.payload._tag === "ToolCallPrepared")).toBe(
        false,
      );

      const settled = yield* runtime.awaitSettlement(receipt);
      expect(settled).toMatchObject({
        outcome: "failed",
        failure: { errorTag: "AgentToolAuthorizationDenied" },
      });
      const replay = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(desk.toolLayer));
      expect(replay).toEqual([]);
      expect(yield* desk.count("r-denied")).toBe(0);
      expect(yield* authorization.requests).toHaveLength(1);
      yield* authorization.reset;
    }),
  );

  it.effect(
    "a prepared call without a settled record marks unknown under the default reconciler and frees the worker permit",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookTools);
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-unknown" }))
            : finalParts('{"answer":"never"}'),
        );
        const agent = Agent.withModel(bookDefinition, scripted.model);
        const conversation = "conversation-mark-unknown";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(conversation, "unknown-1"),
        );
        yield* armFailpoint("tools:after-prepared-append");
        const killed = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(desk.toolLayer)),
        );
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const reports = yield* runtime.runRecovery;
        const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
        expect(report?.decision._tag).toBe("MarkUnknown");
        expect(report?.disposition).toBe("unknown");
        expect(yield* lookupState(receipt.submissionId)).toBe("unknown");

        const runId = runIdForSubmission(receipt.submissionId);
        const records = yield* readLog(conversation);
        expect(records.map((envelope) => envelope.record.recordId)).toContain(
          `tool-unknown:${runId}:1:book-1`,
        );
        // The lane is durably blocked: a worker claim grants nothing and no settlement occurs.
        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer));
        expect(settlements).toEqual([]);
        expect(yield* desk.count("r-unknown")).toBe(0);
        expect(yield* lookupState(receipt.submissionId)).toBe("unknown");

        // resolveUnknown(NeverHappened) reopens the lane; the batch resumes exactly the open call.
        const intent = yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("book-1"),
            author: "operator",
            reason: "the supplier confirmed the call never started",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
        expect(intent.toolCallId).toBe("book-1");
        expect(yield* lookupState(receipt.submissionId)).toBe("input-applied");

        const resumed = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer));
        expect(resumed[0]?.outcome).toBe("completed");
        expect(yield* desk.count("r-unknown")).toBe(1);
        expect(scripted.prompts).toHaveLength(2);

        const finalRecords = yield* readLog(conversation);
        const resolved = finalRecords.find(
          (envelope) => envelope.record.recordId === `tool-resolved:${runId}:1:book-1`,
        )?.record.payload;
        expect(resolved?._tag).toBe("ToolCallResolved");
        if (resolved?._tag === "ToolCallResolved") {
          expect(resolved.resolution).toBe("never-started");
          expect(resolved.author).toBe("operator");
        }
        expect(
          finalRecords.filter(
            (envelope) => envelope.record.recordId === `tool-settled:${runId}:1:book-1`,
          ),
        ).toHaveLength(1);
      }),
  );

  it.effect(
    "resolveUnknown applies recovered results without execution and resumes exactly the open calls",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookTools);
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(
                toolCall("book-1", "book", { ref: "r-a" }),
                toolCall("book-2", "book", { ref: "r-b" }),
              )
            : finalParts('{"answer":"resolved"}'),
        );
        const agent = Agent.withModel(bookDefinition, scripted.model);
        const conversation = "conversation-resolve-two";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book both" },
          submitOptions(conversation, "resolve-two-1"),
        );
        yield* armFailpoint("tools:after-prepared-append");
        const killed = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(desk.toolLayer)),
        );
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        yield* runtime.runRecovery;
        expect(yield* lookupState(receipt.submissionId)).toBe("unknown");

        // book-1 completed externally (recovered supplier truth); book-2 provably never started.
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("book-1"),
            author: "operator",
            reason: "the supplier store shows the booking",
            resolution: ResolutionCompletedWithResult.make({
              result: { confirmation: "external-r-a" },
              isFailure: false,
            }),
          }),
        );
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("book-2"),
            author: "operator",
            reason: "the supplier store shows no attempt",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
        expect(yield* lookupState(receipt.submissionId)).toBe("input-applied");

        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer));
        expect(settlements[0]?.outcome).toBe("completed");
        // Only the open call executed; the resolved result was injected without execution.
        expect(yield* desk.count("r-a")).toBe(0);
        expect(yield* desk.count("r-b")).toBe(1);

        const runId = runIdForSubmission(receipt.submissionId);
        const records = yield* readLog(conversation);
        const settledA = records.find(
          (envelope) => envelope.record.recordId === `tool-settled:${runId}:1:book-1`,
        )?.record.payload;
        if (settledA?._tag === "ToolCallSettled") {
          expect(settledA.result).toEqual({ confirmation: "external-r-a" });
          expect(settledA.isFailure).toBe(false);
        }
        expect(
          records.filter((envelope) => envelope.record.payload._tag === "ToolCallSettled"),
        ).toHaveLength(2);

        // The audit tags stay prompt-transparent: the journal replays one contiguous tool
        // message for the Turn regardless of the late per-call settles, while omitting the
        // prior Run's instruction and wake prefix.
        const prompt = yield* promptFromCanonicalRecords(records);
        expect(prompt.content.map((message) => message.role)).toEqual([
          "assistant",
          "tool",
          "assistant",
        ]);
        const toolMessage = prompt.content.find((message) => message.role === "tool");
        expect(
          toolMessage?.content.filter((part) => part.type === "tool-result").map((part) => part.id),
        ).toEqual(["book-1", "book-2"]);
      }),
  );

  it.effect("a reconciler-recovered result settles canonically without executing the handler", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const runtime = yield* DurableAgentRuntime;
      const control = yield* ReconcilerTestControl;
      const desk = yield* makeBookDesk(bookTools);
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-rec" }))
          : finalParts('{"answer":"recovered"}'),
      );
      const agent = Agent.withModel(bookDefinition, scripted.model);
      const conversation = "conversation-reconciled";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "reconciled-1"),
      );
      yield* armFailpoint("tools:after-prepared-append");
      const killed = yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      yield* control.set(() =>
        ReconciliationCompleted.make({
          result: { confirmation: "recovered-r-rec" },
          isFailure: false,
        }),
      );
      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("MarkUnknown");
      expect(report?.disposition).toBe("repaired");

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      const settled = records.find(
        (envelope) => envelope.record.recordId === `tool-settled:${runId}:1:book-1`,
      )?.record.payload;
      expect(settled?._tag).toBe("ToolCallSettled");
      if (settled?._tag === "ToolCallSettled") {
        expect(settled.result).toEqual({ confirmation: "recovered-r-rec" });
      }
      const resolved = records.find(
        (envelope) => envelope.record.recordId === `tool-resolved:${runId}:1:book-1`,
      )?.record.payload;
      if (resolved?._tag === "ToolCallResolved") {
        expect(resolved.resolution).toBe("completed-with-result");
        expect(resolved.author).toBe("reconciler");
      }
      expect(yield* desk.count("r-rec")).toBe(0);

      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-rec")).toBe(0);
      // The next model request saw the recovered result as the Tool message content.
      const resumedPrompt = scripted.prompts[1];
      const toolMessage = resumedPrompt?.content.find((message) => message.role === "tool");
      const resultPart = toolMessage?.content.find((part) => part.type === "tool-result");
      expect(resultPart?.result).toEqual({ confirmation: "recovered-r-rec" });
    }),
  );

  it.effect(
    "an idempotent-annotated tool re-executes on the worker resume without reconciliation proof",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const control = yield* ReconcilerTestControl;
        const desk = yield* makeBookDesk(bookIdempotentTools);
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-idem" }))
            : finalParts('{"answer":"idem"}'),
        );
        const agent = Agent.withModel(bookIdempotentDefinition, scripted.model);
        const conversation = "conversation-idempotent-retry";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(conversation, "idem-1"),
        );
        yield* armFailpoint("tools:after-prepared-append");
        const killed = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(desk.toolLayer)),
        );
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        // The worker resumes directly: the declared idempotency contract needs no reconciler.
        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer));
        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* desk.count("r-idem")).toBe(1);
        expect(yield* control.consultations).toBe(0);
        const records = yield* readLog(conversation);
        expect(logTags(records)).not.toContain("ToolCallUnknown");
        expect(yield* lookupState(receipt.submissionId)).toBe("settled");
      }),
  );

  it.effect("a canonical settlement beats open tool calls: abort records the uncertainty", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk(bookTools);
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-abort" }))
          : finalParts('{"answer":"never"}'),
      );
      const agent = Agent.withModel(bookDefinition, scripted.model);
      const conversation = "conversation-abort-open";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "abort-open-1"),
      );
      yield* armFailpoint("tools:after-prepared-append");
      const killed = yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "give up on the booking",
        }),
      );
      // Kill the aborting recovery between the canonical settlement append and the ledger
      // finalization: history now carries BOTH the terminal outcome and the open tool call.
      yield* armFailpoint("terminalize:after-canonical-append");
      const killedRecovery = yield* Effect.exit(runtime.runRecovery);
      expect(failureTag(killedRecovery)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // Precedence (plan §4.2): the recorded terminal outcome beats the open tool call — the
      // next pass finalizes the ledger from history instead of re-marking unknown (DUR-015).
      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("FinalizeLedgerFromHistory");
      expect(report?.disposition).toBe("repaired");

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("aborted");
      expect(yield* desk.count("r-abort")).toBe(0);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      const tags = logTags(records);
      // The open call became a canonical Unknown Outcome audit — abort never asserts rollback.
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        `tool-unknown:${runId}:1:book-1`,
      );
      expect(tags.indexOf("ToolCallUnknown")).toBeLessThan(tags.indexOf("SubmissionSettled"));
      expect(tags).not.toContain("ToolCallResolved");

      // The recorded terminal outcome is never revisited: settled work leaves the nonterminal
      // recovery scan entirely, so no later pass can re-mark it.
      const after = yield* runtime.runRecovery;
      expect(after.find((entry) => entry.submissionId === receipt.submissionId)).toBeUndefined();
    }),
  );

  it.effect("resolveUnknown(AbortSubmission) routes into the abort path", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk(bookTools);
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-resabort" }))
          : finalParts('{"answer":"never"}'),
      );
      const agent = Agent.withModel(bookDefinition, scripted.model);
      const conversation = "conversation-resolve-abort";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "resolve-abort-1"),
      );
      yield* armFailpoint("tools:after-prepared-append");
      yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer)),
      );
      yield* clearFailpoint;
      yield* runtime.runRecovery;
      expect(yield* lookupState(receipt.submissionId)).toBe("unknown");

      yield* runtime.resolveUnknown(
        UnknownResolutionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: decodeToolCallId("book-1"),
          author: "operator",
          reason: "unresolvable; abort the submission",
          resolution: ResolutionAbortSubmission.make(),
        }),
      );

      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements[0]?.outcome).toBe("aborted");
      expect(yield* desk.count("r-resabort")).toBe(0);
      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      // The unknown call stays recorded ToolCallUnknown; nothing settles it (durability §13).
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        `tool-unknown:${runId}:1:book-1`,
      );
      expect(records.map((envelope) => envelope.record.recordId)).not.toContain(
        `tool-settled:${runId}:1:book-1`,
      );
    }),
  );

  it.effect(
    "resolveUnknown is idempotent across the intent failpoint and conflicts on divergence",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookTools);
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-idemres" }))
            : finalParts('{"answer":"resolved"}'),
        );
        const agent = Agent.withModel(bookDefinition, scripted.model);
        const conversation = "conversation-resolve-idempotent";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(conversation, "resolve-idem-1"),
        );
        yield* armFailpoint("tools:after-prepared-append");
        yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation))
            .pipe(Effect.provide(desk.toolLayer)),
        );
        yield* clearFailpoint;
        yield* runtime.runRecovery;

        const command = UnknownResolutionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: decodeToolCallId("book-1"),
          author: "operator",
          reason: "the call never started",
          resolution: ResolutionNeverHappened.make(),
        });

        // Kill immediately after the durable intent write: the intent survives, the caller replays.
        yield* armFailpoint("resolve:after-intent");
        const killed = yield* Effect.exit(runtime.resolveUnknown(command));
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const replayed = yield* runtime.resolveUnknown(command);
        expect(replayed.resolution._tag).toBe("NeverHappened");

        // A divergent re-resolution conflicts typed (DUR-017).
        const divergent = yield* Effect.exit(
          runtime.resolveUnknown(
            UnknownResolutionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: decodeToolCallId("book-1"),
              author: "operator",
              reason: "changed my mind",
              resolution: ResolutionCompletedWithResult.make({
                result: { confirmation: "no" },
                isFailure: false,
              }),
            }),
          ),
        );
        expect(failureTag(divergent)).toBe("UnknownResolutionConflict");

        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer));
        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* desk.count("r-idemres")).toBe(1);
      }),
  );

  it.effect("recorded Tool outcomes do not rerun and supersession is audited", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk(bookTools);
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-results" }))
          : finalParts('{"answer":"done"}'),
      );
      const agent = Agent.withModel(bookDefinition, scripted.model);
      const conversation = "conversation-results-kill";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "results-kill-1"),
      );
      yield* armFailpoint("turn:after-results-append");
      const killed = yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      expect(yield* desk.count("r-results")).toBe(1);

      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements[0]?.outcome).toBe("completed");
      // The recorded outcome did not rerun (exit gate).
      expect(yield* desk.count("r-results")).toBe(1);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      expect(
        records.filter((envelope) => envelope.record.recordId === `tool-settled:${runId}:1:book-1`),
      ).toHaveLength(1);
      // The resuming Attempt superseded epoch 1 and re-invoked the model: audited exactly once,
      // prompt-transparently (durability §9).
      expect(
        records.filter(
          (envelope) => envelope.record.recordId === modelResponseInterruptedRecordId(runId, 1),
        ),
      ).toHaveLength(1);
      const prompt = yield* promptFromCanonicalRecords(records);
      expect(prompt.content.map((message) => message.role)).toEqual([
        "assistant",
        "tool",
        "assistant",
      ]);
    }),
  );

  it.effect("completed Step results replay without executing; the handler re-enters honestly", () =>
    Effect.gen(function* () {
      yield* resetReconciler;
      const runtime = yield* DurableAgentRuntime;
      const control = yield* ReconcilerTestControl;
      const desk = yield* makeItineraryDesk;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("itinerary-1", "itinerary", { ref: "trip" }))
          : finalParts('{"answer":"reserved"}'),
      );
      const agent = Agent.withModel(itineraryDefinition, scripted.model);
      const conversation = "conversation-durable-steps";

      const receipt = yield* runtime.submit(
        agent,
        { question: "reserve it" },
        submitOptions(conversation, "steps-1"),
      );
      // Kill right after the FIRST Step commit: reserve-flight is exactly-once-recorded,
      // reserve-lodging never ran, the Attempt aborts without settling.
      yield* armFailpoint("step:after-step-append");
      const killed = yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(desk.toolLayer)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      expect(yield* desk.entries).toBe(1);
      expect(yield* desk.flightRuns).toBe(1);
      expect(yield* desk.lodgingRuns).toBe(0);
      expect(yield* lookupState(receipt.submissionId)).not.toBe("settled");

      const runId = runIdForSubmission(receipt.submissionId);
      const callId = decodeToolCallId("itinerary-1");
      const committed = yield* readLog(conversation);
      expect(committed.map((envelope) => envelope.record.recordId)).toContain(
        toolStepSettledRecordId(runId, callId, "reserve-flight"),
      );

      // The supplier proves the call is safe to repeat; the handler re-enters (at-least-once),
      // replays Step 1 from its record, and executes only Step 2.
      yield* control.set(() => ReconciliationSafeToRetry.make());
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.entries).toBe(2);
      expect(yield* desk.flightRuns).toBe(1);
      expect(yield* desk.lodgingRuns).toBe(1);

      const records = yield* readLog(conversation);
      expect(
        records.filter(
          (envelope) =>
            envelope.record.recordId === toolStepSettledRecordId(runId, callId, "reserve-flight"),
        ),
      ).toHaveLength(1);
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        toolStepSettledRecordId(runId, callId, "reserve-lodging"),
      );
      const settled = records.find(
        (envelope) => envelope.record.recordId === `tool-settled:${runId}:1:itinerary-1`,
      )?.record.payload;
      if (settled?._tag === "ToolCallSettled") {
        expect(settled.result).toEqual({ state: "flight-trip+lodging-trip" });
      }
    }),
  );

  it.effect("keeps failure and requirement channels typed (E/R proofs)", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const command = UnknownResolutionCommand.make({
        submissionId: decodeSubmissionId("submission-types"),
        toolCallId: decodeToolCallId("call-types"),
        author: "operator",
        reason: "type proof",
        resolution: ResolutionNeverHappened.make(),
      });
      const resolveProgram = runtime.resolveUnknown(command);
      type ResolveError = Effect.Error<typeof resolveProgram>;
      const resolveHasConflict: UnknownResolutionConflict extends ResolveError ? true : false =
        true;
      const resolveHasSettlement: SettlementConflict extends ResolveError ? true : false = true;
      const resolveHasFailpoint: DurableRuntimeFailpointError extends ResolveError ? true : false =
        true;

      type LayerIn<L> = L extends Layer.Layer<infer _A, infer _E, infer R> ? R : never;
      const layerNeedsReconciler: ToolReconciler extends LayerIn<typeof DurableAgentRuntime.layer>
        ? true
        : false = true;
      const customLayerNeedsContext: RunContextPreparation extends LayerIn<
        typeof DurableAgentRuntime.layerWithContext
      >
        ? true
        : false = true;
      const defaultLayerClosesContext: RunContextPreparation extends LayerIn<
        typeof DurableAgentRuntime.layer
      >
        ? false
        : true = true;

      expect(resolveHasConflict).toBe(true);
      expect(resolveHasSettlement).toBe(true);
      expect(resolveHasFailpoint).toBe(true);
      expect(layerNeedsReconciler).toBe(true);
      expect(customLayerNeedsContext).toBe(true);
      expect(defaultLayerClosesContext).toBe(true);
    }),
  );
});

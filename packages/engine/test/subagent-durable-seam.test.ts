import {
  Agent,
  AgentId,
  AgentPolicy,
  ConversationId,
  DelegationId,
  IdGenerator,
  ReceiptId,
  RunId,
  SubmissionId,
  ToolCallId,
  TurnId,
  type RunEvent,
} from "@effect-agent/core";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  ErrorReporter,
  Exit,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import {
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

import {
  AgentChildPending,
  AgentRuntime,
  AgentSpawner,
  SubagentDurability,
  SubagentDurabilityError,
  ToolCallWaiting,
  type ChildEstablishStatus,
  type RunSubagentChildIdentity,
  type RunSubagentEstablishRequest,
  type RunSubagentHook,
  type RunSubagentJoinRequest,
  type RunTurnResume,
} from "../src/index.ts";

class DelegationFailed extends Schema.TaggedError<DelegationFailed>()("DelegationFailed", {
  message: Schema.String,
}) {}

class HookFailure extends Schema.TaggedError<HookFailure>()("HookFailure", {
  message: Schema.String,
}) {}

class TypedHookService extends Context.Service<TypedHookService, { readonly enabled: true }>()(
  "@effect-agent/engine/test/SubagentTypedHookService",
) {}

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeRunId = Schema.decodeSync(RunId);
const decodeTurnId = Schema.decodeSync(TurnId);
const decodeSubmissionId = Schema.decodeSync(SubmissionId);
const decodeReceiptId = Schema.decodeSync(ReceiptId);
const decodeToolCallId = Schema.decodeUnknownEffect(ToolCallId);

/** Deterministic, distinct identities; a fresh counter per Layer build. */
const makeIdentifiers = () =>
  Layer.effect(IdGenerator)(
    Effect.gen(function* () {
      const counter = yield* Ref.make(0);
      const next = <A>(decode: (value: string) => A, prefix: string) =>
        Ref.updateAndGet(counter, (value) => value + 1).pipe(
          Effect.map((value) => decode(`${prefix}-${value}`)),
        );
      return {
        nextConversationId: next(decodeConversationId, "conversation"),
        nextRunId: next(decodeRunId, "run"),
        nextTurnId: next(decodeTurnId, "turn"),
      };
    }),
  );

const identifiers = makeIdentifiers();

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

/** Scripted two-turn model: a fresh turn counter per Layer build (per Run). */
const scriptedModel = (firstTurn: ReadonlyArray<Response.StreamPartEncoded>, finalText: string) =>
  Model.make(
    "scripted",
    "subagent-durable-seam",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);
        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                Effect.map((value) =>
                  Stream.fromIterable<Response.StreamPartEncoded>(
                    value === 0 ? firstTurn : finalParts(finalText),
                  ),
                ),
              ),
            ),
        });
      }),
    ),
  );

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Effect to fail");
  }
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed failure in the Cause");
  }
  return failure.value;
};

const delegationId = Schema.decodeSync(DelegationId)("delegate_child");
const targetAgentId = Schema.decodeSync(AgentId)("scripted-child");

const childIdentityFor = (suffix: string): RunSubagentChildIdentity => ({
  childConversationId: decodeConversationId(`child-conversation-${suffix}`),
  childSubmissionId: decodeSubmissionId(`child-submission-${suffix}`),
  childRunId: decodeRunId(`child-run-${suffix}`),
  receiptId: decodeReceiptId(`child-receipt-${suffix}`),
});

const ChildResult = Schema.Struct({ answer: Schema.String });

/**
 * The engine-facing durable delegation handler shape WP5 implements: mode
 * dispatch on the per-batch service, establishment request assembly, the
 * waiting signal while the child runs, and decode + join on a settled child.
 */
const durableDelegateHandler =
  (log?: {
    readonly establishes?: Ref.Ref<ReadonlyArray<RunSubagentEstablishRequest>>;
    readonly executions?: Ref.Ref<number>;
  }) =>
  (
    parameters: { readonly question: string },
    context: { readonly toolCallId?: string | undefined },
  ) =>
    Effect.gen(function* () {
      if (log?.executions !== undefined) {
        yield* Ref.update(log.executions, (count) => count + 1);
      }
      const durability = yield* SubagentDurability;
      if (durability.mode === "ephemeral") {
        return yield* Effect.die(new Error("Expected the durable-mode SubagentDurability service"));
      }
      const toolCallId = yield* decodeToolCallId(context.toolCallId).pipe(Effect.orDie);
      const request: RunSubagentEstablishRequest = {
        toolCallId,
        delegationId,
        targetAgentId,
        depth: 1,
        targetDigests: { agent: "digest-agent", model: "digest-model", tools: "digest-tools" },
        encodedChildInput: { question: parameters.question },
        encodedGrant: { allowedToolNames: [], maxDepth: 1 },
        encodedAllocation: { turns: 2, toolCalls: 1 },
      };
      if (log?.establishes !== undefined) {
        yield* Ref.update(log.establishes, (all) => [...all, request]);
      }
      const status = yield* durability.establish(request);
      switch (status._tag) {
        case "waiting": {
          return yield* durability.waiting(toolCallId, status);
        }
        case "denied": {
          return yield* DelegationFailed.make({ message: status.message });
        }
        case "settled": {
          if (status.outcome !== "completed") {
            return yield* DelegationFailed.make({
              message: `child settled ${status.outcome}`,
            });
          }
          const decoded = yield* Schema.decodeUnknownEffect(ChildResult)(status.encodedResult).pipe(
            Effect.orDie,
          );
          yield* durability.join({
            toolCallId,
            encodedResult: { answer: decoded.answer },
            isFailure: false,
            encodedAccounting: { consumed: { turns: 1 }, released: { turns: 1 } },
          });
          return { answer: decoded.answer };
        }
      }
    });

const DelegateChild = Tool.make("delegate_child", {
  parameters: Schema.Struct({ question: Schema.String }),
  success: Schema.Struct({ answer: Schema.String }),
  failure: Schema.Union([ToolCallWaiting, SubagentDurabilityError, DelegationFailed]),
}).addDependency(SubagentDurability);

const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.String,
});

const batchTools = Toolkit.make(DelegateChild, Lookup);
const batchDefinition = Agent.define("durable-delegating-parent", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Delegate and look up, then answer as JSON.",
  toolkit: batchTools,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
});

const delegateCallPart = (id: string): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name: "delegate_child",
  params: { question: "child?" },
  providerExecuted: false,
});

const lookupCallPart = (id: string, key: string): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name: "lookup",
  params: { key },
  providerExecuted: false,
});

/** Hook whose establish answer is scripted per call; joins are recorded. */
const scriptedHook = (
  establish: (request: RunSubagentEstablishRequest) => ChildEstablishStatus,
  joins?: Ref.Ref<ReadonlyArray<RunSubagentJoinRequest>>,
): RunSubagentHook => ({
  establish: (request) => Effect.sync(() => establish(request)),
  join: (request) =>
    joins === undefined ? Effect.void : Ref.update(joins, (all) => [...all, request]),
});

layer(identifiers)("S2 WP1 durable Subagent engine seam", (it) => {
  it.effect("a waiting delegation call does not trigger the batch failure policy", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const child = childIdentityFor("wait");
      const subagent = scriptedHook(() => ({ _tag: "waiting", ...child }));
      const model = scriptedModel(
        [
          delegateCallPart("delegate-1"),
          lookupCallPart("lookup-1", "a"),
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"unreachable"}',
      );
      const toolLayer = batchTools.toLayer({
        delegate_child: durableDelegateHandler(),
        lookup: ({ key }) => Effect.succeed(`handled-${key}`),
      });

      const exit = yield* AgentRuntime.stream(
        Agent.withModel(batchDefinition, model),
        { question: "root?" },
        { subagent },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );

      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(AgentChildPending);
      if (!(failure instanceof AgentChildPending)) {
        throw new Error("Expected AgentChildPending");
      }
      expect(failure.children).toEqual([
        {
          toolCallId: "delegate-1",
          childConversationId: child.childConversationId,
          childSubmissionId: child.childSubmissionId,
          childRunId: child.childRunId,
        },
      ]);

      const observed = yield* Ref.get(events);
      const tags = observed.map((event) => event._tag);
      // The sibling settled normally; the waiting call has no terminal event
      // (neither failure nor success) — it stays open for the resumed batch.
      expect(tags.filter((tag) => tag === "ToolCallFailed")).toEqual([]);
      const succeeded = observed.filter((event) => event._tag === "ToolCallSucceeded");
      expect(succeeded.map((event) => event.toolCallId)).toEqual(["lookup-1"]);
      const started = observed.filter((event) => event._tag === "ToolCallStarted");
      expect(started.map((event) => event.toolCallId).sort()).toEqual(["delegate-1", "lookup-1"]);
      expect(tags.at(-1)).toBe("RunSuspended");
    }),
  );

  it.effect("siblings run to completion before the run suspends", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const siblingInterrupted = yield* Ref.make(false);
      const siblingCompleted = yield* Ref.make(false);
      const child = childIdentityFor("slow-sibling");
      const subagent = scriptedHook(() => ({ _tag: "waiting", ...child }));
      const model = scriptedModel(
        [
          delegateCallPart("delegate-1"),
          lookupCallPart("lookup-1", "slow"),
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"unreachable"}',
      );
      const toolLayer = batchTools.toLayer({
        // The delegation raises the waiting signal immediately …
        delegate_child: durableDelegateHandler(),
        // … while the sibling keeps running long past it.
        lookup: ({ key }) =>
          Effect.gen(function* () {
            yield* Effect.yieldNow;
            yield* Effect.yieldNow;
            yield* Effect.yieldNow;
            yield* Ref.set(siblingCompleted, true);
            return `handled-${key}`;
          }).pipe(Effect.onInterrupt(() => Ref.set(siblingInterrupted, true))),
      });

      const exit = yield* AgentRuntime.stream(
        Agent.withModel(batchDefinition, model),
        { question: "root?" },
        { subagent },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );

      expect(failureFrom(exit)).toBeInstanceOf(AgentChildPending);
      expect(yield* Ref.get(siblingCompleted)).toBe(true);
      expect(yield* Ref.get(siblingInterrupted)).toBe(false);
      const observed = yield* Ref.get(events);
      const succeededIndex = observed.findIndex(
        (event) => event._tag === "ToolCallSucceeded" && event.toolCallId === "lookup-1",
      );
      const suspendedIndex = observed.findIndex((event) => event._tag === "RunSuspended");
      expect(succeededIndex).toBeGreaterThanOrEqual(0);
      expect(suspendedIndex).toBeGreaterThan(succeededIndex);
    }),
  );

  it.effect("aggregates every waiting delegation call in declaration order", () =>
    Effect.gen(function* () {
      // The SECOND-declared call raises the waiting signal FIRST, proving the
      // AgentChildPending listing is declaration-ordered, not completion-ordered.
      const firstMayWait = yield* Deferred.make<void>();
      const childA = childIdentityFor("a");
      const childB = childIdentityFor("b");
      const subagent = scriptedHook((request) =>
        request.toolCallId === "delegate-1"
          ? { _tag: "waiting", ...childA }
          : { _tag: "waiting", ...childB },
      );
      const DelegateOnly = Toolkit.make(DelegateChild);
      const definition = Agent.define("two-waiting-delegations", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Delegate twice.",
        toolkit: DelegateOnly,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        }),
      });
      const model = scriptedModel(
        [
          delegateCallPart("delegate-1"),
          delegateCallPart("delegate-2"),
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"unreachable"}',
      );
      const toolLayer = DelegateOnly.toLayer({
        delegate_child: (parameters, context) =>
          context.toolCallId === "delegate-1"
            ? // The first-declared call raises only after the second's handler
              // already exited with its waiting signal.
              Deferred.await(firstMayWait).pipe(
                Effect.andThen(durableDelegateHandler()(parameters, context)),
              )
            : durableDelegateHandler()(parameters, context).pipe(
                Effect.onExit(() => Deferred.succeed(firstMayWait, undefined)),
              ),
      });

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "root?" },
        { subagent },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);

      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(AgentChildPending);
      if (!(failure instanceof AgentChildPending)) {
        throw new Error("Expected AgentChildPending");
      }
      expect(failure.children.map((entry) => entry.toolCallId)).toEqual([
        "delegate-1",
        "delegate-2",
      ]);
      expect(failure.children.map((entry) => entry.childSubmissionId)).toEqual([
        childA.childSubmissionId,
        childB.childSubmissionId,
      ]);
    }),
  );

  it.effect("a real sibling failure keeps the batch failure policy while a call waits", () =>
    Effect.gen(function* () {
      const waitingRaised = yield* Deferred.make<void>();
      const child = childIdentityFor("failing-sibling");
      const subagent = scriptedHook(() => ({ _tag: "waiting", ...child }));
      const FailingLookup = Tool.make("lookup", {
        parameters: Schema.Struct({ key: Schema.String }),
        success: Schema.String,
        failure: DelegationFailed,
      });
      const tools = Toolkit.make(DelegateChild, FailingLookup);
      const definition = Agent.define("waiting-plus-failing-sibling", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Delegate and look up.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        }),
      });
      const model = scriptedModel(
        [
          delegateCallPart("delegate-1"),
          lookupCallPart("lookup-1", "boom"),
          { type: "finish", reason: "tool-calls", usage },
        ],
        '{"answer":"unreachable"}',
      );
      const toolLayer = tools.toLayer({
        delegate_child: (parameters, context) =>
          durableDelegateHandler()(parameters, context).pipe(
            Effect.onExit(() => Deferred.succeed(waitingRaised, undefined)),
          ),
        lookup: () =>
          Deferred.await(waitingRaised).pipe(
            Effect.andThen(DelegationFailed.make({ message: "sibling failed" })),
          ),
      });

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "root?" },
        { subagent },
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);

      // The waiting call was collected first, yet the sibling's typed failure
      // wins: real failures keep the existing batch failure policy and the
      // durable ledger state (not this stream) carries the attached child.
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(DelegationFailed);
      expect((failure as DelegationFailed).message).toBe("sibling failed");
    }),
  );

  it.effect("an established child that already settled joins without suspending", () =>
    Effect.gen(function* () {
      const joins = yield* Ref.make<ReadonlyArray<RunSubagentJoinRequest>>([]);
      const establishes = yield* Ref.make<ReadonlyArray<RunSubagentEstablishRequest>>([]);
      const child = childIdentityFor("settled");
      const subagent = scriptedHook(
        () => ({
          _tag: "settled",
          ...child,
          outcome: "completed",
          encodedResult: { answer: "child-answer" },
        }),
        joins,
      );
      const model = scriptedModel(
        [delegateCallPart("delegate-1"), { type: "finish", reason: "tool-calls", usage }],
        '{"answer":"joined"}',
      );
      const toolLayer = batchTools.toLayer({
        delegate_child: durableDelegateHandler({ establishes }),
        lookup: ({ key }) => Effect.succeed(`handled-${key}`),
      });

      const result = yield* AgentRuntime.run(
        Agent.withModel(batchDefinition, model),
        { question: "root?" },
        { subagent },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);

      expect(result.output).toEqual({ answer: "joined" });
      const recordedEstablishes = yield* Ref.get(establishes);
      expect(recordedEstablishes).toHaveLength(1);
      expect(recordedEstablishes[0]).toMatchObject({
        toolCallId: "delegate-1",
        delegationId,
        targetAgentId,
        depth: 1,
        targetDigests: { agent: "digest-agent", model: "digest-model", tools: "digest-tools" },
        encodedChildInput: { question: "child?" },
      });
      const recordedJoins = yield* Ref.get(joins);
      expect(recordedJoins).toEqual([
        {
          toolCallId: "delegate-1",
          encodedResult: { answer: "child-answer" },
          isFailure: false,
          encodedAccounting: { consumed: { turns: 1 }, released: { turns: 1 } },
        },
      ]);
    }),
  );

  it.effect("resume injects settled siblings and re-executes only the waiting call", () =>
    Effect.gen(function* () {
      const joins = yield* Ref.make<ReadonlyArray<RunSubagentJoinRequest>>([]);
      const delegateExecutions = yield* Ref.make(0);
      const lookupExecutions = yield* Ref.make(0);
      let nextRequestPrompt: Prompt.Prompt | undefined;
      const child = childIdentityFor("resume");
      // On the resumed Attempt the child has settled: establishment replays
      // idempotently and reports the verified settlement.
      const subagent = scriptedHook(
        () => ({
          _tag: "settled",
          ...child,
          outcome: "completed",
          encodedResult: { answer: "child-answer" },
        }),
        joins,
      );
      const model = Model.make(
        "scripted",
        "resume-joining",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) => {
              nextRequestPrompt = request.prompt;
              return Stream.fromIterable(finalParts('{"answer":"resumed"}'));
            },
          }),
        ),
      );
      const toolLayer = batchTools.toLayer({
        delegate_child: durableDelegateHandler({ executions: delegateExecutions }),
        lookup: ({ key }) =>
          Ref.update(lookupExecutions, (count) => count + 1).pipe(Effect.as(`re-handled-${key}`)),
      });
      const resume: RunTurnResume = {
        turn: 1,
        turnId: decodeTurnId("turn-resume-join"),
        calls: [
          { id: "delegate-1", name: "delegate_child", params: { question: "child?" } },
          { id: "lookup-1", name: "lookup", params: { key: "a" } },
        ],
        settled: [{ id: "lookup-1", result: "recorded-a", isFailure: false }],
      };

      const result = yield* AgentRuntime.run(
        Agent.withModel(batchDefinition, model),
        { question: "root?" },
        { subagent, resume },
      ).pipe(Effect.provide(toolLayer), Effect.scoped);

      expect(result.output).toEqual({ answer: "resumed" });
      // Only the waiting delegation call re-executed; the settled sibling was
      // injected without starting its handler.
      expect(yield* Ref.get(delegateExecutions)).toBe(1);
      expect(yield* Ref.get(lookupExecutions)).toBe(0);
      expect(yield* Ref.get(joins)).toHaveLength(1);

      expect(nextRequestPrompt).toBeDefined();
      if (nextRequestPrompt === undefined) {
        throw new Error("Expected the follow-up model request to be captured");
      }
      const toolResults = nextRequestPrompt.content
        .filter((message) => message.role === "tool")
        .flatMap((message) => message.content)
        .filter((part) => part.type === "tool-result");
      expect(toolResults.map((part) => [part.id, part.result])).toEqual([
        ["delegate-1", { answer: "child-answer" }],
        ["lookup-1", "recorded-a"],
      ]);
    }),
  );

  it.effect("a denied establishment is an ordinary typed Tool failure", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const subagent = scriptedHook(() => ({
        _tag: "denied",
        errorTag: "ChildVerificationFailed",
        message: "parent link verification failed closed",
      }));
      const model = scriptedModel(
        [delegateCallPart("delegate-1"), { type: "finish", reason: "tool-calls", usage }],
        '{"answer":"unreachable"}',
      );
      const toolLayer = batchTools.toLayer({
        delegate_child: durableDelegateHandler(),
        lookup: ({ key }) => Effect.succeed(`handled-${key}`),
      });

      const exit = yield* AgentRuntime.stream(
        Agent.withModel(batchDefinition, model),
        { question: "root?" },
        { subagent },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );

      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(DelegationFailed);
      expect((failure as DelegationFailed).message).toContain("verification failed closed");
      const observed = yield* Ref.get(events);
      const failed = observed.filter((event) => event._tag === "ToolCallFailed");
      expect(failed.map((event) => event.toolCallId)).toEqual(["delegate-1"]);
      // A denied establishment never suspends: RunFailed, not RunSuspended.
      expect(observed.at(-1)?._tag).toBe("RunFailed");
    }),
  );

  it.effect("a hook failure surfaces as typed SubagentDurabilityError", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const reported: Array<string> = [];
      const reporter = ErrorReporter.make(({ error }) => {
        reported.push(error.message);
      });
      const subagent: RunSubagentHook<HookFailure> = {
        establish: () => Effect.fail(HookFailure.make({ message: "ledger unavailable" })),
        join: () => Effect.void,
      };
      const model = scriptedModel(
        [delegateCallPart("delegate-1"), { type: "finish", reason: "tool-calls", usage }],
        '{"answer":"unreachable"}',
      );
      const toolLayer = batchTools.toLayer({
        delegate_child: durableDelegateHandler(),
        lookup: ({ key }) => Effect.succeed(`handled-${key}`),
      });

      const exit = yield* AgentRuntime.stream(
        Agent.withModel(batchDefinition, model),
        { question: "root?" },
        { subagent },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(
          Layer.merge(toolLayer, ErrorReporter.layer([reporter], { mergeWithExisting: true })),
        ),
        Effect.exit,
      );

      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(SubagentDurabilityError);
      if (!(failure instanceof SubagentDurabilityError)) {
        throw new Error("Expected SubagentDurabilityError");
      }
      expect(failure.operation).toBe("establish");
      expect(failure.reason).toBe("hook-failed");
      expect(failure.message).toBe("Durable child establishment failed");
      expect(JSON.stringify(failure)).not.toContain("ledger unavailable");
      expect(JSON.stringify(yield* Ref.get(events))).not.toContain("ledger unavailable");
      expect(reported.some((message) => message.includes("ledger unavailable"))).toBe(true);
    }),
  );

  it.effect("the ephemeral default keeps S1 behavior byte-identical", () =>
    Effect.gen(function* () {
      const childDefinition = Agent.define("ephemeral-default-child", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer as JSON.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 1,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const SpawningDelegate = Tool.make("delegate_child", {
        parameters: Schema.Struct({ question: Schema.String }),
        success: Schema.Struct({ answer: Schema.String }),
      })
        .addDependency(AgentSpawner)
        .addDependency(IdGenerator)
        .addDependency(SubagentDurability);
      const tools = Toolkit.make(SpawningDelegate);
      const definition = Agent.define("ephemeral-default-parent", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Delegate, then answer as JSON.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const spawnChild = (
        parameters: { readonly question: string },
        toolCallId: string | undefined,
      ) =>
        Effect.gen(function* () {
          const spawner = yield* AgentSpawner;
          const parentToolCallId = yield* decodeToolCallId(toolCallId).pipe(Effect.orDie);
          const childBinding = Agent.withModel(
            childDefinition,
            scriptedModel(finalParts('{"answer":"child-answer"}'), '{"answer":"child-answer"}'),
          );
          const child = yield* spawner.spawn(
            childBinding,
            { question: parameters.question },
            { delegationId, parentToolCallId },
          );
          const result = yield* child.await.pipe(Effect.orDie);
          return { answer: result.output.answer };
        }).pipe(Effect.scoped);

      const observedMode = yield* Ref.make<string | undefined>(undefined);
      // Baseline: the exact S1 handler shape — spawn and await, never
      // consulting the durability seam.
      const baselineLayer = tools.toLayer({
        delegate_child: (parameters, context) => spawnChild(parameters, context.toolCallId),
      });
      // Candidate: identical behavior after dispatching on the engine's
      // explicit ephemeral-mode default.
      const dispatchingLayer = tools.toLayer({
        delegate_child: (parameters, context) =>
          Effect.gen(function* () {
            const durability = yield* SubagentDurability;
            yield* Ref.set(observedMode, durability.mode);
            if (durability.mode !== "ephemeral") {
              return yield* Effect.die(new Error("Expected the ephemeral default"));
            }
            return yield* spawnChild(parameters, context.toolCallId);
          }),
      });

      const runWith = (toolLayer: typeof baselineLayer) =>
        AgentRuntime.stream(
          Agent.withModel(
            definition,
            scriptedModel(
              [delegateCallPart("delegate-1"), { type: "finish", reason: "tool-calls", usage }],
              '{"answer":"parent-answer"}',
            ),
          ),
          { question: "root?" },
          // NO subagent option: the engine constructs the explicit
          // ephemeral-mode default.
        ).pipe(Stream.runCollect, Effect.provide(Layer.merge(toolLayer, makeIdentifiers())));

      const normalize = (events: ReadonlyArray<RunEvent>) =>
        events.map((event) => ({ ...event, timestamp: "normalized" }));

      const baseline = yield* runWith(baselineLayer);
      const dispatched = yield* runWith(dispatchingLayer);

      expect(yield* Ref.get(observedMode)).toBe("ephemeral");
      expect(normalize(dispatched)).toEqual(normalize(baseline));
      expect(baseline.at(-1)?._tag).toBe("RunCompleted");
    }),
  );

  it.effect("keeps SubagentDurability out of the public requirements", () => {
    const toolLayer = batchTools.toLayer({
      delegate_child: () => Effect.succeed({ answer: "typed" }),
      lookup: ({ key }) => Effect.succeed(`handled-${key}`),
    });
    const agent = Agent.withModel(
      batchDefinition,
      scriptedModel(
        [delegateCallPart("delegate-1"), { type: "finish", reason: "tool-calls", usage }],
        '{"answer":"typed"}',
      ),
    );
    const program = AgentRuntime.run(agent, { question: "types" });

    type Services = Effect.Services<typeof program>;
    type DurabilityExcluded = [Extract<Services, SubagentDurability>] extends [never]
      ? true
      : false;
    type SpawnerExcluded = [Extract<Services, AgentSpawner>] extends [never] ? true : false;
    type IdGeneratorKept = IdGenerator extends Services ? true : false;
    const durabilityExcluded: DurabilityExcluded = true;
    const spawnerExcluded: SpawnerExcluded = true;
    const idGeneratorKept: IdGeneratorKept = true;

    expect({ durabilityExcluded, spawnerExcluded, idGeneratorKept }).toEqual({
      durabilityExcluded: true,
      spawnerExcluded: true,
      idGeneratorKept: true,
    });
    return AgentRuntime.run(agent, { question: "types" }).pipe(
      Effect.provide(toolLayer),
      Effect.scoped,
      Effect.asVoid,
    );
  });

  it.effect("keeps subagent hook failures and requirements visible in E and R", () => {
    const subagent: RunSubagentHook<HookFailure, TypedHookService> = {
      establish: () =>
        Effect.gen(function* () {
          yield* TypedHookService;
          return yield* HookFailure.make({ message: "establish failed" });
        }),
      join: () => Effect.andThen(TypedHookService, Effect.void),
    };
    const agent = Agent.withModel(batchDefinition, scriptedModel([], '{"answer":"typed"}'));
    const program = AgentRuntime.run(agent, { question: "typed" }, { subagent });

    type ErrorProof = HookFailure extends Effect.Error<typeof program> ? true : false;
    type RequirementsProof =
      TypedHookService extends Effect.Services<typeof program> ? true : false;
    type ChildPendingProof = AgentChildPending extends Effect.Error<typeof program> ? true : false;
    const errorProof: ErrorProof = true;
    const requirementsProof: RequirementsProof = true;
    const childPendingProof: ChildPendingProof = true;

    expect({ errorProof, requirementsProof, childPendingProof }).toEqual({
      errorProof: true,
      requirementsProof: true,
      childPendingProof: true,
    });
    return Effect.void;
  });
});

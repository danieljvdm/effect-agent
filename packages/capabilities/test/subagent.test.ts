import { describe, expect, it, layer } from "@effect/vitest";

import {
  Cause,
  Context,
  Deferred,
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
  type AiError,
  LanguageModel,
  Model,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";
import {
  Agent,
  type AgentOutputError,
  AgentPolicy,
  ConversationId,
  IdGenerator,
  type RunEvent,
  RunId,
  ToolCallId,
  TurnId,
} from "@effect-agent/core";
import {
  AgentRuntime,
  AgentSpawner,
  RunEventSink,
  type RuntimeBinding,
} from "@effect-agent/engine";

import {
  delegationAllocationFromPolicy,
  delegationCapsFromPolicy,
  isDelegationToolName,
  Subagent,
  SubagentBudgetExhausted,
  type SubagentChildRunFailure,
  SubagentDelegationCaps,
  SubagentGrant,
  SubagentPolicy,
  SubagentPrestartDenied,
  SubagentProjectionFailure,
  SubagentReservations,
  SubagentReservationsMemoryLive,
  type SubagentReservationView,
  SubagentRuntime,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeRunId = Schema.decodeSync(RunId);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodeTurnId = Schema.decodeSync(TurnId);

/** Deterministic, distinct identities so preallocated child IDs are observable. */
const identifiers = Layer.effect(IdGenerator)(
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

const TestServices = Layer.mergeAll(identifiers, SubagentReservationsMemoryLive);

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

/** One-turn scripted child model that answers immediately and can capture its prompt. */
const answeringModel = (name: string, answerText: string, promptRef?: Ref.Ref<unknown>) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) =>
          promptRef === undefined
            ? Stream.fromIterable(finalParts(answerText))
            : Stream.unwrap(
                Ref.set(promptRef, options.prompt).pipe(
                  Effect.as(Stream.fromIterable(finalParts(answerText))),
                ),
              ),
      }),
    ),
  );

/** Scripted parent model: turn one declares the given Tool Calls, turn two answers. */
const delegatingModel = (
  name: string,
  toolName: string,
  calls: ReadonlyArray<{ readonly id: string; readonly params: unknown }>,
  answerText: string,
) =>
  Model.make(
    "scripted",
    name,
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
                    value === 0
                      ? [
                          ...calls.map(
                            (call): Response.StreamPartEncoded => ({
                              type: "tool-call",
                              id: call.id,
                              name: toolName,
                              params: call.params,
                              providerExecuted: false,
                            }),
                          ),
                          { type: "finish", reason: "tool-calls", usage },
                        ]
                      : finalParts(answerText),
                  ),
                ),
              ),
            ),
        });
      }),
    ),
  );

const ChildInput = Schema.Struct({ question: Schema.String });
const ChildOutput = Schema.Struct({ answer: Schema.String });

const childPolicy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

const childDefinition = Agent.define("research-child", {
  input: ChildInput,
  output: ChildOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: childPolicy,
});

class ResearchDelegationFailed extends Schema.TaggedErrorClass<ResearchDelegationFailed>()(
  "ResearchDelegationFailed",
  {
    childErrorTag: Schema.String,
  },
) {}

const ResearchParams = Schema.Struct({ topic: Schema.String });
const ResearchFindings = Schema.Struct({ summary: Schema.String });

const researchPolicy = SubagentPolicy.make({
  maxChildren: 2,
  maxConcurrency: 2,
  maxTurns: 4,
  maxToolCalls: 4,
  maxDuration: "10 seconds",
});

const researchDelegation = Subagent.define("delegate_research", {
  description: "Research one bounded question and return findings.",
  target: childDefinition,
  parameters: ResearchParams,
  success: ResearchFindings,
  failure: ResearchDelegationFailed,
  prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: researchPolicy,
});

const parentPolicy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 2,
  maxDuration: "30 seconds",
  toolConcurrency: 2,
});

const coordinatorDefinition = Agent.define("coordinator", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate, then answer as JSON.",
  toolkit: Toolkit.make(researchDelegation.tool),
  policy: parentPolicy,
});

const mapChildFailure = (failure: { readonly _tag: string }) =>
  ResearchDelegationFailed.make({ childErrorTag: failure._tag });

const researchLayer = <Provider, ModelProvides, ModelRequires>(
  childBinding: RuntimeBinding<
    typeof ChildInput,
    typeof ChildOutput,
    string,
    {},
    Provider,
    ModelProvides,
    ModelRequires
  >,
) => SubagentRuntime.layer(researchDelegation, childBinding, { mapChildFailure });

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

const findEvent = <Tag extends RunEvent["_tag"]>(
  events: ReadonlyArray<RunEvent>,
  tag: Tag,
): Extract<RunEvent, { readonly _tag: Tag }> | undefined =>
  events.find((event): event is Extract<RunEvent, { readonly _tag: Tag }> => event._tag === tag);

const subagentTags = (events: ReadonlyArray<RunEvent>): ReadonlyArray<string> =>
  events.map((event) => event._tag).filter((tag) => tag.startsWith("Subagent"));

const outerCallId = decodeToolCallId("outer-call-1");

const dimensionKeys = [
  "turns",
  "toolCalls",
  "durationMillis",
  "inputTokens",
  "outputTokens",
  "costMicrousd",
  "resultBytes",
] as const;

/** Spec §7: at `released`, allocated = covered + released and observed = covered + overrun. */
const expectSettledOnce = (view: SubagentReservationView | undefined): void => {
  expect(view).toBeDefined();
  if (view === undefined) {
    return;
  }
  expect(view.status).toBe("released");
  for (const key of dimensionKeys) {
    expect(view.allocated[key]).toBe(view.coveredConsumed[key] + view.released[key]);
    expect(view.observedConsumed[key] ?? 0).toBe(view.coveredConsumed[key] + view.overrun[key]);
  }
};

layer(TestServices)("SubagentRuntime S1 attached delegation", (it) => {
  it.effect("joins one attached child through explicit projections", () =>
    Effect.gen(function* () {
      const promptRef = yield* Ref.make<unknown>(undefined);
      const childBinding = Agent.withModel(
        childDefinition,
        answeringModel("child-success", '{"answer":"child-answer"}', promptRef),
      );
      const parent = Agent.withModel(
        coordinatorDefinition,
        delegatingModel(
          "parent-success",
          "delegate_research",
          [{ id: "call-1", params: { topic: "paris" } }],
          '{"report":"done"}',
        ),
      );
      const runId = decodeRunId("parent-run-success");

      const detached = yield* AgentRuntime.start(
        parent,
        { mission: "parent-secret-mission" },
        {
          runId,
        },
      ).pipe(Effect.provide(researchLayer(childBinding)));
      const result = yield* detached.await;
      const events = yield* detached.events;

      expect(result.output).toEqual({ report: "done" });
      expect(subagentTags(events)).toEqual([
        "SubagentRequested",
        "SubagentStarted",
        "SubagentCompleted",
        "SubagentJoined",
      ]);

      const joined = findEvent(events, "SubagentJoined");
      expect(joined).toMatchObject({
        delegationId: "delegate_research",
        targetAgentId: "research-child",
        toolCallId: "call-1",
        depth: 1,
        runId,
      });
      // The child owns fresh, distinct identity preallocated by the spawner.
      expect(joined?.childRunId).not.toBe(runId);
      expect(joined?.childConversationId).not.toBe(result.conversationId);
      const requested = findEvent(events, "SubagentRequested");
      expect(requested?.childRunId).toBe(joined?.childRunId);
      expect(findEvent(events, "SubagentCompleted")).toMatchObject({ turns: 1 });

      // The parent Tool result is the projected, Schema-encoded value.
      expect(findEvent(events, "ToolCallSucceeded")).toMatchObject({
        result: { summary: "finding:child-answer" },
      });

      // Child isolation (SUB-006/015): the child prompt contains only the
      // projected input, never the parent transcript.
      const childPrompt = JSON.stringify(yield* Ref.get(promptRef));
      expect(childPrompt).toContain("research:paris");
      expect(childPrompt).not.toContain("parent-secret-mission");

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.caps).toEqual(delegationCapsFromPolicy(researchPolicy));
      expect(snapshot.totalChildInvocations).toBe(1);
      expect(snapshot.reservations).toHaveLength(1);
      const view = snapshot.reservations[0];
      expectSettledOnce(view);
      // The child ran one model turn, observed through the wired budget hook.
      expect(view?.observedConsumed.turns).toBe(1);
      expect(view?.released.turns).toBe(researchPolicy.maxTurns - 1);
    }),
  );

  it.effect("total-maps expected child failures to the declared Tool failure", () =>
    Effect.gen(function* () {
      const childBinding = Agent.withModel(
        childDefinition,
        answeringModel("child-invalid-output", "not-json"),
      );
      const parent = Agent.withModel(
        coordinatorDefinition,
        delegatingModel(
          "parent-mapped-failure",
          "delegate_research",
          [{ id: "call-1", params: { topic: "berlin" } }],
          '{"report":"unreached"}',
        ),
      );
      const runId = decodeRunId("parent-run-mapped-failure");

      const detached = yield* AgentRuntime.start(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(researchLayer(childBinding)),
      );
      const exit = yield* Effect.exit(detached.await);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(ResearchDelegationFailed);
      expect(failure).toMatchObject({ childErrorTag: "AgentOutputError" });

      const events = yield* detached.events;
      expect(findEvent(events, "SubagentFailed")).toMatchObject({ errorTag: "AgentOutputError" });
      expect(findEvent(events, "SubagentCompleted")).toBeUndefined();
      expect(findEvent(events, "SubagentJoined")).toBeUndefined();

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      expectSettledOnce(snapshot.reservations[0]);
    }),
  );

  it.effect("rejects nested delegation at preflight before any reservation (SUB-029)", () =>
    Effect.gen(function* () {
      // A handmade (non-delegation) spawning Tool runs a mid-level Agent whose
      // Toolkit contains the delegation Tool; invoking it at depth 1 must be
      // denied before a child or reservation exists.
      const grandchildBinding = Agent.withModel(
        childDefinition,
        answeringModel("grandchild", '{"answer":"leaf"}'),
      );
      const midDefinition = Agent.define("midlevel", {
        input: ChildInput,
        output: ChildOutput,
        instructions: "Delegate research, then answer as JSON.",
        toolkit: Toolkit.make(researchDelegation.tool),
        policy: childPolicy,
      });
      const midBinding = Agent.withModel(
        midDefinition,
        delegatingModel(
          "mid-model",
          "delegate_research",
          [{ id: "nested-1", params: { topic: "nested" } }],
          '{"answer":"mid"}',
        ),
      );

      const capturedDenial = yield* Ref.make<unknown>(undefined);
      const capturedMidRunId = yield* Ref.make<RunId>(decodeRunId("mid-run-unset"));
      const dependencies = yield* Effect.context<SubagentReservations | IdGenerator>();
      const midDelegationLayer = Layer.provide(
        researchLayer(grandchildBinding),
        Layer.succeedContext(dependencies),
      );

      const SpawnMid = Tool.make("spawn_mid", {
        parameters: Schema.Struct({}),
        success: Schema.Struct({ acknowledged: Schema.Boolean }),
      })
        .addDependency(AgentSpawner)
        .addDependency(IdGenerator);
      const spawnTools = Toolkit.make(SpawnMid);
      const spawnToolLayer = spawnTools.toLayer({
        spawn_mid: () =>
          Effect.gen(function* () {
            const spawner = yield* AgentSpawner;
            const mid = yield* spawner.spawn(
              midBinding,
              { question: "root" },
              {
                delegationId: researchDelegation.delegationId,
                parentToolCallId: outerCallId,
              },
            );
            yield* Ref.set(capturedMidRunId, mid.runId);
            const exit = yield* Effect.exit(mid.await);
            if (Exit.isFailure(exit)) {
              yield* Ref.set(
                capturedDenial,
                Option.getOrUndefined(Cause.findErrorOption(exit.cause)),
              );
            }
            return { acknowledged: true };
          }).pipe(Effect.provide(midDelegationLayer), Effect.scoped),
      });

      const outerDefinition = Agent.define("outer", {
        input: Schema.Struct({}),
        output: Schema.Struct({ ok: Schema.Boolean }),
        instructions: "Spawn, then answer as JSON.",
        toolkit: spawnTools,
        policy: childPolicy,
      });
      const outer = Agent.withModel(
        outerDefinition,
        delegatingModel(
          "outer-model",
          "spawn_mid",
          [{ id: "outer-call-1", params: {} }],
          '{"ok":true}',
        ),
      );

      const result = yield* AgentRuntime.run(outer, {}).pipe(Effect.provide(spawnToolLayer));
      expect(result.output).toEqual({ ok: true });

      const denial = yield* Ref.get(capturedDenial);
      expect(denial).toBeInstanceOf(SubagentPrestartDenied);
      expect(denial).toMatchObject({
        reason: "nested-delegation",
        delegationId: "delegate_research",
      });

      // No child started, so no delegation budget was ever registered for the
      // mid-level Run.
      const reservations = yield* SubagentReservations;
      const midRunId = yield* Ref.get(capturedMidRunId);
      const snapshotExit = yield* Effect.exit(reservations.parentSnapshot(midRunId));
      expect(failureFrom(snapshotExit)._tag).toBe("SubagentParentBudgetUnknown");
    }),
  );

  it.effect("denies the invocation beyond maxChildren and settles accounting", () =>
    Effect.gen(function* () {
      const soloDelegation = Subagent.define("delegate_solo", {
        description: "Single-shot delegation.",
        target: childDefinition,
        parameters: ResearchParams,
        success: ResearchFindings,
        failure: ResearchDelegationFailed,
        prepareInput: ({ topic }) => Effect.succeed({ question: `solo:${topic}` }),
        projectResult: (output) => Effect.succeed({ summary: output.answer }),
        policy: SubagentPolicy.make({
          maxChildren: 1,
          maxConcurrency: 1,
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "10 seconds",
        }),
      });
      const soloParentDefinition = Agent.define("coordinator-solo", {
        input: Schema.Struct({ mission: Schema.String }),
        output: Schema.Struct({ report: Schema.String }),
        instructions: "Delegate twice, then answer as JSON.",
        toolkit: Toolkit.make(soloDelegation.tool),
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const childBinding = Agent.withModel(
        childDefinition,
        answeringModel("solo-child", '{"answer":"one"}'),
      );
      const parent = Agent.withModel(
        soloParentDefinition,
        delegatingModel(
          "parent-solo",
          "delegate_solo",
          [
            { id: "call-1", params: { topic: "a" } },
            { id: "call-2", params: { topic: "b" } },
          ],
          '{"report":"unreached"}',
        ),
      );
      const runId = decodeRunId("parent-run-max-children");

      const exit = yield* AgentRuntime.run(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(SubagentRuntime.layer(soloDelegation, childBinding, { mapChildFailure })),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(SubagentBudgetExhausted);
      expect(failure).toMatchObject({ dimension: "total-child-invocations" });

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      // The denied second invocation never reserved; the first settled once.
      expect(snapshot.totalChildInvocations).toBe(1);
      expect(snapshot.reservations).toHaveLength(1);
      expectSettledOnce(snapshot.reservations[0]);
    }),
  );

  it.effect("bounds concurrent children through the Scope-owned slot gate", () =>
    Effect.gen(function* () {
      const parallelDelegation = Subagent.define("delegate_parallel", {
        description: "Serialized delegation.",
        target: childDefinition,
        parameters: ResearchParams,
        success: ResearchFindings,
        failure: ResearchDelegationFailed,
        prepareInput: ({ topic }) => Effect.succeed({ question: `parallel:${topic}` }),
        projectResult: (output) => Effect.succeed({ summary: output.answer }),
        policy: SubagentPolicy.make({
          maxChildren: 2,
          maxConcurrency: 1,
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "10 seconds",
        }),
      });
      const parallelParentDefinition = Agent.define("coordinator-parallel", {
        input: Schema.Struct({ mission: Schema.String }),
        output: Schema.Struct({ report: Schema.String }),
        instructions: "Delegate twice, then answer as JSON.",
        toolkit: Toolkit.make(parallelDelegation.tool),
        policy: parentPolicy,
      });

      const active = yield* Ref.make(0);
      const maxActive = yield* Ref.make(0);
      const latch = yield* Deferred.make<void>();
      const firstStarted = yield* Deferred.make<void>();
      const gatedModel = Model.make(
        "scripted",
        "gated-child",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const current = yield* Ref.updateAndGet(active, (count) => count + 1);
                  yield* Ref.update(maxActive, (previous) => Math.max(previous, current));
                  yield* Deferred.succeed(firstStarted, undefined);
                  yield* Deferred.await(latch);
                  return Stream.fromIterable<Response.StreamPartEncoded>(
                    finalParts('{"answer":"done"}'),
                  ).pipe(Stream.ensuring(Ref.update(active, (count) => count - 1)));
                }),
              ),
          }),
        ),
      );
      const childBinding = Agent.withModel(childDefinition, gatedModel);
      const parent = Agent.withModel(
        parallelParentDefinition,
        delegatingModel(
          "parent-parallel",
          "delegate_parallel",
          [
            { id: "call-1", params: { topic: "a" } },
            { id: "call-2", params: { topic: "b" } },
          ],
          '{"report":"both"}',
        ),
      );
      const runId = decodeRunId("parent-run-concurrency");

      const fiber = yield* AgentRuntime.run(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(
          SubagentRuntime.layer(parallelDelegation, childBinding, { mapChildFailure }),
        ),
        Effect.scoped,
        Effect.exit,
        Effect.forkChild,
      );
      yield* Deferred.await(firstStarted);
      // The second child is queued on the concurrency gate, not executing.
      expect(yield* Ref.get(active)).toBe(1);
      yield* Deferred.succeed(latch, undefined);
      const exit = yield* Fiber.join(fiber);
      expect(Exit.isSuccess(exit)).toBe(true);
      expect(yield* Ref.get(maxActive)).toBe(1);

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.totalChildInvocations).toBe(2);
      expect(snapshot.reservations).toHaveLength(2);
      for (const view of snapshot.reservations) {
        expectSettledOnce(view);
      }
    }),
  );

  it.effect("settles the reservation when parent interruption reaches the child", () =>
    Effect.gen(function* () {
      const modelStarted = yield* Deferred.make<void>();
      const modelReleased = yield* Deferred.make<void>();
      const blockingModel = Model.make(
        "scripted",
        "blocking-child",
        Layer.effect(
          LanguageModel.LanguageModel,
          Effect.acquireRelease(
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Deferred.succeed(modelStarted, undefined).pipe(Effect.andThen(Effect.never)),
                ),
            }),
            () => Deferred.succeed(modelReleased, undefined),
          ),
        ),
      );
      const childBinding = Agent.withModel(childDefinition, blockingModel);
      const parent = Agent.withModel(
        coordinatorDefinition,
        delegatingModel(
          "parent-interrupted",
          "delegate_research",
          [{ id: "call-1", params: { topic: "blocked" } }],
          '{"report":"unreached"}',
        ),
      );
      const runId = decodeRunId("parent-run-interrupted");

      const fiber = yield* AgentRuntime.run(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(researchLayer(childBinding)),
        Effect.scoped,
        Effect.exit,
        Effect.forkChild,
      );
      yield* Deferred.await(modelStarted);
      yield* Fiber.interrupt(fiber);
      // Interruption reached the child Run and its model Layer finalizer.
      yield* Deferred.await(modelReleased);

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      const view = snapshot.reservations[0];
      expectSettledOnce(view);
      // The wall clock never advanced, so the honest duration observation is
      // zero and the full duration allocation returns; turns were never
      // observed and settle conservatively.
      expect(view?.observedConsumed.durationMillis).toBe(0);
      expect(view?.released.durationMillis).toBe(10_000);
      expect(view?.released.turns).toBe(0);
    }),
  );

  it.effect("keeps child defects defects and still settles the reservation", () =>
    Effect.gen(function* () {
      const Boom = Tool.make("boom_tool", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const boomTools = Toolkit.make(Boom);
      const boomChildDefinition = Agent.define("boom-child", {
        input: ChildInput,
        output: ChildOutput,
        instructions: "Boom.",
        toolkit: boomTools,
        policy: childPolicy,
      });
      const boomDelegation = Subagent.define("delegate_boom", {
        description: "Delegation whose child dies.",
        target: boomChildDefinition,
        parameters: ResearchParams,
        success: ResearchFindings,
        failure: ResearchDelegationFailed,
        prepareInput: ({ topic }) => Effect.succeed({ question: topic }),
        projectResult: (output) => Effect.succeed({ summary: output.answer }),
        policy: researchPolicy,
      });
      const boomParentDefinition = Agent.define("coordinator-boom", {
        input: Schema.Struct({ mission: Schema.String }),
        output: Schema.Struct({ report: Schema.String }),
        instructions: "Delegate, then answer as JSON.",
        toolkit: Toolkit.make(boomDelegation.tool),
        policy: parentPolicy,
      });
      const boomToolLayer = boomTools.toLayer({
        boom_tool: () => Effect.die(new Error("boom-defect")),
      });
      const childBinding = Agent.withModel(
        boomChildDefinition,
        delegatingModel(
          "boom-model",
          "boom_tool",
          [{ id: "boom-1", params: {} }],
          '{"answer":"x"}',
        ),
      );
      const parent = Agent.withModel(
        boomParentDefinition,
        delegatingModel(
          "parent-boom",
          "delegate_boom",
          [{ id: "call-1", params: { topic: "boom" } }],
          '{"report":"unreached"}',
        ),
      );
      const runId = decodeRunId("parent-run-defect");

      const exit = yield* AgentRuntime.run(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(
          Layer.provide(
            SubagentRuntime.layer(boomDelegation, childBinding, { mapChildFailure }),
            boomToolLayer,
          ),
        ),
        Effect.exit,
      );
      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        // The defect was not coerced into an expected Tool failure.
        expect(Option.isNone(Cause.findErrorOption(exit.cause))).toBe(true);
      }

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      expectSettledOnce(snapshot.reservations[0]);
    }),
  );

  it.effect("exhausts the delegation duration budget deterministically", () =>
    Effect.gen(function* () {
      const slowDelegation = Subagent.define("delegate_slow", {
        description: "Delegation with a small duration budget.",
        target: childDefinition,
        parameters: ResearchParams,
        success: ResearchFindings,
        failure: ResearchDelegationFailed,
        prepareInput: ({ topic }) => Effect.succeed({ question: topic }),
        projectResult: (output) => Effect.succeed({ summary: output.answer }),
        policy: SubagentPolicy.make({
          maxChildren: 1,
          maxConcurrency: 1,
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "5 seconds",
        }),
      });
      const slowParentDefinition = Agent.define("coordinator-slow", {
        input: Schema.Struct({ mission: Schema.String }),
        output: Schema.Struct({ report: Schema.String }),
        instructions: "Delegate, then answer as JSON.",
        toolkit: Toolkit.make(slowDelegation.tool),
        policy: parentPolicy,
      });
      const modelStarted = yield* Deferred.make<void>();
      const blockingModel = Model.make(
        "scripted",
        "slow-child",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Deferred.succeed(modelStarted, undefined).pipe(Effect.andThen(Effect.never)),
              ),
          }),
        ),
      );
      const childBinding = Agent.withModel(childDefinition, blockingModel);
      const parent = Agent.withModel(
        slowParentDefinition,
        delegatingModel(
          "parent-slow",
          "delegate_slow",
          [{ id: "call-1", params: { topic: "slow" } }],
          '{"report":"unreached"}',
        ),
      );
      const runId = decodeRunId("parent-run-duration");

      const fiber = yield* AgentRuntime.run(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(SubagentRuntime.layer(slowDelegation, childBinding, { mapChildFailure })),
        Effect.scoped,
        Effect.exit,
        Effect.forkChild,
      );
      yield* Deferred.await(modelStarted);
      yield* TestClock.adjust("5 seconds");
      const exit = yield* Fiber.join(fiber);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(SubagentBudgetExhausted);
      expect(failure).toMatchObject({ dimension: "duration", limitValue: 5_000 });

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      const view = snapshot.reservations[0];
      expectSettledOnce(view);
      expect(view?.observedConsumed.durationMillis).toBe(5_000);
      expect(view?.released.durationMillis).toBe(0);
    }),
  );

  it.effect("fails closed when the projected result escapes the success Schema", () =>
    Effect.gen(function* () {
      const BoundedFindings = Schema.Struct({
        summary: Schema.String.check(Schema.isMaxLength(8)),
      });
      const boundedDelegation = Subagent.define("delegate_bounded", {
        description: "Delegation with a bounded result Schema.",
        target: childDefinition,
        parameters: ResearchParams,
        success: BoundedFindings,
        failure: ResearchDelegationFailed,
        prepareInput: ({ topic }) => Effect.succeed({ question: topic }),
        projectResult: (output) => Effect.succeed({ summary: `oversized:${output.answer}` }),
        policy: researchPolicy,
      });
      const boundedParentDefinition = Agent.define("coordinator-bounded", {
        input: Schema.Struct({ mission: Schema.String }),
        output: Schema.Struct({ report: Schema.String }),
        instructions: "Delegate, then answer as JSON.",
        toolkit: Toolkit.make(boundedDelegation.tool),
        policy: parentPolicy,
      });
      const childBinding = Agent.withModel(
        childDefinition,
        answeringModel("bounded-child", '{"answer":"secret-child-answer"}'),
      );
      const parent = Agent.withModel(
        boundedParentDefinition,
        delegatingModel(
          "parent-bounded",
          "delegate_bounded",
          [{ id: "call-1", params: { topic: "bounded" } }],
          '{"report":"unreached"}',
        ),
      );
      const runId = decodeRunId("parent-run-projection");

      const detached = yield* AgentRuntime.start(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(SubagentRuntime.layer(boundedDelegation, childBinding, { mapChildFailure })),
      );
      const exit = yield* Effect.exit(detached.await);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(SubagentProjectionFailure);
      expect(failure).toMatchObject({ stage: "result" });
      // Fail closed: the raw child value never enters the failure message.
      if (failure instanceof SubagentProjectionFailure) {
        expect(failure.message).not.toContain("secret-child-answer");
      }

      const events = yield* detached.events;
      // The child itself completed; the join never happened.
      expect(findEvent(events, "SubagentCompleted")).toBeDefined();
      expect(findEvent(events, "SubagentJoined")).toBeUndefined();

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      expectSettledOnce(snapshot.reservations[0]);
    }),
  );

  it.effect("denies a child Tool outside the grant ceiling before any budget exists", () =>
    Effect.gen(function* () {
      const Probe = Tool.make("probe_docs", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const probeTools = Toolkit.make(Probe);
      const probeChildDefinition = Agent.define("probe-child", {
        input: ChildInput,
        output: ChildOutput,
        instructions: "Probe.",
        toolkit: probeTools,
        policy: childPolicy,
      });
      const restrictedDelegation = Subagent.define("delegate_restricted", {
        description: "Delegation with an empty grant ceiling.",
        target: probeChildDefinition,
        parameters: ResearchParams,
        success: ResearchFindings,
        failure: ResearchDelegationFailed,
        prepareInput: ({ topic }) => Effect.succeed({ question: topic }),
        projectResult: (output) => Effect.succeed({ summary: output.answer }),
        policy: researchPolicy,
        grant: SubagentGrant.make({ allowedToolNames: [], maxDepth: 1 }),
      });
      const restrictedParentDefinition = Agent.define("coordinator-restricted", {
        input: Schema.Struct({ mission: Schema.String }),
        output: Schema.Struct({ report: Schema.String }),
        instructions: "Delegate, then answer as JSON.",
        toolkit: Toolkit.make(restrictedDelegation.tool),
        policy: parentPolicy,
      });
      const probeToolLayer = probeTools.toLayer({
        probe_docs: () => Effect.succeed("probed"),
      });
      const childBinding = Agent.withModel(
        probeChildDefinition,
        answeringModel("probe-child-model", '{"answer":"never-runs"}'),
      );
      const parent = Agent.withModel(
        restrictedParentDefinition,
        delegatingModel(
          "parent-restricted",
          "delegate_restricted",
          [{ id: "call-1", params: { topic: "restricted" } }],
          '{"report":"unreached"}',
        ),
      );
      const runId = decodeRunId("parent-run-grant");

      const exit = yield* AgentRuntime.run(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(
          Layer.provide(
            SubagentRuntime.layer(restrictedDelegation, childBinding, { mapChildFailure }),
            probeToolLayer,
          ),
        ),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(SubagentPrestartDenied);
      expect(failure).toMatchObject({ reason: "grant-violation" });

      const reservations = yield* SubagentReservations;
      const snapshotExit = yield* Effect.exit(reservations.parentSnapshot(runId));
      expect(failureFrom(snapshotExit)._tag).toBe("SubagentParentBudgetUnknown");
    }),
  );

  it.effect("fails closed on a zero concurrent-children cap override", () =>
    Effect.gen(function* () {
      const childBinding = Agent.withModel(
        childDefinition,
        answeringModel("capped-child", '{"answer":"never-runs"}'),
      );
      const parent = Agent.withModel(
        coordinatorDefinition,
        delegatingModel(
          "parent-capped",
          "delegate_research",
          [{ id: "call-1", params: { topic: "capped" } }],
          '{"report":"unreached"}',
        ),
      );
      const runId = decodeRunId("parent-run-capped");

      const exit = yield* AgentRuntime.run(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(
          SubagentRuntime.layer(researchDelegation, childBinding, {
            mapChildFailure,
            parentCaps: SubagentDelegationCaps.make({ maxConcurrentChildren: 0 }),
          }),
        ),
        Effect.exit,
      );
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(SubagentBudgetExhausted);
      expect(failure).toMatchObject({ dimension: "concurrent-children" });

      // The reservation was taken before the slot denial and settled through
      // the finalizer: the never-started child observed zero usage.
      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      const view = snapshot.reservations[0];
      expectSettledOnce(view);
      expect(view?.released).toEqual(view?.allocated);
    }),
  );
});

describe("Subagent.define", () => {
  it("rejects delegation names outside the naming convention", () => {
    expect(() =>
      Subagent.define("research", {
        description: "Missing prefix.",
        target: childDefinition,
        parameters: ResearchParams,
        success: ResearchFindings,
        failure: ResearchDelegationFailed,
        prepareInput: ({ topic }) => Effect.succeed({ question: topic }),
        projectResult: (output) => Effect.succeed({ summary: output.answer }),
        policy: researchPolicy,
      }),
    ).toThrow();
  });

  it("rejects a target whose Toolkit already contains a delegation Tool", () => {
    const nestedDefinition = Agent.define("nested-target", {
      input: ChildInput,
      output: ChildOutput,
      instructions: "Nested.",
      toolkit: Toolkit.make(researchDelegation.tool),
      policy: childPolicy,
    });
    expect(() =>
      Subagent.define("delegate_nested", {
        description: "Nested delegation.",
        target: nestedDefinition,
        parameters: ResearchParams,
        success: ResearchFindings,
        failure: ResearchDelegationFailed,
        prepareInput: ({ topic }) => Effect.succeed({ question: topic }),
        projectResult: (output) => Effect.succeed({ summary: output.answer }),
        policy: researchPolicy,
      }),
    ).toThrow(/nested delegation/);
  });

  it("marks delegation Tools recognizably for preflight", () => {
    expect(isDelegationToolName("delegate_research")).toBe(true);
    expect(isDelegationToolName("delegate_anything_else")).toBe(true);
    expect(isDelegationToolName("search_docs")).toBe(false);
    expect(researchDelegation.delegationId).toBe("delegate_research");
    expect(researchDelegation.grant.allowedToolNames).toEqual([]);
    expect(researchDelegation.grant.maxDepth).toBe(1);
    expect(Object.isFrozen(researchDelegation)).toBe(true);
  });

  it("derives caps and allocation from the delegation policy", () => {
    const caps = delegationCapsFromPolicy(researchPolicy);
    expect(caps).toMatchObject({
      maxTotalChildInvocations: 2,
      maxConcurrentChildren: 2,
      maxTurns: 8,
      maxToolCalls: 8,
      maxDurationMillis: 20_000,
    });
    expect(caps.maxInputTokens).toBeUndefined();
    const allocation = delegationAllocationFromPolicy(researchPolicy);
    expect(allocation).toMatchObject({
      turns: 4,
      toolCalls: 4,
      durationMillis: 10_000,
      inputTokens: 0,
      outputTokens: 0,
      costMicrousd: 0,
      resultBytes: 0,
    });
  });
});

// ---------------------------------------------------------------------------
// Compile-time proofs (spec/subagents.md §4, §16.1)
// ---------------------------------------------------------------------------

class ChildModelConfig extends Context.Service<ChildModelConfig, { readonly modelName: string }>()(
  "@effect-agent/capabilities/test/ChildModelConfig",
) {}

class ChildCatalog extends Context.Service<
  ChildCatalog,
  { readonly search: Effect.Effect<ReadonlyArray<string>> }
>()("@effect-agent/capabilities/test/ChildCatalog") {}

class PrepareDirectory extends Context.Service<PrepareDirectory, { readonly prefix: string }>()(
  "@effect-agent/capabilities/test/PrepareDirectory",
) {}

class ProjectStamper extends Context.Service<ProjectStamper, { readonly stamp: string }>()(
  "@effect-agent/capabilities/test/ProjectStamper",
) {}

class SearchFailure extends Schema.TaggedErrorClass<SearchFailure>()("SearchFailure", {
  message: Schema.String,
}) {}

const SearchDocs = Tool.make("search_docs", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Array(Schema.String),
  failure: SearchFailure,
  dependencies: [ChildCatalog],
});
const typedChildTools = Toolkit.make(SearchDocs);

const typedChildDefinition = Agent.define("typed-child", {
  input: ChildInput,
  output: ChildOutput,
  instructions: "Search, then answer as JSON.",
  toolkit: typedChildTools,
  policy: childPolicy,
});

const typedModel = Model.make(
  "scripted",
  "typed-model",
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      yield* ChildModelConfig;
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.empty,
      });
    }),
  ),
);

const typedDelegation = Subagent.define("delegate_typed", {
  description: "Typed delegation for compile proofs.",
  target: typedChildDefinition,
  parameters: ResearchParams,
  success: ResearchFindings,
  failure: ResearchDelegationFailed,
  prepareInput: ({ topic }) =>
    Effect.gen(function* () {
      const directory = yield* PrepareDirectory;
      return { question: `${directory.prefix}:${topic}` };
    }),
  projectResult: (output) =>
    Effect.gen(function* () {
      const stamper = yield* ProjectStamper;
      return { summary: `${stamper.stamp}:${output.answer}` };
    }),
  policy: researchPolicy,
});

const typedBinding = Agent.withModel(typedChildDefinition, typedModel);

const typedLayer = SubagentRuntime.layer(typedDelegation, typedBinding, {
  mapChildFailure: (failure) => ResearchDelegationFailed.make({ childErrorTag: failure._tag }),
});

const typedParentDefinition = Agent.define("typed-parent", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate, then answer as JSON.",
  toolkit: Toolkit.make(typedDelegation.tool),
  policy: parentPolicy,
});
const typedParent = Agent.withModel(
  typedParentDefinition,
  answeringModel("typed-parent-model", '{"report":"typed"}'),
);
const typedProgram = AgentRuntime.run(typedParent, { mission: "m" }).pipe(
  Effect.provide(typedLayer),
);

type LayerContext<L> = L extends Layer.Layer<infer _ROut, infer _E, infer RIn> ? RIn : never;
type TypedLayerRequirements = LayerContext<typeof typedLayer>;
type TypedProgramServices = Effect.Services<typeof typedProgram>;
type TypedHandlerError = Tool.HandlerError<typeof typedDelegation.tool>;
type TypedHandlerServices = Tool.HandlerServices<typeof typedDelegation.tool>;

// The delegation exposes a real Effect AI Tool (SUB-001).
type ToolProof = Assert<Equal<typeof typedDelegation.tool extends Tool.Any ? true : false, true>>;
// Per-call handler failures are exactly the declared Tool failure union plus
// Effect AI's own error (spec §4.2); nothing else can leak through `E`.
type HandlerErrorProof = Assert<
  Equal<
    TypedHandlerError,
    | ResearchDelegationFailed
    | SubagentPrestartDenied
    | SubagentBudgetExhausted
    | SubagentProjectionFailure
    | AiError.AiError
  >
>;
// Per-call handler services are exactly the declared engine dependencies
// (SUB-003); child requirements never appear here.
type HandlerSpawnerProof = Assert<Equal<Extract<TypedHandlerServices, AgentSpawner>, AgentSpawner>>;
type HandlerSinkProof = Assert<Equal<Extract<TypedHandlerServices, RunEventSink>, RunEventSink>>;
type HandlerIdGeneratorProof = Assert<
  Equal<Extract<TypedHandlerServices, IdGenerator>, IdGenerator>
>;
type HandlerHidesModelProof = Assert<Equal<Extract<TypedHandlerServices, ChildModelConfig>, never>>;
type HandlerHidesReservationsProof = Assert<
  Equal<Extract<TypedHandlerServices, SubagentReservations>, never>
>;
// Layer construction carries the child Model, child Tool handler, projection,
// and reservation requirements visibly (spec §4 compile-proof list).
type LayerModelProof = Assert<
  Equal<Extract<TypedLayerRequirements, ChildModelConfig>, ChildModelConfig>
>;
type LayerChildToolServiceProof = Assert<
  Equal<Extract<TypedLayerRequirements, ChildCatalog>, ChildCatalog>
>;
type LayerChildHandlersProof = Assert<
  Equal<
    Extract<TypedLayerRequirements, Tool.HandlersFor<Toolkit.Tools<typeof typedChildTools>>>,
    Tool.HandlersFor<Toolkit.Tools<typeof typedChildTools>>
  >
>;
type LayerPrepareProof = Assert<
  Equal<Extract<TypedLayerRequirements, PrepareDirectory>, PrepareDirectory>
>;
type LayerProjectProof = Assert<
  Equal<Extract<TypedLayerRequirements, ProjectStamper>, ProjectStamper>
>;
type LayerReservationsProof = Assert<
  Equal<Extract<TypedLayerRequirements, SubagentReservations>, SubagentReservations>
>;
// Engine-provided Tool services are never construction requirements.
type LayerSpawnerExcludedProof = Assert<
  Equal<Extract<TypedLayerRequirements, AgentSpawner>, never>
>;
type LayerSinkExcludedProof = Assert<Equal<Extract<TypedLayerRequirements, RunEventSink>, never>>;
// The provided parent program still needs the child requirements but never
// the engine-provided services.
type ProgramModelProof = Assert<
  Equal<Extract<TypedProgramServices, ChildModelConfig>, ChildModelConfig>
>;
type ProgramReservationsProof = Assert<
  Equal<Extract<TypedProgramServices, SubagentReservations>, SubagentReservations>
>;
type ProgramSpawnerExcludedProof = Assert<
  Equal<Extract<TypedProgramServices, AgentSpawner>, never>
>;
type ProgramSinkExcludedProof = Assert<Equal<Extract<TypedProgramServices, RunEventSink>, never>>;

const unboundDefinitionRejected = () =>
  SubagentRuntime.layer(
    typedDelegation,
    // @ts-expect-error an unbound child Definition cannot be executed
    typedChildDefinition,
    {
      mapChildFailure: (failure) => ResearchDelegationFailed.make({ childErrorTag: failure._tag }),
    },
  );

// The expected child-failure domain covers the child's Tool failures and the
// interpreter's own failures; a mapping over any narrower domain is not
// assignable, so a non-total mapping is a compile error (SUB-028).
type TypedChildFailure = SubagentChildRunFailure<
  typeof ChildInput,
  typeof ChildOutput,
  string,
  Toolkit.Tools<typeof typedChildTools>,
  "scripted",
  never,
  never
>;
type MappingDomainToolFailureProof = Assert<
  Equal<Extract<TypedChildFailure, SearchFailure>, SearchFailure>
>;
type MappingDomainOutputFailureProof = Assert<
  Equal<Extract<TypedChildFailure, AgentOutputError>, AgentOutputError>
>;
type PartialMappingRejectedProof = Assert<
  Equal<
    ((failure: SearchFailure) => ResearchDelegationFailed) extends (
      failure: TypedChildFailure,
    ) => ResearchDelegationFailed
      ? true
      : false,
    false
  >
>;

describe("Subagent type proofs", () => {
  it("keeps per-call and construction requirements distinct", () => {
    const toolProof: ToolProof = true;
    const handlerErrorProof: HandlerErrorProof = true;
    const handlerSpawnerProof: HandlerSpawnerProof = true;
    const handlerSinkProof: HandlerSinkProof = true;
    const handlerIdGeneratorProof: HandlerIdGeneratorProof = true;
    const handlerHidesModelProof: HandlerHidesModelProof = true;
    const handlerHidesReservationsProof: HandlerHidesReservationsProof = true;
    const layerModelProof: LayerModelProof = true;
    const layerChildToolServiceProof: LayerChildToolServiceProof = true;
    const layerChildHandlersProof: LayerChildHandlersProof = true;
    const layerPrepareProof: LayerPrepareProof = true;
    const layerProjectProof: LayerProjectProof = true;
    const layerReservationsProof: LayerReservationsProof = true;
    const layerSpawnerExcludedProof: LayerSpawnerExcludedProof = true;
    const layerSinkExcludedProof: LayerSinkExcludedProof = true;
    const programModelProof: ProgramModelProof = true;
    const programReservationsProof: ProgramReservationsProof = true;
    const programSpawnerExcludedProof: ProgramSpawnerExcludedProof = true;
    const programSinkExcludedProof: ProgramSinkExcludedProof = true;
    const mappingDomainToolFailureProof: MappingDomainToolFailureProof = true;
    const mappingDomainOutputFailureProof: MappingDomainOutputFailureProof = true;
    const partialMappingRejectedProof: PartialMappingRejectedProof = true;

    expect([
      toolProof,
      handlerErrorProof,
      handlerSpawnerProof,
      handlerSinkProof,
      handlerIdGeneratorProof,
      handlerHidesModelProof,
      handlerHidesReservationsProof,
      layerModelProof,
      layerChildToolServiceProof,
      layerChildHandlersProof,
      layerPrepareProof,
      layerProjectProof,
      layerReservationsProof,
      layerSpawnerExcludedProof,
      layerSinkExcludedProof,
      programModelProof,
      programReservationsProof,
      programSpawnerExcludedProof,
      programSinkExcludedProof,
      mappingDomainToolFailureProof,
      mappingDomainOutputFailureProof,
      partialMappingRejectedProof,
    ]).not.toContain(false);
    expect(unboundDefinitionRejected).toBeInstanceOf(Function);
  });
});

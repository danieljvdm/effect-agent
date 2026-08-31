import {
  Agent,
  AgentId,
  AgentPolicy,
  ConversationId,
  DelegationId,
  IdGenerator,
  RunId,
  type RunEvent,
  type SubagentParentLink,
  ToolCallId,
  TurnId,
} from "@effect-agent/core";
import { expect, layer } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

import { ConversationHistory } from "../src/conversation-history.ts";
import {
  AgentRuntime,
  AgentSpawner,
  RunEventSink,
  RunEventSinkClosedError,
  type RunEventSinkService,
  type SubagentEventBasePayload,
} from "../src/index.ts";

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeRunId = Schema.decodeSync(RunId);
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

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const modelFromParts = (name: string, parts: ReadonlyArray<Response.StreamPartEncoded>) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.fromIterable(parts),
      }),
    ),
  );

/** Turn one declares the `delegate` Tool Call; turn two returns the final answer. */
const delegatingModel = (name: string, params: unknown, answerText: string) =>
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
                          {
                            type: "tool-call",
                            id: "delegate-1",
                            name: "delegate",
                            params,
                            providerExecuted: false,
                          },
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

const simpleDefinition = Agent.make("seam-simple", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const delegationId = Schema.decodeSync(DelegationId)("delegation-research");
const delegateCallId = Schema.decodeSync(ToolCallId)("delegate-1");

/** Delegation Tool whose handler emits Subagent events through the engine sink. */
const EmittingDelegate = Tool.make("delegate", {
  parameters: Schema.Struct({ question: Schema.String }),
  success: Schema.Struct({ value: Schema.String }),
}).addDependency(RunEventSink);
const emittingTools = Toolkit.make(EmittingDelegate);
const emittingDefinition = Agent.make("seam-emitting-parent", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Delegate, then answer as JSON.",
  toolkit: emittingTools,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

/** Delegation Tool whose handler runs a real child through `AgentSpawner`. */
const SpawningDelegate = Tool.make("delegate", {
  parameters: Schema.Struct({ question: Schema.String }),
  success: Schema.Struct({ answer: Schema.String }),
})
  .addDependency(AgentSpawner)
  .addDependency(IdGenerator);
const spawningTools = Toolkit.make(SpawningDelegate);
const spawningDefinition = Agent.make("seam-spawning-parent", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Delegate, then answer as JSON.",
  toolkit: spawningTools,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const fabricatedChildIdentity: SubagentEventBasePayload = {
  toolCallId: delegateCallId,
  delegationId,
  childConversationId: Schema.decodeSync(ConversationId)("child-conversation"),
  childRunId: Schema.decodeSync(RunId)("child-run"),
  targetAgentId: Schema.decodeSync(AgentId)("child-agent"),
  depth: 1,
};

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

const isSubagentEvent = (event: RunEvent) => event._tag.startsWith("Subagent");

const testLayer = Layer.merge(identifiers, ConversationHistory.layerTransient);

layer(testLayer)("SUB S1 engine execution seam", (it) => {
  it.effect("honors preallocated Conversation and Run identity in every emitted event", () => {
    const conversationId = Schema.decodeSync(ConversationId)("conversation-preallocated");
    const runId = Schema.decodeSync(RunId)("run-preallocated");
    const agent = Agent.withModel(
      simpleDefinition,
      modelFromParts("preallocated", finalParts('{"answer":"ok"}')),
    );

    return Effect.gen(function* () {
      const events = yield* AgentRuntime.stream(
        agent,
        { question: "who?" },
        { conversationId, runId },
      ).pipe(Stream.runCollect);

      expect(events.length).toBeGreaterThan(0);
      for (const event of events) {
        expect(event.conversationId).toBe(conversationId);
        expect(event.runId).toBe(runId);
      }
      expect(events.at(-1)?._tag).toBe("RunCompleted");
    });
  });

  it.effect(
    "weaves sink-emitted Subagent events into the parent stream with a stamped base",
    () => {
      const toolLayer = emittingTools.toLayer({
        delegate: () =>
          Effect.gen(function* () {
            const sink = yield* RunEventSink;
            yield* sink.emit({ _tag: "SubagentRequested", ...fabricatedChildIdentity });
            yield* sink.emit({ _tag: "SubagentStarted", ...fabricatedChildIdentity });
            yield* sink.emit({
              _tag: "SubagentProgress",
              ...fabricatedChildIdentity,
              summary: "halfway",
            });
            yield* sink.emit({
              _tag: "SubagentCompleted",
              ...fabricatedChildIdentity,
              turns: 1,
              finishReason: "completed",
            });
            yield* sink.emit({ _tag: "SubagentJoined", ...fabricatedChildIdentity });
            return { value: "done" };
            // The sink cannot be closed while this batch is live.
          }).pipe(Effect.orDie),
      });
      const agent = Agent.withModel(
        emittingDefinition,
        delegatingModel("emitting", { question: "child?" }, '{"answer":"joined"}'),
      );

      return Effect.gen(function* () {
        const events = yield* AgentRuntime.stream(
          agent,
          { question: "root?" },
          { bufferLimits: { maxSubagentEventsPerBatch: 1 } },
        ).pipe(Stream.runCollect, Effect.provide(toolLayer));

        const subagentEvents = events.filter(isSubagentEvent);
        expect(subagentEvents.map((event) => event._tag)).toEqual([
          "SubagentRequested",
          "SubagentStarted",
          "SubagentProgress",
          "SubagentCompleted",
          "SubagentJoined",
        ]);

        const runStarted = events.find((event) => event._tag === "RunStarted");
        const firstTurn = events.find((event) => event._tag === "TurnStarted");
        expect(runStarted).toBeDefined();
        expect(firstTurn).toBeDefined();
        for (const event of subagentEvents) {
          // Engine-stamped base identity is the parent's, plus the batch Turn.
          expect(event.eventVersion).toBe(1);
          expect(event.conversationId).toBe(runStarted?.conversationId);
          expect(event.runId).toBe(runStarted?.runId);
          expect(event.agentId).toBe(emittingDefinition.id);
          expect(event.turnId).toBe(firstTurn?.turnId);
          expect("toolCallId" in event && event.toolCallId).toBe(delegateCallId);
        }
        const completed = subagentEvents.find((event) => event._tag === "SubagentCompleted");
        const progress = subagentEvents.find((event) => event._tag === "SubagentProgress");
        expect(completed).toMatchObject({
          delegationId,
          childConversationId: "child-conversation",
          childRunId: "child-run",
          targetAgentId: "child-agent",
          depth: 1,
          turns: 1,
          finishReason: "completed",
        });
        expect(progress).toMatchObject({ summary: "halfway" });

        // Sink events surface inside the settled batch: after the declaring
        // Turn's Tool Call, before the next Turn starts.
        const tags = events.map((event) => event._tag);
        const declaredIndex = tags.indexOf("ToolCallDeclared");
        const secondTurnIndex = tags.indexOf("TurnStarted", tags.indexOf("TurnStarted") + 1);
        const requestedIndex = tags.indexOf("SubagentRequested");
        const joinedIndex = tags.indexOf("SubagentJoined");
        expect(requestedIndex).toBeGreaterThan(declaredIndex);
        expect(joinedIndex).toBeLessThan(secondTurnIndex);

        // Sequences are stamped through the shared monotonic counter.
        expect(new Set(events.map((event) => event.sequence)).size).toBe(events.length);
        const subagentSequences = subagentEvents.map((event) => event.sequence);
        for (let index = 1; index < subagentSequences.length; index += 1) {
          expect(subagentSequences[index]).toBeGreaterThan(subagentSequences[index - 1] ?? -1);
        }
      });
    },
  );

  it.effect("fails closed when a handler emits after its Tool batch settled", () =>
    Effect.gen(function* () {
      const leakedSink = yield* Deferred.make<RunEventSinkService>();
      const toolLayer = emittingTools.toLayer({
        delegate: () =>
          Effect.gen(function* () {
            const sink = yield* RunEventSink;
            yield* Deferred.succeed(leakedSink, sink);
            return { value: "done" };
          }),
      });
      const agent = Agent.withModel(
        emittingDefinition,
        delegatingModel("leaky", { question: "child?" }, '{"answer":"settled"}'),
      );

      const result = yield* AgentRuntime.run(agent, { question: "root?" }).pipe(
        Effect.provide(toolLayer),
      );
      expect(result.output).toEqual({ answer: "settled" });

      const sink = yield* Deferred.await(leakedSink);
      const exit = yield* sink
        .emit({ _tag: "SubagentRequested", ...fabricatedChildIdentity })
        .pipe(Effect.exit);
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(RunEventSinkClosedError);
      expect(failure.message).toContain("settled");
    }),
  );

  it.effect(
    "spawns a scripted child with preallocated identity, Parent Link, and observable events",
    () =>
      Effect.gen(function* () {
        const rootDepth = yield* Ref.make(-1);
        const childDepth = yield* Ref.make(-1);
        const captured = yield* Ref.make<
          | {
              readonly conversationId: string;
              readonly runId: string;
              readonly parentLink: SubagentParentLink;
              readonly childEvents: ReadonlyArray<RunEvent>;
            }
          | undefined
        >(undefined);

        const Probe = Tool.make("probe", {
          parameters: Schema.Struct({}),
          success: Schema.Struct({ acknowledged: Schema.Boolean }),
        }).addDependency(AgentSpawner);
        const childTools = Toolkit.make(Probe);
        const childDefinition = Agent.make("seam-child", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Probe, then answer as JSON.",
          toolkit: childTools,
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const childModel = Model.make(
          "scripted",
          "probing-child",
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
                                {
                                  type: "tool-call",
                                  id: "probe-1",
                                  name: "probe",
                                  params: {},
                                  providerExecuted: false,
                                },
                                { type: "finish", reason: "tool-calls", usage },
                              ]
                            : finalParts('{"answer":"child-answer"}'),
                        ),
                      ),
                    ),
                  ),
              });
            }),
          ),
        );
        const childBinding = Agent.withModel(childDefinition, childModel);
        const childToolLayer = childTools.toLayer({
          probe: () =>
            Effect.gen(function* () {
              const spawner = yield* AgentSpawner;
              yield* Ref.set(childDepth, spawner.depth);
              return { acknowledged: true };
            }),
        });

        const parentToolLayer = spawningTools.toLayer({
          delegate: ({ question }) =>
            Effect.gen(function* () {
              const spawner = yield* AgentSpawner;
              yield* Ref.set(rootDepth, spawner.depth);
              const child = yield* spawner.spawn(
                childBinding,
                { question },
                { delegationId, parentToolCallId: delegateCallId },
              );
              const result = yield* child.await.pipe(Effect.orDie);
              const childEvents = yield* child.events;
              yield* Ref.set(captured, {
                conversationId: child.conversationId,
                runId: child.runId,
                parentLink: child.parentLink,
                childEvents,
              });
              return { answer: result.output.answer };
            }).pipe(Effect.provide(childToolLayer), Effect.scoped),
        });
        const parent = Agent.withModel(
          spawningDefinition,
          delegatingModel("spawning", { question: "child?" }, '{"answer":"parent-answer"}'),
        );

        const result = yield* AgentRuntime.run(parent, { question: "root?" }).pipe(
          Effect.provide(parentToolLayer),
        );

        expect(result.output).toEqual({ answer: "parent-answer" });
        expect(yield* Ref.get(rootDepth)).toBe(0);
        expect(yield* Ref.get(childDepth)).toBe(1);

        const snapshot = yield* Ref.get(captured);
        expect(snapshot).toBeDefined();
        if (snapshot === undefined) {
          throw new Error("Expected the delegate handler to capture the spawned child");
        }
        // The child owns fresh, distinct identity supplied by the spawner.
        expect(snapshot.conversationId).not.toBe(result.conversationId);
        expect(snapshot.runId).not.toBe(result.runId);
        expect(snapshot.parentLink).toMatchObject({
          delegationId,
          parentAgentId: spawningDefinition.id,
          parentConversationId: result.conversationId,
          parentRunId: result.runId,
          parentToolCallId: delegateCallId,
          depth: 1,
        });
        // The preallocated identity flowed through the interpreter unchanged.
        const childStarted = snapshot.childEvents.at(0);
        expect(childStarted?._tag).toBe("RunStarted");
        expect(childStarted?.conversationId).toBe(snapshot.conversationId);
        expect(childStarted?.runId).toBe(snapshot.runId);
        expect(snapshot.childEvents.at(-1)?._tag).toBe("RunCompleted");
      }),
  );

  it.effect("interrupts the spawned child and runs its finalizers on parent interruption", () =>
    Effect.gen(function* () {
      const childToolStarted = yield* Deferred.make<void>();
      const childToolFinalized = yield* Deferred.make<void>();
      const childModelFinalized = yield* Deferred.make<void>();
      const handlerFinalized = yield* Deferred.make<void>();

      const Block = Tool.make("block", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const childTools = Toolkit.make(Block);
      const childToolLayer = childTools.toLayer({
        block: () =>
          Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.acquireRelease(Deferred.succeed(childToolStarted, undefined), () =>
                Deferred.succeed(childToolFinalized, undefined),
              );
              return yield* Effect.never;
            }),
          ),
      });
      const childDefinition = Agent.make("seam-blocking-child", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Block forever.",
        toolkit: childTools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const childModel = Model.make(
        "scripted",
        "blocking-child",
        Layer.effect(
          LanguageModel.LanguageModel,
          Effect.acquireRelease(
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: "tool-call",
                    id: "block-1",
                    name: "block",
                    params: {},
                    providerExecuted: false,
                  },
                  { type: "finish", reason: "tool-calls", usage },
                ]),
            }),
            () => Deferred.succeed(childModelFinalized, undefined),
          ),
        ),
      );
      const childBinding = Agent.withModel(childDefinition, childModel);

      const parentToolLayer = spawningTools.toLayer({
        delegate: ({ question }) =>
          Effect.gen(function* () {
            const spawner = yield* AgentSpawner;
            yield* Effect.acquireRelease(Effect.void, () =>
              Deferred.succeed(handlerFinalized, undefined),
            );
            const child = yield* spawner.spawn(
              childBinding,
              { question },
              { delegationId, parentToolCallId: delegateCallId },
            );
            const result = yield* child.await.pipe(Effect.orDie);
            return { answer: result.output.answer };
          }).pipe(Effect.provide(childToolLayer), Effect.scoped),
      });
      const parent = Agent.withModel(
        spawningDefinition,
        delegatingModel("interrupted-parent", { question: "child?" }, '{"answer":"unreached"}'),
      );

      const fiber = yield* AgentRuntime.run(parent, { question: "root?" }).pipe(
        Effect.provide(parentToolLayer),
        Effect.scoped,
        Effect.exit,
        Effect.forkChild,
      );
      yield* Deferred.await(childToolStarted);
      yield* Fiber.interrupt(fiber);

      // Parent interruption reached the child Run, its Tool handler, its
      // model Layer, and the delegating handler's own scoped region.
      yield* Deferred.await(childToolFinalized);
      yield* Deferred.await(childModelFinalized);
      yield* Deferred.await(handlerFinalized);
      expect(yield* Deferred.isDone(childToolFinalized)).toBe(true);
      expect(yield* Deferred.isDone(childModelFinalized)).toBe(true);
      expect(yield* Deferred.isDone(handlerFinalized)).toBe(true);
    }),
  );

  it.effect("keeps engine-provided Tool services out of the public requirements", () => {
    const toolLayer = spawningTools.toLayer({
      delegate: () => Effect.succeed({ answer: "typed" }),
    });
    const agent = Agent.withModel(
      spawningDefinition,
      delegatingModel("typed", { question: "child?" }, '{"answer":"typed"}'),
    );
    const program = AgentRuntime.run(agent, { question: "types" });

    type Services = Effect.Services<typeof program>;
    type SpawnerExcluded = [Extract<Services, AgentSpawner>] extends [never] ? true : false;
    type SinkExcluded = [Extract<Services, RunEventSink>] extends [never] ? true : false;
    type IdGeneratorKept = IdGenerator extends Services ? true : false;
    const spawnerExcluded: SpawnerExcluded = true;
    const sinkExcluded: SinkExcluded = true;
    const idGeneratorKept: IdGeneratorKept = true;

    expect({ spawnerExcluded, sinkExcluded, idGeneratorKept }).toEqual({
      spawnerExcluded: true,
      sinkExcluded: true,
      idGeneratorKept: true,
    });
    return AgentRuntime.run(agent, { question: "types" }).pipe(
      Effect.provide(toolLayer),
      Effect.asVoid,
    );
  });
});

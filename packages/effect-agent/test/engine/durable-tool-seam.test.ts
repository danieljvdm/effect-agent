import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Clock,
  DateTime,
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
import * as Agent from "effect-agent/agent";
import { AgentPolicyError } from "effect-agent/agent-error";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import { type RunEvent } from "effect-agent/run-event";
import { RunContextPreparationPassthrough, type RunTurnResume } from "effect-agent/run-options";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const oneCallResumeUsage = {
  committedTurns: 1,
  toolCalls: 1,
  programmaticToolCalls: 0,
  consecutiveToolFailures: 0,
  finalizationUsed: false,
  modelCalls: 1,
  inputTokens: 0,
  outputTokens: 0,
  lastInputTokens: 0,
  lastOutputTokens: 0,
  costMicrousd: 0,
};

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => Schema.decodeSync(ThreadId)(`thread-1-${++threadSequence}`)),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-1")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-1")),
});

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

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

const resumeTurnId = Schema.decodeSync(TurnId)("turn-resume");

const policy = (overrides?: Partial<Parameters<typeof AgentPolicy.make>[0]>) =>
  AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
    ...overrides,
  });

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("P5 WP1 durable Tool seams", (it) => {
  it.effect("future expiry interrupts a forged resumed Tool and runs its finalizer", () =>
    Effect.gen(function* () {
      const handlerStarted = yield* Deferred.make<void>();
      const handlerFinalized = yield* Deferred.make<void>();
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      let modelCalls = 0;

      const DelegateLookup = Tool.make("delegate_lookup", {
        parameters: Schema.Struct({ key: Schema.String }),
        success: Schema.String,
      });

      const tools = Toolkit.make(DelegateLookup);

      const definition = Agent.make("cleanup-deadline", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Join, then answer.",
        toolkit: tools,
        policy: policy({ maxDuration: "5 seconds" }),
      });

      const model = Model.make(
        "scripted",
        "cleanup-deadline",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              modelCalls += 1;

              return Stream.fromIterable(finalParts('{"answer":"too late"}'));
            },
          }),
        ),
      );

      const toolLayer = tools.toLayer({
        delegate_lookup: () =>
          Deferred.succeed(handlerStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(handlerFinalized, undefined)),
          ),
      });

      const resume: RunTurnResume & {
        readonly settledChildJoinCallIdsPastDeadline: ReadonlyArray<string>;
      } = {
        turn: 1,
        turnId: resumeTurnId,
        calls: [{ id: "call-child", name: "delegate_lookup", params: { key: "a" } }],
        settled: [],
        settledChildJoinCallIdsPastDeadline: ["call-child"],
      };

      const now = yield* Clock.currentTimeMillis;

      const durationDeadline = DateTime.addDuration(
        DateTime.toUtc(DateTime.makeUnsafe(now)),
        "5 seconds",
      );

      const fiber = yield* AgentRuntime.stream(
        Agent.withModel(definition, model),
        { question: "resume" },
        { durationDeadline, resume, resumeUsage: oneCallResumeUsage },
      ).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.forkChild,
      );

      yield* Deferred.await(handlerStarted);
      yield* TestClock.adjust("5 seconds");
      const exit = yield* Fiber.await(fiber);
      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(AgentPolicyError);
      expect(failure).toMatchObject({ limit: "duration" });
      expect(yield* Deferred.isDone(handlerFinalized)).toBe(true);
      expect(modelCalls).toBe(0);
      expect(observed.filter((event) => event._tag === "ToolCallStarted")).toHaveLength(1);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
      expect(observed.filter((event) => event._tag === "RunCompleted")).toHaveLength(0);
    }),
  );
});

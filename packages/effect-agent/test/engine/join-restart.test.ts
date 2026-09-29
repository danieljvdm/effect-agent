import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Schema, Stream, Tracer } from "effect";
import * as Agent from "effect-agent/agent";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import type { RunEvent } from "effect-agent/run-event";
import type { RunInputCommand } from "effect-agent/run-options";
import { ThreadHistory } from "effect-agent/thread-history";
import { LanguageModel, Model, type Response, Toolkit } from "effect/unstable/ai";

// Requested engine seam: an in-flight joined input must replace disposable drafts,
// retain reported usage and stop restarting after two cancellations in the same Run.
it.live("restarts disposable calls twice, discards their drafts and then drains at the seam", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const requests = yield* Effect.forEach([0, 1, 2, 3], () => Deferred.make<void>());
      const releases = yield* Effect.forEach([0, 1, 2], () => Deferred.make<void>());
      let wake = yield* Deferred.make<void>();
      const queued: Array<RunInputCommand> = [];
      const prompts: Array<string> = [];
      const events: Array<RunEvent> = [];
      const spans: Array<Tracer.NativeSpan> = [];
      const cancelled: Array<number> = [];
      let turnIds = 0;

      const model = Model.make(
        "scripted",
        "join-restart",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (options) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const index = prompts.length;

                  prompts.push(JSON.stringify(options.prompt));

                  const parts: ReadonlyArray<Response.StreamPartEncoded> = [
                    { type: "text-start", id: "draft" },
                    { type: "text-delta", id: "draft", delta: `"answer-${index}"` },
                    { type: "text-end", id: "draft" },
                    {
                      type: "finish",
                      reason: "stop",
                      usage: { inputTokens: { total: 10 }, outputTokens: { total: 2 } },
                    },
                  ];

                  return Stream.fromIterable(parts).pipe(
                    Stream.concat(
                      Stream.fromEffect(
                        Effect.gen(function* () {
                          yield* Deferred.succeed(requests[index]!, undefined);
                          if (index < 3) yield* Deferred.await(releases[index]!);
                        }),
                      ).pipe(Stream.drain),
                    ),
                    Stream.onExit((exit) =>
                      Effect.sync(() => {
                        if (exit._tag === "Failure") cancelled.push(index);
                      }),
                    ),
                  );
                }),
              ),
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("join-restart", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer all input.",
          toolkit: Toolkit.empty,
          policy: { maxTurns: 3, restartOnJoinedInput: true },
        }),
        model,
      );

      const fiber = yield* AgentRuntime.stream(agent, "first", {
        input: {
          awaitJoin: Effect.suspend(() => Deferred.await(wake)),
          drain: () => Effect.sync(() => queued.splice(0)),
        },
      }).pipe(
        Stream.tap((event) => Effect.sync(() => events.push(event))),
        Stream.runCollect,
        Effect.provide(
          Layer.mergeAll(
            ThreadHistory.layer,
            Layer.succeed(IdGenerator, {
              nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("join-thread")),
              nextRunId: Effect.succeed(Schema.decodeSync(RunId)("join-run")),
              nextTurnId: Effect.sync(() => Schema.decodeSync(TurnId)(`turn-${++turnIds}`)),
            }),
          ),
        ),
        Effect.provideService(
          Tracer.Tracer,
          Tracer.make({
            span: (options) => {
              const span = new Tracer.NativeSpan(options);

              spans.push(span);

              return span;
            },
          }),
        ),
        Effect.forkScoped,
      );

      for (const index of [0, 1]) {
        yield* Deferred.await(requests[index]!);
        queued.push({ kind: "steering", input: `joined-${index}` });
        const prior = wake;

        wake = yield* Deferred.make<void>();
        yield* Deferred.succeed(prior, undefined);
        yield* Deferred.await(requests[index + 1]!).pipe(Effect.timeout("2 seconds"));
      }
      queued.push({ kind: "steering", input: "after-cap" });
      yield* Deferred.succeed(wake, undefined);
      yield* Deferred.succeed(releases[2]!, undefined);
      yield* Fiber.join(fiber);
      expect(cancelled).toEqual([0, 1]);
      expect(prompts).toHaveLength(4);
      expect(prompts[1]).toContain("first");
      expect(prompts[1]).toContain("joined-0");
      expect(prompts[2]).toContain("joined-1");
      expect(prompts[2]).not.toContain("answer-0");
      expect(prompts[3]).toContain("after-cap");
      expect(events.filter((event) => event._tag === "ModelRestarted")).toHaveLength(2);
      expect(events.filter((event) => event._tag === "RunCompleted")).toHaveLength(1);
      const completed = events.find((event) => event._tag === "RunCompleted");

      expect(completed).toMatchObject({
        usage: { modelCalls: 4, inputTokens: 40, outputTokens: 8 },
      });
      const chats = spans.filter((span) => span.name === "chat join-restart");

      expect(
        chats.slice(0, 2).map((span) => span.attributes.get("effect_agent.model.abort_reason")),
      ).toEqual(["joined-input", "joined-input"]);
      expect(
        chats
          .slice(0, 2)
          .every((span) => span.status._tag === "Ended" && span.status.exit._tag === "Failure"),
      ).toBe(true);
    }),
  ),
);

// A cancelled call may report usage before its stream closes. A replacement must
// honor the same hard budget rail instead of treating accounting as best effort.
it.live("charges aborted usage before admitting a replacement against the cost budget", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const reported = yield* Deferred.make<void>();
      const joined = yield* Deferred.make<void>();
      let calls = 0;

      const model = Model.make(
        "scripted",
        "over-budget",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => {
              calls++;

              return Stream.fromIterable<Response.StreamPartEncoded>([
                { type: "text-start", id: "answer" },
                { type: "text-delta", id: "answer", delta: '"old"' },
                { type: "text-end", id: "answer" },
                {
                  type: "finish",
                  reason: "stop",
                  usage: { inputTokens: { total: 10 }, outputTokens: { total: 2 } },
                },
              ]).pipe(
                Stream.concat(
                  Stream.fromEffect(
                    Deferred.succeed(reported, undefined).pipe(Effect.andThen(Effect.never)),
                  ),
                ),
              );
            },
          }),
        ),
      );

      const agent = Agent.withModel(
        Agent.make("over-budget", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer.",
          toolkit: Toolkit.empty,
          policy: { restartOnJoinedInput: true, costBudgetMicrousd: 1 },
        }),
        model,
      );

      const fiber = yield* AgentRuntime.run(agent, "first", {
        input: { awaitJoin: Deferred.await(joined), drain: () => Effect.succeed([]) },
        estimateCostMicrousd: () => Effect.succeed(2),
      }).pipe(
        Effect.exit,
        Effect.provide(
          Layer.mergeAll(
            ThreadHistory.layer,
            Layer.succeed(IdGenerator, {
              nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("cost-thread")),
              nextRunId: Effect.succeed(Schema.decodeSync(RunId)("cost-run")),
              nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("cost-turn")),
            }),
          ),
        ),
        Effect.forkScoped,
      );

      yield* Deferred.await(reported);
      yield* Deferred.succeed(joined, undefined);
      const result = yield* Fiber.join(fiber).pipe(Effect.timeout("2 seconds"));

      expect(Exit.isFailure(result)).toBe(true);
      expect(calls).toBe(1);
    }),
  ),
);

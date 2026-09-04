import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { RunId, ThreadId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import type { RunEvent } from "@effect-agent/core/RunEvent";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { expect, layer } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("turn-lifetime-thread")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("turn-lifetime-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-lifetime-turn")),
});

class PreparationFailed extends Schema.TaggedError<PreparationFailed>()("PreparationFailed", {}) {}

layer(Layer.mergeAll(identifiers, ThreadHistory.layerTransient))("Turn lifetime", (it) => {
  for (const ending of ["complete", "failure", "defect", "interrupt"] as const) {
    it.effect(`releases completed Turn resources before the next Turn and handles ${ending}`, () =>
      Effect.gen(function* () {
        const thirdTurnEntered = yield* Deferred.make<void>();
        const active = new Set<number>();
        const activeBeforePreparation: Array<number> = [];
        const finalized: Array<number> = [];
        const historyLengths: Array<number> = [];
        const events: Array<RunEvent> = [];
        let modelCalls = 0;
        let modelFinalizers = 0;
        let toolCalls = 0;

        const tools = Toolkit.make(
          Tool.make("next", { parameters: Schema.Struct({}), success: Schema.String }),
        );

        const model = Model.make(
          "scripted",
          "turn-lifetime",
          Layer.effect(
            LanguageModel.LanguageModel,
            Effect.gen(function* () {
              yield* Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  modelFinalizers++;
                }),
              );

              return yield* LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: () => {
                  modelCalls++;
                  expect(modelFinalizers).toBe(0);

                  const parts: ReadonlyArray<Response.StreamPartEncoded> =
                    modelCalls < 3
                      ? [
                          {
                            type: "tool-call",
                            id: `call-${modelCalls}`,
                            name: "next",
                            params: {},
                          },
                          {
                            type: "finish",
                            reason: "tool-calls",
                            usage: { inputTokens: {}, outputTokens: {} },
                          },
                        ]
                      : [
                          { type: "text-start", id: "answer" },
                          { type: "text-delta", id: "answer", delta: '"done"' },
                          { type: "text-end", id: "answer" },
                          {
                            type: "finish",
                            reason: "stop",
                            usage: { inputTokens: {}, outputTokens: {} },
                          },
                        ];

                  return Stream.fromIterable(parts);
                },
              });
            }),
          ),
        );

        const agent = Agent.withModel(
          Agent.make("turn-lifetime", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Use next twice, then answer.",
            toolkit: tools,
            policy: AgentPolicy.make({
              maxTurns: 3,
              maxToolCalls: 2,
              maxDuration: "30 seconds",
              toolConcurrency: 1,
            }),
          }),
          model,
        );

        const run = AgentRuntime.stream(agent, "begin", {
          context: {
            prepare: ({ source, turn }) =>
              Effect.gen(function* () {
                activeBeforePreparation.push(active.size);
                historyLengths.push(source.content.length);
                yield* Effect.acquireRelease(
                  Effect.sync(() => active.add(turn)),
                  () =>
                    Effect.sync(() => {
                      active.delete(turn);
                      finalized.push(turn);
                    }),
                );
                if (turn === 3) {
                  if (ending === "failure") return yield* new PreparationFailed();
                  if (ending === "defect") return yield* Effect.die("preparation defect");
                  if (ending === "interrupt") {
                    yield* Deferred.succeed(thirdTurnEntered, undefined);

                    return yield* Effect.never;
                  }
                }

                return { prompt: source };
              }),
          },
        }).pipe(
          Stream.tap((event) => Effect.sync(() => events.push(event))),
          Stream.runDrain,
          Effect.provide(
            tools.toLayer({
              next: () =>
                Effect.sync(() => {
                  toolCalls++;

                  return "continue";
                }),
            }),
          ),
        );

        const exit = yield* Effect.scoped(
          ending === "interrupt"
            ? Effect.gen(function* () {
                const fiber = yield* Effect.forkChild(run);

                yield* Deferred.await(thirdTurnEntered);
                yield* Fiber.interrupt(fiber);

                return yield* Fiber.await(fiber);
              })
            : Effect.exit(run),
        );

        expect(activeBeforePreparation).toEqual([0, 0, 0]);
        expect(finalized).toEqual([1, 2, 3]);
        expect(active.size).toBe(0);
        expect(modelFinalizers).toBe(1);
        expect(toolCalls).toBe(2);
        expect(historyLengths).toEqual([2, 4, 6]);
        expect(events.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(2);
        if (ending === "complete") {
          expect(Exit.isSuccess(exit)).toBe(true);
          expect(events.at(-1)).toMatchObject({ _tag: "RunCompleted", output: "done", turns: 3 });
        } else {
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isSuccess(exit)) throw new Error("Expected the selected failure");
          if (ending === "failure") {
            expect(Cause.findErrorOption(exit.cause)).toEqual(Option.some(new PreparationFailed()));
            expect(events.at(-1)).toMatchObject({
              _tag: "RunFailed",
              errorTag: "PreparationFailed",
            });
          } else if (ending === "defect") {
            expect(Cause.hasDies(exit.cause)).toBe(true);
          } else {
            expect(Cause.hasInterrupts(exit.cause)).toBe(true);
          }
        }
      }),
    );
  }
});

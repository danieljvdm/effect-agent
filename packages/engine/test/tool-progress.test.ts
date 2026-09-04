import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId, RunId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { RunContextPreparationPassthrough } from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { expect, layer } from "@effect/vitest";
import { Deferred, Effect, Exit, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

const Progress = Tool.make("report_progress", {
  parameters: Schema.Struct({}),
  success: Schema.Any,
});

const Hosted = Tool.providerDefined({
  id: "test.hosted_progress",
  customName: "Hosted",
  providerName: "hosted",
  parameters: Schema.Struct({}),
  success: Schema.Struct({ status: Schema.String }),
})(undefined);

const toolkit = Toolkit.make(Progress, Hosted);
const usage = { inputTokens: {}, outputTokens: {} };

const applicationCall: Response.StreamPartEncoded = {
  type: "tool-call",
  id: "application",
  name: "report_progress",
  params: {},
  providerExecuted: false,
};

const makeAgent = (firstTurn: ReadonlyArray<Response.StreamPartEncoded> = [applicationCall]) =>
  Agent.withModel(
    Agent.make("progress-bounds", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Report progress, then answer.",
      toolkit,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 2,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    }),
    Model.make(
      "scripted",
      "progress-bounds",
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
                        ? [...firstTurn, { type: "finish", reason: "tool-calls", usage }]
                        : [
                            { type: "text-start", id: "answer" },
                            { type: "text-delta", id: "answer", delta: '"done"' },
                            { type: "text-end", id: "answer" },
                            { type: "finish", reason: "stop", usage },
                          ],
                    ),
                  ),
                ),
              ),
          });
        }),
      ),
    ),
  );

const testLayer = Layer.mergeAll(
  Layer.succeed(IdGenerator, {
    nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("progress-thread")),
    nextRunId: Effect.succeed(Schema.decodeSync(RunId)("progress-run")),
    nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("progress-turn")),
  }),
  ThreadHistory.layerTransient,
  RunContextPreparationPassthrough,
);

layer(testLayer)("Tool progress ownership and byte limits", (it) => {
  it.effect("retains owned progress in detached replay after the handler mutates its result", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const retained = { nested: { value: "original" } };

      const detached = yield* AgentRuntime.start(makeAgent(), "go").pipe(
        Effect.provide(
          toolkit.toLayer({
            report_progress: (_, context) =>
              Effect.gen(function* () {
                yield* context.preliminary(retained);
                yield* Deferred.await(release);

                return "terminal result";
              }),
          }),
        ),
      );

      yield* detached.observe.pipe(
        Stream.filter((event) => event._tag === "ToolProgress"),
        Stream.take(1),
        Stream.runDrain,
      );
      retained.nested.value = "changed";
      yield* Deferred.succeed(release, undefined);
      expect((yield* detached.await).output).toBe("done");
      const progress = (yield* detached.events).filter((event) => event._tag === "ToolProgress");

      expect(progress).toHaveLength(1);
      expect(progress[0]?.result).toEqual({ nested: { value: "original" } });
      expect(progress[0]?.result).not.toBe(retained);
      expect(Object.isFrozen(progress[0]?.result)).toBe(true);
    }),
  );

  it.effect.each([2, 3])(
    "admits at most eight cumulative UTF-8 progress bytes for %i values",
    (count) =>
      Effect.gen(function* () {
        const detached = yield* AgentRuntime.start(makeAgent(), "go", {
          bufferLimits: { maxToolProgressBytes: 8 },
        }).pipe(
          Effect.provide(
            toolkit.toLayer({
              report_progress: (_, context) =>
                Effect.gen(function* () {
                  for (let index = 0; index < count; index += 1) yield* context.preliminary("é");

                  return "terminal result exceeds the progress allowance";
                }),
            }),
          ),
        );

        const exit = yield* Effect.exit(detached.await);
        const events = yield* detached.events;
        const progress = events.filter((event) => event._tag === "ToolProgress");

        expect(progress.map((event) => event.result)).toEqual(["é", "é"]);
        expect(Exit.isSuccess(exit)).toBe(count === 2);
        expect(events.at(-1)?._tag).toBe(count === 2 ? "RunCompleted" : "RunFailed");
        if (count === 3) expect(events.at(-1)).toMatchObject({ errorTag: "ModelProtocolError" });
      }),
  );

  it.effect.each(["oversized", "accessor"])(
    "rejects %s progress and finalizes its active handler",
    (kind) =>
      Effect.gen(function* () {
        let finalized = false;

        const payload =
          kind === "oversized"
            ? "x".repeat(64)
            : {
                get value() {
                  return "x";
                },
              };

        const events = yield* AgentRuntime.stream(makeAgent(), "go", {
          bufferLimits: { maxToolProgressBytes: 8 },
        }).pipe(
          Stream.takeUntil((event) => event._tag === "ToolProgress" || event._tag === "RunFailed"),
          Stream.runCollect,
          Effect.provide(
            toolkit.toLayer({
              report_progress: (_, context) =>
                context.preliminary(payload).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Effect.sync(() => {
                      finalized = true;
                    }),
                  ),
                ),
            }),
          ),
        );

        expect(events.filter((event) => event._tag === "ToolProgress")).toHaveLength(0);
        expect(events.at(-1)).toMatchObject({ _tag: "RunFailed", errorTag: "ModelProtocolError" });
        expect(finalized).toBe(true);
      }),
  );

  it.effect(
    "shares the cumulative progress allowance between provider and application results",
    () =>
      Effect.gen(function* () {
        const detached = yield* AgentRuntime.start(
          makeAgent([
            { type: "tool-call", id: "hosted", name: "Hosted", params: {}, providerExecuted: true },
            {
              type: "tool-result",
              id: "hosted",
              name: "Hosted",
              result: { status: "x" },
              isFailure: false,
              providerExecuted: true,
              preliminary: true,
            },
            {
              type: "tool-result",
              id: "hosted",
              name: "Hosted",
              result: { status: "done" },
              isFailure: false,
              providerExecuted: true,
            },
            applicationCall,
          ]),
          "go",
          { bufferLimits: { maxToolProgressBytes: 14 } },
        ).pipe(
          Effect.provide(
            toolkit.toLayer({
              report_progress: (_, context) => context.preliminary("x").pipe(Effect.as("terminal")),
            }),
          ),
        );

        const exit = yield* Effect.exit(detached.await);
        const events = yield* detached.events;

        expect(Exit.isFailure(exit)).toBe(true);
        expect(
          events.filter((event) => event._tag === "ToolProgress").map((event) => event.result),
        ).toEqual([{ status: "x" }]);
        expect(events.at(-1)).toMatchObject({ _tag: "RunFailed", errorTag: "ModelProtocolError" });
      }),
  );
});

import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { IdGenerator } from "effect-agent/id-generator";
import { RunId, ThreadId, TurnId } from "effect-agent/identifiers";
import { RunContextPreparationPassthrough, type RunUsageDelta } from "effect-agent/run-options";
import { ThreadHistory } from "effect-agent/thread-history";
import {
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

let threadSequence = 0;

const tools = Toolkit.make(
  Tool.make("search", {
    parameters: Schema.Struct({ query: Schema.String }),
    success: Schema.String,
  }),
  Tool.make("complete", {
    parameters: Schema.Struct({ answer: Schema.String }),
    success: Schema.String,
  }),
);

const definition = () =>
  Agent.make("completion-correction", {
    input: Schema.String,
    output: Schema.String,
    instructions: "Research before completing.",
    toolkit: tools,
    completion: {
      tool: "complete",
      required: true,
      project: ({ result }) => result,
    },
    completionFromTools: undefined,
    policy: {
      maxTurns: 5,
      maxToolCalls: 10,
      maxDuration: "30 seconds",
      toolConcurrency: 2,
    },
  });

const call = (
  id: string,
  name: string,
  params: Schema.Json,
  providerExecuted = false,
): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted,
});

const finish: Response.StreamPartEncoded = {
  type: "finish",
  reason: "tool-calls",
  usage: { inputTokens: { total: 10 }, outputTokens: { total: 5 } },
};

const mixed = [
  call("premature", "complete", { answer: "unresearched" }),
  call("rejected-search", "search", { query: "Tahoe" }),
  finish,
];

const complete = [call("final", "complete", { answer: "researched" }), finish];

const scriptedModel = (responses: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>) => {
  const prompts: Array<Prompt.Prompt> = [];

  const model = Model.make(
    "scripted",
    "completion-correction",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          const index = prompts.length;

          prompts.push(request.prompt);

          return Stream.fromIterable(responses[Math.min(index, responses.length - 1)] ?? []);
        },
      }),
    ),
  );

  return { model, prompts };
};

const resumeUsage = {
  modelCalls: 1,
  committedTurns: 1,
  toolCalls: 2,
  inputTokens: 10,
  outputTokens: 5,
  lastInputTokens: 10,
  lastOutputTokens: 5,
  costMicrousd: 0,
  programmaticToolCalls: 0,
  consecutiveToolFailures: 0,
  finalizationUsed: false,
};

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => ThreadId.make(`correction-thread-${++threadSequence}`)),
  nextRunId: Effect.succeed(RunId.make("correction-run")),
  nextTurnId: Effect.succeed(TurnId.make("correction-turn")),
});

layer(Layer.mergeAll(identifiers, ThreadHistory.layer, RunContextPreparationPassthrough))(
  "mixed completion correction",
  (it) => {
    {
      it.effect("required completion corrects before any handler starts through the stream", () =>
        Effect.gen(function* () {
          const { model, prompts } = scriptedModel([
            mixed,
            [
              call("research-1", "search", { query: "Tahoe" }),
              call("research-2", "search", { query: "dates" }),
              finish,
            ],
            complete,
          ]);

          const starts: Array<string> = [];
          const usage: Array<RunUsageDelta> = [];
          const agent = Agent.withModel(definition(), model);

          const options = {
            budget: {
              guard: <A, E, R>(effect: Effect.Effect<A, E, R>) => effect,
              consume: (delta: RunUsageDelta) =>
                Effect.sync(() => {
                  usage.push(delta);
                }),
            },
          };

          const handlers = tools.toLayer({
            search: ({ query }) =>
              Effect.sync(() => {
                starts.push(query);

                return `found ${query}`;
              }),
            complete: ({ answer }) =>
              Effect.sync(() => {
                starts.push(answer);

                return answer;
              }),
          });

          const events = yield* AgentRuntime.stream(agent, "travel", options).pipe(
            Stream.runCollect,
            Effect.provide(handlers),
          );

          const failures = events.filter((event) => event._tag === "ToolCallFailed");

          expect(
            failures.map(({ toolCallId, toolName, errorTag, budgetRejected }) => ({
              toolCallId,
              toolName,
              errorTag,
              budgetRejected,
            })),
          ).toEqual([
            {
              toolCallId: "premature",
              toolName: "complete",
              errorTag: "ModelProtocolError",
              budgetRejected: undefined,
            },
            {
              toolCallId: "rejected-search",
              toolName: "search",
              errorTag: "ModelProtocolError",
              budgetRejected: undefined,
            },
          ]);
          expect(
            events
              .filter((event) => event._tag === "ToolCallStarted")
              .map((event) => event.toolCallId),
          ).toEqual(["research-1", "research-2", "final"]);
          expect(events.at(-1)).toMatchObject({
            _tag: "RunCompleted",
            output: "researched",
            turns: 3,
          });

          expect(starts).toEqual(["Tahoe", "dates", "researched"]);
          expect(prompts).toHaveLength(3);

          const results = prompts[1]?.content.flatMap((message) =>
            message.role === "tool" ? message.content : [],
          );

          expect(results).toMatchObject([
            {
              id: "premature",
              name: "complete",
              isFailure: true,
              result: {
                _tag: "ModelProtocolError",
                message: expect.stringContaining("none of its tools ran"),
              },
            },
            {
              id: "rejected-search",
              name: "search",
              isFailure: true,
              result: { _tag: "ModelProtocolError" },
            },
          ]);
          expect(
            usage
              .filter((delta) => delta.modelCalls > 0)
              .map((delta) => [delta.toolCalls, delta.inputTokens, delta.outputTokens]),
          ).toEqual([
            [2, 10, 5],
            [2, 10, 5],
            [1, 10, 5],
          ]);
        }),
      );
    }

    it.effect("refuses a canonically declared mixed resume with a settled call", () =>
      Effect.gen(function* () {
        const { model, prompts } = scriptedModel([complete]);

        const exit = yield* AgentRuntime.run(Agent.withModel(definition(), model), "travel", {
          resumeUsage,
          resume: {
            turn: 1,
            turnId: TurnId.make("resumed"),
            calls: [
              { id: "premature", name: "complete", params: { answer: "unresearched" } },
              { id: "search", name: "search", params: { query: "Tahoe" } },
            ],
            settled: [{ id: "search", result: "already happened", isFailure: false }],
          },
        }).pipe(
          Effect.provide(
            tools.toLayer({
              search: () => Effect.die("must not replay"),
              complete: () => Effect.die("must not execute"),
            }),
          ),
          Effect.exit,
        );

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit))
          expect(Cause.findErrorOption(exit.cause)).toMatchObject({
            _tag: "Some",
            value: { _tag: "ModelProtocolError" },
          });
        expect(prompts).toHaveLength(0);
      }),
    );
  },
);

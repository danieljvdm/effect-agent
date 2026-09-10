import * as Agent from "@effect-agent/core/Agent";
import { type AgentPolicyInput } from "@effect-agent/core/AgentPolicy";
import { RunId, ThreadId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { type RunEvent } from "@effect-agent/core/RunEvent";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import {
  RunContextPreparationPassthrough,
  type RunUsageDelta,
} from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Result,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import {
  AiError,
  LanguageModel,
  Model,
  type Prompt,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

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

const definition = (
  kind: "required" | "optional" | "action" = "required",
  policy: Partial<AgentPolicyInput> = {},
) =>
  Agent.make("completion-correction", {
    input: Schema.String,
    output: Schema.String,
    instructions: "Research before completing.",
    toolkit: tools,
    completion:
      kind === "action"
        ? undefined
        : {
            tool: "complete",
            required: kind === "required",
            project: ({ result }) => result,
          },
    completionFromTools:
      kind === "action"
        ? [
            {
              tool: "complete",
              project: ({ result }) => Option.some(result),
            },
          ]
        : undefined,
    policy: {
      maxTurns: 5,
      maxToolCalls: 10,
      maxDuration: "30 seconds",
      toolConcurrency: 2,
      ...policy,
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

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(ThreadId.make("correction-thread")),
  nextRunId: Effect.succeed(RunId.make("correction-run")),
  nextTurnId: Effect.succeed(TurnId.make("correction-turn")),
});

layer(Layer.mergeAll(identifiers, ThreadHistory.layerTransient, RunContextPreparationPassthrough))(
  "mixed completion correction",
  (it) => {
    for (const kind of ["required", "optional", "action"] as const) {
      it.effect.each(["run", "stream"] as const)(
        `${kind} completion corrects before any handler starts through %s`,
        (mode) =>
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
            const agent = Agent.withModel(definition(kind), model);

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

            if (mode === "run") {
              const result = yield* AgentRuntime.run(agent, "travel", options).pipe(
                Effect.provide(handlers),
              );

              expect(result.output).toBe("researched");
            } else {
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
            }

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

    it.effect.each([
      {
        name: "repeated failures",
        policy: {},
        requests: 2,
        tag: "AgentPolicyError",
        limit: "repeated-failures",
      },
      {
        name: "strict turns",
        policy: { maxTurns: 1, onExhaustion: "fail" },
        requests: 1,
        tag: "AgentPolicyError",
        limit: "turns",
      },
      {
        name: "finalization",
        policy: { maxTurns: 1, repeatedFailureLimit: 0 },
        requests: 2,
        tag: "ModelProtocolError",
      },
      {
        name: "strict calls",
        policy: { maxToolCalls: 1, onExhaustion: "fail" },
        requests: 1,
        tag: "AgentPolicyError",
        limit: "tool-calls",
      },
      {
        name: "exhausted calls",
        policy: { maxToolCalls: 1 },
        requests: 2,
        tag: "ModelProtocolError",
      },
      {
        name: "strict tokens",
        policy: { tokenBudget: 1_000, completionReserveTokens: 0, onExhaustion: "fail" },
        requests: 1,
        tag: "AgentPolicyError",
        limit: "tokens",
      },
      {
        name: "exhausted tokens",
        policy: { tokenBudget: 1_000, completionReserveTokens: 0 },
        requests: 2,
        tag: "ModelProtocolError",
      },
      {
        name: "cost",
        policy: { costBudgetMicrousd: 0 },
        requests: 1,
        tag: "AgentPolicyError",
        limit: "cost",
      },
    ] satisfies ReadonlyArray<{
      name: string;
      policy: Partial<AgentPolicyInput>;
      requests: number;
      tag: string;
      limit?: string;
    }>)("respects $name without executing or falsely completing", (scenario) =>
      Effect.gen(function* () {
        const response = scenario.name.includes("tokens")
          ? [
              ...mixed.slice(0, -1),
              {
                type: "finish" as const,
                reason: "tool-calls" as const,
                usage: { inputTokens: { total: 900 }, outputTokens: { total: 200 } },
              },
            ]
          : mixed;

        const { model, prompts } = scriptedModel([response]);
        const events: Array<RunEvent> = [];

        const exit = yield* AgentRuntime.stream(
          Agent.withModel(definition("required", scenario.policy), model),
          "travel",
          scenario.name === "cost" ? { estimateCostMicrousd: () => Effect.succeed(1) } : {},
        ).pipe(
          Stream.tap((event) =>
            Effect.sync(() => {
              events.push(event);
            }),
          ),
          Stream.runDrain,
          Effect.provide(
            tools.toLayer({
              search: () => Effect.die("must not execute"),
              complete: () => Effect.die("must not execute"),
            }),
          ),
          Effect.exit,
        );

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit))
          expect(Cause.findErrorOption(exit.cause)).toMatchObject({
            _tag: "Some",
            value: {
              _tag: scenario.tag,
              ...("limit" in scenario ? { limit: scenario.limit } : {}),
            },
          });
        expect(prompts).toHaveLength(scenario.requests);
        expect(
          events.some((event) => event._tag === "RunCompleted" || event._tag === "ToolCallStarted"),
        ).toBe(false);
      }),
    );

    it.effect.each([false, true])(
      "refuses mixed provider batches with terminal result=%s",
      (settled) =>
        Effect.gen(function* () {
          const { model, prompts } = scriptedModel([
            [
              call("provider-search", "search", { query: "Tahoe" }, true),
              ...(settled
                ? [
                    {
                      type: "tool-result" as const,
                      id: "provider-search",
                      name: "search",
                      result: "found",
                      isFailure: false,
                      providerExecuted: true,
                    },
                  ]
                : []),
              call("premature", "complete", { answer: "unresearched" }),
              finish,
            ],
            complete,
          ]);

          const exit = yield* AgentRuntime.run(Agent.withModel(definition(), model), "travel").pipe(
            Effect.provide(
              tools.toLayer({
                search: () => Effect.die("provider calls never invoke handlers"),
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
          expect(prompts).toHaveLength(1);
        }),
    );

    it.effect.each([false, true])(
      "refuses a canonically declared mixed resume with settled calls=%s",
      (settled) =>
        Effect.gen(function* () {
          const { model, prompts } = scriptedModel([complete]);

          const exit = yield* AgentRuntime.run(Agent.withModel(definition(), model), "travel", {
            resume: {
              turn: 1,
              turnId: TurnId.make("resumed"),
              calls: [
                { id: "premature", name: "complete", params: { answer: "unresearched" } },
                { id: "search", name: "search", params: { query: "Tahoe" } },
              ],
              settled: settled
                ? [{ id: "search", result: "already happened", isFailure: false }]
                : [],
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

    it.effect.each(["failure", "defect", "timeout", "interruption"] as const)(
      "retains %s semantics and releases the corrective model request",
      (outcome) =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const closed = yield* Deferred.make<void>();
          let requests = 0;

          const nativeError = AiError.make({
            module: "test",
            method: "streamText",
            reason: new AiError.InvalidRequestError({ description: "corrective request failed" }),
          });

          const defect = new Error("corrective request defect");

          const model = Model.make(
            "scripted",
            "correction-lifecycle",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: () =>
                  requests++ === 0
                    ? Stream.fromIterable(mixed)
                    : Stream.unwrap(
                        Effect.gen(function* () {
                          yield* Effect.addFinalizer(() => Deferred.succeed(closed, undefined));
                          yield* Deferred.succeed(entered, undefined);

                          return outcome === "failure"
                            ? Stream.fail(nativeError)
                            : outcome === "defect"
                              ? Stream.die(defect)
                              : Stream.never;
                        }),
                      ),
              }),
            ),
          );

          const events: Array<RunEvent> = [];

          const fiber = yield* AgentRuntime.stream(
            Agent.withModel(definition(), model),
            "travel",
          ).pipe(
            Stream.tap((event) =>
              Effect.sync(() => {
                events.push(event);
              }),
            ),
            Stream.runDrain,
            Effect.provide(
              tools.toLayer({
                search: () => Effect.die("must not execute"),
                complete: () => Effect.die("must not execute"),
              }),
            ),
            Effect.forkChild,
          );

          yield* Deferred.await(entered);
          if (outcome === "timeout") yield* TestClock.adjust("31 seconds");
          if (outcome === "interruption") yield* Fiber.interrupt(fiber);
          const exit = yield* Fiber.await(fiber);

          yield* Deferred.await(closed);
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            if (outcome === "defect")
              expect(Cause.findDefect(exit.cause)).toEqual(Result.succeed(defect));
            else if (outcome === "interruption") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
            else
              expect(Cause.findErrorOption(exit.cause)).toMatchObject({
                _tag: "Some",
                value:
                  outcome === "failure"
                    ? nativeError
                    : { _tag: "AgentPolicyError", limit: "duration" },
              });
          }
          expect(requests).toBe(2);
          expect(
            events.some(
              (event) => event._tag === "RunCompleted" || event._tag === "ToolCallStarted",
            ),
          ).toBe(false);
        }),
    );
  },
);

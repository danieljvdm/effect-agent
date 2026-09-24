import { expect, layer } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Ref, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import {
  RunToolAuthorization,
  type RunOptions,
  type RunToolAuthorizationRequest,
} from "effect-agent/run-options";
import {
  ToolBroker,
  type ToolBrokerPass,
  type ToolBrokerPassOptions,
} from "effect-agent/tool-broker";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

import { RunContextPreparationPassthrough } from "../../src/engine/RunOptions.ts";
import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

class QueryFailure extends Schema.TaggedError<QueryFailure>()("QueryFailure", {
  message: Schema.String,
}) {}

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => Schema.decodeSync(ThreadId)(`thread-broker-${++threadSequence}`)),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-broker")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-broker")),
});

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

/** Scripted model: one scripted first turn, then a fixed final answer. */
const scriptedModel = (firstTurn: ReadonlyArray<Response.StreamPartEncoded>, finalText: string) =>
  Model.make(
    "scripted",
    "tool-broker",
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

const policy = (overrides?: Partial<Parameters<typeof AgentPolicy.make>[0]>) =>
  AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 8,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
    ...overrides,
  });

const Query = Tool.make("query", {
  parameters: Schema.Struct({ sql: Schema.String }),
  success: Schema.Struct({ rows: Schema.Array(Schema.Int) }),
  failure: QueryFailure,
});

const LooseTool = Tool.make("loose", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.Any,
});

const orchestrateCall = (id: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id,
    name: "orchestrate",
    params: { plan: "run" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/**
 * Build a Run whose single `orchestrate` Tool Call opens one broker pass over
 * the inner toolkit and executes `program` against it, returning the encoded
 * outcomes as its Tool success so the test observes exactly what generated
 * code would.
 */
const runOrchestrated = <
  InnerTools extends Record<string, Tool.Any>,
  HookError = never,
  HookRequirements = never,
>(options: {
  readonly innerToolkit: Toolkit.Toolkit<InnerTools>;
  readonly innerHandlers: Layer.Layer<Tool.HandlersFor<InnerTools>, never, never>;
  readonly program: (pass: ToolBrokerPass) => Effect.Effect<unknown>;
  readonly passOptions?: ToolBrokerPassOptions;
  readonly agentPolicy?: AgentPolicy;
  readonly runOptions?: RunOptions<HookError, HookRequirements>;
}) =>
  Effect.gen(function* () {
    const Orchestrate = Tool.make("orchestrate", {
      parameters: Schema.Struct({ plan: Schema.String }),
      success: Schema.Any,
    }).addDependency(ToolBroker);

    const outerToolkit = Toolkit.make(Orchestrate);

    const definition = Agent.make("broker-host", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Orchestrate.",
      toolkit: outerToolkit,
      policy: options.agentPolicy ?? policy(),
    });

    const model = scriptedModel(orchestrateCall("orchestrate-1"), '{"answer":"done"}');

    const toolLayer = outerToolkit
      .toLayer(
        Effect.gen(function* () {
          const inner = yield* options.innerToolkit;

          return {
            orchestrate: () =>
              Effect.gen(function* () {
                const broker = yield* ToolBroker;

                // Inside a live Tool batch the broker is always bound; an
                // unavailable broker here is a harness defect, not a test case.
                const pass = yield* broker
                  .openPass(inner, options.passOptions ?? { maxResultBytes: 1024 * 1024 })
                  .pipe(Effect.orDie);

                return yield* options.program(pass);
              }),
          };
        }),
      )
      .pipe(Layer.provide(options.innerHandlers));

    const result = yield* AgentRuntime.run(
      Agent.withModel(definition, model),
      { question: "go" },
      options.runOptions ?? {},
    ).pipe(Effect.provide(toolLayer), Effect.scoped);

    return result;
  });

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("RUN-016 programmatic Tool broker", (it) => {
  it.effect("serializes cumulative reservations across concurrent outer handlers", () =>
    Effect.gen(function* () {
      const Orchestrate = Tool.make("orchestrate", {
        parameters: Schema.Struct({ plan: Schema.String }),
        success: Schema.Unknown,
      }).addDependency(ToolBroker);

      const outer = Toolkit.make(Orchestrate);
      const inner = Toolkit.make(Query);
      const bothEntered = yield* Deferred.make<void>();
      let entered = 0;
      let activeReservations = 0;
      let maxActiveReservations = 0;
      const reservations: Array<number> = [];
      let handlerStarts = 0;

      const definition = Agent.make("parallel-reservations", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Answer.",
        toolkit: outer,
        policy: policy({ maxToolCalls: 4 }),
      });

      const model = scriptedModel(
        [...orchestrateCall("outer-1").slice(0, -1), ...orchestrateCall("outer-2")],
        '"done"',
      );

      const handlers = outer
        .toLayer(
          Effect.gen(function* () {
            const toolkit = yield* inner;

            return {
              orchestrate: () =>
                Effect.gen(function* () {
                  entered += 1;
                  if (entered === 2) yield* Deferred.succeed(bothEntered, undefined);
                  const broker = yield* ToolBroker;

                  const pass = yield* broker
                    .openPass(toolkit, { maxResultBytes: 1024 })
                    .pipe(Effect.orDie);

                  return yield* pass.invoke({
                    toolName: "query",
                    encodedArguments: { sql: "select" },
                  });
                }),
            };
          }),
        )
        .pipe(
          Layer.provide(
            inner.toLayer({
              query: () =>
                Effect.sync(() => {
                  handlerStarts += 1;

                  return { rows: [1] };
                }),
            }),
          ),
        );

      yield* AgentRuntime.run(Agent.withModel(definition, model), "q", {
        durability: {
          commitResponse: () => Effect.void,
          prepareToolCalls: () => Effect.void,
          commitCompaction: () => Effect.void,
          noteTurnUsage: () => Effect.void,
          step: { lookup: () => Effect.succeed(Option.none()), commit: () => Effect.void },
          reservePolicyUsage: (usage) =>
            Effect.gen(function* () {
              activeReservations += 1;
              maxActiveReservations = Math.max(maxActiveReservations, activeReservations);
              yield* Deferred.await(bothEntered);
              yield* Effect.yieldNow;
              reservations.push(usage.programmaticToolCalls);
              activeReservations -= 1;
            }),
        },
      }).pipe(Effect.provide(handlers));
      expect(reservations).toEqual([1, 2]);
      expect(maxActiveReservations).toBe(1);
      expect(handlerStarts).toBe(2);
    }),
  );

  it.effect("a reservation interrupted after commit starts no programmatic handler", () =>
    Effect.gen(function* () {
      const innerToolkit = Toolkit.make(Query);
      let starts = 0;
      let reserved = 0;

      const exit = yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          query: () =>
            Effect.sync(() => {
              starts += 1;

              return { rows: [1] };
            }),
        }),
        runOptions: {
          durability: {
            commitResponse: () => Effect.void,
            prepareToolCalls: () => Effect.void,
            commitCompaction: () => Effect.void,
            noteTurnUsage: () => Effect.void,
            step: { lookup: () => Effect.succeed(Option.none()), commit: () => Effect.void },
            reservePolicyUsage: (usage) =>
              Effect.sync(() => {
                reserved = usage.programmaticToolCalls;
              }).pipe(Effect.andThen(Effect.interrupt)),
          },
        },
        program: (pass) => pass.invoke({ toolName: "query", encodedArguments: { sql: "never" } }),
      }).pipe(Effect.exit);

      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
      expect(reserved).toBe(1);
      expect(starts).toBe(0);
    }),
  );

  it.effect("uses the Run's ambient authorization despite an inner policy substitution", () =>
    Effect.gen(function* () {
      const innerToolkit = Toolkit.make(Query);
      const requests: Array<RunToolAuthorizationRequest> = [];

      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({ query: () => Effect.die("Denied handler started") }),
        program: (pass) =>
          pass.invoke({ toolName: "query", encodedArguments: { sql: "denied" } }).pipe(
            Effect.tap((outcome) =>
              Effect.sync(() => {
                expect(outcome).toMatchObject({
                  _tag: "ProgrammaticCallError",
                  errorTag: "ProgrammaticToolAuthorizationDenied",
                });
              }),
            ),
            Effect.provideService(RunToolAuthorization, {
              authorize: () => Effect.succeed({ _tag: "allowed" }),
            }),
          ),
      }).pipe(
        Effect.provideService(RunToolAuthorization, {
          authorize: (request) =>
            Effect.sync(() => {
              requests.push(request);

              return request.programmatic === undefined
                ? { _tag: "allowed" as const }
                : { _tag: "denied" as const, reason: "Inner call denied" };
            }),
        }),
      );
      expect(requests.map((request) => request.call.toolName)).toEqual(["orchestrate", "query"]);
    }),
  );

  it.effect("interrupts active and queued calls without starting queued handlers", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const innerToolkit = Toolkit.make(Query);
      let starts = 0;
      let finalized = 0;

      yield* runOrchestrated({
        innerToolkit,
        passOptions: { maxResultBytes: 1024, concurrency: 1 },
        innerHandlers: innerToolkit.toLayer({
          query: () =>
            Effect.gen(function* () {
              starts++;
              yield* Deferred.succeed(started, undefined);

              return yield* Effect.never;
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  finalized++;
                }),
              ),
            ),
        }),
        program: (pass) =>
          Effect.gen(function* () {
            const first = yield* pass
              .invoke({ toolName: "query", encodedArguments: { sql: "a" } })
              .pipe(Effect.forkChild);

            yield* Deferred.await(started);

            const queued = yield* pass
              .invoke({ toolName: "query", encodedArguments: { sql: "b" } })
              .pipe(Effect.forkChild);

            yield* Effect.yieldNow;
            yield* Fiber.interrupt(queued);
            yield* Fiber.interrupt(first);
            expect(yield* pass.snapshot).toEqual([
              { sequenceIndex: 0, toolName: "query", status: "uncertain" },
              { sequenceIndex: 1, toolName: "query", status: "not-started" },
            ]);

            return null;
          }),
      });
      expect(starts).toBe(1);
      expect(finalized).toBe(1);
    }),
  );

  it.effect("owns the redacted JSON result across later mutation", () =>
    Effect.gen(function* () {
      const innerToolkit = Toolkit.make(LooseTool);
      const maxResultBytes = 256;
      let expanded = false;
      const retained = { nested: { value: "small" } };

      const dynamic = {
        get value() {
          return expanded ? "x".repeat(1_048_576) : "small";
        },
      };

      const values = [dynamic, retained];
      let nextResult = 0;

      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          loose: () => Effect.sync(() => null),
        }),
        passOptions: {
          maxResultBytes,
          redactResult: () => Effect.sync(() => values[nextResult++]),
        },
        program: (pass) =>
          Effect.gen(function* () {
            const dynamicOutcome = yield* pass.invoke({
              toolName: "loose",
              encodedArguments: { key: "dynamic" },
            });

            const retainedOutcome = yield* pass.invoke({
              toolName: "loose",
              encodedArguments: { key: "retained" },
            });

            expanded = true;
            retained.nested.value = "x".repeat(1_048_576);
            for (const outcome of [dynamicOutcome, retainedOutcome]) {
              expect(outcome._tag).toBe("ProgrammaticCallSuccess");
              if (outcome._tag !== "ProgrammaticCallSuccess") {
                throw new Error("Expected a JSON result within the broker limit");
              }
              // Every character in these fixtures is ASCII, so string length is UTF-8 bytes.
              expect(JSON.stringify(outcome.encodedResult).length).toBeLessThanOrEqual(
                maxResultBytes,
              );
            }
            expect(dynamicOutcome).toMatchObject({ encodedResult: { value: "small" } });
            expect(retainedOutcome).toMatchObject({
              encodedResult: { nested: { value: "small" } },
            });

            return null;
          }),
      });
    }),
  );
});

import { Agent, AgentPolicy, ConversationId, IdGenerator, RunId, TurnId } from "@effect-agent/core";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Inspectable,
  Layer,
  Logger,
  Option,
  Ref,
  References,
  Schema,
  Stream,
  Tracer,
} from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

import { ConversationHistory } from "../src/conversation-history.ts";
import {
  AgentRuntime,
  ToolBroker,
  ToolBrokerConfigurationError,
  ToolExecutionClass,
  type ProgrammaticCallOutcome,
  type ToolBrokerPass,
  type ToolBrokerPassOptions,
  type RunOptions,
} from "../src/index.ts";

class QueryFailure extends Schema.TaggedError<QueryFailure>()("QueryFailure", {
  message: Schema.String,
}) {}

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const identifiers = Layer.succeed(IdGenerator, {
  nextConversationId: Effect.succeed(Schema.decodeSync(ConversationId)("conversation-broker")),
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

const ApprovalQuery = Tool.make("approval_query", {
  parameters: Schema.Struct({ sql: Schema.String }),
  success: Schema.Struct({ rows: Schema.Array(Schema.Int) }),
  needsApproval: true,
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
const runOrchestrated = <InnerTools extends Record<string, Tool.Any>>(options: {
  readonly innerToolkit: Toolkit.Toolkit<InnerTools>;
  readonly innerHandlers: Layer.Layer<Tool.HandlersFor<InnerTools>, never, never>;
  readonly program: (pass: ToolBrokerPass) => Effect.Effect<unknown>;
  readonly passOptions?: ToolBrokerPassOptions;
  readonly agentPolicy?: AgentPolicy;
  readonly runOptions?: RunOptions;
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

const testLayer = Layer.merge(identifiers, ConversationHistory.layerTransient);

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

  it.effect(
    "restores programmatic allowance and reserves the remaining slot before its handler",
    () =>
      Effect.gen(function* () {
        const innerToolkit = Toolkit.make(Query);
        const marks: Array<string> = [];
        const outcomes: Array<ProgrammaticCallOutcome> = [];
        yield* runOrchestrated({
          innerToolkit,
          innerHandlers: innerToolkit.toLayer({
            query: () =>
              Effect.sync(() => {
                marks.push("handler");
                return { rows: [1] };
              }),
          }),
          agentPolicy: policy({ maxToolCalls: 5, onExhaustion: "fail" }),
          runOptions: {
            resumeUsage: {
              committedTurns: 1,
              toolCalls: 1,
              programmaticToolCalls: 2,
              consecutiveToolFailures: 0,
              finalizationUsed: false,
              modelCalls: 1,
              inputTokens: 0,
              outputTokens: 0,
              lastInputTokens: 0,
              lastOutputTokens: 0,
              costMicrousd: 0,
            },
            durability: {
              commitResponse: () => Effect.void,
              prepareToolCalls: () => Effect.void,
              commitCompaction: () => Effect.void,
              noteTurnUsage: () => Effect.void,
              step: { lookup: () => Effect.succeed(Option.none()), commit: () => Effect.void },
              reservePolicyUsage: (usage) =>
                Effect.sync(() => {
                  marks.push(`reserve:${usage.programmaticToolCalls}:${usage.finalizationUsed}`);
                }),
            },
          },
          program: (pass) =>
            Effect.gen(function* () {
              outcomes.push(
                yield* pass.invoke({ toolName: "query", encodedArguments: { sql: "first" } }),
              );
              outcomes.push(
                yield* pass.invoke({ toolName: "query", encodedArguments: { sql: "second" } }),
              );
              return null;
            }),
        });
        expect(marks).toEqual(["reserve:3:false", "handler"]);
        expect(outcomes[0]).toMatchObject({ _tag: "ProgrammaticCallSuccess" });
        expect(outcomes[1]).toMatchObject({
          _tag: "ProgrammaticCallError",
          errorTag: "AgentPolicyError",
        });
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

  it.effect("rejects omitted pass options through the typed configuration channel", () =>
    Effect.gen(function* () {
      const captured = yield* Ref.make<unknown>(undefined);
      const innerToolkit = Toolkit.make(Query);
      const Orchestrate = Tool.make("orchestrate", {
        parameters: Schema.Struct({ plan: Schema.String }),
        success: Schema.Any,
      }).addDependency(ToolBroker);
      const outerToolkit = Toolkit.make(Orchestrate);
      const definition = Agent.make("broker-missing-options", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Orchestrate.",
        toolkit: outerToolkit,
        policy: policy(),
      });
      const handlers = outerToolkit
        .toLayer(
          Effect.gen(function* () {
            const inner = yield* innerToolkit;
            return {
              orchestrate: () =>
                Effect.gen(function* () {
                  const broker = yield* ToolBroker;
                  // @ts-expect-error Exercise the JavaScript boundary where the required argument
                  // can be omitted despite the TypeScript declaration.
                  const exit = yield* broker.openPass(inner).pipe(Effect.exit);
                  if (Exit.isSuccess(exit)) {
                    throw new Error("Expected missing pass options to fail");
                  }
                  yield* Ref.set(
                    captured,
                    Option.getOrUndefined(Cause.findErrorOption(exit.cause)),
                  );
                  return null;
                }),
            };
          }),
        )
        .pipe(Layer.provide(innerToolkit.toLayer({ query: () => Effect.succeed({ rows: [] }) })));

      yield* AgentRuntime.run(
        Agent.withModel(
          definition,
          scriptedModel(orchestrateCall("missing-options-1"), '{"answer":"done"}'),
        ),
        { question: "go" },
      ).pipe(Effect.provide(handlers), Effect.scoped);

      expect(yield* Ref.get(captured)).toBeInstanceOf(ToolBrokerConfigurationError);
    }),
  );

  it.effect(
    "RUN-016 direct and programmatic invocation of the same Tool are observably equivalent",
    () =>
      Effect.gen(function* () {
        const seen = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const handler = {
          query: ({ sql }: { readonly sql: string }) =>
            Ref.update(seen, (all) => [...all, sql]).pipe(Effect.as({ rows: [1, 2, 3] })),
        };
        const innerToolkit = Toolkit.make(Query);

        // Direct: the model declares the call itself, and the second model
        // request captures the recorded tool-result exactly as the model
        // sees it.
        const directDefinition = Agent.make("direct-host", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Query.",
          toolkit: innerToolkit,
          policy: policy(),
        });
        const directResults = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const directModel = Model.make(
          "scripted",
          "direct-capture",
          Layer.effect(
            LanguageModel.LanguageModel,
            Effect.gen(function* () {
              const turn = yield* Ref.make(0);
              return yield* LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: ({ prompt }) =>
                  Stream.unwrap(
                    Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                      Effect.tap(() =>
                        Ref.update(directResults, (all) => [
                          ...all,
                          ...prompt.content
                            .filter((message) => message.role === "tool")
                            .flatMap((message) => message.content)
                            .filter((part) => part.type === "tool-result")
                            .map((part) => part.result),
                        ]),
                      ),
                      Effect.map((value) =>
                        Stream.fromIterable<Response.StreamPartEncoded>(
                          value === 0
                            ? [
                                {
                                  type: "tool-call",
                                  id: "query-1",
                                  name: "query",
                                  params: { sql: "select 1" },
                                  providerExecuted: false,
                                },
                                { type: "finish", reason: "tool-calls", usage },
                              ]
                            : finalParts('{"answer":"direct"}'),
                        ),
                      ),
                    ),
                  ),
              });
            }),
          ),
        );
        const direct = yield* AgentRuntime.run(
          Agent.withModel(directDefinition, directModel),
          { question: "go" },
          {},
        ).pipe(Effect.provide(innerToolkit.toLayer(handler)), Effect.scoped);
        expect(direct.output).toEqual({ answer: "direct" });
        const [directRecorded] = yield* Ref.get(directResults);
        expect(directRecorded).toEqual({ rows: [1, 2, 3] });

        // Programmatic: the same Tool, same encoded arguments, through the broker.
        const programmatic = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
        const result = yield* runOrchestrated({
          innerToolkit,
          innerHandlers: innerToolkit.toLayer(handler),
          program: (pass) =>
            pass
              .invoke({ toolName: "query", encodedArguments: { sql: "select 1" } })
              .pipe(Effect.tap((outcome) => Ref.set(programmatic, [outcome]))),
        });
        expect(result.output).toEqual({ answer: "done" });
        expect(yield* Ref.get(seen)).toEqual(["select 1", "select 1"]);
        // The broker's encoded success is the same value the direct path
        // records for the model — equivalence of the observable result, not
        // only of the handler inputs.
        const [outcome] = yield* Ref.get(programmatic);
        expect(outcome).toMatchObject({
          _tag: "ProgrammaticCallSuccess",
          index: 0,
        });
        if (outcome._tag !== "ProgrammaticCallSuccess") {
          throw new Error("Expected the programmatic call to succeed");
        }
        expect(outcome.encodedResult).toEqual(directRecorded);
      }),
  );

  it.effect(
    "exports privacy-safe canonical telemetry for programmatic success and returned failure",
    () => {
      const spans: Array<Tracer.NativeSpan> = [];
      const logs: Array<{
        readonly message: unknown;
        readonly annotations: Readonly<Record<string, unknown>>;
        readonly cause: string;
      }> = [];
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      const logger = Logger.make<unknown, void>(({ cause, fiber, message }) => {
        logs.push({
          message,
          annotations: { ...fiber.getRef(References.CurrentLogAnnotations) },
          cause: Cause.pretty(cause),
        });
      });
      const argumentSecret = "select private_token from tenant_secrets";
      const failureSecret = "database failure contains a private tenant value";
      const ObservedQuery = Tool.make("observed_query", {
        parameters: Schema.Struct({ sql: Schema.String }),
        success: Schema.Struct({ rows: Schema.Array(Schema.Int) }),
      }).annotate(ToolExecutionClass, "readonly");
      const ObservedFailure = Tool.make("observed_failure", {
        parameters: Schema.Struct({ sql: Schema.String }),
        success: Schema.Struct({ rows: Schema.Array(Schema.Int) }),
        failure: QueryFailure,
        failureMode: "return",
      }).annotate(ToolExecutionClass, "readonly");
      const innerToolkit = Toolkit.make(ObservedQuery, ObservedFailure);

      return Effect.gen(function* () {
        const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
        yield* runOrchestrated({
          innerToolkit,
          innerHandlers: innerToolkit.toLayer({
            observed_query: () =>
              Effect.succeed({ rows: [1] }).pipe(Effect.withSpan("host.programmatic.query")),
            observed_failure: () => Effect.fail(QueryFailure.make({ message: failureSecret })),
          }),
          program: (pass) =>
            Effect.gen(function* () {
              const success = yield* pass.invoke({
                toolName: "observed_query",
                encodedArguments: { sql: argumentSecret },
              });
              const failure = yield* pass.invoke({
                toolName: "observed_failure",
                encodedArguments: { sql: argumentSecret },
              });
              yield* Ref.set(outcomes, [success, failure]);
              return null;
            }),
        }).pipe(
          Effect.provideService(Tracer.Tracer, tracer),
          Effect.provide(Logger.layer([logger])),
        );

        expect(yield* Ref.get(outcomes)).toMatchObject([
          { _tag: "ProgrammaticCallSuccess", index: 0 },
          { _tag: "ProgrammaticCallFailure", index: 1 },
        ]);

        const programmaticSpans = spans.filter(
          (span) => span.attributes.get("effect_agent.tool.invocation_kind") === "programmatic",
        );
        expect(programmaticSpans.map((span) => span.name)).toEqual([
          "execute_tool observed_query",
          "execute_tool observed_failure",
        ]);
        expect(spans.some((span) => span.name === "AgentRuntime.programmaticTool")).toBe(false);
        expect(spans.some((span) => span.name === "AgentRuntime.toolkit.handle")).toBe(false);

        const [successSpan, failureSpan] = programmaticSpans;
        expect(Object.fromEntries(successSpan?.attributes ?? [])).toMatchObject({
          "gen_ai.operation.name": "execute_tool",
          "gen_ai.tool.name": "observed_query",
          "gen_ai.tool.call.id": "orchestrate-1#0",
          "gen_ai.agent.name": "broker-host",
          "gen_ai.conversation.id": "conversation-broker",
          "effect_agent.tool.execution_class": "readonly",
          "effect_agent.tool.invocation_kind": "programmatic",
          "effect_agent.tool.outcome": "success",
          "effect_agent.tool.parent_call.id": "orchestrate-1",
          "effect_agent.tool.sequence_index": 0,
          toolCallId: "orchestrate-1#0",
          parentToolCallId: "orchestrate-1",
          sequenceIndex: 0,
          toolName: "observed_query",
        });
        expect(Object.fromEntries(failureSpan?.attributes ?? [])).toMatchObject({
          "gen_ai.tool.name": "observed_failure",
          "gen_ai.tool.call.id": "orchestrate-1#1",
          "effect_agent.tool.outcome": "failure",
          "effect_agent.tool.sequence_index": 1,
          toolCallId: "orchestrate-1#1",
        });
        if (successSpan?.status._tag !== "Ended" || failureSpan?.status._tag !== "Ended") {
          throw new Error("Expected both programmatic Tool spans to end");
        }
        expect(Exit.isSuccess(successSpan.status.exit)).toBe(true);
        expect(Exit.isFailure(failureSpan.status.exit)).toBe(true);
        if (Exit.isFailure(failureSpan.status.exit)) {
          expect(Cause.pretty(failureSpan.status.exit.cause)).not.toContain(failureSecret);
        }

        const hostSpan = spans.find((span) => span.name === "host.programmatic.query");
        const hostParent = Option.getOrUndefined(hostSpan?.parent ?? Option.none());
        expect(hostParent?._tag).toBe("Span");
        if (hostParent?._tag !== "Span") {
          throw new Error("Expected the host span to have a canonical programmatic Tool parent");
        }
        expect(hostParent.name).toBe("execute_tool observed_query");
        expect(hostParent.attributes.get("effect_agent.tool.invocation_kind")).toBe("programmatic");

        const programmaticLogs = logs.filter(
          ({ annotations }) => annotations["effect_agent.tool.invocation_kind"] === "programmatic",
        );
        expect(
          programmaticLogs.map(({ annotations, message }) => ({
            message: Array.isArray(message)
              ? message.map((value) => Inspectable.toStringUnknown(value)).join(" ")
              : Inspectable.toStringUnknown(message),
            outcome: annotations.toolOutcome,
            toolCallId: annotations.toolCallId,
            toolName: annotations.toolName,
          })),
        ).toEqual([
          {
            message: "agent tool execution completed",
            outcome: "success",
            toolCallId: "orchestrate-1#0",
            toolName: "observed_query",
          },
          {
            message: "agent tool execution failed",
            outcome: "failure",
            toolCallId: "orchestrate-1#1",
            toolName: "observed_failure",
          },
        ]);

        const exportedText = [
          ...spans.map((span) =>
            JSON.stringify({
              name: span.name,
              attributes: Object.fromEntries(span.attributes),
              events: span.events,
              status:
                span.status._tag === "Ended" && Exit.isFailure(span.status.exit)
                  ? Cause.pretty(span.status.exit.cause)
                  : span.status._tag,
            }),
          ),
          ...logs.map((entry) => Inspectable.toStringUnknown(entry)),
        ].join("\n");
        expect(exportedText).not.toContain(argumentSecret);
        expect(exportedText).not.toContain(failureSecret);
      });
    },
  );

  it.effect("RUN-016 allocates deterministic sequential indices from broker-owned state", () =>
    Effect.gen(function* () {
      const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      const innerToolkit = Toolkit.make(Query);
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          query: () => Effect.succeed({ rows: [1] }),
        }),
        program: (pass) =>
          Effect.gen(function* () {
            const first = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "a" },
            });
            const second = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "b" },
            });
            yield* Ref.set(outcomes, [first, second]);
            return null;
          }),
      });
      const [first, second] = yield* Ref.get(outcomes);
      expect(first).toMatchObject({ _tag: "ProgrammaticCallSuccess", index: 0 });
      expect(second).toMatchObject({ _tag: "ProgrammaticCallSuccess", index: 1 });
    }),
  );

  it.effect("RUN-016 a concurrent host call fails typed without consuming an identity", () =>
    Effect.gen(function* () {
      const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      const started = yield* Deferred.make<void>();
      const gate = yield* Deferred.make<void>();
      const innerToolkit = Toolkit.make(Query);
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          query: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(gate)),
              Effect.as({ rows: [1] }),
            ),
        }),
        program: (pass) =>
          Effect.gen(function* () {
            const firstFiber = yield* pass
              .invoke({ toolName: "query", encodedArguments: { sql: "a" } })
              .pipe(Effect.forkChild);
            yield* Deferred.await(started);
            const second = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "b" },
            });
            yield* Deferred.succeed(gate, undefined);
            const first = yield* Fiber.join(firstFiber);
            const third = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "c" },
            });
            yield* Ref.set(outcomes, [first, second, third]);
            return null;
          }),
      });
      const [first, second, third] = yield* Ref.get(outcomes);
      expect(first).toMatchObject({ _tag: "ProgrammaticCallSuccess", index: 0 });
      expect(second).toMatchObject({
        _tag: "ProgrammaticCallError",
        index: undefined,
        errorTag: "ProgrammaticCallConcurrencyError",
      });
      // The rejected call consumed no identity: the next sequential call gets index 1.
      expect(third).toMatchObject({ _tag: "ProgrammaticCallSuccess", index: 1 });
    }),
  );

  it.effect("RUN-016 a non-allowlisted Tool and invalid parameters fail closed pre-start", () =>
    Effect.gen(function* () {
      const invocations = yield* Ref.make(0);
      const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      const innerToolkit = Toolkit.make(Query);
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          query: () => Ref.update(invocations, (n) => n + 1).pipe(Effect.as({ rows: [] })),
        }),
        program: (pass) =>
          Effect.gen(function* () {
            const unknown = yield* pass.invoke({
              toolName: "not_allowlisted",
              encodedArguments: {},
            });
            const invalid = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { wrong: true },
            });
            yield* Ref.set(outcomes, [unknown, invalid]);
            return null;
          }),
      });
      const [unknown, invalid] = yield* Ref.get(outcomes);
      expect(unknown).toMatchObject({
        _tag: "ProgrammaticCallError",
        errorTag: "ProgrammaticToolUnknownError",
      });
      expect(invalid).toMatchObject({
        _tag: "ProgrammaticCallError",
        errorTag: "ModelProtocolError",
      });
      expect(yield* Ref.get(invocations)).toBe(0);
    }),
  );

  it.effect("RUN-016 an approval-requiring Tool never starts programmatically", () =>
    Effect.gen(function* () {
      const invocations = yield* Ref.make(0);
      const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      const innerToolkit = Toolkit.make(ApprovalQuery);
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          approval_query: () => Ref.update(invocations, (n) => n + 1).pipe(Effect.as({ rows: [] })),
        }),
        program: (pass) =>
          Effect.gen(function* () {
            const denied = yield* pass.invoke({
              toolName: "approval_query",
              encodedArguments: { sql: "select 1" },
            });
            yield* Ref.set(outcomes, [denied]);
            return null;
          }),
      });
      const [denied] = yield* Ref.get(outcomes);
      expect(denied).toMatchObject({
        _tag: "ProgrammaticCallError",
        errorTag: "ProgrammaticApprovalUnsupportedError",
      });
      expect(yield* Ref.get(invocations)).toBe(0);
    }),
  );

  it.effect("RUN-016 a typed handler failure stays typed in the outcome", () =>
    Effect.gen(function* () {
      const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      const innerToolkit = Toolkit.make(Query);
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          query: () => Effect.fail(QueryFailure.make({ message: "no such table" })),
        }),
        program: (pass) =>
          Effect.gen(function* () {
            const failed = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "select" },
            });
            yield* Ref.set(outcomes, [failed]);
            return null;
          }),
      });
      const [failed] = yield* Ref.get(outcomes);
      expect(failed).toMatchObject({
        _tag: "ProgrammaticCallError",
        index: 0,
        errorTag: "QueryFailure",
        message: "no such table",
      });
    }),
  );

  it.effect("RUN-016 a success encoding outside JSON fails instead of crossing the boundary", () =>
    Effect.gen(function* () {
      const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      const innerToolkit = Toolkit.make(LooseTool);
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          loose: () => Effect.succeed(() => 1),
        }),
        program: (pass) =>
          Effect.gen(function* () {
            const bad = yield* pass.invoke({
              toolName: "loose",
              encodedArguments: { key: "x" },
            });
            yield* Ref.set(outcomes, [bad]);
            return null;
          }),
      });
      const [bad] = yield* Ref.get(outcomes);
      expect(bad).toMatchObject({
        _tag: "ProgrammaticCallError",
        errorTag: "ModelProtocolError",
      });
    }),
  );

  it.effect("RUN-016 broker-owned result bounds and redaction govern the sandbox boundary", () =>
    Effect.gen(function* () {
      const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      const innerToolkit = Toolkit.make(Query);
      const handlers = innerToolkit.toLayer({
        query: () => Effect.succeed({ rows: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] }),
      });
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: handlers,
        passOptions: { maxResultBytes: 8 },
        program: (pass) =>
          Effect.gen(function* () {
            const over = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "big" },
            });
            yield* Ref.set(outcomes, [over]);
            return null;
          }),
      });
      const [over] = yield* Ref.get(outcomes);
      expect(over).toMatchObject({
        _tag: "ProgrammaticCallError",
        errorTag: "ProgrammaticResultLimitError",
      });

      const redacted = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: handlers,
        passOptions: {
          maxResultBytes: 1024,
          redactResult: () => Effect.succeed({ rows: "[REDACTED]" }),
        },
        program: (pass) =>
          Effect.gen(function* () {
            const outcome = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "select" },
            });
            yield* Ref.set(redacted, [outcome]);
            return null;
          }),
      });
      const [outcome] = yield* Ref.get(redacted);
      expect(outcome).toMatchObject({
        _tag: "ProgrammaticCallSuccess",
        encodedResult: { rows: "[REDACTED]" },
      });
    }),
  );

  it.effect("RUN-017 budget exhaustion prevents the next call mid-pass", () =>
    Effect.gen(function* () {
      const invocations = yield* Ref.make(0);
      const outcomes = yield* Ref.make<ReadonlyArray<ProgrammaticCallOutcome>>([]);
      const innerToolkit = Toolkit.make(Query);
      yield* runOrchestrated({
        innerToolkit,
        innerHandlers: innerToolkit.toLayer({
          query: () => Ref.update(invocations, (n) => n + 1).pipe(Effect.as({ rows: [1] })),
        }),
        // The outer orchestrate call itself is 1 declared Tool Call, so a
        // limit of 2 leaves room for exactly one inner invocation.
        agentPolicy: policy({ maxToolCalls: 2 }),
        program: (pass) =>
          Effect.gen(function* () {
            const first = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "a" },
            });
            const second = yield* pass.invoke({
              toolName: "query",
              encodedArguments: { sql: "b" },
            });
            yield* Ref.set(outcomes, [first, second]);
            return null;
          }),
      });
      const [first, second] = yield* Ref.get(outcomes);
      expect(first).toMatchObject({ _tag: "ProgrammaticCallSuccess", index: 0 });
      expect(second).toMatchObject({
        _tag: "ProgrammaticCallError",
        errorTag: "AgentPolicyError",
      });
      expect(yield* Ref.get(invocations)).toBe(1);
    }),
  );

  it.effect("RUN-017 consumed inner calls count against the Turn-seam Tool Call limit", () =>
    Effect.gen(function* () {
      // maxToolCalls 3: the outer call (1) plus two inner calls (3 total)
      // pass mid-pass checks, but a SECOND declared batch would exceed the
      // limit at the Turn seam because programmatic calls are included.
      const innerToolkit = Toolkit.make(Query);
      const Orchestrate = Tool.make("orchestrate", {
        parameters: Schema.Struct({ plan: Schema.String }),
        success: Schema.Any,
      }).addDependency(ToolBroker);
      const outerToolkit = Toolkit.make(Orchestrate);
      const definition = Agent.make("broker-turn-seam", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Orchestrate twice.",
        toolkit: outerToolkit,
        // "fail" pins the Turn-seam fatality this test asserts; the default
        // final-answer resolution is covered by the RUN-018 suite.
        policy: policy({ maxToolCalls: 3, maxTurns: 4, onExhaustion: "fail" }),
      });
      const model = Model.make(
        "scripted",
        "turn-seam",
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
                          ? orchestrateCall("orchestrate-1")
                          : orchestrateCall("orchestrate-2"),
                      ),
                    ),
                  ),
                ),
            });
          }),
        ),
      );
      const toolLayer = outerToolkit
        .toLayer(
          Effect.gen(function* () {
            const inner = yield* innerToolkit;
            return {
              orchestrate: () =>
                Effect.gen(function* () {
                  const broker = yield* ToolBroker;
                  const pass = yield* broker
                    .openPass(inner, { maxResultBytes: 1024 })
                    .pipe(Effect.orDie);
                  yield* pass.invoke({ toolName: "query", encodedArguments: { sql: "a" } });
                  yield* pass.invoke({ toolName: "query", encodedArguments: { sql: "b" } });
                  return null;
                }),
            };
          }),
        )
        .pipe(Layer.provide(innerToolkit.toLayer({ query: () => Effect.succeed({ rows: [] }) })));

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, model),
        { question: "go" },
        {},
      ).pipe(Effect.provide(toolLayer), Effect.scoped, Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isSuccess(exit)) {
        throw new Error("Expected the Run to fail at the Turn seam");
      }
      const failure = Cause.findErrorOption(exit.cause);
      expect(Option.isSome(failure)).toBe(true);
      if (Option.isNone(failure)) {
        throw new Error("Expected a typed failure in the Cause");
      }
      expect(failure.value).toMatchObject({
        _tag: "AgentPolicyError",
        limit: "tool-calls",
      });
    }),
  );
});

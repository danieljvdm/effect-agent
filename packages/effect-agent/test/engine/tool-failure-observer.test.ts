import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  ErrorReporter,
  Exit,
  Fiber,
  Layer,
  Logger,
  Ref,
  References,
  Schema,
  Stream,
  Tracer,
} from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import { type RunEvent } from "effect-agent/run-event";
import {
  toolFailureObserverLayer,
  type RunOptions,
  type RunToolFailureObserver,
  type ToolFailureObservation,
} from "effect-agent/run-options";
import { ToolBroker, type ToolBrokerPass, type ToolBrokerService } from "effect-agent/tool-broker";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";
import type { expectTypeOf as ExpectTypeOf } from "vite-plus/test";

import { deliverToolFailure } from "../../src/engine/internal/tool-derivative.ts";
import { RunContextPreparationPassthrough } from "../../src/engine/RunOptions.ts";
import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

class QueryFailure extends Schema.TaggedError<QueryFailure>()("QueryFailure", {
  message: Schema.String,
  privateDetail: Schema.String,
}) {}

class DiagnosticSource extends Context.Service<DiagnosticSource, string>()(
  "test/DiagnosticSource",
) {}

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => ThreadId.make(`thread-observer-${++threadSequence}`)),
  nextRunId: Effect.succeed(RunId.make("run-observer")),
  nextTurnId: Effect.succeed(TurnId.make("turn-observer")),
});

const usage = { inputTokens: {}, outputTokens: {} };
const failure = QueryFailure.make({ message: "Query rejected", privateDetail: "DECLARED_SECRET" });
const outerId = "provider-call";

const call = (
  name: string,
  id = outerId,
  params: unknown = { value: 1 },
): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

const model = (calls: ReadonlyArray<Response.StreamPartEncoded>) =>
  Model.make(
    "scripted",
    "observer-test",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(turn, (n) => n + 1).pipe(
                Effect.map((n) =>
                  Stream.fromIterable<Response.StreamPartEncoded>(
                    n === 0 && calls.length > 0
                      ? [...calls, { type: "finish", reason: "tool-calls", usage }]
                      : [
                          { type: "text-start", id: "answer" },
                          { type: "text-delta", id: "answer", delta: '"recovered"' },
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
  );

const binding = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  calls: ReadonlyArray<Response.StreamPartEncoded>,
  overrides: Partial<Parameters<typeof AgentPolicy.make>[0]> = {},
) =>
  Agent.withModel(
    Agent.make("observer-test", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Use the Tool, then recover.",
      toolkit,
      policy: AgentPolicy.make({
        maxTurns: 3,
        maxToolCalls: 12,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
        repeatedFailureLimit: 0,
        ...overrides,
      }),
    }),
    model(calls),
  );

const Query = Tool.make("query", {
  parameters: Schema.Struct({ value: Schema.Int }),
  success: Schema.Unknown,
  failure: QueryFailure,
}).annotate(ToolExecutionClass, "readonly");

const Returned = Tool.make("returned", {
  parameters: Schema.Struct({ value: Schema.Int }),
  success: Schema.String,
  failure: QueryFailure,
  failureMode: "return",
}).annotate(ToolExecutionClass, "idempotent");

const queries = Toolkit.make(Query);
const returns = Toolkit.make(Returned);

const hostTools = Toolkit.make(
  Tool.make("host", {
    parameters: Schema.Struct({ value: Schema.Int }),
    success: Schema.Unknown,
    failure: QueryFailure,
    failureMode: "return",
  }).addDependency(ToolBroker),
);

const runBroker = <R>(
  program: (broker: ToolBrokerService) => Effect.Effect<unknown, QueryFailure, R>,
) =>
  AgentRuntime.stream(binding(hostTools, [call("host")]), "go").pipe(
    Stream.provide(
      hostTools.toLayer(
        Effect.gen(function* () {
          const services = yield* Effect.context<R>();

          return {
            host: () =>
              Effect.flatMap(ToolBroker, (broker) =>
                program(broker).pipe(Effect.provideContext(services)),
              ),
          };
        }),
      ),
    ),
    Stream.runCollect,
  );

const runPass = <Tools extends Record<string, Tool.Any>>(
  inner: Toolkit.WithHandler<Tools>,
  program: (pass: ToolBrokerPass) => Effect.Effect<unknown>,
) =>
  runBroker((broker) =>
    broker.openPass(inner, { maxResultBytes: 1_024 }).pipe(Effect.orDie, Effect.flatMap(program)),
  );

const collect = (observations: Array<ToolFailureObservation>): RunToolFailureObserver => ({
  observe: (observation) =>
    Effect.sync(() => {
      observations.push(observation);
    }),
});

const identity = {
  agentId: "observer-test",
  runId: "run-observer",
  turnId: "turn-observer",
};

const invoke = (pass: ToolBrokerPass) =>
  pass.invoke({ toolName: "query", encodedArguments: { value: 1 } });

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("RUN-036 trusted Tool failure observation", (it) => {
  it.effect("observes a direct declared failure once on early close, without its payload", () =>
    Effect.gen(function* () {
      {
        const observations: Array<ToolFailureObservation> = [];
        const stream = AgentRuntime.stream(binding(returns, [call("returned")]), "go");

        const events = yield* stream
          .pipe(Stream.takeUntil((event) => event._tag === "ToolCallFailed"))
          .pipe(
            Stream.runCollect,
            Effect.provide([
              returns.toLayer({ returned: () => Effect.fail(failure) }),
              toolFailureObserverLayer(collect(observations)),
            ]),
          );

        expect(observations).toEqual([
          {
            ...identity,
            threadId: events[0]?.threadId,
            _tag: "ModelToolFailure",
            kind: "declared-failure",
            toolCallId: outerId,
            toolName: "returned",
            executionClass: "idempotent",
            tag: "QueryFailure",
          },
        ]);
        expect(events.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
        expect(events.at(-1)?._tag).toBe("ToolCallFailed");
        expect(JSON.stringify(events)).not.toContain("DECLARED_SECRET");
      }
    }),
  );

  it.effect(
    "preserves the exact broker Cause, Reasons and annotations while the Run completes",
    () =>
      Effect.gen(function* () {
        const original = Cause.fromReasons([
          Cause.makeFailReason(
            QueryFailure.make({ message: "query failed", privateDetail: "CAUSE_SECRET" }),
          ),
          Cause.makeFailReason(failure),
        ]).pipe(Cause.annotate(Context.make(DiagnosticSource, "REASON_SECRET")));

        const inner: Toolkit.WithHandler<Record<string, typeof Query>> = {
          tools: queries.tools,
          handle: () => Effect.succeed(Stream.failCause(original)),
        };

        const observations: Array<ToolFailureObservation> = [];

        const events = yield* runPass(inner, (pass) => invoke(pass).pipe(Effect.as(null))).pipe(
          Effect.provide(toolFailureObserverLayer(collect(observations))),
        );

        expect(events.at(-1)?._tag).toBe("RunCompleted");
        expect(observations).toMatchObject([
          {
            ...identity,
            threadId: events[0]?.threadId,
            _tag: "ProgrammaticToolFailure",
            kind: "handler-error",
            toolName: "query",
            tag: "QueryFailure",
            toolCallId: `${outerId}#0`,
            parentToolCallId: outerId,
            sequenceIndex: 0,
            executionClass: "readonly",
          },
        ]);
        const observedCause = observations[0]?.cause;

        expect(observedCause?.reasons).toHaveLength(2);
        expect(observedCause?.reasons.filter(Cause.isFailReason)[0]?.error).toBe(
          original.reasons.filter(Cause.isFailReason)[0]?.error,
        );
        expect(observedCause?.reasons.filter(Cause.isFailReason)[1]?.error).toBe(failure);
        expect(observedCause?.reasons[0]?.annotations.get(DiagnosticSource.key)).toBe(
          "REASON_SECRET",
        );
        // Effect adds stack annotations when a failure is raised. At delivery, preserve the
        // Cause the engine actually holds, including the exact Reasons and their annotations.
        const delivered: Array<ToolFailureObservation> = [];
        const observation = observations[0];

        if (observation === undefined || observedCause === undefined)
          throw new Error("Expected a live Cause");
        yield* deliverToolFailure(collect(delivered), observation);
        expect(delivered[0]).toBe(observation);
        expect(delivered[0]?.cause).toBe(observedCause);
        expect(delivered[0]?.cause?.reasons[0]).toBe(observedCause.reasons[0]);
        expect(delivered[0]?.cause?.reasons[1]).toBe(observedCause.reasons[1]);
        expect(delivered[0]?.cause?.reasons[0]?.annotations).toBe(
          observedCause.reasons[0]?.annotations,
        );
        expect(
          events.filter((event) => "toolName" in event).every((event) => event.toolName === "host"),
        ).toBe(true);
        expect(JSON.stringify(events)).not.toContain("CAUSE_SECRET");
      }),
  );

  it.effect("isolates observer and reporter defects on direct failures", () =>
    Effect.gen(function* () {
      for (const reporterDefects of [false, true]) {
        const defects: Array<Error> = [];
        const reports: Array<Cause.Cause<unknown>> = [];
        const observations: Array<ToolFailureObservation> = [];

        const observer: RunToolFailureObserver = {
          observe: (observation) => {
            observations.push(observation);
            // A synchronous callback throw is also inside the isolation boundary.
            const defect = new Error("observer defect");

            defects.push(defect);
            throw defect;
          },
        };

        const reporting = ErrorReporter.layer([
          ErrorReporter.make(({ cause }) => {
            reports.push(cause);
            if (reporterDefects) throw new Error("reporter defect");
          }),
        ]);

        const direct = yield* AgentRuntime.stream(binding(returns, [call("returned")]), "go").pipe(
          Stream.runCollect,
          Effect.provide([
            returns.toLayer({ returned: () => Effect.fail(failure) }),
            toolFailureObserverLayer(observer),
            reporting,
          ]),
        );

        expect(direct.at(-1)?._tag).toBe("RunCompleted");
        expect(direct.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
        expect(observations).toHaveLength(1);
        expect(reports).toHaveLength(1);
        expect(
          reports.flatMap((cause) =>
            cause.reasons.filter(Cause.isDieReason).map((reason) => reason.defect),
          ),
        ).toEqual(defects);
      }
    }),
  );

  it.effect(
    "holds the call's permit during delivery and preserves interruption after its terminal event",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const terminalObserved = yield* Deferred.make<void>();
        const events: Array<RunEvent> = [];
        let starts = 0;
        let attempts = 0;
        let finalized = 0;

        const observer: RunToolFailureObserver = {
          observe: () =>
            Effect.sync(() => {
              attempts += 1;
            }).pipe(
              Effect.andThen(Deferred.succeed(entered, undefined)),
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Effect.sync(() => {
                  finalized += 1;
                }),
              ),
            ),
        };

        const fiber = yield* AgentRuntime.stream(
          binding(returns, [call("returned", "one"), call("returned", "two")]),
          "go",
        ).pipe(
          Stream.tap((event) =>
            Effect.sync(() => {
              events.push(event);
            }).pipe(
              Effect.andThen(
                event._tag === "ToolCallFailed"
                  ? Deferred.succeed(terminalObserved, undefined)
                  : Effect.void,
              ),
            ),
          ),
          Stream.runDrain,
          Effect.provide([
            returns.toLayer({
              returned: () =>
                Effect.sync(() => {
                  starts += 1;
                }).pipe(Effect.andThen(Effect.fail(failure))),
            }),
            toolFailureObserverLayer(observer),
          ]),
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        yield* Deferred.await(terminalObserved);
        expect(starts).toBe(1);
        fiber.interruptUnsafe(7_333);
        const exit = yield* Fiber.await(fiber);

        if (Exit.isSuccess(exit)) throw new Error("Expected external interruption");
        expect(Cause.hasInterrupts(exit.cause)).toBe(true);
        expect(
          events
            .filter((event) => event._tag === "ToolCallFailed")
            .map((event) => event.toolCallId),
        ).toEqual(["one"]);
        expect(events.some((event) => event._tag === "RunCompleted")).toBe(false);
        expect({ starts, attempts, finalized }).toEqual({ starts: 1, attempts: 1, finalized: 1 });
      }),
  );

  it.effect("keeps declared payloads and Causes out of automatic logs, spans and Run events", () =>
    Effect.gen(function* () {
      const observations: Array<ToolFailureObservation> = [];
      const exported: Array<unknown> = [];
      const spans: Array<Tracer.NativeSpan> = [];

      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);

          spans.push(span);

          return span;
        },
      });

      const logger = Logger.make<unknown, void>(({ message, cause, fiber }) => {
        exported.push({
          message,
          cause: Cause.pretty(cause),
          annotations: fiber.getRef(References.CurrentLogAnnotations),
        });
      });

      const original = Cause.fail(
        QueryFailure.make({ message: "query failed", privateDetail: "CAUSE_SECRET" }),
      ).pipe(Cause.annotate(Context.make(DiagnosticSource, "REASON_SECRET")));

      const inner = yield* returns.pipe(
        Effect.provide(returns.toLayer({ returned: () => Effect.fail(failure) })),
      );

      const failing: Toolkit.WithHandler<Record<string, typeof Query>> = {
        tools: queries.tools,
        handle: () => Effect.succeed(Stream.failCause(original)),
      };

      const program = Effect.gen(function* () {
        exported.push(
          yield* runPass(inner, (pass) =>
            pass
              .invoke({ toolName: "returned", encodedArguments: { value: 1 } })
              .pipe(Effect.as(null)),
          ),
        );
        exported.push(yield* runPass(failing, (pass) => invoke(pass).pipe(Effect.as(null))));
      });

      // Compare the default-none path and the installed path against the same public outcome.
      yield* program.pipe(
        Effect.provideService(Tracer.Tracer, tracer),
        Effect.provide(Logger.layer([logger])),
      );
      yield* program.pipe(
        Effect.provideService(Tracer.Tracer, tracer),
        Effect.provide([Logger.layer([logger]), toolFailureObserverLayer(collect(observations))]),
      );
      exported.push(
        ...spans.map((span) => ({
          attributes: Object.fromEntries(span.attributes),
          events: span.events,
          cause:
            span.status._tag === "Ended" && Exit.isFailure(span.status.exit)
              ? Cause.pretty(span.status.exit.cause)
              : "",
        })),
      );
      const text = JSON.stringify(exported);

      for (const secret of ["DECLARED_SECRET", "CAUSE_SECRET", "REASON_SECRET"])
        expect(text).not.toContain(secret);
      expect(observations).toHaveLength(2);
      expect(observations[1]?.cause?.reasons.filter(Cause.isFailReason)[0]?.error).toBe(
        original.reasons.filter(Cause.isFailReason)[0]?.error,
      );
    }),
  );
});

export const verifyNarrowsDiagnosticsByKindAndPreservesInferredRunStreamStartErrorAndRequirementChannels =
  () => {
    type HandlerError = Extract<ToolFailureObservation, { readonly kind: "handler-error" }>;
    type DeclaredFailure = Extract<ToolFailureObservation, { readonly kind: "declared-failure" }>;
    type Diagnostic = Extract<
      ToolFailureObservation,
      { readonly kind: "infrastructure" | "protocol" }
    >;
    expectTypeOf<HandlerError["cause"]>().toEqualTypeOf<Cause.Cause<unknown>>();
    expectTypeOf<HandlerError["message"]>().toEqualTypeOf<undefined>();
    expectTypeOf<DeclaredFailure["cause"]>().toEqualTypeOf<undefined>();
    expectTypeOf<DeclaredFailure["message"]>().toEqualTypeOf<undefined>();
    expectTypeOf<Diagnostic["message"]>().toEqualTypeOf<string>();
    const agent = binding(queries, [call("query")]);

    const options: RunOptions<QueryFailure, DiagnosticSource> = {
      budget: { guard: (effect) => effect, consume: () => Effect.asVoid(DiagnosticSource) },
    };

    const observer = toolFailureObserverLayer({
      observe: (observation) =>
        observation.kind === "handler-error"
          ? ErrorReporter.report(observation.cause)
          : Effect.void,
    });

    const run = AgentRuntime.run(agent, "go", options);
    const observedRun = run.pipe(Effect.provide(observer));
    const stream = AgentRuntime.stream(agent, "go", options);
    const observedStream = stream.pipe(Stream.provide(observer));
    const start = AgentRuntime.start(agent, "go", options);
    const observedStart = start.pipe(Effect.provide(observer));

    expectTypeOf<Effect.Error<typeof observedRun>>().toEqualTypeOf<Effect.Error<typeof run>>();
    expectTypeOf<Effect.Services<typeof observedRun>>().toEqualTypeOf<
      Effect.Services<typeof run>
    >();
    expectTypeOf<Stream.Error<typeof observedStream>>().toEqualTypeOf<
      Stream.Error<typeof stream>
    >();
    expectTypeOf<Stream.Services<typeof observedStream>>().toEqualTypeOf<
      Stream.Services<typeof stream>
    >();
    expectTypeOf<Effect.Error<typeof observedStart>>().toEqualTypeOf<Effect.Error<typeof start>>();
    expectTypeOf<Effect.Services<typeof observedStart>>().toEqualTypeOf<
      Effect.Services<typeof start>
    >();
  };

declare const expectTypeOf: typeof ExpectTypeOf;

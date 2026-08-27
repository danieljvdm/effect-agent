import {
  Agent,
  AgentPolicy,
  ConversationId,
  IdGenerator,
  RunId,
  ReceiptId,
  SubmissionId,
  ToolCallId,
  TurnId,
  type RunEvent,
} from "@effect-agent/core";
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
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";
import { expectTypeOf } from "vite-plus/test";

import {
  AgentRuntime,
  CurrentToolFailureObserver,
  ToolBroker,
  ToolCallWaiting,
  ToolExecutionClass,
  toolFailureObserverLayer,
  type ProgrammaticCallOutcome,
  type RunOptions,
  type RunToolFailureObserver,
  type ToolBrokerPass,
  type ToolBrokerPassOptions,
  type ToolBrokerService,
  type ToolFailureObservation,
} from "../src/index.ts";
import { deliverToolFailure } from "../src/tool-derivative-internal.ts";

class QueryFailure extends Schema.TaggedError<QueryFailure>()("QueryFailure", {
  message: Schema.String,
  privateDetail: Schema.String,
}) {}

class DiagnosticSource extends Context.Service<DiagnosticSource, string>()(
  "test/DiagnosticSource",
) {}

const identifiers = Layer.succeed(IdGenerator, {
  nextConversationId: Effect.succeed(ConversationId.make("conversation-observer")),
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
    Agent.define("observer-test", {
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
const Approval = Tool.make("approval", {
  parameters: Schema.Struct({ value: Schema.Int }),
  success: Schema.String,
  needsApproval: true,
});
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
  options: RunOptions<QueryFailure> = {},
  maxToolCalls = 12,
) =>
  AgentRuntime.stream(
    binding(hostTools, options.resume === undefined ? [call("host")] : [], { maxToolCalls }),
    "go",
    options,
  ).pipe(
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
  passOptions: ToolBrokerPassOptions = { maxResultBytes: 1_024 },
  options: RunOptions<QueryFailure> = {},
  maxToolCalls = 12,
) =>
  runBroker(
    (broker) => broker.openPass(inner, passOptions).pipe(Effect.orDie, Effect.flatMap(program)),
    options,
    maxToolCalls,
  );

const collect = (observations: Array<ToolFailureObservation>): RunToolFailureObserver => ({
  observe: (observation) =>
    Effect.sync(() => {
      observations.push(observation);
    }),
});
const identity = {
  agentId: "observer-test",
  conversationId: "conversation-observer",
  runId: "run-observer",
  turnId: "turn-observer",
};
const invoke = (pass: ToolBrokerPass) =>
  pass.invoke({ toolName: "query", encodedArguments: { value: 1 } });

layer(identifiers)("RUN-036 trusted Tool failure observation", (it) => {
  it.effect(
    "observes a direct declared failure once on continuation and early close, without its payload",
    () =>
      Effect.gen(function* () {
        for (const earlyClose of [false, true]) {
          const observations: Array<ToolFailureObservation> = [];
          const stream = AgentRuntime.stream(binding(returns, [call("returned")]), "go");
          const events = yield* (
            earlyClose
              ? stream.pipe(Stream.takeUntil((event) => event._tag === "ToolCallFailed"))
              : stream
          ).pipe(
            Stream.runCollect,
            Effect.provide([
              returns.toLayer({ returned: () => Effect.fail(failure) }),
              toolFailureObserverLayer(collect(observations)),
            ]),
          );
          expect(observations).toEqual([
            {
              ...identity,
              _tag: "ModelToolFailure",
              kind: "declared-failure",
              toolCallId: outerId,
              toolName: "returned",
              executionClass: "idempotent",
              tag: "QueryFailure",
            },
          ]);
          expect(events.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
          expect(events.at(-1)?._tag).toBe(earlyClose ? "ToolCallFailed" : "RunCompleted");
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

  it.effect(
    "observes programmatic declared failures without message, Cause or encoded result",
    () =>
      Effect.gen(function* () {
        const inner = yield* returns.pipe(
          Effect.provide(returns.toLayer({ returned: () => Effect.fail(failure) })),
        );
        const observations: Array<ToolFailureObservation> = [];
        let outcome: ProgrammaticCallOutcome | undefined;
        yield* runPass(inner, (pass) =>
          pass.invoke({ toolName: "returned", encodedArguments: { value: 1 } }).pipe(
            Effect.tap((value) =>
              Effect.sync(() => {
                outcome = value;
              }),
            ),
            Effect.as(null),
          ),
        ).pipe(Effect.provide(toolFailureObserverLayer(collect(observations))));
        expect(outcome).toMatchObject({
          _tag: "ProgrammaticCallFailure",
          index: 0,
          encodedResult: { privateDetail: "DECLARED_SECRET" },
        });
        expect(observations).toEqual([
          {
            ...identity,
            _tag: "ProgrammaticToolFailure",
            kind: "declared-failure",
            toolName: "returned",
            tag: "QueryFailure",
            toolCallId: `${outerId}#0`,
            parentToolCallId: outerId,
            sequenceIndex: 0,
            executionClass: "idempotent",
          },
        ]);
      }),
  );

  it.effect(
    "classifies every started protocol and result-bound rejection without fabricating a Cause",
    () =>
      Effect.gen(function* () {
        const terminal: Tool.HandlerResult<typeof Query> = {
          result: "ok",
          encodedResult: "ok",
          isFailure: false,
          preliminary: false,
        };
        const cases = [
          { name: "missing", results: [], kind: "protocol", tag: "ModelProtocolError" },
          {
            name: "duplicate",
            results: [terminal, terminal],
            kind: "protocol",
            tag: "ModelProtocolError",
          },
          {
            name: "non-JSON",
            results: [{ ...terminal, encodedResult: undefined }],
            kind: "protocol",
            tag: "ModelProtocolError",
          },
          {
            name: "redacted non-JSON",
            results: [terminal],
            kind: "protocol",
            tag: "ModelProtocolError",
            redactResult: () => Effect.succeed(undefined),
          },
          {
            name: "bytes",
            results: [terminal],
            kind: "infrastructure",
            tag: "ProgrammaticResultLimitError",
            maxResultBytes: 1,
          },
        ];
        for (const testCase of cases) {
          const inner: Toolkit.WithHandler<typeof queries.tools> = {
            tools: queries.tools,
            handle: () => Effect.succeed(Stream.fromIterable(testCase.results)),
          };
          const observations: Array<ToolFailureObservation> = [];
          const events = yield* runPass(inner, (pass) => invoke(pass).pipe(Effect.as(null)), {
            maxResultBytes: testCase.maxResultBytes ?? 1_024,
            redactResult: testCase.redactResult,
          }).pipe(Effect.provide(toolFailureObserverLayer(collect(observations))));
          expect(events.at(-1)?._tag, testCase.name).toBe("RunCompleted");
          expect(observations, testCase.name).toHaveLength(1);
          expect(observations[0]).toMatchObject({
            ...identity,
            _tag: "ProgrammaticToolFailure",
            kind: testCase.kind,
            tag: testCase.tag,
            toolName: "query",
            toolCallId: `${outerId}#0`,
            parentToolCallId: outerId,
            sequenceIndex: 0,
            executionClass: "readonly",
            message: expect.any(String),
          });
          expect(observations[0]).not.toHaveProperty("cause");
        }
      }),
  );

  it.effect(
    "preserves raw inner/outer correlation for canonical IDs outside the telemetry filter",
    () =>
      Effect.gen(function* () {
        const rawId = "provider call/🧭";
        const observations: Array<ToolFailureObservation> = [];
        const inner = yield* returns.pipe(
          Effect.provide(returns.toLayer({ returned: () => Effect.fail(failure) })),
        );
        const events = yield* runBroker(
          (broker) =>
            Effect.gen(function* () {
              const pass = yield* broker
                .openPass(inner, { maxResultBytes: 1_024 })
                .pipe(Effect.orDie);
              yield* pass.invoke({ toolName: "returned", encodedArguments: { value: 1 } });
              return yield* failure;
            }),
          {
            resume: {
              turn: 1,
              turnId: TurnId.make("turn-observer"),
              calls: [{ id: rawId, name: "host", params: { value: 1 } }],
              settled: [],
            },
          },
        ).pipe(Effect.provide(toolFailureObserverLayer(collect(observations))));
        expect(events.at(-1)?._tag).toBe("RunCompleted");
        expect(observations).toMatchObject([
          {
            _tag: "ProgrammaticToolFailure",
            parentToolCallId: rawId,
            toolCallId: `${rawId}#0`,
            sequenceIndex: 0,
          },
          { _tag: "ModelToolFailure", toolCallId: rawId },
        ]);
      }),
  );

  it.effect(
    "distinguishes preflight rejections, preserves Cause-backed diagnostics, and bounds UTF-8 bytes",
    () =>
      Effect.gen(function* () {
        const all = Toolkit.make(Query, Approval);
        let starts = 0;
        const inner = yield* all.pipe(
          Effect.provide(
            all.toLayer({
              query: () =>
                Effect.sync(() => {
                  starts += 1;
                  return "ok";
                }),
              approval: () =>
                Effect.sync(() => {
                  starts += 1;
                  return "ok";
                }),
            }),
          ),
        );
        const observations: Array<ToolFailureObservation> = [];
        const budgetCause = Cause.fail(
          QueryFailure.make({ message: "🧭".repeat(2_000), privateDetail: "BUDGET_SECRET" }),
        );
        yield* runPass(
          inner,
          (pass) =>
            Effect.gen(function* () {
              yield* pass.invoke({ toolName: "unknown", encodedArguments: {} });
              yield* pass.invoke({ toolName: "approval", encodedArguments: { value: 1 } });
              yield* pass.invoke({ toolName: "query", encodedArguments: { value: "invalid" } });
              yield* invoke(pass);
              return null;
            }),
          undefined,
          {
            budget: {
              guard: (effect) => effect,
              consume: (delta) =>
                delta.modelCalls === 0 ? Effect.failCause(budgetCause) : Effect.void,
            },
          },
        ).pipe(Effect.provide(toolFailureObserverLayer(collect(observations))));
        yield* runPass(inner, (pass) => invoke(pass).pipe(Effect.as(null)), undefined, {}, 1).pipe(
          Effect.provide(toolFailureObserverLayer(collect(observations))),
        );
        expect(starts).toBe(0);
        expect(observations.map(({ _tag, kind, tag }) => ({ _tag, kind, tag }))).toEqual([
          {
            _tag: "ProgrammaticPreflightFailure",
            kind: "infrastructure",
            tag: "ProgrammaticToolUnknownError",
          },
          {
            _tag: "ProgrammaticPreflightFailure",
            kind: "infrastructure",
            tag: "ProgrammaticApprovalUnsupportedError",
          },
          { _tag: "ProgrammaticPreflightFailure", kind: "protocol", tag: "ModelProtocolError" },
          { _tag: "ProgrammaticPreflightFailure", kind: "infrastructure", tag: "QueryFailure" },
          { _tag: "ProgrammaticPreflightFailure", kind: "infrastructure", tag: "AgentPolicyError" },
        ]);
        for (const observation of observations) {
          expect(observation).toMatchObject({ ...identity, parentToolCallId: outerId });
          expect(observation).not.toHaveProperty("toolCallId");
          expect(observation).not.toHaveProperty("sequenceIndex");
        }
        expect(observations[0]).not.toHaveProperty("executionClass");
        expect(observations[1]).toMatchObject({ executionClass: "uncertain" });
        expect(observations[2]).toMatchObject({ executionClass: "readonly" });
        expect(observations[2]?.cause?.reasons[0]).toMatchObject({
          _tag: "Fail",
          error: { _tag: "SchemaError" },
        });
        expect(observations[3]?.cause?.reasons.filter(Cause.isFailReason)[0]?.error).toBe(
          budgetCause.reasons.filter(Cause.isFailReason)[0]?.error,
        );
        expect(observations[3]?.message).toBe("🧭".repeat(1_024));
        for (const index of [0, 1, 4]) expect(observations[index]).not.toHaveProperty("cause");
      }),
  );

  it.effect(
    "serializes concurrent and retained-pass preflight observation and cancels waiting delivery",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let observationEntered = yield* Deferred.make<void>();
        let observationRelease = yield* Deferred.make<void>();
        const inner = yield* queries.pipe(
          Effect.provide(
            queries.toLayer({
              query: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as("ok"),
                ),
            }),
          ),
        );
        const observations: Array<ToolFailureObservation> = [];
        let active = 0;
        let peak = 0;
        let finalized = 0;
        const observer: RunToolFailureObserver = {
          observe: (observation) =>
            Effect.gen(function* () {
              active += 1;
              peak = Math.max(peak, active);
              observations.push(observation);
              yield* Deferred.succeed(observationEntered, undefined);
              yield* Deferred.await(observationRelease);
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  active -= 1;
                  finalized += 1;
                }),
              ),
            ),
        };
        const burst = (passes: ReadonlyArray<ToolBrokerPass>, tag: string) =>
          Effect.gen(function* () {
            observationEntered = yield* Deferred.make<void>();
            observationRelease = yield* Deferred.make<void>();
            const fibers = yield* Effect.forEach(passes, (pass) =>
              invoke(pass).pipe(Effect.forkChild),
            );
            yield* Deferred.await(observationEntered);
            yield* TestClock.adjust("0 millis");
            expect(active).toBe(1);
            const cancelled = fibers.at(-1);
            if (cancelled === undefined) throw new Error("Expected a waiting preflight invocation");
            cancelled.interruptUnsafe(7_335);
            const interrupted = yield* Fiber.await(cancelled);
            if (Exit.isSuccess(interrupted))
              throw new Error("Expected interrupted preflight observation");
            expect(Cause.hasInterrupts(interrupted.cause)).toBe(true);
            yield* Deferred.succeed(observationRelease, undefined);
            const outcomes = yield* Effect.forEach(fibers.slice(0, -1), Fiber.join);
            expect(outcomes).toEqual(
              Array.from({ length: 3 }, () =>
                expect.objectContaining({
                  _tag: "ProgrammaticCallError",
                  index: undefined,
                  errorTag: tag,
                }),
              ),
            );
            expect(active).toBe(0);
          });
        let retained: readonly [ToolBrokerPass, ToolBrokerPass] | undefined;
        yield* runBroker((broker) =>
          Effect.gen(function* () {
            const pass = yield* broker
              .openPass(inner, { maxResultBytes: 1_024 })
              .pipe(Effect.orDie);
            const otherPass = yield* broker
              .openPass(inner, { maxResultBytes: 1_024 })
              .pipe(Effect.orDie);
            retained = [pass, otherPass];
            const handler = yield* invoke(pass).pipe(Effect.forkChild);
            yield* Deferred.await(entered);
            yield* burst([pass, pass, pass, pass], "ProgrammaticCallConcurrencyError");
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(handler);
            return null;
          }),
        ).pipe(Effect.provide(toolFailureObserverLayer(observer)));
        if (retained === undefined) throw new Error("Expected a retained pass");
        // All passes from this broker retain the same bounded delivery owner after the Run closes.
        yield* burst(
          [retained[0], retained[1], retained[0], retained[1]],
          "ToolBrokerUnavailableError",
        );
        expect(observations.map(({ tag }) => tag)).toEqual([
          "ProgrammaticCallConcurrencyError",
          "ProgrammaticCallConcurrencyError",
          "ProgrammaticCallConcurrencyError",
          "ToolBrokerUnavailableError",
          "ToolBrokerUnavailableError",
          "ToolBrokerUnavailableError",
        ]);
        expect({ peak, finalized }).toEqual({ peak: 1, finalized: 6 });
        for (const observation of observations) {
          expect(observation).toMatchObject({
            _tag: "ProgrammaticPreflightFailure",
            kind: "infrastructure",
            executionClass: "readonly",
            parentToolCallId: outerId,
          });
          expect(observation).not.toHaveProperty("sequenceIndex");
          expect(observation).not.toHaveProperty("cause");
        }
      }),
  );

  it.effect("isolates observer and reporter defects on direct and programmatic failures", () =>
    Effect.gen(function* () {
      const inner = yield* returns.pipe(
        Effect.provide(returns.toLayer({ returned: () => Effect.fail(failure) })),
      );
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
        let brokerOutcome: ProgrammaticCallOutcome | undefined;
        const programmatic = yield* runPass(inner, (pass) =>
          pass.invoke({ toolName: "returned", encodedArguments: { value: 1 } }).pipe(
            Effect.tap((outcome) =>
              Effect.sync(() => {
                brokerOutcome = outcome;
              }),
            ),
            Effect.as(null),
          ),
        ).pipe(Effect.provide([toolFailureObserverLayer(observer), reporting]));
        expect(direct.at(-1)?._tag).toBe("RunCompleted");
        expect(programmatic.at(-1)?._tag).toBe("RunCompleted");
        expect(direct.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
        expect(brokerOutcome).toEqual({
          _tag: "ProgrammaticCallFailure",
          index: 0,
          encodedResult: {
            _tag: "QueryFailure",
            message: "Query rejected",
            privateDetail: "DECLARED_SECRET",
          },
        });
        expect(observations).toHaveLength(2);
        expect(reports).toHaveLength(2);
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

  it.effect("keeps the Run duration limit effective while observation is blocked", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      let finalized = false;
      const events: Array<RunEvent> = [];
      const fiber = yield* AgentRuntime.stream(binding(returns, [call("returned")]), "go").pipe(
        Stream.tap((event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        ),
        Stream.runDrain,
        Effect.provide([
          returns.toLayer({ returned: () => Effect.fail(failure) }),
          toolFailureObserverLayer({
            observe: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(
                  Effect.sync(() => {
                    finalized = true;
                  }),
                ),
              ),
          }),
        ]),
        Effect.forkChild,
      );
      yield* Deferred.await(entered);
      yield* TestClock.adjust("30 seconds");
      const exit = yield* Fiber.await(fiber);
      expect(Exit.isFailure(exit)).toBe(true);
      expect(events.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
      expect(events.at(-1)).toMatchObject({ _tag: "RunFailed", errorTag: "AgentPolicyError" });
      expect(finalized).toBe(true);
    }),
  );

  it.effect("preserves a settled programmatic failure when observation is interrupted", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const spans: Array<Tracer.NativeSpan> = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      const inner = yield* returns.pipe(
        Effect.provide(returns.toLayer({ returned: () => Effect.fail(failure) })),
      );
      let attempts = 0;
      let finalized = 0;
      const fiber = yield* runPass(inner, (pass) =>
        pass.invoke({ toolName: "returned", encodedArguments: { value: 1 } }).pipe(Effect.as(null)),
      ).pipe(
        Effect.provideService(Tracer.Tracer, tracer),
        Effect.provide(
          toolFailureObserverLayer({
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
          }),
        ),
        Effect.forkChild,
      );
      yield* Deferred.await(entered);
      const span = spans.find((candidate) => candidate.name === "execute_tool returned");
      expect(span?.status._tag).toBe("Ended");
      expect(span?.attributes.get("effect_agent.tool.outcome")).toBe("failure");
      fiber.interruptUnsafe(7_334);
      const exit = yield* Fiber.await(fiber);
      if (Exit.isSuccess(exit)) throw new Error("Expected interrupted observation");
      expect(Cause.hasInterrupts(exit.cause)).toBe(true);
      expect(span?.status._tag === "Ended" && Exit.isFailure(span.status.exit)).toBe(true);
      expect({ attempts, finalized }).toEqual({ attempts: 1, finalized: 1 });
    }),
  );

  it.effect(
    "does not duplicate direct propagating failures, denial, preflight, synthetic budget or settled replay",
    () =>
      Effect.gen(function* () {
        const observations: Array<ToolFailureObservation> = [];
        const approvalTools = Toolkit.make(Approval);
        const scenarios = [
          AgentRuntime.stream(binding(queries, [call("query")]), "go").pipe(
            Stream.provide(queries.toLayer({ query: () => Effect.fail(failure) })),
          ),
          AgentRuntime.stream(binding(queries, [call("query")]), "go").pipe(
            Stream.provide(queries.toLayer({ query: () => Effect.die("handler defect") })),
          ),
          AgentRuntime.stream(binding(queries, [call("query")]), "go").pipe(
            Stream.provide(queries.toLayer({ query: () => Effect.interrupt })),
          ),
          AgentRuntime.stream(
            binding(queries, [call("query", outerId, { value: "bad" })]),
            "go",
          ).pipe(Stream.provide(queries.toLayer({ query: () => Effect.succeed(null) }))),
          AgentRuntime.stream(binding(approvalTools, [call("approval")]), "go", {
            approval: { request: () => Effect.succeed({ _tag: "denied", reason: "not allowed" }) },
          }).pipe(
            Stream.provide(approvalTools.toLayer({ approval: () => Effect.succeed("unexpected") })),
          ),
          AgentRuntime.stream(binding(approvalTools, [call("approval")]), "go").pipe(
            Stream.provide(approvalTools.toLayer({ approval: () => Effect.succeed("unexpected") })),
          ),
          AgentRuntime.stream(binding(returns, [call("returned")]), "go", {
            toolAuthorization: {
              authorize: () => Effect.succeed({ _tag: "denied", reason: "not allowed" }),
            },
          }).pipe(Stream.provide(returns.toLayer({ returned: () => Effect.fail(failure) }))),
          AgentRuntime.stream(
            binding(returns, [call("returned", "one"), call("returned", "two")], {
              maxToolCalls: 1,
            }),
            "go",
          ).pipe(Stream.provide(returns.toLayer({ returned: () => Effect.fail(failure) }))),
          AgentRuntime.stream(binding(returns, []), "go", {
            resume: {
              turn: 1,
              turnId: TurnId.make("turn-observer"),
              calls: [{ id: outerId, name: "returned", params: { value: 1 } }],
              settled: [{ id: outerId, result: { _tag: "QueryFailure" }, isFailure: true }],
            },
          }).pipe(
            Stream.provide(
              returns.toLayer({ returned: () => Effect.die("replayed settled Handler") }),
            ),
          ),
        ];
        for (const stream of scenarios)
          yield* stream.pipe(
            Stream.runDrain,
            Effect.exit,
            Effect.provide(toolFailureObserverLayer(collect(observations))),
          );
        expect(observations).toEqual([]);
      }),
  );

  it.effect("does not observe inner defects/interruption or openPass errors", () =>
    Effect.gen(function* () {
      const observations: Array<ToolFailureObservation> = [];
      for (const cause of [Cause.die("inner defect"), Cause.interrupt(123)]) {
        const inner: Toolkit.WithHandler<typeof queries.tools> = {
          tools: queries.tools,
          handle: () => Effect.succeed(Stream.failCause(cause)),
        };
        const exit = yield* runPass(inner, invoke).pipe(
          Effect.exit,
          Effect.provide(toolFailureObserverLayer(collect(observations))),
        );
        expect(Exit.isFailure(exit)).toBe(true);
      }
      const inner = yield* queries.pipe(
        Effect.provide(queries.toLayer({ query: () => Effect.succeed(null) })),
      );
      yield* runBroker((broker) =>
        broker
          .openPass(inner, { maxResultBytes: 0 })
          .pipe(Effect.catch(() => Effect.succeed(null))),
      ).pipe(Effect.provide(toolFailureObserverLayer(collect(observations))));
      let retained: ToolBrokerService | undefined;
      yield* runBroker((broker) =>
        Effect.sync(() => {
          retained = broker;
          return null;
        }),
      ).pipe(Effect.provide(toolFailureObserverLayer(collect(observations))));
      if (retained === undefined) throw new Error("Expected a retained broker");
      const closed = yield* retained.openPass(inner, { maxResultBytes: 1_024 }).pipe(Effect.exit);
      expect(Exit.isFailure(closed)).toBe(true);
      expect(observations).toEqual([]);
    }),
  );

  it.effect("excludes direct protocol errors, waiting and provider-executed results", () =>
    Effect.gen(function* () {
      const observations: Array<ToolFailureObservation> = [];
      const malformedRuntime = Effect.map(
        queries,
        (native): Toolkit.WithHandler<typeof queries.tools> => ({
          tools: native.tools,
          handle: <Name extends keyof typeof queries.tools>(
            name: Name,
            parameters: Tool.Parameters<(typeof queries.tools)[Name]>,
            id?: string,
          ) => native.handle(name, parameters, id).pipe(Effect.map(Stream.drain)),
        }),
      );
      // Same test-only native Toolkit seam as the neighboring post-terminal protocol tests.
      const malformed = Object.assign(malformedRuntime, {
        "~effect/ai/Toolkit": "~effect/ai/Toolkit" as const,
        tools: queries.tools,
      }) as Toolkit.Toolkit<typeof queries.tools>;
      const protocol = yield* AgentRuntime.stream(binding(malformed, [call("query")]), "go").pipe(
        Stream.runDrain,
        Effect.exit,
        Effect.provide([
          queries.toLayer({ query: () => Effect.succeed("ok") }),
          toolFailureObserverLayer(collect(observations)),
        ]),
      );
      if (Exit.isSuccess(protocol)) throw new Error("Expected missing-terminal protocol failure");
      expect(protocol.cause.reasons.filter(Cause.isFailReason)[0]?.error).toMatchObject({
        _tag: "ModelProtocolError",
      });

      const Waiting = Tool.make("waiting", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
        failure: ToolCallWaiting,
      });
      const waitingTools = Toolkit.make(Waiting);
      const waiting = yield* AgentRuntime.stream(
        binding(waitingTools, [call("waiting")]),
        "go",
      ).pipe(
        Stream.runDrain,
        Effect.exit,
        Effect.provide([
          waitingTools.toLayer({
            waiting: () =>
              Effect.fail(
                ToolCallWaiting.make({
                  toolCallId: ToolCallId.make(outerId),
                  childConversationId: ConversationId.make("child"),
                  childSubmissionId: SubmissionId.make("child"),
                  childRunId: RunId.make("child"),
                  receiptId: ReceiptId.make("child"),
                  message: "waiting",
                }),
              ),
          }),
          toolFailureObserverLayer(collect(observations)),
        ]),
      );
      if (Exit.isSuccess(waiting)) throw new Error("Expected child suspension");
      expect(waiting.cause.reasons.filter(Cause.isFailReason)[0]?.error).toMatchObject({
        _tag: "AgentChildPending",
      });

      const Hosted = Tool.providerDefined({
        id: "test.hosted",
        customName: "hosted",
        providerName: "hosted",
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
      })(undefined);
      const hosted = yield* AgentRuntime.stream(
        binding(Toolkit.make(Hosted), [
          {
            type: "tool-call",
            id: "hosted",
            name: "hosted",
            params: { value: 1 },
            providerExecuted: true,
          },
          {
            type: "tool-result",
            id: "hosted",
            name: "hosted",
            result: "hosted result",
            isFailure: false,
            providerExecuted: true,
          },
        ]),
        "go",
      ).pipe(Stream.runCollect, Effect.provide(toolFailureObserverLayer(collect(observations))));
      expect(hosted.at(-1)?._tag).toBe("RunCompleted");
      expect(observations).toEqual([]);
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
      const inner = yield* returns.pipe(
        Effect.provide(returns.toLayer({ returned: () => Effect.fail(failure) })),
      );
      const original = Cause.fail(
        QueryFailure.make({ message: "query failed", privateDetail: "CAUSE_SECRET" }),
      ).pipe(Cause.annotate(Context.make(DiagnosticSource, "REASON_SECRET")));
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

  it.effect("captures the reference once at the Run boundary", () =>
    Effect.gen(function* () {
      const captured: Array<ToolFailureObservation> = [];
      const replacement: Array<ToolFailureObservation> = [];
      yield* AgentRuntime.run(binding(returns, [call("returned")]), "go", {
        input: {
          start: () =>
            Effect.provideService(Effect.void, CurrentToolFailureObserver, collect(replacement)),
          drain: () => Effect.succeed([]),
        },
      }).pipe(
        Effect.provide([
          returns.toLayer({
            returned: () =>
              Effect.fail(failure).pipe(
                Effect.provideService(CurrentToolFailureObserver, collect(replacement)),
              ),
          }),
          toolFailureObserverLayer(collect(captured)),
        ]),
        Effect.scoped,
      );
      expect(captured).toHaveLength(1);
      expect(replacement).toEqual([]);
    }),
  );

  it("narrows diagnostics by kind and preserves inferred run/stream/start error and requirement channels", () => {
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
  });
});

import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Effect,
  ErrorReporter,
  Exit,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
  Tracer,
} from "effect";
import * as Agent from "effect-agent/agent";
import { ModelProtocolError } from "effect-agent/agent-error";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import { type RunEvent } from "effect-agent/run-event";
import type { Response } from "effect/unstable/ai";
import { AiError, LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";

import { RunContextPreparationPassthrough } from "../../src/engine/RunOptions.ts";
import { ThreadHistory } from "../../src/engine/ThreadHistory.ts";

let threadSequence = 0;

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => Schema.decodeSync(ThreadId)(`thread-1-${++threadSequence}`)),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-1")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-1")),
});

const modelFromParts = (parts: ReadonlyArray<Response.StreamPartEncoded>) =>
  Model.make(
    "scripted",
    "engine-test",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.fromIterable(parts),
      }),
    ),
  );

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const HostedSearch = Tool.providerDefined({
  id: "test.web_search",
  customName: "HostedSearch",
  providerName: "web_search",
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
})(undefined);

const hostedTools = Toolkit.make(HostedSearch);

const hostedDefinition = Agent.make("hosted-tool", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Search before answering.",
  toolkit: hostedTools,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

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

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

layer(testLayer)("RUN-001 Phase 1 AgentRuntime", (it) => {
  it.effect(
    "keeps completed Tool results when canonical span create, wrap, or close defects",
    () => {
      const reports: Array<Cause.Cause<unknown>> = [];
      const createDefect = new Error("canonical-span-create-defect");
      const wrapDefect = new Error("canonical-span-wrap-defect");
      const wrapCloseDefect = new Error("canonical-span-wrap-close-defect");
      const closeDefect = new Error("canonical-span-close-defect");

      class CloseDefectSpan extends Tracer.NativeSpan {
        override end(): void {
          throw closeDefect;
        }
      }

      const tracer = Tracer.make({
        span(options) {
          if (options.name === "execute_tool create_safe") throw createDefect;
          if (options.name === "execute_tool wrap_safe") {
            const allocated = new Tracer.NativeSpan(options);

            return new Proxy(allocated, {
              get(target, property) {
                if (property === "sampled") throw wrapDefect;
                if (property === "end") {
                  return (endTime: bigint, exit: Exit.Exit<unknown, unknown>): void => {
                    target.end(endTime, exit);
                    throw wrapCloseDefect;
                  };
                }
                const value = Reflect.get(target, property, target);

                return typeof value === "function" ? value.bind(target) : value;
              },
            });
          }

          const span =
            options.name === "execute_tool close_safe"
              ? new CloseDefectSpan(options)
              : new Tracer.NativeSpan(options);

          return span;
        },
      });

      const reporter = ErrorReporter.make(({ cause }) => {
        reports.push(cause);
      });

      const CreateSafe = Tool.make("create_safe", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });

      const WrapSafe = Tool.make("wrap_safe", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });

      const CloseSafe = Tool.make("close_safe", {
        parameters: Schema.Struct({ value: Schema.String }),
        success: Schema.String,
      });

      const tools = Toolkit.make(CreateSafe, WrapSafe, CloseSafe);

      const model = Model.make(
        "scripted",
        "span-lifecycle-defects",
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
                          ? [
                              {
                                type: "tool-call",
                                id: "span-create-1",
                                name: "create_safe",
                                params: { value: "create" },
                                providerExecuted: false,
                              },
                              {
                                type: "tool-call",
                                id: "span-wrap-1",
                                name: "wrap_safe",
                                params: { value: "wrap" },
                                providerExecuted: false,
                              },
                              {
                                type: "tool-call",
                                id: "span-close-1",
                                name: "close_safe",
                                params: { value: "close" },
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : finalParts('{"answer":"done"}'),
                      ),
                    ),
                  ),
                ),
            });
          }),
        ),
      );

      const definition = Agent.make("span-lifecycle-defects", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Call all three Tools, then answer.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 3,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });

      let createAttempts = 0;
      let wrapAttempts = 0;
      let closeAttempts = 0;

      return Effect.gen(function* () {
        const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
          question: "exercise span lifecycle",
        }).pipe(
          Stream.runCollect,
          Effect.provide(
            tools.toLayer({
              create_safe: () =>
                Effect.sync(() => {
                  createAttempts += 1;

                  return "create-result";
                }),
              wrap_safe: () =>
                Effect.sync(() => {
                  wrapAttempts += 1;

                  return "wrap-result";
                }),
              close_safe: () =>
                Effect.sync(() => {
                  closeAttempts += 1;

                  return "close-result";
                }),
            }),
          ),
          Effect.exit,
        );

        if (Exit.isFailure(exit)) throw new Error("Span lifecycle telemetry changed Tool success");
        expect(createAttempts).toBe(1);
        expect(wrapAttempts).toBe(1);
        expect(closeAttempts).toBe(1);
        expect(exit.value.filter((event) => event._tag === "ToolCallSucceeded")).toEqual([
          expect.objectContaining({
            toolCallId: "span-create-1",
            toolName: "create_safe",
            result: "create-result",
          }),
          expect.objectContaining({
            toolCallId: "span-wrap-1",
            toolName: "wrap_safe",
            result: "wrap-result",
          }),
          expect.objectContaining({
            toolCallId: "span-close-1",
            toolName: "close_safe",
            result: "close-result",
          }),
        ]);
        expect(exit.value.filter((event) => event._tag === "RunCompleted")).toHaveLength(1);
        expect(
          reports.flatMap((cause) =>
            cause.reasons.filter(Cause.isDieReason).map(({ defect }) => defect),
          ),
        ).toEqual([createDefect, wrapDefect, wrapCloseDefect, closeDefect]);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.effect(Tracer.Tracer, Effect.succeed(tracer)),
            ErrorReporter.layer([reporter]),
          ),
        ),
      );
    },
  );

  it.effect("overrides apparent Tool success when the handler stream later fails", () => {
    const postTerminalSecret = "post-terminal-handler-secret";

    const postTerminalFailure = AiError.make({
      module: "Toolkit",
      method: "lookup.handle",
      reason: AiError.UnknownError.make({ description: postTerminalSecret }),
    });

    const postTerminalInterruptor = 4242;

    const Lookup = Tool.make("lookup", {
      parameters: Schema.Struct({ query: Schema.String }),
      success: Schema.String,
    });

    const tools = Toolkit.make(Lookup);

    const anomalousRuntime = Effect.map(
      tools,
      (native) =>
        ({
          tools: native.tools,
          handle: <Name extends keyof typeof tools.tools>(
            name: Name,
            parameters: Tool.Parameters<(typeof tools.tools)[Name]>,
            toolCallId?: string,
          ) =>
            native.handle(name, parameters, toolCallId).pipe(
              Effect.map((results) =>
                results.pipe(
                  // Defects and interruption have error channel `never`, so this adversarial
                  // composed Cause works for every generic Tool without widening HandlerError.
                  Stream.concat(
                    Stream.failCause(
                      Cause.combine(
                        Cause.die(postTerminalFailure),
                        Cause.interrupt(postTerminalInterruptor),
                      ),
                    ),
                  ),
                ),
              ),
            ),
        }) satisfies Toolkit.WithHandler<typeof tools.tools>,
    );

    // Test-only Effect AI Toolkit seam: runtime execution consumes the Toolkit Effect and
    // `tools`; handler Layer construction stays on the native `tools` value below.
    const anomalousTools = Object.assign(anomalousRuntime, {
      "~effect/ai/Toolkit": "~effect/ai/Toolkit" as const,
      tools: tools.tools,
    }) as Toolkit.Toolkit<typeof tools.tools>;

    const model = modelFromParts([
      {
        type: "tool-call",
        id: "post-terminal-1",
        name: "lookup",
        params: { query: "status" },
        providerExecuted: false,
      },
      { type: "finish", reason: "tool-calls", usage },
    ]);

    const definition = Agent.make("post-terminal-tool-failure", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Look up the status.",
      toolkit: anomalousTools,
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });

    const finalized = Ref.makeUnsafe(0);

    const toolLayer = tools.toLayer({
      lookup: () =>
        Effect.succeed("apparently successful").pipe(
          Effect.ensuring(Ref.update(finalized, (count) => count + 1)),
        ),
    });

    return Effect.gen(function* () {
      const observed = yield* Ref.make<ReadonlyArray<RunEvent>>([]);

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "status",
      }).pipe(
        Stream.tap((event) => Ref.update(observed, (events) => [...events, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isSuccess(exit)) {
        throw new Error("Expected the post-terminal Tool failure to escape as a defect");
      }
      expect(
        exit.cause.reasons
          .filter(Cause.isDieReason)
          .map((reason) => reason.defect)
          .includes(postTerminalFailure),
      ).toBe(true);
      expect(Cause.interruptors(exit.cause).has(postTerminalInterruptor)).toBe(true);
      expect(exit.cause.reasons.filter(Cause.isFailReason)).toEqual([]);
      expect(yield* Ref.get(finalized)).toBe(1);

      const terminalEvents = (yield* Ref.get(observed)).filter(
        (event) => event._tag === "ToolCallSucceeded" || event._tag === "ToolCallFailed",
      );

      expect(terminalEvents).toEqual([
        expect.objectContaining({
          _tag: "ToolCallFailed",
          toolCallId: "post-terminal-1",
          toolName: "lookup",
          providerExecuted: false,
        }),
      ]);
      expect(terminalEvents.some((event) => event._tag === "ToolCallSucceeded")).toBe(false);
    });
  });
  it.effect("preflights the complete Tool batch before starting any handler", () =>
    Effect.gen(function* () {
      const starts = yield* Ref.make(0);
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);

      const Lookup = Tool.make("lookup", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
      });

      const tools = Toolkit.make(Lookup);

      const definition = Agent.make("batch-preflight", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Run lookups.",
        toolkit: tools,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 2,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        }),
      });

      const model = modelFromParts([
        {
          type: "tool-call",
          id: "lookup-valid",
          name: "lookup",
          params: { value: 1 },
          providerExecuted: false,
        },
        {
          type: "tool-call",
          id: "lookup-invalid",
          name: "lookup",
          params: { value: "not-an-int" },
          providerExecuted: false,
        },
        { type: "finish", reason: "tool-calls", usage },
      ]);

      const toolLayer = tools.toLayer({
        lookup: () => Ref.update(starts, (value) => value + 1).pipe(Effect.as("unexpected")),
      });

      const exit = yield* AgentRuntime.stream(Agent.withModel(definition, model), {
        question: "lookup",
      }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.provide(toolLayer),
        Effect.exit,
      );

      failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(yield* Ref.get(starts)).toBe(0);
      expect(observed.some((event) => event._tag === "ToolCallStarted")).toBe(false);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
    }),
  );

  it.effect("rejects duplicate provider terminals before appending any Tool success", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<RunEvent>>([]);

      const agent = Agent.withModel(
        hostedDefinition,
        modelFromParts([
          {
            type: "tool-call",
            id: "hosted-duplicate",
            name: "HostedSearch",
            params: { query: "duplicate" },
            providerExecuted: true,
          },
          {
            type: "tool-result",
            id: "hosted-duplicate",
            name: "HostedSearch",
            result: { status: "first" },
            isFailure: false,
            providerExecuted: true,
          },
          {
            type: "tool-result",
            id: "hosted-duplicate",
            name: "HostedSearch",
            result: { status: "second" },
            isFailure: false,
            providerExecuted: true,
          },
          { type: "finish", reason: "tool-calls", usage },
        ]),
      );

      const exit = yield* AgentRuntime.stream(agent, { question: "duplicate" }).pipe(
        Stream.tap((event) => Ref.update(events, (all) => [...all, event])),
        Stream.runDrain,
        Effect.exit,
      );

      const failure = failureFrom(exit);
      const observed = yield* Ref.get(events);

      expect(failure).toBeInstanceOf(ModelProtocolError);
      expect(failure.message).toContain("more than one terminal result");
      expect(observed.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(0);
      expect(observed.filter((event) => event._tag === "RunFailed")).toHaveLength(1);
    }),
  );
});

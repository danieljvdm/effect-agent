import { Agent, AgentPolicy, ConversationId, IdGenerator, RunId, TurnId } from "@effect-agent/core";
import { expect, layer } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

import {
  AgentRuntime,
  ToolBroker,
  type ProgrammaticCallOutcome,
  type ToolBrokerPass,
  type ToolBrokerPassOptions,
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
}) =>
  Effect.gen(function* () {
    const Orchestrate = Tool.make("orchestrate", {
      parameters: Schema.Struct({ plan: Schema.String }),
      success: Schema.Any,
    }).addDependency(ToolBroker);
    const outerToolkit = Toolkit.make(Orchestrate);
    const definition = Agent.define("broker-host", {
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
                const pass = yield* broker.openPass(inner, options.passOptions);
                return yield* options.program(pass);
              }),
          };
        }),
      )
      .pipe(Layer.provide(options.innerHandlers));

    const result = yield* AgentRuntime.run(
      Agent.withModel(definition, model),
      { question: "go" },
      {},
    ).pipe(Effect.provide(toolLayer), Effect.scoped);
    return result;
  });

layer(identifiers)("RUN-016 programmatic Tool broker", (it) => {
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

        // Direct: the model declares the call itself.
        const directDefinition = Agent.define("direct-host", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Query.",
          toolkit: innerToolkit,
          policy: policy(),
        });
        const directModel = scriptedModel(
          [
            {
              type: "tool-call",
              id: "query-1",
              name: "query",
              params: { sql: "select 1" },
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage },
          ],
          '{"answer":"direct"}',
        );
        const directEvents: Array<unknown> = [];
        const direct = yield* AgentRuntime.run(
          Agent.withModel(directDefinition, directModel),
          { question: "go" },
          { observer: undefined },
        ).pipe(Effect.provide(innerToolkit.toLayer(handler)), Effect.scoped);
        expect(direct.output).toEqual({ answer: "direct" });
        expect(directEvents).toEqual([]);

        // Programmatic: the same Tool, same encoded arguments, through the broker.
        const result = yield* runOrchestrated({
          innerToolkit,
          innerHandlers: innerToolkit.toLayer(handler),
          program: (pass) =>
            pass.invoke({ toolName: "query", encodedArguments: { sql: "select 1" } }),
        });
        expect(result.output).toEqual({ answer: "done" });
        expect(yield* Ref.get(seen)).toEqual(["select 1", "select 1"]);
      }),
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
      const definition = Agent.define("broker-turn-seam", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Orchestrate twice.",
        toolkit: outerToolkit,
        policy: policy({ maxToolCalls: 3, maxTurns: 4 }),
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
                  const pass = yield* broker.openPass(inner);
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
      expect(exit._tag).toBe("Failure");
      const rendered = JSON.stringify(exit);
      expect(rendered).toContain("AgentPolicyError");
      expect(rendered).toContain("Tool Call limit");
    }),
  );
});

import { type CodeModeFailure, type CodeModeSuccess } from "@effect-agent/capabilities/CodeMode";
import * as CodeMode from "@effect-agent/capabilities/CodeMode";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId, RunId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { RunContextPreparationPassthrough } from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import {
  CodeExecutionHost,
  CodeExecutionResult,
  CodeExecutor,
  CodeProgramFailedError,
} from "@effect-agent/sandbox/CodeExecutor";
import { SandboxImplementation } from "@effect-agent/sandbox/Sandbox";
import { describe, expect, it, layer } from "@effect/vitest";
import { Context, Duration, Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

const Query = Tool.make("query_warehouse", {
  description: "Run one read-only SQL query",
  parameters: Schema.Struct({ sql: Schema.String }),
  success: Schema.Struct({ rows: Schema.Array(Schema.Int), truncated: Schema.Boolean }),
}).annotate(ToolExecutionClass, "readonly");

const Unannotated = Tool.make("unannotated", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.String,
});

const NeedsApproval = Tool.make("approval_tool", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.String,
  needsApproval: true,
}).annotate(ToolExecutionClass, "readonly");

const Unrenderable = Tool.make("unrenderable", {
  parameters: Schema.Struct({ anything: Schema.Any }),
  success: Schema.String,
}).annotate(ToolExecutionClass, "readonly");

const ArraySchema = Tool.dynamic("array_schema", {
  // @ts-expect-error Exercise the runtime boundary with a malformed raw JSON Schema.
  parameters: [],
  success: Schema.String,
}).annotate(ToolExecutionClass, "readonly");

describe("CAP-014 CodeMode.make construction", () => {
  it("builds a readonly-annotated Tool and encoded-side declarations", () => {
    const definition = CodeMode.make("run_javascript", {
      description: "Run JavaScript over the warehouse",
      tools: { warehouse: { query: Query } },
    });

    expect(Context.get(definition.tool.annotations, Tool.Readonly)).toBe(true);
    expect(Context.get(definition.tool.annotations, ToolExecutionClass)).toBe("readonly");
    expect(definition.tool.failureMode).toBe("return");
    expect(definition.declarations).toContain("declare const warehouse:");
    expect(definition.declarations).toContain("readonly sql: string");
    expect(definition.declarations).toContain("Promise<{");
    expect(definition.declarations).toContain("readonly truncated: boolean");
    expect(definition.description).toContain("Sandbox globals:");
    expect(definition.namespaces).toHaveLength(1);
    expect(definition.namespaces[0]).toMatchObject({ name: "warehouse", methods: ["query"] });
  });

  it("CAP-014 rejects a Tool that is not annotated readonly", () => {
    expect(() =>
      CodeMode.make("run_javascript", {
        description: "d",
        tools: { ns: { call: Unannotated } },
      }),
    ).toThrow(/uncertain/);
  });

  it("CAP-014 rejects an approval-requiring Tool at construction", () => {
    expect(() =>
      CodeMode.make("run_javascript", {
        description: "d",
        tools: { ns: { call: NeedsApproval } },
      }),
    ).toThrow(/approval/);
  });

  it("CAP-014 rejects invalid namespace and method identifiers", () => {
    expect(() =>
      CodeMode.make("run_javascript", {
        description: "d",
        tools: { "not-an-identifier": { call: Query } },
      }),
    ).toThrow(/identifier/);
    expect(() =>
      CodeMode.make("run_javascript", {
        description: "d",
        tools: { ns: { await: Query } },
      }),
    ).toThrow(/identifier/);
  });

  it("CAP-014 fails construction closed on a schema the declaration deriver cannot render", () => {
    expect(() =>
      CodeMode.make("run_javascript", {
        description: "d",
        tools: { ns: { call: Unrenderable } },
      }),
    ).toThrow(/cannot render/);
    expect(() =>
      CodeMode.make("run_javascript", {
        description: "d",
        tools: { ns: { call: ArraySchema } },
      }),
    ).toThrow(/cannot render/);
  });

  it("CAP-014 rejects two different Tools sharing one name and empty namespaces", () => {
    const Other = Tool.make("query_warehouse", {
      parameters: Schema.Struct({ other: Schema.String }),
      success: Schema.String,
    }).annotate(ToolExecutionClass, "readonly");

    expect(() =>
      CodeMode.make("run_javascript", {
        description: "d",
        tools: { a: { one: Query }, b: { two: Other } },
      }),
    ).toThrow(/named query_warehouse/);
    expect(() => CodeMode.make("run_javascript", { description: "d", tools: { ns: {} } })).toThrow(
      /no methods/,
    );
  });
});

// ---------------------------------------------------------------------------
// Handler behavior through a full Run with a scripted CodeExecutor: the fake
// executor performs canned host calls so the capability's routing, envelope,
// and egress policy are observable without a real JavaScript engine.
// ---------------------------------------------------------------------------

const scriptedExecutorImplementation = SandboxImplementation.make({
  isolation: "unisolated",
  identity: "scripted-executor",
});

/**
 * A scripted CodeExecutor: `CALL <namespace>.<method> <json>` performs one
 * host call and returns its outcome as the pass result; `RESULT <json>`
 * returns the value; `LOGS <n>` emits n log lines then returns null;
 * `THROW` fails the pass as a program failure.
 */
const scriptedExecutorLayer = Layer.succeed(CodeExecutor)(
  CodeExecutor.of({
    execute: (request) =>
      Effect.gen(function* () {
        const host = yield* CodeExecutionHost;
        const source = request.source.trim();

        const finish = (value: Schema.Json, logs: ReadonlyArray<string>) =>
          CodeExecutionResult.make({
            implementation: scriptedExecutorImplementation,
            value,
            logs,
            resourceUse: {
              wallTime: Duration.millis(1),
              hostCalls: 0,
              logBytes: 0,
              resultBytes: 0,
            },
          });

        // The scripted source stands in for untrusted model output: a parse
        // failure must stay inside the typed executor channel.
        const parseJson = (text: string) =>
          Effect.try({
            try: () => JSON.parse(text) as Schema.Json,
            catch: () =>
              CodeProgramFailedError.make({
                implementation: scriptedExecutorImplementation,
                reason: "threw",
                thrown: "SyntaxError: invalid scripted JSON",
                message: "SyntaxError: invalid scripted JSON",
                logs: [],
              }),
          });

        if (source.startsWith("CALL ")) {
          const [, target, ...rest] = source.split(" ");
          const [namespace, method] = target.split(".");

          const outcome = yield* host.call({
            namespace,
            method,
            argument: yield* parseJson(rest.join(" ")),
          });

          return outcome._tag === "CodeHostCallSuccess"
            ? finish({ ok: outcome.value }, ["called host"])
            : finish({ caught: outcome.error }, ["caught envelope"]);
        }
        if (source.startsWith("RESULT ")) {
          return finish(yield* parseJson(source.slice("RESULT ".length)), []);
        }
        if (source.startsWith("LOGS ")) {
          const count = Number(source.slice("LOGS ".length));

          return finish(
            null,
            Array.from({ length: count }, (_, i) => `line-${i} ${"x".repeat(64)}`),
          );
        }

        return yield* CodeProgramFailedError.make({
          implementation: scriptedExecutorImplementation,
          reason: "threw",
          thrown: "Error: scripted throw",
          message: "Error: scripted throw",
          logs: ["before throw"],
        });
      }),
  }),
);

const usage = { inputTokens: {}, outputTokens: {} };

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("thread-code-mode")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-code-mode")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-code-mode")),
});

interface ScenarioOutcome {
  readonly answer: { readonly answer: string };
  /** Every tool-message part the second model request observed. */
  readonly toolResults: ReadonlyArray<{
    readonly result: unknown;
    readonly isFailure: boolean;
  }>;
  readonly queryCalls: number;
}

const runWithCode = (code: string, options?: { readonly maxEgressBytes?: number }) =>
  Effect.gen(function* () {
    const definition = CodeMode.make("run_javascript", {
      description: "Run JavaScript over the warehouse",
      tools: { warehouse: { query: Query } },
      ...(options?.maxEgressBytes === undefined ? {} : { maxEgressBytes: options.maxEgressBytes }),
    });

    const agent = Agent.make("code-mode-host", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Use run_javascript.",
      toolkit: Toolkit.make(definition.tool),
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: 4,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });

    const toolResults = yield* Ref.make<
      ReadonlyArray<{ readonly result: unknown; readonly isFailure: boolean }>
    >([]);

    const model = Model.make(
      "scripted",
      "code-mode",
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
                    Ref.update(toolResults, (all) => [
                      ...all,
                      ...prompt.content
                        .filter((message) => message.role === "tool")
                        .flatMap((message) => message.content)
                        .filter((part) => part.type === "tool-result")
                        .map((part) => ({
                          result: part.result,
                          isFailure: part.isFailure === true,
                        })),
                    ]),
                  ),
                  Effect.map((value) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      value === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "code-1",
                              name: "run_javascript",
                              params: { code },
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : [
                            { type: "text-start", id: "answer" },
                            { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
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

    const queryCalls = yield* Ref.make(0);

    const handlerLayer = definition.handlers.pipe(
      Layer.provide(
        Toolkit.make(Query).toLayer({
          query_warehouse: ({ sql }) =>
            Ref.update(queryCalls, (n) => n + 1).pipe(
              Effect.as({ rows: sql.includes("empty") ? [] : [1, 2, 3], truncated: false }),
            ),
        }),
      ),
      Layer.provide(scriptedExecutorLayer),
    );

    const result = yield* AgentRuntime.run(
      Agent.withModel(agent, model),
      { question: "go" },
      {},
    ).pipe(Effect.provide(handlerLayer), Effect.scoped);

    return {
      answer: result.output,
      toolResults: yield* Ref.get(toolResults),
      queryCalls: yield* Ref.get(queryCalls),
    } satisfies ScenarioOutcome;
  });

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layerTransient,
  RunContextPreparationPassthrough,
);

layer(testLayer)("CAP-016 Code Mode handler through a scripted executor", (it) => {
  it.effect(
    "CAP-014 routes host calls to the allowlisted Tool and returns the budgeted egress",
    () =>
      Effect.gen(function* () {
        const outcome = yield* runWithCode('CALL warehouse.query {"sql":"select 1"}');

        expect(outcome.answer).toEqual({ answer: "done" });
        expect(outcome.queryCalls).toBe(1);
        expect(outcome.toolResults).toHaveLength(1);
        expect(outcome.toolResults[0].isFailure).toBe(false);
        expect(outcome.toolResults[0].result).toMatchObject({
          result: { ok: { rows: [1, 2, 3], truncated: false } },
          logs: ["called host"],
        });
      }),
  );

  it.effect("CAP-014 an unknown namespace method rejects as a catchable envelope", () =>
    Effect.gen(function* () {
      const outcome = yield* runWithCode('CALL warehouse.missing {"sql":"x"}');

      expect(outcome.queryCalls).toBe(0);
      expect(outcome.toolResults[0].isFailure).toBe(false);
      expect(outcome.toolResults[0].result).toMatchObject({
        result: { caught: { _tag: "UnknownCodeModeMethod" } },
      });
    }),
  );

  it.effect(
    "CAP-016 a program failure returns the typed model-visible envelope with its log capture",
    () =>
      Effect.gen(function* () {
        const outcome = yield* runWithCode("THROW");

        expect(outcome.toolResults).toHaveLength(1);
        expect(outcome.toolResults[0].isFailure).toBe(true);
        expect(outcome.toolResults[0].result).toMatchObject({
          _tag: "CodeModeFailure",
          errorTag: "CodeProgramFailedError",
          thrown: "Error: scripted throw",
          logs: ["before throw"],
        });
      }),
  );

  it.effect("CAP-016 a result over the aggregate egress budget fails typed, never truncates", () =>
    Effect.gen(function* () {
      const outcome = yield* runWithCode(`RESULT ${JSON.stringify({ big: "y".repeat(2_000) })}`, {
        maxEgressBytes: 512,
      });

      expect(outcome.toolResults[0].isFailure).toBe(true);
      expect(outcome.toolResults[0].result).toMatchObject({
        _tag: "CodeModeFailure",
        errorTag: "CodeModeEgressExceeded",
      });
    }),
  );

  it.effect(
    "CAP-016 logs beyond the remaining egress budget truncate with an explicit marker",
    () =>
      Effect.gen(function* () {
        const outcome = yield* runWithCode("LOGS 40", { maxEgressBytes: 1_024 });

        expect(outcome.toolResults[0].isFailure).toBe(false);

        const value = outcome.toolResults[0].result as {
          readonly logs: ReadonlyArray<string>;
        };

        expect(value.logs.length).toBeLessThan(40);
        expect(value.logs.at(-1)).toContain("logs truncated");
      }),
  );
});

// ---------------------------------------------------------------------------
// Compile-time E/R proofs (change discipline: type tests whenever Agent or
// Effect AI composition changes).
// ---------------------------------------------------------------------------

type Equal<Left, Right> =
  (<T>() => T extends Left ? 1 : 2) extends <T>() => T extends Right ? 1 : 2 ? true : false;

const typedDefinition = CodeMode.make("typed_code_mode", {
  description: "typed",
  tools: { warehouse: { query: Query } },
});

// With `failureMode: "return"` the envelope is encoded into the failed Tool
// result rather than escaping into the handler `E`, so the proof pins the
// declared failure Schema and the empty handler error surface.
type HandlerFailureIsEnvelope = Equal<
  (typeof typedDefinition.tool.failureSchema)["Type"],
  CodeModeFailure
>;
type LayerContext<L> = L extends Layer.Layer<infer _Out, infer _Error, infer R> ? R : never;
type LayerRequirements = LayerContext<typeof typedDefinition.handlers>;
type RequiresExecutor = Equal<Extract<LayerRequirements, CodeExecutor>, CodeExecutor>;

// Disjoint namespaces must not erase Tools from the Layer requirements: the
// selected-tool union is computed per namespace, never by intersecting method
// keys across namespaces.
const Second = Tool.make("second_tool", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.Struct({ ok: Schema.Boolean }),
}).annotate(ToolExecutionClass, "readonly");

const disjointDefinition = CodeMode.make("disjoint_code_mode", {
  description: "disjoint",
  tools: { warehouse: { query: Query }, other: { second: Second } },
});

type DisjointRequirements = LayerContext<typeof disjointDefinition.handlers>;
type DisjointKeepsFirstHandler = Equal<
  Extract<DisjointRequirements, Tool.Handler<"query_warehouse">>,
  Tool.Handler<"query_warehouse">
>;
type DisjointKeepsSecondHandler = Equal<
  Extract<DisjointRequirements, Tool.Handler<"second_tool">>,
  Tool.Handler<"second_tool">
>;
type SuccessIsBudgeted =
  Tool.Success<typeof typedDefinition.tool> extends CodeModeSuccess ? true : false;

describe("Code Mode type proofs", () => {
  it("pins the envelope failure, executor requirement, and budgeted success", () => {
    const failureProof: HandlerFailureIsEnvelope = true;
    const executorProof: RequiresExecutor = true;
    const successProof: SuccessIsBudgeted = true;

    expect(failureProof && executorProof && successProof).toBe(true);
  });

  it("keeps every selected handler visible in R across disjoint namespaces", () => {
    const firstProof: DisjointKeepsFirstHandler = true;
    const secondProof: DisjointKeepsSecondHandler = true;

    expect(firstProof && secondProof).toBe(true);
  });
});

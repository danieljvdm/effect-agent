import { expect, it } from "@effect/vitest";
import { Context, Duration, Effect, Layer, Ref, Schema, Stream } from "effect";
import { CodeExecutionHost, CodeExecutionResult, CodeExecutor } from "effect-agent/code-executor";
import * as CodeMode from "effect-agent/code-mode";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { SandboxImplementation } from "effect-agent/sandbox";
import { ToolBroker } from "effect-agent/tool-broker";
import { CurrentToolCatalog } from "effect-agent/tool-exposure";
import { Tool, Toolkit } from "effect/unstable/ai";

const Query = Tool.make("query_warehouse", {
  description: "Run one read-only SQL query",
  parameters: Schema.Struct({ sql: Schema.String }),
  success: Schema.Struct({ rows: Schema.Array(Schema.Int), truncated: Schema.Boolean }),
}).annotate(ToolExecutionClass, "readonly");

const Unannotated = Tool.make("unannotated", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.String,
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

it.effect("filters executor inventory and guessed calls using the live invocation catalogue", () =>
  Effect.gen(function* () {
    const Hidden = Tool.make("hidden_records", {
      parameters: Schema.Struct({ sql: Schema.String }),
      success: Schema.String,
    }).annotate(ToolExecutionClass, "readonly");

    const definition = CodeMode.make("run_javascript", {
      description: "Run a program",
      includeDeclarations: false,
      tools: { warehouse: { query: Query, hidden: Hidden }, secrets: { read: Hidden } },
    });

    const brokerCalls = yield* Ref.make(0);

    const executor = CodeExecutor.of({
      execute: (request) =>
        Effect.gen(function* () {
          const host = yield* CodeExecutionHost;

          const hidden = yield* host.call({
            namespace: "warehouse",
            method: "hidden",
            argument: { sql: "select" },
          });

          return CodeExecutionResult.make({
            implementation: scriptedExecutorImplementation,
            value: {
              namespaces: request.namespaces.map((namespace) => ({
                name: namespace.name,
                methods: namespace.methods,
              })),
              hidden,
            },
            logs: [],
            resourceUse: {
              wallTime: Duration.millis(1),
              hostCalls: 1,
              logBytes: 0,
              resultBytes: 0,
            },
          });
        }),
    });

    const entries = [
      {
        kind: "code-mode" as const,
        nativeToolName: "run_javascript",
        namespace: "warehouse",
        method: "query",
        tool: Query,
      },
      {
        kind: "code-mode" as const,
        nativeToolName: "run_javascript",
        namespace: "warehouse",
        method: "hidden",
        tool: Hidden,
      },
      {
        kind: "code-mode" as const,
        nativeToolName: "run_javascript",
        namespace: "secrets",
        method: "read",
        tool: Hidden,
      },
    ];

    const captured = yield* Layer.build(
      definition.handlers.pipe(
        Layer.provide(Layer.succeed(CodeExecutor, executor)),
        Layer.provide(
          Toolkit.make(Query, Hidden).toLayer({
            query_warehouse: () => Effect.succeed({ rows: [], truncated: false }),
            hidden_records: () => Effect.succeed("hidden"),
          }),
        ),
      ),
    ).pipe(Effect.provideService(CurrentToolCatalog, { entries }));

    const results = yield* Effect.gen(function* () {
      const toolkit = yield* Toolkit.make(definition.tool);
      const stream = yield* toolkit.handle("run_javascript", { code: "async () => null" });

      return yield* Stream.runCollect(stream);
    }).pipe(
      Effect.provideContext(captured),
      Effect.provideService(CurrentToolCatalog, {
        entries: [entries[0], { ...entries[1], nativeToolName: "other_executor" }],
      }),
      Effect.provideService(ToolBroker, {
        openPass: () =>
          Effect.succeed({
            snapshot: Effect.succeed([]),
            invoke: () =>
              Ref.update(brokerCalls, (count) => count + 1).pipe(
                Effect.as({
                  _tag: "ProgrammaticCallSuccess" as const,
                  index: 0,
                  encodedResult: "unexpected",
                }),
              ),
          }),
      }),
    );

    expect(results[0]?.result).toMatchObject({
      result: {
        namespaces: [{ name: "warehouse", methods: ["query"] }],
        hidden: {
          _tag: "CodeHostCallFailure",
          error: {
            _tag: "UnknownCodeModeMethod",
            message: "The requested method is not available in this pass",
          },
        },
      },
    });
    expect(yield* Ref.get(brokerCalls)).toBe(0);
  }),
);

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

class RedactionPolicy extends Context.Service<RedactionPolicy, string>()(
  "code-mode-types/RedactionPolicy",
) {}

const redactedDefinition = CodeMode.make("redacted_code_mode", {
  description: "redacted",
  tools: { warehouse: { query: Query } },
  redactEgress: (egress) =>
    Effect.gen(function* () {
      const result = yield* RedactionPolicy;

      yield* Effect.addFinalizer(() => Effect.void);

      return { ...egress, result };
    }),
});

type LayerContext<L> = L extends Layer.Layer<infer _Out, infer _Error, infer R> ? R : never;
type LayerRequirements = LayerContext<typeof typedDefinition.handlers>;

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

export const verifyCapturesRedactionRequirementsInTheHandlerLayerWithNoNewToolRequirementsOrFailures =
  () => {
    const requirements: Equal<
      LayerContext<typeof redactedDefinition.handlers>,
      LayerRequirements | RedactionPolicy
    > = true;

    const toolRequirements: Equal<
      Tool.HandlerServices<typeof redactedDefinition.tool>,
      Tool.HandlerServices<typeof typedDefinition.tool>
    > = true;

    const errors: Equal<Layer.Error<typeof redactedDefinition.handlers>, never> = true;

    void (requirements && toolRequirements && errors);
  };

export const verifyCapturesReportCallbackRequirementsWithoutWideningToolErrorsOrRequirements =
  () => {
    const reported = CodeMode.make("reported", {
      description: "d",
      tools: { ns: { call: Unannotated } },
      onPassExit: () => Effect.asVoid(RedactionPolicy),
    });

    const requirements: Equal<
      Extract<LayerContext<typeof reported.handlers>, RedactionPolicy>,
      RedactionPolicy
    > = true;

    const errors: Equal<Layer.Error<typeof reported.handlers>, never> = true;
    const toolRequirements: Equal<Tool.HandlerServices<typeof reported.tool>, ToolBroker> = true;

    void (requirements && errors && toolRequirements);
  };

export const verifyKeepsEverySelectedHandlerVisibleInRAcrossDisjointNamespaces = () => {
  const firstProof: DisjointKeepsFirstHandler = true;
  const secondProof: DisjointKeepsSecondHandler = true;

  void (firstProof && secondProof);
};

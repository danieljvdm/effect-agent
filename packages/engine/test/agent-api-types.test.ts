import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { AgentRuntime } from "@effect-agent/engine";
import {
  type AgentResult,
  type AgentRuntimeFailure,
  type AgentRuntimeRequirements,
  type AgentCompletionProjectionRequirements,
} from "@effect-agent/engine/AgentRuntime";
import { type ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { Context, Effect, Layer, Schema, SchemaGetter, type Scope, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";
import { expectTypeOf, it } from "vite-plus/test";

class Instructions extends Context.Service<Instructions, string>()("api-types/Instructions") {}
class ProviderClient extends Context.Service<ProviderClient, string>()(
  "api-types/ProviderClient",
) {}
class Decoder extends Context.Service<Decoder, string>()("api-types/Decoder") {}
class Encoder extends Context.Service<Encoder, string>()("api-types/Encoder") {}
class Catalog extends Context.Service<Catalog, string>()("api-types/Catalog") {}
class InstructionError extends Schema.TaggedError<InstructionError>()("InstructionError", {}) {}
class ToolError extends Schema.TaggedError<ToolError>()("ToolError", {}) {}

const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.String,
  failure: ToolError,
  failureMode: "error",
  dependencies: [Catalog],
});

const toolkit = Toolkit.make(Lookup);

const Input = Schema.Struct({
  city: Schema.String,
  days: Schema.NumberFromString.pipe(
    Schema.decode({
      decode: SchemaGetter.transformOrFail((days) => Effect.as(Decoder, days)),
      encode: SchemaGetter.transformOrFail((days) => Effect.as(Encoder, days)),
    }),
  ),
});

const planner = Agent.make("typed-planner", {
  input: Input,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ city, days }) =>
    Effect.gen(function* () {
      expectTypeOf(days).toEqualTypeOf<number>();
      const prefix = yield* Instructions;

      if (prefix === "") return yield* new InstructionError({});

      return `${prefix} ${city} ${days}`;
    }),
  toolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const model = Model.make(
  "test",
  "typed-model",
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      yield* ProviderClient;

      return yield* LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.empty,
      });
    }),
  ),
);

it("preserves encoded input, output, failures and every unsatisfied service", () => {
  const input = { city: "Lisbon", days: "2" };
  const run = AgentRuntime.run(planner, input);
  const provided = run.pipe(Effect.provide(model));

  type DefinitionServices =
    | ThreadHistory
    | Instructions
    | Decoder
    | Encoder
    | Catalog
    | Tool.HandlersFor<typeof toolkit.tools>;
  type NativeServices = LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName;
  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
    DefinitionServices | NativeServices | IdGenerator
  >();
  expectTypeOf<Effect.Services<typeof provided>>().toEqualTypeOf<
    DefinitionServices | ProviderClient | IdGenerator
  >();
  expectTypeOf<Effect.Success<typeof run>>().toEqualTypeOf<
    AgentResult<{ readonly answer: string }>
  >();
  expectTypeOf<Extract<Effect.Error<typeof run>, InstructionError | ToolError>>().toEqualTypeOf<
    InstructionError | ToolError
  >();
  expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<AgentRuntimeFailure<typeof planner>>();

  const stream = AgentRuntime.stream(planner, input);

  expectTypeOf<Stream.Services<typeof stream>>().toEqualTypeOf<
    DefinitionServices | NativeServices | IdGenerator
  >();
  const start = AgentRuntime.start(planner, input);

  expectTypeOf<Effect.Services<typeof start>>().toEqualTypeOf<
    Effect.Services<typeof run> | Scope.Scope
  >();
  expectTypeOf<Effect.Error<typeof start>>().toEqualTypeOf<never>();

  const captured = Effect.gen(function* () {
    const modelLayer = yield* model.captureRequirements;

    return yield* AgentRuntime.run(planner, input).pipe(Effect.provide(modelLayer));
  });

  expectTypeOf<Effect.Services<typeof captured>>().toEqualTypeOf<
    Effect.Services<typeof provided>
  >();

  const composed = AgentRuntime.run(planner, input).pipe(
    Effect.provide(Layer.mergeAll(model, IdGenerator.layer)),
  );

  expectTypeOf<Effect.Services<typeof composed>>().toEqualTypeOf<
    DefinitionServices | ProviderClient
  >();

  // These Effects are never executed. Their signatures must reject incomplete composition.
  expectTypeOf<typeof run>().not.toMatchTypeOf<Effect.Effect<unknown, unknown>>();
  expectTypeOf<typeof provided>().not.toMatchTypeOf<Effect.Effect<unknown, unknown>>();

  const cannotRun = () => {
    // @ts-expect-error All required input fields must be present.
    const missing = AgentRuntime.run(planner, { city: "Lisbon" });
    // @ts-expect-error The primary operation accepts Encoded, not decoded Type.
    const decoded = AgentRuntime.run(planner, { city: "Lisbon", days: 2 });
    // @ts-expect-error Malformed inputs are rejected by stream too.
    const malformed = AgentRuntime.stream(planner, { city: 42, days: "2" });
    // @ts-expect-error Missing fields are rejected by start too.
    const empty = AgentRuntime.start(planner, {});
    const external: unknown = input;
    // @ts-expect-error Unknown data has an explicitly named boundary.
    const unknown = AgentRuntime.run(planner, external);
    const externalRun = AgentRuntime.runUnknown(planner, external);
    const externalStream = AgentRuntime.streamUnknown(planner, external);
    const externalStart = AgentRuntime.startUnknown(planner, external);

    const languageOnly = Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.empty,
      }),
    );

    // @ts-expect-error A binding needs both model identity services too.
    const missingIdentity = Agent.withModel(planner, languageOnly);

    return {
      missing,
      decoded,
      malformed,
      empty,
      unknown,
      externalRun,
      externalStream,
      externalStart,
      missingIdentity,
    };
  };

  expectTypeOf(cannotRun).toBeFunction();
});

it("preserves disjoint tool schemas and callbacks with different input types", () => {
  class ParametersDecoder extends Context.Service<ParametersDecoder, string>()(
    "union/ParametersDecoder",
  ) {}
  class ResultDecoder extends Context.Service<ResultDecoder, string>()("union/ResultDecoder") {}
  class TopicInstructions extends Context.Service<TopicInstructions, string>()(
    "union/TopicInstructions",
  ) {}
  class TopicFailure extends Schema.TaggedError<TopicFailure>()("TopicFailure", {}) {}

  const Complete = Tool.make("complete", {
    parameters: Schema.Struct({
      text: Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.transformOrFail((value) => Effect.as(ParametersDecoder, value)),
          encode: SchemaGetter.transform((value) => value),
        }),
      ),
    }),
    success: Schema.String.pipe(
      Schema.decode({
        decode: SchemaGetter.transformOrFail((value) => Effect.as(ResultDecoder, value)),
        encode: SchemaGetter.transform((value) => value),
      }),
    ),
    failure: TopicFailure,
    failureMode: "error",
  });

  const topicTools = Toolkit.make(Complete);

  const topic = Agent.make("topic-agent", {
    input: Schema.Struct({ topic: Schema.String }),
    output: Schema.String,
    instructions: ({ topic }) =>
      Effect.gen(function* () {
        const prefix = yield* TopicInstructions;

        if (prefix === "") return yield* TopicFailure.make({});

        return `${prefix}: ${topic}`;
      }),
    inputPrompt: ({ topic }) => Effect.map(TopicInstructions, (prefix) => `${prefix}: ${topic}`),
    toolkit: topicTools,
    policy: planner.policy,
    completion: { tool: "complete", project: ({ result }) => result },
  });

  type Selected = typeof planner | typeof topic;
  type ExpectedServices =
    | Instructions
    | Decoder
    | Encoder
    | Catalog
    | TopicInstructions
    | ParametersDecoder
    | ResultDecoder
    | Tool.HandlersFor<typeof toolkit.tools>
    | Tool.HandlersFor<typeof topicTools.tools>;
  expectTypeOf<Agent.ToolUnion<Selected>>().toEqualTypeOf<typeof Lookup | typeof Complete>();
  expectTypeOf<Agent.DefinitionRequirements<Selected>>().toEqualTypeOf<ExpectedServices>();
  expectTypeOf<AgentCompletionProjectionRequirements<Selected>>().toEqualTypeOf<
    ParametersDecoder | ResultDecoder
  >();
  expectTypeOf<
    Extract<Agent.Failure<Selected>, InstructionError | ToolError | TopicFailure>
  >().toEqualTypeOf<InstructionError | ToolError | TopicFailure>();
  const select = (useTopic: boolean) => (useTopic ? topic : planner);
  const binding = Agent.withModel(select(true), model);
  const execution = AgentRuntime.run(binding, { topic: "Lisbon" });

  expectTypeOf<Effect.Services<typeof execution>>().toEqualTypeOf<
    ExpectedServices | ProviderClient | ThreadHistory | IdGenerator
  >();
  expectTypeOf<
    Extract<Effect.Error<typeof execution>, InstructionError | ToolError | TopicFailure>
  >().toEqualTypeOf<InstructionError | ToolError | TopicFailure>();
  expectTypeOf<Effect.Success<typeof execution>["output"]>().toEqualTypeOf<
    string | { readonly answer: string }
  >();
});

it("retains every branch's tool requirements and failures across execution views", () => {
  const withoutTools = Agent.make("without-tools", {
    input: Input,
    output: planner.output,
    instructions: "Answer directly.",
    toolkit: Toolkit.empty,
    policy: planner.policy,
  });

  const select = (withTools: boolean) => (withTools ? planner : withoutTools);
  const selected = select(false);
  const input = { city: "Lisbon", days: "2" };

  type DefinitionServices =
    | Instructions
    | Decoder
    | Encoder
    | Catalog
    | Tool.HandlersFor<typeof toolkit.tools>;
  type RuntimeServices =
    | DefinitionServices
    | ThreadHistory
    | IdGenerator
    | LanguageModel.LanguageModel
    | Model.ProviderName
    | Model.ModelName;
  type ExpectedFailure = AgentRuntimeFailure<typeof planner>;

  expectTypeOf<Agent.DefinitionRequirements<typeof selected>>().toEqualTypeOf<DefinitionServices>();
  expectTypeOf<AgentRuntimeRequirements<typeof selected>>().toEqualTypeOf<RuntimeServices>();
  expectTypeOf<Agent.Failure<typeof selected>>().toEqualTypeOf<Agent.Failure<typeof planner>>();

  const run = AgentRuntime.run(selected, input);
  const runUnknown = AgentRuntime.runUnknown(selected, input);
  const stream = AgentRuntime.stream(selected, input);
  const streamUnknown = AgentRuntime.streamUnknown(selected, input);
  const start = AgentRuntime.start(selected, input);
  const startUnknown = AgentRuntime.startUnknown(selected, input);

  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<RuntimeServices>();
  expectTypeOf<Effect.Services<typeof runUnknown>>().toEqualTypeOf<RuntimeServices>();
  expectTypeOf<Stream.Services<typeof stream>>().toEqualTypeOf<RuntimeServices>();
  expectTypeOf<Stream.Services<typeof streamUnknown>>().toEqualTypeOf<RuntimeServices>();
  expectTypeOf<Effect.Services<typeof start>>().toEqualTypeOf<RuntimeServices | Scope.Scope>();
  expectTypeOf<Effect.Services<typeof startUnknown>>().toEqualTypeOf<
    RuntimeServices | Scope.Scope
  >();
  expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<ExpectedFailure>();
  expectTypeOf<Effect.Error<typeof runUnknown>>().toEqualTypeOf<ExpectedFailure>();
  expectTypeOf<Stream.Error<typeof stream>>().toEqualTypeOf<ExpectedFailure>();
  expectTypeOf<Stream.Error<typeof streamUnknown>>().toEqualTypeOf<ExpectedFailure>();
  expectTypeOf<
    Effect.Error<Effect.Success<typeof start>["await"]>
  >().toEqualTypeOf<ExpectedFailure>();
  expectTypeOf<
    Effect.Error<Effect.Success<typeof startUnknown>["await"]>
  >().toEqualTypeOf<ExpectedFailure>();

  const bindSelected = Agent.withModel(selected, model);

  const selectBinding = (withTools: boolean) =>
    withTools ? Agent.withModel(planner, model) : Agent.withModel(withoutTools, model);

  const binding = selectBinding(false);

  type BoundServices = DefinitionServices | ProviderClient;
  expectTypeOf<Agent.Requirements<typeof bindSelected>>().toEqualTypeOf<BoundServices>();
  expectTypeOf<Agent.Requirements<typeof binding>>().toEqualTypeOf<BoundServices>();
  expectTypeOf<Agent.Failure<typeof bindSelected>>().toEqualTypeOf<Agent.Failure<typeof planner>>();
  expectTypeOf<Agent.Failure<typeof binding>>().toEqualTypeOf<Agent.Failure<typeof planner>>();
  expectTypeOf<Agent.ToolUnion<typeof bindSelected>>().toEqualTypeOf<typeof Lookup>();
  expectTypeOf<Agent.ToolUnion<typeof binding>>().toEqualTypeOf<typeof Lookup>();
  const boundRun = AgentRuntime.run(binding, input);
  const selectedRun = AgentRuntime.run(bindSelected, input);

  expectTypeOf<Effect.Services<typeof boundRun>>().toEqualTypeOf<
    BoundServices | ThreadHistory | IdGenerator
  >();
  expectTypeOf<Effect.Services<typeof selectedRun>>().toEqualTypeOf<
    BoundServices | ThreadHistory | IdGenerator
  >();
});

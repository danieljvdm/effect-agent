import { Agent, AgentPolicy, IdGenerator } from "@effect-agent/core";
import { Context, Effect, Layer, Schema, SchemaGetter, type Scope, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";
import { expectTypeOf, it } from "vite-plus/test";

import {
  AgentRuntime,
  type AgentResult,
  type AgentRuntimeFailure,
  type ConversationHistory,
} from "../src/index.ts";

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
    | ConversationHistory
    | Instructions
    | Decoder
    | Encoder
    | Catalog
    | Tool.HandlersFor<typeof toolkit.tools>;
  type NativeServices = LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName;
  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
    DefinitionServices | NativeServices | IdGenerator | Scope.Scope
  >();
  expectTypeOf<Effect.Services<typeof provided>>().toEqualTypeOf<
    DefinitionServices | ProviderClient | IdGenerator | Scope.Scope
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
  expectTypeOf<Effect.Services<typeof start>>().toEqualTypeOf<Effect.Services<typeof run>>();
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
    DefinitionServices | ProviderClient | Scope.Scope
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

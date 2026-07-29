import { describe, expect, it } from "vite-plus/test";

import { Context, Effect, Layer, Schema, Stream } from "effect";
import { type AiError, LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";

import { Agent, AgentInputError, AgentOutputError, AgentPolicy } from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

class InstructionContext extends Context.Service<InstructionContext, { readonly locale: string }>()(
  "@effect-agent/core/test/InstructionContext",
) {}

class ModelConfig extends Context.Service<ModelConfig, { readonly modelName: string }>()(
  "@effect-agent/core/test/ModelConfig",
) {}

class AvailabilityCatalog extends Context.Service<
  AvailabilityCatalog,
  { readonly search: Effect.Effect<ReadonlyArray<string>> }
>()("@effect-agent/core/test/AvailabilityCatalog") {}

class InstructionFailure extends Schema.TaggedErrorClass<InstructionFailure>()(
  "InstructionFailure",
  { message: Schema.String },
) {}

class AvailabilityFailure extends Schema.TaggedErrorClass<AvailabilityFailure>()(
  "AvailabilityFailure",
  { message: Schema.String },
) {}

const SearchAvailability = Tool.make("search_availability", {
  parameters: Schema.Struct({ destination: Schema.String }),
  success: Schema.Array(Schema.String),
  failure: AvailabilityFailure,
  dependencies: [AvailabilityCatalog],
});
const TravelTools = Toolkit.make(SearchAvailability);

const model = Model.make(
  "scripted",
  "type-proof",
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      yield* ModelConfig;
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.empty,
      });
    }),
  ),
);

const definition = Agent.define("type-proof", {
  input: Schema.Struct({ destination: Schema.String }),
  output: Schema.Struct({ summary: Schema.String }),
  instructions: ({ destination }) =>
    Effect.gen(function* () {
      const context = yield* InstructionContext;
      if (context.locale.length === 0) {
        return yield* new InstructionFailure({
          message: "locale is required",
        });
      }
      return `Search ${destination} using ${context.locale}.`;
    }),
  toolkit: TravelTools,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});
const agent = Agent.withModel(definition, model);

type ExpectedRequirements =
  | InstructionContext
  | ModelConfig
  | AvailabilityCatalog
  | Tool.HandlersFor<Toolkit.Tools<typeof TravelTools>>;
type ExpectedDefinitionRequirements =
  | InstructionContext
  | AvailabilityCatalog
  | Tool.HandlersFor<Toolkit.Tools<typeof TravelTools>>;
type ExpectedFailure =
  | InstructionFailure
  | AvailabilityFailure
  | AiError.AiError
  | AgentInputError
  | AgentOutputError;

type RequirementsProof = Assert<Equal<Agent.Requirements<typeof agent>, ExpectedRequirements>>;
type DefinitionRequirementsProof = Assert<
  Equal<Agent.DefinitionRequirements<typeof definition>, ExpectedDefinitionRequirements>
>;
type FailureProof = Assert<Equal<Agent.Failure<typeof agent>, ExpectedFailure>>;
type DefinitionIsNotRunnableProof = Assert<
  Equal<typeof definition extends Agent.Any ? true : false, false>
>;

describe("Phase 0 Agent type inference", () => {
  it("separates immutable definition from model binding", () => {
    const requirementsProof: RequirementsProof = true;
    const definitionRequirementsProof: DefinitionRequirementsProof = true;
    const failureProof: FailureProof = true;
    const definitionIsNotRunnableProof: DefinitionIsNotRunnableProof = true;

    expect(requirementsProof).toBe(true);
    expect(definitionRequirementsProof).toBe(true);
    expect(failureProof).toBe(true);
    expect(definitionIsNotRunnableProof).toBe(true);
    expect(agent.definition).toBe(definition);
    expect(agent.model).toBe(model);
    expect(Object.isFrozen(definition)).toBe(true);
    expect(Object.isFrozen(agent)).toBe(true);
  });
});

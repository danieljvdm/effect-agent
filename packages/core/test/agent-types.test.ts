import { Context, Effect, Layer, Schema, Stream } from "effect";
import { type AiError, LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";
import { describe, expect, it } from "vite-plus/test";

import {
  Agent,
  type AgentError,
  type AgentInputError,
  type AgentOutputError,
  AgentPolicy,
  type CompactionPolicy,
  type ContextOverflowError,
} from "../src/index.ts";

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

class InstructionFailure extends Schema.TaggedError<InstructionFailure>()("InstructionFailure", {
  message: Schema.String,
}) {}

class AvailabilityFailure extends Schema.TaggedError<AvailabilityFailure>()("AvailabilityFailure", {
  message: Schema.String,
}) {}

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
        return yield* InstructionFailure.make({
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
type BindingRetainsNativeModelProof = Assert<Equal<(typeof agent)["model"], typeof model>>;
type InputProjectionProof = Assert<
  Equal<Agent.Input<typeof agent>, { readonly destination: string }>
>;
type OutputProjectionProof = Assert<
  Equal<Agent.Output<typeof agent>, { readonly summary: string }>
>;
type PolicyExhaustionModeProof = Assert<
  Equal<AgentPolicy["onExhaustion"], "final-answer" | "fail">
>;
type PolicyRunStatusProof = Assert<Equal<AgentPolicy["runStatus"], "appended" | "off">>;
type PolicyContextLimitOptionalityProof = Assert<
  Equal<AgentPolicy["contextTokenLimit"], AgentPolicy["tokenBudget"]>
>;
type PolicyCompactionProof = Assert<Equal<AgentPolicy["compaction"], CompactionPolicy>>;
type ContextOverflowTagProof = Assert<Equal<ContextOverflowError["_tag"], "ContextOverflowError">>;
// Union MEMBERSHIP, not just the tag: extracting the member by tag from the
// framework error union must yield exactly the class type (F5, PR #54 review).
type ContextOverflowInAgentErrorProof = Assert<
  Equal<Extract<AgentError, { _tag: "ContextOverflowError" }>, ContextOverflowError>
>;

describe("Agent type inference", () => {
  it("separates immutable definition from model binding", () => {
    const requirementsProof: RequirementsProof = true;
    const definitionRequirementsProof: DefinitionRequirementsProof = true;
    const failureProof: FailureProof = true;
    const definitionIsNotRunnableProof: DefinitionIsNotRunnableProof = true;
    const bindingRetainsNativeModelProof: BindingRetainsNativeModelProof = true;
    const inputProjectionProof: InputProjectionProof = true;
    const outputProjectionProof: OutputProjectionProof = true;
    const policyExhaustionModeProof: PolicyExhaustionModeProof = true;
    const policyRunStatusProof: PolicyRunStatusProof = true;
    const policyContextLimitOptionalityProof: PolicyContextLimitOptionalityProof = true;
    const policyCompactionProof: PolicyCompactionProof = true;
    const contextOverflowTagProof: ContextOverflowTagProof = true;
    const contextOverflowInAgentErrorProof: ContextOverflowInAgentErrorProof = true;

    expect(policyExhaustionModeProof).toBe(true);
    expect(policyRunStatusProof).toBe(true);
    expect(policyContextLimitOptionalityProof).toBe(true);
    expect(policyCompactionProof).toBe(true);
    expect(contextOverflowTagProof).toBe(true);
    expect(contextOverflowInAgentErrorProof).toBe(true);
    expect(requirementsProof).toBe(true);
    expect(definitionRequirementsProof).toBe(true);
    expect(failureProof).toBe(true);
    expect(definitionIsNotRunnableProof).toBe(true);
    expect(bindingRetainsNativeModelProof).toBe(true);
    expect(inputProjectionProof).toBe(true);
    expect(outputProjectionProof).toBe(true);
    expect(agent.definition).toBe(definition);
    expect(agent.model).toBe(model);
    expect(Object.isFrozen(definition)).toBe(true);
    expect(Object.isFrozen(agent)).toBe(true);
  });
});

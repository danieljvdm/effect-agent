import { Context, Effect, Layer, Schema, Stream } from "effect";
import { Agent } from "effect-agent";
import {
  type AgentInputError,
  type AgentOutputError,
  type AgentRunDispositionError,
} from "effect-agent/agent-error";
import { AgentPolicy } from "effect-agent/agent-policy";
import { type AiError, LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";

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

class InputPromptContext extends Context.Service<InputPromptContext, { readonly prefix: string }>()(
  "@effect-agent/core/test/InputPromptContext",
) {}

class InputPromptFailure extends Schema.TaggedError<InputPromptFailure>()("InputPromptFailure", {
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

const definition = Agent.make("type-proof", {
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

const inputPromptDefinition = Agent.make("input-prompt-type-proof", {
  input: Schema.Struct({ destination: Schema.String }),
  output: Schema.Struct({ summary: Schema.String }),
  instructions: "Answer as JSON.",
  inputPrompt: ({ destination }) =>
    destination === ""
      ? []
      : Effect.gen(function* () {
          const context = yield* InputPromptContext;

          if (context.prefix === "") {
            return yield* InputPromptFailure.make({ message: "prefix is required" });
          }

          return `${context.prefix}${destination}`;
        }),
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const RunDisposition = Schema.Literal("completed");

const dispositionDefinition = Agent.make("disposition-type-proof", {
  input: Schema.Struct({ destination: Schema.String }),
  output: Schema.Struct({
    summary: Schema.String,
    runDisposition: Schema.optionalKey(RunDisposition),
  }),
  instructions: "Answer with a typed disposition when the run completed the application work.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  runDisposition: {
    workerLifecycle: "assignment",
    schema: RunDisposition,
    fromOutput: (output) => output.runDisposition,
  },
});

const RecoverableSearch = Tool.make("search_availability", {
  parameters: Schema.Struct({ destination: Schema.NonEmptyString }),
  success: SearchAvailability.successSchema,
  failure: AvailabilityFailure,
  dependencies: [AvailabilityCatalog],
  failureMode: "return",
});

const recoverableDefinition = Agent.make("recoverable-type-proof", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Search.",
  toolkit: Toolkit.make(RecoverableSearch),
});

const recoverableAgent = Agent.withModel(recoverableDefinition, model);

export type RecoverableFailureProof = Assert<
  Equal<
    Agent.Failure<typeof recoverableAgent>,
    AiError.AiError | AgentInputError | AgentOutputError
  >
>;

export type RecoverableRequirementsProof = Assert<
  Equal<
    Agent.DefinitionRequirements<typeof recoverableDefinition>,
    AvailabilityCatalog | Tool.Handler<"search_availability">
  >
>;

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

export type RequirementsProof = Assert<
  Equal<Agent.Requirements<typeof agent>, ExpectedRequirements>
>;

export type DefinitionRequirementsProof = Assert<
  Equal<Agent.DefinitionRequirements<typeof definition>, ExpectedDefinitionRequirements>
>;

export type FailureProof = Assert<Equal<Agent.Failure<typeof agent>, ExpectedFailure>>;

export type InputPromptRequirementsProof = Assert<
  Equal<Agent.DefinitionRequirements<typeof inputPromptDefinition>, InputPromptContext>
>;

export type InputPromptFailureProof = Assert<
  Equal<
    Extract<Agent.Failure<typeof inputPromptDefinition>, InputPromptFailure>,
    InputPromptFailure
  >
>;

export type DefinitionIsNotBindingProof = Assert<
  Equal<typeof definition extends Agent.Any ? true : false, false>
>;

export type BindingRetainsNativeModelProof = Assert<Equal<(typeof agent)["model"], typeof model>>;

export type InputProjectionProof = Assert<
  Equal<Agent.Input<typeof agent>, { readonly destination: string }>
>;

export type OutputProjectionProof = Assert<
  Equal<Agent.Output<typeof agent>, { readonly summary: string }>
>;

export type RunDispositionProjectionProof = Assert<
  Equal<Agent.RunDisposition<typeof dispositionDefinition>, "completed">
>;

export type RunDispositionRequirementsProof = Assert<
  Equal<Agent.DefinitionRequirements<typeof dispositionDefinition>, never>
>;

export type PlainRunDispositionFailureProof = Assert<
  Equal<Agent.RunDispositionFailure<typeof definition>, never>
>;

export type DeclaredRunDispositionFailureProof = Assert<
  Equal<Agent.RunDispositionFailure<typeof dispositionDefinition>, AgentRunDispositionError>
>;

export type RunDispositionFailureProof = Assert<
  Equal<
    Extract<Agent.Failure<typeof dispositionDefinition>, { _tag: "AgentRunDispositionError" }>,
    AgentRunDispositionError
  >
>;

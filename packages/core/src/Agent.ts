import type { Effect, Layer, Schema } from "effect";
import { Schema as S } from "effect";
import type { AiError, LanguageModel, Model, Prompt, Tool, Toolkit } from "effect/unstable/ai";

import type { AgentInputError, AgentOutputError, AgentRunDispositionError } from "./AgentError.ts";
import { AgentPolicy, type AgentPolicyInput } from "./AgentPolicy.ts";
import { AgentId } from "./Identifiers.ts";

/** Prompt input produced directly or by an Effect that preserves its failure and requirements. */
export type InstructionResult<E = never, R = never> =
  | Prompt.RawInput
  | Effect.Effect<Prompt.RawInput, E, R>;

/** Static prompt input or an input-dependent source evaluated once while preparing a run. */
export type InstructionSource<Input, E = never, R = never> =
  | Prompt.RawInput
  | ((input: Input) => InstructionResult<E, R>);

/** Definition-owned projection from decoded Agent input to model-visible prompt content. */
export type InputPromptSource<Input, E = never, R = never> = (
  input: Input,
) => InstructionResult<E, R>;

/** Native services needed to execute and identify model calls. */
export type ModelServices = LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName;

/** Accepts Layers providing the native model and both identity services. */
export type NativeModel<ModelValue> =
  ModelValue extends Layer.Layer<infer Provides, never, infer _Requires>
    ? ModelServices extends Provides
      ? ModelValue
      : never
    : never;

/** Definition-owned boundary for selecting and validating an application run disposition. */
export interface RunDispositionDeclaration<Output, DispositionSchema extends Schema.Top> {
  /** Canonical Schema used to validate and encode the selected disposition. */
  readonly schema: DispositionSchema;
  /** Pure selection from decoded output; `undefined` means this ordinary Run declares none. */
  readonly fromOutput: (output: Output) => unknown;
}

/** Values available to a Definition-owned completion Tool projector after canonical decoding. */
export interface CompletionProjectionInput<Parameters = unknown, Result = unknown> {
  readonly parameters: Parameters;
  readonly result: Result;
}

/** One application Tool whose successful canonical result can complete its owning Agent. */
export interface CompletionToolDeclaration<
  Parameters = unknown,
  Result = unknown,
  Output = unknown,
> {
  readonly tool: string;
  /** Require native Tool use on every model Turn and this Tool for final completion. */
  readonly required?: boolean | undefined;
  readonly project: (input: CompletionProjectionInput<Parameters, Result>) => Output;
}

type CompletionToolFor<ToolkitValue extends Toolkit.Any, Output> = {
  readonly [Name in keyof ToolkitValue["tools"] & string]: CompletionToolDeclaration<
    Tool.Parameters<ToolkitValue["tools"][Name]>,
    Tool.Success<ToolkitValue["tools"][Name]>,
    Output
  > & { readonly tool: Name };
}[keyof ToolkitValue["tools"] & string];

/** Immutable, model-agnostic schemas, behavior, tools, and bounds for an agent. */
export interface Definition<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  ToolkitValue extends Toolkit.Any,
  RunDispositionValue = undefined,
  InputPromptValue = undefined,
> {
  /** Stable agent identity; changing it creates a distinct definition identity. */
  readonly id: AgentId;
  /** Canonical schema used to decode and encode run input. */
  readonly input: InputSchema;
  /** Canonical schema used to decode the final model output. */
  readonly output: OutputSchema;
  /** Instructions evaluated once while preparing each run. */
  readonly instructions: Instructions;
  /** Optional projection from decoded input to model-visible native Effect AI prompt content. */
  readonly inputPrompt?: InputPromptValue | undefined;
  /** Native Effect AI toolkit whose failures and requirements remain visible. */
  readonly toolkit: ToolkitValue;
  /** Finite execution bounds enforced by the runtime. */
  readonly policy: AgentPolicy;
  /** Explicit policy fields, retained so delegated runs can inherit omitted fields. */
  readonly policyOverrides?: Partial<AgentPolicyInput> | undefined;
  /** Optional successful Tool result that projects directly to the Agent output and settles. */
  readonly completion?: CompletionToolDeclaration | undefined;
  readonly runDisposition?: RunDispositionValue | undefined;
  readonly description?: string | undefined;
  readonly metadata?: Readonly<Record<string, string>> | undefined;
}

/** Options for a model-agnostic agent definition. */
export interface DefinitionOptions<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions extends InstructionSource<InputSchema["Type"], unknown, unknown>,
  ToolkitValue extends Toolkit.Any,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
  InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined =
    undefined,
> {
  readonly input: InputSchema;
  readonly output: OutputSchema;
  readonly instructions: Instructions;
  readonly inputPrompt?: InputPromptValue | undefined;
  readonly toolkit: ToolkitValue;
  readonly policy?: Partial<AgentPolicyInput> | undefined;
  readonly completion?: CompletionToolFor<ToolkitValue, OutputSchema["Type"]> | undefined;
  readonly runDisposition?: RunDispositionValue | undefined;
  readonly description?: string | undefined;
  readonly metadata?: Readonly<Record<string, string>> | undefined;
}

type AnyDefinitionShape = Definition<
  Schema.Top,
  Schema.Top,
  unknown,
  Toolkit.Any,
  RunDispositionDeclaration<never, Schema.Top> | undefined,
  unknown
>;

/** Immutable pairing of an agent definition with its native model Layer. */
export interface Binding<DefinitionValue extends AnyDefinitionShape, ModelValue> {
  readonly definition: DefinitionValue;
  readonly model: NativeModel<ModelValue>;
}

type InstructionEffect<Instructions, Input> = Instructions extends (input: Input) => infer Result
  ? Result
  : Instructions;

type InputPromptEffect<InputPrompt, Input> = InputPrompt extends (input: Input) => infer Result
  ? Result
  : never;

type EffectError<Value> =
  Value extends Effect.Effect<infer _Success, infer Error, infer _Services> ? Error : never;

type EffectServices<Value> =
  Value extends Effect.Effect<infer _Success, infer _Error, infer Services> ? Services : never;

type ModelRequirements<Value> =
  Value extends Layer.Layer<infer _Provides, infer _Error, infer Services> ? Services : never;

/** Constructors and type projections for definitions and runnable model bindings. */

/** Type-erased definition used at generic framework boundaries. */
export type AnyDefinition = AnyDefinitionShape;

/** Read-only interpreter program or binding used at inspection and projection boundaries. */
export interface Any {
  readonly definition: AnyDefinition;
  readonly model?: unknown;
}

type DefinitionOf<AgentValue extends AnyDefinition | Any> = AgentValue extends {
  readonly definition: infer DefinitionValue extends AnyDefinition;
}
  ? DefinitionValue
  : AgentValue;

type RunDispositionSchemaOf<DefinitionValue extends AnyDefinition> = [
  Exclude<DefinitionValue["runDisposition"], undefined>,
] extends [never]
  ? never
  : Exclude<DefinitionValue["runDisposition"], undefined> extends RunDispositionDeclaration<
        never,
        infer DispositionSchema
      >
    ? DispositionSchema
    : never;

/** Decoded input type of a definition or binding. */
export type Input<AgentValue extends AnyDefinition | Any> =
  DefinitionOf<AgentValue>["input"]["Type"];

/** Encoded input accepted by the primary execution operations. */
export type EncodedInput<AgentValue extends AnyDefinition | Any> =
  DefinitionOf<AgentValue>["input"]["Encoded"];

/** Decoded output type of a definition or binding. */
export type Output<AgentValue extends AnyDefinition | Any> =
  DefinitionOf<AgentValue>["output"]["Type"];

/** Input Schema carried by a definition or binding. */
export type InputSchema<AgentValue extends AnyDefinition | Any> = DefinitionOf<AgentValue>["input"];

/** Output Schema carried by a definition or binding. */
export type OutputSchema<AgentValue extends AnyDefinition | Any> =
  DefinitionOf<AgentValue>["output"];

/** Application run-disposition Schema carried by a definition or binding, or `never`. */
export type RunDispositionSchema<AgentValue extends AnyDefinition | Any> = RunDispositionSchemaOf<
  DefinitionOf<AgentValue>
>;

/** Decoded application run disposition declared by a definition or binding. */
export type RunDisposition<AgentValue extends AnyDefinition | Any> = [
  RunDispositionSchema<AgentValue>,
] extends [never]
  ? never
  : RunDispositionSchema<AgentValue>["Type"];

/** Validation failure admitted only by definitions that declare a run disposition. */
export type RunDispositionFailure<AgentValue extends AnyDefinition | Any> = [
  RunDispositionSchemaOf<DefinitionOf<AgentValue>>,
] extends [never]
  ? never
  : AgentRunDispositionError;

/** Effect AI tool map carried by a definition or binding. */
export type Tools<AgentValue extends AnyDefinition | Any> = Toolkit.Tools<
  DefinitionOf<AgentValue>["toolkit"]
>;

type ToolMapValues<ToolMap extends Record<string, Tool.Any>> = ToolMap extends unknown
  ? ToolMap[keyof ToolMap]
  : never;

/** All possible tools, including keys present in only one definition or binding branch. */
export type ToolUnion<AgentValue extends AnyDefinition | Any> = ToolMapValues<Tools<AgentValue>>;

/** Instruction, tool-handler, and Schema services required before a Model is bound. */
export type DefinitionRequirements<DefinitionValue extends AnyDefinition> =
  DefinitionValue extends unknown
    ?
        | EffectServices<InstructionEffect<DefinitionValue["instructions"], Input<DefinitionValue>>>
        | EffectServices<InputPromptEffect<DefinitionValue["inputPrompt"], Input<DefinitionValue>>>
        | Tool.HandlersFor<Tools<DefinitionValue>>
        | Tool.HandlerServices<ToolUnion<DefinitionValue>>
        | Tool.SuccessSchema<ToolUnion<DefinitionValue>>["DecodingServices"]
        | DefinitionValue["input"]["DecodingServices"]
        | DefinitionValue["input"]["EncodingServices"]
        | DefinitionValue["output"]["DecodingServices"]
        | DefinitionValue["output"]["EncodingServices"]
        | RunDispositionSchemaOf<DefinitionValue>["DecodingServices"]
        | RunDispositionSchemaOf<DefinitionValue>["EncodingServices"]
    : never;

/** Definition services plus native model services, or a binding's model Layer requirements. */
export type Requirements<AgentValue extends AnyDefinition | Any> =
  | DefinitionRequirements<DefinitionOf<AgentValue>>
  | (AgentValue extends { readonly model: infer ModelValue }
      ? ModelRequirements<ModelValue>
      : ModelServices);

// Distribute after unwrapping bindings too: withModel may itself contain a definition union.
type DefinitionFailure<DefinitionValue extends AnyDefinition> = DefinitionValue extends unknown
  ?
      | EffectError<InstructionEffect<DefinitionValue["instructions"], Input<DefinitionValue>>>
      | EffectError<InputPromptEffect<DefinitionValue["inputPrompt"], Input<DefinitionValue>>>
      | Tool.HandlerError<ToolUnion<DefinitionValue>>
  : never;

/** Failures from every possible definition, preserving each branch's instruction input type. */
export type Failure<AgentValue extends AnyDefinition | Any> =
  | DefinitionFailure<DefinitionOf<AgentValue>>
  | AiError.AiError
  | AgentInputError
  | AgentOutputError
  | RunDispositionFailure<AgentValue>;

/** Validate an agent ID and return a shallowly frozen, model-agnostic definition. */
export function make<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions extends InstructionSource<InputSchema["Type"], unknown, unknown>,
  ToolkitValue extends Toolkit.Any,
  DispositionSchema extends Schema.Top,
  InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined,
>(
  id: string,
  options: DefinitionOptions<
    InputSchema,
    OutputSchema,
    Instructions,
    ToolkitValue,
    RunDispositionDeclaration<OutputSchema["Type"], DispositionSchema>,
    InputPromptValue
  > & {
    readonly runDisposition: RunDispositionDeclaration<OutputSchema["Type"], DispositionSchema>;
    readonly inputPrompt: InputPromptValue;
  },
): Definition<
  InputSchema,
  OutputSchema,
  Instructions,
  ToolkitValue,
  RunDispositionDeclaration<OutputSchema["Type"], DispositionSchema>,
  InputPromptValue
>;

export function make<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions extends InstructionSource<InputSchema["Type"], unknown, unknown>,
  ToolkitValue extends Toolkit.Any,
  DispositionSchema extends Schema.Top,
>(
  id: string,
  options: DefinitionOptions<
    InputSchema,
    OutputSchema,
    Instructions,
    ToolkitValue,
    RunDispositionDeclaration<OutputSchema["Type"], DispositionSchema>
  > & {
    readonly runDisposition: RunDispositionDeclaration<OutputSchema["Type"], DispositionSchema>;
    readonly inputPrompt?: undefined;
  },
): Definition<
  InputSchema,
  OutputSchema,
  Instructions,
  ToolkitValue,
  RunDispositionDeclaration<OutputSchema["Type"], DispositionSchema>
>;

export function make<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions extends InstructionSource<InputSchema["Type"], unknown, unknown>,
  ToolkitValue extends Toolkit.Any,
  InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined,
>(
  id: string,
  options: DefinitionOptions<
    InputSchema,
    OutputSchema,
    Instructions,
    ToolkitValue,
    undefined,
    InputPromptValue
  > & {
    readonly inputPrompt: InputPromptValue;
    readonly runDisposition?: undefined;
  },
): Definition<InputSchema, OutputSchema, Instructions, ToolkitValue, undefined, InputPromptValue>;

export function make<
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions extends InstructionSource<InputSchema["Type"], unknown, unknown>,
  ToolkitValue extends Toolkit.Any,
>(
  id: string,
  options: DefinitionOptions<InputSchema, OutputSchema, Instructions, ToolkitValue> & {
    readonly inputPrompt?: undefined;
    readonly runDisposition?: undefined;
  },
): Definition<InputSchema, OutputSchema, Instructions, ToolkitValue>;

export function make(
  id: string,
  options: {
    readonly input: Schema.Top;
    readonly output: Schema.Top;
    readonly instructions: unknown;
    readonly inputPrompt?: unknown;
    readonly toolkit: Toolkit.Any;
    readonly policy?: Partial<AgentPolicyInput> | undefined;
    readonly completion?: CompletionToolDeclaration | undefined;
    readonly runDisposition?: RunDispositionDeclaration<never, Schema.Top> | undefined;
    readonly description?: string | undefined;
    readonly metadata?: Readonly<Record<string, string>> | undefined;
  },
): AnyDefinition {
  return Object.freeze({
    ...options,
    policy: AgentPolicy.resolve(options.policy),
    policyOverrides: Object.freeze({ ...options.policy }),
    id: S.decodeSync(AgentId)(id),
    metadata: options.metadata === undefined ? undefined : Object.freeze({ ...options.metadata }),
    completion:
      options.completion === undefined ? undefined : Object.freeze({ ...options.completion }),
    runDisposition:
      options.runDisposition === undefined
        ? undefined
        : Object.freeze({ ...options.runDisposition }),
  });
}

/** Fix a model Layer for registration or delegation without acquiring or hiding its requirements. */
export const withModel = <DefinitionValue extends AnyDefinition, ModelValue>(
  definition: DefinitionValue,
  model: NativeModel<ModelValue>,
): Binding<DefinitionValue, ModelValue> =>
  Object.freeze({
    definition,
    model,
  });

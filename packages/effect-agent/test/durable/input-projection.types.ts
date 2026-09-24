import { type Crypto, Context, Effect, Schema, SchemaGetter, Layer } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import type * as DecisionTurn from "effect-agent/decision-turn";
import { type DurableWorkerRequirements } from "effect-agent/durable-agent-runtime";
import { Toolkit, type LanguageModel, type Model, Tool } from "effect/unstable/ai";

import { compileRegistrations } from "../../src/durable/internal/agent-registration.ts";
import { DefinitionDigestInput } from "../../src/durable/Records.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

class InputProjectionFailure extends Schema.TaggedError<InputProjectionFailure>()(
  "InputProjectionFailure",
  {},
) {}

class InputProjection extends Context.Service<
  InputProjection,
  { readonly render: (question: string) => Effect.Effect<string, InputProjectionFailure> }
>()("@effect-agent/thread/test/InputProjection") {}

class InstructionContext extends Context.Service<InstructionContext, { readonly text: string }>()(
  "@effect-agent/thread/test/InstructionContext",
) {}

class ProviderInfrastructure extends Context.Service<ProviderInfrastructure, string>()(
  "@effect-agent/thread/test/ProviderInfrastructure",
) {}

class SecondProviderInfrastructure extends Context.Service<SecondProviderInfrastructure, string>()(
  "@effect-agent/thread/test/SecondProviderInfrastructure",
) {}

class SchemaDecoder extends Context.Service<SchemaDecoder, string>()(
  "@effect-agent/thread/test/SchemaDecoder",
) {}

class SchemaEncoder extends Context.Service<SchemaEncoder, string>()(
  "@effect-agent/thread/test/SchemaEncoder",
) {}

class LookupDependency extends Context.Service<LookupDependency, string>()(
  "@effect-agent/thread/test/LookupDependency",
) {}

class AuditDependency extends Context.Service<AuditDependency, string>()(
  "@effect-agent/thread/test/AuditDependency",
) {}

class ReportDependency extends Context.Service<ReportDependency, string>()(
  "@effect-agent/thread/test/ReportDependency",
) {}

const ServiceString = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((value) => Effect.as(SchemaEncoder, value)),
    encode: SchemaGetter.transformEffect((value) => Effect.as(SchemaDecoder, value)),
  }),
);

const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ query: ServiceString }),
  success: ServiceString,
  dependencies: [LookupDependency],
});

const Audit = Tool.make("audit", {
  parameters: Schema.Struct({ value: ServiceString }),
  success: ServiceString,
  dependencies: [AuditDependency],
});

const secondDefinition = Agent.make("durable-heterogeneous-registration-types", {
  input: Schema.Struct({ question: ServiceString }),
  output: ServiceString,
  instructions: "Use both tools.",
  toolkit: Toolkit.make(Lookup, Audit),
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
});

const definition = Agent.make("durable-input-projection-types", {
  input: Schema.Struct({ question: Schema.String, hostOnly: Schema.String }),
  output: Schema.String,
  instructions: () => Effect.map(InstructionContext, ({ text }) => text),
  inputPrompt: ({ question }) =>
    question === "" ? [] : Effect.flatMap(InputProjection, ({ render }) => render(question)),
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  runDisposition: {
    schema: Schema.Literal("answered"),
    fromOutput: () => "answered",
  },
});

export const proveRegistrationRequirements = (
  firstModel: Layer.Layer<
    LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName,
    never,
    ProviderInfrastructure
  >,
  secondModel: Layer.Layer<
    LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName,
    never,
    SecondProviderInfrastructure
  >,
  enabled: boolean,
) => {
  const first = Agent.withModel(definition, firstModel);
  const second = Agent.withModel(secondDefinition, secondModel);

  const compiled = compileRegistrations([
    {
      agent: definition,
      model: firstModel,
      definitions: DefinitionDigestInput.make({ agent: "first", model: "first", tools: [] }),
    },
    {
      agent: second,
      definitions: DefinitionDigestInput.make({
        agent: "second",
        model: "second",
        tools: ["lookup", "audit"],
      }),
    },
  ]);

  const conditionalEntry = {
    agent: first,
    definitions: DefinitionDigestInput.make({ agent: "conditional", model: "first", tools: [] }),
  };

  const attemptLayer = () =>
    Layer.effect(InstructionContext)(Effect.map(ReportDependency, (text) => ({ text })));

  const conditionalAttempt = compileRegistrations([
    { ...conditionalEntry, attemptLayer: enabled ? attemptLayer : undefined },
  ]);

  const optionalOptions = (options: { readonly attemptLayer?: typeof attemptLayer }) =>
    compileRegistrations([
      { ...conditionalEntry, ...options },
      {
        agent: second,
        definitions: DefinitionDigestInput.make({ agent: "second", model: "second", tools: [] }),
      },
    ]);

  // Regression: 39828502 dropped services when the Decision descriptor was optional.
  const optionalDecisionOptions = (options: {
    readonly decisionTurn?: DecisionTurn.Definition<never, ReportDependency>;
  }) => compileRegistrations([{ ...conditionalEntry, ...options }]);

  const conditionalRequirements: readonly [
    Assert<
      Equal<
        Effect.Services<typeof conditionalAttempt>,
        Crypto.Crypto | DurableWorkerRequirements<typeof first> | ReportDependency
      >
    >,
    Assert<
      Equal<
        Effect.Services<ReturnType<typeof optionalOptions>>,
        | Crypto.Crypto
        | DurableWorkerRequirements<typeof first>
        | DurableWorkerRequirements<typeof second>
        | ReportDependency
      >
    >,
    Assert<
      Equal<
        Effect.Services<ReturnType<typeof optionalDecisionOptions>>,
        Crypto.Crypto | DurableWorkerRequirements<typeof first> | ReportDependency
      >
    >,
  ] = [true, true, true];

  void conditionalRequirements;

  const scoped = compileRegistrations([
    {
      agent: definition,
      model: firstModel,
      definitions: DefinitionDigestInput.make({ agent: "direct", model: "direct", tools: [] }),
      attemptLayer: () =>
        Layer.effect(InstructionContext)(Effect.map(LookupDependency, (text) => ({ text }))),
    },
    {
      agent: first,
      definitions: DefinitionDigestInput.make({ agent: "scoped", model: "scoped", tools: [] }),
      attemptLayer: () =>
        Layer.effect(InstructionContext)(Effect.map(LookupDependency, (text) => ({ text }))),
    },
  ]);

  const scopedRequirements: Assert<
    Equal<
      Effect.Services<typeof scoped>,
      Crypto.Crypto | InputProjection | ProviderInfrastructure | LookupDependency
    >
  > = true;

  void scopedRequirements;

  type Services = Effect.Services<typeof compiled>;
  type Expected =
    | Crypto.Crypto
    | DurableWorkerRequirements<typeof first>
    | DurableWorkerRequirements<typeof second>;
  type CompleteUnion = Assert<Equal<Services, Expected>>;

  const proofs: readonly [CompleteUnion] = [true];

  return proofs;
};

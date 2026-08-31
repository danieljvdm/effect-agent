import { Agent, AgentPolicy, type ConversationId, type IdGenerator } from "@effect-agent/core";
import {
  type ConversationHistory,
  AgentRuntime,
  type AgentRuntimeFailure,
} from "@effect-agent/engine";
import { describe, expect, it } from "@effect/vitest";
import {
  type Stream,
  Context,
  Crypto,
  Effect,
  Schema,
  SchemaGetter,
  type Scope,
  type Layer,
} from "effect";
import { Toolkit, type LanguageModel, type Model, Tool } from "effect/unstable/ai";

import { compileRegistrations } from "../src/agent-registration.ts";
import { PersistentHistory } from "../src/history.ts";
import {
  type DurableAgentRuntime,
  DurableWorkerBinding,
  type DefinitionDigests,
  type DigestError,
  type DurableWorkerRequirements,
  type ConversationStore,
  type ResolvedBinding,
} from "../src/index.ts";
import { DefinitionDigestInput } from "../src/records.ts";

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
>()("@effect-agent/session/test/InputProjection") {}

class InstructionContext extends Context.Service<InstructionContext, { readonly text: string }>()(
  "@effect-agent/session/test/InstructionContext",
) {}

class ProviderInfrastructure extends Context.Service<ProviderInfrastructure, string>()(
  "@effect-agent/session/test/ProviderInfrastructure",
) {}

class SecondProviderInfrastructure extends Context.Service<SecondProviderInfrastructure, string>()(
  "@effect-agent/session/test/SecondProviderInfrastructure",
) {}

class SchemaDecoder extends Context.Service<SchemaDecoder, string>()(
  "@effect-agent/session/test/SchemaDecoder",
) {}

class SchemaEncoder extends Context.Service<SchemaEncoder, string>()(
  "@effect-agent/session/test/SchemaEncoder",
) {}

class LookupDependency extends Context.Service<LookupDependency, string>()(
  "@effect-agent/session/test/LookupDependency",
) {}

class AuditDependency extends Context.Service<AuditDependency, string>()(
  "@effect-agent/session/test/AuditDependency",
) {}

const ServiceString = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformOrFail((value) => Effect.as(SchemaEncoder, value)),
    encode: SchemaGetter.transformOrFail((value) => Effect.as(SchemaDecoder, value)),
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

const proveWorkerRequirements = (
  runtime: DurableAgentRuntime["Service"],
  model: Layer.Layer<
    LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName,
    never,
    ProviderInfrastructure
  >,
  conversationId: ConversationId,
  digests: DefinitionDigests,
) => {
  const agent = Agent.withModel(definition, model);
  const process = runtime.processConversation(agent, conversationId);
  const worker = runtime.runWorker(agent);
  const registered = DurableWorkerBinding.make(agent, digests);
  const execution = AgentRuntime.run(
    agent,
    { question: "hello", hostOnly: "private" },
    { conversationId },
  );
  const retained = execution.pipe(Effect.provide(PersistentHistory.layer));
  const events = AgentRuntime.stream(
    agent,
    { question: "hello", hostOnly: "private" },
    { conversationId },
  );
  const detached = AgentRuntime.start(
    agent,
    { question: "hello", hostOnly: "private" },
    { conversationId },
  );

  type Expected = InputProjection | InstructionContext | ProviderInfrastructure;
  type ProcessProof = Assert<Equal<Effect.Services<typeof process>, Expected>>;
  type WorkerProof = Assert<Equal<Effect.Services<typeof worker>, Expected>>;
  type RegisteredProof = Assert<Equal<Effect.Services<typeof registered>, Expected>>;
  type PublicProof = Assert<Equal<DurableWorkerRequirements<typeof agent>, Expected>>;
  type FailureProof = Assert<
    Equal<Extract<Agent.Failure<typeof agent>, InputProjectionFailure>, InputProjectionFailure>
  >;
  type RunRequirementsProof = Assert<
    Equal<Effect.Services<typeof execution>, Expected | ConversationHistory | IdGenerator>
  >;
  type StreamRequirementsProof = Assert<
    Equal<Stream.Services<typeof events>, Expected | ConversationHistory | IdGenerator>
  >;
  type StartRequirementsProof = Assert<
    Equal<
      Effect.Services<typeof detached>,
      Expected | ConversationHistory | IdGenerator | Scope.Scope
    >
  >;
  type HistoryRequirementsProof = Assert<
    Equal<Effect.Services<typeof retained>, Expected | ConversationStore | IdGenerator>
  >;
  type HistoryFailureProof = Assert<
    Equal<Effect.Error<typeof retained>, AgentRuntimeFailure<typeof agent>>
  >;
  type HistoryOutputProof = Assert<Equal<Effect.Success<typeof retained>["output"], string>>;
  const proofs: readonly [
    ProcessProof,
    WorkerProof,
    RegisteredProof,
    PublicProof,
    FailureProof,
    RunRequirementsProof,
    StreamRequirementsProof,
    StartRequirementsProof,
    HistoryRequirementsProof,
    HistoryFailureProof,
    HistoryOutputProof,
  ] = [true, true, true, true, true, true, true, true, true, true, true];
  return proofs;
};

const proveRegistrationRequirements = (
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
) => {
  const first = Agent.withModel(definition, firstModel);
  const second = Agent.withModel(secondDefinition, secondModel);
  const compiled = compileRegistrations([
    {
      agent: first,
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
  const empty = compileRegistrations([]);
  const identityOnly = {
    agentId: first.definition.id,
    attempt: () => Effect.never,
  };
  // @ts-expect-error Exact registrations require a definition digest triple.
  const rejectedIdentityOnly: ResolvedBinding = identityOnly;
  void rejectedIdentityOnly;

  type Services = Effect.Services<typeof compiled>;
  type Expected =
    | Crypto.Crypto
    | DurableWorkerRequirements<typeof first>
    | DurableWorkerRequirements<typeof second>;
  type CompleteUnion = Assert<Equal<Services, Expected>>;
  type FirstModel = Assert<
    Equal<Extract<Services, ProviderInfrastructure>, ProviderInfrastructure>
  >;
  type SecondModel = Assert<
    Equal<Extract<Services, SecondProviderInfrastructure>, SecondProviderInfrastructure>
  >;
  type SchemaDecode = Assert<Equal<Extract<Services, SchemaDecoder>, SchemaDecoder>>;
  type SchemaEncode = Assert<Equal<Extract<Services, SchemaEncoder>, SchemaEncoder>>;
  type FirstTool = Assert<Equal<Extract<Services, LookupDependency>, LookupDependency>>;
  type SecondTool = Assert<Equal<Extract<Services, AuditDependency>, AuditDependency>>;
  type Failure = Assert<Equal<Effect.Error<typeof compiled>, DigestError>>;
  type Empty = Assert<Equal<Effect.Services<typeof empty>, Crypto.Crypto>>;
  const proofs: readonly [
    CompleteUnion,
    FirstModel,
    SecondModel,
    SchemaDecode,
    SchemaEncode,
    FirstTool,
    SecondTool,
    Failure,
    Empty,
  ] = [true, true, true, true, true, true, true, true, true];
  return proofs;
};

describe("durable input projection types", () => {
  it("retains projection and provider requirements when registering a native model Layer", () => {
    expect(proveWorkerRequirements).toBeInstanceOf(Function);
  });

  it("retains every heterogeneous registration requirement", () => {
    expect(proveRegistrationRequirements).toBeInstanceOf(Function);
  });
});

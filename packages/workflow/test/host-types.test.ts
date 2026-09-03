import * as Agent from "@effect-agent/core/Agent";
import { type ModelServices } from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { type DigestError } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { type WorkflowHostConfigError } from "@effect-agent/workflow/WorkflowAgentHost";
import { WorkflowAgentHost } from "@effect-agent/workflow/WorkflowAgentHost";
import {
  type WorkflowDispatchStore,
  type WorkflowRepairTrigger,
} from "@effect-agent/workflow/WorkflowDispatch";
import { expect, it } from "@effect/vitest";
import { Context, type Crypto, Effect, Layer, Schema, SchemaGetter } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import { WorkflowEngine } from "effect/unstable/workflow";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<T extends true> = T;

class Provider extends Context.Service<Provider, string>()("workflow-types/Provider") {}
class Instructions extends Context.Service<Instructions, string>()("workflow-types/Instructions") {}
class Decoder extends Context.Service<Decoder, string>()("workflow-types/Decoder") {}
class Encoder extends Context.Service<Encoder, string>()("workflow-types/Encoder") {}
class ToolDependency extends Context.Service<ToolDependency, string>()("workflow-types/Tool") {}
class EngineConfig extends Context.Service<EngineConfig, string>()("workflow-types/EngineConfig") {}
class StorageConfig extends Context.Service<StorageConfig, string>()(
  "workflow-types/StorageConfig",
) {}
class AuthorizationConfig extends Context.Service<AuthorizationConfig, string>()(
  "workflow-types/AuthorizationConfig",
) {}
class DispatchConfig extends Context.Service<DispatchConfig, string>()(
  "workflow-types/DispatchConfig",
) {}
class TriggerConfig extends Context.Service<TriggerConfig, string>()(
  "workflow-types/TriggerConfig",
) {}
class EngineError extends Schema.TaggedError<EngineError>()("EngineSetupError", {}) {}
class StorageError extends Schema.TaggedError<StorageError>()("StorageSetupError", {}) {}
class AuthorizationError extends Schema.TaggedError<AuthorizationError>()(
  "AuthorizationSetupError",
  {},
) {}
class DispatchError extends Schema.TaggedError<DispatchError>()("DispatchSetupError", {}) {}
class TriggerError extends Schema.TaggedError<TriggerError>()("TriggerSetupError", {}) {}

const ServiceString = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformOrFail((value) => Effect.as(Decoder, value)),
    encode: SchemaGetter.transformOrFail((value) => Effect.as(Encoder, value)),
  }),
);

const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ query: ServiceString }),
  success: ServiceString,
  dependencies: [ToolDependency],
});

const definition = Agent.make("workflow-types", {
  input: ServiceString,
  output: ServiceString,
  instructions: () => Instructions,
  toolkit: Toolkit.make(Lookup),
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "1 minute",
    toolConcurrency: 1,
  }),
});

const definitions = DefinitionDigestInput.make({ agent: "v1", model: "v1", tools: "v1" });

type RuntimeServices = Layer.Services<typeof DurableAgentRuntime.layerWithServices>;

const proveComposition = (
  model: Layer.Layer<ModelServices, never, Provider>,
  engine: Layer.Layer<WorkflowEngine.WorkflowEngine, EngineError, EngineConfig>,
  runtime: Layer.Layer<
    RuntimeServices,
    StorageError | AuthorizationError,
    StorageConfig | AuthorizationConfig
  >,
  dispatch: Layer.Layer<WorkflowDispatchStore, DispatchError, DispatchConfig>,
  trigger: Layer.Layer<WorkflowRepairTrigger, TriggerError, TriggerConfig>,
) => {
  const registered = DurableAgentRuntime.layerRegistered([
    { agent: definition, model, definitions },
  ]);

  const live = WorkflowAgentHost.layer({
    deploymentId: "types",
  }).pipe(Layer.provide(registered));

  type AgentServices =
    | Provider
    | Instructions
    | Decoder
    | Encoder
    | ToolDependency
    | Tool.Handler<"lookup">;
  type HostServices =
    | RuntimeServices
    | WorkflowEngine.WorkflowEngine
    | WorkflowDispatchStore
    | WorkflowRepairTrigger
    | Crypto.Crypto;

  const requirements: Assert<Equal<Layer.Services<typeof live>, AgentServices | HostServices>> =
    true;

  const registrationRequirements: Assert<
    Equal<Layer.Services<typeof registered>, AgentServices | RuntimeServices>
  > = true;

  const registrationErrors: Assert<Equal<Layer.Error<typeof registered>, DigestError>> = true;

  const registrationOutput: Assert<Equal<Layer.Success<typeof registered>, DurableAgentRuntime>> =
    true;

  const errors: Assert<Equal<Layer.Error<typeof live>, DigestError | WorkflowHostConfigError>> =
    true;

  const output: Assert<Equal<Layer.Success<typeof live>, WorkflowAgentHost>> = true;

  const supplied = live.pipe(
    Layer.provide(engine),
    Layer.provide(runtime),
    Layer.provide(dispatch),
    Layer.provide(trigger),
  );

  const suppliedRequirements: Assert<
    Equal<
      Layer.Services<typeof supplied>,
      | AgentServices
      | EngineConfig
      | StorageConfig
      | AuthorizationConfig
      | DispatchConfig
      | TriggerConfig
    >
  > = true;

  const suppliedErrors: Assert<
    Equal<
      Layer.Error<typeof supplied>,
      | DigestError
      | WorkflowHostConfigError
      | EngineError
      | StorageError
      | AuthorizationError
      | DispatchError
      | TriggerError
    >
  > = true;

  const memory = live.pipe(Layer.provide(WorkflowEngine.layerMemory));

  const substituted: Assert<
    Equal<
      Layer.Services<typeof memory>,
      AgentServices | Exclude<HostServices, WorkflowEngine.WorkflowEngine>
    >
  > = true;

  const empty = WorkflowAgentHost.layer({ deploymentId: "types" }).pipe(
    Layer.provide(DurableAgentRuntime.layerRegistered([])),
  );

  const emptyRequirements: Assert<Equal<Layer.Services<typeof empty>, HostServices>> = true;

  return [
    requirements,
    registrationRequirements,
    registrationErrors,
    registrationOutput,
    errors,
    output,
    suppliedRequirements,
    suppliedErrors,
    substituted,
    emptyRequirements,
  ];
};

it("preserves exact registration and supplied Layer errors and requirements", () => {
  expect(proveComposition).toBeTypeOf("function");
});

import {
  type AgentApprovalDenied,
  type AgentApprovalPending,
  type AgentInputError,
  type AgentOutputError,
  type AgentPolicyError,
  type ContextBudgetError,
  type ContextOverflowError,
  type IdGenerator,
  type ModelProtocolError,
  Agent,
  AgentPolicy,
  type AgentToolAuthorizationDenied,
} from "@effect-agent/core";
import {
  type AgentChildPending,
  AgentRuntime,
  type CompactionError,
  type AgentRuntimeFailure,
  type AgentRuntimeRequirements,
} from "@effect-agent/engine";
import type { DurableWorkerRequirements } from "@effect-agent/session";
import { Context, Effect, Schema, SchemaGetter, type Scope } from "effect";
import { type AiError, Model, Tool, Toolkit } from "effect/unstable/ai";
import { describe, expect, it } from "vite-plus/test";

import type {
  ActivityCatalog,
  ActivityUnavailable,
  FlightCatalog,
  FlightUnavailable,
  GuidanceFailure,
  LodgingCatalog,
  LodgingUnavailable,
  TravelGuidance,
  TravelPlannerToolkit,
} from "../src/index.ts";
import { phase1Trip, ScriptedModel, TravelPlanner } from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const model = Model.make("scripted", "travel-planner-type-proof", ScriptedModel.layer([]));
const agent = Agent.withModel(TravelPlanner, model);
const program = AgentRuntime.run(agent, phase1Trip);

class CompletionResultDecoder extends Context.Service<
  CompletionResultDecoder,
  { readonly validate: (value: string) => string }
>()("@effect-agent/testing/CompletionResultDecoder") {}

const ContextualCompletionResult = Schema.String.pipe(
  Schema.decode({
    decode: SchemaGetter.transformOrFail((value) =>
      Effect.map(CompletionResultDecoder, ({ validate }) => validate(value)),
    ),
    encode: SchemaGetter.transform((value) => value),
  }),
);
const Complete = Tool.make("complete", {
  parameters: Schema.Struct({ answer: Schema.String }),
  success: ContextualCompletionResult,
});
const completionToolkit = Toolkit.make(Complete);
const completionDefinition = Agent.define("completion-requirements-proof", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Complete through the Tool.",
  toolkit: completionToolkit,
  policy: AgentPolicy.make({
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  completion: {
    tool: "complete",
    project: ({ result }) => ({ answer: result }),
  },
});
const completionAgent = Agent.withModel(completionDefinition, model);

type ExpectedRequirements =
  | FlightCatalog
  | LodgingCatalog
  | ActivityCatalog
  | TravelGuidance
  | Tool.HandlersFor<Toolkit.Tools<typeof TravelPlannerToolkit>>
  | IdGenerator
  | Scope.Scope;
type ExpectedFailure =
  | FlightUnavailable
  | LodgingUnavailable
  | ActivityUnavailable
  | GuidanceFailure
  | AiError.AiError
  | AgentInputError
  | AgentOutputError
  | AgentPolicyError
  | ContextBudgetError
  | ContextOverflowError
  | CompactionError
  | ModelProtocolError
  | AgentApprovalDenied
  | AgentToolAuthorizationDenied
  | AgentApprovalPending
  | AgentChildPending;

type RequirementsProof = Assert<Equal<Effect.Services<typeof program>, ExpectedRequirements>>;
type FailureProof = Assert<Equal<Effect.Error<typeof program>, ExpectedFailure>>;
type PublicRequirementsProof = Assert<
  Equal<AgentRuntimeRequirements<typeof agent> | Scope.Scope, ExpectedRequirements>
>;
type PublicFailureProof = Assert<Equal<AgentRuntimeFailure<typeof agent>, ExpectedFailure>>;
type CompletionRuntimeRequirementsProof = Assert<
  CompletionResultDecoder extends AgentRuntimeRequirements<typeof completionAgent> ? true : false
>;
type CompletionDurableRequirementsProof = Assert<
  CompletionResultDecoder extends DurableWorkerRequirements<typeof completionAgent> ? true : false
>;

describe("TEST-009 P1 Travel Planner public-contract inference", () => {
  it("preserves Tool and instruction failures and requirements in Run E/R", () => {
    const requirementsProof: RequirementsProof = true;
    const failureProof: FailureProof = true;
    const publicRequirementsProof: PublicRequirementsProof = true;
    const publicFailureProof: PublicFailureProof = true;
    const completionRuntimeRequirementsProof: CompletionRuntimeRequirementsProof = true;
    const completionDurableRequirementsProof: CompletionDurableRequirementsProof = true;

    expect({
      requirementsProof,
      failureProof,
      publicRequirementsProof,
      publicFailureProof,
      completionRuntimeRequirementsProof,
      completionDurableRequirementsProof,
    }).toEqual({
      requirementsProof: true,
      failureProof: true,
      publicRequirementsProof: true,
      publicFailureProof: true,
      completionRuntimeRequirementsProof: true,
      completionDurableRequirementsProof: true,
    });
  });
});

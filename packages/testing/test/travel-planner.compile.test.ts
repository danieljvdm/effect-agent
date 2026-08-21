import {
  type AgentApprovalDenied,
  type AgentApprovalPending,
  type AgentInputError,
  type AgentOutputError,
  type AgentPolicyError,
  type ContextOverflowError,
  type IdGenerator,
  type ModelProtocolError,
  Agent,
  type AgentToolAuthorizationDenied,
} from "@effect-agent/core";
import {
  type AgentChildPending,
  AgentRuntime,
  type AgentRuntimeFailure,
  type AgentRuntimeRequirements,
} from "@effect-agent/engine";
import { type Effect, type Scope } from "effect";
import { type AiError, Model, type Tool, type Toolkit } from "effect/unstable/ai";
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
  | ContextOverflowError
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

describe("TEST-009 P1 Travel Planner public-contract inference", () => {
  it("preserves Tool and instruction failures and requirements in Run E/R", () => {
    const requirementsProof: RequirementsProof = true;
    const failureProof: FailureProof = true;
    const publicRequirementsProof: PublicRequirementsProof = true;
    const publicFailureProof: PublicFailureProof = true;

    expect({
      requirementsProof,
      failureProof,
      publicRequirementsProof,
      publicFailureProof,
    }).toEqual({
      requirementsProof: true,
      failureProof: true,
      publicRequirementsProof: true,
      publicFailureProof: true,
    });
  });
});

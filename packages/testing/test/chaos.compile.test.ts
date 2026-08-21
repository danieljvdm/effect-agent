import type {
  ConversationStore,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpointTestControl,
  SubmissionLedger,
} from "@effect-agent/session";
import type { Crypto, Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  ChaosPlan,
  ChaosSubmissionSpec,
  runChaosPlan,
  type ChaosConvergenceFailure,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const program = runChaosPlan(
  ChaosPlan.make({
    seed: 1,
    lanes: 1,
    submissions: [ChaosSubmissionSpec.make({ lane: 0, kind: "plain" })],
    failpointArms: [],
    adapterArms: [],
    abortInjections: [],
    resolutionInjections: [],
    approvalDecisions: [],
  }),
);

type ExpectedRequirements =
  | ConversationStore
  | Crypto.Crypto
  | DurableAgentRuntime
  | DurableRuntimeConfig
  | DurableRuntimeFailpointTestControl
  | SubmissionLedger;

type FailureProof = Assert<Equal<Effect.Error<typeof program>, ChaosConvergenceFailure>>;
type RequirementsProof = Assert<Equal<Effect.Services<typeof program>, ExpectedRequirements>>;

describe("chaos runner Effect contract", () => {
  it("keeps convergence failures and runtime requirements visible", () => {
    const failureProof: FailureProof = true;
    const requirementsProof: RequirementsProof = true;

    expect({ failureProof, requirementsProof }).toEqual({
      failureProof: true,
      requirementsProof: true,
    });
  });
});

import { Context, type Effect, type Option, type Schema } from "effect";
import type { Prompt } from "effect/unstable/ai";

import type { DecisionTurnError } from "../../core/AgentError.ts";
import {
  type DecisionTurnEvidence,
  type DecisionTurnModel,
  type DecisionTurnResult,
} from "../../core/DecisionTurn.ts";
import type {
  AgentId,
  AttemptId,
  RunId,
  SubmissionId,
  ThreadId,
  TurnId,
} from "../../core/Identifiers.ts";

/** Trusted native context before an eligible ordinary-Turn inference. This value grants no action authority. */
export interface DecisionTurnRequest {
  readonly agentId: AgentId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly turn: number;
  readonly submissionId: SubmissionId;
  readonly attemptId: AttemptId;
  /** Originating input remains stable; current steering belongs to the assembled Prompt. */
  readonly input: unknown;
  /** Exact assembled inference Prompt, after context preparation and transient instructions. */
  readonly prompt: Prompt.Prompt;
}

export interface PreparedDecisionTurn<Error = never, Requirements = never> {
  readonly toolName: string;
  readonly model: DecisionTurnModel;
  readonly evaluate: Effect.Effect<
    {
      readonly result: DecisionTurnResult;
      readonly project: Effect.Effect<
        {
          readonly evidence: DecisionTurnEvidence;
          readonly call: Option.Option<{ readonly text: string; readonly parameters: Schema.Json }>;
        },
        DecisionTurnError,
        Requirements
      >;
    },
    Error | DecisionTurnError,
    Requirements
  >;
}

/** Installed only by a durable registration, with its concrete requirements captured. */
export const ConfiguredDecisionTurn = Context.Reference<
  | ((
      request: Omit<DecisionTurnRequest, "submissionId" | "attemptId">,
    ) => Effect.Effect<Option.Option<PreparedDecisionTurn>, DecisionTurnError>)
  | undefined
>("@effect-agent/engine/internal/ConfiguredDecisionTurn", { defaultValue: () => undefined });

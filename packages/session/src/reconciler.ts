import { ConversationId, RunId, SubmissionId, ToolCallId } from "@effect-agent/core";
import { Context, Effect, Layer, Schema } from "effect";

import { Digest, PersistedJson, ToolCallPrepared } from "./records.ts";

/**
 * Everything a reconciliation policy can consult about one prepared-but-unsettled ordinary Tool
 * Call (durability §10): deterministic identities for supplier-side lookup plus the exact
 * Schema-encoded parameters and their digest. Built from the canonical `ToolCallPrepared` record
 * and the owning Submission's identities — never from in-memory Attempt state.
 */
export class PreparedToolCallEvidence extends Schema.Class<PreparedToolCallEvidence>(
  "@effect-agent/session/PreparedToolCallEvidence",
)({
  conversationId: ConversationId,
  submissionId: SubmissionId,
  runId: RunId,
  turn: ToolCallPrepared.fields.turn,
  toolCallId: ToolCallId,
  toolName: ToolCallPrepared.fields.toolName,
  parameters: PersistedJson,
  parametersDigest: Digest,
}) {}

/** Proof that the external execution never started: the call may re-execute on resume. */
export class ReconciliationNeverStarted extends Schema.TaggedClass<ReconciliationNeverStarted>(
  "@effect-agent/session/ReconciliationNeverStarted",
)("NeverStarted", {}) {}

/**
 * Proof that the external execution completed: `result` is the recovered supplier truth, which
 * becomes canonical (`ToolCallSettled` + `ToolCallResolved`) WITHOUT executing anything.
 */
export class ReconciliationCompleted extends Schema.TaggedClass<ReconciliationCompleted>(
  "@effect-agent/session/ReconciliationCompleted",
)("CompletedWithResult", {
  result: PersistedJson,
  isFailure: Schema.Boolean,
}) {}

/** The call is safe to repeat under a stable external idempotency contract. */
export class ReconciliationSafeToRetry extends Schema.TaggedClass<ReconciliationSafeToRetry>(
  "@effect-agent/session/ReconciliationSafeToRetry",
)("SafeToRetry", {}) {}

/** No proof either way: the outcome is Unknown and requires authorized resolution (DUR-017). */
export class ReconciliationUncertain extends Schema.TaggedClass<ReconciliationUncertain>(
  "@effect-agent/session/ReconciliationUncertain",
)("Uncertain", {
  reason: Schema.String,
}) {}

/**
 * What a reconciliation policy can prove about one prepared-but-unsettled ordinary Tool Call
 * (durability §10): execution never started, execution completed with a recoverable result,
 * execution is safe to repeat, or nothing — in which case the Run enters Unknown. The engine
 * never manufactures an error result and continues.
 */
export const ReconciliationDecision = Schema.Union([
  ReconciliationNeverStarted,
  ReconciliationCompleted,
  ReconciliationSafeToRetry,
  ReconciliationUncertain,
]);
export type ReconciliationDecision = typeof ReconciliationDecision.Type;

/** The reconciliation policy itself failed (supplier unreachable, corrupt lookup, ...). The
 * caller treats this as no proof: the call stays open and the pass may retry. */
export class ToolReconcilerError extends Schema.TaggedErrorClass<ToolReconcilerError>()(
  "ToolReconcilerError",
  {
    toolCallId: ToolCallId,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * Application-registered reconciliation policy consulted by recovery for every open ordinary
 * Tool Call before an Unknown Outcome is recorded (durability §10, DUR-009). A decision is a
 * claim about EXTERNAL truth — typically an idempotency-keyed supplier lookup — never a guess
 * from canonical state alone.
 *
 * `ToolReconciler.uncertain` is the fail-closed default (AGENTS rule 11): with no registered
 * policy, every open call stays Unknown and routes to the authorized DUR-017 resolution path.
 */
export class ToolReconciler extends Context.Service<
  ToolReconciler,
  {
    readonly reconcile: (
      evidence: PreparedToolCallEvidence,
    ) => Effect.Effect<ReconciliationDecision, ToolReconcilerError>;
  }
>()("@effect-agent/session/ToolReconciler") {
  /** Fail-closed default: no proof is ever asserted, every open call stays Unknown. */
  static readonly uncertain: Layer.Layer<ToolReconciler> = Layer.succeed(this)({
    reconcile: () =>
      Effect.succeed(
        ReconciliationUncertain.make({
          reason: "No reconciliation policy is registered; the outcome stays unknown",
        }),
      ),
  });
}

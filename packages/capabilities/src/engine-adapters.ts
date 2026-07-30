import { Clock, DateTime, Effect, Schema } from "effect";
import type { Prompt } from "effect/unstable/ai";
import type {
  PreparedRunContext,
  RunApprovalHook,
  RunApprovalRequest,
  RunBudgetHook,
  RunContextHook,
  RunContextRequest,
  RunInputHook,
  RunOptions,
  RunSchedulingHook,
} from "@effect-agent/engine";

import {
  ApprovalAudit,
  type ApprovalAuditLimitExceeded,
  type ApprovalDecisionMismatch,
  ApprovalRequestDraft,
  type ApprovalResolver,
  type ApprovalResolverError,
  makeApprovalRequest,
  requestApproval,
} from "./approval.ts";
import { type BudgetExceeded, UsageDelta, type UsageBudgetNode } from "./budget.ts";
import type { RunCommandQueue } from "./commands.ts";
import {
  type ConversationEncodingError,
  type ConversationHistoryDiverged,
  type ConversationLimitExceeded,
  type ConversationNotFound,
  conversationPrompt,
  EphemeralConversations,
} from "./conversation.ts";
import type { RedactionError, Redactor } from "./redaction.ts";
import type { RunSchedulingOverride } from "./scheduling.ts";

/** Capability policy could not be normalized into the bounded approval request Schema. */
export class ApprovalAdapterError extends Schema.TaggedErrorClass<ApprovalAdapterError>()(
  "ApprovalAdapterError",
  { message: Schema.String },
) {}

/** Engine usage data did not satisfy the non-negative budget delta Schema. */
export class BudgetAdapterError extends Schema.TaggedErrorClass<BudgetAdapterError>()(
  "BudgetAdapterError",
  { message: Schema.String },
) {}

/** Adapt the richer audited FIFO commands to the engine's safe-seam input hook. */
export const toRunInputHook = (queue: RunCommandQueue): RunInputHook<never, never> => ({
  drain: (policy) =>
    queue.drain(policy).pipe(
      Effect.map((commands) =>
        commands.map((command) => ({
          kind: command._tag === "SteeringCommand" ? ("steering" as const) : ("follow-up" as const),
          input: command.content,
        })),
      ),
    ),
  end: () => queue.shutdown,
});

/** Explicit policy needed to turn a native Effect AI approval request into an audit request. */
export interface RunApprovalAdapterPolicy {
  readonly expiresInMillis: number;
  readonly risk: ApprovalRequestDraft["risk"];
  readonly denial: ApprovalRequestDraft["denial"];
  readonly actionSummary: (request: RunApprovalRequest) => string;
  readonly resourceTargets: (request: RunApprovalRequest) => ReadonlyArray<string>;
}

/** Adapt native Effect AI approval parts through structural redaction, audit, and timeout policy. */
export const toRunApprovalHook = (
  policy: RunApprovalAdapterPolicy,
): RunApprovalHook<
  | ApprovalResolverError
  | ApprovalAuditLimitExceeded
  | ApprovalDecisionMismatch
  | ApprovalAdapterError
  | RedactionError,
  ApprovalResolver | ApprovalAudit | Redactor
> => ({
  request: (engineRequest) =>
    Effect.gen(function* () {
      const validatedPolicy = yield* Schema.decodeUnknownEffect(RunApprovalAdapterPolicySchema)(
        policy,
      ).pipe(
        Effect.mapError((error) =>
          ApprovalAdapterError.make({
            message: `Approval adapter policy is invalid: ${error.message}`,
          }),
        ),
      );
      const now = yield* Clock.currentTimeMillis;
      const metadata = yield* Effect.try({
        try: () => ({
          actionSummary: policy.actionSummary(engineRequest),
          resourceTargets: policy.resourceTargets(engineRequest),
        }),
        catch: () =>
          ApprovalAdapterError.make({
            message: "Approval policy failed while describing the native Tool request",
          }),
      });
      const draft = yield* Schema.decodeUnknownEffect(ApprovalRequestDraft)({
        requestId: engineRequest.request.approvalId,
        runId: engineRequest.runId,
        conversationId: engineRequest.conversationId,
        toolCallId: engineRequest.toolCallId,
        toolName: engineRequest.toolName,
        actionSummary: metadata.actionSummary,
        resourceTargets: metadata.resourceTargets,
        risk: validatedPolicy.risk,
        expiresAt: DateTime.toUtc(DateTime.makeUnsafe(now + validatedPolicy.expiresInMillis)),
        denial: validatedPolicy.denial,
      }).pipe(
        Effect.mapError((error) =>
          ApprovalAdapterError.make({
            message: `Approval adapter policy is invalid: ${error.message}`,
          }),
        ),
      );
      const request = yield* makeApprovalRequest(draft, engineRequest.parameters);
      const decision = yield* requestApproval(request);
      if (decision._tag === "ApprovalApproved") {
        return { _tag: "approved" as const };
      }
      if (decision.timedOut && request.denial === "recoverable") {
        return {
          _tag: "unresolved" as const,
          reason: decision.reason,
        };
      }
      return {
        _tag: "denied" as const,
        reason: decision.reason,
      };
    }),
});

/** Adapt one hierarchical budget node to the engine's usage accounting seam. */
export const toRunBudgetHook = (
  budget: UsageBudgetNode,
): RunBudgetHook<BudgetExceeded | BudgetAdapterError, never> => ({
  guard: budget.guard,
  consume: (delta) =>
    Schema.decodeUnknownEffect(UsageDelta)({
      inputTokens: delta.inputTokens,
      outputTokens: delta.outputTokens,
      toolCalls: delta.toolCalls,
      costMicrousd: delta.costMicrousd,
    }).pipe(
      Effect.mapError((error) =>
        BudgetAdapterError.make({
          message: `Engine usage delta is invalid: ${error.message}`,
        }),
      ),
      Effect.flatMap((usage) => budget.consume(usage)),
      Effect.asVoid,
    ),
});

export type ConversationAdapterError =
  | ConversationNotFound
  | ConversationLimitExceeded
  | ConversationEncodingError
  | ConversationHistoryDiverged;

/**
 * Bind one process-local Conversation to the engine's exact history seams.
 * The initial Prompt and every onHistory update retain all native Effect AI
 * parts and provider options without a role/text projection.
 */
export const toRunConversationOptions = Effect.fn("toRunConversationOptions")(function* (
  conversations: EphemeralConversations["Service"],
  conversationId: import("@effect-agent/core").ConversationId,
  runId: import("@effect-agent/core").RunId,
): Effect.fn.Return<
  Pick<RunOptions<ConversationAdapterError>, "conversationId" | "history" | "onHistory">,
  ConversationNotFound
> {
  const snapshot = yield* conversations.snapshot(conversationId);
  return {
    conversationId,
    history: conversationPrompt(snapshot),
    onHistory: (history) =>
      conversations.recordHistory(conversationId, runId, history).pipe(Effect.asVoid),
  };
});

/** Prompt transform used by the adapter; the engine retains the authoritative source separately. */
export interface EnginePromptTransform<Error = never, Requirements = never> {
  readonly prepare: (
    source: Prompt.Prompt,
    request: RunContextRequest,
  ) => Effect.Effect<Prompt.Prompt, Error, Requirements>;
}

/** Adapt a prompt-only context transform without granting it authority over engine source history. */
export const toRunContextHook = <Error, Requirements>(
  transform: EnginePromptTransform<Error, Requirements>,
): RunContextHook<Error, Requirements> => ({
  prepare: (request): Effect.Effect<PreparedRunContext, Error, Requirements> =>
    transform.prepare(request.source, request).pipe(Effect.map((prompt) => ({ prompt }))),
});

/** Scheduling values are structurally aligned and only reduce finite concurrency. */
export const toRunSchedulingHook = (
  runOverride: RunSchedulingOverride | undefined,
  toolRequiresSequential?: (toolName: string) => boolean,
): RunSchedulingHook => ({
  runOverride,
  ...(toolRequiresSequential === undefined ? {} : { toolRequiresSequential }),
});

/** Validate policy configuration at adapter construction boundaries. */
export const RunApprovalAdapterPolicySchema = Schema.Struct({
  expiresInMillis: Schema.Int.check(Schema.isGreaterThan(0)),
  risk: Schema.Literals(["low", "medium", "high", "critical"]),
  denial: Schema.Literals(["terminal", "recoverable"]),
});

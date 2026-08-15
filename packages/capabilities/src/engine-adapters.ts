import type {
  PreparedRunContext,
  RunApprovalDecision,
  RunApprovalHook,
  RunApprovalRequest,
  RunBudgetHook,
  RunContextHook,
  RunContextRequest,
  RunInputHook,
  RunOptions,
  RunSchedulingHook,
} from "@effect-agent/engine";
import { Clock, DateTime, Effect, Schema } from "effect";
import type { Prompt } from "effect/unstable/ai";

import {
  type ApprovalAudit,
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
  type EphemeralConversations,
} from "./conversation.ts";
import type { RedactionError, Redactor } from "./redaction.ts";
import type { RunSchedulingOverride } from "./scheduling.ts";

/** Capability policy could not be normalized into the bounded approval request Schema. */
export class ApprovalAdapterError extends Schema.TaggedError<ApprovalAdapterError>()(
  "ApprovalAdapterError",
  { message: Schema.String },
) {}

/** Engine usage data did not satisfy the non-negative budget delta Schema. */
export class BudgetAdapterError extends Schema.TaggedError<BudgetAdapterError>()(
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
        expiresAt: DateTime.formatIso(
          DateTime.toUtc(DateTime.makeUnsafe(now + validatedPolicy.expiresInMillis)),
        ),
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

/**
 * Durable variant of `toRunApprovalHook` (P5 plan §2.6): the durable coordinator consults this
 * delegate for policy-AUTO decisions only, after its recorded-decision lookup misses. It reuses
 * the exact P2 approval stack — `ApprovalRequestDraft` policy metadata, structural redaction,
 * audit sink, expiry/timeout policy — but differs in two durable-specific ways:
 *
 * 1. The capability services (`ApprovalResolver | ApprovalAudit | Redactor`) are captured up
 *    front, so the returned hook is `RunApprovalHook<never, never>` — the shape the
 *    coordinator's `DurableApprovalResolver` reference accepts without leaking capability
 *    requirements into the durable runtime Layer.
 * 2. It FAILS CLOSED into `unresolved`: any adapter, audit, redaction, or resolver failure
 *    defers the decision to the durable suspension + `resolveApproval` path instead of
 *    approving, denying, or crashing the Attempt on a transient policy fault. Explicit
 *    policy denials (including the P2 timeout-denial for `denial: "terminal"`) still deny.
 */
export const toDurableRunApprovalHook = Effect.fn("toDurableRunApprovalHook")(function* (
  policy: RunApprovalAdapterPolicy,
): Effect.fn.Return<
  RunApprovalHook<never, never>,
  never,
  ApprovalResolver | ApprovalAudit | Redactor
> {
  const services = yield* Effect.context<ApprovalResolver | ApprovalAudit | Redactor>();
  const hook = toRunApprovalHook(policy);
  return {
    request: (request) =>
      hook.request(request).pipe(
        Effect.provideContext(services),
        Effect.catch((error) =>
          Effect.succeed<RunApprovalDecision>({
            _tag: "unresolved",
            reason: `Approval delegation failed (${error._tag}); the decision defers to the durable resolveApproval path`,
          }),
        ),
      ),
  };
});

/** Adapt one hierarchical budget node to the engine's usage accounting seam. */
export const toRunBudgetHook = (
  budget: UsageBudgetNode,
): RunBudgetHook<BudgetExceeded | BudgetAdapterError, never> => ({
  guard: budget.guard,
  consume: (delta) =>
    Schema.decodeUnknownEffect(UsageDelta)({
      modelCalls: delta.modelCalls,
      inputTokens: delta.inputTokens,
      outputTokens: delta.outputTokens,
      cacheReadInputTokens: Math.max(0, delta.usage.inputTokens.cacheRead ?? 0),
      cacheWriteInputTokens: Math.max(0, delta.usage.inputTokens.cacheWrite ?? 0),
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

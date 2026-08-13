import type { Effect } from "effect";
import type { Prompt, Response } from "effect/unstable/ai";

import type { ConversationId, RunId, ToolCallId, TurnId } from "@effect-agent/core";

/** Number of queued inputs consumed at one documented Turn seam. */
export type CommandDrainPolicy = "one" | "all";

/** Engine-normalized input command. Capability packages may adapt richer audit records to it. */
export interface RunInputCommand {
  readonly kind: "steering" | "follow-up";
  readonly input: Prompt.RawInput;
}

/**
 * Dependency-neutral input seam used by the interpreter.
 *
 * A capability adapter owns queue bounds and Scope finalization. The engine calls
 * `start` before the first drain, `drain` only at safe Turn seams, and `end`
 * exactly once when the Run leaves its Scope.
 */
export interface RunInputHook<Error = never, Requirements = never> {
  readonly start?: (() => Effect.Effect<void, Error, Requirements>) | undefined;
  readonly drain: (
    policy: CommandDrainPolicy,
  ) => Effect.Effect<ReadonlyArray<RunInputCommand>, Error, Requirements>;
  readonly end?: (() => Effect.Effect<void, never, Requirements>) | undefined;
}

/** Decision returned by an ephemeral approval adapter. */
export type RunApprovalDecision =
  | {
      readonly _tag: "approved";
      readonly reason?: string | undefined;
    }
  | {
      readonly _tag: "denied";
      readonly reason?: string | undefined;
    }
  | {
      readonly _tag: "unresolved";
      readonly reason?: string | undefined;
    };

/** Native Effect AI approval request enriched with stable Run identities. */
export interface RunApprovalRequest {
  readonly request: Response.ToolApprovalRequestPart;
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly toolCallId: ToolCallId;
  readonly toolName: string;
  readonly parameters: unknown;
}

/** Dependency-neutral approval decision hook. */
export interface RunApprovalHook<Error = never, Requirements = never> {
  readonly request: (
    request: RunApprovalRequest,
  ) => Effect.Effect<RunApprovalDecision, Error, Requirements>;
}

/** Input passed to ordered context transformation and compaction adapters. */
export interface RunContextRequest {
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly turn: number;
  /** Official ephemeral history. The engine never replaces or mutates this value. */
  readonly source: Prompt.Prompt;
}

/** Prepared model-only context returned by a context adapter. */
export interface PreparedRunContext {
  readonly prompt: Prompt.Prompt;
}

/** Dependency-neutral ordered context transformation / compaction hook. */
export interface RunContextHook<Error = never, Requirements = never> {
  readonly prepare: (
    request: RunContextRequest,
  ) => Effect.Effect<PreparedRunContext, Error, Requirements>;
}

/** One model-call usage delta consumed after a complete response and before any Tool starts. */
export interface RunUsageDelta {
  readonly modelCalls: 1;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly totalTokens: number;
  readonly toolCalls: number;
  readonly costMicrousd: number;
  readonly usage: Response.Usage;
}

/** Dependency-neutral hierarchical budget hook. A typed failure stops the Run. */
export interface RunBudgetHook<Error = never, Requirements = never> {
  /**
   * Guard one active model or Tool stream pull. Hierarchical budget adapters
   * use an absolute deadline so repeated pulls share one duration allowance.
   */
  readonly guard: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | Error, R | Requirements>;
  readonly consume: (delta: RunUsageDelta) => Effect.Effect<void, Error, Requirements>;
}

/** Run-level scheduler override; it may only make the Agent's finite bound stricter. */
export type RunSchedulingOverride =
  | {
      readonly mode: "bounded";
      readonly concurrency: number;
    }
  | {
      readonly mode: "sequential";
    };

/** Dependency-neutral scheduling policy surrounding native Effect AI Tool handlers. */
export interface RunSchedulingHook {
  readonly runOverride?: RunSchedulingOverride | undefined;
  readonly toolRequiresSequential?: ((toolName: string) => boolean) | undefined;
}

/**
 * Optional Phase 2 seams. Hook failures and requirements stay visible in the
 * returned Stream / Effect through the generic parameters.
 */
export interface RunOptions<HookError = never, HookRequirements = never> {
  /** Reuse an existing ephemeral Conversation identity instead of allocating one. */
  readonly conversationId?: ConversationId | undefined;
  /**
   * Official prior history. The engine preserves it as the exact initial
   * prefix, then appends this Run's evaluated instructions and decoded input.
   * Context preparation never mutates this source.
   */
  readonly history?: Prompt.Prompt | undefined;
  readonly commandDrainPolicy?: CommandDrainPolicy | undefined;
  readonly input?: RunInputHook<HookError, HookRequirements> | undefined;
  readonly approval?: RunApprovalHook<HookError, HookRequirements> | undefined;
  readonly context?: RunContextHook<HookError, HookRequirements> | undefined;
  readonly budget?: RunBudgetHook<HookError, HookRequirements> | undefined;
  /** Required when the core policy declares `costBudgetMicrousd`. */
  readonly estimateCostMicrousd?:
    | ((usage: Response.Usage) => Effect.Effect<number, HookError, HookRequirements>)
    | undefined;
  readonly scheduling?: RunSchedulingHook | undefined;
  /** Internal/public observation seam invoked whenever official history advances. */
  readonly onHistory?:
    | ((history: Prompt.Prompt) => Effect.Effect<void, HookError, HookRequirements>)
    | undefined;
}

export type RunOptionsError<Options> =
  Options extends RunOptions<infer Error, infer _Requirements> ? Error : never;

export type RunOptionsRequirements<Options> =
  Options extends RunOptions<infer _Error, infer Requirements> ? Requirements : never;

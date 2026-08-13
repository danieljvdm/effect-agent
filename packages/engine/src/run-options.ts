import type { Effect } from "effect";
import type { Prompt, Response } from "effect/unstable/ai";

import type {
  ConversationId,
  RunId,
  SubagentParentLink,
  ToolCallId,
  TurnId,
} from "@effect-agent/core";

import type { RunStepHook, ToolExecutionClassValue } from "./durable-step.ts";

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

/**
 * One application Tool Call of a completed Turn as the durable runtime sees
 * it: stable identity, the encoded (wire-form) parameters exactly as official
 * history carries them, and the Tool's declared execution class (fail-closed
 * `"uncertain"` for unannotated Tools).
 */
export interface RunToolCallDescriptor {
  readonly toolCallId: ToolCallId;
  readonly toolName: string;
  /** Encoded JSON parameters — the same value official history and canonical records carry. */
  readonly parameters: unknown;
  readonly executionClass: ToolExecutionClassValue;
}

/**
 * Payload of one durable response commit: the completed Turn's identity, its
 * response messages in official (encoded) form, and every application Tool
 * Call it declared. The engine invokes it only for Turns that declare
 * application Tool Calls; no-tool Turns keep their late single-batch commit.
 */
export interface RunTurnResponseCommit {
  readonly turn: number;
  readonly turnId: TurnId;
  /** The Turn's response messages in official history form (encoded Tool parameters). */
  readonly responseMessages: Prompt.Prompt;
  readonly calls: ReadonlyArray<RunToolCallDescriptor>;
}

/**
 * Dependency-neutral durability seam implemented by a durable coordinator.
 *
 * Invocation ordering inside one Tool-declaring Turn is normative:
 * `commitResponse` fires after the finish part's continuation validations and
 * before approval preflight (making the response canonical before any Tool
 * work — the provably-safe resume window); `prepareToolCalls` fires after
 * every approval resolved approved and before any handler acquires a
 * scheduler permit, with the non-`readonly` calls of the batch (it is skipped
 * entirely when no call needs preparation); `step` persists Durable Step
 * results mid-flight. When the hook is absent the engine behaves exactly as
 * the ephemeral runtime always has.
 */
export interface RunDurabilityHook<Error = never, Requirements = never> {
  /** After the finish part's validations, before approval preflight. */
  readonly commitResponse: (
    commit: RunTurnResponseCommit,
  ) => Effect.Effect<void, Error, Requirements>;
  /** After every approval resolved approved, before any handler starts. */
  readonly prepareToolCalls: (
    calls: ReadonlyArray<RunToolCallDescriptor>,
  ) => Effect.Effect<void, Error, Requirements>;
  readonly step: RunStepHook<Error, Requirements>;
}

/** One declared Tool Call of a Turn being resumed, in canonical encoded form. */
export interface RunTurnResumeCall {
  readonly id: string;
  readonly name: string;
  /** Canonical encoded parameters; re-validated through the Tool's parameter Schema before anything executes. */
  readonly params: unknown;
}

/** One already-settled Tool Call of a Turn being resumed; injected without execution. */
export interface RunTurnResumeSettledCall {
  readonly id: string;
  /** The recorded encoded result, exactly as the Tool message will carry it. */
  readonly result: unknown;
  readonly isFailure: boolean;
}

/**
 * Resume one canonically declared Tool batch without re-invoking the model.
 *
 * When present, the engine's first Turn skips the model request entirely: the
 * declared calls are re-validated through their Tool parameter Schemas (a
 * decode failure executes nothing), approval preflight runs against recorded
 * decisions, `prepareToolCalls` replays the full prepared batch idempotently,
 * calls listed in `settled` are injected as final results without starting
 * their handlers, and only the remaining open calls execute. The Run then
 * proceeds through the normal continuation.
 */
export interface RunTurnResume {
  readonly turn: number;
  readonly turnId: TurnId;
  readonly calls: ReadonlyArray<RunTurnResumeCall>;
  readonly settled: ReadonlyArray<RunTurnResumeSettledCall>;
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
   * Preallocated Run identity used instead of `IdGenerator` when supplied.
   * The S1 Subagent seam preallocates child Run identity through this option
   * so `SubagentRequested` can carry the intended child identity.
   */
  readonly runId?: RunId | undefined;
  /**
   * Non-model-visible Parent Link for a delegated child Run (S1 seam). It
   * never enters the model prompt or the Run's event payloads directly; the
   * engine uses it only to fix the Run's delegation depth (`parentLink.depth`
   * for a child, `0` when absent) exposed through the locally provided
   * `AgentSpawner`, and future durable work persists it as child lineage.
   */
  readonly parentLink?: SubagentParentLink | undefined;
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
  /**
   * Durable turn-commit seam (P5). When absent the engine behaves exactly as
   * the ephemeral runtime: no response/prepared commits, and `DurableStep`
   * executes pass-through.
   */
  readonly durability?: RunDurabilityHook<HookError, HookRequirements> | undefined;
  /**
   * Resume a declared, canonically committed Tool batch without re-invoking
   * the model (durable batch-resume seam). Consumed by the Run's first Turn.
   */
  readonly resume?: RunTurnResume | undefined;
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

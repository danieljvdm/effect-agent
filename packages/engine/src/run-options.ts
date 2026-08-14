import type {
  AgentId,
  ConversationId,
  DelegationDepth,
  DelegationId,
  ReceiptId,
  RunId,
  SubagentParentLink,
  SubmissionId,
  ToolCallId,
  TurnId,
} from "@effect-agent/core";
import type { Effect } from "effect";
import type { Prompt, Response } from "effect/unstable/ai";

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

/**
 * `DefinitionDigests`-shaped digests of one child Agent Binding in plain
 * string form. The durable coordinator's session-owned digest Schema never
 * crosses inward: the delegation capability computes these exact digests at
 * handler-Layer construction and the coordinator stores and verifies them
 * byte-for-byte (SUB-023 exact-digest binding resolution).
 */
export interface RunSubagentDigests {
  readonly agent: string;
  readonly model: string;
  readonly tools: string;
}

/**
 * Establishment request assembled by a delegation Tool handler for the
 * durable coordinator (spec/subagents.md §12 steps 2-9), expressed strictly
 * in core/engine vocabulary. The coordinator derives everything else
 * deterministically: reservation identity from `(parentRunId, toolCallId)`,
 * child Conversation/Submission identity and the admission idempotency key
 * from the parent Run and Tool Call pair, the child principal from the parent
 * Submission, and digests of the encoded input/grant/allocation values (it
 * owns the canonical digest authority). Establishment is idempotent by
 * construction (SUB-016): replaying the identical request converges on the
 * one existing child.
 */
export interface RunSubagentEstablishRequest {
  readonly toolCallId: ToolCallId;
  readonly delegationId: DelegationId;
  readonly targetAgentId: AgentId;
  /** The child Run's root-relative delegation depth (S2 fixes the ceiling at 1). */
  readonly depth: DelegationDepth;
  /** Exact child Binding digests computed at handler-Layer construction. */
  readonly targetDigests: RunSubagentDigests;
  /** The prepared child input in encoded (wire) form; it rides the canonical request record so recovery admission never needs a live handler. */
  readonly encodedChildInput: unknown;
  /** The delegation's authority ceiling in encoded form. */
  readonly encodedGrant: unknown;
  /** The per-invocation budget allocation in encoded form (opaque to the engine and to storage adapters). */
  readonly encodedAllocation: unknown;
}

/** Durable identity of one established attached child, in core vocabulary. */
export interface RunSubagentChildIdentity {
  readonly childConversationId: ConversationId;
  readonly childSubmissionId: SubmissionId;
  readonly childRunId: RunId;
  /** The child Receipt: establishment is never durable-visible before it exists (SUB-017). */
  readonly receiptId: ReceiptId;
}

/** The established child has not settled; the parent must suspend, never poll or respawn (SUB-018). */
export interface ChildEstablishWaiting extends RunSubagentChildIdentity {
  readonly _tag: "waiting";
}

/**
 * The established child already has a verified canonical Settlement. The
 * coordinator verified Parent Link, target, digests, and settlement identity
 * fail-closed before returning it; `encodedResult` is the Schema-encoded
 * child terminal output for `completed` and the coordinator's bounded
 * `{errorTag, message}` failure projection otherwise — never a raw Cause.
 */
export interface ChildEstablishSettled extends RunSubagentChildIdentity {
  readonly _tag: "settled";
  readonly outcome: "completed" | "failed" | "aborted";
  readonly encodedResult: unknown;
}

/**
 * Establishment was refused or failed verification fail-closed (typed, bounded;
 * never a raw Cause). No further establishment attempt is made for this call.
 */
export interface ChildEstablishDenied {
  readonly _tag: "denied";
  readonly errorTag: string;
  readonly message: string;
}

/** Result of one idempotent durable child establishment attempt. */
export type ChildEstablishStatus =
  | ChildEstablishWaiting
  | ChildEstablishSettled
  | ChildEstablishDenied;

/**
 * One settlement join handed back to the coordinator by the delegation
 * handler after it decoded the verified child output and applied its bounded
 * result/failure projection. The coordinator appends `SubagentJoined` and the
 * parent `ToolCallSettled` as ONE atomic canonical batch (SUB-019) and then
 * applies the accounting decision through the idempotent reservation-release
 * transitions.
 */
export interface RunSubagentJoinRequest {
  readonly toolCallId: ToolCallId;
  /** The encoded parent Tool result exactly as the Tool message will carry it. */
  readonly encodedResult: unknown;
  readonly isFailure: boolean;
  /** Final consumed/released accounting decision per budget dimension (opaque encoded policy math owned by the delegation capability). */
  readonly encodedAccounting: unknown;
}

/**
 * Dependency-neutral durable-Subagent seam implemented by a durable
 * coordinator (S2 plan §2 option c) and consumed by the delegation Tool
 * handler through the engine-provided per-batch `SubagentDurability` service.
 *
 * `establish` performs (or replays) the recoverable establishment protocol of
 * spec/subagents.md §12 under the parent's ownership fence and reports where
 * the one child stands; `join` atomically commits the verified settlement
 * join. When the hook is absent the engine provides the explicit
 * ephemeral-mode service and the S1 in-process spawn semantics apply honestly
 * — absence means no durable claim is being made.
 */
export interface RunSubagentHook<Error = never, Requirements = never> {
  readonly establish: (
    request: RunSubagentEstablishRequest,
  ) => Effect.Effect<ChildEstablishStatus, Error, Requirements>;
  readonly join: (request: RunSubagentJoinRequest) => Effect.Effect<void, Error, Requirements>;
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
  /**
   * The pending Turn's committed LEADING messages — the messages the durable
   * coordinator committed inside the pending Turn's canonical response record
   * BEFORE the assistant tool-call message (Turn-1 evaluated instructions +
   * input, or steering drained at the prior seam). A resumed Attempt's
   * canonical prompt boundary excludes the pending Turn entirely, so without
   * this field those messages would be absent from the resumed Run's live
   * model context. When present the engine threads them into official history
   * between the re-evaluated initial prompt and the rebuilt assistant
   * tool-call message. Optional: absent keeps the prior behavior byte-for-byte.
   */
  readonly leadingMessages?: Prompt.Prompt | undefined;
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
   * Durable-Subagent establishment/join seam (S2). When present the engine's
   * per-batch `SubagentDurability` service runs in durable mode over this
   * hook; when absent the service is the explicit ephemeral-mode default and
   * delegation Tools keep their S1 in-process spawn semantics unchanged.
   */
  readonly subagent?: RunSubagentHook<HookError, HookRequirements> | undefined;
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

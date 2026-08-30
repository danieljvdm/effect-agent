import type {
  AgentId,
  ConversationId,
  DelegationDepth,
  DelegationId,
  ReceiptId,
  RunId,
  ModelCallUsage,
  SubagentParentLink,
  SubmissionId,
  ToolCallId,
  ToolExecutionKind,
  TurnId,
} from "@effect-agent/core";
import { RunPolicyUsage } from "@effect-agent/core";
import { type Cause, type Effect, Context, type DateTime, Layer, Schema } from "effect";
import type { Prompt, Response } from "effect/unstable/ai";

import type { ContextCompactor } from "./context-compactor.ts";
import type { RunStepHook, ToolExecutionClassValue } from "./durable-step.ts";

/** Live, trusted application diagnostics. Never persisted, transported, or automatically logged. */
interface ToolFailureIdentity {
  readonly agentId: AgentId;
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly toolName: string;
  /** Best-effort error tag, with the existing `UnknownError` fallback. */
  readonly tag: string;
}

/** A model-declared Handler returned a declared failure instead of failing the Run. */
export interface ModelToolFailure extends ToolFailureIdentity {
  readonly _tag: "ModelToolFailure";
  readonly kind: "declared-failure";
  /** Raw provider identity, without the telemetry ID filter. */
  readonly toolCallId: ToolCallId;
  readonly executionClass: ToolExecutionClassValue;
  readonly message?: never;
  readonly cause?: never;
}

interface ProgrammaticToolFailureIdentity extends ToolFailureIdentity {
  readonly _tag: "ProgrammaticToolFailure";
  /** `${parentToolCallId}#${sequenceIndex}`, raw and unique only within this in-memory pass. */
  readonly toolCallId: string;
  readonly parentToolCallId: ToolCallId;
  /** Presence means the Handler started and consumed budget; side effects may exist. */
  readonly sequenceIndex: number;
  readonly executionClass: ToolExecutionClassValue;
}

interface ProgrammaticDeclaredFailure extends ProgrammaticToolFailureIdentity {
  readonly kind: "declared-failure";
  readonly message?: never;
  readonly cause?: never;
}

interface ProgrammaticHandlerFailure extends ProgrammaticToolFailureIdentity {
  readonly kind: "handler-error";
  readonly message?: never;
  /** The original, uncollapsed Cause captured before the broker's diagnostic projection. */
  readonly cause: Cause.Cause<unknown>;
}

interface ProgrammaticDiagnosticFailure extends ProgrammaticToolFailureIdentity {
  readonly kind: "infrastructure" | "protocol";
  /** At most 4096 UTF-8 bytes. Never a declared payload. */
  readonly message: string;
  /** Original Cause when one exists; never fabricated from a source-less rejection. */
  readonly cause?: Cause.Cause<unknown> | undefined;
}

/** A programmatic Handler started and its failure became a broker outcome. */
export type ProgrammaticToolFailure =
  | ProgrammaticDeclaredFailure
  | ProgrammaticHandlerFailure
  | ProgrammaticDiagnosticFailure;

/** A programmatic invocation was rejected before its Handler started. No inner identity exists. */
export interface ProgrammaticPreflightFailure extends ToolFailureIdentity {
  readonly _tag: "ProgrammaticPreflightFailure";
  readonly kind: "infrastructure" | "protocol";
  readonly parentToolCallId: ToolCallId;
  /** Absent if the Tool could not be resolved. */
  readonly executionClass?: ToolExecutionClassValue | undefined;
  /** At most 4096 UTF-8 bytes. */
  readonly message: string;
  /** Original Cause only for Cause-backed rejection, never fabricated from an outcome. */
  readonly cause?: Cause.Cause<unknown> | undefined;
}

/** Plain readonly interfaces, intentionally not persisted or transported Schemas (RUN-036). */
export type ToolFailureObservation =
  | ModelToolFailure
  | ProgrammaticToolFailure
  | ProgrammaticPreflightFailure;

/**
 * Trusted in-process observation of non-propagating application Tool failures (RUN-036).
 * Capture reporting dependencies before installation. Delivery is inline under the existing
 * Tool permit for started calls; preflight reporting is serialized per broker. Delivery is at
 * most once per in-memory attempt, with isolated observer/reporter defects.
 * External interruption may end delivery. Replacement Attempts may repeat IDs and observations.
 * Never reenter ToolBroker, RunEventSink, or Agent execution, or intentionally self-interrupt.
 */
export interface RunToolFailureObserver {
  readonly observe: (observation: ToolFailureObservation) => Effect.Effect<void>;
}

/** Resolved once per Run; durable coordinators capture it at Layer acquisition. Default absent. */
export const CurrentToolFailureObserver = Context.Reference<RunToolFailureObserver | undefined>(
  "@effect-agent/engine/CurrentToolFailureObserver",
  { defaultValue: () => undefined },
);

/** The sole installation seam, shared by ephemeral Runs and durable platform options. */
export const toolFailureObserverLayer = (observer: RunToolFailureObserver): Layer.Layer<never> =>
  Layer.succeed(CurrentToolFailureObserver)(observer);

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
  /**
   * The exact model-visible final-output contract the engine appends to the
   * prepared prompt after this hook returns (RUN-028), or
   * undefined when the definition's output Schema cannot render to JSON
   * Schema. Exposed so a limit-targeting adapter can reserve the contract's
   * overhead in its own window calculation; the hook can size for the
   * contract but cannot remove or alter it.
   */
  readonly outputContract?: string | undefined;
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

/** A host-supplied model-context preparer failed in its closed, expected error channel. */
export class RunContextPreparationError extends Schema.TaggedError<RunContextPreparationError>()(
  "RunContextPreparationError",
  {
    preparerId: Schema.NonEmptyString,
    message: Schema.String.check(Schema.isMaxLength(4_096)),
    /** Diagnostic cause for the live Effect only; durable hosts persist the bounded projection. */
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * One usage delta. Turn-boundary consumption charges `modelCalls: 1` after a
 * complete response and before any Tool starts; the programmatic Tool broker
 * charges `modelCalls: 0, toolCalls: 1` before each inner handler starts
 * (RUN-017).
 */
export interface RunUsageDelta {
  readonly modelCalls: 0 | 1;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly totalTokens: number;
  readonly toolCalls: number;
  readonly costMicrousd: number;
  readonly usage: Response.Usage;
  /** Present for model-call deltas; absent for programmatic Tool-only charges. */
  readonly modelUsage?: ModelCallUsage | undefined;
}

/** Stable pricing identity returned alongside a host's microdollar estimate. */
export interface RunCostEstimate {
  readonly costMicrousd: number;
  readonly serviceTier?: string | undefined;
  readonly pricingVersion?: string | undefined;
}

/** A number preserves the original estimator API; the object form adds pricing provenance. */
export type RunCostEstimateValue = number | RunCostEstimate;

/** Model identity presented beside the legacy raw-usage estimator argument. */
export interface RunCostEstimateRequest {
  readonly provider: string;
  readonly model: string;
  readonly usage: Response.Usage;
}

/**
 * Host-owned price lookup; durable hosts close every dependency before installing it. Raw usage
 * remains the first argument for source and runtime compatibility with the original estimator API.
 */
export type RunCostEstimator<Error = never, Requirements = never> = (
  usage: Response.Usage,
  request: RunCostEstimateRequest,
) => Effect.Effect<RunCostEstimateValue, Error, Requirements>;

/**
 * Dependency-neutral hierarchical budget hook. A typed failure at a Turn-seam
 * consumption or a stream-guard pull stops the Run. A mid-pass programmatic
 * consumption failure instead becomes that call's outcome (RUN-017 —
 * exhaustion prevents the call): the Run still stops at the next Turn seam,
 * because the following model call's `consume` and the guarded stream pulls
 * re-enforce the same budget.
 */
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
  /** Definition-owned classification; never inferred from the Tool name. */
  readonly executionKind: ToolExecutionKind;
}

/** Decision returned by a host's action-time Tool authorization policy. */
export type RunToolAuthorizationDecision =
  | { readonly _tag: "allowed" }
  | { readonly _tag: "denied"; readonly reason: string };

/**
 * Exact authority presented before one model-declared application Tool Handler may start.
 *
 * `input` is the Agent Schema's encoded Run input. Durable coordinators replace it with the exact
 * canonical Submission input admitted for the logical Run on every Attempt.
 * `call` is the exact still-executable call being authorized. Recorded settled calls are never
 * reauthorized because no Handler can start for them.
 */
export interface RunToolAuthorizationRequest {
  readonly conversationId: ConversationId;
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly turn: number;
  readonly input: unknown;
  readonly call: RunToolCallDescriptor;
}

/** Host policy invoked for each still-executable model-declared call in an application batch. */
export interface RunToolAuthorizationHook<Error = never, Requirements = never> {
  readonly authorize: (
    request: RunToolAuthorizationRequest,
  ) => Effect.Effect<RunToolAuthorizationDecision, Error, Requirements>;
}

/**
 * Generic host-owned extensions for one Run's model context and action-time Tool authority.
 *
 * The service is intentionally narrower than a Conversation store. Durable coordinators capture
 * it once while their runtime Layer is acquired. When absent, compatible assemblies preserve the
 * existing pass-through behavior.
 */
export class RunContextPreparation extends Context.Service<
  RunContextPreparation,
  {
    /** Optional transformation of the model-visible prompt. */
    readonly hook?: RunContextHook<RunContextPreparationError, never> | undefined;
    /** Replaces native compaction after prompt reconstruction; acquired with the host Layer. */
    readonly compactor?: ContextCompactor["Service"] | undefined;
    /** Optional action-time Tool authorization, closed over all host dependencies. */
    readonly toolAuthorization?: RunToolAuthorizationHook<never, never> | undefined;
  }
>()("@effect-agent/engine/RunContextPreparation") {}

/** Explicit no-preparer/no-authorizer Layer used by compatible runtime assemblies. */
export const RunContextPreparationPassthrough: Layer.Layer<RunContextPreparation> = Layer.succeed(
  RunContextPreparation,
)({});

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
 * One compaction decision the engine applied to its model-visible view
 * (RUN-026). The durable coordinator maps the covered source prefix to complete prior-Run
 * records. It must never infer a wider cutoff from policy or token estimates.
 */
export interface RunCompactionCommit {
  readonly turn: number;
  /** Exact pre-compaction source and exclusive message bound; live values, never persisted. */
  readonly source: Prompt.Prompt;
  readonly through: number;
  readonly kind: "clear-tool-results" | "summarize";
  /** Present exactly when `kind` is `"summarize"`. */
  readonly summary?: string | undefined;
  readonly tokensBeforeEstimate: number;
  readonly tokensAfterEstimate: number;
}

/** One completed model call's provider-reported usage, staged for the Turn's canonical commit. */
export interface RunTurnUsage {
  readonly turn: number;
  readonly usage: ModelCallUsage;
}

/**
 * Dependency-neutral durability seam implemented by a durable coordinator.
 *
 * Invocation ordering inside one Tool-declaring Turn is normative:
 * `commitResponse` fires after the finish part's continuation validations and staged canonical
 * provider/Turn events have been emitted, but before approval preflight (making the response
 * canonical before any Tool work — the provably-safe resume window); `prepareToolCalls` fires after
 * every approval and host authorization resolved allowed and before any handler acquires a
 * scheduler permit, with all delegations and non-`readonly` ordinary calls (it is skipped
 * entirely when no call needs preparation); `step` persists Durable Step
 * results mid-flight. When the hook is absent the engine behaves exactly as
 * the ephemeral runtime always has.
 */
export interface RunDurabilityHook<Error = never, Requirements = never> {
  /**
   * Reserve cumulative programmatic calls and grace finalization before external execution.
   * Calls are serialized across the Run. A committed reservation
   * is never refunded, even if ownership is lost before the Handler or model starts.
   * Coordinators must append fenced, schema-backed records before returning.
   */
  readonly reservePolicyUsage?:
    | ((
        usage: Pick<RunPolicyUsage, "programmaticToolCalls" | "finalizationUsed">,
      ) => Effect.Effect<void, Error, Requirements>)
    | undefined;
  /** After validated staged response events are emitted, before approval preflight. */
  readonly commitResponse: (
    commit: RunTurnResponseCommit,
  ) => Effect.Effect<void, Error, Requirements>;
  /** After every approval and host authorization resolved allowed, before any handler starts. */
  readonly prepareToolCalls: (
    calls: ReadonlyArray<RunToolCallDescriptor>,
  ) => Effect.Effect<void, Error, Requirements>;
  readonly step: RunStepHook<Error, Requirements>;
  /**
   * RUN-026: called at the pre-Turn seam BEFORE the engine applies a
   * compaction to its model-visible view or starts the model call whose
   * prompt reflects it, so a crash between the two resumes onto the compacted
   * projection. Required by the durability protocol: a coordinator that
   * silently dropped the record would let the engine use a compacted prompt
   * that recovery cannot reproduce. Reject unpersistable decisions in the typed
   * error channel; success means the same summary and coverage are durable.
   */
  readonly commitCompaction: (
    commit: RunCompactionCommit,
  ) => Effect.Effect<void, Error, Requirements>;
  /**
   * RUN-023: stage one completed model call's usage for the Turn's canonical
   * commit (the response record carries it for resume re-seeding). Staging is
   * not itself a durable mutation, but the member is required by the
   * durability protocol: dropping it writes response records without the
   * usage a later Attempt needs, so ownership changes would silently reset
   * token budgets instead of failing closed.
   */
  readonly noteTurnUsage: (usage: RunTurnUsage) => Effect.Effect<void, Error, Requirements>;
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
 * durable coordinator, expressed strictly
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
  /**
   * Present when the child's durable Settlement carries the honest
   * exhaustion marker (RUN-011/RUN-018): the child completed through the
   * final-answer resolution, so its output is a budget-truncated partial.
   */
  readonly finishReason?: "budget-exhausted" | undefined;
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
 * `establish` performs (or replays) child establishment
 * under the parent's ownership fence and reports where
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
  /** Provider calls must have a canonical settled result and are never dispatched locally. */
  readonly providerExecuted?: boolean;
}

/**
 * One already-settled Tool Call of a Turn being resumed; injected without execution.
 *
 * The engine decodes this Schema at the recovery boundary before the value can
 * enter official history. The result remains the exact canonical JSON value
 * carried by the Tool message.
 */
export const RunTurnResumeSettledCallSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  result: Schema.Json,
  isFailure: Schema.Boolean,
  /** Engine-owned rejection evidence; never inferred from the encoded result. */
  budgetRejected: Schema.optionalKey(Schema.Literal(true)),
});

export type RunTurnResumeSettledCall = typeof RunTurnResumeSettledCallSchema.Type;

/**
 * Canonical cumulative usage and Stop Policy accounting restored from prior Attempts.
 * Turns and declared Tool calls include a pending resumed batch. The failure
 * streak excludes that entire batch, whose terminal outcomes the engine folds once.
 * Programmatic reservations include pending work and are never refunded.
 *
 * Counts and microdollars are non-negative safe integers. The most recent
 * call cannot exceed its cumulative total. Every committed Turn has a model
 * call, and a failure streak cannot exceed the declared Tool call count.
 */
export const RunResumeUsageSchema = Schema.Struct({
  ...RunPolicyUsage.fields,
  modelCalls: Schema.Natural,
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  lastInputTokens: Schema.Natural,
  lastOutputTokens: Schema.Natural,
  costMicrousd: Schema.Natural,
}).check(
  Schema.makeFilter(
    (usage) =>
      usage.lastInputTokens <= usage.inputTokens && usage.lastOutputTokens <= usage.outputTokens,
    {
      expected: "last-call token usage no greater than cumulative token usage",
    },
  ),
  Schema.makeFilter(
    (usage) =>
      usage.modelCalls >= usage.committedTurns && usage.consecutiveToolFailures <= usage.toolCalls,
    {
      expected:
        "model calls covering committed Turns and a failure streak within declared Tool calls",
    },
  ),
);

export type RunResumeUsage = typeof RunResumeUsageSchema.Type;

/**
 * Resume one canonically declared Tool batch without re-invoking the model.
 *
 * When present, the engine's first Turn skips the model request entirely: the
 * declared calls are re-validated through their Tool parameter Schemas (a
 * decode failure executes nothing), approval preflight runs against recorded
 * decisions, host Tool authorization is re-evaluated, `prepareToolCalls` replays the full
 * prepared batch idempotently,
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
 * Tightening-only memory limits for one Run. The engine supplies finite ceilings for every field;
 * callers may lower them for a deployment or test but cannot widen the engine defaults.
 */
export interface RunBufferLimits {
  /** Maximum decoded response parts retained from one model call, including compaction calls. */
  readonly maxModelResponseParts?: number | undefined;
  /** Maximum conservative retained-byte estimate for one model response. */
  readonly maxModelResponseBytes?: number | undefined;
  /** Maximum semantic Run events, including the reserved terminal event. */
  readonly maxRunEvents?: number | undefined;
  /** Maximum Subagent lifecycle payloads queued by one Tool batch. */
  readonly maxSubagentEventsPerBatch?: number | undefined;
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
   * Host-owned action-time authorization for model-declared application Tool batches. The engine
   * invokes it for every still-executable call after complete-batch validation and approval, but
   * before durable preparation or any Handler permit. A resumed durable batch invokes it again
   * with the same canonical Run/Turn/input authority and Tool Call identity. Programmatic
   * `ToolBroker` calls are outside this hook.
   */
  readonly toolAuthorization?: RunToolAuthorizationHook<HookError, HookRequirements> | undefined;
  /**
   * Actual wall-clock start of the logical Run, used for elapsed-time status.
   * Durable coordinators supply the canonical `RunStarted` record timestamp on
   * every replacement Attempt. It is independent from `durationDeadline`,
   * which may tighten the remaining allowance without changing how long the
   * Run has existed (RUN-024/RUN-030).
   */
  readonly runStartedAt?: DateTime.Utc | undefined;
  /**
   * Optional absolute deadline for the Run's `maxDuration` rail. The engine
   * uses the earlier of this value and the fresh policy deadline, so callers
   * may preserve or tighten an existing Run allowance but can never widen it.
   * Durable coordinators derive this value from the canonical `RunStarted`
   * record timestamp and stored duration so replacement Attempts share one
   * wall-clock allowance (RUN-030).
   */
  readonly durationDeadline?: DateTime.Utc | undefined;
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
   * Requires `resumeUsage`; missing or contradictory accounting fails with
   * `ModelProtocolError` before input, model, or Tool execution.
   */
  readonly resume?: RunTurnResume | undefined;
  /**
   * Cumulative usage and Stop Policy accounting of the Run's prior Attempts,
   * projected from canonical records. A fresh continuation starts at
   * `committedTurns + 1`; a pending batch uses the already-counted declarations
   * and folds its terminal outcomes onto the prior failure streak once.
   * Omit only for fresh Runs. Incomplete or invalid seeds fail typed before execution.
   * For a pending batch, `committedTurns` must equal `resume.turn`, `toolCalls`
   * must include every pending declaration, and the failure streak cannot
   * exceed the declared calls before that batch.
   */
  readonly resumeUsage?: RunResumeUsage | undefined;
  /**
   * Per-Run Tool Call allowance (RUN-021): a TIGHTENING-ONLY bound below the
   * Agent Policy's `maxToolCalls` — the effective limit is
   * `min(policy.maxToolCalls, max(1, floor(toolCallAllowance)))`, so an
   * allowance can never widen the Definition's ceiling. The `onExhaustion`
   * resolution (RUN-018) keys off the effective limit, which is how an
   * orchestrator grants a delegated child a budget extension by re-invoking
   * with a larger allowance up to the child Definition's policy.
   */
  readonly toolCallAllowance?: number | undefined;
  /**
   * Per-Run Turn allowance (RUN-021): tightening-only below the Agent
   * Policy's `maxTurns`, with the same normalization and the same
   * `onExhaustion` resolution (RUN-019 grace) at the effective limit.
   */
  readonly turnAllowance?: number | undefined;
  /** Required when the core policy declares `costBudgetMicrousd`. */
  readonly estimateCostMicrousd?: RunCostEstimator<HookError, HookRequirements> | undefined;
  readonly scheduling?: RunSchedulingHook | undefined;
  /** Optional tightening-only overrides for the engine's finite in-memory buffer ceilings. */
  readonly bufferLimits?: RunBufferLimits | undefined;
  /** Internal/public observation seam invoked whenever official history advances. */
  readonly onHistory?:
    | ((history: Prompt.Prompt) => Effect.Effect<void, HookError, HookRequirements>)
    | undefined;
}

export type RunOptionsError<Options> =
  Options extends RunOptions<infer Error, infer _Requirements> ? Error : never;

export type RunOptionsRequirements<Options> =
  Options extends RunOptions<infer _Error, infer Requirements> ? Requirements : never;

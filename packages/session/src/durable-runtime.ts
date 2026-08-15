import {
  AgentApprovalPending,
  AgentInputError,
  ConversationId,
  DelegationDepth,
  IdGenerator,
  isDelegationToolName,
  ReceiptId,
  SubagentParentLink,
  SubmissionId,
  ToolCallId,
  type Agent,
  type AgentId,
  type AttemptId,
  type RunEvent,
  type RunId,
  type TurnId,
} from "@effect-agent/core";
import {
  AgentChildPending,
  AgentRuntime,
  getToolExecutionClass,
  type AgentRuntimeRequirements,
  type ChildEstablishStatus,
  type RunApprovalHook,
  type RunContextHook,
  type RunDurabilityHook,
  type RunInputCommand,
  type RunInputHook,
  type RunOptions,
  type RunSubagentEstablishRequest,
  type RunSubagentHook,
  type RunSubagentJoinRequest,
  type RuntimeBinding,
  type ToolExecutionClassValue,
} from "@effect-agent/engine";
import {
  Clock,
  Context,
  Crypto,
  DateTime,
  Duration,
  Effect,
  Layer,
  Option,
  Ref,
  Schedule,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import { Prompt, type Tool } from "effect/unstable/ai";

import {
  ExplainedEvidence,
  ExplainedSubmission,
  ExplainedUnknownCall,
  IntegrityCheck,
  IntegrityReport,
  ObligationEntry,
  ObligationReport,
  RecoveryExplanation,
  RetryCommand,
  RetryRefused,
  obligationSeverityOf,
  predictRecoveryDisposition,
  recoveryDecisionMeaning,
  type ObligationBlockedOn,
  type ObligationThresholds,
} from "./admin.ts";
import {
  AgentBindingResolver,
  BindingDigestMismatch,
  BindingUnavailable,
  definitionDigestsEqual,
  DurableWorkerBinding,
  type DurableBindingFailure,
  type ResolvedAttemptDriver,
  type ResolvedBinding,
} from "./binding-resolver.ts";
import { verifyConversationInvariants } from "./certification.ts";
import { digestJson, type DigestError } from "./digest.ts";
import {
  DurableRuntimeFailpoint,
  type DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "./durable-failpoint.ts";
import {
  AbortCommand,
  AbortIntent,
  AdmissionConflict,
  AdmissionRequest,
  ApprovalDecisionCommand,
  ApprovalPendingSuspension,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ChildSettledNotification,
  Claim,
  ClaimJoiningRequest,
  ClaimRequest,
  IdempotencyKey,
  LedgerError,
  JoinedToHost,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  OwnershipToken,
  ParentLinkage,
  Principal,
  QueueSequence,
  RecoverySnapshot,
  RecoverySnapshotRequest,
  ReleaseChildBudgetRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  RevertJoiningRequest,
  SettlementConflict,
  SettlementFinalization,
  SettlementReservation,
  Settlement,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  SubmissionSnapshot,
  SuspendRequest,
  UnknownResolutionCommand,
  WaitingChild,
  WaitingForChildSuspension,
  submissionAbortBatchId,
  submissionAbortRecordId,
  submissionInputBatchId,
  submissionInputRecordId,
  submissionSettlementBatchId,
  submissionSettlementId,
  submissionSettlementRecordId,
  type AdmissionResult,
  type ApprovalConflict,
  type ApprovalDecisionIntent,
  type ChildBudgetReservationSnapshot,
  type JoinSnapshot,
  type UnknownResolutionConflict,
  type UnknownResolutionIntent,
} from "./ledger.ts";
import {
  OperationAuthorizationRequest,
  OperationAuthorizer,
  type OperationDenied,
} from "./operation-authorizer.ts";
import { PreparedToolCallEvidence, ToolReconciler } from "./reconciler.ts";
import {
  AbortRequested,
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  CompactionCreated,
  ConversationCreated,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ModelResponseInterrupted,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
  SubagentJoined,
  SubagentLineageRecorded,
  SubagentRequested,
  SubagentStarted,
  SubmissionSettled,
  ToolApprovalDecided,
  ToolApprovalRequested,
  ToolCallPrepared,
  ToolCallResolved,
  ToolCallSettled,
  ToolCallUnknown,
  ToolStepSettled,
  UserInputRecorded,
  type ApprovalDecision,
  type CanonicalRecordPayload,
  type SettlementOutcome,
  type ToolCallResolution,
} from "./records.ts";
import {
  classifyRecovery,
  DeclaredPendingBatchEvidence,
  OpenDelegationCallEvidence,
  OpenToolCallEvidence,
  PendingApprovalEvidence,
  RecoveryDecision,
  RecoveryEvidence,
  type DelegationAdmissionEvidence,
  type MarkUnknown as MarkUnknownDecision,
  type SettleAborted as SettleAbortedDecision,
} from "./recovery.ts";
import {
  RunJournalError,
  approvalDecisionBatchId,
  childConversationIdFor,
  childIdempotencyKeyFor,
  compactionBatchId,
  compactionRecordId,
  markUnknownBatchId,
  modelResponseInterruptedBatchId,
  modelResponseInterruptedRecordId,
  modelResponseRecordId,
  projectRunJournal,
  runIdForSubmission,
  subagentJoinBatchId,
  subagentJoinedRecordId,
  subagentLineageBatchId,
  subagentLineageRecordId,
  subagentRequestedBatchId,
  subagentRequestedRecordId,
  subagentStartedBatchId,
  subagentStartedRecordId,
  toolApprovalDecisionRecordId,
  toolApprovalRequestRecordId,
  toolCallPreparedRecordId,
  toolCallResolutionBatchId,
  toolCallResolvedRecordId,
  toolCallResultBatchId,
  toolCallSettledRecordId,
  toolCallUnknownRecordId,
  toolStepSettledBatchId,
  toolStepSettledRecordId,
  turnApprovalsBatchId,
  turnCanonicalBatch,
  turnIdForRun,
  turnPreparedBatchId,
  turnResponseBatch,
  turnResultsBatch,
} from "./run-journal.ts";
import {
  AppendConflict,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationNotMaterialized,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  ConversationTailRequest,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  type ConversationCheckpoint,
} from "./store.ts";
import { WakeScheduler } from "./wake.ts";

/**
 * Instruction failure/requirement derivation mirroring the engine's `RuntimeBinding` defaults:
 * declaring them as generic DEFAULTS (instead of independent inference sites) keeps a plain
 * string-returning instruction function from widening both parameters to `unknown`.
 */
type InstructionResultOf<Instructions, Input> = Instructions extends (input: Input) => infer Result
  ? Result
  : Instructions;

type InstructionErrorOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer Error,
    infer _Requirements
  >
    ? Error
    : never;

type InstructionRequirementsOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer _Error,
    infer Requirements
  >
    ? Requirements
    : never;

const decodeBatchId = Schema.decodeSync(BatchId);
const decodeRecordId = Schema.decodeSync(RecordId);
const decodeToolCallIdUnknown = Schema.decodeUnknownEffect(ToolCallId);
const ZERO_EPOCH = Schema.decodeSync(ProducerEpoch)(0);
const READ_PAGE = 1_024;
/** Stale-tail append retries per batch before the conflict propagates (see `appendBatch`). */
const MAX_APPEND_FENCE_REFRESHES = 8;
const MAX_FAILURE_MESSAGE_LENGTH = 16_384;
const RECONCILER_AUTHOR = "reconciler";
/** Canonical `ToolApprovalDecided.resolver` for policy-auto decisions made by the delegate. */
const APPROVAL_POLICY_RESOLVER = "approval-policy";
/** Ledger `ApprovalDecisionIntent.resolver` for the abort-driven suspension closure. */
const RECOVERY_RESOLVER = "recovery";
/** Upper bound of one `drain("all")` joining pass (`claimJoining.maxCount` must be positive). */
const MAX_JOIN_DRAIN = 32;
/**
 * Token presented when reserving a joined Submission's settlement (plan §2.5). A `joined` lane is
 * never worker-claimable (WP2 claim rule), so no real ownership token can exist for it: the
 * ledger authorizes the reservation by the recorded host linkage and does not consult this value.
 */
const JOINED_SETTLEMENT_TOKEN = Schema.decodeSync(OwnershipToken)("ownership-joined-settlement");

/**
 * Placeholder token for the P7 §7(c) queued-abort settlement: an aborted, never-claimed,
 * still-queued `ready` Submission has no live ownership to fence against, so the ledger
 * authorizes its aborted reservation by the durable abort intent itself (the joined-settlement
 * pattern) and the presented token is not consulted.
 */
const QUEUED_ABORT_SETTLEMENT_TOKEN = Schema.decodeSync(OwnershipToken)(
  "ownership-aborted-queued-settlement",
);

/** Bound a hook-supplied approval reason to the canonical `BoundedText` persistence limits. */
const boundedApprovalReason = (reason: string | undefined, fallback: string): string => {
  const value = reason === undefined || reason.length === 0 ? fallback : reason;
  return value.length > MAX_FAILURE_MESSAGE_LENGTH
    ? value.slice(0, MAX_FAILURE_MESSAGE_LENGTH)
    : value;
};

const boundedText = (value: string): string =>
  value.length > MAX_FAILURE_MESSAGE_LENGTH ? value.slice(0, MAX_FAILURE_MESSAGE_LENGTH) : value;

const decodePrincipalSync = Schema.decodeSync(Principal);
const decodeIdempotencyKeySync = Schema.decodeSync(IdempotencyKey);
const decodeChildReservationIdSync = Schema.decodeSync(ChildReservationId);
const decodeDefinitionDigests = Schema.decodeUnknownEffect(DefinitionDigests);

/**
 * Deterministic parent-owned child budget reservation identity (spec
 * §12 step 2, D4): one reservation per (parent Run, parent Tool Call) pair,
 * so a replayed establishment converges on the one existing row (SUB-016).
 */
export const childReservationIdFor = (runId: RunId, toolCallId: ToolCallId): ChildReservationId =>
  decodeChildReservationIdSync(`subagent-reservation:${runId}:${toolCallId}`);

/**
 * S2 fixes every attached durable child at delegation depth 1 (SUB-029
 * rejects all nested delegation at preflight), so recovery can rebuild the
 * child lineage from the canonical `SubagentRequested` record alone — the
 * record deliberately does not carry a depth field.
 */
const CHILD_DELEGATION_DEPTH: DelegationDepth = Schema.decodeSync(DelegationDepth)(1);

/** Canonical `AbortRequested.author` of every propagated parent-abort command (spec §13.1). */
const SUBAGENT_ABORT_AUTHOR = "subagent-parent-abort";
/** Deterministic propagated-abort reason: replaying the identical command is the repair (DUR-012). */
const SUBAGENT_ABORT_REASON =
  "The parent Submission was aborted; request-abort-and-join propagates the durable abort intent to every attached child (spec/subagents.md 13.1)";

/**
 * The deterministic zero-consumed accounting decision frozen for a
 * provably-childless orphaned reservation (spec §13/§14: "releases the
 * reservation exactly once"). Constant so every repair pass freezes the SAME
 * decision — an identical `beginChildBudgetRelease` replay is a no-op.
 */
const ORPHAN_ZERO_CONSUMED_ACCOUNTING = Schema.decodeUnknownSync(PersistedJson)({
  basis: "orphan-zero-consumed",
});

/** The four parent-log/child-log subagent records of one Run, indexed per Tool Call. */
interface SubagentCallRecords {
  readonly requested: Map<ToolCallId, SubagentRequested>;
  readonly started: Map<ToolCallId, SubagentStarted>;
  readonly joined: Map<ToolCallId, SubagentJoined>;
  /** Declared Tool name per prepared call (delegation joins reuse it for `ToolCallSettled`). */
  readonly preparedNames: Map<ToolCallId, string>;
}

/** Pure fold of one Run's canonical subagent lifecycle records (plan §1.2). */
const subagentRecordsOf = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  runId: RunId,
): SubagentCallRecords => {
  const requested = new Map<ToolCallId, SubagentRequested>();
  const started = new Map<ToolCallId, SubagentStarted>();
  const joined = new Map<ToolCallId, SubagentJoined>();
  const preparedNames = new Map<ToolCallId, string>();
  for (const envelope of records) {
    const payload = envelope.record.payload;
    switch (payload._tag) {
      case "SubagentRequested": {
        if (payload.runId === runId) requested.set(payload.toolCallId, payload);
        break;
      }
      case "SubagentStarted": {
        if (payload.runId === runId) started.set(payload.toolCallId, payload);
        break;
      }
      case "SubagentJoined": {
        if (payload.runId === runId) joined.set(payload.toolCallId, payload);
        break;
      }
      case "ToolCallPrepared": {
        if (payload.runId === runId) preparedNames.set(payload.toolCallId, payload.toolName);
        break;
      }
      default: {
        break;
      }
    }
  }
  return { requested, started, joined, preparedNames };
};

/** Deterministic batch identity of one Conversation's initial `ConversationCreated` append. */
export const conversationCreatedBatchId = (conversationId: ConversationId): BatchId =>
  decodeBatchId(`conversation-created:${conversationId}`);

/** Deterministic record identity of one Conversation's `ConversationCreated` record. */
export const conversationCreatedRecordId = (conversationId: ConversationId): RecordId =>
  decodeRecordId(`conversation-created:${conversationId}`);

/** Deterministic batch identity of one executed recovery decision's audit append (DUR-013). */
export const recoveryRepairBatchId = (submissionId: SubmissionId, decisionTag: string): BatchId =>
  decodeBatchId(`repair:${submissionId}:${decisionTag}`);

/** Deterministic record identity of one executed recovery decision's `RepairAnnotated` record. */
export const recoveryRepairRecordId = (submissionId: SubmissionId, decisionTag: string): RecordId =>
  decodeRecordId(`repair:${submissionId}:${decisionTag}`);

/**
 * The durable identity returned once ledger admission, Conversation materialization, and
 * readiness are committed (DUR-001). It is an identifier for observation and reattachment, not
 * an authorization capability.
 */
export class Receipt extends Schema.Class<Receipt>("@effect-agent/session/Receipt")({
  receiptId: ReceiptId,
  submissionId: SubmissionId,
  conversationId: ConversationId,
  queueSequence: QueueSequence,
}) {}

/** One executed (or deliberately deferred) recovery decision (durability §14, DUR-013). */
export class RecoveryReport extends Schema.Class<RecoveryReport>(
  "@effect-agent/session/RecoveryReport",
)({
  submissionId: SubmissionId,
  conversationId: ConversationId,
  decision: RecoveryDecision,
  /**
   * `repaired` = executed; `deferred` = a claiming worker must finish it; `none` = settled;
   * `unknown` = the lane is durably blocked on an Unknown Outcome awaiting the authorized
   * DUR-017 resolution path — the settlement obligation stays visible while no worker permit
   * is consumed (durability §16).
   */
  disposition: Schema.Literals(["repaired", "deferred", "none", "unknown"]),
}) {}

/** Per-submission options accepted by `DurableAgentRuntime.submit` (D2). */
export interface DurableSubmitOptions {
  readonly conversationId: ConversationId;
  readonly principal: Principal;
  readonly idempotencyKey: IdempotencyKey;
  /** Application-computed digests of the Agent/Model/Toolkit definitions (see `digestDefinitions`). */
  readonly definitions: DefinitionDigests;
}

export interface DurableObserveOptions {
  /** Resume the observation after an adapter-owned offset previously returned by `observe`. */
  readonly after?: CanonicalRecordEnvelope["offset"] | undefined;
}

/** The structural slice of an Agent Binding that `submit` needs. */
export interface DurableSubmitAgent<InputSchema extends Schema.Top> {
  readonly definition: {
    readonly id: AgentId;
    readonly input: InputSchema;
  };
}

export type DurableSubmitFailure =
  | AgentInputError
  | DigestError
  | AdmissionConflict
  | LedgerError
  | ConversationStoreError
  | ConversationNotMaterialized
  | AppendConflict
  | FenceRejected
  | DurableRuntimeFailpointError;

export type DurableWorkerFailure =
  | DigestError
  | LedgerError
  | OwnershipLost
  | SettlementConflict
  | ConversationStoreError
  | ConversationNotMaterialized
  | AppendConflict
  | FenceRejected
  | RunJournalError
  | DurableRuntimeFailpointError;

export type DurableAwaitFailure = LedgerError | SettlementConflict;

export type DurableAbortFailure =
  | LedgerError
  | SettlementConflict
  | JoinedToHost
  | DurableRuntimeFailpointError;

/**
 * Failure family of `resolveUnknown` (DUR-017, abort-shaped): the durable intent may conflict
 * with a divergent prior resolution, the Submission may already be settled, an `AbortSubmission`
 * resolution routes through the abort path (whose `JoinedToHost` conflict stays visible), and the
 * failpoint after the intent write is armable like every other durable mutation.
 */
export type DurableResolveFailure =
  | LedgerError
  | SettlementConflict
  | UnknownResolutionConflict
  | JoinedToHost
  | DurableRuntimeFailpointError;

/**
 * Failure family of `resolveApproval` (plan §2.6, abort-shaped): the durable intent may conflict
 * with a divergent prior decision, and the Submission may already be settled. The ledger adapter
 * owns the crash boundaries of the intent write (`ledger:approval-decision:{before,after}`).
 */
export type DurableApprovalFailure = LedgerError | SettlementConflict | ApprovalConflict;

/**
 * Failure family of the read-only `explain`/`explainConversation` operations (P7 WP1): pure
 * observation over the two ports plus the evidence assembler's typed failures, and the
 * fail-closed `OperationDenied` when a host-supplied authorizer refuses.
 */
export type DurableExplainFailure =
  | LedgerError
  | ConversationStoreError
  | RunJournalError
  | OperationDenied;

/**
 * Failure family of the read-only `verify` operation (P7 WP1). `ConversationNotMaterialized`
 * stays visible: verifying a Conversation that does not exist is a caller error, not an empty
 * report.
 */
export type DurableVerifyFailure =
  | LedgerError
  | ConversationStoreError
  | ConversationNotMaterialized
  | OperationDenied;

/**
 * Failure family of `retry` (P7 WP1): the scoped recovery execution carries the worker failure
 * family, refusals are typed (`RetryRefused` — settled work and lanes owned by the
 * resolveUnknown/resolveApproval paths), and denial is fail-closed.
 */
export type DurableRetryFailure = DurableWorkerFailure | RetryRefused | OperationDenied;

/** Failure family of `scanObligations` (P7 WP1): ledger scan + canonical unknown-age reads. */
export type DurableObligationFailure = LedgerError | ConversationStoreError | OperationDenied;

/**
 * Optional policy-auto approval delegate consulted by the durable approval hook (plan §2.6 step
 * 2) AFTER the recorded-decision lookup misses. An immediate `approved`/`denied` answer becomes
 * canonical (`ToolApprovalRequested` + `ToolApprovalDecided`, one atomic batch) before it is
 * honored; `unresolved` falls through to the durable suspension path. The default (`undefined`)
 * suspends every undecided approval durably until `resolveApproval` decides it — the fail-closed
 * posture. Adapt the P2 `ApprovalResolver` capability stack through
 * `@effect-agent/capabilities`' `toDurableRunApprovalHook`.
 */
export const DurableApprovalResolver: Context.Reference<RunApprovalHook<never, never> | undefined> =
  Context.Reference<RunApprovalHook<never, never> | undefined>(
    "@effect-agent/session/DurableApprovalResolver",
    { defaultValue: () => undefined },
  );

/**
 * Services a durable worker needs beyond the runtime's own Layer: the Agent Binding's inferred
 * requirements minus `IdGenerator`, which the coordinator provides itself so Run/Turn identity is
 * deterministic per Submission across Attempts.
 */
export type DurableWorkerRequirements<
  AgentValue extends Agent.Any,
  InstructionRequirements = never,
> = Exclude<AgentRuntimeRequirements<AgentValue, never, InstructionRequirements>, IdGenerator>;

export interface DurableRuntimeConfigOptions {
  readonly deploymentId: DeploymentId;
  readonly producerId: ProducerId;
  /** `awaitSettlement` ledger re-check cadence when no wake arrives (default 500ms). */
  readonly settlementPollInterval?: Duration.Duration | undefined;
  /** Worker ownership-lease renewal cadence (default 10s; D5 lease default is 30s). */
  readonly leaseRenewalInterval?: Duration.Duration | undefined;
  /** Active-Run abort-intent poll cadence (default 500ms). */
  readonly abortPollInterval?: Duration.Duration | undefined;
}

/** Deployment-scoped identity and liveness cadences for the durable coordinator. */
export class DurableRuntimeConfig extends Context.Service<
  DurableRuntimeConfig,
  {
    readonly deploymentId: DeploymentId;
    readonly producerId: ProducerId;
    readonly settlementPollInterval: Duration.Duration;
    readonly leaseRenewalInterval: Duration.Duration;
    readonly abortPollInterval: Duration.Duration;
  }
>()("@effect-agent/session/DurableRuntimeConfig") {
  static make(options: DurableRuntimeConfigOptions): (typeof DurableRuntimeConfig)["Service"] {
    return {
      deploymentId: options.deploymentId,
      producerId: options.producerId,
      settlementPollInterval: options.settlementPollInterval ?? Duration.millis(500),
      leaseRenewalInterval: options.leaseRenewalInterval ?? Duration.seconds(10),
      abortPollInterval: options.abortPollInterval ?? Duration.millis(500),
    };
  }

  static layer(options: DurableRuntimeConfigOptions): Layer.Layer<DurableRuntimeConfig> {
    return Layer.succeed(DurableRuntimeConfig)(DurableRuntimeConfig.make(options));
  }
}

/** Terminal outcome an Attempt decided before terminalization (DUR-011). */
type AttemptOutcome =
  | {
      readonly _tag: "completed";
      readonly result: PersistedJson;
      /** Set when the Run settled through the final-answer exhaustion resolution (RUN-018). */
      readonly finishReason?: "budget-exhausted";
    }
  | { readonly _tag: "failed"; readonly result: PersistedJson }
  | { readonly _tag: "aborted" };

/**
 * What one `runModel` pass produced: a terminal `AttemptOutcome` for terminalization, a
 * durable approval suspension (plan §2.6) — the unresolved call's canonical request is already
 * appended, NO settlement is owed by this pass, and `runAttempt` transitions the ledger — or a
 * durable `waitingForChild` suspension (spec/subagents.md §12 step 10): every non-waiting
 * sibling result is already committed as a per-call late-settle batch and `runAttempt` executes
 * `ledger.suspend(WaitingForChild)`.
 */
type RunPhaseOutcome =
  | AttemptOutcome
  | { readonly _tag: "suspended"; readonly toolCallId: ToolCallId }
  | { readonly _tag: "suspendedChild"; readonly children: AgentChildPending["children"] };

/**
 * Internal marker separating coordinator infrastructure failures (fencing, storage, failpoints —
 * the Attempt aborts cleanly and the accepted work stays owed) from Agent Run failures (typed
 * engine errors — the Submission settles `failed`). Never exported: it exists only so the two
 * failure families cannot be confused inside the run phase.
 */
class CoordinatorHalt {
  readonly _tag = "CoordinatorHalt";
  constructor(readonly failure: DurableWorkerFailure) {}
}

const ErrorMessage = Schema.Struct({ message: Schema.String });
const ErrorTag = Schema.Struct({ _tag: Schema.NonEmptyString });
const decodeErrorMessage = Schema.decodeUnknownOption(ErrorMessage);
const decodeErrorTag = Schema.decodeUnknownOption(ErrorTag);

const errorMessageOf = (error: unknown): string =>
  Option.match(decodeErrorMessage(error), {
    onNone: () => String(error),
    onSome: ({ message }) => message,
  });

const errorTagOf = (error: unknown): string =>
  Option.match(decodeErrorTag(error), {
    onNone: () => "UnknownError",
    onSome: ({ _tag }) => _tag,
  });

const nowUtc: Effect.Effect<DateTime.Utc> = Effect.map(Clock.currentTimeMillis, (millis) =>
  DateTime.toUtc(DateTime.makeUnsafe(millis)),
);

const decodePrompt = Schema.decodeUnknownEffect(Prompt.Prompt);
const decodePersisted = Schema.decodeUnknownEffect(PersistedJson);

/** One application Tool Call declared inside a canonical `ModelResponseRecorded`'s messages. */
interface DeclaredApplicationCall {
  readonly id: string;
  readonly name: string;
  /** Encoded JSON parameters exactly as canonical history carries them. */
  readonly params: unknown;
}

/**
 * Pure extraction of a Turn's declared application Tool Calls from the canonical (encoded)
 * `ModelResponseRecorded.messages` value. Provider-executed calls are the provider's own
 * responsibility and never enter the durable prepared/settled protocol.
 */
const declaredApplicationCalls = Effect.fn("DurableAgentRuntime.declaredApplicationCalls")(
  (messages: PersistedJson): Effect.Effect<Array<DeclaredApplicationCall>, RunJournalError> =>
    decodePrompt(messages).pipe(
      Effect.mapError((cause) =>
        RunJournalError.make({
          message: "ModelResponseRecorded messages are not Schema-encoded Prompt messages",
          cause,
        }),
      ),
      Effect.map((prompt) => {
        const calls: Array<DeclaredApplicationCall> = [];
        for (const message of prompt.content) {
          if (message.role !== "assistant") continue;
          for (const part of message.content) {
            if (part.type !== "tool-call" || part.providerExecuted) continue;
            calls.push({ id: part.id, name: part.name, params: part.params });
          }
        }
        return calls;
      }),
    ),
);

/**
 * The declared-but-unsettled Tool batch of one Run's last committed Turn (§2.4 batch resume):
 * every declared call in canonical encoded form plus the recorded results of the calls that
 * already settled (results batch never committed, or per-call late settles from the resolution
 * path). `undefined` when the Run's journal ends at a complete Turn boundary.
 */
interface PendingToolBatch {
  readonly turn: number;
  readonly turnId: TurnId;
  readonly calls: ReadonlyArray<DeclaredApplicationCall>;
  readonly settled: ReadonlyArray<{
    readonly id: string;
    readonly result: PersistedJson;
    readonly isFailure: boolean;
  }>;
  readonly declaredIds: ReadonlySet<string>;
  readonly responseRecordId: RecordId;
  /** The pending `ModelResponseRecorded.messages`: its pre-assistant slice is the resume's leading messages. */
  readonly messages: PersistedJson;
}

interface AttemptAppendContext {
  readonly conversationId: ConversationId;
  readonly producerEpoch: ProducerEpoch;
  readonly tailRef: Ref.Ref<{ readonly sequence: CanonicalSequence; readonly digest: Digest }>;
  /** Serializes every canonical append of one Attempt (run commits vs. the abort watcher). */
  readonly gate: Semaphore.Semaphore;
}

/** What the resuming worker knows about the ownership period it superseded (durability §9). */
interface AttemptLineage {
  readonly attemptId: AttemptId;
  /** The Conversation-store fence BEFORE this Attempt advanced it (0 = no prior producer). */
  readonly supersededEpoch: ProducerEpoch;
  /** The canonical `input:{sid}` record existed before this Attempt started. */
  readonly inputWasRecorded: boolean;
}

/** Review of one Submission's open (prepared-without-outcome) ordinary Tool Calls (DUR-009). */
interface OpenCallReview {
  /** No proof either way: these calls must become Unknown Outcomes (never auto-replayed). */
  readonly uncertain: Array<OpenToolCallEvidence>;
  /** The reconciliation policy itself failed: stay open and blocked WITHOUT marking; retry later. */
  readonly unproven: Array<OpenToolCallEvidence>;
  /** Proven safe to re-execute (never-started / safe-retry / declared idempotent contract). */
  readonly retryable: Array<OpenToolCallEvidence>;
  /** Calls closed canonically with a recovered result (no handler execution). */
  readonly recovered: number;
}

const make = Effect.gen(function* () {
  const ledger = yield* SubmissionLedger;
  const store = yield* ConversationStore;
  const wake = yield* WakeScheduler;
  const failpoint = yield* DurableRuntimeFailpoint;
  const config = yield* DurableRuntimeConfig;
  const crypto = yield* Crypto.Crypto;
  const reconciler = yield* ToolReconciler;
  const approvalResolver = yield* DurableApprovalResolver;
  // Possession-default authorization reference (P7 WP1): the default allows everything —
  // exactly the pre-P7 service-possession boundary — and a host-supplied non-default Layer is
  // consulted fail-closed by observe, the admin operations, and the two resolution paths.
  const operationAuthorizer = yield* OperationAuthorizer;

  const withCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>): Effect.Effect<A, E> =>
    Effect.provideService(effect, Crypto.Crypto, crypto);

  const hit = (
    location: DurableRuntimeFailpointLocation,
  ): Effect.Effect<void, DurableRuntimeFailpointError> => failpoint.hit(location);

  const makeEnvelope = (
    recordId: RecordId,
    payload: CanonicalRecordPayload,
  ): Effect.Effect<RecordEnvelope> =>
    Effect.map(nowUtc, (createdAt) =>
      RecordEnvelope.make({
        recordId,
        family: "conversation",
        schemaVersion: 1,
        createdAt,
        deploymentId: config.deploymentId,
        payload,
      }),
    );

  const readAll = Effect.fn("DurableAgentRuntime.readAll")(function* (
    conversationId: ConversationId,
  ): Effect.fn.Return<
    Array<CanonicalRecordEnvelope>,
    ConversationStoreError | ConversationNotMaterialized
  > {
    const collected: Array<CanonicalRecordEnvelope> = [];
    let after: CanonicalSequence | undefined = undefined;
    while (true) {
      const request: ConversationRead =
        after === undefined
          ? ConversationRead.make({ conversationId, limit: READ_PAGE })
          : ConversationRead.make({ conversationId, limit: READ_PAGE, afterSequence: after });
      const page: Array<CanonicalRecordEnvelope> = yield* Stream.runCollect(store.read(request));
      collected.push(...page);
      const last = page.at(-1);
      if (page.length < READ_PAGE || last === undefined) return collected;
      after = last.sequence;
    }
  });

  const readAllTolerant = Effect.fn("DurableAgentRuntime.readAllTolerant")(
    (
      conversationId: ConversationId,
    ): Effect.Effect<
      { readonly records: ReadonlyArray<CanonicalRecordEnvelope>; readonly materialized: boolean },
      ConversationStoreError
    > =>
      readAll(conversationId).pipe(
        Effect.map((records) => ({ records, materialized: true })),
        Effect.catchTag("ConversationNotMaterialized", () =>
          Effect.succeed({ records: [], materialized: false }),
        ),
      ),
  );

  const knownRecordIdsOf = (records: ReadonlyArray<CanonicalRecordEnvelope>): Set<string> =>
    new Set(records.map((envelope) => envelope.record.recordId));

  /**
   * Fold structured recovery evidence from canonical records (plan §2.2). Canonical history is
   * the only recovery truth (DUR-015): open Tool Calls are `ToolCallPrepared` without a closing
   * `ToolCallSettled`/`ToolCallResolved`, a declared-pending batch is a committed tool-declaring
   * response with zero prepared and zero settled records for its Turn (the provably-safe
   * durability §15 window), approvals pend until a canonical decision exists, and joined-side
   * prompt coverage requires a host `ModelResponseRecorded` after the joined `input:{sid}` record.
   */
  const evidenceFor = Effect.fn("DurableAgentRuntime.evidenceFor")(function* (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    submissionId: SubmissionId,
    materialized: boolean,
    hostSubmissionId?: SubmissionId,
  ): Effect.fn.Return<RecoveryEvidence, RunJournalError | LedgerError> {
    const runId = runIdForSubmission(submissionId);
    const hostRunId =
      hostSubmissionId === undefined ? undefined : runIdForSubmission(hostSubmissionId);
    const inputId = submissionInputRecordId(submissionId);
    const abortId = submissionAbortRecordId(submissionId);
    const settlementId = submissionSettlementRecordId(submissionId);
    const hostSettlementId =
      hostSubmissionId === undefined ? undefined : submissionSettlementRecordId(hostSubmissionId);
    let inputRecorded = false;
    let inputSequence: CanonicalSequence | undefined;
    let abortRecorded = false;
    let subagentLineageRecorded = false;
    let recordedSettlementOutcome: SettlementOutcome | undefined;
    let hostSettlementOutcome: SettlementOutcome | undefined;
    let hostRespondedAfterInput = false;
    const prepared: Array<OpenToolCallEvidence> = [];
    const preparedTurns = new Set<number>();
    const settledIds = new Set<string>();
    const resolvedIds = new Set<string>();
    const requested: Array<PendingApprovalEvidence> = [];
    const decidedIds = new Set<string>();
    let lastResponse: { readonly turn: number; readonly messages: PersistedJson } | undefined;

    for (const envelope of records) {
      const recordId = envelope.record.recordId;
      const payload = envelope.record.payload;
      if (recordId === inputId) {
        inputRecorded = true;
        inputSequence = envelope.sequence;
        continue;
      }
      if (recordId === abortId) {
        abortRecorded = true;
        continue;
      }
      if (recordId === settlementId && payload._tag === "SubmissionSettled") {
        recordedSettlementOutcome = payload.outcome;
        continue;
      }
      if (
        hostSettlementId !== undefined &&
        recordId === hostSettlementId &&
        payload._tag === "SubmissionSettled"
      ) {
        hostSettlementOutcome = payload.outcome;
        continue;
      }
      switch (payload._tag) {
        case "ToolCallPrepared": {
          if (payload.runId !== runId) break;
          prepared.push(
            OpenToolCallEvidence.make({
              toolCallId: payload.toolCallId,
              toolName: payload.toolName,
              turn: payload.turn,
            }),
          );
          preparedTurns.add(payload.turn);
          break;
        }
        case "ToolCallSettled": {
          if (payload.runId === runId) settledIds.add(payload.toolCallId);
          break;
        }
        case "ToolCallResolved": {
          if (payload.runId === runId) resolvedIds.add(payload.toolCallId);
          break;
        }
        case "ToolApprovalRequested": {
          if (payload.runId !== runId) break;
          requested.push(
            PendingApprovalEvidence.make({ toolCallId: payload.toolCallId, turn: payload.turn }),
          );
          break;
        }
        case "ToolApprovalDecided": {
          if (payload.runId === runId) decidedIds.add(payload.toolCallId);
          break;
        }
        case "SubagentLineageRecorded": {
          // Conversation-level fact (one lineage record per child Conversation, spec §11):
          // its presence gates the P7 §7(a) AwaitParentEstablishment row for parent-linked
          // Submissions; root Conversations never carry it and never consult it.
          subagentLineageRecorded = true;
          break;
        }
        case "ModelResponseRecorded": {
          if (
            payload.runId === runId &&
            (lastResponse === undefined || payload.turn > lastResponse.turn)
          ) {
            lastResponse = { turn: payload.turn, messages: payload.messages };
          }
          if (
            hostRunId !== undefined &&
            payload.runId === hostRunId &&
            inputSequence !== undefined &&
            envelope.sequence > inputSequence
          ) {
            hostRespondedAfterInput = true;
          }
          break;
        }
        default: {
          break;
        }
      }
    }

    const allOpenCalls = prepared.filter(
      (call) => !settledIds.has(call.toolCallId) && !resolvedIds.has(call.toolCallId),
    );

    // S2 delegation evidence (plan §4.1, WP2 contract): every Tool Call with any canonical
    // subagent lifecycle record — plus every open call matching the core-owned delegation
    // naming rule — is separated from `openToolCalls`, because its establishment protocol is
    // idempotent by construction and must never be marked Unknown (spec §13 vs. DUR-009).
    const subagent = subagentRecordsOf(records, runId);
    const openByCallId = new Map(allOpenCalls.map((call) => [call.toolCallId, call]));
    const delegationCallIds: Array<ToolCallId> = [];
    const seenDelegationIds = new Set<ToolCallId>();
    const noteDelegation = (toolCallId: ToolCallId): void => {
      if (seenDelegationIds.has(toolCallId)) return;
      seenDelegationIds.add(toolCallId);
      delegationCallIds.push(toolCallId);
    };
    for (const toolCallId of subagent.requested.keys()) noteDelegation(toolCallId);
    for (const toolCallId of subagent.started.keys()) noteDelegation(toolCallId);
    for (const toolCallId of subagent.joined.keys()) noteDelegation(toolCallId);
    for (const call of allOpenCalls) {
      if (isDelegationToolName(call.toolName)) noteDelegation(call.toolCallId);
    }
    const openDelegationCalls: Array<OpenDelegationCallEvidence> = [];
    for (const toolCallId of delegationCallIds) {
      const open = openByCallId.get(toolCallId);
      const requestedRecord = subagent.requested.get(toolCallId);
      const startedRecord = subagent.started.get(toolCallId);
      // Authoritative admission tri-state for a requested-but-unstarted call (SUB-031): the
      // deterministic idempotency key queries the ledger directly — projection absence is
      // never proof of absence, and only a proven `not-admitted` permits an admission attempt.
      let admission: DelegationAdmissionEvidence | undefined;
      let childSubmissionId = startedRecord?.childSubmissionId;
      if (requestedRecord !== undefined && startedRecord === undefined) {
        const resolution = yield* ledger.resolveAdmission(
          SubmissionLookupByKey.make({
            conversationId: requestedRecord.childConversationId,
            principal: decodePrincipalSync(requestedRecord.childPrincipal),
            idempotencyKey: decodeIdempotencyKeySync(requestedRecord.childIdempotencyKey),
          }),
        );
        switch (resolution._tag) {
          case "NotAdmitted": {
            admission = "not-admitted";
            break;
          }
          case "Admitted": {
            admission = "admitted";
            childSubmissionId = resolution.submission.submissionId;
            break;
          }
          case "Indeterminate": {
            admission = "indeterminate";
            break;
          }
        }
      }
      const childConversationId =
        startedRecord?.childConversationId ?? requestedRecord?.childConversationId;
      openDelegationCalls.push(
        OpenDelegationCallEvidence.make({
          toolCallId,
          toolName:
            open?.toolName ??
            subagent.preparedNames.get(toolCallId) ??
            requestedRecord?.delegationId ??
            "delegate_unknown",
          turn: open?.turn ?? requestedRecord?.turn ?? 1,
          requested: requestedRecord !== undefined,
          started: startedRecord !== undefined,
          joined: subagent.joined.has(toolCallId),
          ...(childConversationId === undefined ? {} : { childConversationId }),
          ...(childSubmissionId === undefined ? {} : { childSubmissionId }),
          ...(admission === undefined ? {} : { admission }),
        }),
      );
    }
    const openToolCalls = allOpenCalls.filter((call) => !seenDelegationIds.has(call.toolCallId));

    let declaredPendingBatch: DeclaredPendingBatchEvidence | undefined;
    if (lastResponse !== undefined && !preparedTurns.has(lastResponse.turn)) {
      const declared = yield* declaredApplicationCalls(lastResponse.messages);
      if (declared.length > 0 && !declared.some((call) => settledIds.has(call.id))) {
        declaredPendingBatch = DeclaredPendingBatchEvidence.make({
          turn: lastResponse.turn,
          callCount: declared.length,
        });
      }
    }
    const approvalsPending = requested.filter((pending) => !decidedIds.has(pending.toolCallId));

    return RecoveryEvidence.make({
      conversationMaterialized: materialized,
      inputRecorded,
      abortRecorded,
      subagentLineageRecorded,
      openToolCalls,
      openDelegationCalls,
      approvalsPending,
      joinedInputCovered: hostSubmissionId === undefined ? true : hostRespondedAfterInput,
      ...(recordedSettlementOutcome === undefined ? {} : { recordedSettlementOutcome }),
      ...(declaredPendingBatch === undefined ? {} : { declaredPendingBatch }),
      ...(hostSettlementOutcome === undefined ? {} : { hostSettlementOutcome }),
    });
  });

  /**
   * The declared-but-unsettled Tool batch of the Run's LAST committed Turn (§2.4). Keyed on
   * SETTLED coverage deliberately: a call closed by a never-started/safe-retry `ToolCallResolved`
   * has authorized re-execution, so it stays in the resumed batch until its own `ToolCallSettled`
   * exists.
   */
  const pendingToolBatchFor = Effect.fn("DurableAgentRuntime.pendingToolBatchFor")(function* (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    runId: ReturnType<typeof runIdForSubmission>,
  ): Effect.fn.Return<PendingToolBatch | undefined, RunJournalError> {
    let lastResponse: { readonly turn: number; readonly messages: PersistedJson } | undefined;
    const settledByCallId = new Map<
      string,
      { readonly result: PersistedJson; readonly isFailure: boolean }
    >();
    for (const envelope of records) {
      const payload = envelope.record.payload;
      if (payload._tag === "ModelResponseRecorded" && payload.runId === runId) {
        if (lastResponse === undefined || payload.turn > lastResponse.turn) {
          lastResponse = { turn: payload.turn, messages: payload.messages };
        }
        continue;
      }
      if (payload._tag === "ToolCallSettled" && payload.runId === runId) {
        settledByCallId.set(payload.toolCallId, {
          result: payload.result,
          isFailure: payload.isFailure,
        });
      }
    }
    if (lastResponse === undefined) return undefined;
    const calls = yield* declaredApplicationCalls(lastResponse.messages);
    if (calls.length === 0) return undefined;
    const settled: Array<{ id: string; result: PersistedJson; isFailure: boolean }> = [];
    for (const call of calls) {
      const recorded = settledByCallId.get(call.id);
      if (recorded !== undefined) {
        settled.push({ id: call.id, result: recorded.result, isFailure: recorded.isFailure });
      }
    }
    if (settled.length >= calls.length) return undefined;
    return {
      turn: lastResponse.turn,
      turnId: turnIdForRun(runId, lastResponse.turn),
      calls,
      settled,
      declaredIds: new Set(calls.map((call) => call.id)),
      responseRecordId: modelResponseRecordId(runId, lastResponse.turn),
      messages: lastResponse.messages,
    };
  });

  /**
   * Records without the pending Turn's response and its partial results: the resumed Attempt's
   * canonical prompt boundary sits BEFORE the pending Turn, whose messages re-enter official
   * history through the engine's batch-resume continuation.
   */
  const withoutPendingBatch = (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    pending: PendingToolBatch,
    runId: ReturnType<typeof runIdForSubmission>,
  ): ReadonlyArray<CanonicalRecordEnvelope> =>
    records.filter((envelope) => {
      if (envelope.record.recordId === pending.responseRecordId) return false;
      const payload = envelope.record.payload;
      return !(
        payload._tag === "ToolCallSettled" &&
        payload.runId === runId &&
        pending.declaredIds.has(payload.toolCallId)
      );
    });

  /** Materialize without regressing a fence someone else already advanced. */
  const materializeAtLeast = (
    conversationId: ConversationId,
    producerEpoch: ProducerEpoch,
  ): Effect.Effect<void, ConversationStoreError> =>
    store
      .materialize(ConversationMaterialization.make({ conversationId, producerEpoch }))
      .pipe(Effect.catchTag("FenceRejected", () => Effect.void));

  /**
   * Coordinator invariant: every Conversation's first canonical record is `ConversationCreated`,
   * so `tailSequence >= 1` is the deterministic already-created check. A lost race (conflict or
   * fence) is verified against that invariant instead of being trusted blindly.
   */
  const ensureConversationCreated = Effect.fn("DurableAgentRuntime.ensureConversationCreated")(
    function* (
      conversationId: ConversationId,
      agentId: AgentId,
      definitions: DefinitionDigests,
    ): Effect.fn.Return<
      void,
      ConversationStoreError | ConversationNotMaterialized | AppendConflict | FenceRejected
    > {
      const tail = yield* store.inspectTail(ConversationTailRequest.make({ conversationId }));
      if (tail.tailSequence > 0) return;
      const record = yield* makeEnvelope(
        conversationCreatedRecordId(conversationId),
        ConversationCreated.make({ agentId, definitions }),
      );
      yield* store
        .append(
          FencedAppendRequest.make({
            conversationId,
            batch: CanonicalBatch.make({
              batchId: conversationCreatedBatchId(conversationId),
              producerId: config.producerId,
              records: [record],
            }),
            expectedTailSequence: tail.tailSequence,
            expectedTailDigest: tail.tailDigest,
            producerEpoch: tail.producerEpoch,
          }),
        )
        .pipe(
          Effect.catch((error) =>
            error._tag === "AppendConflict" || error._tag === "FenceRejected"
              ? store
                  .inspectTail(ConversationTailRequest.make({ conversationId }))
                  .pipe(
                    Effect.flatMap((current) =>
                      current.tailSequence > 0 ? Effect.void : Effect.fail(error),
                    ),
                  )
              : Effect.fail(error),
          ),
          Effect.asVoid,
        );
    },
  );

  const attemptContextFor = Effect.fn("DurableAgentRuntime.attemptContextFor")(function* (
    conversationId: ConversationId,
    producerEpoch: ProducerEpoch,
  ): Effect.fn.Return<AttemptAppendContext, ConversationStoreError | ConversationNotMaterialized> {
    const tail = yield* store.inspectTail(ConversationTailRequest.make({ conversationId }));
    const tailRef = yield* Ref.make({ sequence: tail.tailSequence, digest: tail.tailDigest });
    const gate = yield* Semaphore.make(1);
    return { conversationId, producerEpoch, tailRef, gate };
  });

  /**
   * Append context at the Conversation's CURRENT fence, without a claim. Used only where no live
   * owner can exist (an `unknown` head is never claimable, WP2 claim rule): record-identity
   * dedupe absorbs a racing recovery process, and a fence advance defers to the advancing owner.
   */
  const attemptContextAtTail = Effect.fn("DurableAgentRuntime.attemptContextAtTail")(function* (
    conversationId: ConversationId,
  ): Effect.fn.Return<AttemptAppendContext, ConversationStoreError | ConversationNotMaterialized> {
    const tail = yield* store.inspectTail(ConversationTailRequest.make({ conversationId }));
    const tailRef = yield* Ref.make({ sequence: tail.tailSequence, digest: tail.tailDigest });
    const gate = yield* Semaphore.make(1);
    return { conversationId, producerEpoch: tail.producerEpoch, tailRef, gate };
  });

  const appendBatch = (ctx: AttemptAppendContext, batch: CanonicalBatch) =>
    ctx.gate.withPermits(1)(
      Effect.gen(function* () {
        // Bounded fence refresh on a stale-tail conflict: `AppendConflict(reason: "tail")`
        // means this batch was NOT appended — another legitimate same-epoch writer advanced
        // the log after this context read its tail (a parent's establishment repair appending
        // the deterministic lineage/start records to a child Conversation while the child's
        // own Attempt runs — routine when the child lives in its own Durable Object). The
        // retry re-reads the ACTUAL tail the conflict carries and re-appends under the SAME
        // epoch: a superseded epoch still fails `FenceRejected` (DUR-006 untouched) and the
        // batch/record identity dedupe absorbs true replays. Treating a stale-tail conflict
        // as "already appended" at the tolerant call sites would let a settlement finalize
        // WITHOUT its canonical record.
        for (let refresh = 0; ; refresh++) {
          const tail = yield* Ref.get(ctx.tailRef);
          const result = yield* store
            .append(
              FencedAppendRequest.make({
                conversationId: ctx.conversationId,
                batch,
                expectedTailSequence: tail.sequence,
                expectedTailDigest: tail.digest,
                producerEpoch: ctx.producerEpoch,
              }),
            )
            .pipe(
              Effect.catchTag("AppendConflict", (conflict) =>
                conflict.reason === "tail" &&
                conflict.actualTailSequence !== undefined &&
                conflict.actualTailDigest !== undefined &&
                refresh < MAX_APPEND_FENCE_REFRESHES
                  ? Effect.as(
                      Ref.set(ctx.tailRef, {
                        sequence: conflict.actualTailSequence,
                        digest: conflict.actualTailDigest,
                      }),
                      undefined,
                    )
                  : Effect.fail(conflict),
              ),
            );
          if (result !== undefined) {
            yield* Ref.set(ctx.tailRef, {
              sequence: result.lastSequence,
              digest: result.tailDigest,
            });
            return result;
          }
        }
      }),
    );

  /** Append the canonical `AbortRequested` record; an identity conflict means it already exists. */
  const appendAbortRecord = Effect.fn("DurableAgentRuntime.appendAbortRecord")(function* (
    ctx: AttemptAppendContext,
    intent: AbortIntent,
  ) {
    const envelope = yield* makeEnvelope(
      submissionAbortRecordId(intent.submissionId),
      AbortRequested.make({
        submissionId: intent.submissionId,
        author: intent.author,
        reason: intent.reason,
      }),
    );
    yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: submissionAbortBatchId(intent.submissionId),
        producerId: config.producerId,
        records: [envelope],
      }),
    ).pipe(
      Effect.catch((error) => (error._tag === "AppendConflict" ? Effect.void : Effect.fail(error))),
      Effect.asVoid,
    );
  });

  /**
   * Append the canonical `ToolCallUnknown` audit records for the given open calls, grouped into
   * one batch per Turn (batch `mark-unknown:{sid}:{turn}`). Canonical-only: the operational
   * ledger marking is a separate step so abort settlement can record the uncertainty WITHOUT
   * blocking the lane (durability §13: abort never asserts external rollback).
   */
  const appendUnknownRecords = Effect.fn("DurableAgentRuntime.appendUnknownRecords")(function* (
    ctx: AttemptAppendContext,
    submissionId: SubmissionId,
    knownIds: Set<string>,
    calls: ReadonlyArray<OpenToolCallEvidence>,
    reason: string,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const runId = runIdForSubmission(submissionId);
    const byTurn = new Map<number, Array<OpenToolCallEvidence>>();
    for (const call of calls) {
      const group = byTurn.get(call.turn);
      if (group === undefined) byTurn.set(call.turn, [call]);
      else group.push(call);
    }
    for (const [turn, group] of byTurn) {
      const missing = group.filter(
        (call) => !knownIds.has(toolCallUnknownRecordId(runId, turn, call.toolCallId)),
      );
      const first = missing[0];
      if (first === undefined) continue;
      const envelopes: Array<RecordEnvelope> = [];
      for (const call of missing) {
        envelopes.push(
          yield* makeEnvelope(
            toolCallUnknownRecordId(runId, turn, call.toolCallId),
            ToolCallUnknown.make({
              runId,
              turn: call.turn,
              toolCallId: call.toolCallId,
              toolName: call.toolName,
              reason,
            }),
          ),
        );
      }
      const head = envelopes[0];
      if (head === undefined) continue;
      // An identity conflict means another pass already recorded the same uncertainty.
      yield* appendBatch(
        ctx,
        CanonicalBatch.make({
          batchId: markUnknownBatchId(submissionId, turn),
          producerId: config.producerId,
          records: [head, ...envelopes.slice(1)],
        }),
      ).pipe(
        Effect.catch((error) =>
          error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
        ),
        Effect.asVoid,
      );
      for (const call of missing) {
        knownIds.add(toolCallUnknownRecordId(runId, turn, call.toolCallId));
      }
    }
  });

  /**
   * Close one open Tool Call canonically: the recovered result (when one exists) settles under
   * the per-call late-settle batch (`turn-results:{runId}:{turn}:{toolCallId}`), then the
   * `ToolCallResolved` audit records who authorized the closure and how (DUR-017). Record
   * identity dedupes double-settles across the batch path and this path; an `AppendConflict`
   * means an identical closure (modulo timestamp) already committed.
   */
  const appendClosedCall = Effect.fn("DurableAgentRuntime.appendClosedCall")(function* (
    ctx: AttemptAppendContext,
    submissionId: SubmissionId,
    knownIds: Set<string>,
    call: OpenToolCallEvidence,
    closure: {
      readonly result?: { readonly value: PersistedJson; readonly isFailure: boolean } | undefined;
      readonly resolution: ToolCallResolution;
      readonly author: string;
      readonly reason: string;
    },
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const runId = runIdForSubmission(submissionId);
    const swallowIdentityConflict = (effect: ReturnType<typeof appendBatch>) =>
      effect.pipe(
        Effect.catch((error) =>
          error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
        ),
        Effect.asVoid,
      );
    if (closure.result !== undefined) {
      const settledId = toolCallSettledRecordId(runId, call.turn, call.toolCallId);
      if (!knownIds.has(settledId)) {
        const envelope = yield* makeEnvelope(
          settledId,
          ToolCallSettled.make({
            runId,
            toolCallId: call.toolCallId,
            toolName: call.toolName,
            result: closure.result.value,
            isFailure: closure.result.isFailure,
          }),
        );
        yield* swallowIdentityConflict(
          appendBatch(
            ctx,
            CanonicalBatch.make({
              batchId: toolCallResultBatchId(runId, call.turn, call.toolCallId),
              producerId: config.producerId,
              records: [envelope],
            }),
          ),
        );
        knownIds.add(settledId);
      }
    }
    const resolvedId = toolCallResolvedRecordId(runId, call.turn, call.toolCallId);
    if (!knownIds.has(resolvedId)) {
      const envelope = yield* makeEnvelope(
        resolvedId,
        ToolCallResolved.make({
          runId,
          toolCallId: call.toolCallId,
          resolution: closure.resolution,
          author: closure.author,
          reason: closure.reason,
        }),
      );
      yield* swallowIdentityConflict(
        appendBatch(
          ctx,
          CanonicalBatch.make({
            batchId: toolCallResolutionBatchId(submissionId, call.toolCallId),
            producerId: config.producerId,
            records: [envelope],
          }),
        ),
      );
      knownIds.add(resolvedId);
    }
  });

  /**
   * Reconcile-then-mark (durability §10, DUR-009) over one Submission's open ordinary Tool Calls.
   *
   * Per open call, in order of authority: a durable DUR-017 resolution intent is applied
   * canonically (recovered results settle without execution; never-happened/safe-retry authorize
   * re-execution through the batch resume); a declared `idempotent` execution class is the
   * external idempotency contract and needs no proof; otherwise the registered `ToolReconciler`
   * is consulted — `NeverStarted`/`SafeToRetry` are point-in-time proofs (nothing is recorded;
   * every later pass re-proves them), `CompletedWithResult` becomes canonical supplier truth,
   * `Uncertain` collects for Unknown marking, and a reconciler FAILURE is no proof at all (the
   * call stays open and blocked without being marked). Nothing here ever executes a handler.
   */
  const reconcileOpenCalls = Effect.fn("DurableAgentRuntime.reconcileOpenCalls")(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    snapshot: RecoverySnapshot,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    openCalls: ReadonlyArray<OpenToolCallEvidence>,
    knownIds: Set<string>,
    executionClassFor: (toolName: string) => ToolExecutionClassValue | undefined,
  ): Effect.fn.Return<OpenCallReview, DurableWorkerFailure> {
    const submissionId = submission.submissionId;
    const runId = runIdForSubmission(submissionId);
    const preparedByCallId = new Map<string, ToolCallPrepared>();
    for (const envelope of records) {
      const payload = envelope.record.payload;
      if (payload._tag === "ToolCallPrepared" && payload.runId === runId) {
        preparedByCallId.set(payload.toolCallId, payload);
      }
    }
    const intents = new Map<string, UnknownResolutionIntent>();
    for (const intent of snapshot.unknownResolutions) {
      intents.set(intent.toolCallId, intent);
    }
    const review: OpenCallReview = { uncertain: [], unproven: [], retryable: [], recovered: 0 };
    let recovered = 0;
    for (const call of openCalls) {
      const intent = intents.get(call.toolCallId);
      if (intent !== undefined) {
        switch (intent.resolution._tag) {
          case "AbortSubmission": {
            // The paired abort intent settles the Submission; the SettleAborted executor
            // records the `ToolCallUnknown` audit — nothing resolves here (durability §13).
            continue;
          }
          case "CompletedWithResult": {
            yield* appendClosedCall(ctx, submissionId, knownIds, call, {
              result: {
                value: intent.resolution.result,
                isFailure: intent.resolution.isFailure,
              },
              resolution: intent.resolution.isFailure
                ? "failed-with-error"
                : "completed-with-result",
              author: intent.author,
              reason: intent.reason,
            });
            recovered += 1;
            continue;
          }
          case "NeverHappened": {
            yield* appendClosedCall(ctx, submissionId, knownIds, call, {
              resolution: "never-started",
              author: intent.author,
              reason: intent.reason,
            });
            review.retryable.push(call);
            continue;
          }
          case "SafeToRetry": {
            yield* appendClosedCall(ctx, submissionId, knownIds, call, {
              resolution: "safe-retry",
              author: intent.author,
              reason: intent.reason,
            });
            review.retryable.push(call);
            continue;
          }
        }
      }
      if (executionClassFor(call.toolName) === "idempotent") {
        // The annotation IS the declared external idempotency contract (ADR-0004): recovery may
        // re-execute without reconciliation proof; duplicate external effects stay observable.
        review.retryable.push(call);
        continue;
      }
      const prepared = preparedByCallId.get(call.toolCallId);
      if (prepared === undefined) {
        // Open evidence without its prepared record is unreadable state: stay blocked,
        // never guess (fail-closed).
        review.unproven.push(call);
        continue;
      }
      const reconciled = yield* reconciler
        .reconcile(
          PreparedToolCallEvidence.make({
            conversationId: submission.conversationId,
            submissionId,
            runId,
            turn: prepared.turn,
            toolCallId: prepared.toolCallId,
            toolName: prepared.toolName,
            parameters: prepared.parameters,
            parametersDigest: prepared.parametersDigest,
          }),
        )
        .pipe(
          Effect.map(Option.some),
          Effect.catchTag("ToolReconcilerError", () => Effect.succeed(Option.none())),
        );
      if (Option.isNone(reconciled)) {
        review.unproven.push(call);
        continue;
      }
      const decision = reconciled.value;
      switch (decision._tag) {
        case "CompletedWithResult": {
          yield* appendClosedCall(ctx, submissionId, knownIds, call, {
            result: { value: decision.result, isFailure: decision.isFailure },
            resolution: decision.isFailure ? "failed-with-error" : "completed-with-result",
            author: RECONCILER_AUTHOR,
            reason: "A registered reconciliation policy recovered the external outcome",
          });
          recovered += 1;
          continue;
        }
        case "NeverStarted":
        case "SafeToRetry": {
          review.retryable.push(call);
          continue;
        }
        case "Uncertain": {
          review.uncertain.push(call);
          continue;
        }
      }
    }
    return { ...review, recovered };
  });

  /**
   * Record the Unknown Outcomes durably: canonical `ToolCallUnknown` audit records FIRST
   * (history is the recovery truth, DUR-015), then the ownership-free ledger marking that blocks
   * the lane until the authorized DUR-017 resolution path covers every marked call.
   */
  const markCallsUnknown = Effect.fn("DurableAgentRuntime.markCallsUnknown")(function* (
    ctx: AttemptAppendContext,
    submissionId: SubmissionId,
    knownIds: Set<string>,
    calls: ReadonlyArray<OpenToolCallEvidence>,
    reason: string,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const first = calls[0];
    if (first === undefined) return;
    yield* appendUnknownRecords(ctx, submissionId, knownIds, calls, reason);
    yield* ledger.markUnknown(
      MarkUnknownRequest.make({
        submissionId,
        toolCallIds: [first.toolCallId, ...calls.slice(1).map((call) => call.toolCallId)],
        reason,
      }),
    );
  });

  /**
   * Plan step 2: append the deterministic `UserInputRecorded` record (batch idempotency makes it
   * exactly-once canonical, DUR-007) and mark it applied. When canonical history already carries
   * the record — an earlier Attempt crashed after its append — only the ledger marker is repaired
   * (DUR-015/DUR-016).
   */
  const applyCanonicalInput = Effect.fn("DurableAgentRuntime.applyCanonicalInput")(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    tokenRef: Ref.Ref<OwnershipToken>,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    inputApplied: RecoverySnapshot["inputApplied"],
  ) {
    const submissionId = submission.submissionId;
    const recordId = submissionInputRecordId(submissionId);
    const existing = records.find((envelope) => envelope.record.recordId === recordId);
    if (existing !== undefined) {
      if (inputApplied === undefined) {
        const ownershipToken = yield* Ref.get(tokenRef);
        yield* ledger.markInputApplied(
          MarkInputAppliedRequest.make({
            submissionId,
            ownershipToken,
            recordId,
            sequence: existing.sequence,
          }),
        );
      }
      return;
    }
    const envelope = yield* makeEnvelope(
      recordId,
      UserInputRecorded.make({
        submissionId,
        kind: "user",
        runId: runIdForSubmission(submissionId),
        input: submission.inputPayload,
      }),
    );
    const result = yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: submissionInputBatchId(submissionId),
        producerId: config.producerId,
        records: [envelope],
      }),
    );
    yield* hit("input:after-canonical-append");
    const ownershipToken = yield* Ref.get(tokenRef);
    yield* ledger.markInputApplied(
      MarkInputAppliedRequest.make({
        submissionId,
        ownershipToken,
        recordId,
        sequence: result.firstSequence,
      }),
    );
  });

  /**
   * Settle ONE joined Submission with its host Run's outcome (plan §2.5, DUR-002: every accepted
   * Submission is owed its own settlement). The same recoverable reserve → append → finalize
   * sequence as `terminalize`, with two joined-specific rules: the canonical record's `runId` is
   * the HOST Run (the joined input was consumed there), and the reservation is authorized by the
   * recorded host linkage instead of lane ownership — a `joined` lane is never worker-claimable,
   * so no ownership token can exist for it. Each step is idempotent; recovery completes any
   * prefix (`AppendReservedSettlement` / `FinalizeLedgerFromHistory` / `SettleJoinedWithHost`).
   */
  /**
   * Cross-lane drive-forward after one child Submission settles (spec §12 step 10): the child's
   * canonical Settlement is already durable, so the idempotent `recordChildSettled` wake is the
   * durable notification — `suspended(WaitingForChild) → input-applied` once every listed child
   * settled — and the parent-lane wake hint is liveness only. Invoked from every settlement
   * finalization path; a crash between the child's finalize and this notification is repaired by
   * the `ResumeWaitingParent` recovery row (a dropped wake is never a lost obligation).
   */
  const notifyParentOfChildSettlement = Effect.fn(
    "DurableAgentRuntime.notifyParentOfChildSettlement",
  )(function* (submission: SubmissionSnapshot): Effect.fn.Return<void, LedgerError> {
    const linkage = submission.parentLinkage;
    if (linkage === undefined) return;
    yield* ledger.recordChildSettled(
      ChildSettledNotification.make({
        parentSubmissionId: linkage.parentSubmissionId,
        childSubmissionId: submission.submissionId,
      }),
    );
    const parent = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: linkage.parentSubmissionId }),
    );
    if (Option.isSome(parent)) {
      yield* wake.notify(parent.value.conversationId);
    }
  });

  const settleOneJoined = Effect.fn("DurableAgentRuntime.settleOneJoined")(function* (
    ctx: AttemptAppendContext,
    hostSubmissionId: SubmissionId,
    outcome: SettlementOutcome,
    joined: RecoverySnapshot,
  ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
    const submission = joined.submission;
    const submissionId = submission.submissionId;
    const settlementId = submissionSettlementId(submissionId);
    let record: RecordEnvelope;
    if (joined.reservation !== undefined) {
      // A prior pass already reserved the exact outcome: re-append the STORED record so the
      // batch replay is byte-identical (DUR-011).
      record = joined.reservation.record;
    } else {
      const payload = SubmissionSettled.make({
        submissionId,
        settlementId,
        receiptId: submission.receiptId,
        outcome,
        runId: runIdForSubmission(hostSubmissionId),
      });
      const envelope = yield* makeEnvelope(submissionSettlementRecordId(submissionId), payload);
      // The envelope was constructed from validated parts, so an encode failure is a defect.
      const encoded = yield* Schema.encodeEffect(RecordEnvelope)(envelope).pipe(Effect.orDie);
      const recordDigest = yield* withCrypto(digestJson(encoded));
      const reserved = yield* ledger
        .reserveSettlement(
          SettlementReservation.make({
            submissionId,
            ownershipToken: JOINED_SETTLEMENT_TOKEN,
            settlementId,
            outcome,
            record: envelope,
            recordDigest,
          }),
        )
        .pipe(
          // A racing pass reserved first (its envelope differs only by `createdAt`): the
          // stored reservation with the same host-derived outcome is the exact record owed.
          Effect.catchTag("SettlementConflict", (conflict) =>
            Effect.gen(function* () {
              const current = yield* ledger.loadRecoverySnapshot(
                RecoverySnapshotRequest.make({ submissionId }),
              );
              const reservation = current.reservation;
              if (reservation === undefined || reservation.outcome !== outcome) {
                return yield* conflict;
              }
              return reservation;
            }),
          ),
        );
      record = reserved.record;
      yield* hit("terminalize:after-reserve");
    }
    yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: submissionSettlementBatchId(submissionId),
        producerId: config.producerId,
        records: [record],
      }),
    ).pipe(
      Effect.catch((error) => (error._tag === "AppendConflict" ? Effect.void : Effect.fail(error))),
      Effect.asVoid,
    );
    yield* hit("terminalize:after-canonical-append");
    const settlement = yield* ledger.finalizeSettlement(
      SettlementFinalization.make({ submissionId, settlementId }),
    );
    yield* wake.notify(submission.conversationId);
    yield* notifyParentOfChildSettlement(submission);
    return settlement;
  });

  /**
   * Terminalize joined-settlement loop (plan §2.5): after the host's own reserve → append →
   * finalize, every Submission joined to the host settles with the host outcome. `terminalizing`
   * rows are a prior pass's crashed joined settlement (reservation committed, finalize lost) and
   * complete here too; `joining` rows were never consumed and are recovery's to revert, and
   * settled rows are done. A crash anywhere in the loop leaves a classifiable prefix
   * (`SettleJoinedWithHost` / `AppendReservedSettlement` finish the rest).
   */
  const settleJoinedSubmissions = Effect.fn("DurableAgentRuntime.settleJoinedSubmissions")(
    function* (
      ctx: AttemptAppendContext,
      hostSubmissionId: SubmissionId,
      outcome: SettlementOutcome,
    ): Effect.fn.Return<void, DurableWorkerFailure> {
      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: hostSubmissionId }),
      );
      for (const join of snapshot.joins) {
        if (join.state !== "joined" && join.state !== "terminalizing") continue;
        const joinedSnapshot = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: join.submissionId }),
        );
        if (joinedSnapshot.submission.state === "settled") continue;
        yield* settleOneJoined(ctx, hostSubmissionId, outcome, joinedSnapshot);
      }
    },
  );

  /**
   * Terminalization (durability §12, DUR-011): reserve the single exact settlement record, append
   * that exact record canonically, finalize the ledger, release the lane, and hint waiters.
   * Submissions joined to this host Run settle with the host outcome immediately after
   * (plan §2.5); recovery completes any prefix of that loop.
   */
  const terminalize = Effect.fn("DurableAgentRuntime.terminalize")(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    tokenRef: Ref.Ref<OwnershipToken>,
    outcome: AttemptOutcome,
    includeRunId: boolean,
  ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
    const submissionId = submission.submissionId;
    const settlementId = submissionSettlementId(submissionId);
    const payload = SubmissionSettled.make({
      submissionId,
      settlementId,
      receiptId: submission.receiptId,
      outcome: outcome._tag,
      ...(includeRunId ? { runId: runIdForSubmission(submissionId) } : {}),
      ...(outcome._tag === "aborted" ? {} : { result: outcome.result }),
      ...(outcome._tag === "completed" && outcome.finishReason !== undefined
        ? { finishReason: outcome.finishReason }
        : {}),
    });
    const record = yield* makeEnvelope(submissionSettlementRecordId(submissionId), payload);
    // The envelope was constructed from validated parts, so an encode failure is a defect.
    const encoded = yield* Schema.encodeEffect(RecordEnvelope)(record).pipe(Effect.orDie);
    const recordDigest = yield* withCrypto(digestJson(encoded));
    const ownershipToken = yield* Ref.get(tokenRef);
    const reserved = yield* ledger.reserveSettlement(
      SettlementReservation.make({
        submissionId,
        ownershipToken,
        settlementId,
        outcome: outcome._tag,
        record,
        recordDigest,
      }),
    );
    yield* hit("terminalize:after-reserve");
    yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: submissionSettlementBatchId(submissionId),
        producerId: config.producerId,
        records: [reserved.record],
      }),
    ).pipe(
      Effect.catch((error) => (error._tag === "AppendConflict" ? Effect.void : Effect.fail(error))),
      Effect.asVoid,
    );
    yield* hit("terminalize:after-canonical-append");
    const settlement = yield* ledger.finalizeSettlement(
      SettlementFinalization.make({ submissionId, settlementId }),
    );
    yield* wake.notify(submission.conversationId);
    yield* notifyParentOfChildSettlement(submission);
    yield* settleJoinedSubmissions(ctx, submissionId, outcome._tag);
    return settlement;
  });

  /** Complete a previously reserved settlement: append the EXACT reserved record, then finalize. */
  const completeReservation = Effect.fn("DurableAgentRuntime.completeReservation")(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    reservation: NonNullable<RecoverySnapshot["reservation"]>,
    alreadyRecorded: boolean,
  ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
    if (!alreadyRecorded) {
      yield* appendBatch(
        ctx,
        CanonicalBatch.make({
          batchId: submissionSettlementBatchId(submission.submissionId),
          producerId: config.producerId,
          records: [reservation.record],
        }),
      ).pipe(
        Effect.catch((error) =>
          error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
        ),
        Effect.asVoid,
      );
      yield* hit("terminalize:after-canonical-append");
    }
    const settlement = yield* ledger.finalizeSettlement(
      SettlementFinalization.make({
        submissionId: submission.submissionId,
        settlementId: reservation.settlementId,
      }),
    );
    yield* wake.notify(submission.conversationId);
    yield* notifyParentOfChildSettlement(submission);
    yield* settleJoinedSubmissions(ctx, submission.submissionId, settlement.outcome);
    return settlement;
  });

  /** Canonical settlement exists: rebuild the ledger from history, never the reverse (DUR-015). */
  const finalizeFromHistory = Effect.fn("DurableAgentRuntime.finalizeFromHistory")(function* (
    submission: SubmissionSnapshot,
    settlementId: Settlement["settlementId"],
  ): Effect.fn.Return<Settlement, LedgerError | SettlementConflict> {
    const settlement = yield* ledger.finalizeSettlement(
      SettlementFinalization.make({ submissionId: submission.submissionId, settlementId }),
    );
    yield* wake.notify(submission.conversationId);
    yield* notifyParentOfChildSettlement(submission);
    return settlement;
  });

  /**
   * Durable abort of owned work: canonical `AbortRequested` first, then `ToolCallUnknown` audit
   * records for every open ordinary Tool Call (abort settles the obligation but never asserts
   * external rollback, durability §13), then settle aborted.
   */
  const settleAborted = Effect.fn("DurableAgentRuntime.settleAborted")(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    tokenRef: Ref.Ref<OwnershipToken>,
    intent: AbortIntent,
    evidence: RecoveryEvidence,
    knownIds: Set<string>,
  ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
    if (!evidence.abortRecorded) {
      yield* appendAbortRecord(ctx, intent);
    }
    if (evidence.openToolCalls.length > 0) {
      yield* appendUnknownRecords(
        ctx,
        submission.submissionId,
        knownIds,
        evidence.openToolCalls,
        "The Submission was aborted while this ordinary Tool call had no canonical outcome; abort never asserts external rollback",
      );
    }
    return yield* terminalize(
      ctx,
      submission,
      tokenRef,
      { _tag: "aborted" },
      evidence.inputRecorded,
    );
  });

  /** Map a ledger conflict outside `DurableWorkerFailure` into the coordinator failure family. */
  const conflictToLedgerError =
    (operation: string) =>
    (conflict: { readonly _tag: string; readonly message?: string }): LedgerError =>
      LedgerError.make({
        operation,
        message: `${conflict._tag}${conflict.message === undefined ? "" : `: ${conflict.message}`}`,
        cause: conflict,
      });

  /**
   * Append the child Conversation's immutable lineage record (spec §12 step 6, §11): its own
   * single-record batch under `subagent-lineage:{childConversationId}` so the generic
   * `conversation-created:{cid}` batch identity is never contradicted. Idempotent by record
   * identity; a fence advance defers to a log that provably carries the record already.
   */
  const ensureChildLineage = Effect.fn("DurableAgentRuntime.ensureChildLineage")(function* (
    parent: SubmissionSnapshot,
    request: SubagentRequested,
    childRecords: ReadonlyArray<CanonicalRecordEnvelope>,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const recordId = subagentLineageRecordId(request.childConversationId);
    if (childRecords.some((envelope) => envelope.record.recordId === recordId)) return;
    const envelope = yield* makeEnvelope(
      recordId,
      SubagentLineageRecorded.make({
        parentLink: SubagentParentLink.make({
          delegationId: request.delegationId,
          parentAgentId: parent.agentId,
          parentConversationId: parent.conversationId,
          parentRunId: request.runId,
          parentToolCallId: request.toolCallId,
          depth: CHILD_DELEGATION_DEPTH,
        }),
        parentSubmissionId: parent.submissionId,
        childDefinitionDigests: request.targetDigests,
        childInputDigest: request.childInputDigest,
        grantDigest: request.grantDigest,
      }),
    );
    const tail = yield* store.inspectTail(
      ConversationTailRequest.make({ conversationId: request.childConversationId }),
    );
    yield* store
      .append(
        FencedAppendRequest.make({
          conversationId: request.childConversationId,
          batch: CanonicalBatch.make({
            batchId: subagentLineageBatchId(request.childConversationId),
            producerId: config.producerId,
            records: [envelope],
          }),
          expectedTailSequence: tail.tailSequence,
          expectedTailDigest: tail.tailDigest,
          producerEpoch: tail.producerEpoch,
        }),
      )
      .pipe(
        Effect.catch((error) =>
          error._tag === "AppendConflict" || error._tag === "FenceRejected"
            ? // A racing establishment pass (or the child's own claimed worker) advanced the
              // log; the deterministic identity means the record either exists or the next
              // pass re-proves it — verify instead of trusting the race blindly.
              readAll(request.childConversationId).pipe(
                Effect.flatMap((current) =>
                  current.some((candidate) => candidate.record.recordId === recordId)
                    ? Effect.void
                    : Effect.fail(error),
                ),
              )
            : Effect.fail(error),
        ),
        Effect.asVoid,
      );
  });

  /** Where one idempotent child admission pass ended (spec §12 steps 4-8). */
  type ChildAdmissionOutcome =
    | {
        readonly _tag: "established";
        readonly childSubmissionId: SubmissionId;
        readonly receiptId: ReceiptId;
      }
    | { readonly _tag: "indeterminate"; readonly reason: string };

  /**
   * Complete (or replay) the child-admission half of establishment from the canonical
   * `SubagentRequested` payload alone — no live delegation handler is required (D3): the
   * `resolveAdmission` tri-state gate (SUB-031), the idempotency-keyed `admit` with immutable
   * parent linkage, child Conversation materialization plus the immutable lineage record, and
   * readiness. Every step is get-or-create; a replay converges on the one existing child
   * (SUB-016) and an `indeterminate` answer NEVER admits a second child.
   */
  const establishChildFromRequest = Effect.fn("DurableAgentRuntime.establishChildFromRequest")(
    function* (
      parent: SubmissionSnapshot,
      request: SubagentRequested,
    ): Effect.fn.Return<ChildAdmissionOutcome, DurableWorkerFailure> {
      const principal = decodePrincipalSync(request.childPrincipal);
      const idempotencyKey = decodeIdempotencyKeySync(request.childIdempotencyKey);
      const resolution = yield* ledger.resolveAdmission(
        SubmissionLookupByKey.make({
          conversationId: request.childConversationId,
          principal,
          idempotencyKey,
        }),
      );
      let childSubmissionId: SubmissionId;
      let receiptId: ReceiptId;
      switch (resolution._tag) {
        case "Indeterminate": {
          return { _tag: "indeterminate", reason: resolution.reason };
        }
        case "NotAdmitted": {
          const admitted: AdmissionResult = yield* ledger
            .admit(
              AdmissionRequest.make({
                conversationId: request.childConversationId,
                principal,
                idempotencyKey,
                agentId: request.targetAgentId,
                agentDigests: request.targetDigests,
                deploymentId: config.deploymentId,
                inputPayload: request.childInput,
                inputDigest: request.childInputDigest,
                parentLinkage: ParentLinkage.make({
                  parentSubmissionId: parent.submissionId,
                  parentToolCallId: request.toolCallId,
                }),
              }),
            )
            .pipe(
              Effect.catchTag(
                "AdmissionConflict",
                conflictToLedgerError("establishChildFromRequest"),
              ),
            );
          childSubmissionId = admitted.submissionId;
          receiptId = admitted.receiptId;
          break;
        }
        case "Admitted": {
          // The one existing child: verify the immutable admission facts against the canonical
          // request before reattaching — a divergent row can never be "the same child"
          // (fail-closed; identifiers are never capabilities, D10).
          const child = resolution.submission;
          const linkage = child.parentLinkage;
          if (
            child.agentId !== request.targetAgentId ||
            !definitionDigestsEqual(child.agentDigests, request.targetDigests) ||
            child.inputDigest !== request.childInputDigest ||
            linkage === undefined ||
            linkage.parentSubmissionId !== parent.submissionId ||
            linkage.parentToolCallId !== request.toolCallId
          ) {
            return yield* LedgerError.make({
              operation: "establishChildFromRequest",
              message: `The admitted child ${child.submissionId} diverges from the canonical SubagentRequested record for Tool Call ${request.toolCallId}; establishment fails closed (SUB-016)`,
            });
          }
          childSubmissionId = child.submissionId;
          receiptId = child.receiptId;
          break;
        }
      }
      yield* hit("subagent:after-admit");
      yield* materializeAtLeast(request.childConversationId, ZERO_EPOCH);
      yield* ensureConversationCreated(
        request.childConversationId,
        request.targetAgentId,
        request.targetDigests,
      );
      const childRead = yield* readAllTolerant(request.childConversationId);
      yield* ensureChildLineage(parent, request, childRead.records);
      yield* ledger.markReady(MarkReadyRequest.make({ submissionId: childSubmissionId }));
      yield* hit("subagent:after-child-ready");
      yield* wake.notify(request.childConversationId);
      return { _tag: "established", childSubmissionId, receiptId };
    },
  );

  /**
   * Bounded usage summary rebuilt from canonical child evidence (D11 structural dimensions).
   * The two finite counters always satisfy the persistence bounds, so a decode failure is a
   * defect (`orDie`), matching the file's `annotateRepair` pattern for provably-valid inputs.
   */
  const childUsageSummaryOf = (
    childRecords: ReadonlyArray<CanonicalRecordEnvelope>,
    childRunId: RunId,
  ): Effect.Effect<PersistedJson> => {
    let turns = 0;
    let toolCalls = 0;
    for (const envelope of childRecords) {
      const payload = envelope.record.payload;
      if (payload._tag === "ModelResponseRecorded" && payload.runId === childRunId) turns += 1;
      if (payload._tag === "ToolCallSettled" && payload.runId === childRunId) toolCalls += 1;
    }
    return decodePersisted({ turns, toolCalls }).pipe(Effect.orDie);
  };

  /** The coordinator's bounded `{errorTag, message}` projection of a non-completed child. */
  const boundedChildFailureResult = (
    payload: SubmissionSettled,
    childSubmissionId: SubmissionId,
  ): Effect.Effect<PersistedJson> => {
    if (payload.outcome === "failed" && payload.result !== undefined) {
      return Effect.succeed(payload.result);
    }
    return decodePersisted({
      errorTag: payload.outcome === "aborted" ? "SubagentAborted" : "ChildRunFailed",
      message:
        payload.outcome === "aborted"
          ? `Attached child ${childSubmissionId} settled aborted`
          : `Attached child ${childSubmissionId} settled failed without a recorded failure payload`,
    }).pipe(Effect.orDie);
  };

  /** One verified child Settlement, ready to join (spec §12 join steps 1-3). */
  interface VerifiedChildSettlement {
    readonly outcome: SettlementOutcome;
    /** Child terminal output for `completed`; the bounded `{errorTag, message}` projection otherwise. */
    readonly encodedResult: PersistedJson;
    readonly settlement: SubmissionSettled;
    readonly childRecords: ReadonlyArray<CanonicalRecordEnvelope>;
  }

  type ChildVerification =
    | { readonly _tag: "verified"; readonly value: VerifiedChildSettlement }
    | { readonly _tag: "mismatch"; readonly message: string };

  /**
   * §1.6 join verification, fail-closed (SUB-019/SUB-023, D10): the child's CANONICAL
   * Settlement and lineage records are read from the child Conversation Log — cached ledger
   * state never fabricates a Settlement — and every identity/digest is checked against the
   * canonical `SubagentRequested` payload: Parent Link identity, target agent, stored
   * definition digests, input/grant digests, settlement identity, and the settlement record
   * digest pinned by the child's reservation. Any mismatch is a typed verification failure.
   */
  const verifySettledChild = Effect.fn("DurableAgentRuntime.verifySettledChild")(function* (
    parent: SubmissionSnapshot,
    request: SubagentRequested,
    childSubmissionId: SubmissionId,
  ): Effect.fn.Return<ChildVerification, DurableWorkerFailure> {
    const mismatch = (message: string): ChildVerification => ({ _tag: "mismatch", message });
    // The child's lane state crosses the store boundary through `lookup` — a status check on
    // the child's owning ledger — because recovery snapshots are lane-local by construction:
    // on Cloudflare the child Conversation lives in a different Durable Object whose
    // operational rows are not readable across the boundary (deployment §11). The child's
    // CANONICAL Settlement, read below from its Conversation Log, remains the only
    // cross-lane verification authority (spec §12 join step 1, SUB-019, DUR-015).
    const childLookup = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: childSubmissionId }),
    );
    if (Option.isNone(childLookup)) {
      return yield* LedgerError.make({
        operation: "verifySettledChild",
        message: `Attached child ${childSubmissionId} is unknown to its owning Submission Ledger`,
      });
    }
    const child = childLookup.value;
    if (child.conversationId !== request.childConversationId) {
      return mismatch("The child Conversation does not match the intended child identity");
    }
    if (child.agentId !== request.targetAgentId) {
      return mismatch("The child Agent identity does not match the declared delegation target");
    }
    if (!definitionDigestsEqual(child.agentDigests, request.targetDigests)) {
      return mismatch("The child definition digests do not match the stored target digests");
    }
    if (child.inputDigest !== request.childInputDigest) {
      return mismatch("The child input digest does not match the canonical request");
    }
    const linkage = child.parentLinkage;
    if (
      linkage === undefined ||
      linkage.parentSubmissionId !== parent.submissionId ||
      linkage.parentToolCallId !== request.toolCallId
    ) {
      return mismatch("The child admission linkage does not name this parent Tool Call");
    }
    const childRecords = yield* readAll(request.childConversationId);
    const lineageEnvelope = childRecords.find(
      (envelope) =>
        envelope.record.recordId === subagentLineageRecordId(request.childConversationId),
    );
    const lineage = lineageEnvelope?.record.payload;
    if (lineage === undefined || lineage._tag !== "SubagentLineageRecorded") {
      return mismatch("The child Conversation carries no immutable lineage record");
    }
    if (
      lineage.parentLink.delegationId !== request.delegationId ||
      lineage.parentLink.parentAgentId !== parent.agentId ||
      lineage.parentLink.parentConversationId !== parent.conversationId ||
      lineage.parentLink.parentRunId !== request.runId ||
      lineage.parentLink.parentToolCallId !== request.toolCallId ||
      lineage.parentSubmissionId !== parent.submissionId
    ) {
      return mismatch("The child Parent Link does not name exactly this parent Run and Tool Call");
    }
    if (
      !definitionDigestsEqual(lineage.childDefinitionDigests, request.targetDigests) ||
      lineage.childInputDigest !== request.childInputDigest ||
      lineage.grantDigest !== request.grantDigest
    ) {
      return mismatch("The child lineage digests do not match the canonical request");
    }
    const settlementEnvelope = childRecords.find(
      (envelope) => envelope.record.recordId === submissionSettlementRecordId(childSubmissionId),
    );
    const settlement = settlementEnvelope?.record.payload;
    if (settlement === undefined || settlement._tag !== "SubmissionSettled") {
      return mismatch("The child has no canonical Settlement record");
    }
    if (
      settlement.submissionId !== childSubmissionId ||
      settlement.settlementId !== submissionSettlementId(childSubmissionId) ||
      settlement.receiptId !== child.receiptId
    ) {
      return mismatch("The child Settlement identity does not match the child Receipt");
    }
    // No parent-side crosscheck against the child's settlement RESERVATION row exists here:
    // the reservation lives in the child's own store, invisible across the Object boundary on
    // Cloudflare, and its byte-identity with the canonical record is the child store's own
    // conformance-tested reserve→append→finalize invariant (DUR-011) — never a parent
    // obligation. The canonical Settlement verified above is the sole cross-lane authority.
    if (settlement.outcome === "completed" && settlement.result === undefined) {
      return mismatch("The completed child Settlement carries no terminal output");
    }
    const encodedResult =
      settlement.outcome === "completed" && settlement.result !== undefined
        ? settlement.result
        : yield* boundedChildFailureResult(settlement, childSubmissionId);
    return {
      _tag: "verified",
      value: { outcome: settlement.outcome, encodedResult, settlement, childRecords },
    };
  });

  /**
   * Apply the frozen accounting decision to one reservation (spec §12 join step 6, DUR-015):
   * `beginChildBudgetRelease` freezes it exactly once (an identical replay is a no-op) and
   * `releaseChildBudget` applies it exactly once — budget stays unavailable until repair, never
   * available twice.
   */
  const applyReservationRelease = Effect.fn("DurableAgentRuntime.applyReservationRelease")(
    function* (
      reservationId: ChildReservationId,
      accounting: PersistedJson,
    ): Effect.fn.Return<void, DurableWorkerFailure> {
      yield* ledger
        .beginChildBudgetRelease(BeginChildBudgetReleaseRequest.make({ reservationId, accounting }))
        .pipe(
          Effect.catchTag(
            "ChildReservationConflict",
            conflictToLedgerError("beginChildBudgetRelease"),
          ),
        );
      yield* hit("subagent:after-release-pending");
      yield* ledger
        .releaseChildBudget(ReleaseChildBudgetRequest.make({ reservationId }))
        .pipe(
          Effect.catchTag("ChildReservationConflict", conflictToLedgerError("releaseChildBudget")),
        );
      yield* hit("subagent:after-release");
    },
  );

  /**
   * Release a provably-childless reservation exactly once (spec §13 "reservation exists,
   * request absent"): freeze the deterministic zero-consumed decision — a conflict means a
   * different decision already froze first, and the release below applies THAT frozen decision.
   */
  const releaseOrphanReservation = Effect.fn("DurableAgentRuntime.releaseOrphanReservation")(
    function* (reservationId: ChildReservationId): Effect.fn.Return<void, DurableWorkerFailure> {
      yield* ledger
        .beginChildBudgetRelease(
          BeginChildBudgetReleaseRequest.make({
            reservationId,
            accounting: ORPHAN_ZERO_CONSUMED_ACCOUNTING,
          }),
        )
        .pipe(
          Effect.catchTag("ChildReservationConflict", () => Effect.void),
          Effect.asVoid,
        );
      yield* hit("subagent:after-release-pending");
      yield* ledger
        .releaseChildBudget(ReleaseChildBudgetRequest.make({ reservationId }))
        .pipe(
          Effect.catchTag("ChildReservationConflict", conflictToLedgerError("releaseChildBudget")),
        );
      yield* hit("subagent:after-release");
    },
  );

  /**
   * Coordinator-side settlement join of one settled child under a parent ABORT (spec §13.1):
   * the parent settles aborted only after every attached child's terminal outcome is joined,
   * and no delegation handler runs on the abort path, so the framework itself appends the
   * atomic `[SubagentJoined, ToolCallSettled]` batch with the bounded parent-aborted projection
   * and the deterministic conservative accounting derived from the stored allocation.
   */
  const joinSettledChildForAbort = Effect.fn("DurableAgentRuntime.joinSettledChildForAbort")(
    function* (
      ctx: AttemptAppendContext,
      parent: SubmissionSnapshot,
      knownIds: Set<string>,
      subagent: SubagentCallRecords,
      reservation: ChildBudgetReservationSnapshot,
      toolCallId: ToolCallId,
      childSubmissionId: SubmissionId,
    ): Effect.fn.Return<void, DurableWorkerFailure> {
      const runId = runIdForSubmission(parent.submissionId);
      const request = subagent.requested.get(toolCallId);
      if (request === undefined) {
        return yield* LedgerError.make({
          operation: "joinSettledChildForAbort",
          message: `Tool Call ${toolCallId} has an attached child but no canonical SubagentRequested record`,
        });
      }
      const joinedRecordId = subagentJoinedRecordId(runId, toolCallId);
      let finalAccounting: PersistedJson;
      const existing = subagent.joined.get(toolCallId);
      if (existing !== undefined) {
        finalAccounting = existing.finalAccounting;
      } else {
        const verification = yield* verifySettledChild(parent, request, childSubmissionId);
        if (verification._tag === "mismatch") {
          return yield* LedgerError.make({
            operation: "joinSettledChildForAbort",
            message: `Child Settlement verification failed for Tool Call ${toolCallId}: ${verification.message}`,
          });
        }
        const verified = verification.value;
        // Static-shape projections; a bounds failure is a defect, matching `annotateRepair`.
        const boundedResult = yield* decodePersisted({
          errorTag: "SubagentParentAborted",
          message: boundedText(
            `The parent Submission aborted; attached child ${childSubmissionId} settled ${verified.outcome}`,
          ),
        }).pipe(Effect.orDie);
        finalAccounting = yield* decodePersisted({
          basis: "aborted-conservative",
          allocation: reservation.allocation,
        }).pipe(Effect.orDie);
        const childRunId = runIdForSubmission(childSubmissionId);
        const joinedPayload = SubagentJoined.make({
          runId,
          toolCallId,
          childSubmissionId,
          childSettlementId: verified.settlement.settlementId,
          childOutcome: verified.outcome,
          childResultDigest: yield* withCrypto(digestJson(verified.settlement.result ?? null)),
          projectedResultDigest: yield* withCrypto(digestJson(boundedResult)),
          usageSummary: yield* childUsageSummaryOf(verified.childRecords, childRunId),
          reservationId: reservation.reservationId,
          finalAccounting,
        });
        const settledRecordId = toolCallSettledRecordId(runId, request.turn, toolCallId);
        const joinedEnvelope = yield* makeEnvelope(joinedRecordId, joinedPayload);
        const settledEnvelope = yield* makeEnvelope(
          settledRecordId,
          ToolCallSettled.make({
            runId,
            toolCallId,
            toolName: subagent.preparedNames.get(toolCallId) ?? request.delegationId,
            result: boundedResult,
            isFailure: true,
          }),
        );
        if (!knownIds.has(joinedRecordId)) {
          yield* appendBatch(
            ctx,
            CanonicalBatch.make({
              batchId: subagentJoinBatchId(runId, toolCallId),
              producerId: config.producerId,
              records: [joinedEnvelope, settledEnvelope],
            }),
          ).pipe(
            Effect.catch((error) =>
              error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
            ),
            Effect.asVoid,
          );
          knownIds.add(joinedRecordId);
          knownIds.add(settledRecordId);
          subagent.joined.set(toolCallId, joinedPayload);
          yield* hit("subagent:after-join-append");
        }
      }
      yield* applyReservationRelease(reservation.reservationId, finalAccounting);
    },
  );

  /**
   * Complete every joined-but-unreleased reservation BEFORE the parent settles (spec §12 join
   * step 6): the canonical `SubagentJoined` accounting authorizes the release (DUR-015), and a
   * parent that settled first would strand the repair — a settled lane classifies `NoAction`.
   * Returns whether any reservation remains unreleased (a reserved row without a canonical
   * join), which must block the settlement (spec §13: a parent never settles across an open
   * child obligation).
   */
  const completeJoinedReleases = Effect.fn("DurableAgentRuntime.completeJoinedReleases")(function* (
    submission: SubmissionSnapshot,
  ): Effect.fn.Return<boolean, DurableWorkerFailure> {
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
    );
    if (snapshot.childReservations.length === 0) return false;
    const records = yield* readAll(submission.conversationId);
    const subagent = subagentRecordsOf(records, runIdForSubmission(submission.submissionId));
    let open = false;
    for (const reservation of snapshot.childReservations) {
      if (reservation.status === "released") continue;
      const joined = subagent.joined.get(reservation.parentToolCallId);
      if (joined !== undefined) {
        yield* applyReservationRelease(reservation.reservationId, joined.finalAccounting);
        continue;
      }
      if (reservation.status === "releasePending" && reservation.accounting !== undefined) {
        yield* applyReservationRelease(reservation.reservationId, reservation.accounting);
        continue;
      }
      open = true;
    }
    return open;
  });

  /** Where the request-abort-and-join pass over attached children ended (spec §13.1). */
  type ChildAbortDisposition = "clear" | "waiting" | "blocked";

  /**
   * Request-abort-and-join over every attached child of an aborting parent (spec §13.1,
   * SUB-022): joined-but-unreleased reservations finish their idempotent release; settled
   * children join coordinator-side; nonterminal children receive the ONE idempotent durable
   * abort command (the recorded `AbortIntent` row IS the propagation marker, DUR-012) and the
   * parent suspends `waitingForChild` for their joins; an admitted-but-unlinked child is
   * reattached first; a provably-childless reservation releases exactly once; an indeterminate
   * admission blocks honestly — never a second admission, never a fabricated settlement.
   */
  const abortAttachedChildren = Effect.fn("DurableAgentRuntime.abortAttachedChildren")(function* (
    ctx: AttemptAppendContext,
    parent: SubmissionSnapshot,
    tokenRef: Ref.Ref<OwnershipToken>,
    knownIds: Set<string>,
  ): Effect.fn.Return<ChildAbortDisposition, DurableWorkerFailure> {
    const submissionId = parent.submissionId;
    const runId = runIdForSubmission(submissionId);
    while (true) {
      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId }),
      );
      if (snapshot.childReservations.length === 0) return "clear";
      const records = yield* readAll(parent.conversationId);
      for (const envelope of records) knownIds.add(envelope.record.recordId);
      const subagent = subagentRecordsOf(records, runId);
      const waiting: Array<WaitingChild> = [];
      let blocked = false;
      for (const reservation of snapshot.childReservations) {
        if (reservation.status === "released") continue;
        const toolCallId = reservation.parentToolCallId;
        const joined = subagent.joined.get(toolCallId);
        if (joined !== undefined) {
          yield* applyReservationRelease(reservation.reservationId, joined.finalAccounting);
          continue;
        }
        if (reservation.status === "releasePending") {
          // The decision is already frozen (join or orphan): finish the idempotent release —
          // NEVER re-freeze, a divergent second decision would conflict.
          yield* applyReservationRelease(
            reservation.reservationId,
            reservation.accounting ?? ORPHAN_ZERO_CONSUMED_ACCOUNTING,
          );
          continue;
        }
        const request = subagent.requested.get(toolCallId);
        let childSubmissionId =
          subagent.started.get(toolCallId)?.childSubmissionId ?? reservation.childSubmissionId;
        if (childSubmissionId === undefined) {
          if (request === undefined) {
            // Reservation without a canonical request under abort: provably childless —
            // release the unused allocation exactly once (spec §13/§14).
            yield* releaseOrphanReservation(reservation.reservationId);
            continue;
          }
          const admission = yield* establishChildFromRequest(parent, request);
          if (admission._tag === "indeterminate") {
            // A child may exist: never release, settle, or re-admit until the authoritative
            // owner answers (SUB-031).
            blocked = true;
            continue;
          }
          childSubmissionId = admission.childSubmissionId;
        }
        const child = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: childSubmissionId }),
        );
        if (Option.isNone(child)) {
          return yield* LedgerError.make({
            operation: "abortAttachedChildren",
            message: `Attached child ${childSubmissionId} is unknown to the ledger`,
          });
        }
        if (child.value.state === "settled") {
          yield* joinSettledChildForAbort(
            ctx,
            parent,
            knownIds,
            subagent,
            reservation,
            toolCallId,
            childSubmissionId,
          );
          continue;
        }
        yield* ledger
          .requestAbort(
            AbortCommand.make({
              submissionId: childSubmissionId,
              author: SUBAGENT_ABORT_AUTHOR,
              reason: SUBAGENT_ABORT_REASON,
            }),
          )
          .pipe(
            // The child settled concurrently: the one winning Settlement joins on the next
            // pass of this loop (spec §13 "child terminal races abort").
            Effect.catchTag("SettlementConflict", () => Effect.void),
            Effect.catchTag("JoinedToHost", conflictToLedgerError("abortAttachedChildren")),
            Effect.asVoid,
          );
        yield* hit("subagent:after-child-abort-intent");
        yield* wake.notify(child.value.conversationId);
        waiting.push(WaitingChild.make({ toolCallId, childSubmissionId }));
      }
      if (blocked) return "blocked";
      const first = waiting[0];
      if (first === undefined) return "clear";
      const ownershipToken = yield* Ref.get(tokenRef);
      const suspension = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId,
          ownershipToken,
          reason: WaitingForChildSuspension.make({ children: [first, ...waiting.slice(1)] }),
        }),
      );
      yield* hit("subagent:after-suspend");
      if (suspension === "suspended") return "waiting";
      // Every listed child settled before the suspend committed: loop to join the winners.
    }
  });

  const failureOutcome = (error: unknown): Effect.Effect<AttemptOutcome> =>
    Schema.decodeUnknownEffect(PersistedJson)({
      errorTag: errorTagOf(error),
      message: errorMessageOf(error).slice(0, MAX_FAILURE_MESSAGE_LENGTH),
    }).pipe(
      // Two bounded strings always satisfy the canonical persistence limits.
      Effect.orDie,
      Effect.map((result) => ({ _tag: "failed" as const, result })),
    );

  const halt = <A, R>(
    effect: Effect.Effect<A, DurableWorkerFailure, R>,
  ): Effect.Effect<A, CoordinatorHalt, R> =>
    Effect.mapError(effect, (failure) => new CoordinatorHalt(failure));

  /**
   * Superseding-Attempt interruption audit (durability §9): appended at most once per superseded
   * fence epoch before this Attempt re-invokes the model. It deliberately over-approximates — a
   * prior ownership period that ended cleanly between commits still gets one — because durable
   * state cannot distinguish a mid-stream provider loss from a crash between boundaries, and the
   * honest direction is to record that duplicate provider cost is possible, never to hide it.
   */
  const appendInterruptedAudit = Effect.fn("DurableAgentRuntime.appendInterruptedAudit")(function* (
    ctx: AttemptAppendContext,
    runId: ReturnType<typeof runIdForSubmission>,
    lineage: AttemptLineage,
    knownIds: Set<string>,
  ): Effect.fn.Return<void, DurableWorkerFailure> {
    const recordId = modelResponseInterruptedRecordId(runId, lineage.supersededEpoch);
    if (knownIds.has(recordId)) return;
    const envelope = yield* makeEnvelope(
      recordId,
      ModelResponseInterrupted.make({
        runId,
        supersededEpoch: lineage.supersededEpoch,
        attemptId: lineage.attemptId,
        reason:
          "A prior ownership period ended without a canonical settlement; any in-flight model response was lost and the model may be re-invoked (duplicate provider cost is observable)",
      }),
    );
    yield* appendBatch(
      ctx,
      CanonicalBatch.make({
        batchId: modelResponseInterruptedBatchId(runId, lineage.supersededEpoch),
        producerId: config.producerId,
        records: [envelope],
      }),
    ).pipe(
      Effect.catch((error) => (error._tag === "AppendConflict" ? Effect.void : Effect.fail(error))),
      Effect.asVoid,
    );
    knownIds.add(recordId);
  });

  /**
   * Plan step 3 (+6): drive `AgentRuntime.stream` with history rebuilt by the run journal, commit
   * each Turn canonically through the fenced append, watch for durable abort intent, and keep the
   * ownership lease renewed. Engine Run failures settle `failed`; coordinator failures abort the
   * Attempt cleanly with the obligation still owed.
   *
   * Phase 5 commit shape (plan §2.1): a tool-declaring Turn splits into a RESPONSE batch
   * (committed by the engine's `commitResponse` hook at the finish part — pending steering plus
   * the response messages, creating the durability §15 provably-safe window), the durable
   * approval preflight (§2.6 — recorded decisions replay deterministically, unresolved requests
   * become canonical and suspend the Attempt), an optional PREPARED batch (before any handler
   * starts), and a RESULTS batch at the next TurnStarted/RunCompleted/RunFailed seam. No-tool
   * Turns keep the exact P4 single-batch shape. When the journal ends mid-batch,
   * `RunOptions.resume` replays the declared batch without re-invoking the model (§2.4).
   */
  const runModel = <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  >(
    agent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements
    >,
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    tokenRef: Ref.Ref<OwnershipToken>,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    lineage: AttemptLineage,
    approvalDecisions: ReadonlyArray<ApprovalDecisionIntent>,
  ) =>
    Effect.gen(function* () {
      const submissionId = submission.submissionId;
      const runId = runIdForSubmission(submissionId);
      const journal = yield* projectRunJournal(records, runId);
      const pending = yield* pendingToolBatchFor(records, runId);
      const resumeProjection =
        pending === undefined
          ? journal
          : yield* projectRunJournal(withoutPendingBatch(records, pending, runId), runId);
      // Batch-resume Turn numbers are already canonical (resume.turn is the pending canonical
      // Turn); ordinary Attempts restart engine Turns at 1 and offset by the committed count.
      const turnOffset = pending === undefined ? journal.committedTurns : 0;

      const knownIds = knownRecordIdsOf(records);
      const stepOutputs = new Map<string, PersistedJson>();
      for (const envelope of records) {
        const payload = envelope.record.payload;
        if (payload._tag === "ToolStepSettled" && payload.runId === runId) {
          stepOutputs.set(
            toolStepSettledRecordId(runId, payload.toolCallId, payload.stepName),
            payload.output,
          );
        }
      }
      // Recorded approval authority (plan §2.6): canonical `ToolApprovalDecided` records are the
      // deterministic decision source across Attempts; durable `resolveApproval` intents become
      // canonical before they are honored; requested Turns pin the Turn-shared approval batch
      // identity so a later append never contradicts a committed batch.
      const canonicalApprovalDecisions = new Map<string, ApprovalDecision>();
      const approvalRequestedTurns = new Set<number>();
      for (const envelope of records) {
        const payload = envelope.record.payload;
        if (payload._tag === "ToolApprovalDecided" && payload.runId === runId) {
          canonicalApprovalDecisions.set(payload.toolCallId, payload.decision);
        } else if (payload._tag === "ToolApprovalRequested" && payload.runId === runId) {
          approvalRequestedTurns.add(payload.turn);
        }
      }
      const approvalIntents = new Map<string, ApprovalDecisionIntent>();
      for (const intent of approvalDecisions) {
        approvalIntents.set(intent.toolCallId, intent);
      }
      // Joined queued input (plan §2.5): canonical facts for the prompt-coverage rule, computed
      // once per Attempt from the same strongly consistent read as the journal. A joined
      // `input:{sid}` record is prompt-covered iff a host `ModelResponseRecorded` committed
      // after it — commit 1 of every later Turn carries all pending steering (D8 extension), so
      // covered inputs are already inside the journal prompt and must never re-deliver.
      const joinedInputEnvelopes = new Map<string, CanonicalRecordEnvelope>();
      let lastHostResponseSequence: CanonicalSequence | undefined;
      for (const envelope of records) {
        const payload = envelope.record.payload;
        if (
          payload._tag === "UserInputRecorded" &&
          payload.runId === runId &&
          payload.submissionId !== submissionId
        ) {
          joinedInputEnvelopes.set(payload.submissionId, envelope);
        } else if (payload._tag === "ModelResponseRecorded" && payload.runId === runId) {
          if (
            lastHostResponseSequence === undefined ||
            envelope.sequence > lastHostResponseSequence
          ) {
            lastHostResponseSequence = envelope.sequence;
          }
        }
      }
      // RUN-023: per-Turn usage staged by the engine's `noteTurnUsage` for the
      // Turn's canonical response record (keyed by CANONICAL turn number).
      const stagedUsage = new Map<
        number,
        {
          readonly inputTokens: number;
          readonly outputTokens: number;
          readonly costMicrousd: number;
        }
      >();
      // RUN-026: durable compaction covers only records of PRIOR Runs — never
      // the appending Run's own records — so the owner's instruction/input
      // messages survive every projection and the resume-splice arithmetic
      // stays untouched. The in-memory view may cover more; the record is
      // canonical (RUN-026).
      let ownerFirstSequence: CanonicalSequence | undefined;
      for (const envelope of records) {
        const payload = envelope.record.payload;
        if ("runId" in payload && payload.runId === runId) {
          ownerFirstSequence = envelope.sequence;
          break;
        }
      }
      /** Rough chars/4 estimate of one record's prompt contribution (selection only). */
      const estimateRecordTokens = (envelope: CanonicalRecordEnvelope): number => {
        let text: string | undefined;
        try {
          text = JSON.stringify(envelope.record.payload);
        } catch {
          text = undefined;
        }
        return text === undefined ? 0 : Math.ceil(text.length / 4);
      };
      /** Pre-existing joins of this host Run, loaded lazily at the first drain seam. */
      let joinBacklog: ReadonlyArray<JoinSnapshot> | undefined;
      /** Joined inputs already handed to the engine during THIS Attempt (never re-deliver). */
      const deliveredJoinInputs = new Set<string>();
      // Canonical encoded parameters per declared call, for the approval request digest: seeded
      // by `commitResponse` (which the engine invokes before approval preflight) and by the
      // resumed batch's declared calls.
      const encodedParamsByCallId = new Map<string, unknown>();
      // Declared Tool name per call: the subagent join/sibling-settle appends rebuild exact
      // `ToolCallSettled` records outside the engine's results commit, so the declared name must
      // be recoverable per call id (seeded from canonical prepared records, the resumed batch,
      // and every `commitResponse`).
      const declaredNamesByCallId = new Map<string, string>();
      // Canonical subagent lifecycle state of this Run (SUB-016): seeded from canonical records,
      // advanced by the establish/join hook closures below, and consulted on every replay so an
      // identical establishment converges on the one existing child.
      const subagentState = subagentRecordsOf(records, runId);
      for (const [callId, name] of subagentState.preparedNames) {
        declaredNamesByCallId.set(callId, name);
      }
      if (pending !== undefined) {
        for (const call of pending.calls) {
          encodedParamsByCallId.set(call.id, call.params);
          declaredNamesByCallId.set(call.id, call.name);
        }
      }
      // Task #12 (WP1 `resume.leadingMessages`): the pending canonical response's messages
      // BEFORE its first assistant message — Turn-1 evaluated instructions + input, or steering
      // committed inside the pending record — re-enter official history through the engine so
      // the resumed live model context does not silently drop them.
      let resumeLeadingMessages: Prompt.Prompt | undefined;
      if (pending !== undefined) {
        const pendingMessages = yield* decodePrompt(pending.messages).pipe(
          Effect.mapError((cause) =>
            RunJournalError.make({
              message:
                "Pending ModelResponseRecorded messages are not Schema-encoded Prompt messages",
              cause,
            }),
          ),
        );
        const firstAssistant = pendingMessages.content.findIndex(
          (message) => message.role === "assistant",
        );
        if (firstAssistant > 0) {
          resumeLeadingMessages = Prompt.fromMessages(
            pendingMessages.content.slice(0, firstAssistant),
          );
        }
      }
      let currentToolTurn: { readonly turn: number; readonly turnId: TurnId } | undefined =
        pending === undefined ? undefined : { turn: pending.turn, turnId: pending.turnId };
      // Terminal sibling results observed from the Run event stream, keyed by Tool Call id: the
      // suspension seam commits each settled non-waiting sibling as a per-call late-settle batch
      // BEFORE the `waitingForChild` suspension so no sibling effect is lost to it (plan §2).
      const siblingResults = new Map<
        string,
        { readonly toolCallId: ToolCallId; readonly result: unknown; readonly isFailure: boolean }
      >();
      // Step-hook coordinator failures are re-wrapped by the engine as `DurableStepError` in the
      // handler channel; this side channel preserves the original failure so the Attempt aborts
      // (obligation still owed) instead of settling the Run `failed` on an infrastructure fault.
      const haltRef = yield* Ref.make<DurableWorkerFailure | undefined>(undefined);
      const recordHalt = <A, R>(
        effect: Effect.Effect<A, DurableWorkerFailure, R>,
      ): Effect.Effect<A, CoordinatorHalt, R> =>
        effect.pipe(
          Effect.tapError((failure) => Ref.set(haltRef, failure)),
          Effect.mapError((failure) => new CoordinatorHalt(failure)),
        );

      interface RunState {
        readonly baseLen: number | undefined;
        readonly lastCommitLen: number;
        readonly history: Prompt.Prompt | undefined;
        readonly pendingTurn: { readonly turn: number; readonly turnId: TurnId } | undefined;
        readonly completedOutput: PersistedJson | undefined;
        readonly completedFinishReason: "budget-exhausted" | undefined;
      }
      const stateRef = yield* Ref.make<RunState>({
        baseLen: undefined,
        lastCommitLen: 0,
        history: undefined,
        pendingTurn: undefined,
        completedOutput: undefined,
        completedFinishReason: undefined,
      });
      const turnCounter = yield* Ref.make(journal.committedTurns);
      const idGenerator: (typeof IdGenerator)["Service"] = {
        nextConversationId: Effect.succeed(submission.conversationId),
        nextRunId: Effect.succeed(runId),
        nextTurnId: Ref.modify(turnCounter, (turn) => [turnIdForRun(runId, turn + 1), turn + 1]),
      };

      const onHistory = (history: Prompt.Prompt): Effect.Effect<void> =>
        Ref.update(stateRef, (state) =>
          state.baseLen === undefined
            ? {
                ...state,
                baseLen: history.content.length,
                // A fresh Run's first commit starts at the engine-provided history boundary so
                // the evaluated instruction + user messages become canonical inside Turn 1 (D8).
                // A resumed Run's boundary is the engine's re-evaluated initial prompt: those
                // messages are already canonical inside the original Turn 1 and never re-enter
                // (a pending batch resume always implies at least one committed Turn).
                lastCommitLen:
                  journal.committedTurns === 0
                    ? journal.historyBefore.content.length
                    : history.content.length,
                history,
              }
            : { ...state, history },
        );

      // On resume the model must see the canonical prompt (committed Turns included) plus what
      // THIS Attempt appended — never the engine's re-appended instructions + input. For a batch
      // resume the canonical boundary excludes the pending Turn: its messages re-enter official
      // history through the engine's continuation and ride the slice after `baseLen`.
      const resumeContext: RunContextHook<never, never> = {
        prepare: ({ source }) =>
          Ref.get(stateRef).pipe(
            Effect.map((state) => ({
              prompt: Prompt.fromMessages([
                ...resumeProjection.prompt.content,
                ...source.content.slice(state.baseLen ?? source.content.length),
              ]),
            })),
          ),
      };

      const durability: RunDurabilityHook<CoordinatorHalt, never> = {
        commitResponse: (commit) =>
          recordHalt(
            Effect.gen(function* () {
              const canonicalTurn = turnOffset + commit.turn;
              currentToolTurn = { turn: canonicalTurn, turnId: commit.turnId };
              for (const call of commit.calls) {
                encodedParamsByCallId.set(call.toolCallId, call.parameters);
                declaredNamesByCallId.set(call.toolCallId, call.toolName);
              }
              const responseId = modelResponseRecordId(runId, canonicalTurn);
              if (knownIds.has(responseId)) return;
              const state = yield* Ref.get(stateRef);
              const history = state.history;
              if (history === undefined) {
                return yield* RunJournalError.make({
                  message: `Turn ${canonicalTurn} committed a response before official history advanced`,
                });
              }
              // D8 extension (decision point 6): the pending slice — evaluated instructions +
              // input for Turn 1, queued steering for later Turns — becomes canonical as the
              // leading messages of this response batch.
              const pendingSlice = history.content.slice(state.lastCommitLen);
              const createdAt = yield* nowUtc;
              const batch = yield* withCrypto(
                turnResponseBatch({
                  runId,
                  turn: canonicalTurn,
                  turnId: commit.turnId,
                  appended: [...pendingSlice, ...commit.responseMessages.content],
                  producerId: config.producerId,
                  deploymentId: config.deploymentId,
                  createdAt,
                  usage: stagedUsage.get(canonicalTurn),
                }),
              );
              yield* appendBatch(ctx, batch);
              knownIds.add(responseId);
              yield* hit("turn:after-response-append");
            }),
          ),
        prepareToolCalls: (calls) =>
          recordHalt(
            Effect.gen(function* () {
              const first = calls[0];
              if (first === undefined) return;
              const turnInfo = currentToolTurn;
              if (turnInfo === undefined) {
                return yield* RunJournalError.make({
                  message: "Tool Calls were prepared before any canonical response commit",
                });
              }
              // The prepared batch is atomic: one canonical record implies all of them, so a
              // batch-identity replay (resume) is skipped wholesale.
              if (knownIds.has(toolCallPreparedRecordId(runId, turnInfo.turn, first.toolCallId))) {
                return;
              }
              const preparedRecords: Array<RecordEnvelope> = [];
              for (const call of calls) {
                const parameters = yield* decodePersisted(call.parameters).pipe(
                  Effect.mapError((cause) =>
                    RunJournalError.make({
                      message: `Tool Call ${call.toolCallId} parameters exceed canonical persistence bounds`,
                      cause,
                    }),
                  ),
                );
                const parametersDigest = yield* withCrypto(digestJson(parameters));
                preparedRecords.push(
                  yield* makeEnvelope(
                    toolCallPreparedRecordId(runId, turnInfo.turn, call.toolCallId),
                    ToolCallPrepared.make({
                      runId,
                      turnId: turnInfo.turnId,
                      turn: turnInfo.turn,
                      toolCallId: call.toolCallId,
                      toolName: call.toolName,
                      parameters,
                      parametersDigest,
                    }),
                  ),
                );
              }
              const head = preparedRecords[0];
              if (head === undefined) return;
              yield* appendBatch(
                ctx,
                CanonicalBatch.make({
                  batchId: turnPreparedBatchId(runId, turnInfo.turn),
                  producerId: config.producerId,
                  records: [head, ...preparedRecords.slice(1)],
                }),
              );
              for (const record of preparedRecords) knownIds.add(record.recordId);
              yield* hit("tools:after-prepared-append");
            }),
          ),
        step: {
          lookup: (key) =>
            Effect.sync(() => {
              const output = stepOutputs.get(
                toolStepSettledRecordId(runId, key.toolCallId, key.stepName),
              );
              return output === undefined ? Option.none() : Option.some({ encodedOutput: output });
            }),
          commit: (key, encodedOutput) =>
            recordHalt(
              Effect.gen(function* () {
                const recordId = toolStepSettledRecordId(runId, key.toolCallId, key.stepName);
                if (knownIds.has(recordId)) return;
                const output = yield* decodePersisted(encodedOutput).pipe(
                  Effect.mapError((cause) =>
                    RunJournalError.make({
                      message: `Durable Step ${key.stepName} output exceeds canonical persistence bounds`,
                      cause,
                    }),
                  ),
                );
                const outputDigest = yield* withCrypto(digestJson(output));
                const envelope = yield* makeEnvelope(
                  recordId,
                  ToolStepSettled.make({
                    runId,
                    toolCallId: key.toolCallId,
                    stepName: key.stepName,
                    output,
                    outputDigest,
                  }),
                );
                yield* appendBatch(
                  ctx,
                  CanonicalBatch.make({
                    batchId: toolStepSettledBatchId(runId, key.toolCallId, key.stepName),
                    producerId: config.producerId,
                    records: [envelope],
                  }),
                );
                knownIds.add(recordId);
                stepOutputs.set(recordId, output);
                yield* hit("step:after-step-append");
              }),
            ),
        },
        noteTurnUsage: (usage) =>
          Effect.sync(() => {
            // Accumulate, never replace: a compaction summarizer and the
            // Turn's own response stage into the same canonical Turn.
            const key = turnOffset + usage.turn;
            const prior = stagedUsage.get(key);
            stagedUsage.set(key, {
              inputTokens: (prior?.inputTokens ?? 0) + usage.inputTokens,
              outputTokens: (prior?.outputTokens ?? 0) + usage.outputTokens,
              costMicrousd: (prior?.costMicrousd ?? 0) + usage.costMicrousd,
            });
          }),
        commitCompaction: (commit) =>
          recordHalt(
            Effect.gen(function* () {
              const canonicalTurn = turnOffset + commit.turn;
              const recordId = compactionRecordId(runId, canonicalTurn, commit.kind);
              if (knownIds.has(recordId)) return;
              // Coverage selection walks the attempt-start snapshot: records
              // appended by THIS Attempt are all owner-Run and excluded by the
              // prior-Runs-only rule regardless.
              const coverable: Array<CanonicalRecordEnvelope> = [];
              for (const envelope of records) {
                if (ownerFirstSequence !== undefined && envelope.sequence >= ownerFirstSequence) {
                  break;
                }
                const tag = envelope.record.payload._tag;
                if (tag === "ModelResponseRecorded" || tag === "ToolCallSettled") {
                  coverable.push(envelope);
                }
              }
              if (coverable.length === 0) return;
              // Keep the newest ~keepRecentTokens of prompt-visible records;
              // the cut lands only immediately before a ModelResponseRecorded
              // so a Turn is always covered atomically (pairing preserved).
              const keepRecentTokens = agent.definition.policy.compaction.keepRecentTokens;
              let kept = 0;
              let cutIndex = -1;
              for (let index = coverable.length - 1; index >= 0; index -= 1) {
                const envelope = coverable[index];
                if (envelope === undefined) continue;
                kept += estimateRecordTokens(envelope);
                if (kept >= keepRecentTokens) {
                  cutIndex = index;
                  break;
                }
              }
              if (cutIndex === -1) return;
              // The threshold-crossing record's tokens were counted as kept,
              // so its WHOLE Turn stays retained: walk BACK to that Turn's
              // ModelResponseRecorded and end the covered prefix just before
              // it. Walking forward instead would fold the counted Turn — and
              // for a newest-Turn threshold could cover all prior history.
              while (
                cutIndex >= 0 &&
                coverable[cutIndex]?.record.payload._tag !== "ModelResponseRecorded"
              ) {
                cutIndex -= 1;
              }
              if (cutIndex < 0) return;
              const lastCovered = cutIndex > 0 ? coverable[cutIndex - 1] : undefined;
              if (lastCovered === undefined) return;
              if (commit.kind === "summarize" && (commit.summary ?? "").length === 0) {
                return yield* RunJournalError.make({
                  message: "A summarize compaction commit carried no summary",
                });
              }
              const envelope = yield* makeEnvelope(
                recordId,
                CompactionCreated.make({
                  runId,
                  turn: canonicalTurn,
                  kind: commit.kind,
                  coversThrough: lastCovered.sequence,
                  ...(commit.kind === "summarize"
                    ? { summary: (commit.summary ?? "").slice(0, 64 * 1024) }
                    : {}),
                }),
              );
              yield* appendBatch(
                ctx,
                CanonicalBatch.make({
                  batchId: compactionBatchId(runId, canonicalTurn, commit.kind),
                  producerId: config.producerId,
                  records: [envelope],
                }),
              );
              knownIds.add(recordId);
              yield* hit("compaction:after-canonical-append");
            }),
          ),
      };

      /** Digest of one declared call's canonical encoded parameters (same family as prepared). */
      const approvalParametersDigest = (
        toolCallId: string,
      ): Effect.Effect<Digest, DurableWorkerFailure> =>
        Effect.gen(function* () {
          if (!encodedParamsByCallId.has(toolCallId)) {
            return yield* RunJournalError.make({
              message: `Tool Call ${toolCallId} requested approval before its response commit`,
            });
          }
          const parameters = yield* decodePersisted(encodedParamsByCallId.get(toolCallId)).pipe(
            Effect.mapError((cause) =>
              RunJournalError.make({
                message: `Tool Call ${toolCallId} parameters exceed canonical persistence bounds`,
                cause,
              }),
            ),
          );
          return yield* withCrypto(digestJson(parameters));
        });

      /**
       * Append the canonical approval records for one declared call: the `ToolApprovalRequested`
       * record — plus the immediate `ToolApprovalDecided` for a policy-auto or intent-backed
       * decision, one atomic batch — or the decision alone when the request is already canonical.
       * Batch identity: the Turn's FIRST canonical approval append owns
       * `turn-approvals:{runId}:{turn}`; later appends of the same Turn (across suspension
       * cycles) use deterministic per-call batches so committed batch content is never
       * contradicted; decision-only appends use `approval-decision:{sid}:{toolCallId}`. Record
       * identity dedupes every replay; an identity conflict means another pass already committed
       * the same records.
       */
      const appendApprovalRecords = (
        turnInfo: { readonly turn: number; readonly turnId: TurnId },
        toolCallId: ToolCallId,
        toolName: string,
        decided:
          | {
              readonly decision: ApprovalDecision;
              readonly resolver: string;
              readonly reason: string;
            }
          | undefined,
      ): Effect.Effect<void, DurableWorkerFailure> =>
        Effect.gen(function* () {
          const requestRecordId = toolApprovalRequestRecordId(runId, turnInfo.turn, toolCallId);
          const decisionRecordId = toolApprovalDecisionRecordId(runId, turnInfo.turn, toolCallId);
          const envelopes: Array<RecordEnvelope> = [];
          const appendRequest = !knownIds.has(requestRecordId);
          if (appendRequest) {
            const parametersDigest = yield* approvalParametersDigest(toolCallId);
            envelopes.push(
              yield* makeEnvelope(
                requestRecordId,
                ToolApprovalRequested.make({
                  runId,
                  turnId: turnInfo.turnId,
                  turn: turnInfo.turn,
                  toolCallId,
                  toolName,
                  parametersDigest,
                }),
              ),
            );
          }
          if (decided !== undefined && !knownIds.has(decisionRecordId)) {
            envelopes.push(
              yield* makeEnvelope(
                decisionRecordId,
                ToolApprovalDecided.make({
                  runId,
                  turn: turnInfo.turn,
                  toolCallId,
                  decision: decided.decision,
                  resolver: decided.resolver,
                  reason: decided.reason,
                }),
              ),
            );
          }
          const head = envelopes[0];
          if (head !== undefined) {
            const batchId = appendRequest
              ? approvalRequestedTurns.has(turnInfo.turn)
                ? decodeBatchId(`approval-request:${runId}:${turnInfo.turn}:${toolCallId}`)
                : turnApprovalsBatchId(runId, turnInfo.turn)
              : approvalDecisionBatchId(submissionId, toolCallId);
            yield* appendBatch(
              ctx,
              CanonicalBatch.make({
                batchId,
                producerId: config.producerId,
                records: [head, ...envelopes.slice(1)],
              }),
            ).pipe(
              Effect.catch((error) =>
                error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
              ),
              Effect.asVoid,
            );
            for (const record of envelopes) knownIds.add(record.recordId);
          }
          if (appendRequest) {
            approvalRequestedTurns.add(turnInfo.turn);
            yield* hit("approval:after-request-append");
          }
          if (decided !== undefined) {
            canonicalApprovalDecisions.set(toolCallId, decided.decision);
          }
        });

      /**
       * Durable approval hook (plan §2.6). Resolution order per declared call: (1) a canonical
       * `ToolApprovalDecided` record — the deterministic decision authority across Attempts;
       * (2) a durable `resolveApproval` intent, appended canonically before it is honored;
       * (3) the optional policy-auto delegate, whose immediate decision becomes canonical
       * (request + decision, one atomic batch) before it is honored; (4) otherwise the canonical
       * `ToolApprovalRequested` record is appended and the call reports unresolved — with the
       * request canonical, "waiting for explicit approval" is a safe durable boundary
       * (durability §8), the engine raises `AgentApprovalPending`, and the Attempt suspends
       * without settling. A denied decision fails the Run through the engine's
       * `AgentApprovalDenied` path with the denial already canonical.
       */
      const approval: RunApprovalHook<CoordinatorHalt, never> = {
        request: (request) =>
          recordHalt(
            Effect.gen(function* () {
              const turnInfo = currentToolTurn;
              if (turnInfo === undefined) {
                return yield* RunJournalError.make({
                  message: `Tool Call ${request.toolCallId} requested approval before any canonical response commit`,
                });
              }
              const toolCallId = request.toolCallId;
              const canonical = canonicalApprovalDecisions.get(toolCallId);
              if (canonical !== undefined) {
                return canonical === "approved"
                  ? { _tag: "approved" as const }
                  : { _tag: "denied" as const };
              }
              const intent = approvalIntents.get(toolCallId);
              if (intent !== undefined) {
                yield* appendApprovalRecords(turnInfo, toolCallId, request.toolName, {
                  decision: intent.decision,
                  resolver: intent.resolver,
                  reason: intent.reason,
                });
                return intent.decision === "approved"
                  ? { _tag: "approved" as const, reason: intent.reason }
                  : { _tag: "denied" as const, reason: intent.reason };
              }
              if (approvalResolver !== undefined) {
                const delegated = yield* approvalResolver.request(request);
                if (delegated._tag !== "unresolved") {
                  yield* appendApprovalRecords(turnInfo, toolCallId, request.toolName, {
                    decision: delegated._tag,
                    resolver: APPROVAL_POLICY_RESOLVER,
                    reason: boundedApprovalReason(
                      delegated.reason,
                      "The configured approval policy decided immediately",
                    ),
                  });
                  return delegated;
                }
              }
              yield* appendApprovalRecords(turnInfo, toolCallId, request.toolName, undefined);
              return {
                _tag: "unresolved" as const,
                reason:
                  "The approval request is canonical and awaits a durable resolveApproval decision",
              };
            }),
          ),
      };

      /**
       * Joining/Joined queued input hook (plan §2.5, DUR-016). At every engine drain seam:
       *
       * 1. Reattach first — pre-existing joins whose canonical `input:{sid}` record is not yet
       *    prompt-covered re-deliver WITHOUT re-appending (record identity is the dedupe);
       *    covered inputs are already inside the journal prompt. A `joining` row whose input is
       *    canonical only lost its marker: it is repaired from history under the host's live
       *    ownership (DUR-015). A `joining` row WITHOUT canonical input was never consumed and
       *    is recovery's to revert — the hook never guesses at it (fail-closed).
       * 2. Claim fresh — `claimJoining` atomically transitions the contiguous ready prefix of
       *    strictly-later queue sequences to `joining` under the host's ownership token (no
       *    epoch bump; an admitted-not-ready row breaks the prefix). Per fresh claim: honor a
       *    pre-consumption abort intent by reverting (revert-then-abort), else append the
       *    deterministic `UserInputRecorded` (batch `submission-input:{sid}`, record
       *    `input:{sid}`, `runId` = host Run, kind `steering`) → `join:after-canonical-append`
       *    → `markJoined` → hand the engine the `RunInputCommand`.
       *
       * Delivered input is `steering`: the engine appends it to official history at the seam,
       * so the next Turn's response commit makes it model-visible canonically — exactly the
       * prompt-coverage rule recovery relies on.
       */
      const input: RunInputHook<CoordinatorHalt, never> = {
        drain: (policy) =>
          recordHalt(
            Effect.gen(function* () {
              const limit = policy === "one" ? 1 : MAX_JOIN_DRAIN;
              const commands: Array<RunInputCommand> = [];
              if (joinBacklog === undefined) {
                const hostSnapshot = yield* ledger.loadRecoverySnapshot(
                  RecoverySnapshotRequest.make({ submissionId }),
                );
                joinBacklog = hostSnapshot.joins;
              }
              for (const join of joinBacklog) {
                if (commands.length >= limit) break;
                const joinId = join.submissionId;
                if (deliveredJoinInputs.has(joinId)) continue;
                if (join.state !== "joining" && join.state !== "joined") continue;
                const existing = joinedInputEnvelopes.get(joinId);
                if (existing === undefined) continue;
                if (join.state === "joining") {
                  // Canonical input without its joined marker (crash between the append and
                  // `markJoined`): repair the marker from history before reattaching.
                  const ownershipToken = yield* Ref.get(tokenRef);
                  yield* ledger.markJoined(
                    MarkJoinedRequest.make({
                      submissionId: joinId,
                      ownershipToken,
                      recordId: existing.record.recordId,
                      sequence: existing.sequence,
                    }),
                  );
                }
                deliveredJoinInputs.add(joinId);
                if (
                  lastHostResponseSequence !== undefined &&
                  lastHostResponseSequence > existing.sequence
                ) {
                  continue;
                }
                const payload = existing.record.payload;
                if (payload._tag !== "UserInputRecorded") continue;
                commands.push({ kind: "steering", input: JSON.stringify(payload.input) });
              }
              if (commands.length < limit) {
                const ownershipToken = yield* Ref.get(tokenRef);
                const claims = yield* ledger.claimJoining(
                  ClaimJoiningRequest.make({
                    conversationId: submission.conversationId,
                    hostSubmissionId: submissionId,
                    ownershipToken,
                    maxCount: limit - commands.length,
                  }),
                );
                if (claims.length > 0) {
                  yield* hit("join:after-claim");
                }
                for (const claim of claims) {
                  const claimSnapshot = yield* ledger.loadRecoverySnapshot(
                    RecoverySnapshotRequest.make({ submissionId: claim.submissionId }),
                  );
                  if (claimSnapshot.abortIntent !== undefined) {
                    // Aborted before the host consumed the input: honor the intent by
                    // returning the claim to ready (revert-then-abort, plan §2.5); it settles
                    // aborted once it heads the lane.
                    yield* ledger.revertJoining(
                      RevertJoiningRequest.make({ submissionId: claim.submissionId }),
                    );
                    continue;
                  }
                  const recordId = submissionInputRecordId(claim.submissionId);
                  let sequence: CanonicalSequence;
                  const existing = joinedInputEnvelopes.get(claim.submissionId);
                  if (existing !== undefined) {
                    // Defensive reattach: the exact record is already canonical, so only the
                    // marker and the delivery remain (DUR-016 — never a duplicate append).
                    sequence = existing.sequence;
                  } else {
                    const envelope = yield* makeEnvelope(
                      recordId,
                      UserInputRecorded.make({
                        submissionId: claim.submissionId,
                        kind: "steering",
                        runId,
                        input: claim.inputPayload,
                      }),
                    );
                    const result = yield* appendBatch(
                      ctx,
                      CanonicalBatch.make({
                        batchId: submissionInputBatchId(claim.submissionId),
                        producerId: config.producerId,
                        records: [envelope],
                      }),
                    );
                    sequence = result.firstSequence;
                    knownIds.add(recordId);
                    yield* hit("join:after-canonical-append");
                  }
                  // Re-read the token: the concurrent lease renewal may rotate it mid-batch.
                  const markToken = yield* Ref.get(tokenRef);
                  yield* ledger.markJoined(
                    MarkJoinedRequest.make({
                      submissionId: claim.submissionId,
                      ownershipToken: markToken,
                      recordId,
                      sequence,
                    }),
                  );
                  deliveredJoinInputs.add(claim.submissionId);
                  commands.push({ kind: "steering", input: JSON.stringify(claim.inputPayload) });
                }
              }
              return commands;
            }),
          ),
      };

      /**
       * Durable-Subagent seam (spec/subagents.md §12, plan §1.2/§1.6): `establish` performs —
       * or replays — the ten-step idempotent get-or-create establishment protocol under the
       * parent's ownership fence and reports where the one child stands; `join` appends the
       * atomic `[SubagentJoined, ToolCallSettled]` settlement batch (SUB-019) and applies the
       * reservation release. Both use the halt side channel exactly like the durability/step
       * hooks: the engine wraps a `CoordinatorHalt` into `SubagentDurabilityError` in the
       * handler channel while the Attempt aborts with the original infrastructure failure.
       */
      const establishSubagent = (
        request: RunSubagentEstablishRequest,
      ): Effect.Effect<ChildEstablishStatus, CoordinatorHalt> =>
        recordHalt(
          Effect.gen(function* () {
            const toolCallId = request.toolCallId;
            const turnInfo = currentToolTurn;
            if (turnInfo === undefined) {
              return yield* RunJournalError.make({
                message: `Delegation Tool Call ${toolCallId} established before any canonical response commit`,
              });
            }
            const denied = (errorTag: string, message: string): ChildEstablishStatus => ({
              _tag: "denied",
              errorTag,
              message: boundedText(message),
            });
            let requestedPayload = subagentState.requested.get(toolCallId);
            if (requestedPayload === undefined) {
              // FIRST establishment of this call: fix every digest fail-closed BEFORE any
              // durable mutation, then reserve → append the canonical request (spec §12
              // steps 2-3). Replays below proceed from the canonical record instead — the
              // recorded request, not the re-computed handler values, is the establishment
              // authority (SUB-016/SUB-018).
              if (request.depth !== CHILD_DELEGATION_DEPTH) {
                return denied(
                  "SubagentDepthUnsupported",
                  `S2 fixes attached durable children at delegation depth 1; depth ${request.depth} was requested`,
                );
              }
              const decodedDigests = yield* decodeDefinitionDigests({
                agent: request.targetDigests.agent,
                model: request.targetDigests.model,
                tools: request.targetDigests.tools,
              }).pipe(Effect.option);
              if (Option.isNone(decodedDigests)) {
                return denied(
                  "SubagentDigestsInvalid",
                  "The declared child Binding digests are not valid stored digests",
                );
              }
              const childInput = yield* decodePersisted(request.encodedChildInput).pipe(
                Effect.option,
              );
              if (Option.isNone(childInput)) {
                return denied(
                  "SubagentInputUnpersistable",
                  "The prepared child input does not satisfy the canonical persistence bounds",
                );
              }
              const grant = yield* decodePersisted(request.encodedGrant).pipe(Effect.option);
              const allocation = yield* decodePersisted(request.encodedAllocation).pipe(
                Effect.option,
              );
              if (Option.isNone(grant) || Option.isNone(allocation)) {
                return denied(
                  "SubagentDeclarationUnpersistable",
                  "The delegation grant or allocation does not satisfy the canonical persistence bounds",
                );
              }
              const childInputDigest = yield* withCrypto(digestJson(childInput.value));
              const grantDigest = yield* withCrypto(digestJson(grant.value));
              const allocationDigest = yield* withCrypto(digestJson(allocation.value));
              const reservationId = childReservationIdFor(runId, toolCallId);
              const ownershipToken = yield* Ref.get(tokenRef);
              yield* ledger
                .reserveChildBudget(
                  ChildBudgetReservationRequest.make({
                    reservationId,
                    parentSubmissionId: submissionId,
                    parentToolCallId: toolCallId,
                    ownershipToken,
                    allocation: allocation.value,
                    allocationDigest,
                  }),
                )
                .pipe(
                  Effect.catchTag(
                    "ChildReservationConflict",
                    conflictToLedgerError("reserveChildBudget"),
                  ),
                );
              yield* hit("subagent:after-reserve");
              requestedPayload = SubagentRequested.make({
                runId,
                turnId: turnInfo.turnId,
                turn: turnInfo.turn,
                toolCallId,
                delegationId: request.delegationId,
                targetAgentId: request.targetAgentId,
                targetDigests: decodedDigests.value,
                childInput: childInput.value,
                childInputDigest,
                grantDigest,
                reservationId,
                reservationDigest: allocationDigest,
                childConversationId: childConversationIdFor(submissionId, toolCallId),
                childPrincipal: submission.principal,
                childIdempotencyKey: childIdempotencyKeyFor(runId, toolCallId),
              });
              const requestRecordId = subagentRequestedRecordId(runId, toolCallId);
              if (!knownIds.has(requestRecordId)) {
                const envelope = yield* makeEnvelope(requestRecordId, requestedPayload);
                yield* appendBatch(
                  ctx,
                  CanonicalBatch.make({
                    batchId: subagentRequestedBatchId(runId, toolCallId),
                    producerId: config.producerId,
                    records: [envelope],
                  }),
                ).pipe(
                  Effect.catch((error) =>
                    error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
                  ),
                  Effect.asVoid,
                );
                knownIds.add(requestRecordId);
              }
              subagentState.requested.set(toolCallId, requestedPayload);
              yield* hit("subagent:after-request-append");
            }
            // Steps 4-8: resolveAdmission-gated admission with immutable parent linkage,
            // child materialization + lineage, readiness, Receipt (SUB-016/SUB-017/SUB-031).
            const admission = yield* establishChildFromRequest(submission, requestedPayload);
            if (admission._tag === "indeterminate") {
              // Wait-and-retry, never a second admission: the Attempt aborts with the
              // obligation still owed and the next pass re-queries the authoritative owner.
              return yield* LedgerError.make({
                operation: "establishSubagent",
                message: `Child admission for Tool Call ${toolCallId} is indeterminate (${admission.reason}); retrying without a second admission (SUB-031)`,
              });
            }
            const childRunId = runIdForSubmission(admission.childSubmissionId);
            // Step 9: the start link is appended only after the child Receipt exists (SUB-017).
            let startedPayload = subagentState.started.get(toolCallId);
            if (startedPayload === undefined) {
              startedPayload = SubagentStarted.make({
                runId,
                toolCallId,
                childConversationId: requestedPayload.childConversationId,
                childSubmissionId: admission.childSubmissionId,
                childReceiptId: admission.receiptId,
                childRunId,
              });
              const startRecordId = subagentStartedRecordId(runId, toolCallId);
              if (!knownIds.has(startRecordId)) {
                const envelope = yield* makeEnvelope(startRecordId, startedPayload);
                yield* appendBatch(
                  ctx,
                  CanonicalBatch.make({
                    batchId: subagentStartedBatchId(runId, toolCallId),
                    producerId: config.producerId,
                    records: [envelope],
                  }),
                ).pipe(
                  Effect.catch((error) =>
                    error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
                  ),
                  Effect.asVoid,
                );
                knownIds.add(startRecordId);
              }
              subagentState.started.set(toolCallId, startedPayload);
              yield* hit("subagent:after-start-append");
            } else if (startedPayload.childSubmissionId !== admission.childSubmissionId) {
              return yield* LedgerError.make({
                operation: "establishSubagent",
                message: `The canonical SubagentStarted record names child ${startedPayload.childSubmissionId} but admission resolved ${admission.childSubmissionId}; establishment fails closed (SUB-016)`,
              });
            }
            const attachToken = yield* Ref.get(tokenRef);
            yield* ledger
              .attachChildToReservation(
                AttachChildToReservationRequest.make({
                  reservationId: decodeChildReservationIdSync(requestedPayload.reservationId),
                  ownershipToken: attachToken,
                  childSubmissionId: startedPayload.childSubmissionId,
                }),
              )
              .pipe(
                Effect.catchTag(
                  "ChildReservationConflict",
                  conflictToLedgerError("attachChildToReservation"),
                ),
              );
            const identity = {
              childConversationId: startedPayload.childConversationId,
              childSubmissionId: startedPayload.childSubmissionId,
              childRunId: startedPayload.childRunId,
              receiptId: startedPayload.childReceiptId,
            };
            const child = yield* ledger.lookup(
              SubmissionLookupById.make({ submissionId: startedPayload.childSubmissionId }),
            );
            if (Option.isNone(child)) {
              return yield* LedgerError.make({
                operation: "establishSubagent",
                message: `Established child ${startedPayload.childSubmissionId} is unknown to the ledger`,
              });
            }
            if (child.value.state !== "settled") {
              return { _tag: "waiting", ...identity };
            }
            // §1.6: the child already settled — verify Parent Link, target, digests, and the
            // settlement record fail-closed before handing the outcome to the handler.
            const verification = yield* verifySettledChild(
              submission,
              requestedPayload,
              startedPayload.childSubmissionId,
            );
            if (verification._tag === "mismatch") {
              return denied("SubagentVerificationFailed", verification.message);
            }
            return {
              _tag: "settled",
              ...identity,
              outcome: verification.value.outcome,
              encodedResult: verification.value.encodedResult,
              // The child Settlement's honest exhaustion marker (RUN-018)
              // rides to the parent handler so the delegation can surface a
              // budget-truncated partial to the orchestrator (SUB-034).
              ...(verification.value.settlement.finishReason === undefined
                ? {}
                : { finishReason: verification.value.settlement.finishReason }),
            };
          }),
        );

      const joinSubagent = (
        request: RunSubagentJoinRequest,
      ): Effect.Effect<void, CoordinatorHalt> =>
        recordHalt(
          Effect.gen(function* () {
            const toolCallId = request.toolCallId;
            const turnInfo = currentToolTurn;
            if (turnInfo === undefined) {
              return yield* RunJournalError.make({
                message: `Delegation Tool Call ${toolCallId} joined before any canonical response commit`,
              });
            }
            const requestedPayload = subagentState.requested.get(toolCallId);
            const startedPayload = subagentState.started.get(toolCallId);
            if (requestedPayload === undefined || startedPayload === undefined) {
              return yield* RunJournalError.make({
                message: `Delegation Tool Call ${toolCallId} joined without canonical establishment records`,
              });
            }
            const reservationId = decodeChildReservationIdSync(requestedPayload.reservationId);
            const joinedRecordId = subagentJoinedRecordId(runId, toolCallId);
            let finalAccounting: PersistedJson;
            const existingJoined = subagentState.joined.get(toolCallId);
            if (existingJoined !== undefined) {
              // The atomic join batch is already canonical: only the reservation release below
              // may remain (spec §12 join step 6 — the canonical record is the replay source).
              finalAccounting = existingJoined.finalAccounting;
            } else {
              const encodedResult = yield* decodePersisted(request.encodedResult).pipe(
                Effect.mapError((cause) =>
                  RunJournalError.make({
                    message: `The projected result of Tool Call ${toolCallId} exceeds canonical persistence bounds`,
                    cause,
                  }),
                ),
              );
              const accounting = yield* decodePersisted(request.encodedAccounting).pipe(
                Effect.mapError((cause) =>
                  RunJournalError.make({
                    message: `The join accounting of Tool Call ${toolCallId} exceeds canonical persistence bounds`,
                    cause,
                  }),
                ),
              );
              const verification = yield* verifySettledChild(
                submission,
                requestedPayload,
                startedPayload.childSubmissionId,
              );
              if (verification._tag === "mismatch") {
                return yield* LedgerError.make({
                  operation: "joinSubagent",
                  message: `Child Settlement verification failed for Tool Call ${toolCallId}: ${verification.message}`,
                });
              }
              const verified = verification.value;
              const toolName = declaredNamesByCallId.get(toolCallId);
              if (toolName === undefined) {
                return yield* RunJournalError.make({
                  message: `Delegation Tool Call ${toolCallId} joined without a declared Tool name`,
                });
              }
              const joinedPayload = SubagentJoined.make({
                runId,
                toolCallId,
                childSubmissionId: startedPayload.childSubmissionId,
                childSettlementId: verified.settlement.settlementId,
                childOutcome: verified.outcome,
                childResultDigest: yield* withCrypto(
                  digestJson(verified.settlement.result ?? null),
                ),
                projectedResultDigest: yield* withCrypto(digestJson(encodedResult)),
                usageSummary: yield* childUsageSummaryOf(
                  verified.childRecords,
                  startedPayload.childRunId,
                ),
                reservationId: requestedPayload.reservationId,
                finalAccounting: accounting,
              });
              const settledRecordId = toolCallSettledRecordId(runId, turnInfo.turn, toolCallId);
              const joinedEnvelope = yield* makeEnvelope(joinedRecordId, joinedPayload);
              const settledEnvelope = yield* makeEnvelope(
                settledRecordId,
                ToolCallSettled.make({
                  runId,
                  toolCallId,
                  toolName,
                  result: encodedResult,
                  isFailure: request.isFailure,
                }),
              );
              yield* appendBatch(
                ctx,
                CanonicalBatch.make({
                  batchId: subagentJoinBatchId(runId, toolCallId),
                  producerId: config.producerId,
                  records: [joinedEnvelope, settledEnvelope],
                }),
              ).pipe(
                Effect.catch((error) =>
                  error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
                ),
                Effect.asVoid,
              );
              knownIds.add(joinedRecordId);
              knownIds.add(settledRecordId);
              subagentState.joined.set(toolCallId, joinedPayload);
              finalAccounting = accounting;
              yield* hit("subagent:after-join-append");
            }
            yield* applyReservationRelease(reservationId, finalAccounting);
          }),
        );

      const subagent: RunSubagentHook<CoordinatorHalt, never> = {
        establish: establishSubagent,
        join: joinSubagent,
      };

      const options: RunOptions<CoordinatorHalt, never> = {
        conversationId: submission.conversationId,
        runId,
        history: pending === undefined ? journal.historyBefore : resumeProjection.historyBefore,
        onHistory,
        input,
        approval,
        durability,
        subagent,
        ...(pending === undefined
          ? {}
          : {
              resume: {
                turn: pending.turn,
                turnId: pending.turnId,
                calls: pending.calls,
                settled: pending.settled,
                ...(resumeLeadingMessages === undefined
                  ? {}
                  : { leadingMessages: resumeLeadingMessages }),
              },
            }),
        ...(journal.committedTurns === 0
          ? {}
          : { context: resumeContext, resumeUsage: journal.usage }),
      };

      const commitPendingTurn: Effect.Effect<void, DurableWorkerFailure> = Effect.gen(function* () {
        const state = yield* Ref.get(stateRef);
        const history = state.history;
        if (state.pendingTurn === undefined || history === undefined) return;
        let appended = history.content.slice(state.lastCommitLen);
        if (appended.length === 0) return;
        const canonicalTurn = turnOffset + state.pendingTurn.turn;
        const createdAt = yield* nowUtc;
        let committedLen = history.content.length;
        if (knownIds.has(modelResponseRecordId(runId, canonicalTurn))) {
          // The response is already durable (commit 1 of the split shape): only the results
          // batch remains. The slice is [response messages…, tool message, trailing input…]:
          // this commit's canonical coverage ends at the batch's Tool message — messages drained
          // at the post-batch seam stay pending and become the leading messages of the NEXT
          // response batch (decision point 6 / D8), never silently dropped.
          for (let index = appended.length - 1; index >= 0; index -= 1) {
            if (appended[index]?.role === "tool") {
              committedLen = state.lastCommitLen + index + 1;
              appended = appended.slice(0, index + 1);
              break;
            }
          }
          // Already-canonical per-call settles (late settles from the resolution path, or
          // resume-injected results) are excluded by record identity.
          const remaining: Array<Prompt.Message> = [];
          let toolParts = 0;
          for (const message of appended) {
            if (message.role !== "tool") {
              remaining.push(message);
              continue;
            }
            const parts = message.content.filter(
              (part): part is Prompt.ToolResultPart =>
                part.type === "tool-result" &&
                !knownIds.has(`tool-settled:${runId}:${canonicalTurn}:${part.id}`),
            );
            if (parts.length === 0) continue;
            toolParts += parts.length;
            remaining.push(Prompt.makeMessage("tool", { content: parts }));
          }
          if (toolParts > 0) {
            const batch = yield* turnResultsBatch({
              runId,
              turn: canonicalTurn,
              turnId: state.pendingTurn.turnId,
              appended: remaining,
              producerId: config.producerId,
              deploymentId: config.deploymentId,
              createdAt,
            });
            yield* appendBatch(ctx, batch);
            for (const record of batch.records) knownIds.add(record.recordId);
            yield* hit("turn:after-results-append");
          }
        } else {
          // No durable response commit: the P4 single-batch shape (no-tool Turns).
          const batch = yield* withCrypto(
            turnCanonicalBatch({
              runId,
              turn: canonicalTurn,
              turnId: state.pendingTurn.turnId,
              appended,
              producerId: config.producerId,
              deploymentId: config.deploymentId,
              createdAt,
              usage: stagedUsage.get(canonicalTurn),
            }),
          );
          yield* appendBatch(ctx, batch);
          for (const record of batch.records) knownIds.add(record.recordId);
          yield* hit("turn:after-canonical-append");
        }
        yield* Ref.update(stateRef, (current) => ({
          ...current,
          lastCommitLen: committedLen,
          pendingTurn: undefined,
        }));
      });

      const recordCompleted = (
        output: unknown,
        finishReason: "completed" | "model-stop" | "budget-exhausted",
      ): Effect.Effect<void, DurableWorkerFailure> =>
        Schema.decodeUnknownEffect(PersistedJson)(output).pipe(
          Effect.mapError(
            (cause): DurableWorkerFailure =>
              LedgerError.make({
                operation: "recordCompleted",
                message: "Run output exceeds canonical persistence bounds",
                cause,
              }),
          ),
          Effect.flatMap((result) =>
            Ref.update(stateRef, (state) => ({
              ...state,
              completedOutput: result,
              completedFinishReason: finishReason === "budget-exhausted" ? finishReason : undefined,
            })),
          ),
        );

      const handleEvent = (event: RunEvent): Effect.Effect<void, DurableWorkerFailure> => {
        switch (event._tag) {
          case "TurnStarted": {
            return commitPendingTurn;
          }
          case "TurnCompleted": {
            const turnId = event.turnId ?? turnIdForRun(runId, turnOffset + event.turn);
            return Ref.update(stateRef, (state) => ({
              ...state,
              pendingTurn: { turn: event.turn, turnId },
            }));
          }
          case "RunCompleted": {
            return commitPendingTurn.pipe(
              Effect.andThen(recordCompleted(event.output, event.finishReason)),
            );
          }
          case "RunFailed": {
            // Preserve a completed-and-advanced final Turn for audit before the Run settles failed.
            return commitPendingTurn;
          }
          case "ToolCallSucceeded": {
            // Collected for the waitingForChild suspension seam: a batch that suspends never
            // reaches its results commit, so each settled sibling result is committed there as
            // a per-call late-settle batch instead (plan §2 step 2).
            if (!event.providerExecuted) {
              siblingResults.set(event.toolCallId, {
                toolCallId: event.toolCallId,
                result: event.result,
                isFailure: false,
              });
            }
            return Effect.void;
          }
          case "ToolCallFailed": {
            // Only the bounded diagnostics survive the event stream for a failed sibling; the
            // per-call late-settle carries this same bounded `{errorTag, message}` projection.
            if (!event.providerExecuted) {
              siblingResults.set(event.toolCallId, {
                toolCallId: event.toolCallId,
                result: { errorTag: event.errorTag, message: boundedText(event.message) },
                isFailure: true,
              });
            }
            return Effect.void;
          }
          default: {
            return Effect.void;
          }
        }
      };

      /**
       * The waitingForChild suspension seam (plan §2 steps 1-4): every non-waiting sibling of
       * the suspending batch has settled — commit each terminal sibling result as a per-call
       * late-settle batch (`turn-results:{runId}:{turn}:{toolCallId}`) in the batch's declared
       * order, so no sibling effect is lost to the suspension and the resumed batch injects
       * them via `resume.settled`. Record identity dedupes results already canonical (joined
       * delegation calls, resume-injected siblings).
       */
      const commitSiblingLateSettles = (
        children: AgentChildPending["children"],
      ): Effect.Effect<void, DurableWorkerFailure> =>
        Effect.gen(function* () {
          const turnInfo = currentToolTurn;
          if (turnInfo === undefined) return;
          const waitingIds = new Set<string>(children.map((child) => child.toolCallId));
          // Declared order first (SUB-013's commit-order rule), then any residue in arrival order.
          const ordered = [
            ...[...declaredNamesByCallId.keys()].filter((callId) => siblingResults.has(callId)),
            ...[...siblingResults.keys()].filter((callId) => !declaredNamesByCallId.has(callId)),
          ];
          for (const callId of ordered) {
            if (waitingIds.has(callId)) continue;
            const settled = siblingResults.get(callId);
            if (settled === undefined) continue;
            const toolCallId = settled.toolCallId;
            const recordId = toolCallSettledRecordId(runId, turnInfo.turn, toolCallId);
            if (knownIds.has(recordId)) continue;
            const toolName = declaredNamesByCallId.get(callId);
            if (toolName === undefined) {
              return yield* RunJournalError.make({
                message: `Settled sibling ${toolCallId} has no declared Tool name at the suspension seam`,
              });
            }
            const result = yield* decodePersisted(settled.result).pipe(
              Effect.mapError((cause) =>
                RunJournalError.make({
                  message: `Sibling result ${toolCallId} exceeds canonical persistence bounds`,
                  cause,
                }),
              ),
            );
            const envelope = yield* makeEnvelope(
              recordId,
              ToolCallSettled.make({
                runId,
                toolCallId,
                toolName,
                result,
                isFailure: settled.isFailure,
              }),
            );
            yield* appendBatch(
              ctx,
              CanonicalBatch.make({
                batchId: toolCallResultBatchId(runId, turnInfo.turn, toolCallId),
                producerId: config.producerId,
                records: [envelope],
              }),
            ).pipe(
              Effect.catch((error) =>
                error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
              ),
              Effect.asVoid,
            );
            knownIds.add(recordId);
            yield* hit("subagent:after-sibling-settle");
          }
        });

      // Durability §9: the superseding Attempt records the interruption BEFORE re-invoking the
      // model. A batch resume never re-invokes the model for the pending Turn, so it is exempt.
      if (
        pending === undefined &&
        lineage.inputWasRecorded &&
        lineage.supersededEpoch >= 1 &&
        lineage.supersededEpoch < ctx.producerEpoch
      ) {
        yield* appendInterruptedAudit(ctx, runId, lineage, knownIds);
      }

      const consume = Stream.runForEach(
        AgentRuntime.stream(agent, submission.inputPayload, options),
        (event) => halt(handleEvent(event)),
      ).pipe(Effect.as({ _tag: "run" as const }));

      // Durable §13: the abort command becomes canonical (serialized on the append gate) BEFORE
      // the Run fiber is interrupted by losing the race.
      const abortWatcher = halt(
        Effect.gen(function* () {
          while (true) {
            yield* Effect.sleep(config.abortPollInterval);
            const snapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId }),
            );
            const intent = snapshot.abortIntent;
            if (intent === undefined) continue;
            yield* appendAbortRecord(ctx, intent);
            return { _tag: "aborted" as const };
          }
        }),
      );

      // Liveness only: the lease keeps the claim visible; correctness stays with the epoch fence.
      // OwnershipLost ends the race and interrupts the Run fiber cleanly.
      const renewal = halt(
        Effect.repeat(
          Effect.gen(function* () {
            const ownershipToken = yield* Ref.get(tokenRef);
            const renewal = yield* ledger.renewOwnership(
              RenewOwnershipRequest.make({ submissionId, ownershipToken }),
            );
            yield* Ref.set(tokenRef, renewal.ownershipToken);
          }),
          { schedule: Schedule.spaced(config.leaseRenewalInterval) },
        ).pipe(Effect.andThen(Effect.never)),
      );

      const raced = Effect.raceFirst(consume, Effect.raceFirst(abortWatcher, renewal)).pipe(
        Effect.provideService(IdGenerator, idGenerator),
      );

      const result = yield* raced.pipe(
        Effect.catch(
          (
            error,
          ): Effect.Effect<
            | { readonly _tag: "failedRun"; readonly outcome: AttemptOutcome }
            | { readonly _tag: "suspendedRun"; readonly toolCallId: ToolCallId }
            | {
                readonly _tag: "suspendedChildRun";
                readonly children: AgentChildPending["children"];
              },
            DurableWorkerFailure
          > => {
            if (error instanceof CoordinatorHalt) {
              return Effect.fail(error.failure);
            }
            if (error instanceof AgentApprovalPending) {
              // Durable approval suspension (plan §2.6): the approval hook already made the
              // request canonical; `runAttempt` owns the ledger transition. The engine decoded
              // the declared call id before raising the suspension, so a failure is a defect.
              return decodeToolCallIdUnknown(error.toolCallId).pipe(
                Effect.orDie,
                Effect.map((toolCallId) => ({ _tag: "suspendedRun" as const, toolCallId })),
              );
            }
            if (error instanceof AgentChildPending) {
              // Durable waitingForChild suspension (spec §12 step 10): every non-waiting
              // sibling settled before the Run terminated; commit their results as per-call
              // late-settle batches FIRST so no sibling effect is lost, then let `runAttempt`
              // own the ledger transition.
              return commitSiblingLateSettles(error.children).pipe(
                Effect.map(() => ({
                  _tag: "suspendedChildRun" as const,
                  children: error.children,
                })),
              );
            }
            return Effect.gen(function* () {
              // A recorded halt means a coordinator mutation failed inside a Tool handler
              // (the engine re-wraps step-hook and subagent-hook errors): abort the Attempt
              // with the original infrastructure failure instead of settling the Run failed.
              const halted = yield* Ref.get(haltRef);
              if (halted !== undefined) {
                return yield* halted;
              }
              return {
                _tag: "failedRun" as const,
                outcome: yield* failureOutcome(error),
              };
            });
          },
        ),
      );

      if (result._tag === "suspendedRun") {
        return { _tag: "suspended", toolCallId: result.toolCallId } as RunPhaseOutcome;
      }
      if (result._tag === "suspendedChildRun") {
        return { _tag: "suspendedChild", children: result.children } as RunPhaseOutcome;
      }
      if (result._tag === "aborted") {
        return { _tag: "aborted" } as RunPhaseOutcome;
      }
      if (result._tag === "failedRun") {
        return result.outcome as RunPhaseOutcome;
      }
      const state = yield* Ref.get(stateRef);
      if (state.completedOutput === undefined) {
        return yield* LedgerError.make({
          operation: "runModel",
          message: "Agent Run stream ended without RunCompleted",
        });
      }
      return {
        _tag: "completed",
        result: state.completedOutput,
        ...(state.completedFinishReason === undefined
          ? {}
          : { finishReason: state.completedFinishReason }),
      } as RunPhaseOutcome;
    });

  /**
   * One ownership period over the claimed lane head. The step order mirrors the pure recovery
   * classifier's decision table, so a fresh Attempt and a recovering Attempt take the same path
   * through the same idempotent steps. Returns `Option.none` when the lane is durably blocked
   * (Unknown Outcomes, or a durable approval suspension) — no settlement occurs and the
   * obligation stays owed.
   */
  const runAttempt = <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  >(
    agent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements
    >,
    conversationId: ConversationId,
    claim: Claim,
  ) =>
    Effect.gen(function* () {
      const submissionId = claim.submissionId;
      const tokenRef = yield* Ref.make(claim.ownershipToken);
      // The Conversation-store fence BEFORE this Attempt advances it identifies the superseded
      // ownership period for the durability §9 interruption audit.
      const supersededEpoch = yield* store
        .inspectTail(ConversationTailRequest.make({ conversationId }))
        .pipe(
          Effect.map((tail) => tail.producerEpoch),
          Effect.catchTag("ConversationNotMaterialized", () => Effect.succeed(ZERO_EPOCH)),
        );
      // Advance the Conversation-store fence to this Attempt's epoch (idempotent when equal).
      yield* store.materialize(
        ConversationMaterialization.make({ conversationId, producerEpoch: claim.producerEpoch }),
      );
      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId }),
      );
      const submission = snapshot.submission;
      yield* ensureConversationCreated(conversationId, submission.agentId, submission.agentDigests);
      if (submission.state === "admitted") {
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId }));
      }
      const ctx = yield* attemptContextFor(conversationId, claim.producerEpoch);
      const records = yield* readAll(conversationId);
      const evidence = yield* evidenceFor(records, submissionId, true, snapshot.hostSubmissionId);
      const knownIds = knownRecordIdsOf(records);

      if (evidence.recordedSettlementOutcome !== undefined) {
        const settlement = yield* finalizeFromHistory(
          submission,
          snapshot.reservation?.settlementId ?? submissionSettlementId(submissionId),
        );
        yield* settleJoinedSubmissions(ctx, submissionId, settlement.outcome);
        return Option.some(settlement);
      }
      if (snapshot.reservation !== undefined) {
        return Option.some(
          yield* completeReservation(ctx, submission, snapshot.reservation, false),
        );
      }
      if (snapshot.abortIntent !== undefined) {
        // request-abort-and-join (spec §13.1, SUB-022): propagate the durable abort to every
        // nonterminal attached child, join every settled child coordinator-side, and settle
        // aborted ONLY once no child obligation stays open. A waiting/blocked disposition ends
        // the ownership period without settling — the obligation stays owed.
        const disposition = yield* abortAttachedChildren(ctx, submission, tokenRef, knownIds);
        if (disposition === "waiting") {
          return Option.none<Settlement>();
        }
        if (disposition === "blocked") {
          const ownershipToken = yield* Ref.get(tokenRef);
          yield* ledger
            .releaseOwnership(ReleaseOwnershipRequest.make({ submissionId, ownershipToken }))
            .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
          return Option.none<Settlement>();
        }
        return Option.some(
          yield* settleAborted(ctx, submission, tokenRef, snapshot.abortIntent, evidence, knownIds),
        );
      }
      yield* applyCanonicalInput(ctx, submission, tokenRef, records, snapshot.inputApplied);

      // Open ordinary Tool Calls gate the Run (DUR-009): reconcile-then-mark, never auto-replay.
      if (evidence.openToolCalls.length > 0) {
        const tools: Record<string, Tool.Any> = agent.definition.toolkit.tools;
        const review = yield* reconcileOpenCalls(
          ctx,
          submission,
          snapshot,
          records,
          evidence.openToolCalls,
          knownIds,
          (toolName) => {
            const tool = tools[toolName];
            return tool === undefined ? undefined : getToolExecutionClass(tool);
          },
        );
        if (review.uncertain.length > 0 || review.unproven.length > 0) {
          if (review.uncertain.length > 0) {
            yield* markCallsUnknown(
              ctx,
              submissionId,
              knownIds,
              review.uncertain,
              "An ordinary Tool call may have executed without a canonical outcome",
            );
          }
          // The lane is blocked (marked Unknown, or the reconciliation policy itself failed):
          // release the claim without settling — the accepted-work obligation stays owed.
          const ownershipToken = yield* Ref.get(tokenRef);
          yield* ledger
            .releaseOwnership(ReleaseOwnershipRequest.make({ submissionId, ownershipToken }))
            .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
          return Option.none<Settlement>();
        }
      }

      const lineage: AttemptLineage = {
        attemptId: claim.attemptId,
        supersededEpoch,
        inputWasRecorded: evidence.inputRecorded,
      };
      let approvalDecisionIntents = snapshot.approvalDecisions;
      while (true) {
        const currentRecords = yield* readAll(conversationId);
        const outcome = yield* runModel(
          agent,
          ctx,
          submission,
          tokenRef,
          currentRecords,
          lineage,
          approvalDecisionIntents,
        );
        if (outcome._tag === "suspendedChild") {
          // Durable waitingForChild suspension (spec §12 step 10, SUB-030): the sibling
          // late-settles are already canonical; the ledger transition ends the ownership
          // period WITHOUT settling and the lane consumes no worker permit while each listed
          // child runs on its own Conversation lane. A child settlement racing ahead of the
          // suspend transaction returns `resume-immediately`: the declared batch replays under
          // this same claim and the handler joins the settled child.
          const [firstChild, ...restChildren] = outcome.children;
          const waitingChildren: readonly [WaitingChild, ...Array<WaitingChild>] = [
            WaitingChild.make({
              toolCallId: firstChild.toolCallId,
              childSubmissionId: firstChild.childSubmissionId,
            }),
            ...restChildren.map((child) =>
              WaitingChild.make({
                toolCallId: child.toolCallId,
                childSubmissionId: child.childSubmissionId,
              }),
            ),
          ];
          const ownershipToken = yield* Ref.get(tokenRef);
          const suspension = yield* ledger.suspend(
            SuspendRequest.make({
              submissionId,
              ownershipToken,
              reason: WaitingForChildSuspension.make({ children: waitingChildren }),
            }),
          );
          yield* hit("subagent:after-suspend");
          for (const child of outcome.children) {
            yield* wake.notify(child.childConversationId);
          }
          if (suspension === "suspended") {
            return Option.none<Settlement>();
          }
          continue;
        }
        if (outcome._tag === "suspended") {
          // Durable approval suspension (plan §2.6): the ledger transition ends the ownership
          // period WITHOUT settling — the accepted-work obligation stays owed while the lane
          // consumes no worker permit. A decision that raced ahead of the suspend transaction
          // returns `resume-immediately`: the declared batch replays under this same claim with
          // the fresh decision intents (no model re-invocation — the response is canonical).
          const ownershipToken = yield* Ref.get(tokenRef);
          const suspension = yield* ledger.suspend(
            SuspendRequest.make({
              submissionId,
              ownershipToken,
              reason: ApprovalPendingSuspension.make({ toolCallIds: [outcome.toolCallId] }),
            }),
          );
          yield* hit("approval:after-suspend");
          if (suspension === "suspended") {
            return Option.none<Settlement>();
          }
          approvalDecisionIntents = (yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId }),
          )).approvalDecisions;
          continue;
        }
        if (outcome._tag === "aborted") {
          // The abort watcher ended the Run while attached children may still be open:
          // request-abort-and-join before the aborted settlement (spec §13.1).
          const disposition = yield* abortAttachedChildren(ctx, submission, tokenRef, knownIds);
          if (disposition === "waiting") {
            return Option.none<Settlement>();
          }
          if (disposition === "blocked") {
            const ownershipToken = yield* Ref.get(tokenRef);
            yield* ledger
              .releaseOwnership(ReleaseOwnershipRequest.make({ submissionId, ownershipToken }))
              .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
            return Option.none<Settlement>();
          }
          return Option.some(yield* terminalize(ctx, submission, tokenRef, outcome, true));
        }
        // Canonical joins drive their reservation release BEFORE the parent settles (a settled
        // lane would strand the repair); a reserved row WITHOUT a canonical join is an open
        // attached-child obligation and the parent never settles across it (spec §13).
        const openObligation = yield* completeJoinedReleases(submission);
        if (openObligation) {
          if (outcome._tag === "failed") {
            // Release the claim and leave the lane to recovery classification.
            const ownershipToken = yield* Ref.get(tokenRef);
            yield* ledger
              .releaseOwnership(ReleaseOwnershipRequest.make({ submissionId, ownershipToken }))
              .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
            return Option.none<Settlement>();
          }
          // A completed Run with an unjoined reservation is structurally unreachable (the
          // delegation Tool Call settles only through the atomic join batch): fail closed
          // with the obligation visibly owed instead of settling across it.
          return yield* LedgerError.make({
            operation: "runAttempt",
            message: `Submission ${submissionId} completed with an unjoined child budget reservation; the settlement is withheld fail-closed (spec 13)`,
          });
        }
        return Option.some(yield* terminalize(ctx, submission, tokenRef, outcome, true));
      }
    });

  /**
   * The one fenced-Attempt entry every resolved binding drives; passing `runAttempt` through
   * this named value keeps the polymorphic driver instantiation explicit at the seam.
   */
  const attemptDriver: ResolvedAttemptDriver = (agent, conversationId, claim) =>
    runAttempt(agent, conversationId, claim);

  /**
   * Framework-owned Schema-stable `ChildCompatibilityFailure` Settlement for a parent-linked
   * child whose exact Binding cannot be resolved at claim time (spec §11, SUB-023/SUB-032): no
   * application code runs, the child never executes, and the parent joins the bounded failure
   * through the normal verification path. A pre-existing reservation or canonical settlement is
   * completed instead of re-decided.
   */
  const settleChildCompatibility = Effect.fn("DurableAgentRuntime.settleChildCompatibility")(
    function* (
      claim: Claim,
      submission: SubmissionSnapshot,
      failure: DurableBindingFailure,
    ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
      yield* store.materialize(
        ConversationMaterialization.make({
          conversationId: submission.conversationId,
          producerEpoch: claim.producerEpoch,
        }),
      );
      yield* ensureConversationCreated(
        submission.conversationId,
        submission.agentId,
        submission.agentDigests,
      );
      const ctx = yield* attemptContextFor(submission.conversationId, claim.producerEpoch);
      const tokenRef = yield* Ref.make(claim.ownershipToken);
      const records = yield* readAll(submission.conversationId);
      const recorded = records.some(
        (envelope) =>
          envelope.record.recordId === submissionSettlementRecordId(submission.submissionId),
      );
      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
      );
      if (snapshot.reservation !== undefined) {
        // An earlier pass already reserved the one exact outcome: complete it, never re-decide.
        return yield* completeReservation(ctx, submission, snapshot.reservation, recorded);
      }
      const result = yield* decodePersisted({
        errorTag: "ChildCompatibilityFailure",
        message: boundedText(failure.message),
      }).pipe(Effect.orDie);
      return yield* terminalize(ctx, submission, tokenRef, { _tag: "failed", result }, false);
    },
  );

  /**
   * Drain one Conversation lane under claim-time Binding resolution (plan §1.7): every claimed
   * head's stored `(agentId, agentDigests)` resolves through the supplied resolver BEFORE any
   * code runs. A refusal writes the framework `ChildCompatibilityFailure` Settlement for a
   * parent-linked child and surfaces the typed refusal (after releasing the claim) for a root —
   * a worker never runs a claimed head against different code (SUB-023).
   */
  const drainConversation = (
    resolve: (
      submission: SubmissionSnapshot,
    ) => Effect.Effect<ResolvedBinding, DurableBindingFailure | LedgerError>,
    conversationId: ConversationId,
  ): Effect.Effect<ReadonlyArray<Settlement>, DurableWorkerFailure | DurableBindingFailure> =>
    Effect.gen(function* () {
      const settlements: Array<Settlement> = [];
      while (true) {
        const claimed = yield* ledger.claim(
          ClaimRequest.make({ conversationId, producerId: config.producerId }),
        );
        if (Option.isNone(claimed)) return settlements as ReadonlyArray<Settlement>;
        yield* hit("claim:after-claim");
        const claim = claimed.value;
        const found = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: claim.submissionId }),
        );
        if (Option.isNone(found)) {
          return yield* LedgerError.make({
            operation: "drainConversation",
            message: `Claimed unknown Submission ${claim.submissionId}`,
          });
        }
        const submission = found.value;
        // P7 §7(a): the claim head rule legally grants an `admitted` head, so the WORKER path
        // enforces the same AwaitParentEstablishment discipline as the recovery classifier —
        // a parent-linked child whose Conversation lacks its canonical lineage record is not
        // runnable yet (the parent's idempotent establishment appends lineage BEFORE
        // readiness, SUB-016; model-checked in `formal/SubagentEstablishmentFix.cfg`).
        // Release the claim, nudge the parent lane, and leave the child to establishment.
        if (submission.parentLinkage !== undefined && submission.state === "admitted") {
          const read = yield* readAllTolerant(conversationId);
          const lineageRecorded = read.records.some(
            (envelope) => envelope.record.payload._tag === "SubagentLineageRecorded",
          );
          if (!lineageRecorded) {
            yield* ledger
              .releaseOwnership(
                ReleaseOwnershipRequest.make({
                  submissionId: claim.submissionId,
                  ownershipToken: claim.ownershipToken,
                }),
              )
              .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
            const parent = yield* ledger.lookup(
              SubmissionLookupById.make({
                submissionId: submission.parentLinkage.parentSubmissionId,
              }),
            );
            if (Option.isSome(parent)) {
              yield* wake.notify(parent.value.conversationId);
            }
            return settlements as ReadonlyArray<Settlement>;
          }
        }
        const resolution = yield* resolve(submission).pipe(
          Effect.map((binding) => ({ _tag: "resolved" as const, binding })),
          Effect.catchTags({
            BindingUnavailable: (failure) => Effect.succeed({ _tag: "refused" as const, failure }),
            BindingDigestMismatch: (failure) =>
              Effect.succeed({ _tag: "refused" as const, failure }),
          }),
        );
        let refusal: DurableBindingFailure | undefined;
        if (resolution._tag === "refused") {
          refusal = resolution.failure;
        } else if (resolution.binding.agentId !== submission.agentId) {
          refusal = BindingUnavailable.make({
            agentId: submission.agentId,
            message: `The resolver answered with Binding ${resolution.binding.agentId} for Agent ${submission.agentId}; resolution fails closed (SUB-023)`,
          });
        } else if (
          resolution.binding.digests !== undefined &&
          !definitionDigestsEqual(resolution.binding.digests, submission.agentDigests)
        ) {
          refusal = BindingDigestMismatch.make({
            agentId: submission.agentId,
            message:
              "The resolved Binding digests do not match the claimed head's stored digests byte-for-byte (SUB-023)",
          });
        }
        if (refusal !== undefined) {
          if (submission.parentLinkage !== undefined) {
            settlements.push(yield* settleChildCompatibility(claim, submission, refusal));
            continue;
          }
          // Root Submission: release the claim and surface the typed refusal — the obligation
          // stays visible and no different code ever runs (spec §11).
          yield* ledger
            .releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: claim.submissionId,
                ownershipToken: claim.ownershipToken,
              }),
            )
            .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
          return yield* refusal;
        }
        if (resolution._tag !== "resolved") {
          // Unreachable: `refusal` covered every refused shape above.
          continue;
        }
        const settlement = yield* resolution.binding.attempt(attemptDriver, conversationId, claim);
        if (Option.isNone(settlement)) {
          // The head is durably blocked (Unknown Outcome, approval suspension, or a
          // waitingForChild suspension): the lane frees its worker permit while the settlement
          // obligation stays visible (durability §16, plan §2.6, SUB-030).
          return settlements as ReadonlyArray<Settlement>;
        }
        settlements.push(settlement.value);
      }
    });

  const processConversationImpl = <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  >(
    agent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements
    >,
    conversationId: ConversationId,
  ) =>
    Effect.gen(function* () {
      // The legacy single-binding worker is a singleton resolver (plan §1.7): identity-exact —
      // a claimed head with a different Agent never runs against this binding (the latent P4
      // gap) — and digest-transparent, because this call site registers no digest authority.
      const binding = yield* DurableWorkerBinding.makeDigestTransparent(agent);
      return yield* drainConversation(
        (submission) =>
          submission.agentId === binding.agentId
            ? Effect.succeed(binding)
            : Effect.fail(
                BindingUnavailable.make({
                  agentId: submission.agentId,
                  message: `This worker is bound to Agent ${binding.agentId}; the claimed head belongs to Agent ${submission.agentId} (SUB-023)`,
                }),
              ),
        conversationId,
      );
    });

  const processConversationResolvedImpl = (
    conversationId: ConversationId,
  ): Effect.Effect<
    ReadonlyArray<Settlement>,
    DurableWorkerFailure | DurableBindingFailure,
    AgentBindingResolver
  > =>
    Effect.gen(function* () {
      const resolver = yield* AgentBindingResolver;
      return yield* drainConversation(
        (submission) => resolver.resolve(submission.agentId, submission.agentDigests),
        conversationId,
      );
    });

  const claimFor = Effect.fn("DurableAgentRuntime.claimFor")(function* (
    submission: SubmissionSnapshot,
  ): Effect.fn.Return<Option.Option<Claim>, LedgerError | OwnershipLost> {
    const claimed = yield* ledger.claim(
      ClaimRequest.make({
        conversationId: submission.conversationId,
        producerId: config.producerId,
      }),
    );
    if (Option.isNone(claimed)) return Option.none();
    if (claimed.value.submissionId !== submission.submissionId) {
      // FIFO: only the lane head is claimable; this Submission must wait for the head.
      yield* ledger
        .releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: claimed.value.submissionId,
            ownershipToken: claimed.value.ownershipToken,
          }),
        )
        .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
      return Option.none();
    }
    return Option.some(claimed.value);
  });

  /**
   * DUR-013 audit: one deterministic `RepairAnnotated` record per executed (submission, decision)
   * pair. Conflicts mean the annotation is already canonical (or a newer epoch owns the lane), so
   * they never fail the already-idempotent repair itself.
   */
  const annotateRepair = (
    conversationId: ConversationId,
    submissionId: SubmissionId,
    decision: RecoveryDecision,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const encodedDecision = yield* Schema.encodeEffect(RecoveryDecision)(decision).pipe(
        Effect.orDie,
      );
      const details = yield* Schema.decodeUnknownEffect(PersistedJson)(encodedDecision).pipe(
        Effect.orDie,
      );
      const envelope = yield* makeEnvelope(
        recoveryRepairRecordId(submissionId, decision._tag),
        RepairAnnotated.make({ reason: `recovery:${decision._tag}`, details }),
      );
      const tail = yield* store.inspectTail(ConversationTailRequest.make({ conversationId }));
      yield* store.append(
        FencedAppendRequest.make({
          conversationId,
          batch: CanonicalBatch.make({
            batchId: recoveryRepairBatchId(submissionId, decision._tag),
            producerId: config.producerId,
            records: [envelope],
          }),
          expectedTailSequence: tail.tailSequence,
          expectedTailDigest: tail.tailDigest,
          producerEpoch: tail.producerEpoch,
        }),
      );
    }).pipe(Effect.ignore);

  const settleAbortedForRecovery = Effect.fn("DurableAgentRuntime.settleAbortedForRecovery")(
    function* (
      snapshot: RecoverySnapshot,
      evidence: RecoveryEvidence,
      records: ReadonlyArray<CanonicalRecordEnvelope>,
      _decision: SettleAbortedDecision,
    ): Effect.fn.Return<"repaired" | "deferred", DurableWorkerFailure> {
      const intent = snapshot.abortIntent;
      if (intent === undefined) return "deferred";
      const submission = snapshot.submission;
      if (submission.state === "suspended" && snapshot.suspension !== undefined) {
        if (snapshot.suspension.reason._tag === "WaitingForChild") {
          // The classifier reaches SettleAborted only when every attached-child obligation is
          // closed (open ones route to PropagateChildAbort/ResumeWaitingParent, spec §13.1), so
          // every listed child is provably settled: replay the idempotent wake to make the
          // suspended lane claimable, then settle aborted below. A wake the adapter cannot yet
          // verify defers honestly instead of guessing.
          const woken = yield* Effect.gen(function* () {
            for (const child of snapshot.suspension?.reason._tag === "WaitingForChild"
              ? snapshot.suspension.reason.children
              : []) {
              yield* ledger.recordChildSettled(
                ChildSettledNotification.make({
                  parentSubmissionId: submission.submissionId,
                  childSubmissionId: child.childSubmissionId,
                }),
              );
            }
            return true;
          }).pipe(Effect.catchTag("LedgerError", () => Effect.succeed(false)));
          if (!woken) return "deferred";
        } else {
          // A suspended head is never worker-claimable (WP2 claim rule), so the aborted
          // settlement first closes the suspension: every undecided call of the stored reason
          // gets a durable DENIED decision, which wakes the lane (`suspended → input-applied`)
          // without ever resuming the batch — the abort intent settles the Submission before
          // any Run resumes. A raced real decision also covers the reason, so its conflict is
          // absorbed.
          const decided = new Set(
            snapshot.approvalDecisions.map((decision) => decision.toolCallId),
          );
          for (const toolCallId of snapshot.suspension.reason.toolCallIds) {
            if (decided.has(toolCallId)) continue;
            yield* ledger
              .recordApprovalDecision(
                ApprovalDecisionCommand.make({
                  submissionId: submission.submissionId,
                  toolCallId,
                  decision: "denied",
                  resolver: RECOVERY_RESOLVER,
                  reason:
                    "The Submission was aborted while durably suspended; the pending approval closes denied so the aborted settlement can commit",
                }),
              )
              .pipe(
                Effect.catch((error) =>
                  error._tag === "ApprovalConflict" ? Effect.void : Effect.fail(error),
                ),
                Effect.asVoid,
              );
          }
        }
      }
      const claimed = yield* claimFor(submission);
      if (Option.isNone(claimed)) {
        // P7 §7(c): an aborted, never-claimed, still-queued `ready` Submission settles NOW
        // instead of waiting to head the lane — settlement order of never-run work is not
        // execution order (DUR-004 bounds execution; DUR-012 allows settling inactive
        // accepted work without an Attempt). The appends run at the current tail with the
        // durable abort intent as the reservation authority; any racing owner's fence
        // advance (or a concurrent joining claim) defers honestly to the next pass.
        if (submission.state === "ready" && snapshot.ownership === undefined) {
          return yield* Effect.gen(function* () {
            yield* materializeAtLeast(submission.conversationId, ZERO_EPOCH);
            yield* ensureConversationCreated(
              submission.conversationId,
              submission.agentId,
              submission.agentDigests,
            );
            const ctx = yield* attemptContextAtTail(submission.conversationId);
            const tokenRef = yield* Ref.make(QUEUED_ABORT_SETTLEMENT_TOKEN);
            yield* settleAborted(
              ctx,
              submission,
              tokenRef,
              intent,
              evidence,
              knownRecordIdsOf(records),
            );
            return "repaired" as const;
          }).pipe(
            Effect.catchTags({
              FenceRejected: () => Effect.succeed("deferred" as const),
              AppendConflict: () => Effect.succeed("deferred" as const),
              OwnershipLost: () => Effect.succeed("deferred" as const),
            }),
          );
        }
        return "deferred";
      }
      const claim = claimed.value;
      yield* store.materialize(
        ConversationMaterialization.make({
          conversationId: submission.conversationId,
          producerEpoch: claim.producerEpoch,
        }),
      );
      yield* ensureConversationCreated(
        submission.conversationId,
        submission.agentId,
        submission.agentDigests,
      );
      const ctx = yield* attemptContextFor(submission.conversationId, claim.producerEpoch);
      const tokenRef = yield* Ref.make(claim.ownershipToken);
      yield* settleAborted(ctx, submission, tokenRef, intent, evidence, knownRecordIdsOf(records));
      return "repaired";
    },
  );

  /**
   * Execute the reconcile-then-mark flow for a `MarkUnknown` decision (plan §2.2). The recovery
   * pass has no Agent Binding, so no execution-class annotation is visible here — every call
   * without a durable intent or reconciler proof stays fail-closed uncertain. A lane whose open
   * calls all closed reports `repaired`; provably-retryable calls defer to a worker's batch
   * resume; the rest become Unknown Outcomes and report the `unknown` disposition.
   */
  const markUnknownForRecovery = Effect.fn("DurableAgentRuntime.markUnknownForRecovery")(function* (
    snapshot: RecoverySnapshot,
    evidence: RecoveryEvidence,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    decision: MarkUnknownDecision,
  ): Effect.fn.Return<"repaired" | "deferred" | "unknown", DurableWorkerFailure> {
    const submission = snapshot.submission;
    const claimed = yield* claimFor(submission);
    if (Option.isNone(claimed)) return "deferred";
    const claim = claimed.value;
    yield* store.materialize(
      ConversationMaterialization.make({
        conversationId: submission.conversationId,
        producerEpoch: claim.producerEpoch,
      }),
    );
    const ctx = yield* attemptContextFor(submission.conversationId, claim.producerEpoch);
    const knownIds = knownRecordIdsOf(records);
    // The classifier's decision lists ONLY ordinary open calls (S2: an open delegation call
    // never marks Unknown — its establishment is idempotent and routes through the Subagent
    // rows), so reconciliation is scoped to exactly those ids.
    const markableIds = new Set<string>(decision.openToolCallIds);
    const review = yield* reconcileOpenCalls(
      ctx,
      submission,
      snapshot,
      records,
      evidence.openToolCalls.filter((call) => markableIds.has(call.toolCallId)),
      knownIds,
      () => undefined,
    );
    let disposition: "repaired" | "deferred" | "unknown";
    if (review.uncertain.length > 0) {
      yield* markCallsUnknown(
        ctx,
        submission.submissionId,
        knownIds,
        review.uncertain,
        decision.reason,
      );
      disposition = "unknown";
    } else if (review.unproven.length > 0 || review.retryable.length > 0) {
      disposition = "deferred";
    } else {
      disposition = "repaired";
    }
    yield* ledger
      .releaseOwnership(
        ReleaseOwnershipRequest.make({
          submissionId: submission.submissionId,
          ownershipToken: claim.ownershipToken,
        }),
      )
      .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
    return disposition;
  });

  /**
   * Apply covering DUR-017 resolution intents canonically and wake the lane. The `unknown` head
   * is never claimable, so the appends run unfenced at the current tail (no live owner can
   * exist); the wake itself is the ledger's idempotent re-check on intent replay (WP2), so a
   * crash anywhere in this pass converges on the next one.
   */
  const applyUnknownResolutionsForRecovery = Effect.fn(
    "DurableAgentRuntime.applyUnknownResolutionsForRecovery",
  )(function* (
    snapshot: RecoverySnapshot,
    evidence: RecoveryEvidence,
    records: ReadonlyArray<CanonicalRecordEnvelope>,
  ): Effect.fn.Return<"repaired" | "deferred", DurableWorkerFailure> {
    const submission = snapshot.submission;
    const ctx = yield* attemptContextAtTail(submission.conversationId);
    const knownIds = knownRecordIdsOf(records);
    const applied = yield* reconcileOpenCalls(
      ctx,
      submission,
      snapshot,
      records,
      evidence.openToolCalls,
      knownIds,
      () => undefined,
    ).pipe(
      Effect.as(true),
      // A fence advance mid-pass means another owner took the Conversation: defer to it.
      Effect.catchTag("FenceRejected", () => Effect.succeed(false)),
    );
    if (!applied) return "deferred";
    for (const intent of snapshot.unknownResolutions) {
      // Replaying the stored intent re-checks the covering wake idempotently (WP2 contract).
      yield* ledger
        .recordUnknownResolution(
          UnknownResolutionCommand.make({
            submissionId: intent.submissionId,
            toolCallId: intent.toolCallId,
            author: intent.author,
            reason: intent.reason,
            resolution: intent.resolution,
          }),
        )
        .pipe(
          Effect.catchTag("UnknownResolutionConflict", (error) =>
            LedgerError.make({
              operation: "applyUnknownResolutions",
              message: `Replaying the stored resolution intent for ${error.toolCallId} diverged`,
              cause: error,
            }),
          ),
        );
    }
    yield* wake.notify(submission.conversationId);
    return "repaired";
  });

  /**
   * Repair a lost durable suspension from canonical history (plan §2.6, crash between the
   * canonical `ToolApprovalRequested` append and the ledger `suspend` transition): the lane is
   * claimable but the Run cannot proceed without a decision, so the executor claims it and moves
   * it to `suspended` — the ledger op ends the ownership period itself — reporting `repaired`.
   * Nothing executes and nothing settles. A lane that is already suspended has nothing to
   * repair: it waits durably for the authorized `resolveApproval` path, consuming no worker
   * permit, and stays `deferred`. Decisions that raced ahead of the repair leave the lane to a
   * worker's batch resume (`deferred`).
   */
  const awaitApprovalForRecovery = Effect.fn("DurableAgentRuntime.awaitApprovalForRecovery")(
    function* (
      snapshot: RecoverySnapshot,
      evidence: RecoveryEvidence,
    ): Effect.fn.Return<"repaired" | "deferred", DurableWorkerFailure> {
      const submission = snapshot.submission;
      if (submission.state === "suspended") return "deferred";
      const decided = new Set(snapshot.approvalDecisions.map((decision) => decision.toolCallId));
      const undecided = evidence.approvalsPending.filter(
        (pending) => !decided.has(pending.toolCallId),
      );
      const first = undecided[0];
      if (first === undefined) return "deferred";
      const claimed = yield* claimFor(submission);
      if (Option.isNone(claimed)) return "deferred";
      const claim = claimed.value;
      const outcome = yield* ledger.suspend(
        SuspendRequest.make({
          submissionId: submission.submissionId,
          ownershipToken: claim.ownershipToken,
          reason: ApprovalPendingSuspension.make({
            toolCallIds: [
              first.toolCallId,
              ...undecided.slice(1).map((pending) => pending.toolCallId),
            ],
          }),
        }),
      );
      yield* hit("approval:after-suspend");
      if (outcome === "resume-immediately") {
        // Decisions raced in between the snapshot read and the suspend transaction: nothing to
        // repair — release the claim so a worker resumes the declared batch.
        yield* ledger
          .releaseOwnership(
            ReleaseOwnershipRequest.make({
              submissionId: submission.submissionId,
              ownershipToken: claim.ownershipToken,
            }),
          )
          .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
        return "deferred";
      }
      return "repaired";
    },
  );

  /**
   * Defensive branch for a `suspended` lane whose canonical approval requests are all decided
   * (the classifier's `ResumeSuspended`). Under this coordinator the state is unreachable: the
   * covering `recordApprovalDecision` wakes the lane atomically inside the adapter (WP2), and a
   * decision racing ahead of `suspend` returns `resume-immediately` without ever suspending. The
   * executor therefore only re-hints the lane and reports `deferred`, keeping the obligation
   * visible instead of guessing at a wake the ledger port does not offer.
   */
  const resumeSuspendedForRecovery = Effect.fn("DurableAgentRuntime.resumeSuspendedForRecovery")(
    function* (snapshot: RecoverySnapshot): Effect.fn.Return<"deferred", DurableWorkerFailure> {
      yield* wake.notify(snapshot.submission.conversationId);
      return "deferred";
    },
  );

  const executeRecoveryDecision = Effect.fn("DurableAgentRuntime.executeRecoveryDecision")(
    function* (
      snapshot: RecoverySnapshot,
      evidence: RecoveryEvidence,
      decision: RecoveryDecision,
      records: ReadonlyArray<CanonicalRecordEnvelope>,
    ): Effect.fn.Return<"repaired" | "deferred" | "none" | "unknown", DurableWorkerFailure> {
      const submission = snapshot.submission;
      switch (decision._tag) {
        case "NoAction": {
          return "none";
        }
        case "ResumeFromTurnBoundary":
        case "ResumePendingToolBatch": {
          // Resumption needs the Agent Binding: a claiming worker resumes from the committed
          // boundary (the declared batch resumes without model re-invocation, durability §15).
          return "deferred";
        }
        case "MarkUnknown": {
          return yield* markUnknownForRecovery(snapshot, evidence, records, decision);
        }
        case "ApplyUnknownResolutions": {
          return yield* applyUnknownResolutionsForRecovery(snapshot, evidence, records);
        }
        case "AwaitUnknownResolution": {
          // The lane stays durably blocked awaiting the authorized DUR-017 resolution path;
          // the settlement obligation stays visible, nothing replays.
          return "unknown";
        }
        case "AwaitApprovalDecision": {
          return yield* awaitApprovalForRecovery(snapshot, evidence);
        }
        case "ResumeSuspended": {
          return yield* resumeSuspendedForRecovery(snapshot);
        }
        case "RevertJoining": {
          // `joining` without a canonical `input:{sid}` record: the host never consumed the
          // input, so the claim returns to ready and is delivered exactly once later
          // (DUR-016). Ownership-free by contract; the wake hint reopens the lane.
          yield* ledger.revertJoining(
            RevertJoiningRequest.make({ submissionId: submission.submissionId }),
          );
          yield* wake.notify(submission.conversationId);
          return "repaired";
        }
        case "RepairJoinMarker": {
          const hostSubmissionId = snapshot.hostSubmissionId;
          if (hostSubmissionId === undefined) return "deferred";
          const inputEnvelope = records.find(
            (envelope) =>
              envelope.record.recordId === submissionInputRecordId(submission.submissionId),
          );
          if (inputEnvelope === undefined) return "deferred";
          if (evidence.hostSettlementOutcome !== undefined) {
            // The host settled while the marker was lost, so no host ownership can ever repair
            // it. The coverage rule decides honestly (DUR-016): an uncovered input was never
            // consumed by the host and returns to ready to run as its own Run (the canonical
            // `input:{sid}` record reattaches through the ordinary input-marker repair); a
            // covered-but-unmarked input is unreachable under this coordinator (`markJoined`
            // always precedes delivery), so it stays visible instead of being guessed at.
            if (!evidence.joinedInputCovered) {
              yield* ledger.revertJoining(
                RevertJoiningRequest.make({ submissionId: submission.submissionId }),
              );
              yield* wake.notify(submission.conversationId);
              return "repaired";
            }
            return "deferred";
          }
          // `markJoined` is fenced by the HOST lane's ownership: claim the host head, repair
          // the marker from history (DUR-015), and release. A live host defers to that host's
          // own drain-seam repair.
          const host = yield* ledger.lookup(
            SubmissionLookupById.make({ submissionId: hostSubmissionId }),
          );
          if (Option.isNone(host)) return "deferred";
          const claimed = yield* claimFor(host.value);
          if (Option.isNone(claimed)) return "deferred";
          const claim = claimed.value;
          yield* ledger.markJoined(
            MarkJoinedRequest.make({
              submissionId: submission.submissionId,
              ownershipToken: claim.ownershipToken,
              recordId: inputEnvelope.record.recordId,
              sequence: inputEnvelope.sequence,
            }),
          );
          yield* ledger
            .releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: hostSubmissionId,
                ownershipToken: claim.ownershipToken,
              }),
            )
            .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
          yield* wake.notify(submission.conversationId);
          return "repaired";
        }
        case "SettleJoinedWithHost": {
          const hostSubmissionId = snapshot.hostSubmissionId;
          if (hostSubmissionId === undefined) return "deferred";
          // The host settled canonically, so no live owner can exist for this lane (a joined
          // head is never claimable): the joined settlement completes unfenced at the current
          // tail, deferring to any racing fence advance.
          const ctx = yield* attemptContextAtTail(submission.conversationId);
          const applied = yield* settleOneJoined(
            ctx,
            hostSubmissionId,
            decision.outcome,
            snapshot,
          ).pipe(
            Effect.as(true),
            Effect.catchTag("FenceRejected", () => Effect.succeed(false)),
          );
          return applied ? "repaired" : "deferred";
        }
        case "AwaitHostSettlement": {
          // The joined input reattaches through the host Run's resume (prompt-coverage rule);
          // hint the shared lane and keep the obligation visible.
          yield* wake.notify(submission.conversationId);
          return "deferred";
        }
        case "CompleteMaterialization":
        case "RepairReadiness": {
          yield* materializeAtLeast(submission.conversationId, ZERO_EPOCH);
          yield* ensureConversationCreated(
            submission.conversationId,
            submission.agentId,
            submission.agentDigests,
          );
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: submission.submissionId }));
          return "repaired";
        }
        case "ApplyInput":
        case "RepairInputMarker": {
          const claimed = yield* claimFor(submission);
          if (Option.isNone(claimed)) return "deferred";
          const claim = claimed.value;
          yield* store.materialize(
            ConversationMaterialization.make({
              conversationId: submission.conversationId,
              producerEpoch: claim.producerEpoch,
            }),
          );
          yield* ensureConversationCreated(
            submission.conversationId,
            submission.agentId,
            submission.agentDigests,
          );
          const ctx = yield* attemptContextFor(submission.conversationId, claim.producerEpoch);
          const tokenRef = yield* Ref.make(claim.ownershipToken);
          const currentRecords = yield* readAll(submission.conversationId);
          yield* applyCanonicalInput(
            ctx,
            submission,
            tokenRef,
            currentRecords,
            snapshot.inputApplied,
          );
          const ownershipToken = yield* Ref.get(tokenRef);
          yield* ledger
            .releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: submission.submissionId,
                ownershipToken,
              }),
            )
            .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
          return "repaired";
        }
        case "AppendReservedSettlement": {
          const reservation = snapshot.reservation;
          if (reservation === undefined) return "deferred";
          const claimed = yield* claimFor(submission);
          if (Option.isNone(claimed)) {
            // P7 §7(c) crash replay: a queued-abort settlement that committed its reservation
            // but lost the append/finalize completes at the current tail — the aborted,
            // never-claimed row still holds no live ownership, so no claim can ever exist
            // for it while it stays queued behind the head.
            if (
              reservation.outcome === "aborted" &&
              snapshot.abortIntent !== undefined &&
              snapshot.ownership === undefined
            ) {
              const ctx = yield* attemptContextAtTail(submission.conversationId);
              return yield* completeReservation(
                ctx,
                submission,
                reservation,
                evidence.recordedSettlementOutcome !== undefined,
              ).pipe(
                Effect.as("repaired" as const),
                Effect.catchTags({
                  FenceRejected: () => Effect.succeed("deferred" as const),
                  AppendConflict: () => Effect.succeed("deferred" as const),
                }),
              );
            }
            return "deferred";
          }
          const claim = claimed.value;
          yield* store.materialize(
            ConversationMaterialization.make({
              conversationId: submission.conversationId,
              producerEpoch: claim.producerEpoch,
            }),
          );
          const ctx = yield* attemptContextFor(submission.conversationId, claim.producerEpoch);
          yield* completeReservation(
            ctx,
            submission,
            reservation,
            evidence.recordedSettlementOutcome !== undefined,
          );
          return "repaired";
        }
        case "FinalizeLedgerFromHistory": {
          yield* finalizeFromHistory(submission, decision.settlementId);
          return "repaired";
        }
        case "SettleAborted": {
          return yield* settleAbortedForRecovery(snapshot, evidence, records, decision);
        }
        case "CompleteChildAdmission": {
          // Binding-free (D3): the canonical `SubagentRequested` payload carries the encoded
          // child input, intended identity, and every digest, so admission completes without a
          // live delegation handler — one child, same Receipt on every replay (SUB-016).
          const subagent = subagentRecordsOf(records, runIdForSubmission(submission.submissionId));
          const requestedPayload = subagent.requested.get(decision.toolCallId);
          if (requestedPayload === undefined) return "deferred";
          const admission = yield* establishChildFromRequest(submission, requestedPayload);
          return admission._tag === "indeterminate" ? "deferred" : "repaired";
        }
        case "RepairSubagentStartLink": {
          // Resolve the SAME child by its deterministic idempotency key, complete any missing
          // materialization/lineage/readiness, then append the exact deterministic
          // `SubagentStarted` link and reattach the reservation under the parent fence
          // (spec §13, SUB-016/SUB-017).
          const runId = runIdForSubmission(submission.submissionId);
          const subagent = subagentRecordsOf(records, runId);
          const requestedPayload = subagent.requested.get(decision.toolCallId);
          if (requestedPayload === undefined) return "deferred";
          const admission = yield* establishChildFromRequest(submission, requestedPayload);
          if (admission._tag === "indeterminate") return "deferred";
          const claimed = yield* claimFor(submission);
          if (Option.isNone(claimed)) return "deferred";
          const claim = claimed.value;
          yield* store.materialize(
            ConversationMaterialization.make({
              conversationId: submission.conversationId,
              producerEpoch: claim.producerEpoch,
            }),
          );
          const ctx = yield* attemptContextFor(submission.conversationId, claim.producerEpoch);
          const knownIds = knownRecordIdsOf(records);
          const startRecordId = subagentStartedRecordId(runId, decision.toolCallId);
          if (!knownIds.has(startRecordId)) {
            const envelope = yield* makeEnvelope(
              startRecordId,
              SubagentStarted.make({
                runId,
                toolCallId: decision.toolCallId,
                childConversationId: requestedPayload.childConversationId,
                childSubmissionId: admission.childSubmissionId,
                childReceiptId: admission.receiptId,
                childRunId: runIdForSubmission(admission.childSubmissionId),
              }),
            );
            yield* appendBatch(
              ctx,
              CanonicalBatch.make({
                batchId: subagentStartedBatchId(runId, decision.toolCallId),
                producerId: config.producerId,
                records: [envelope],
              }),
            ).pipe(
              Effect.catch((error) =>
                error._tag === "AppendConflict" ? Effect.void : Effect.fail(error),
              ),
              Effect.asVoid,
            );
            yield* hit("subagent:after-start-append");
          }
          yield* ledger
            .attachChildToReservation(
              AttachChildToReservationRequest.make({
                reservationId: decodeChildReservationIdSync(requestedPayload.reservationId),
                ownershipToken: claim.ownershipToken,
                childSubmissionId: admission.childSubmissionId,
              }),
            )
            .pipe(
              Effect.catchTag(
                "ChildReservationConflict",
                conflictToLedgerError("attachChildToReservation"),
              ),
            );
          yield* ledger
            .releaseOwnership(
              ReleaseOwnershipRequest.make({
                submissionId: submission.submissionId,
                ownershipToken: claim.ownershipToken,
              }),
            )
            .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
          yield* wake.notify(requestedPayload.childConversationId);
          yield* wake.notify(submission.conversationId);
          return "repaired";
        }
        case "EnsureWaitingForChild": {
          // Restore the lost `waitingForChild` checkpoint (spec §14 "after parent start, before
          // waitingForChild checkpoint"): claim the lane and suspend it — the ledger op ends the
          // ownership period itself, so the lane holds no worker permit while each child runs
          // on its own lane. Never spawns a replacement invocation (SUB-018/SUB-030).
          const claimed = yield* claimFor(submission);
          if (Option.isNone(claimed)) return "deferred";
          const claim = claimed.value;
          const suspension = yield* ledger.suspend(
            SuspendRequest.make({
              submissionId: submission.submissionId,
              ownershipToken: claim.ownershipToken,
              reason: WaitingForChildSuspension.make({ children: decision.children }),
            }),
          );
          yield* hit("subagent:after-suspend");
          if (suspension === "resume-immediately") {
            // Every listed child already settled: leave the joins to a claiming worker's batch
            // resume (they need the parent Binding and its result projection).
            yield* ledger
              .releaseOwnership(
                ReleaseOwnershipRequest.make({
                  submissionId: submission.submissionId,
                  ownershipToken: claim.ownershipToken,
                }),
              )
              .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
            return "deferred";
          }
          return "repaired";
        }
        case "ResumeWaitingParent": {
          // Every relevant child is provably settled: replay the idempotent ownership-free wake
          // (a dropped wake is never a lost obligation) so a claiming worker resumes the
          // declared batch and joins each child's canonical Settlement (spec §13).
          for (const child of decision.children) {
            yield* ledger.recordChildSettled(
              ChildSettledNotification.make({
                parentSubmissionId: submission.submissionId,
                childSubmissionId: child.childSubmissionId,
              }),
            );
          }
          yield* wake.notify(submission.conversationId);
          return "repaired";
        }
        case "ApplyJoinAccounting": {
          // Budget release incomplete after a canonical join: replay the accounting decision
          // FROM the canonical `SubagentJoined` record — budget stays unavailable until repair,
          // never available twice (spec §12 join step 6, DUR-015).
          const reservation = snapshot.childReservations.find(
            (row) => row.reservationId === decision.reservationId,
          );
          if (reservation === undefined) return "deferred";
          if (reservation.status === "released") return "repaired";
          const subagent = subagentRecordsOf(records, runIdForSubmission(submission.submissionId));
          const joinedPayload = subagent.joined.get(decision.toolCallId);
          if (joinedPayload !== undefined) {
            yield* applyReservationRelease(decision.reservationId, joinedPayload.finalAccounting);
            return "repaired";
          }
          if (reservation.status === "releasePending" && reservation.accounting !== undefined) {
            // The decision is already frozen: finish the idempotent release — never re-freeze.
            yield* applyReservationRelease(decision.reservationId, reservation.accounting);
            return "repaired";
          }
          return "deferred";
        }
        case "PropagateChildAbort": {
          // Request-abort-and-join (spec §13.1): the ONE idempotent durable abort command per
          // nonterminal child — the recorded child `AbortIntent` row IS the propagation marker,
          // so the replayed command returns it unchanged (DUR-012) — while the parent stays (or
          // becomes) suspended `waitingForChild` for the joins.
          for (const child of decision.children) {
            yield* ledger
              .requestAbort(
                AbortCommand.make({
                  submissionId: child.childSubmissionId,
                  author: SUBAGENT_ABORT_AUTHOR,
                  reason: SUBAGENT_ABORT_REASON,
                }),
              )
              .pipe(
                // The child settled concurrently: its one winning Settlement joins next pass.
                Effect.catchTag("SettlementConflict", () => Effect.void),
                Effect.catchTag("JoinedToHost", conflictToLedgerError("PropagateChildAbort")),
                Effect.asVoid,
              );
            yield* hit("subagent:after-child-abort-intent");
            const childRow = yield* ledger.lookup(
              SubmissionLookupById.make({ submissionId: child.childSubmissionId }),
            );
            if (Option.isSome(childRow)) {
              yield* wake.notify(childRow.value.conversationId);
            }
          }
          if (submission.state !== "suspended") {
            const claimed = yield* claimFor(submission);
            if (Option.isNone(claimed)) return "deferred";
            const claim = claimed.value;
            const suspension = yield* ledger.suspend(
              SuspendRequest.make({
                submissionId: submission.submissionId,
                ownershipToken: claim.ownershipToken,
                reason: WaitingForChildSuspension.make({ children: decision.children }),
              }),
            );
            yield* hit("subagent:after-suspend");
            if (suspension === "resume-immediately") {
              // Every child settled while suspending: the next pass joins the winners.
              yield* ledger
                .releaseOwnership(
                  ReleaseOwnershipRequest.make({
                    submissionId: submission.submissionId,
                    ownershipToken: claim.ownershipToken,
                  }),
                )
                .pipe(Effect.catchTag("OwnershipLost", () => Effect.void));
            }
          }
          return "repaired";
        }
        case "ReleaseOrphanChildReservation": {
          // Provably childless reservations release exactly once (spec §13/§14): freeze the
          // deterministic zero-consumed decision, then apply it idempotently.
          for (const reservationId of decision.reservationIds) {
            yield* releaseOrphanReservation(reservationId);
          }
          return "repaired";
        }
        case "AwaitChildAdmissionResolution": {
          // Wait-and-retry (SUB-031): the evidence assembler re-queries the authoritative owner
          // with the deterministic idempotency key on every recovery pass; an indeterminate
          // answer never permits a second admission and holds no worker permit.
          return "deferred";
        }
        case "AwaitChildSettlement": {
          // The parent lane stays dormant `waitingForChild` (SUB-030): the child's Settlement
          // wakes it durably through `recordChildSettled`; an unresolved ordinary Tool inside
          // the child keeps the parent here honestly with the obligation visible (SUB-021).
          return "deferred";
        }
        case "AwaitParentEstablishment": {
          // P7 §7(a): the child lane defers its own materialization/readiness repair until the
          // parent's idempotent establishment appends the immutable lineage record — a child
          // never runs a Turn before its lineage is canonical (model-checked,
          // `formal/SubagentEstablishmentFix.cfg`). Liveness: a droppable wake hint nudges the
          // parent lane, whose own recovery re-drives establishment (CompleteChildAdmission /
          // RepairSubagentStartLink); the deterministic child identity makes every replay
          // converge on this one child (SUB-016).
          const parent = yield* ledger.lookup(
            SubmissionLookupById.make({ submissionId: decision.parentSubmissionId }),
          );
          if (Option.isSome(parent)) {
            yield* wake.notify(parent.value.conversationId);
          }
          return "deferred";
        }
      }
    },
  );

  const recoverSubmission = Effect.fn("DurableAgentRuntime.recoverSubmission")(function* (
    submission: SubmissionSnapshot,
  ): Effect.fn.Return<RecoveryReport, DurableWorkerFailure> {
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
    );
    const read = yield* readAllTolerant(submission.conversationId);
    const evidence = yield* evidenceFor(
      read.records,
      submission.submissionId,
      read.materialized,
      snapshot.hostSubmissionId,
    );
    const decision = classifyRecovery(snapshot, evidence);
    const disposition = yield* executeRecoveryDecision(snapshot, evidence, decision, read.records);
    if (disposition === "repaired") {
      yield* annotateRepair(submission.conversationId, submission.submissionId, decision);
    }
    return RecoveryReport.make({
      submissionId: submission.submissionId,
      conversationId: submission.conversationId,
      decision,
      disposition,
    });
  });

  const runRecoveryImpl = Effect.fn("DurableAgentRuntime.runRecovery")(
    function* (): Effect.fn.Return<ReadonlyArray<RecoveryReport>, DurableWorkerFailure> {
      const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
      const reports: Array<RecoveryReport> = [];
      for (const submission of nonterminal) {
        reports.push(yield* recoverSubmission(submission));
      }
      return reports;
    },
  );

  const submit = Effect.fn("DurableAgentRuntime.submit")(function* <InputSchema extends Schema.Top>(
    agent: DurableSubmitAgent<InputSchema>,
    input: InputSchema["Type"],
    options: DurableSubmitOptions,
  ): Effect.fn.Return<Receipt, DurableSubmitFailure, InputSchema["EncodingServices"]> {
    const encodedInput = yield* Schema.encodeEffect(agent.definition.input)(input).pipe(
      Effect.mapError((cause) =>
        AgentInputError.make({ message: `Unable to encode Agent input: ${cause.message}` }),
      ),
    );
    const inputPayload = yield* Schema.decodeUnknownEffect(PersistedJson)(encodedInput).pipe(
      Effect.mapError(() =>
        AgentInputError.make({
          message: "Agent input does not satisfy the canonical persistence bounds",
        }),
      ),
    );
    const inputDigest = yield* withCrypto(digestJson(inputPayload));
    const admitted = yield* ledger.admit(
      AdmissionRequest.make({
        conversationId: options.conversationId,
        principal: options.principal,
        idempotencyKey: options.idempotencyKey,
        agentId: agent.definition.id,
        agentDigests: options.definitions,
        deploymentId: config.deploymentId,
        inputPayload,
        inputDigest,
      }),
    );
    yield* hit("submit:after-admit");
    const receipt = Receipt.make({
      receiptId: admitted.receiptId,
      submissionId: admitted.submissionId,
      conversationId: options.conversationId,
      queueSequence: admitted.queueSequence,
    });
    // A replay of an already-ready Submission resumes by returning the original Receipt;
    // an admitted-but-not-ready Submission (ours or a crashed predecessor's) is completed here.
    if (admitted.replayed && admitted.state !== "admitted") {
      return receipt;
    }
    yield* materializeAtLeast(options.conversationId, ZERO_EPOCH);
    yield* ensureConversationCreated(
      options.conversationId,
      agent.definition.id,
      options.definitions,
    );
    yield* hit("submit:after-materialize");
    yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
    yield* wake.notify(options.conversationId);
    return receipt;
  });

  const awaitSettlement = Effect.fn("DurableAgentRuntime.awaitSettlement")(function* (
    receipt: Receipt,
  ): Effect.fn.Return<Settlement, DurableAwaitFailure> {
    while (true) {
      const snapshot = yield* ledger.lookup(
        SubmissionLookupById.make({ submissionId: receipt.submissionId }),
      );
      if (Option.isNone(snapshot)) {
        return yield* LedgerError.make({
          operation: "awaitSettlement",
          message: `Unknown Submission ${receipt.submissionId}`,
        });
      }
      if (snapshot.value.state === "settled") {
        // The idempotent finalization replay returns the recorded Settlement (settledAt intact).
        return yield* ledger.finalizeSettlement(
          SettlementFinalization.make({
            submissionId: receipt.submissionId,
            settlementId: submissionSettlementId(receipt.submissionId),
          }),
        );
      }
      // Wake delivery is a pure liveness hint; the ledger poll below guarantees progress.
      yield* Effect.raceFirst(
        Stream.runDrain(
          wake.wakes.pipe(
            Stream.filter((conversationId) => conversationId === receipt.conversationId),
            Stream.take(1),
          ),
        ),
        Effect.sleep(config.settlementPollInterval),
      );
    }
  });

  const observe = (receipt: Receipt, options?: DurableObserveOptions) =>
    Stream.unwrap(
      operationAuthorizer
        .authorize(
          OperationAuthorizationRequest.make({
            operation: "observe",
            conversationId: receipt.conversationId,
            submissionId: receipt.submissionId,
          }),
        )
        .pipe(
          Effect.as(
            store.observe(
              ConversationObservation.make({
                conversationId: receipt.conversationId,
                ...(options?.after === undefined ? {} : { afterOffset: options.after }),
              }),
            ),
          ),
        ),
    );

  const abort = Effect.fn("DurableAgentRuntime.abort")(function* (
    command: AbortCommand,
  ): Effect.fn.Return<AbortIntent, DurableAbortFailure> {
    const intent = yield* ledger.requestAbort(command);
    yield* hit("abort:after-intent");
    const snapshot = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: command.submissionId }),
    );
    if (Option.isSome(snapshot)) {
      yield* wake.notify(snapshot.value.conversationId);
    }
    return intent;
  });

  /**
   * DUR-017 resolution surface (abort-shaped, plan §2.2): the durable ledger intent commits
   * first; the canonical `ToolCallResolved` (+ `ToolCallSettled` for a recovered result) is
   * appended by the recovery pass or the next owning Attempt. An `AbortSubmission` resolution
   * routes into the existing abort path — the unknown calls stay recorded and abort never
   * asserts external rollback (durability §13). Possession of this service plus the mandatory
   * author/reason audit fields is the Phase 5 authorization boundary, identical to `abort`; the
   * authenticated operator surface is a P7 deliverable.
   */
  const resolveUnknown = Effect.fn("DurableAgentRuntime.resolveUnknown")(function* (
    command: UnknownResolutionCommand,
  ): Effect.fn.Return<UnknownResolutionIntent, DurableResolveFailure | OperationDenied> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "resolveUnknown",
        submissionId: command.submissionId,
      }),
    );
    const intent = yield* ledger.recordUnknownResolution(command);
    yield* hit("resolve:after-intent");
    if (command.resolution._tag === "AbortSubmission") {
      yield* ledger.requestAbort(
        AbortCommand.make({
          submissionId: command.submissionId,
          author: command.author,
          reason: command.reason,
        }),
      );
    }
    const snapshot = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: command.submissionId }),
    );
    if (Option.isSome(snapshot)) {
      yield* wake.notify(snapshot.value.conversationId);
    }
    return intent;
  });

  /**
   * Durable approval decision surface (plan §2.6, abort-shaped): the ledger intent commits
   * first — idempotent per (submission, tool call), with a typed `ApprovalConflict` on a
   * divergent re-decision — and the adapter transitions `suspended → input-applied` atomically
   * once every pending call of the stored suspension reason is decided; the wake hint then lets
   * a worker resume the declared batch through the batch-resume seam (no model re-invocation).
   * The canonical `ToolApprovalDecided` record is appended by the resuming Attempt's approval
   * hook before the decision is honored — never here. A denied decision fails the Run through
   * the engine's `AgentApprovalDenied` path (denial-terminal, P2 policy default). Possession of
   * this service plus the mandatory resolver/reason audit fields is the Phase 5 authorization
   * boundary, identical to `abort`; the authenticated operator surface is a P7 deliverable.
   */
  const resolveApproval = Effect.fn("DurableAgentRuntime.resolveApproval")(function* (
    command: ApprovalDecisionCommand,
  ): Effect.fn.Return<ApprovalDecisionIntent, DurableApprovalFailure | OperationDenied> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "resolveApproval",
        submissionId: command.submissionId,
      }),
    );
    const intent = yield* ledger.recordApprovalDecision(command);
    const snapshot = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: command.submissionId }),
    );
    if (Option.isSome(snapshot)) {
      yield* wake.notify(snapshot.value.conversationId);
    }
    return intent;
  });

  // -------------------------------------------------------------------------
  // P7 administrative operations (plan §3): explain/verify/retry/wake/scanObligations over the
  // SAME two ports the coordinator already owns, so they behave identically on DN and DC.
  // -------------------------------------------------------------------------

  /** Whole non-negative seconds between a recorded instant and Clock-now (TestClock-driven). */
  const ageSecondsSince = (instant: DateTime.Utc, nowMillis: number): number =>
    Math.max(0, Math.floor((nowMillis - DateTime.toEpochMillis(instant)) / 1_000));

  const lookupKnownSubmission = Effect.fn("DurableAgentRuntime.lookupKnownSubmission")(function* (
    operation: string,
    submissionId: SubmissionId,
  ): Effect.fn.Return<SubmissionSnapshot, LedgerError> {
    const found = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    if (Option.isNone(found)) {
      return yield* LedgerError.make({
        operation,
        message: `Unknown Submission ${submissionId}`,
      });
    }
    return found.value;
  });

  /**
   * Read-only explanation of one Submission: the same snapshot + tolerant canonical read +
   * pure classification the recovery pass performs, packaged WITHOUT executing anything —
   * assembling it performs zero writes (P7 exit gate: operators explain recovery state without
   * editing storage).
   */
  const explainSubmission = Effect.fn("DurableAgentRuntime.explainSubmission")(function* (
    submission: SubmissionSnapshot,
  ): Effect.fn.Return<RecoveryExplanation, LedgerError | ConversationStoreError | RunJournalError> {
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
    );
    const read = yield* readAllTolerant(submission.conversationId);
    const evidence = yield* evidenceFor(
      read.records,
      submission.submissionId,
      read.materialized,
      snapshot.hostSubmissionId,
    );
    const decision = classifyRecovery(snapshot, evidence);
    const nowMillis = yield* Clock.currentTimeMillis;
    const explainedAt = yield* nowUtc;
    const runId = runIdForSubmission(submission.submissionId);
    const resolvedIds = new Set(snapshot.unknownResolutions.map((intent) => intent.toolCallId));
    const unknownCalls: Array<ExplainedUnknownCall> = [];
    for (const envelope of read.records) {
      const payload = envelope.record.payload;
      if (payload._tag !== "ToolCallUnknown" || payload.runId !== runId) continue;
      unknownCalls.push(
        ExplainedUnknownCall.make({
          toolCallId: payload.toolCallId,
          toolName: payload.toolName,
          reason: payload.reason,
          recordedAt: envelope.record.createdAt,
          resolved: resolvedIds.has(payload.toolCallId),
        }),
      );
    }
    const row = snapshot.submission;
    return RecoveryExplanation.make({
      submission: ExplainedSubmission.make({
        submissionId: row.submissionId,
        conversationId: row.conversationId,
        state: row.state,
        queueSequence: row.queueSequence,
        createdAt: row.createdAt,
        ageSeconds: ageSecondsSince(row.createdAt, nowMillis),
        ...(row.readyAt === undefined
          ? {}
          : { readyAt: row.readyAt, readyAgeSeconds: ageSecondsSince(row.readyAt, nowMillis) }),
        ...(row.parentLinkage === undefined ? {} : { parentLinkage: row.parentLinkage }),
      }),
      evidence: ExplainedEvidence.make({
        conversationMaterialized: evidence.conversationMaterialized,
        inputRecorded: evidence.inputRecorded,
        abortRecorded: evidence.abortRecorded,
        openToolCalls: evidence.openToolCalls,
        openDelegationCalls: evidence.openDelegationCalls,
        approvalsPending: evidence.approvalsPending,
        unknownCalls,
        approvalDecisions: snapshot.approvalDecisions,
        unknownResolutions: snapshot.unknownResolutions,
        childAttachments: snapshot.childAttachments,
        joins: snapshot.joins,
        ...(evidence.recordedSettlementOutcome === undefined
          ? {}
          : { recordedSettlementOutcome: evidence.recordedSettlementOutcome }),
        ...(snapshot.hostSubmissionId === undefined
          ? {}
          : { hostSubmissionId: snapshot.hostSubmissionId }),
        ...(snapshot.suspension === undefined ? {} : { suspension: snapshot.suspension }),
        ...(snapshot.abortIntent === undefined ? {} : { abortIntent: snapshot.abortIntent }),
      }),
      decision,
      decisionMeaning: recoveryDecisionMeaning(decision._tag),
      disposition: predictRecoveryDisposition(decision, snapshot),
      explainedAt,
    });
  });

  const explain = Effect.fn("DurableAgentRuntime.explain")(function* (
    submissionId: SubmissionId,
  ): Effect.fn.Return<RecoveryExplanation, DurableExplainFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "explain", submissionId }),
    );
    const submission = yield* lookupKnownSubmission("explain", submissionId);
    return yield* explainSubmission(submission);
  });

  const explainConversation = Effect.fn("DurableAgentRuntime.explainConversation")(function* (
    conversationId: ConversationId,
  ): Effect.fn.Return<ReadonlyArray<RecoveryExplanation>, DurableExplainFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "explain", conversationId }),
    );
    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
    const explanations: Array<RecoveryExplanation> = [];
    for (const submission of nonterminal) {
      if (submission.conversationId !== conversationId) continue;
      explanations.push(yield* explainSubmission(submission));
    }
    return explanations;
  });

  const verifyImpl = Effect.fn("DurableAgentRuntime.verify")(function* (
    conversationId: ConversationId,
  ): Effect.fn.Return<IntegrityReport, DurableVerifyFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "verify", conversationId }),
    );
    const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
    // Lane rows: the nonterminal scan plus every Submission the canonical log itself names —
    // the ledger port scans nonterminal work only, and canonical history is the authority for
    // everything settled (DUR-015).
    const rows = new Map<SubmissionId, SubmissionSnapshot>();
    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
    for (const submission of nonterminal) {
      if (submission.conversationId === conversationId) {
        rows.set(submission.submissionId, submission);
      }
    }
    const named = new Set<SubmissionId>();
    for (const envelope of exported.records) {
      const payload = envelope.record.payload;
      if (
        payload._tag === "UserInputRecorded" ||
        payload._tag === "SubmissionSettled" ||
        payload._tag === "AbortRequested"
      ) {
        named.add(payload.submissionId);
      }
    }
    for (const submissionId of named) {
      if (rows.has(submissionId)) continue;
      const found = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
      if (Option.isSome(found) && found.value.conversationId === conversationId) {
        rows.set(submissionId, found.value);
      }
    }
    // The latest stored checkpoint binds against the report; a typed load rejection IS an
    // integrity finding rather than an operation failure.
    const checkpointLoad:
      | { readonly _tag: "loaded"; readonly checkpoint: ConversationCheckpoint | undefined }
      | { readonly _tag: "rejected"; readonly reason: string } = yield* store
      .loadCheckpoint(LoadCheckpointRequest.make({ conversationId }))
      .pipe(
        Effect.map((checkpoint) => ({
          _tag: "loaded" as const,
          checkpoint: Option.getOrUndefined(checkpoint),
        })),
        Effect.catchTag("CheckpointRejected", (rejected) =>
          Effect.succeed({ _tag: "rejected" as const, reason: rejected.reason }),
        ),
      );
    const report = yield* verifyConversationInvariants({
      export: exported,
      submissions: [...rows.values()],
      ...(checkpointLoad._tag === "loaded" && checkpointLoad.checkpoint !== undefined
        ? { checkpoint: checkpointLoad.checkpoint }
        : {}),
    }).pipe(withCrypto);
    if (checkpointLoad._tag === "rejected") {
      const checks = [
        ...report.checks.filter((result) => result.name !== "checkpoint-binding"),
        IntegrityCheck.make({
          name: "checkpoint-binding",
          status: "failed",
          detail: `the stored checkpoint was rejected on load: ${checkpointLoad.reason}`,
        }),
      ];
      return IntegrityReport.make({
        conversationId: report.conversationId,
        tailSequence: report.tailSequence,
        recordCount: report.recordCount,
        submissionCount: report.submissionCount,
        checks,
        ok: false,
      });
    }
    return report;
  });

  /**
   * Safe re-drive of exactly one Submission's recovery decision (plan §3): classify, execute
   * the ONE repair the classifier names, append the deterministic `RepairAnnotated` audit when
   * it repairs (DUR-013 — retry adds no new record type), and wake the lane. Typed refusals
   * protect the paths that own their own operations: settled work (`NoAction`), lanes blocked
   * on Unknown Outcomes (`resolveUnknown`, DUR-017), and lanes awaiting approval decisions
   * (`resolveApproval`). The mandatory `author`/`reason` audit fields (SEC-011) annotate the
   * structured operator log.
   */
  const retryImpl = Effect.fn("DurableAgentRuntime.retry")(function* (
    command: RetryCommand,
  ): Effect.fn.Return<RecoveryReport, DurableRetryFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({
        operation: "retry",
        submissionId: command.submissionId,
      }),
    );
    const submission = yield* lookupKnownSubmission("retry", command.submissionId);
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: command.submissionId }),
    );
    const read = yield* readAllTolerant(submission.conversationId);
    const evidence = yield* evidenceFor(
      read.records,
      command.submissionId,
      read.materialized,
      snapshot.hostSubmissionId,
    );
    const decision = classifyRecovery(snapshot, evidence);
    if (decision._tag === "NoAction") {
      return yield* RetryRefused.make({
        submissionId: command.submissionId,
        refusal: "settled",
        decisionTag: decision._tag,
        message: "The Submission is settled; terminal outcomes are never revisited (DUR-002).",
      });
    }
    if (decision._tag === "AwaitUnknownResolution") {
      return yield* RetryRefused.make({
        submissionId: command.submissionId,
        refusal: "await-unknown-resolution",
        decisionTag: decision._tag,
        message:
          "The lane is durably blocked on Unknown Outcomes; resolve them through the authorized resolveUnknown path (DUR-017) instead of retrying.",
      });
    }
    if (decision._tag === "AwaitApprovalDecision") {
      return yield* RetryRefused.make({
        submissionId: command.submissionId,
        refusal: "await-approval-decision",
        decisionTag: decision._tag,
        message:
          "The lane is durably waiting for approval decisions; decide them through resolveApproval instead of retrying.",
      });
    }
    yield* Effect.logInfo("DurableAgentRuntime.retry executed an operator re-drive").pipe(
      Effect.annotateLogs({
        submissionId: command.submissionId,
        conversationId: submission.conversationId,
        author: command.author,
        reason: command.reason,
        decision: decision._tag,
      }),
    );
    const disposition = yield* executeRecoveryDecision(snapshot, evidence, decision, read.records);
    if (disposition === "repaired") {
      yield* annotateRepair(submission.conversationId, command.submissionId, decision);
    }
    yield* wake.notify(submission.conversationId);
    return RecoveryReport.make({
      submissionId: command.submissionId,
      conversationId: submission.conversationId,
      decision,
      disposition,
    });
  });

  /** The documented operator liveness nudge: a droppable wake hint for one lane. */
  const wakeImpl = Effect.fn("DurableAgentRuntime.wake")(function* (
    conversationId: ConversationId,
  ): Effect.fn.Return<void, OperationDenied> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "wake", conversationId }),
    );
    yield* wake.notify(conversationId);
  });

  /**
   * Scan-based DUR-017/OPS-001 obligation report — never a daemon: one ledger scan folded into
   * aged, severity-classified rows. Ages come from timestamps that already exist: `readyAt`/
   * `createdAt` for queued and running work, `suspendedAt` for durable suspensions, and the
   * oldest unresolved canonical `ToolCallUnknown` record for DUR-017 blocks (OPS-002).
   */
  const scanObligationsImpl = Effect.fn("DurableAgentRuntime.scanObligations")(function* (
    thresholds: ObligationThresholds,
  ): Effect.fn.Return<ObligationReport, DurableObligationFailure> {
    yield* operationAuthorizer.authorize(
      OperationAuthorizationRequest.make({ operation: "scanObligations" }),
    );
    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
    const nowMillis = yield* Clock.currentTimeMillis;
    const generatedAt = yield* nowUtc;
    const recordsCache = new Map<ConversationId, ReadonlyArray<CanonicalRecordEnvelope>>();
    const entries: Array<ObligationEntry> = [];
    for (const submission of nonterminal) {
      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: submission.submissionId }),
      );
      let blockedOn: ObligationBlockedOn;
      let since: DateTime.Utc = submission.readyAt ?? submission.createdAt;
      switch (submission.state) {
        case "unknown": {
          blockedOn = "unknown";
          let records = recordsCache.get(submission.conversationId);
          if (records === undefined) {
            records = (yield* readAllTolerant(submission.conversationId)).records;
            recordsCache.set(submission.conversationId, records);
          }
          const resolvedIds = new Set(
            snapshot.unknownResolutions.map((intent) => intent.toolCallId),
          );
          const runId = runIdForSubmission(submission.submissionId);
          let earliest: DateTime.Utc | undefined;
          for (const envelope of records) {
            const payload = envelope.record.payload;
            if (payload._tag !== "ToolCallUnknown" || payload.runId !== runId) continue;
            if (resolvedIds.has(payload.toolCallId)) continue;
            const recordedAt = envelope.record.createdAt;
            if (
              earliest === undefined ||
              DateTime.toEpochMillis(recordedAt) < DateTime.toEpochMillis(earliest)
            ) {
              earliest = recordedAt;
            }
          }
          since = earliest ?? snapshot.suspension?.suspendedAt ?? since;
          break;
        }
        case "suspended": {
          blockedOn =
            snapshot.suspension?.reason._tag === "WaitingForChild" ? "waitingForChild" : "approval";
          since = snapshot.suspension?.suspendedAt ?? since;
          break;
        }
        case "admitted":
        case "ready": {
          blockedOn = "ready-aged";
          break;
        }
        default: {
          blockedOn = "running-aged";
          break;
        }
      }
      const ageSeconds = ageSecondsSince(since, nowMillis);
      entries.push(
        ObligationEntry.make({
          submissionId: submission.submissionId,
          conversationId: submission.conversationId,
          state: submission.state,
          blockedOn,
          ageSeconds,
          severity: obligationSeverityOf(ageSeconds, thresholds),
        }),
      );
    }
    return ObligationReport.make({ thresholds, entries, generatedAt });
  });

  const runWorkerImpl = <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  >(
    agent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements
    >,
  ) =>
    Effect.gen(function* () {
      // Wake subscriptions may drop hints, so a ledger scan seeds the worklist (persistence §14).
      const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
      const seen = new Set<ConversationId>();
      for (const submission of nonterminal) {
        if (seen.has(submission.conversationId)) continue;
        seen.add(submission.conversationId);
        yield* processConversationImpl(agent, submission.conversationId);
      }
      yield* Stream.runForEach(wake.wakes, (conversationId) =>
        processConversationImpl(agent, conversationId),
      );
    });

  const runResolvedWorkerImpl: Effect.Effect<
    void,
    DurableWorkerFailure | DurableBindingFailure,
    AgentBindingResolver
  > = Effect.gen(function* () {
    // The multi-binding worker (plan §1.7): every claimed head resolves its exact stored
    // Binding through the host-supplied resolver, so one worker pool serves parent and child
    // lanes (spec §12's smallest-pool wakeup proof runs over this loop).
    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
    const seen = new Set<ConversationId>();
    for (const submission of nonterminal) {
      if (seen.has(submission.conversationId)) continue;
      seen.add(submission.conversationId);
      yield* processConversationResolvedImpl(submission.conversationId);
    }
    yield* Stream.runForEach(wake.wakes, (conversationId) =>
      processConversationResolvedImpl(conversationId),
    );
  });

  return DurableAgentRuntime.of({
    submit,
    awaitSettlement,
    observe,
    abort,
    resolveUnknown,
    resolveApproval,
    explain,
    explainConversation,
    verify: verifyImpl,
    retry: retryImpl,
    wake: wakeImpl,
    scanObligations: scanObligationsImpl,
    processConversation: processConversationImpl,
    processConversationResolved: processConversationResolvedImpl,
    runWorker: runWorkerImpl,
    runResolvedWorker: runResolvedWorkerImpl,
    runRecovery: runRecoveryImpl(),
  });
});

/**
 * Durable Agent Runtime coordinator (deployment class DN; D1/D2). It coordinates the
 * SubmissionLedger, ConversationStore, and WakeScheduler ports so that once `submit` returns a
 * Receipt, the Submission settles exactly once (DUR-001/DUR-002) while every external effect
 * remains at-least-once (DUR-003) — this runtime never claims exactly-once side effects.
 *
 * - `submit(agent, input, options)` — durable admission → Conversation materialization →
 *   `ConversationCreated` → readiness → Receipt, with failpoints between the steps; a retry with
 *   the same (conversation, principal, idempotencyKey) resumes and returns the same Receipt.
 * - `awaitSettlement(receipt)` — wake-hinted, poll-guaranteed wait; interrupting it detaches the
 *   caller only and never cancels accepted work.
 * - `observe(receipt, {after})` — canonical record observation from a stored offset.
 * - `abort(command)` — durable idempotent abort intent; inactive work settles aborted through
 *   recovery, an active worker makes the command canonical before interrupting its Run (§13),
 *   settled work fails with `SettlementConflict` (DUR-012). A `joined` Submission fails with a
 *   typed `JoinedToHost` conflict carrying the host identity — it settles with its host, so the
 *   abort target is the host; aborting a `joining` Submission records the intent, honored only
 *   if the host has not consumed the input (revert-then-abort, plan §2.5).
 * - `resolveUnknown(command)` — the authorized DUR-017 resolution path for Unknown Outcomes:
 *   the durable intent is idempotent per (submission, tool call) and conflicts typed on
 *   divergence; the canonical resolution records are applied by recovery or the next Attempt,
 *   and the lane wakes once every marked call is covered.
 * - `resolveApproval(command)` — the durable approval decision path (plan §2.6): the intent is
 *   idempotent per (submission, tool call) with a typed `ApprovalConflict` on divergence; once
 *   every pending call of the suspension reason is decided the lane wakes
 *   (`suspended → input-applied`) and the next Attempt resumes the declared batch without model
 *   re-invocation, appending the canonical `ToolApprovalDecided` before honoring the decision.
 * - `processConversation(agent, conversationId)` — drain one lane: fenced FIFO-head claims,
 *   canonical input apply, split response/prepared/results Turn commits (plan §2.1),
 *   reconcile-then-mark for open ordinary Tool Calls (DUR-009, never an automatic replay),
 *   declared-batch resume without model re-invocation (§15), and terminalization. An active
 *   host Run claims the contiguous ready prefix of later queued Submissions at every safe Turn
 *   seam (Joining/Joined, plan §2.5): the queued input becomes canonical (`input:{sid}`) before
 *   the next model request, reattaches through the prompt-coverage rule after a crash, and the
 *   joined Submissions settle with the host outcome (DUR-002/DUR-016).
 * - `runWorker(agent)` — scan-seeded, wake-driven worker loop over every lane (WP4's host driver),
 *   reimplemented over a singleton identity-exact (digest-transparent) Binding resolution: a
 *   claimed head belonging to a different Agent never runs against this binding (SUB-023); a
 *   parent-linked head refused this way settles with the framework `ChildCompatibilityFailure`.
 * - `processConversationResolved(conversationId)` / `runResolvedWorker` — the S2 multi-binding
 *   equivalents over the host-supplied `AgentBindingResolver` (spec §11, D7): every claimed
 *   head's stored `(agentId, agentDigests)` resolves to the exact registered Binding before any
 *   code runs; an unresolvable parent-linked child settles with the Schema-stable framework
 *   `ChildCompatibilityFailure` (no application code, the child never executes), and an
 *   unresolvable root surfaces the typed refusal after releasing the claim.
 * - `runRecovery()` — classify every nonterminal Submission with the pure `classifyRecovery` and
 *   execute the repair decisions, appending a `RepairAnnotated` audit record per executed decision
 *   (DUR-013). Model-resuming work is reported `deferred` for a worker claim; lanes blocked on
 *   Unknown Outcomes are reported `unknown`. The S2 binding-free Subagent executors (admission
 *   completion, start-link repair, waiting restoration, wake replay, canonical join accounting,
 *   abort propagation, orphan reservation release) run here; the settlement join itself is
 *   deferred to a claiming worker because it needs the parent Binding's result projection.
 * - `explain`/`explainConversation`, `verify`, `retry`, `wake`, `scanObligations` — the P7
 *   administrative operations (plan §3) over the same two ports, identical on DN and DC.
 *   `explain` and `verify` are strictly read-only; `retry` re-drives exactly one classified
 *   repair with mandatory author/reason audit and typed refusals; `scanObligations` is the
 *   scan-based DUR-017/OPS-001 obligation surface. Every one of them (plus `observe`,
 *   `resolveUnknown`, `resolveApproval`) consults the `OperationAuthorizer` reference
 *   fail-closed — the default Layer preserves the service-possession behavior, and a
 *   host-supplied authorizer turns denials into the typed `OperationDenied`.
 */
export class DurableAgentRuntime extends Context.Service<
  DurableAgentRuntime,
  {
    readonly submit: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: DurableSubmitOptions,
    ) => Effect.Effect<Receipt, DurableSubmitFailure, InputSchema["EncodingServices"]>;
    readonly awaitSettlement: (receipt: Receipt) => Effect.Effect<Settlement, DurableAwaitFailure>;
    readonly observe: (
      receipt: Receipt,
      options?: DurableObserveOptions,
    ) => Stream.Stream<
      CanonicalRecordEnvelope,
      ConversationStoreError | ConversationNotMaterialized | OperationDenied
    >;
    readonly abort: (command: AbortCommand) => Effect.Effect<AbortIntent, DurableAbortFailure>;
    readonly resolveUnknown: (
      command: UnknownResolutionCommand,
    ) => Effect.Effect<UnknownResolutionIntent, DurableResolveFailure | OperationDenied>;
    readonly resolveApproval: (
      command: ApprovalDecisionCommand,
    ) => Effect.Effect<ApprovalDecisionIntent, DurableApprovalFailure | OperationDenied>;
    /**
     * Read-only recovery explanation of one Submission (P7 plan §3): snapshot + tolerant
     * canonical read + the pure classifier, packaged with the decision's operator meaning and
     * predicted disposition. Performs ZERO writes — the canonical log and every ledger row are
     * byte-identical before and after.
     */
    readonly explain: (
      submissionId: SubmissionId,
    ) => Effect.Effect<RecoveryExplanation, DurableExplainFailure>;
    /** `explain` for every nonterminal lane member of one Conversation, in queue order. */
    readonly explainConversation: (
      conversationId: ConversationId,
    ) => Effect.Effect<ReadonlyArray<RecoveryExplanation>, DurableExplainFailure>;
    /**
     * Read-only integrity verification of one Conversation (P7 plan §3): Schema round-trips,
     * record-identity uniqueness, sequence contiguity, FIFO input/settlement order,
     * ledger-terminal vs canonical-settlement agreement (DUR-015), and checkpoint binding —
     * typed per-check results, never a repair. The digest-chain check reports `skipped` with
     * the honest reason: full chain recomputation needs per-batch producer identity, which the
     * ConversationStore port deliberately does not export (supply it to
     * `verifyConversationInvariants` directly; adapter-level `verifyOnOpen` is the
     * storage-side audit).
     */
    readonly verify: (
      conversationId: ConversationId,
    ) => Effect.Effect<IntegrityReport, DurableVerifyFailure>;
    /**
     * Safe re-drive of one Submission's recovery decision with mandatory author/reason audit
     * (SEC-011): executes exactly the repair the classifier names and appends the
     * deterministic `RepairAnnotated` record when it repairs (DUR-013). Typed `RetryRefused`
     * for settled work and for lanes owned by the resolveUnknown/resolveApproval paths.
     */
    readonly retry: (command: RetryCommand) => Effect.Effect<RecoveryReport, DurableRetryFailure>;
    /** The documented operator liveness nudge: a droppable wake hint for one lane. */
    readonly wake: (conversationId: ConversationId) => Effect.Effect<void, OperationDenied>;
    /**
     * Scan-based DUR-017/OPS-001 obligation report: every nonterminal Submission with what it
     * is visibly blocked on, its age, and a threshold-classified severity. Never a daemon —
     * hosts run it periodically and own the alert loop (OPS-002).
     */
    readonly scanObligations: (
      thresholds: ObligationThresholds,
    ) => Effect.Effect<ObligationReport, DurableObligationFailure>;
    readonly processConversation: <
      InputSchema extends Schema.Top,
      OutputSchema extends Schema.Top,
      Instructions,
      Tools extends Record<string, Tool.Any>,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
      InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
    >(
      agent: RuntimeBinding<
        InputSchema,
        OutputSchema,
        Instructions,
        Tools,
        Provider,
        ModelProvides,
        ModelRequires,
        InstructionError,
        InstructionRequirements
      >,
      conversationId: ConversationId,
    ) => Effect.Effect<
      ReadonlyArray<Settlement>,
      DurableWorkerFailure | DurableBindingFailure,
      DurableWorkerRequirements<
        RuntimeBinding<
          InputSchema,
          OutputSchema,
          Instructions,
          Tools,
          Provider,
          ModelProvides,
          ModelRequires,
          InstructionError,
          InstructionRequirements
        >,
        InstructionRequirements
      >
    >;
    readonly processConversationResolved: (
      conversationId: ConversationId,
    ) => Effect.Effect<
      ReadonlyArray<Settlement>,
      DurableWorkerFailure | DurableBindingFailure,
      AgentBindingResolver
    >;
    readonly runWorker: <
      InputSchema extends Schema.Top,
      OutputSchema extends Schema.Top,
      Instructions,
      Tools extends Record<string, Tool.Any>,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
      InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
    >(
      agent: RuntimeBinding<
        InputSchema,
        OutputSchema,
        Instructions,
        Tools,
        Provider,
        ModelProvides,
        ModelRequires,
        InstructionError,
        InstructionRequirements
      >,
    ) => Effect.Effect<
      void,
      DurableWorkerFailure | DurableBindingFailure,
      DurableWorkerRequirements<
        RuntimeBinding<
          InputSchema,
          OutputSchema,
          Instructions,
          Tools,
          Provider,
          ModelProvides,
          ModelRequires,
          InstructionError,
          InstructionRequirements
        >,
        InstructionRequirements
      >
    >;
    readonly runResolvedWorker: Effect.Effect<
      void,
      DurableWorkerFailure | DurableBindingFailure,
      AgentBindingResolver
    >;
    readonly runRecovery: Effect.Effect<ReadonlyArray<RecoveryReport>, DurableWorkerFailure>;
  }
>()("@effect-agent/session/DurableAgentRuntime") {
  static readonly layer: Layer.Layer<
    DurableAgentRuntime,
    never,
    | SubmissionLedger
    | ConversationStore
    | WakeScheduler
    | DurableRuntimeFailpoint
    | DurableRuntimeConfig
    | ToolReconciler
    | Crypto.Crypto
  > = Layer.effect(DurableAgentRuntime)(make);
}

import {
  AgentApprovalPending,
  AgentInputError,
  ConversationId,
  IdGenerator,
  ReceiptId,
  SubmissionId,
  ToolCallId,
  type Agent,
  type AgentId,
  type AttemptId,
  type RunEvent,
  type TurnId,
} from "@effect-agent/core";
import {
  AgentRuntime,
  getToolExecutionClass,
  type AgentRuntimeRequirements,
  type RunApprovalHook,
  type RunContextHook,
  type RunDurabilityHook,
  type RunInputCommand,
  type RunInputHook,
  type RunOptions,
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
  Principal,
  QueueSequence,
  RecoverySnapshot,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  RevertJoiningRequest,
  SettlementConflict,
  SettlementFinalization,
  SettlementReservation,
  Settlement,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionSnapshot,
  SuspendRequest,
  UnknownResolutionCommand,
  submissionAbortBatchId,
  submissionAbortRecordId,
  submissionInputBatchId,
  submissionInputRecordId,
  submissionSettlementBatchId,
  submissionSettlementId,
  submissionSettlementRecordId,
  type ApprovalConflict,
  type ApprovalDecisionIntent,
  type JoinSnapshot,
  type UnknownResolutionConflict,
  type UnknownResolutionIntent,
} from "./ledger.ts";
import {
  AbortRequested,
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
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
import { PreparedToolCallEvidence, ToolReconciler } from "./reconciler.ts";
import {
  classifyRecovery,
  DeclaredPendingBatchEvidence,
  OpenToolCallEvidence,
  PendingApprovalEvidence,
  RecoveryDecision,
  RecoveryEvidence,
  type SettleAborted as SettleAbortedDecision,
} from "./recovery.ts";
import {
  RunJournalError,
  approvalDecisionBatchId,
  markUnknownBatchId,
  modelResponseInterruptedBatchId,
  modelResponseInterruptedRecordId,
  modelResponseRecordId,
  projectRunJournal,
  runIdForSubmission,
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
  ConversationMaterialization,
  ConversationNotMaterialized,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  ConversationTailRequest,
  FenceRejected,
  FencedAppendRequest,
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

/** Bound a hook-supplied approval reason to the canonical `BoundedText` persistence limits. */
const boundedApprovalReason = (reason: string | undefined, fallback: string): string => {
  const value = reason === undefined || reason.length === 0 ? fallback : reason;
  return value.length > MAX_FAILURE_MESSAGE_LENGTH
    ? value.slice(0, MAX_FAILURE_MESSAGE_LENGTH)
    : value;
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
  | { readonly _tag: "completed"; readonly result: PersistedJson }
  | { readonly _tag: "failed"; readonly result: PersistedJson }
  | { readonly _tag: "aborted" };

/**
 * What one `runModel` pass produced: a terminal `AttemptOutcome` for terminalization, or a
 * durable approval suspension (plan §2.6) — the unresolved call's canonical request is already
 * appended, NO settlement is owed by this pass, and `runAttempt` transitions the ledger.
 */
type RunPhaseOutcome =
  | AttemptOutcome
  | { readonly _tag: "suspended"; readonly toolCallId: ToolCallId };

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
  ): Effect.fn.Return<RecoveryEvidence, RunJournalError> {
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

    const openToolCalls = prepared.filter(
      (call) => !settledIds.has(call.toolCallId) && !resolvedIds.has(call.toolCallId),
    );
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
      openToolCalls,
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
        const tail = yield* Ref.get(ctx.tailRef);
        const result = yield* store.append(
          FencedAppendRequest.make({
            conversationId: ctx.conversationId,
            batch,
            expectedTailSequence: tail.sequence,
            expectedTailDigest: tail.digest,
            producerEpoch: ctx.producerEpoch,
          }),
        );
        yield* Ref.set(ctx.tailRef, { sequence: result.lastSequence, digest: result.tailDigest });
        return result;
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
      /** Pre-existing joins of this host Run, loaded lazily at the first drain seam. */
      let joinBacklog: ReadonlyArray<JoinSnapshot> | undefined;
      /** Joined inputs already handed to the engine during THIS Attempt (never re-deliver). */
      const deliveredJoinInputs = new Set<string>();
      // Canonical encoded parameters per declared call, for the approval request digest: seeded
      // by `commitResponse` (which the engine invokes before approval preflight) and by the
      // resumed batch's declared calls.
      const encodedParamsByCallId = new Map<string, unknown>();
      if (pending !== undefined) {
        for (const call of pending.calls) {
          encodedParamsByCallId.set(call.id, call.params);
        }
      }
      let currentToolTurn: { readonly turn: number; readonly turnId: TurnId } | undefined =
        pending === undefined ? undefined : { turn: pending.turn, turnId: pending.turnId };
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
      }
      const stateRef = yield* Ref.make<RunState>({
        baseLen: undefined,
        lastCommitLen: 0,
        history: undefined,
        pendingTurn: undefined,
        completedOutput: undefined,
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

      const options: RunOptions<CoordinatorHalt, never> = {
        conversationId: submission.conversationId,
        runId,
        history: pending === undefined ? journal.historyBefore : resumeProjection.historyBefore,
        onHistory,
        input,
        approval,
        durability,
        ...(pending === undefined
          ? {}
          : {
              resume: {
                turn: pending.turn,
                turnId: pending.turnId,
                calls: pending.calls,
                settled: pending.settled,
              },
            }),
        ...(journal.committedTurns === 0 ? {} : { context: resumeContext }),
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

      const recordCompleted = (output: unknown): Effect.Effect<void, DurableWorkerFailure> =>
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
            Ref.update(stateRef, (state) => ({ ...state, completedOutput: result })),
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
            return commitPendingTurn.pipe(Effect.andThen(recordCompleted(event.output)));
          }
          case "RunFailed": {
            // Preserve a completed-and-advanced final Turn for audit before the Run settles failed.
            return commitPendingTurn;
          }
          default: {
            return Effect.void;
          }
        }
      };

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
            | { readonly _tag: "suspendedRun"; readonly toolCallId: ToolCallId },
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
            return Effect.gen(function* () {
              // A recorded halt means a coordinator mutation failed inside a Tool handler
              // (the engine re-wraps step-hook errors): abort the Attempt with the original
              // infrastructure failure instead of settling the Run failed.
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
      return { _tag: "completed", result: state.completedOutput } as RunPhaseOutcome;
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
        if (outcome._tag !== "suspended") {
          return Option.some(yield* terminalize(ctx, submission, tokenRef, outcome, true));
        }
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
      const settlements: Array<Settlement> = [];
      while (true) {
        const claimed = yield* ledger.claim(
          ClaimRequest.make({ conversationId, producerId: config.producerId }),
        );
        if (Option.isNone(claimed)) return settlements as ReadonlyArray<Settlement>;
        yield* hit("claim:after-claim");
        const settlement = yield* runAttempt(agent, conversationId, claimed.value);
        if (Option.isNone(settlement)) {
          // The head is durably blocked (Unknown Outcome or approval suspension): the lane
          // frees its worker permit while the settlement obligation stays visible
          // (durability §16, plan §2.6).
          return settlements as ReadonlyArray<Settlement>;
        }
        settlements.push(settlement.value);
      }
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
        // A suspended head is never worker-claimable (WP2 claim rule), so the aborted settlement
        // first closes the suspension: every undecided call of the stored reason gets a durable
        // DENIED decision, which wakes the lane (`suspended → input-applied`) without ever
        // resuming the batch — the abort intent settles the Submission before any Run resumes.
        // A raced real decision also covers the reason, so its conflict is absorbed.
        const decided = new Set(snapshot.approvalDecisions.map((decision) => decision.toolCallId));
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
    reason: string,
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
    const review = yield* reconcileOpenCalls(
      ctx,
      submission,
      snapshot,
      records,
      evidence.openToolCalls,
      knownIds,
      () => undefined,
    );
    let disposition: "repaired" | "deferred" | "unknown";
    if (review.uncertain.length > 0) {
      yield* markCallsUnknown(ctx, submission.submissionId, knownIds, review.uncertain, reason);
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
          return yield* markUnknownForRecovery(snapshot, evidence, records, decision.reason);
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
          if (Option.isNone(claimed)) return "deferred";
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
    store.observe(
      ConversationObservation.make({
        conversationId: receipt.conversationId,
        ...(options?.after === undefined ? {} : { afterOffset: options.after }),
      }),
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
  ): Effect.fn.Return<UnknownResolutionIntent, DurableResolveFailure> {
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
  ): Effect.fn.Return<ApprovalDecisionIntent, DurableApprovalFailure> {
    const intent = yield* ledger.recordApprovalDecision(command);
    const snapshot = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: command.submissionId }),
    );
    if (Option.isSome(snapshot)) {
      yield* wake.notify(snapshot.value.conversationId);
    }
    return intent;
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

  return DurableAgentRuntime.of({
    submit,
    awaitSettlement,
    observe,
    abort,
    resolveUnknown,
    resolveApproval,
    processConversation: processConversationImpl,
    runWorker: runWorkerImpl,
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
 * - `runWorker(agent)` — scan-seeded, wake-driven worker loop over every lane (WP4's host driver).
 * - `runRecovery()` — classify every nonterminal Submission with the pure `classifyRecovery` and
 *   execute the repair decisions, appending a `RepairAnnotated` audit record per executed decision
 *   (DUR-013). Model-resuming work is reported `deferred` for a worker claim; lanes blocked on
 *   Unknown Outcomes are reported `unknown`.
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
      ConversationStoreError | ConversationNotMaterialized
    >;
    readonly abort: (command: AbortCommand) => Effect.Effect<AbortIntent, DurableAbortFailure>;
    readonly resolveUnknown: (
      command: UnknownResolutionCommand,
    ) => Effect.Effect<UnknownResolutionIntent, DurableResolveFailure>;
    readonly resolveApproval: (
      command: ApprovalDecisionCommand,
    ) => Effect.Effect<ApprovalDecisionIntent, DurableApprovalFailure>;
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
      DurableWorkerFailure,
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
      DurableWorkerFailure,
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

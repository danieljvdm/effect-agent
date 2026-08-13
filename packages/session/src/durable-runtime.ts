import {
  AgentInputError,
  ConversationId,
  IdGenerator,
  ReceiptId,
  SubmissionId,
  type Agent,
  type AgentId,
  type RunEvent,
  type TurnId,
} from "@effect-agent/core";
import {
  AgentRuntime,
  type AgentRuntimeRequirements,
  type RunContextHook,
  type RunOptions,
  type RuntimeBinding,
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
  Claim,
  ClaimRequest,
  IdempotencyKey,
  LedgerError,
  MarkInputAppliedRequest,
  MarkReadyRequest,
  OwnershipLost,
  OwnershipToken,
  Principal,
  QueueSequence,
  RecoverySnapshot,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  SettlementConflict,
  SettlementFinalization,
  SettlementReservation,
  Settlement,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionSnapshot,
  submissionAbortBatchId,
  submissionAbortRecordId,
  submissionInputBatchId,
  submissionInputRecordId,
  submissionSettlementBatchId,
  submissionSettlementId,
  submissionSettlementRecordId,
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
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
  SubmissionSettled,
  UserInputRecorded,
  type CanonicalRecordPayload,
  type SettlementOutcome,
} from "./records.ts";
import {
  classifyRecovery,
  RecoveryDecision,
  RecoveryEvidence,
  type SettleAborted as SettleAbortedDecision,
} from "./recovery.ts";
import {
  projectRunJournal,
  runIdForSubmission,
  turnCanonicalBatch,
  turnIdForRun,
  type RunJournalError,
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
const ZERO_EPOCH = Schema.decodeSync(ProducerEpoch)(0);
const READ_PAGE = 1_024;
const MAX_FAILURE_MESSAGE_LENGTH = 16_384;

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
  /** `repaired` = executed; `deferred` = a claiming worker must finish it; `none` = settled. */
  disposition: Schema.Literals(["repaired", "deferred", "none"]),
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

export type DurableAbortFailure = LedgerError | SettlementConflict | DurableRuntimeFailpointError;

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

interface AttemptAppendContext {
  readonly conversationId: ConversationId;
  readonly producerEpoch: ProducerEpoch;
  readonly tailRef: Ref.Ref<{ readonly sequence: CanonicalSequence; readonly digest: Digest }>;
  /** Serializes every canonical append of one Attempt (run commits vs. the abort watcher). */
  readonly gate: Semaphore.Semaphore;
}

const make = Effect.gen(function* () {
  const ledger = yield* SubmissionLedger;
  const store = yield* ConversationStore;
  const wake = yield* WakeScheduler;
  const failpoint = yield* DurableRuntimeFailpoint;
  const config = yield* DurableRuntimeConfig;
  const crypto = yield* Crypto.Crypto;

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

  const evidenceFor = (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    submissionId: SubmissionId,
    materialized: boolean,
  ): RecoveryEvidence => {
    const inputId = submissionInputRecordId(submissionId);
    const abortId = submissionAbortRecordId(submissionId);
    const settlementId = submissionSettlementRecordId(submissionId);
    let inputRecorded = false;
    let abortRecorded = false;
    let recordedSettlementOutcome: SettlementOutcome | undefined;
    for (const envelope of records) {
      const recordId = envelope.record.recordId;
      if (recordId === inputId) inputRecorded = true;
      else if (recordId === abortId) abortRecorded = true;
      else if (recordId === settlementId && envelope.record.payload._tag === "SubmissionSettled") {
        recordedSettlementOutcome = envelope.record.payload.outcome;
      }
    }
    return RecoveryEvidence.make({
      conversationMaterialized: materialized,
      inputRecorded,
      abortRecorded,
      unresolvedToolCall: false,
      ...(recordedSettlementOutcome === undefined ? {} : { recordedSettlementOutcome }),
    });
  };

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
   * Terminalization (durability §12, DUR-011): reserve the single exact settlement record, append
   * that exact record canonically, finalize the ledger, release the lane, and hint waiters.
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

  /** Durable abort of owned work: canonical `AbortRequested` first, then settle aborted (§13). */
  const settleAborted = Effect.fn("DurableAgentRuntime.settleAborted")(function* (
    ctx: AttemptAppendContext,
    submission: SubmissionSnapshot,
    tokenRef: Ref.Ref<OwnershipToken>,
    intent: AbortIntent,
    evidence: RecoveryEvidence,
  ): Effect.fn.Return<Settlement, DurableWorkerFailure> {
    if (!evidence.abortRecorded) {
      yield* appendAbortRecord(ctx, intent);
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
   * Plan step 3 (+6): drive `AgentRuntime.stream` with history rebuilt by the run journal, commit
   * ONE canonical batch per Turn at the TurnCompleted seam through the fenced append, watch for
   * durable abort intent, and keep the ownership lease renewed. Engine Run failures settle
   * `failed`; coordinator failures abort the Attempt cleanly with the obligation still owed.
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
  ) =>
    Effect.gen(function* () {
      const submissionId = submission.submissionId;
      const runId = runIdForSubmission(submissionId);
      const journal = yield* projectRunJournal(records, runId);

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
                // messages are already canonical inside the original Turn 1 and never re-enter.
                lastCommitLen:
                  journal.committedTurns === 0
                    ? journal.historyBefore.content.length
                    : history.content.length,
                history,
              }
            : { ...state, history },
        );

      // On resume the model must see the canonical prompt (committed Turns included) plus what
      // THIS Attempt appended — never the engine's re-appended instructions + input.
      const resumeContext: RunContextHook<never, never> = {
        prepare: ({ source }) =>
          Ref.get(stateRef).pipe(
            Effect.map((state) => ({
              prompt: Prompt.fromMessages([
                ...journal.prompt.content,
                ...source.content.slice(state.baseLen ?? source.content.length),
              ]),
            })),
          ),
      };

      const options: RunOptions<never, never> = {
        conversationId: submission.conversationId,
        runId,
        history: journal.historyBefore,
        onHistory,
        ...(journal.committedTurns === 0 ? {} : { context: resumeContext }),
      };

      const commitPendingTurn: Effect.Effect<void, DurableWorkerFailure> = Effect.gen(function* () {
        const state = yield* Ref.get(stateRef);
        const history = state.history;
        if (state.pendingTurn === undefined || history === undefined) return;
        const appended = history.content.slice(state.lastCommitLen);
        if (appended.length === 0) return;
        const createdAt = yield* nowUtc;
        const batch = yield* withCrypto(
          turnCanonicalBatch({
            runId,
            turn: journal.committedTurns + state.pendingTurn.turn,
            turnId: state.pendingTurn.turnId,
            appended,
            producerId: config.producerId,
            deploymentId: config.deploymentId,
            createdAt,
          }),
        );
        yield* appendBatch(ctx, batch);
        yield* hit("turn:after-canonical-append");
        yield* Ref.update(stateRef, (current) => ({
          ...current,
          lastCommitLen: history.content.length,
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
            const turnId = event.turnId ?? turnIdForRun(runId, journal.committedTurns + event.turn);
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
            { readonly _tag: "failedRun"; readonly outcome: AttemptOutcome },
            DurableWorkerFailure
          > =>
            error instanceof CoordinatorHalt
              ? Effect.fail(error.failure)
              : Effect.map(failureOutcome(error), (outcome) => ({
                  _tag: "failedRun" as const,
                  outcome,
                })),
        ),
      );

      if (result._tag === "aborted") {
        return { _tag: "aborted" } as AttemptOutcome;
      }
      if (result._tag === "failedRun") {
        return result.outcome;
      }
      const state = yield* Ref.get(stateRef);
      if (state.completedOutput === undefined) {
        return yield* LedgerError.make({
          operation: "runModel",
          message: "Agent Run stream ended without RunCompleted",
        });
      }
      return { _tag: "completed", result: state.completedOutput } as AttemptOutcome;
    });

  /**
   * One ownership period over the claimed lane head. The step order mirrors the pure recovery
   * classifier's decision table, so a fresh Attempt and a recovering Attempt take the same path
   * through the same idempotent steps.
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
      const evidence = evidenceFor(records, submissionId, true);

      if (evidence.recordedSettlementOutcome !== undefined) {
        return yield* finalizeFromHistory(
          submission,
          snapshot.reservation?.settlementId ?? submissionSettlementId(submissionId),
        );
      }
      if (snapshot.reservation !== undefined) {
        return yield* completeReservation(ctx, submission, snapshot.reservation, false);
      }
      if (snapshot.abortIntent !== undefined) {
        return yield* settleAborted(ctx, submission, tokenRef, snapshot.abortIntent, evidence);
      }
      yield* applyCanonicalInput(ctx, submission, tokenRef, records, snapshot.inputApplied);
      const currentRecords = yield* readAll(conversationId);
      const outcome = yield* runModel(agent, ctx, submission, tokenRef, currentRecords);
      return yield* terminalize(ctx, submission, tokenRef, outcome, true);
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
        settlements.push(yield* runAttempt(agent, conversationId, claimed.value));
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
      _decision: SettleAbortedDecision,
    ): Effect.fn.Return<"repaired" | "deferred", DurableWorkerFailure> {
      const intent = snapshot.abortIntent;
      if (intent === undefined) return "deferred";
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
      yield* ensureConversationCreated(
        submission.conversationId,
        submission.agentId,
        submission.agentDigests,
      );
      const ctx = yield* attemptContextFor(submission.conversationId, claim.producerEpoch);
      const tokenRef = yield* Ref.make(claim.ownershipToken);
      yield* settleAborted(ctx, submission, tokenRef, intent, evidence);
      return "repaired";
    },
  );

  const executeRecoveryDecision = Effect.fn("DurableAgentRuntime.executeRecoveryDecision")(
    function* (
      snapshot: RecoverySnapshot,
      evidence: RecoveryEvidence,
      decision: RecoveryDecision,
    ): Effect.fn.Return<"repaired" | "deferred" | "none", DurableWorkerFailure> {
      const submission = snapshot.submission;
      switch (decision._tag) {
        case "NoAction": {
          return "none";
        }
        case "ResumeFromTurnBoundary":
        case "MarkUnknown": {
          // Resumption needs the Agent Binding (a worker claim); Unknown needs an authorized
          // resolution path (DUR-017) — both stay visible obligations, never silent drops.
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
          const records = yield* readAll(submission.conversationId);
          yield* applyCanonicalInput(ctx, submission, tokenRef, records, snapshot.inputApplied);
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
          return yield* settleAbortedForRecovery(snapshot, evidence, decision);
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
    const evidence = evidenceFor(read.records, submission.submissionId, read.materialized);
    const decision = classifyRecovery(snapshot, evidence);
    const disposition = yield* executeRecoveryDecision(snapshot, evidence, decision);
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
 *   settled work fails with `SettlementConflict` (DUR-012).
 * - `processConversation(agent, conversationId)` — drain one lane: fenced FIFO-head claims,
 *   canonical input apply, per-Turn canonical commits, terminalization.
 * - `runWorker(agent)` — scan-seeded, wake-driven worker loop over every lane (WP4's host driver).
 * - `runRecovery()` — classify every nonterminal Submission with the pure `classifyRecovery` and
 *   execute the repair decisions, appending a `RepairAnnotated` audit record per executed decision
 *   (DUR-013). Model-resuming work is reported `deferred` for a worker claim.
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
    | Crypto.Crypto
  > = Layer.effect(DurableAgentRuntime)(make);
}

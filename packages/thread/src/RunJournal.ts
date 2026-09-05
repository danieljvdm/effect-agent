import {
  ThreadId,
  RunId,
  ToolCallId,
  TurnId,
  type SubmissionId,
} from "@effect-agent/core/Identifiers";
import { type ExhaustedLimit } from "@effect-agent/core/RunEvent";
import { RunPolicyUsage } from "@effect-agent/core/RunPolicyUsage";
import { ModelCallUsage, summarizeModelUsage } from "@effect-agent/core/Usage";
import {
  CLEARED_TOOL_RESULT,
  COMPACTION_SUMMARY_PREFIX,
  contextWindowId,
  contextWindowMessage,
} from "@effect-agent/engine/Compaction";
import { type Crypto, Effect, Schema, Stream, type DateTime } from "effect";
import { Prompt } from "effect/unstable/ai";

import { digestJson, type DigestError } from "./Digest.ts";
import {
  BatchId,
  CanonicalBatch,
  type CompactionCreated,
  ModelResponseRecorded,
  PersistedJson,
  RecordEnvelope,
  RecordId,
  RunCompleted,
  ToolCallSettled,
  type CanonicalRecordEnvelope,
  type CanonicalSequence,
  type DeploymentId,
  type ProducerId,
} from "./Records.ts";
import { IdempotencyKey } from "./SubmissionLedger.ts";

/** A canonical record could not be projected into Run/Prompt state. */
export class RunJournalError extends Schema.TaggedError<RunJournalError>()("RunJournalError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const journalError = (message: string, cause?: unknown): RunJournalError =>
  cause === undefined
    ? RunJournalError.make({ message })
    : RunJournalError.make({ message, cause });

const decodeRunId = Schema.decodeSync(RunId);
const decodeTurnId = Schema.decodeSync(TurnId);
const decodeBatchId = Schema.decodeSync(BatchId);
const decodeRecordId = Schema.decodeSync(RecordId);

/**
 * Deterministic Run identity pinned per Submission (plan §Coordinator flow). Every Attempt of one
 * Submission shares this Run identity, so canonical per-Turn records from different Attempts
 * belong to one logical Run.
 */
export const runIdForSubmission = (submissionId: SubmissionId): RunId =>
  decodeRunId(`run:${submissionId}`);

/** Stable canonical clock identity shared by every Attempt of one Run. */
export const runStartedRecordId = (runId: RunId): RecordId => decodeRecordId(`run-start:${runId}`);

export const runStartedBatchId = (runId: RunId): BatchId => decodeBatchId(`run-start:${runId}`);

/** Deterministic Turn identity: Attempt-independent for one (Run, canonical turn) pair. */
export const turnIdForRun = (runId: RunId, turn: number): TurnId =>
  decodeTurnId(`turn:${runId}:${turn}`);

/** Deterministic batch identity of one committed canonical no-tool Turn (P4 single-batch shape). */
export const turnBatchId = (runId: RunId, turn: number): BatchId =>
  decodeBatchId(`turn:${runId}:${turn}`);

/**
 * Deterministic batch identity of a tool-declaring Turn's RESPONSE commit (plan §2.1 commit 1):
 * the assistant response plus pending steering becomes canonical BEFORE approval preflight and
 * tool preparation, creating the durability §15 "resume tool scheduling" window.
 */
export const turnResponseBatchId = (runId: RunId, turn: number): BatchId =>
  decodeBatchId(`turn-response:${runId}:${turn}`);

/**
 * Deterministic batch identity of a tool-declaring Turn's RESULTS commit (plan §2.1 commit 5):
 * every `ToolCallSettled` record of the Turn, in declaration order, model-visible atomically.
 */
export const turnResultsBatchId = (runId: RunId, turn: number): BatchId =>
  decodeBatchId(`turn-results:${runId}:${turn}`);

/**
 * Deterministic per-call late-settle batch identity used by the resolution path when one
 * recovered/resolved call settles outside its Turn's results batch. Record identity
 * (`tool-settled:{runId}:{turn}:{toolCallId}`) dedupes double-settles across both paths.
 */
export const toolCallResultBatchId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): BatchId => decodeBatchId(`turn-results:${runId}:${turn}:${toolCallId}`);

/** Deterministic batch identity of one Turn's `ToolCallPrepared` commit (plan §2.1 commit 3). */
export const turnPreparedBatchId = (runId: RunId, turn: number): BatchId =>
  decodeBatchId(`turn-prepared:${runId}:${turn}`);

/**
 * Deterministic identity of one pre-Turn compaction record (RUN-026,
 * RUN-026). Keyed by Run, Turn, and kind — the engine performs at most one
 * threshold compaction per Turn plus at most one overflow-forced summarize,
 * so a superseding Attempt that re-decides the same compaction replays the
 * batch identity instead of duplicating the record.
 */
export const compactionRecordId = (
  runId: RunId,
  turn: number,
  kind: "clear-tool-results" | "summarize" | "rollover",
): RecordId => decodeRecordId(`compaction:${runId}:${turn}:${kind}`);

/** Deterministic batch identity of one compaction append (same string as its record id). */
export const compactionBatchId = (
  runId: RunId,
  turn: number,
  kind: "clear-tool-results" | "summarize" | "rollover",
): BatchId => decodeBatchId(`compaction:${runId}:${turn}:${kind}`);

/** Deterministic canonical record identity of one Turn's `ModelResponseRecorded` record. */
export const modelResponseRecordId = (runId: RunId, turn: number): RecordId =>
  decodeRecordId(`model-response:${runId}:${turn}`);

/** Terminal Tool completion marker committed atomically with its settled Tool result. */
export const runCompletedRecordId = (runId: RunId): RecordId =>
  decodeRecordId(`run-completed:${runId}`);

/** Deterministic canonical record identity of one Turn's `ToolCallSettled` record. */
export const toolCallSettledRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => decodeRecordId(`tool-settled:${runId}:${turn}:${toolCallId}`);

/** Deterministic canonical record identity of one Tool Call's `ToolCallPrepared` record. */
export const toolCallPreparedRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => decodeRecordId(`tool-prepared:${runId}:${turn}:${toolCallId}`);

/** Deterministic batch identity of one Turn's `ToolCallUnknown` marking append. */
export const markUnknownBatchId = (submissionId: SubmissionId, turn: number): BatchId =>
  decodeBatchId(`mark-unknown:${submissionId}:${turn}`);

/** Deterministic canonical record identity of one Tool Call's `ToolCallUnknown` record. */
export const toolCallUnknownRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => decodeRecordId(`tool-unknown:${runId}:${turn}:${toolCallId}`);

/** Deterministic batch identity of one Tool Call's resolution append (DUR-017). */
export const toolCallResolutionBatchId = (
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
): BatchId => decodeBatchId(`resolve:${submissionId}:${toolCallId}`);

/** Deterministic canonical record identity of one Tool Call's `ToolCallResolved` record. */
export const toolCallResolvedRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => decodeRecordId(`tool-resolved:${runId}:${turn}:${toolCallId}`);

/**
 * Deterministic identity of one accepted Durable Step result. The one-record batch reuses the
 * SAME string, so batch idempotency plus the epoch fence realize the durability §11
 * racing-writers rule: only the fenced winner's record commits; the loser replays it.
 */
export const toolStepSettledRecordId = (
  runId: RunId,
  toolCallId: ToolCallId,
  stepName: string,
): RecordId => decodeRecordId(`step:${runId}:${toolCallId}:${stepName}`);

/** Deterministic batch identity of one Durable Step commit (same string as its record id). */
export const toolStepSettledBatchId = (
  runId: RunId,
  toolCallId: ToolCallId,
  stepName: string,
): BatchId => decodeBatchId(`step:${runId}:${toolCallId}:${stepName}`);

/** Deterministic batch identity of one Turn's canonical approval-request append (plan §2.6). */
export const turnApprovalsBatchId = (runId: RunId, turn: number): BatchId =>
  decodeBatchId(`turn-approvals:${runId}:${turn}`);

/** Deterministic canonical record identity of one Tool Call's `ToolApprovalRequested` record. */
export const toolApprovalRequestRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => decodeRecordId(`approval-request:${runId}:${turn}:${toolCallId}`);

/** Deterministic batch identity of one Tool Call's canonical approval-decision append. */
export const approvalDecisionBatchId = (
  submissionId: SubmissionId,
  toolCallId: ToolCallId,
): BatchId => decodeBatchId(`approval-decision:${submissionId}:${toolCallId}`);

/** Deterministic canonical record identity of one Tool Call's `ToolApprovalDecided` record. */
export const toolApprovalDecisionRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => decodeRecordId(`approval-decision:${runId}:${turn}:${toolCallId}`);

/**
 * Deterministic identity of one superseded Attempt's `ModelResponseInterrupted` audit record
 * (durability §9). Keyed by the superseded epoch, so each interrupted ownership period is
 * recorded at most once; the one-record batch reuses the same string.
 */
export const modelResponseInterruptedRecordId = (runId: RunId, supersededEpoch: number): RecordId =>
  decodeRecordId(`interrupted:${runId}:${supersededEpoch}`);

/** Deterministic batch identity of one `ModelResponseInterrupted` append (same string). */
export const modelResponseInterruptedBatchId = (runId: RunId, supersededEpoch: number): BatchId =>
  decodeBatchId(`interrupted:${runId}:${supersededEpoch}`);

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

/**
 * Deterministic canonical record identity of one parent Tool Call's `SubagentRequested` record.
 * The one-record batch reuses the same string, so batch
 * idempotency plus the parent epoch fence make the request append exactly-once-canonical.
 */
export const subagentRequestedRecordId = (runId: RunId, toolCallId: ToolCallId): RecordId =>
  decodeRecordId(`subagent-requested:${runId}:${toolCallId}`);

/** Deterministic batch identity of one `SubagentRequested` append (same string). */
export const subagentRequestedBatchId = (runId: RunId, toolCallId: ToolCallId): BatchId =>
  decodeBatchId(`subagent-requested:${runId}:${toolCallId}`);

/**
 * Deterministic canonical record identity of one parent Tool Call's `SubagentStarted` record.
 * Recovery's start-link repair appends the EXACT same record
 * under this identity, so a raced repair replays instead of duplicating (SUB-016/SUB-017).
 */
export const subagentStartedRecordId = (runId: RunId, toolCallId: ToolCallId): RecordId =>
  decodeRecordId(`subagent-started:${runId}:${toolCallId}`);

/** Deterministic batch identity of one `SubagentStarted` append (same string). */
export const subagentStartedBatchId = (runId: RunId, toolCallId: ToolCallId): BatchId =>
  decodeBatchId(`subagent-started:${runId}:${toolCallId}`);

/**
 * Deterministic batch identity of one parent Tool Call's atomic settlement join: the
 * `SubagentJoined` record plus the parent's `ToolCallSettled` record (its existing
 * `tool-settled:{runId}:{turn}:{toolCallId}` identity) commit as ONE canonical batch (SUB-019).
 */
export const subagentJoinBatchId = (runId: RunId, toolCallId: ToolCallId): BatchId =>
  decodeBatchId(`subagent-join:${runId}:${toolCallId}`);

/** Deterministic canonical record identity of one parent Tool Call's `SubagentJoined` record. */
export const subagentJoinedRecordId = (runId: RunId, toolCallId: ToolCallId): RecordId =>
  decodeRecordId(`subagent-joined:${runId}:${toolCallId}`);

/**
 * Deterministic identity of one child Thread's `SubagentLineageRecorded` record.
 * Its own single-record batch reuses the same string so the generic
 * `thread-created:{cid}` batch identity is never contradicted.
 */
export const subagentLineageRecordId = (threadId: ThreadId): RecordId =>
  decodeRecordId(`subagent-lineage:${threadId}`);

/** Deterministic batch identity of one `SubagentLineageRecorded` append (same string). */
export const subagentLineageBatchId = (threadId: ThreadId): BatchId =>
  decodeBatchId(`subagent-lineage:${threadId}`);

/**
 * Deterministic intended child Thread identity: the
 * parent Submission and Tool Call pair addresses exactly one child Thread, so a replayed
 * establishment converges on the one existing child (SUB-016).
 */
export const childThreadIdFor = (
  parentSubmissionId: SubmissionId,
  toolCallId: ToolCallId,
): ThreadId => decodeThreadId(`subagent:${parentSubmissionId}:${toolCallId}`);

/**
 * Deterministic child admission idempotency key: scoped to
 * the parent Run and Tool Call identity, so duplicate admission attempts resolve through the
 * ledger's idempotency contract to one child Receipt (SUB-016, SUB-031). The ledger key is
 * bounded (256); coordinator-minted Run and Tool Call identities stay far below that bound.
 */
export const childIdempotencyKeyFor = (
  parentRunId: RunId,
  toolCallId: ToolCallId,
): IdempotencyKey => decodeIdempotencyKey(`subagent:${parentRunId}:${toolCallId}`);

const decodePromptMessages = Effect.fn("RunJournal.decodePromptMessages")(
  (messages: PersistedJson): Effect.Effect<Prompt.Prompt, RunJournalError> =>
    Schema.decodeUnknownEffect(Prompt.Prompt)(messages).pipe(
      Effect.mapError((cause) =>
        journalError("Canonical messages are not Schema-encoded Prompt messages", cause),
      ),
    ),
);

interface PendingSettledTool {
  readonly record: ToolCallSettled;
  /** RUN-026: a `clear-tool-results` compaction covers this record's sequence. */
  readonly cleared: boolean;
}

const toolMessageFromSettled = Effect.fn("RunJournal.toolMessageFromSettled")(
  (settled: ReadonlyArray<PendingSettledTool>): Effect.Effect<Prompt.Message, RunJournalError> =>
    Effect.try({
      try: () =>
        Prompt.makeMessage("tool", {
          content: settled.map(({ record, cleared }) =>
            Prompt.makePart("tool-result", {
              id: record.toolCallId,
              name: record.toolName,
              result: cleared ? CLEARED_TOOL_RESULT : record.result,
              isFailure: record.isFailure,
              providerExecuted: false,
            }),
          ),
        }),
      catch: (cause) => journalError("Unable to rebuild Tool message from ToolCallSettled", cause),
    }),
);

/**
 * Pure canonical projection of one Run's durable execution state.
 *
 * Prompt reconstruction contract (D8):
 *
 * - `ModelResponseRecorded.messages` carries the Schema-encoded Prompt messages the Turn appended
 *   to the model-visible prompt — for the Run's FIRST committed Turn that includes the evaluated
 *   instruction and user-input messages, for later Turns only the assistant response messages.
 *   Tool messages are excluded from `messages`.
 * - `ToolCallSettled` records (committed in the same per-Turn batch, in declaration order)
 *   deterministically rebuild the Turn's single Tool message.
 * - `UserInputRecorded` records input, with a Submission identity only for durable admission. Its
 *   Prompt-visible form (instructions + user message) becomes canonical inside the owning Run's
 *   first `ModelResponseRecorded`. The projection consumes it only for Run correlation.
 * - Immediate retained history uses `ModelCompleted.messages` for the successful Run's exact
 *   native Prompt suffix, including any input examples. It has no resumable Turn state.
 */
/** Cumulative committed usage of the projected Run (RUN-023 resume re-seed). */
export interface RunJournalUsage {
  readonly modelCalls: number;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly lastInputTokens: number;
  readonly lastOutputTokens: number;
  /** Cumulative persisted spend of the projected Run's committed calls (RUN-023). */
  readonly costMicrousd: number;
  /** Canonical per-call detail used for settlement aggregation and recovery. */
  readonly modelUsage: ReadonlyArray<ModelCallUsage>;
}

export interface RunJournalProjection {
  readonly policyUsage: RunPolicyUsage;
  /** Canonical projection for the requested Run; may end at its resumable Tool declaration. */
  readonly prompt: Prompt.Prompt;
  /** Valid prior-Run history excluding the projected Run's records and orphan Tool batches. */
  readonly historyBefore: Prompt.Prompt;
  /** Number of canonical Turns already committed for the projected Run. */
  readonly committedTurns: number;
  /** Summed per-call usage of the projected Run's committed responses; zeros for records predating usage capture. */
  readonly usage: RunJournalUsage;
  /** Latest committed context window identity, retained across ownership changes. */
  readonly contextWindowId?: string | undefined;
  /** Canonical evaluated instructions and initial input for this Run, independent of the view. */
  readonly protectedContext?: Prompt.Prompt | undefined;
  /** Last successful singleton Tool awaiting possible context-control interpretation by the engine. */
  readonly pendingContextToolCallId?: string | undefined;
}

interface ProjectedResponseUsage {
  readonly modelCalls: number;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly costMicrousd: number;
  readonly modelUsage: ReadonlyArray<ModelCallUsage>;
}

const projectedResponseUsage = (
  response: ModelResponseRecorded,
): Effect.Effect<ProjectedResponseUsage, RunJournalError> =>
  Effect.gen(function* () {
    const calls = response.modelUsage;

    if (calls === undefined) {
      return {
        modelCalls: 1,
        inputTokens: response.inputTokens ?? 0,
        outputTokens: response.outputTokens ?? 0,
        costMicrousd: response.costMicrousd ?? 0,
        modelUsage: [],
      };
    }
    if (calls.length === 0) {
      return yield* journalError("A canonical modelUsage list must not be empty");
    }

    const summary = yield* summarizeModelUsage(calls).pipe(
      Effect.mapError((cause) =>
        journalError("Canonical model usage exceeds accounting bounds", cause),
      ),
    );

    if (
      (response.inputTokens !== undefined && response.inputTokens !== summary.inputTokens.total) ||
      (response.outputTokens !== undefined &&
        response.outputTokens !== summary.outputTokens.total) ||
      (response.costMicrousd !== undefined && response.costMicrousd !== summary.costMicrousd)
    ) {
      return yield* journalError("Canonical detailed and aggregate model usage disagree");
    }

    return {
      modelCalls: summary.modelCalls,
      inputTokens: summary.inputTokens.total,
      outputTokens: summary.outputTokens.total,
      costMicrousd: summary.costMicrousd,
      modelUsage: calls,
    };
  });

const addProjectedUsage = (
  field: string,
  left: number,
  right: number,
): Effect.Effect<number, RunJournalError> =>
  Schema.decodeUnknownEffect(Schema.Natural)(left + right).pipe(
    Effect.mapError((cause) =>
      journalError(`Canonical projected usage exceeds safe-integer bounds at ${field}`, cause),
    ),
  );

interface FoldState {
  readonly all: Array<Prompt.Message>;
  readonly before: Array<Prompt.Message>;
  readonly pendingTools: Array<PendingSettledTool>;
  readonly pendingToolsForRun: boolean;
  readonly committedTurns: number;
}

const decodeToolCallId = Schema.decodeSync(ToolCallId);

const declaredApplicationToolCallIds = (prompt: Prompt.Prompt): ReadonlyArray<string> => {
  const ids: Array<string> = [];

  for (const message of prompt.content) {
    if (message.role !== "assistant") continue;
    for (const part of message.content) {
      if (part.type === "tool-call" && !part.providerExecuted) ids.push(part.id);
    }
  }

  return ids;
};

const withoutApplicationToolCallMessages = (prompt: Prompt.Prompt): ReadonlyArray<Prompt.Message> =>
  prompt.content.filter(
    (message) =>
      message.role !== "assistant" ||
      !message.content.some((part) => part.type === "tool-call" && !part.providerExecuted),
  );

/**
 * Phase 5 audit tags that are prompt-transparent: they carry durability evidence (preparation,
 * unknown marking, resolution, Step results, approvals, interruption) but contribute nothing to
 * the model-visible Prompt, and — unlike the P4 tags — they do NOT split a contiguous
 * `ToolCallSettled` group into separate Tool messages, so a late-settled call's audit records
 * cannot change the replayed prompt shape.
 *
 * The four S2 Subagent lifecycle tags are prompt-transparent for the same reason (spec §5/§11):
 * the parent prompt never carries child transcript or establishment evidence — the joined child
 * result reaches the model exclusively through the paired `ToolCallSettled` record, and the
 * child-log lineage record is not model input.
 */
const PROMPT_TRANSPARENT_TAGS: ReadonlySet<string> = new Set([
  "RunPolicyUsageReserved",
  "RunStarted",
  "ToolCallPrepared",
  "ToolCallUnknown",
  "ToolCallResolved",
  "ToolStepSettled",
  "ToolApprovalRequested",
  "ToolApprovalDecided",
  "ModelResponseInterrupted",
  "SubagentRequested",
  "SubagentStarted",
  "SubagentJoined",
  "SubagentLineageRecorded",
]);

/**
 * Pure projection: rebuild one Run's resume state from canonical records (DUR-015). Canonical
 * order is authoritative; the fold projects each complete `ModelResponseRecorded` Turn and the
 * owning Run's resumable incomplete Tool Turn, while excluding incomplete Tool batches from prior
 * Runs. It flushes each contiguous group of valid `ToolCallSettled` records into one Tool message,
 * exactly mirroring the per-Turn commit shape produced by `turnCanonicalBatch` (no-tool Turns) and
 * by the
 * `turnResponseBatch`/`turnResultsBatch` split (tool-declaring Turns). The Phase 5 audit tags
 * are skipped transparently, so split-batch commits replay to the same prompt as P4 single-batch
 * commits.
 *
 * An incomplete application Tool turn remains visible while projecting its owning Run so active
 * recovery can resume the declared batch. It is not a valid model-visible Turn boundary for a
 * later Run: the orphan assistant Tool declaration and any partial Tool results from that Turn
 * are excluded, while preceding instruction/user messages in the response record remain history.
 */
/** @internal Lightweight canonical boundaries collected without retaining record payloads. */
export interface JournalBoundary {
  readonly sequence: CanonicalSequence;
  readonly tag: "ModelResponseRecorded" | "ToolCallSettled";
  readonly promptLength: number;
  /** A canonical declaration without all of its settled results cannot be covered by rollover. */
  readonly incomplete?: true | undefined;
}

/**
 * Reconstruct a fixed canonical prefix from a re-readable stream. The caller must keep the same
 * records visible on every traversal. The first collects compaction and settlement metadata;
 * rollovers additionally validate covered Tool batches before rebuilding Prompt and usage. Metadata and the
 * live Prompt remain resident; covered historical message/tool payloads do not. An uncompacted Prompt still grows
 * with its conversation, so hosts must configure an appropriate context compaction policy.
 * @internal
 */
export const projectRunJournalStream = Effect.fn("RunJournal.projectRunJournalStream")(function* <
  E,
  R,
>(
  records: Stream.Stream<CanonicalRecordEnvelope, E, R>,
  ownerRunId: RunId | undefined,
  onBoundary?: (boundary: JournalBoundary) => void,
): Effect.fn.Return<RunJournalProjection, RunJournalError | E, R> {
  let state: FoldState = {
    all: [],
    before: [],
    pendingTools: [],
    pendingToolsForRun: false,
    committedTurns: 0,
  };

  // RUN-026 pre-scan: the widest VALID compaction bounds govern the fold. A
  // valid record covers strictly below its own sequence and never splits a response from its
  // settled tool results. Only rollovers may cover their owner Run's records;
  // a summarize record must carry its summary. Invalid records
  // are ignored fail-safe — the full history stays authoritative. Ties on
  // coversThrough resolve to the record appended later (higher sequence),
  // matching at-most-once replay intent.
  // One span per settled record, paired with its declaring response the same
  // way the fold pairs them: a settled belongs to the most recent
  // ModelResponseRecorded of its Run. A bound inside (response, settled)
  // would orphan the tool message from its declaring response. Orphaned
  // settleds (filtered later by the fold) still contribute spans —
  // over-invalidating is the fail-safe direction.
  const firstSequenceByRun = new Map<string, number>();
  const lastResponseSequenceByRun = new Map<string, number>();
  const settledSpans: Array<{ readonly from: number; readonly to: number }> = [];
  const settledToolCallRecordIds = new Set<string>();
  const settledById = new Map<string, Pick<ToolCallSettled, "isFailure" | "budgetRejected">>();
  const compactions: Array<{ readonly payload: CompactionCreated; readonly sequence: number }> = [];

  yield* Stream.runForEach(records, (envelope) =>
    Effect.sync(() => {
      const payload = envelope.record.payload;

      if (payload._tag === "CompactionCreated") {
        compactions.push({ payload, sequence: envelope.sequence });

        return;
      }
      if ("runId" in payload && typeof payload.runId === "string") {
        if (!firstSequenceByRun.has(payload.runId)) {
          firstSequenceByRun.set(payload.runId, envelope.sequence);
        }
      }
      if (payload._tag === "ModelResponseRecorded") {
        lastResponseSequenceByRun.set(payload.runId, envelope.sequence);
      } else if (payload._tag === "ToolCallSettled") {
        settledToolCallRecordIds.add(envelope.record.recordId);
        if (payload.runId === ownerRunId)
          settledById.set(envelope.record.recordId, {
            isFailure: payload.isFailure,
            ...(payload.budgetRejected === undefined
              ? {}
              : { budgetRejected: payload.budgetRejected }),
          });
        const from = lastResponseSequenceByRun.get(payload.runId);

        if (from !== undefined && from < envelope.sequence) {
          settledSpans.push({ from, to: envelope.sequence });
        }
      }
    }),
  );

  const rolloverCoverage = compactions.reduce(
    (through, { payload }) =>
      payload.kind === "rollover" ? Math.max(through, payload.coversThrough) : through,
    0,
  );

  const incompleteResponseSequences: Array<number> = [];
  let ownerPrefixSequence = Number.POSITIVE_INFINITY;
  let protectedContext: Prompt.Prompt | undefined;

  if (rolloverCoverage > 0) {
    yield* Stream.runForEach(records, (envelope) =>
      Effect.gen(function* () {
        const payload = envelope.record.payload;

        if (payload._tag !== "ModelResponseRecorded" || envelope.sequence > rolloverCoverage)
          return;
        const messages = yield* decodePromptMessages(payload.messages);
        const declared = declaredApplicationToolCallIds(messages);

        for (const id of declared) {
          const callId = yield* Schema.decodeUnknownEffect(ToolCallId)(id).pipe(
            Effect.mapError((cause) =>
              journalError("Failed to decode a declared Tool Call ID", cause),
            ),
          );

          if (
            !settledToolCallRecordIds.has(
              toolCallSettledRecordId(payload.runId, payload.turn, callId),
            )
          ) {
            incompleteResponseSequences.push(envelope.sequence);
            break;
          }
        }
        if (
          payload.runId === ownerRunId &&
          payload.turn === 1 &&
          payload.runScopedPrefixLength !== undefined
        ) {
          ownerPrefixSequence = envelope.sequence;
          protectedContext = Prompt.fromMessages(
            messages.content.slice(0, payload.runScopedPrefixLength),
          );
        }
      }),
    );
  }

  const boundIsValid = (payload: CompactionCreated, ownSequence: number): boolean => {
    const { runId, coversThrough } = payload;

    if (coversThrough <= 0 || coversThrough >= ownSequence) return false;
    const ownerFirst = firstSequenceByRun.get(runId);

    if (payload.kind !== "rollover" && ownerFirst !== undefined && coversThrough >= ownerFirst)
      return false;
    if (
      payload.kind === "rollover" &&
      incompleteResponseSequences.some((sequence) => sequence <= coversThrough)
    )
      return false;
    for (const span of settledSpans) {
      if (span.from <= coversThrough && coversThrough < span.to) return false;
    }

    return true;
  };

  let summarizeBound = 0;
  let replacement: CompactionCreated | undefined;
  let summarizeSequence = -1;
  let clearBound = 0;
  let latestWindowId: string | undefined;
  let latestWindowSequence = -1;
  let rolloverCoveredThrough = 0;

  for (const { payload, sequence } of compactions) {
    if (!boundIsValid(payload, sequence)) continue;
    if (payload.kind === "rollover" && sequence > latestWindowSequence) {
      latestWindowId = contextWindowId(payload.runId, payload.turn);
      latestWindowSequence = sequence;
    }
    if (payload.kind === "rollover")
      rolloverCoveredThrough = Math.max(rolloverCoveredThrough, payload.coversThrough);
    if (payload.kind === "summarize" || payload.kind === "rollover") {
      if (payload.kind === "summarize" && payload.summary === undefined) continue;
      if (
        payload.coversThrough > summarizeBound ||
        (payload.coversThrough === summarizeBound && sequence > summarizeSequence)
      ) {
        summarizeBound = payload.coversThrough;
        summarizeSequence = sequence;
        replacement = payload;
      }
    } else if (payload.coversThrough > clearBound) {
      clearBound = payload.coversThrough;
    }
  }
  let summaryEmitted = false;

  const retainedPrefix =
    replacement?.kind === "rollover" &&
    replacement.runId === ownerRunId &&
    ownerPrefixSequence <= summarizeBound
      ? (protectedContext?.content ?? [])
      : [];

  const replacementLength = replacement === undefined ? 0 : retainedPrefix.length + 1;

  const emitSummary = () => {
    if (summaryEmitted || replacement === undefined) return;
    summaryEmitted = true;

    const message =
      replacement.kind === "rollover"
        ? contextWindowMessage(
            contextWindowId(replacement.runId, replacement.turn),
            replacement.handoff,
          )
        : Prompt.makeMessage("user", {
            content: [
              Prompt.makePart("text", {
                text: `${COMPACTION_SUMMARY_PREFIX}${replacement.summary}`,
              }),
            ],
          });

    state.all.push(...retainedPrefix, message);
    if (replacement.kind !== "rollover" || replacement.runId !== ownerRunId)
      state.before.push(message);
  };

  const modelUsage: Array<ModelCallUsage> = [];

  const usage = {
    modelCalls: 0,
    inputTokens: 0,
    outputTokens: 0,
    lastInputTokens: 0,
    lastOutputTokens: 0,
    costMicrousd: 0,
    modelUsage,
  };

  let usageTurn = 0;

  const incompleteToolTurns = new Set<string>();
  const incompleteToolCalls = new Set<string>();
  let pendingContextToolCallId: string | undefined;
  let ownerTerminated = false;

  const policyUsage = {
    committedTurns: 0,
    toolCalls: 0,
    programmaticToolCalls: 0,
    consecutiveToolFailures: 0,
    finalizationUsed: false,
  };

  const accountResponse = Effect.fn("RunJournal.accountResponse")(function* (
    envelope: CanonicalRecordEnvelope,
    payload: ModelResponseRecorded,
    messages: Prompt.Prompt,
  ) {
    const record = envelope.record;

    const declared = declaredApplicationToolCallIds(messages);
    const declaredRecordIds: Array<RecordId> = [];

    for (const id of declared) {
      const toolCallId = yield* Effect.try({
        try: () => decodeToolCallId(id),
        catch: (cause) => journalError("Failed to decode a declared Tool Call ID", cause),
      });

      declaredRecordIds.push(toolCallSettledRecordId(payload.runId, payload.turn, toolCallId));
    }
    if (declaredRecordIds.some((recordId) => !settledToolCallRecordIds.has(recordId))) {
      incompleteToolTurns.add(envelope.record.recordId);
      for (const recordId of declaredRecordIds) incompleteToolCalls.add(recordId);
    }
    if (payload.runId !== ownerRunId) return;
    if (payload.turn === 1 && payload.runScopedPrefixLength !== undefined) {
      protectedContext = Prompt.fromMessages(
        messages.content.slice(0, payload.runScopedPrefixLength),
      );
    }

    const responseUsage = yield* projectedResponseUsage(payload);

    usage.modelCalls = yield* addProjectedUsage(
      "modelCalls",
      usage.modelCalls,
      responseUsage.modelCalls,
    );
    usage.inputTokens = yield* addProjectedUsage(
      "inputTokens",
      usage.inputTokens,
      responseUsage.inputTokens,
    );
    usage.outputTokens = yield* addProjectedUsage(
      "outputTokens",
      usage.outputTokens,
      responseUsage.outputTokens,
    );
    usage.costMicrousd = yield* addProjectedUsage(
      "costMicrousd",
      usage.costMicrousd,
      responseUsage.costMicrousd,
    );
    usage.modelUsage.push(...responseUsage.modelUsage);
    if (payload.turn > usageTurn) {
      usageTurn = payload.turn;
      usage.lastInputTokens = responseUsage.inputTokens;
      usage.lastOutputTokens = responseUsage.outputTokens;
    }

    policyUsage.committedTurns = Math.max(policyUsage.committedTurns, payload.turn);

    const calls = messages.content.flatMap((message) =>
      message.role === "assistant"
        ? message.content.filter((part) => part.type === "tool-call")
        : [],
    );

    const candidate = calls.length === 1 ? calls[0] : undefined;

    const candidateResult =
      candidate === undefined
        ? undefined
        : settledById.get(`tool-settled:${ownerRunId}:${payload.turn}:${candidate.id}`);

    pendingContextToolCallId =
      candidate !== undefined &&
      !candidate.providerExecuted &&
      candidateResult?.isFailure === false &&
      candidateResult.budgetRejected !== true &&
      envelope.sequence > rolloverCoveredThrough
        ? candidate.id
        : undefined;

    policyUsage.toolCalls += calls.length;
    if (incompleteToolTurns.has(record.recordId)) return;
    for (const call of calls) {
      const result = call.providerExecuted
        ? messages.content
            .flatMap((message) => (message.role === "assistant" ? message.content : []))
            .find(
              (part) => part.type === "tool-result" && part.providerExecuted && part.id === call.id,
            )
        : settledById.get(`tool-settled:${ownerRunId}:${payload.turn}:${call.id}`);

      if (result === undefined || ("budgetRejected" in result && result.budgetRejected === true))
        continue;
      if (!("isFailure" in result)) continue;
      policyUsage.consecutiveToolFailures = result.isFailure
        ? policyUsage.consecutiveToolFailures + 1
        : 0;
    }
  });

  const flushTools = Effect.fn("RunJournal.flushTools")(function* (
    current: FoldState,
  ): Effect.fn.Return<FoldState, RunJournalError> {
    if (current.pendingTools.length === 0) return current;
    const toolMessage = yield* toolMessageFromSettled(current.pendingTools);

    current.all.push(toolMessage);
    if (!current.pendingToolsForRun) current.before.push(toolMessage);

    return {
      ...current,
      pendingTools: [],
      pendingToolsForRun: false,
    };
  });

  yield* Stream.runForEach(records, (envelope) =>
    Effect.gen(function* () {
      const payload = envelope.record.payload;

      if (
        ((payload._tag === "RunCompleted" || payload._tag === "RunFailed") &&
          payload.runId === ownerRunId) ||
        (payload._tag === "SubmissionSettled" &&
          runIdForSubmission(payload.submissionId) === ownerRunId)
      )
        ownerTerminated = true;

      if (payload._tag === "RunPolicyUsageReserved" && payload.runId === ownerRunId) {
        if (
          payload.programmaticToolCalls < policyUsage.programmaticToolCalls ||
          (policyUsage.finalizationUsed && !payload.finalizationUsed)
        ) {
          return yield* journalError("Run policy reservations must be monotonic");
        }
        policyUsage.programmaticToolCalls = payload.programmaticToolCalls;
        policyUsage.finalizationUsed = payload.finalizationUsed;
      }
      if (PROMPT_TRANSPARENT_TAGS.has(payload._tag)) return;
      // The compaction record governs the fold (pre-scan) and contributes no
      // message of its own; records at or below the summarize bound render as
      // the one summary message emitted at the covered/kept transition.
      if (payload._tag === "CompactionCreated") return;
      if (envelope.sequence <= summarizeBound) {
        // Projecting an earlier Run after a later summary still accounts for its covered responses.
        if (payload._tag === "ModelResponseRecorded" && payload.runId === ownerRunId) {
          const messages = yield* decodePromptMessages(payload.messages);

          yield* accountResponse(envelope, payload, messages);
          state = { ...state, committedTurns: Math.max(state.committedTurns, payload.turn) };
        }
        if (payload._tag === "ModelResponseRecorded" || payload._tag === "ToolCallSettled") {
          onBoundary?.({
            sequence: envelope.sequence,
            tag: payload._tag,
            promptLength: replacementLength,
            ...(incompleteToolTurns.has(envelope.record.recordId) ||
            incompleteToolCalls.has(envelope.record.recordId)
              ? { incomplete: true }
              : {}),
          });
        }

        return;
      }
      emitSummary();
      if (payload._tag === "ToolCallSettled") {
        if (payload.runId !== ownerRunId && incompleteToolCalls.has(envelope.record.recordId)) {
          onBoundary?.({
            sequence: envelope.sequence,
            tag: payload._tag,
            promptLength: state.all.length + (state.pendingTools.length === 0 ? 0 : 1),
            incomplete: true,
          });

          return;
        }
        if (
          state.pendingTools.length > 0 &&
          state.pendingToolsForRun !== (payload.runId === ownerRunId)
        ) {
          state = yield* flushTools(state);
        }
        state.pendingTools.push({ record: payload, cleared: envelope.sequence <= clearBound });
        state = {
          ...state,
          pendingToolsForRun: payload.runId === ownerRunId,
        };
        onBoundary?.({
          sequence: envelope.sequence,
          tag: payload._tag,
          promptLength: state.all.length + 1,
          ...(incompleteToolCalls.has(envelope.record.recordId) ? { incomplete: true } : {}),
        });

        return;
      }
      state = yield* flushTools(state);
      if (payload._tag === "ModelCompleted" && payload.messages !== undefined) {
        const messages = yield* decodePromptMessages(payload.messages);

        for (const message of messages.content) {
          state.all.push(message);
          if (payload.runId !== ownerRunId) state.before.push(message);
        }

        return;
      }
      if (payload._tag !== "ModelResponseRecorded") return;
      const messages = yield* decodePromptMessages(payload.messages);
      const forRun = payload.runId === ownerRunId;

      yield* accountResponse(envelope, payload, messages);

      const modelVisible =
        !forRun && payload.runScopedPrefixLength !== undefined
          ? Prompt.fromMessages(messages.content.slice(payload.runScopedPrefixLength))
          : messages;

      const visibleMessages =
        !forRun && incompleteToolTurns.has(envelope.record.recordId)
          ? withoutApplicationToolCallMessages(modelVisible)
          : modelVisible.content;

      for (const message of visibleMessages) {
        state.all.push(message);
        if (!forRun) state.before.push(message);
      }
      state = {
        ...state,
        committedTurns: forRun
          ? Math.max(state.committedTurns, payload.turn)
          : state.committedTurns,
      };
      onBoundary?.({
        sequence: envelope.sequence,
        tag: payload._tag,
        promptLength: state.all.length,
        ...(incompleteToolTurns.has(envelope.record.recordId) ? { incomplete: true } : {}),
      });
    }),
  );
  emitSummary();
  state = yield* flushTools(state);

  const validatedPolicyUsage = yield* Schema.decodeUnknownEffect(RunPolicyUsage)(policyUsage).pipe(
    Effect.mapError((cause) =>
      journalError("Run policy accounting exceeds its Schema bounds", cause),
    ),
  );

  return {
    policyUsage: validatedPolicyUsage,
    prompt: Prompt.fromMessages(state.all),
    historyBefore: Prompt.fromMessages(state.before),
    committedTurns: state.committedTurns,
    usage,
    ...(latestWindowId === undefined ? {} : { contextWindowId: latestWindowId }),
    ...(protectedContext === undefined ? {} : { protectedContext }),
    ...(ownerTerminated || pendingContextToolCallId === undefined
      ? {}
      : { pendingContextToolCallId }),
  };
});

/** Pure projection of one Run's durable recovery state from canonical records. */
export const projectRunJournal = Effect.fn("RunJournal.projectRunJournal")(
  (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
    runId: RunId,
  ): Effect.Effect<RunJournalProjection, RunJournalError> =>
    projectRunJournalStream(Stream.fromIterable(records), runId),
);

/**
 * Pure valid-prompt projection from canonical records: `UserInputRecorded` +
 * `ModelResponseRecorded` + complete `ToolCallSettled` batches → the deterministic model-visible
 * Prompt (plan §Coordinator flow step 3).
 */
export const promptFromCanonicalRecords = Effect.fn("RunJournal.promptFromCanonicalRecords")(
  (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
  ): Effect.Effect<Prompt.Prompt, RunJournalError> =>
    projectRunJournalStream(Stream.fromIterable(records), undefined).pipe(
      Effect.map((projection) => projection.prompt),
    ),
);

/** Everything one committed Turn contributes to its canonical batch. */
/**
 * Staged usage is validated, never repaired: clamping negatives or truncating
 * fractions would under-record canonical usage, and NaN/Infinity must fail
 * typed instead of escaping as a record-construction defect (RUN-023).
 */
const validStagedUsage = (label: string, value: number): Effect.Effect<number, RunJournalError> =>
  Number.isSafeInteger(value) && value >= 0
    ? Effect.succeed(value)
    : Effect.fail(
        journalError(`Staged ${label} must be a non-negative safe integer, got ${String(value)}`),
      );

export interface TurnCommitInput {
  readonly budgetRejectedCalls?: ReadonlySet<string>;
  readonly runId: RunId;
  /** Canonical (Run-relative, Attempt-independent) Turn number; must be positive. */
  readonly turn: number;
  readonly turnId: TurnId;
  /**
   * The Prompt messages this Turn appended to official history, in order, including the Turn's
   * Tool message when application Tools ran. Tool messages become `ToolCallSettled` records; the
   * remaining messages become the Turn's `ModelResponseRecorded.messages`.
   */
  readonly appended: ReadonlyArray<Prompt.Message>;
  readonly producerId: ProducerId;
  readonly deploymentId: DeploymentId;
  readonly createdAt: DateTime.Utc;
  /** Leading instruction/wake messages that stay canonical but are hidden from later Runs. */
  readonly runScopedPrefixLength?: number | undefined;
  /** Terminal output committed atomically with this Turn's final canonical batch. */
  readonly runCompletion?:
    | {
        readonly output: PersistedJson;
        readonly runDisposition?: PersistedJson | undefined;
        readonly finishReason?: "budget-exhausted" | undefined;
        readonly exhausted?: ExhaustedLimit | undefined;
      }
    | undefined;
  /** Per-call provider usage staged by the engine's `noteTurnUsage` (RUN-023). */
  readonly usage?:
    | {
        readonly inputTokens: number;
        readonly outputTokens: number;
        readonly costMicrousd?: number | undefined;
        readonly modelUsage?: ReadonlyArray<ModelCallUsage> | undefined;
      }
    | undefined;
}

const decodePersistedJson = Schema.decodeUnknownEffect(PersistedJson);
const encodePrompt = Schema.encodeEffect(Prompt.Prompt);
const decodeModelUsage = Schema.decodeUnknownEffect(Schema.Array(ModelCallUsage));

const requireCanonicalTurn = (turn: number): Effect.Effect<void, RunJournalError> =>
  !Number.isInteger(turn) || turn <= 0
    ? Effect.fail(journalError(`Canonical turn number must be a positive integer: ${turn}`))
    : Effect.void;

interface SplitTurnMessages {
  readonly promptMessages: ReadonlyArray<Prompt.Message>;
  readonly toolParts: ReadonlyArray<Prompt.ToolResultPart>;
}

const splitTurnMessages = (appended: ReadonlyArray<Prompt.Message>): SplitTurnMessages => {
  const promptMessages: Array<Prompt.Message> = [];
  const toolParts: Array<Prompt.ToolResultPart> = [];

  for (const message of appended) {
    if (message.role !== "tool") {
      promptMessages.push(message);
      continue;
    }
    for (const part of message.content) {
      if (part.type !== "tool-result") continue;
      toolParts.push(part);
    }
  }

  return { promptMessages, toolParts };
};

const modelResponseRecord = Effect.fn("RunJournal.modelResponseRecord")(function* (
  input: TurnCommitInput,
  promptMessages: ReadonlyArray<Prompt.Message>,
): Effect.fn.Return<RecordEnvelope, RunJournalError | DigestError, Crypto.Crypto> {
  if (promptMessages.length === 0) {
    return yield* journalError(`Turn ${input.turn} appended no model-visible Prompt messages`);
  }
  const runScopedPrefixLength = input.runScopedPrefixLength;

  if (
    runScopedPrefixLength !== undefined &&
    (input.turn !== 1 ||
      !Number.isSafeInteger(runScopedPrefixLength) ||
      runScopedPrefixLength <= 0 ||
      runScopedPrefixLength >= promptMessages.length ||
      promptMessages
        .slice(0, runScopedPrefixLength)
        .some((message) => message.role !== "system" && message.role !== "user"))
  ) {
    return yield* journalError(
      "Run-scoped Prompt provenance must identify a non-empty system/user prefix of Turn 1 before its assistant response",
    );
  }

  const encodedMessages = yield* encodePrompt(Prompt.fromMessages([...promptMessages])).pipe(
    Effect.mapError((cause) => journalError("Turn Prompt messages failed to encode", cause)),
  );

  const messages = yield* decodePersistedJson(encodedMessages).pipe(
    Effect.mapError((cause) =>
      journalError("Turn Prompt messages exceed canonical persistence bounds", cause),
    ),
  );

  const messagesDigest = yield* digestJson(messages);

  const modelUsage =
    input.usage?.modelUsage === undefined
      ? undefined
      : yield* decodeModelUsage(input.usage.modelUsage).pipe(
          Effect.mapError((cause) => journalError("Turn model usage failed to decode", cause)),
        );

  if (modelUsage !== undefined) {
    if (modelUsage.length === 0) {
      return yield* journalError("Turn model usage must contain at least one completed call");
    }

    const summary = yield* summarizeModelUsage(modelUsage).pipe(
      Effect.mapError((cause) => journalError("Turn model usage exceeds accounting bounds", cause)),
    );

    if (
      input.usage === undefined ||
      input.usage.inputTokens !== summary.inputTokens.total ||
      input.usage.outputTokens !== summary.outputTokens.total ||
      (input.usage.costMicrousd ?? 0) !== summary.costMicrousd
    ) {
      return yield* journalError("Turn detailed and aggregate model usage disagree");
    }
  }

  return RecordEnvelope.make({
    recordId: modelResponseRecordId(input.runId, input.turn),
    family: "thread",
    schemaVersion: 1,
    createdAt: input.createdAt,
    deploymentId: input.deploymentId,
    payload: ModelResponseRecorded.make({
      runId: input.runId,
      turnId: input.turnId,
      turn: input.turn,
      messages,
      messagesDigest,
      ...(runScopedPrefixLength === undefined ? {} : { runScopedPrefixLength }),
      ...(modelUsage === undefined ? {} : { modelUsage }),
      ...(input.usage === undefined
        ? {}
        : {
            inputTokens: yield* validStagedUsage("inputTokens", input.usage.inputTokens),
            outputTokens: yield* validStagedUsage("outputTokens", input.usage.outputTokens),
            // Written only when non-zero: absent re-seeds as zero, so the
            // no-estimator case stays byte-identical to pre-cost histories.
            ...(input.usage.costMicrousd === undefined || input.usage.costMicrousd === 0
              ? {}
              : {
                  costMicrousd: yield* validStagedUsage("costMicrousd", input.usage.costMicrousd),
                }),
          }),
    }),
  });
});

const toolSettledRecords = Effect.fn("RunJournal.toolSettledRecords")(function* (
  input: TurnCommitInput,
  toolParts: ReadonlyArray<Prompt.ToolResultPart>,
): Effect.fn.Return<Array<RecordEnvelope>, RunJournalError> {
  const toolRecords: Array<RecordEnvelope> = [];

  for (const part of toolParts) {
    const result = yield* decodePersistedJson(part.result).pipe(
      Effect.mapError((cause) =>
        journalError(`Tool result ${part.id} exceeds canonical persistence bounds`, cause),
      ),
    );

    const toolCallId = yield* Effect.try({
      try: () => decodeToolCallId(part.id),
      catch: (cause) => journalError(`Invalid Tool Call ID ${part.id}`, cause),
    });

    toolRecords.push(
      RecordEnvelope.make({
        recordId: toolCallSettledRecordId(input.runId, input.turn, toolCallId),
        family: "thread",
        schemaVersion: 1,
        createdAt: input.createdAt,
        deploymentId: input.deploymentId,
        payload: ToolCallSettled.make({
          runId: input.runId,
          toolCallId,
          toolName: part.name,
          result,
          isFailure: part.isFailure,
          ...(input.budgetRejectedCalls?.has(part.id) === true ? { budgetRejected: true } : {}),
        }),
      }),
    );
  }

  return toolRecords;
});

const runCompletionRecord = (input: TurnCommitInput): RecordEnvelope | undefined =>
  input.runCompletion === undefined
    ? undefined
    : RecordEnvelope.make({
        recordId: runCompletedRecordId(input.runId),
        family: "thread",
        schemaVersion: 1,
        createdAt: input.createdAt,
        deploymentId: input.deploymentId,
        payload: RunCompleted.make({
          runId: input.runId,
          output: input.runCompletion.output,
          ...(input.runCompletion.runDisposition === undefined
            ? {}
            : { runDisposition: input.runCompletion.runDisposition }),
          ...(input.runCompletion.finishReason === undefined
            ? {}
            : { finishReason: input.runCompletion.finishReason }),
          ...(input.runCompletion.exhausted === undefined
            ? {}
            : { exhausted: input.runCompletion.exhausted }),
        }),
      });

/**
 * Pure per-Turn canonical batch builder (TurnCompleted seam fold, D6/D8): one
 * `ModelResponseRecorded` record plus one `ToolCallSettled` record per terminal Tool result, all
 * under the WP0-style deterministic identities, committed as ONE atomic batch. The same input
 * always yields byte-identical content, so an in-Attempt append retry is an honest batch replay.
 *
 * Phase 5 keeps this shape for Turns that declare no application Tool calls; their terminal
 * `RunCompleted` marker joins the response in this same atomic batch. Tool-declaring Turns split
 * into `turnResponseBatch` + `turnResultsBatch`.
 */
export const turnCanonicalBatch = Effect.fn("RunJournal.turnCanonicalBatch")(function* (
  input: TurnCommitInput,
): Effect.fn.Return<CanonicalBatch, RunJournalError | DigestError, Crypto.Crypto> {
  yield* requireCanonicalTurn(input.turn);
  const { promptMessages, toolParts } = splitTurnMessages(input.appended);
  const modelResponse = yield* modelResponseRecord(input, promptMessages);
  const toolRecords = yield* toolSettledRecords(input, toolParts);
  const completionRecord = runCompletionRecord(input);

  return CanonicalBatch.make({
    batchId: turnBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records:
      completionRecord === undefined
        ? [modelResponse, ...toolRecords]
        : [modelResponse, ...toolRecords, completionRecord],
  });
});

/**
 * Commit 1 of a tool-declaring Turn: the Turn's `ModelResponseRecorded` record, including
 * provider results retained in assistant content before application Tools execute.
 * — pending steering plus the assistant response with its declared tool calls — under batch
 * identity `turn-response:{runId}:{turn}`. Committing the response before preparation creates
 * the provably-safe durability §15 window ("after model item commit, before tool preparation →
 * resume tool scheduling"): a crash there resumes the declared batch with no model re-invocation
 * and no Unknown. Tool messages in `appended` are ignored; the record identity is the same
 * `model-response:{runId}:{turn}` as the single-batch shape, so record-id assertions and the
 * prompt projection are unchanged.
 */
export const turnResponseBatch = Effect.fn("RunJournal.turnResponseBatch")(function* (
  input: TurnCommitInput,
): Effect.fn.Return<CanonicalBatch, RunJournalError | DigestError, Crypto.Crypto> {
  yield* requireCanonicalTurn(input.turn);
  const { promptMessages } = splitTurnMessages(input.appended);
  const modelResponse = yield* modelResponseRecord(input, promptMessages);

  return CanonicalBatch.make({
    batchId: turnResponseBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records: [modelResponse],
  });
});

/**
 * Commit 5 of a tool-declaring Turn (plan §2.1): the Turn's `ToolCallSettled` records in
 * declaration order under batch identity `turn-results:{runId}:{turn}` — the batch becomes
 * model-visible atomically. Non-tool messages in `appended` are ignored; a Turn without any
 * terminal Tool result has no results batch and fails typed.
 */
export const turnResultsBatch = Effect.fn("RunJournal.turnResultsBatch")(function* (
  input: TurnCommitInput,
): Effect.fn.Return<CanonicalBatch, RunJournalError> {
  yield* requireCanonicalTurn(input.turn);
  const { toolParts } = splitTurnMessages(input.appended);
  const toolRecords = yield* toolSettledRecords(input, toolParts);
  const first = toolRecords[0];

  if (first === undefined) {
    return yield* journalError(`Turn ${input.turn} has no terminal Tool results to commit`);
  }
  if (input.runCompletion !== undefined && toolRecords.length !== 1) {
    return yield* journalError("A terminal Tool completion requires exactly one settled result");
  }
  const completionRecord = runCompletionRecord(input);

  return CanonicalBatch.make({
    batchId: turnResultsBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records:
      completionRecord === undefined
        ? [first, ...toolRecords.slice(1)]
        : [first, ...toolRecords.slice(1), completionRecord],
  });
});

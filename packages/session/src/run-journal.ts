import { ConversationId, RunId, ToolCallId, TurnId, type SubmissionId } from "@effect-agent/core";
import { Crypto, Effect, Schema, type DateTime } from "effect";
import { Prompt } from "effect/unstable/ai";

import { digestJson, type DigestError } from "./digest.ts";
import { IdempotencyKey } from "./ledger.ts";
import {
  BatchId,
  CanonicalBatch,
  ModelResponseRecorded,
  PersistedJson,
  RecordEnvelope,
  RecordId,
  ToolCallSettled,
  type CanonicalRecordEnvelope,
  type DeploymentId,
  type ProducerId,
} from "./records.ts";

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

/** Deterministic canonical record identity of one Turn's `ModelResponseRecorded` record. */
export const modelResponseRecordId = (runId: RunId, turn: number): RecordId =>
  decodeRecordId(`model-response:${runId}:${turn}`);

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

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

/**
 * Deterministic canonical record identity of one parent Tool Call's `SubagentRequested` record
 * (spec/subagents.md §12 step 3). The one-record batch reuses the same string, so batch
 * idempotency plus the parent epoch fence make the request append exactly-once-canonical.
 */
export const subagentRequestedRecordId = (runId: RunId, toolCallId: ToolCallId): RecordId =>
  decodeRecordId(`subagent-requested:${runId}:${toolCallId}`);

/** Deterministic batch identity of one `SubagentRequested` append (same string). */
export const subagentRequestedBatchId = (runId: RunId, toolCallId: ToolCallId): BatchId =>
  decodeBatchId(`subagent-requested:${runId}:${toolCallId}`);

/**
 * Deterministic canonical record identity of one parent Tool Call's `SubagentStarted` record
 * (spec/subagents.md §12 step 9). Recovery's start-link repair appends the EXACT same record
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
 * Deterministic identity of one child Conversation's `SubagentLineageRecorded` record
 * (spec/subagents.md §11). Its own single-record batch reuses the same string so the generic
 * `conversation-created:{cid}` batch identity is never contradicted.
 */
export const subagentLineageRecordId = (conversationId: ConversationId): RecordId =>
  decodeRecordId(`subagent-lineage:${conversationId}`);

/** Deterministic batch identity of one `SubagentLineageRecorded` append (same string). */
export const subagentLineageBatchId = (conversationId: ConversationId): BatchId =>
  decodeBatchId(`subagent-lineage:${conversationId}`);

/**
 * Deterministic intended child Conversation identity (spec/subagents.md §12 step 4, D4): the
 * parent Submission and Tool Call pair addresses exactly one child Conversation, so a replayed
 * establishment converges on the one existing child (SUB-016).
 */
export const childConversationIdFor = (
  parentSubmissionId: SubmissionId,
  toolCallId: ToolCallId,
): ConversationId => decodeConversationId(`subagent:${parentSubmissionId}:${toolCallId}`);

/**
 * Deterministic child admission idempotency key (spec/subagents.md §12 step 4, D4): scoped to
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
        journalError(
          "ModelResponseRecorded messages are not Schema-encoded Prompt messages",
          cause,
        ),
      ),
    ),
);

const toolMessageFromSettled = Effect.fn("RunJournal.toolMessageFromSettled")(
  (settled: ReadonlyArray<ToolCallSettled>): Effect.Effect<Prompt.Message, RunJournalError> =>
    Effect.try({
      try: () =>
        Prompt.makeMessage("tool", {
          content: settled.map((record) =>
            Prompt.makePart("tool-result", {
              id: record.toolCallId,
              name: record.toolName,
              result: record.result,
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
 * - `UserInputRecorded` is the canonical admission fact, not a prompt-bearing record: its
 *   Prompt-visible form (instructions + user message) becomes canonical inside the owning Run's
 *   first `ModelResponseRecorded`. The projection consumes it only for Run correlation.
 */
export interface RunJournalProjection {
  /** Full canonical model-visible prompt across every committed Turn of every Run. */
  readonly prompt: Prompt.Prompt;
  /** Canonical prompt excluding the projected Run's records: the resuming Attempt's history. */
  readonly historyBefore: Prompt.Prompt;
  /** Number of canonical Turns already committed for the projected Run. */
  readonly committedTurns: number;
}

interface FoldState {
  readonly all: Array<Prompt.Message>;
  readonly before: Array<Prompt.Message>;
  readonly pendingTools: Array<ToolCallSettled>;
  readonly pendingToolsForRun: boolean;
  readonly committedTurns: number;
}

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
 * order is authoritative; the fold appends each `ModelResponseRecorded`'s messages and flushes
 * each contiguous group of `ToolCallSettled` records into one Tool message, exactly mirroring the
 * per-Turn commit shape produced by `turnCanonicalBatch` (no-tool Turns) and by the
 * `turnResponseBatch`/`turnResultsBatch` split (tool-declaring Turns). The Phase 5 audit tags
 * are skipped transparently, so split-batch commits replay to the same prompt as P4 single-batch
 * commits.
 */
export const projectRunJournal = Effect.fn("RunJournal.projectRunJournal")(function* (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  runId: RunId,
): Effect.fn.Return<RunJournalProjection, RunJournalError> {
  let state: FoldState = {
    all: [],
    before: [],
    pendingTools: [],
    pendingToolsForRun: false,
    committedTurns: 0,
  };

  const flushTools = Effect.fn("RunJournal.flushTools")(function* (
    current: FoldState,
  ): Effect.fn.Return<FoldState, RunJournalError> {
    if (current.pendingTools.length === 0) return current;
    const toolMessage = yield* toolMessageFromSettled(current.pendingTools);
    return {
      ...current,
      all: [...current.all, toolMessage],
      before: current.pendingToolsForRun ? current.before : [...current.before, toolMessage],
      pendingTools: [],
      pendingToolsForRun: false,
    };
  });

  for (const envelope of records) {
    const payload = envelope.record.payload;
    if (PROMPT_TRANSPARENT_TAGS.has(payload._tag)) continue;
    if (payload._tag === "ToolCallSettled") {
      if (state.pendingTools.length > 0 && state.pendingToolsForRun !== (payload.runId === runId)) {
        state = yield* flushTools(state);
      }
      state = {
        ...state,
        pendingTools: [...state.pendingTools, payload],
        pendingToolsForRun: payload.runId === runId,
      };
      continue;
    }
    state = yield* flushTools(state);
    if (payload._tag !== "ModelResponseRecorded") continue;
    const messages = yield* decodePromptMessages(payload.messages);
    const forRun = payload.runId === runId;
    state = {
      ...state,
      all: [...state.all, ...messages.content],
      before: forRun ? state.before : [...state.before, ...messages.content],
      committedTurns: forRun ? Math.max(state.committedTurns, payload.turn) : state.committedTurns,
    };
  }
  state = yield* flushTools(state);

  return {
    prompt: Prompt.fromMessages(state.all),
    historyBefore: Prompt.fromMessages(state.before),
    committedTurns: state.committedTurns,
  };
});

const NO_RUN = decodeRunId("run:none");

/**
 * Pure prompt reconstruction from canonical records: `UserInputRecorded` + `ModelResponseRecorded`
 * + `ToolCallSettled` → the deterministic model-visible Prompt (plan §Coordinator flow step 3).
 */
export const promptFromCanonicalRecords = Effect.fn("RunJournal.promptFromCanonicalRecords")(
  (
    records: ReadonlyArray<CanonicalRecordEnvelope>,
  ): Effect.Effect<Prompt.Prompt, RunJournalError> =>
    projectRunJournal(records, NO_RUN).pipe(Effect.map((projection) => projection.prompt)),
);

/** Everything one committed Turn contributes to its canonical batch. */
export interface TurnCommitInput {
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
}

const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodePersistedJson = Schema.decodeUnknownEffect(PersistedJson);
const encodePrompt = Schema.encodeEffect(Prompt.Prompt);

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
  const encodedMessages = yield* encodePrompt(Prompt.fromMessages([...promptMessages])).pipe(
    Effect.mapError((cause) => journalError("Turn Prompt messages failed to encode", cause)),
  );
  const messages = yield* decodePersistedJson(encodedMessages).pipe(
    Effect.mapError((cause) =>
      journalError("Turn Prompt messages exceed canonical persistence bounds", cause),
    ),
  );
  const messagesDigest = yield* digestJson(messages);
  return RecordEnvelope.make({
    recordId: modelResponseRecordId(input.runId, input.turn),
    family: "conversation",
    schemaVersion: 1,
    createdAt: input.createdAt,
    deploymentId: input.deploymentId,
    payload: ModelResponseRecorded.make({
      runId: input.runId,
      turnId: input.turnId,
      turn: input.turn,
      messages,
      messagesDigest,
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
        family: "conversation",
        schemaVersion: 1,
        createdAt: input.createdAt,
        deploymentId: input.deploymentId,
        payload: ToolCallSettled.make({
          runId: input.runId,
          toolCallId,
          toolName: part.name,
          result,
          isFailure: part.isFailure,
        }),
      }),
    );
  }
  return toolRecords;
});

/**
 * Pure per-Turn canonical batch builder (TurnCompleted seam fold, D6/D8): one
 * `ModelResponseRecorded` record plus one `ToolCallSettled` record per terminal Tool result, all
 * under the WP0-style deterministic identities, committed as ONE atomic batch. The same input
 * always yields byte-identical content, so an in-Attempt append retry is an honest batch replay.
 *
 * Phase 5 keeps this exact shape for Turns that declare no application Tool calls (decision
 * point 6: P4 histories replay byte-identically); tool-declaring Turns split into
 * `turnResponseBatch` + `turnResultsBatch`.
 */
export const turnCanonicalBatch = Effect.fn("RunJournal.turnCanonicalBatch")(function* (
  input: TurnCommitInput,
): Effect.fn.Return<CanonicalBatch, RunJournalError | DigestError, Crypto.Crypto> {
  yield* requireCanonicalTurn(input.turn);
  const { promptMessages, toolParts } = splitTurnMessages(input.appended);
  const modelResponse = yield* modelResponseRecord(input, promptMessages);
  const toolRecords = yield* toolSettledRecords(input, toolParts);
  return CanonicalBatch.make({
    batchId: turnBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records: [modelResponse, ...toolRecords],
  });
});

/**
 * Commit 1 of a tool-declaring Turn (plan §2.1): ONLY the Turn's `ModelResponseRecorded` record
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
  return CanonicalBatch.make({
    batchId: turnResultsBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records: [first, ...toolRecords.slice(1)],
  });
});

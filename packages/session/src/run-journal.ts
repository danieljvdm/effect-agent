import { RunId, ToolCallId, TurnId, type SubmissionId } from "@effect-agent/core";
import { Crypto, Effect, Schema, type DateTime } from "effect";
import { Prompt } from "effect/unstable/ai";

import { digestJson, type DigestError } from "./digest.ts";
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
export class RunJournalError extends Schema.TaggedErrorClass<RunJournalError>()("RunJournalError", {
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

/** Deterministic batch identity of one committed canonical Turn. */
export const turnBatchId = (runId: RunId, turn: number): BatchId =>
  decodeBatchId(`turn:${runId}:${turn}`);

/** Deterministic canonical record identity of one Turn's `ModelResponseRecorded` record. */
export const modelResponseRecordId = (runId: RunId, turn: number): RecordId =>
  decodeRecordId(`model-response:${runId}:${turn}`);

/** Deterministic canonical record identity of one Turn's `ToolCallSettled` record. */
export const toolCallSettledRecordId = (
  runId: RunId,
  turn: number,
  toolCallId: ToolCallId,
): RecordId => decodeRecordId(`tool-settled:${runId}:${turn}:${toolCallId}`);

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
 * Pure projection: rebuild one Run's resume state from canonical records (DUR-015). Canonical
 * order is authoritative; the fold appends each `ModelResponseRecorded`'s messages and flushes
 * each contiguous group of `ToolCallSettled` records into one Tool message, exactly mirroring the
 * per-Turn commit shape produced by `turnCanonicalBatch`.
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

/**
 * Pure per-Turn canonical batch builder (TurnCompleted seam fold, D6/D8): one
 * `ModelResponseRecorded` record plus one `ToolCallSettled` record per terminal Tool result, all
 * under the WP0-style deterministic identities, committed as ONE atomic batch. The same input
 * always yields byte-identical content, so an in-Attempt append retry is an honest batch replay.
 */
export const turnCanonicalBatch = Effect.fn("RunJournal.turnCanonicalBatch")(function* (
  input: TurnCommitInput,
): Effect.fn.Return<CanonicalBatch, RunJournalError | DigestError, Crypto.Crypto> {
  if (!Number.isInteger(input.turn) || input.turn <= 0) {
    return yield* journalError(`Canonical turn number must be a positive integer: ${input.turn}`);
  }
  const promptMessages: Array<Prompt.Message> = [];
  const toolParts: Array<Prompt.ToolResultPart> = [];
  for (const message of input.appended) {
    if (message.role !== "tool") {
      promptMessages.push(message);
      continue;
    }
    for (const part of message.content) {
      if (part.type !== "tool-result") continue;
      toolParts.push(part);
    }
  }
  if (promptMessages.length === 0) {
    return yield* journalError(`Turn ${input.turn} appended no model-visible Prompt messages`);
  }

  const encodedMessages = yield* encodePrompt(Prompt.fromMessages(promptMessages)).pipe(
    Effect.mapError((cause) => journalError("Turn Prompt messages failed to encode", cause)),
  );
  const messages = yield* decodePersistedJson(encodedMessages).pipe(
    Effect.mapError((cause) =>
      journalError("Turn Prompt messages exceed canonical persistence bounds", cause),
    ),
  );
  const messagesDigest = yield* digestJson(messages);

  const modelResponse = RecordEnvelope.make({
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

  return CanonicalBatch.make({
    batchId: turnBatchId(input.runId, input.turn),
    producerId: input.producerId,
    records: [modelResponse, ...toolRecords],
  });
});

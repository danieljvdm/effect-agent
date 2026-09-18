import { Context, Effect, Option, Schema, Stream } from "effect";

import { RunId, SubmissionId, ThreadId } from "../core/Identifiers.ts";
import type { ToolCallPrepared, WorkerInputRequested } from "./Records.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  ObservationOffset,
  PersistedJson,
  ProducerEpoch,
  RecordId,
} from "./Records.ts";
import { runIdForSubmission, toolCallPreparedRecordId } from "./RunJournal.ts";
import { SubmissionLedger, SubmissionLookupById } from "./SubmissionLedger.ts";

export const MAX_THREAD_EXPORT_RECORDS = 131_072;

/** Exact canonical identity; absence is not proof that an admission was never accepted. */
export const ThreadRecordRequest = Schema.Struct({ threadId: ThreadId, recordId: RecordId });
export type ThreadRecordRequest = typeof ThreadRecordRequest.Type;

/** The original user input for a Run, excluding later joined inputs. */
export const ThreadRunInputRequest = Schema.Struct({ threadId: ThreadId, runId: RunId });
export type ThreadRunInputRequest = typeof ThreadRunInputRequest.Type;

export const ThreadOutstandingRequest = Schema.Struct({
  threadId: ThreadId,
  /** Exceeding this bound fails; a truncated inventory never grants authority. */
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(4_096)),
});

export type ThreadOutstandingRequest = typeof ThreadOutstandingRequest.Type;

export interface ThreadOutstanding {
  readonly threadId: ThreadId;
  readonly throughSequence: CanonicalSequence;
  readonly complete: true;
  readonly operations: ReadonlyArray<{
    readonly submissionId: SubmissionId;
    readonly prepared: ToolCallPrepared;
    /** Prepared is not an unknown outcome or a grant to execute. */
    readonly state: "prepared" | "unknown";
  }>;
  /** Source reservations without proof that external effects are resolved. */
  readonly workerInputs: ReadonlyArray<WorkerInputRequested>;
}

/** Native worker reservation/accounting snapshot at a captured canonical tail. */
export const ThreadWorkerStateRequest = Schema.Struct({
  threadId: ThreadId,
  sourceSubmissionId: Schema.optionalKey(SubmissionId),
  limit: Schema.Int.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(MAX_THREAD_EXPORT_RECORDS),
  ),
});

export type ThreadWorkerStateRequest = typeof ThreadWorkerStateRequest.Type;

/** Saturating lifetime peer-message count: the result is at most the requested cap. */
export const ThreadPeerCountRequest = Schema.Struct({
  threadId: ThreadId,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1000)),
});

export type ThreadPeerCountRequest = typeof ThreadPeerCountRequest.Type;

const incomplete = (operation: string) =>
  ThreadStoreError.make({
    operation,
    message: "Native canonical read is incomplete or corrupt",
  });

/** Collect a complete bounded selection, using canonical sequence as a sparse cursor. */
const selectedRecords = Effect.fnUntraced(function* (
  threadId: ThreadId,
  selection: ThreadSelection,
  limit: number,
) {
  const store = yield* ThreadStore;
  const records: Array<CanonicalRecordEnvelope> = [];
  let afterSequence: CanonicalSequence | undefined;

  while (true) {
    const pageLimit = Math.min(1024, limit + 1 - records.length);

    const page = yield* Stream.runCollect(
      store.read({
        threadId,
        selection,
        page: { limit: pageLimit, ...(afterSequence === undefined ? {} : { afterSequence }) },
      }),
    );

    for (const entry of page) {
      if (
        entry.threadId !== threadId ||
        entry.sequence <= (afterSequence ?? 0) ||
        ("expectedTailSequence" in selection && entry.sequence > selection.expectedTailSequence)
      )
        return yield* incomplete("read selection identity");
      afterSequence = entry.sequence;
      records.push(entry);
    }
    if (page.length > pageLimit || records.length > limit)
      return yield* incomplete("read selection limit");
    if (page.length < pageLimit) return records;
  }
});

/** Host-owned read: authenticate the Thread and exact locator before calling. */
export const getRecord = Effect.fn("ThreadStore.getRecord")(function* (
  request: ThreadRecordRequest,
) {
  yield* Schema.decodeEffect(ThreadRecordRequest)(request).pipe(
    Effect.mapError(() => incomplete("getRecord request")),
  );

  const records = yield* selectedRecords(
    request.threadId,
    { _tag: "RecordId", recordId: request.recordId },
    1,
  );

  if (records.some((entry) => entry.record.recordId !== request.recordId))
    return yield* incomplete("getRecord identity");

  return Option.fromUndefinedOr(records[0]);
});

export const getRunInput = Effect.fn("ThreadStore.getRunInput")(function* (
  request: ThreadRunInputRequest,
) {
  yield* Schema.decodeEffect(ThreadRunInputRequest)(request).pipe(
    Effect.mapError(() => incomplete("getRunInput request")),
  );

  const records = yield* selectedRecords(
    request.threadId,
    { _tag: "RunInput", runId: request.runId },
    1,
  );

  if (
    records.some(
      ({ record: { payload } }) =>
        payload._tag !== "UserInputRecorded" ||
        payload.kind !== "user" ||
        payload.runId !== request.runId,
    )
  )
    return yield* incomplete("getRunInput identity");

  return Option.fromUndefinedOr(records[0]);
});

/** A bounded snapshot of native lifetime and source-submission worker accounting. */
export const readWorkerState = Effect.fn("ThreadStore.readWorkerState")(function* (
  request: ThreadWorkerStateRequest,
) {
  yield* Schema.decodeEffect(ThreadWorkerStateRequest)(request).pipe(
    Effect.mapError(() => incomplete("readWorkerState request")),
  );
  const store = yield* ThreadStore;
  const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId: request.threadId }));

  const records = yield* selectedRecords(
    request.threadId,
    {
      _tag: "WorkerState",
      expectedTailSequence: tail.tailSequence,
      expectedTailDigest: tail.tailDigest,
      ...(request.sourceSubmissionId === undefined
        ? {}
        : { sourceSubmissionId: request.sourceSubmissionId }),
    },
    request.limit,
  );

  return {
    threadId: request.threadId,
    tailSequence: tail.tailSequence,
    tailDigest: tail.tailDigest,
    records,
  };
});

/**
 * Work is proportional to outstanding records, independent of completed history. Unknown
 * effects survive abort and resolution intent; only canonical closure removes them. A
 * preparation from another Run still requires native recovery before it can be treated as
 * safe. This snapshot grants no execution authority and does not freeze other owners.
 */
export const readOutstanding = Effect.fn("ThreadStore.readOutstanding")(function* (
  request: ThreadOutstandingRequest,
) {
  yield* Schema.decodeEffect(ThreadOutstandingRequest)(request).pipe(
    Effect.mapError(() => incomplete("readOutstanding request")),
  );
  const store = yield* ThreadStore;
  const ledger = yield* SubmissionLedger;
  const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId: request.threadId }));

  const records = yield* selectedRecords(
    request.threadId,
    {
      _tag: "Outstanding",
      expectedTailSequence: tail.tailSequence,
      expectedTailDigest: tail.tailDigest,
    },
    request.limit,
  );

  const operations: Array<ThreadOutstanding["operations"][number]> = [];
  const workerInputs: Array<WorkerInputRequested> = [];

  for (const entry of records) {
    const payload = entry.record.payload;

    if (payload._tag === "WorkerInputRequested") {
      workerInputs.push(payload);
      continue;
    }
    if (payload._tag !== "ToolCallPrepared" && payload._tag !== "ToolCallUnknown")
      return yield* incomplete("readOutstanding record");

    const preparedEntry =
      payload._tag === "ToolCallPrepared"
        ? entry
        : Option.getOrUndefined(
            yield* getRecord({
              threadId: request.threadId,
              recordId: toolCallPreparedRecordId(payload.runId, payload.turn, payload.toolCallId),
            }),
          );

    const inputEntry = Option.getOrUndefined(
      yield* getRunInput({ threadId: request.threadId, runId: payload.runId }),
    );

    if (
      preparedEntry === undefined ||
      inputEntry === undefined ||
      preparedEntry.sequence > tail.tailSequence ||
      inputEntry.sequence > tail.tailSequence
    )
      return yield* incomplete("readOutstanding proof beyond snapshot");
    const prepared = preparedEntry.record.payload;
    const input = inputEntry.record.payload;

    if (
      prepared?._tag !== "ToolCallPrepared" ||
      prepared.runId !== payload.runId ||
      prepared.turn !== payload.turn ||
      prepared.toolCallId !== payload.toolCallId ||
      prepared.toolName !== payload.toolName ||
      input?._tag !== "UserInputRecorded" ||
      input.submissionId === undefined ||
      runIdForSubmission(input.submissionId) !== prepared.runId
    )
      return yield* incomplete("readOutstanding canonical ownership");

    const submission = yield* ledger.lookup(
      SubmissionLookupById.make({ submissionId: input.submissionId }),
    );

    if (Option.isNone(submission) || submission.value.threadId !== request.threadId)
      return yield* incomplete("readOutstanding native admission");
    operations.push({
      submissionId: input.submissionId,
      prepared,
      state: payload._tag === "ToolCallUnknown" ? "unknown" : "prepared",
    });
  }

  return {
    threadId: request.threadId,
    throughSequence: tail.tailSequence,
    complete: true as const,
    operations,
    workerInputs,
  };
});

export class ThreadMaterialization extends Schema.Class<ThreadMaterialization>(
  "@effect-agent/thread/ThreadMaterialization",
)({
  threadId: ThreadId,
  producerEpoch: ProducerEpoch,
}) {}

export class FencedAppendRequest extends Schema.Class<FencedAppendRequest>(
  "@effect-agent/thread/FencedAppendRequest",
)({
  threadId: ThreadId,
  batch: CanonicalBatch,
  expectedTailSequence: CanonicalSequence,
  expectedTailDigest: Digest,
  producerEpoch: ProducerEpoch,
}) {}

export class AppendResult extends Schema.Class<AppendResult>("@effect-agent/thread/AppendResult")({
  firstSequence: CanonicalSequence,
  lastSequence: CanonicalSequence,
  tailDigest: Digest,
  replayed: Schema.Boolean,
}) {}

export class ThreadRead extends Schema.Class<ThreadRead>("@effect-agent/thread/ThreadRead")({
  threadId: ThreadId,
  afterSequence: Schema.optionalKey(CanonicalSequence),
  limit: Schema.Natural.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_024)),
}) {}

/** Closed native selections; mutable metadata is read at this exact canonical tail. */
export const ThreadSelection = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("RecordId"), recordId: RecordId }),
  Schema.Struct({ _tag: Schema.Literal("RunInput"), runId: RunId }),
  Schema.Struct({
    _tag: Schema.Literal("Outstanding"),
    expectedTailSequence: CanonicalSequence,
    expectedTailDigest: Digest,
  }),
  Schema.Struct({
    _tag: Schema.Literal("WorkerState"),
    expectedTailSequence: CanonicalSequence,
    expectedTailDigest: Digest,
    sourceSubmissionId: Schema.optionalKey(SubmissionId),
  }),
]);

export type ThreadSelection = typeof ThreadSelection.Type;

/** Nested page makes an old ThreadRead decoder reject, never strip a selection into history. */
export const SelectedThreadRead = Schema.Struct({
  threadId: ThreadId,
  selection: ThreadSelection,
  page: Schema.Struct({
    afterSequence: Schema.optionalKey(CanonicalSequence),
    limit: ThreadRead.fields.limit,
  }),
});

export type SelectedThreadRead = typeof SelectedThreadRead.Type;
export const ThreadReadRequest = Schema.Union([SelectedThreadRead, ThreadRead]);
export type ThreadReadRequest = typeof ThreadReadRequest.Type;

export class ThreadObservation extends Schema.Class<ThreadObservation>(
  "@effect-agent/thread/ThreadObservation",
)({
  threadId: ThreadId,
  afterOffset: Schema.optionalKey(ObservationOffset),
}) {}

export class ThreadExportRequest extends Schema.Class<ThreadExportRequest>(
  "@effect-agent/thread/ThreadExportRequest",
)({
  threadId: ThreadId,
}) {}

export class ThreadTailRequest extends Schema.Class<ThreadTailRequest>(
  "@effect-agent/thread/ThreadTailRequest",
)({
  threadId: ThreadId,
}) {}

/**
 * The committed tail of one Thread Log. A resuming producer composes its next
 * FencedAppendRequest from this value instead of exporting the whole log.
 */
export class ThreadTail extends Schema.Class<ThreadTail>("@effect-agent/thread/ThreadTail")({
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  producerEpoch: ProducerEpoch,
}) {}

/** Maximum canonical records represented by one Thread export. */

export class ThreadExport extends Schema.Class<ThreadExport>("@effect-agent/thread/ThreadExport")({
  format: Schema.Literal("effect-agent/thread@1"),
  threadId: ThreadId,
  tailSequence: CanonicalSequence,
  tailDigest: Digest,
  records: Schema.Array(CanonicalRecordEnvelope).check(
    Schema.isMaxLength(MAX_THREAD_EXPORT_RECORDS),
  ),
}) {}

/**
 * A disposable projection snapshot. Adapters bind its sequence and digest to the canonical
 * log; consumers decode `state` and decide projection compatibility before suffix replay.
 * Legacy metadata is optional for application projections and preserved when supplied.
 * Runtime-owned recovery checkpoints populate and compare it before using cached state.
 */
export class ThreadCheckpoint extends Schema.Class<ThreadCheckpoint>(
  "@effect-agent/thread/ThreadCheckpoint",
)({
  schemaVersion: Schema.Literal(1),
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  engineVersion: Schema.optionalKey(Schema.NonEmptyString),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  agentDefinitionDigest: Schema.optionalKey(Digest),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  modelDigest: Schema.optionalKey(Digest),
  /** @deprecated For application projections; retained for existing data and runtime recovery. */
  toolDigest: Schema.optionalKey(Digest),
  state: PersistedJson,
  createdAt: Schema.DateTimeUtcFromString,
}) {}

export class SaveCheckpointRequest extends Schema.Class<SaveCheckpointRequest>(
  "@effect-agent/thread/SaveCheckpointRequest",
)({
  checkpoint: ThreadCheckpoint,
}) {}

/** Replace the disposable recovery view only under the current canonical producer fence. */
export class SaveRecoveryCheckpointRequest extends Schema.Class<SaveRecoveryCheckpointRequest>(
  "@effect-agent/thread/SaveRecoveryCheckpointRequest",
)({
  checkpoint: ThreadCheckpoint,
  producerEpoch: ProducerEpoch,
}) {}

export class LoadCheckpointRequest extends Schema.Class<LoadCheckpointRequest>(
  "@effect-agent/thread/LoadCheckpointRequest",
)({
  threadId: ThreadId,
  atOrBeforeSequence: Schema.optionalKey(CanonicalSequence),
}) {}

/** Content-free storage provenance. Never include rejected values, SQL, or schema messages. */
export class ThreadStoreDiagnostic extends Schema.Class<ThreadStoreDiagnostic>(
  "@effect-agent/thread/ThreadStoreDiagnostic",
)({
  causeTag: Schema.String.check(Schema.isMaxLength(128)),
  operation: Schema.String.check(Schema.isMaxLength(256)),
  decoder: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(128))),
  issueTag: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(128))),
  sequence: Schema.optionalKey(CanonicalSequence),
}) {}

export class ThreadStoreError extends Schema.TaggedError<ThreadStoreError>()("ThreadStoreError", {
  operation: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
  diagnostic: Schema.optionalKey(ThreadStoreDiagnostic),
}) {}

export class ThreadNotMaterialized extends Schema.TaggedError<ThreadNotMaterialized>()(
  "ThreadNotMaterialized",
  { threadId: ThreadId },
) {}

/**
 * A canonical append that cannot commit: `batch-digest` replays a batch ID with different
 * content, `record-identity` reuses a canonical record ID, and `tail` declares a stale expected
 * tail. Tail conflicts carry the actual committed tail as a diagnostic resume hint.
 */
export class AppendConflict extends Schema.TaggedError<AppendConflict>()("AppendConflict", {
  threadId: ThreadId,
  batchId: BatchId,
  reason: Schema.Literals(["batch-digest", "record-identity", "tail"]),
  actualTailSequence: Schema.optionalKey(CanonicalSequence),
  actualTailDigest: Schema.optionalKey(Digest),
}) {}

export class FenceRejected extends Schema.TaggedError<FenceRejected>()("FenceRejected", {
  threadId: ThreadId,
  actualEpoch: ProducerEpoch,
  attemptedEpoch: ProducerEpoch,
}) {}

export class CheckpointRejected extends Schema.TaggedError<CheckpointRejected>()(
  "CheckpointRejected",
  {
    threadId: ThreadId,
    reason: Schema.Literals(["ahead-of-tail", "digest-mismatch", "unsupported-version", "corrupt"]),
  },
) {}

export type ThreadStoreFailure =
  | ThreadStoreError
  | ThreadNotMaterialized
  | AppendConflict
  | FenceRejected;

/**
 * Optional, disposable projection storage. Neither history execution nor durable recovery
 * requires it. Adapters that offer it must bind every checkpoint to a canonical batch tail.
 * Application consumers own projection compatibility; this port does not interpret metadata.
 */
export interface ThreadCheckpoints {
  readonly save: (
    request: SaveCheckpointRequest,
  ) => Effect.Effect<void, ThreadStoreError | ThreadNotMaterialized | CheckpointRejected>;
  readonly load: (
    request: LoadCheckpointRequest,
  ) => Effect.Effect<
    Option.Option<ThreadCheckpoint>,
    ThreadStoreError | ThreadNotMaterialized | CheckpointRejected
  >;
}

/**
 * Optional latest-only recovery cache, independent of application projection checkpoints.
 * Saves atomically validate the producer epoch and canonical batch tail. An older snapshot
 * cannot replace a newer one; equal-tail replacement repairs disposable state. Invalid cached
 * data fails with CheckpointRejected, while infrastructure failures remain ThreadStoreError.
 * Loading at an earlier tail or outside the adapter's cache locality may return none so callers
 * can replay canonical records. Canonical records and the ledger remain authority.
 */
export interface ThreadRecoveryCheckpoints {
  readonly save: (
    request: SaveRecoveryCheckpointRequest,
  ) => Effect.Effect<
    void,
    ThreadStoreError | ThreadNotMaterialized | CheckpointRejected | FenceRejected
  >;
  readonly load: ThreadCheckpoints["load"];
}

export class ThreadStore extends Context.Service<
  ThreadStore,
  {
    readonly materialize: (
      request: ThreadMaterialization,
    ) => Effect.Effect<void, ThreadStoreError | FenceRejected>;
    readonly append: (
      request: FencedAppendRequest,
    ) => Effect.Effect<
      AppendResult,
      ThreadStoreError | ThreadNotMaterialized | AppendConflict | FenceRejected
    >;
    readonly read: (
      request: ThreadReadRequest,
    ) => Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>;
    readonly observe: (
      request: ThreadObservation,
    ) => Stream.Stream<CanonicalRecordEnvelope, ThreadStoreError | ThreadNotMaterialized>;
    readonly export: (
      request: ThreadExportRequest,
    ) => Effect.Effect<ThreadExport, ThreadStoreError | ThreadNotMaterialized>;
    readonly inspectTail: (
      request: ThreadTailRequest,
    ) => Effect.Effect<ThreadTail, ThreadStoreError | ThreadNotMaterialized>;
    /** Absent when this adapter does not support disposable checkpoints. */
    readonly checkpoints?: ThreadCheckpoints | undefined;
    readonly recoveryCheckpoints?: ThreadRecoveryCheckpoints | undefined;
    /** Indexed scalar count; unsupported adapters fail closed at the caller. */
    readonly countPeerMessages?:
      | ((
          request: ThreadPeerCountRequest,
        ) => Effect.Effect<number, ThreadStoreError | ThreadNotMaterialized>)
      | undefined;
  }
>()("@effect-agent/thread/ThreadStore") {}

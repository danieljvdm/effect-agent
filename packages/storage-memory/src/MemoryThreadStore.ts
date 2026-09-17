import {
  Context,
  Crypto,
  Effect,
  Encoding,
  Layer,
  Option,
  PubSub,
  Ref,
  Schema,
  Stream,
} from "effect";
import { digestCanonicalBatch, EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { ThreadId } from "effect-agent/identifiers";
import {
  type ProducerEpoch,
  type ToolCallPrepared,
  type RecordId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ObservationOffset,
  type BatchId,
  type Digest,
} from "effect-agent/records";
import { runIdForSubmission } from "effect-agent/run-journal";
import {
  type ThreadCheckpoint,
  ThreadRecordRequest,
  ThreadWorkerInputsPageRequest,
  ThreadWorkerStateRequest,
  ThreadPeerCountRequest,
  ThreadRunInputRequest,
  ThreadOutstandingRequest,
  type ThreadOutstanding,
  type ThreadNativeReads,
  AppendConflict,
  AppendResult,
  CheckpointRejected,
  ThreadExportRequest,
  ThreadExport,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadObservation,
  ThreadRead,
  ThreadStore,
  type ThreadCheckpoints,
  ThreadStoreError,
  ThreadTail,
  ThreadTailRequest,
  FenceRejected,
  FencedAppendRequest,
  LoadCheckpointRequest,
  SaveCheckpointRequest,
  SaveRecoveryCheckpointRequest,
  type ThreadRecoveryCheckpoints,
  MAX_THREAD_EXPORT_RECORDS,
} from "effect-agent/thread-store";

const MAX_THREADS = 256;
const MAX_RECORDS_PER_THREAD = MAX_THREAD_EXPORT_RECORDS;
const MAX_CHECKPOINTS_PER_THREAD = 1_024;

const ThreadCapacity = Context.Reference<number>(
  "@effect-agent/storage-memory/MemoryThreadStore/ThreadCapacity",
  { defaultValue: () => MAX_THREADS },
);

interface StoredBatch {
  readonly digest: Digest;
  readonly result: AppendResult;
}

interface StoredThread {
  readonly peerCount: number;
  readonly unverifiedWorkerInputs: ReadonlySet<string>;
  readonly workerRecords: ReadonlyMap<string, ReadonlyArray<CanonicalRecordEnvelope>>;
  readonly byId: ReadonlyMap<string, CanonicalRecordEnvelope>;
  readonly runInputs: ReadonlyMap<string, CanonicalRecordEnvelope | null>;
  readonly prepared: ReadonlyMap<string, ToolCallPrepared>;
  readonly outstanding: ReadonlyMap<string, CanonicalRecordEnvelope>;
  readonly producerEpoch: ProducerEpoch;
  readonly tailSequence: CanonicalSequence;
  readonly tailDigest: Digest;
  readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
  readonly recordIds: ReadonlySet<RecordId>;
  readonly batches: ReadonlyMap<BatchId, StoredBatch>;
  readonly tailDigests: ReadonlyMap<CanonicalSequence, Digest>;
  readonly checkpoints: ReadonlyMap<CanonicalSequence, ThreadCheckpoint>;
  readonly recoveryCheckpoint?: ThreadCheckpoint;
}

interface MemoryState {
  readonly threads: ReadonlyMap<ThreadId, StoredThread>;
}

type AppendDecision =
  | {
      readonly _tag: "failure";
      readonly error: ThreadStoreError | ThreadNotMaterialized | AppendConflict | FenceRejected;
    }
  | {
      readonly _tag: "success";
      readonly result: AppendResult;
      readonly records: ReadonlyArray<CanonicalRecordEnvelope>;
    };

type MaterializeDecision =
  | { readonly _tag: "failure"; readonly error: ThreadStoreError | FenceRejected }
  | { readonly _tag: "success" };

type CheckpointDecision =
  | {
      readonly _tag: "failure";
      readonly error: ThreadNotMaterialized | ThreadStoreError | CheckpointRejected;
    }
  | { readonly _tag: "success" };

const storeError = (operation: string, message: string, cause?: unknown): ThreadStoreError =>
  cause === undefined
    ? ThreadStoreError.make({ operation, message })
    : ThreadStoreError.make({ operation, message, cause });

const validate = Effect.fn("MemoryThreadStore.validate")(
  <A, I>(
    schema: Schema.Codec<A, I>,
    operation: string,
    value: unknown,
  ): Effect.Effect<A, ThreadStoreError> =>
    Schema.encodeUnknownEffect(schema)(value).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
      Effect.mapError((error) => storeError(operation, `Invalid ${operation} request`, error)),
    ),
);

const decodeCanonicalSequence = Schema.decodeSync(CanonicalSequence);
const ZERO_CANONICAL_SEQUENCE = decodeCanonicalSequence(0);

const offsetSequence = Effect.fn("MemoryThreadStore.offsetSequence")((
  threadId: ThreadId,
  offset: ObservationOffset | undefined,
): Effect.Effect<CanonicalSequence, ThreadStoreError> => {
  if (offset === undefined) return Effect.succeed(ZERO_CANONICAL_SEQUENCE);
  const prefix = `memory:v1:${Encoding.encodeBase64(threadId)}:`;
  const encodedSequence = offset.startsWith(prefix) ? offset.slice(prefix.length) : "";

  if (!/^\d+$/.test(encodedSequence)) {
    return Effect.fail(storeError("observe", "Malformed observation offset"));
  }
  const sequence = Number(encodedSequence);

  return Number.isSafeInteger(sequence)
    ? Schema.decodeEffect(CanonicalSequence)(sequence).pipe(
        Effect.mapError(() => storeError("observe", "Malformed observation offset")),
      )
    : Effect.fail(storeError("observe", "Malformed observation offset"));
});

const observationOffset = (threadId: ThreadId, sequence: CanonicalSequence): ObservationOffset =>
  Schema.decodeSync(ObservationOffset)(`memory:v1:${Encoding.encodeBase64(threadId)}:${sequence}`);

const findThread = Effect.fn("MemoryThreadStore.findThread")((
  state: MemoryState,
  threadId: ThreadId,
): Effect.Effect<StoredThread, ThreadNotMaterialized> => {
  const thread = state.threads.get(threadId);

  return thread === undefined
    ? Effect.fail(ThreadNotMaterialized.make({ threadId }))
    : Effect.succeed(thread);
});

const CheckpointVersionEnvelope = Schema.Struct({
  checkpoint: Schema.Struct({
    threadId: ThreadId,
    schemaVersion: Schema.Natural,
  }),
});

const validateCheckpointVersion = Effect.fn("MemoryThreadStore.validateCheckpointVersion")(
  function* (value: unknown): Effect.fn.Return<void, ThreadStoreError | CheckpointRejected> {
    const envelope = yield* Schema.decodeUnknownEffect(CheckpointVersionEnvelope)(value).pipe(
      Effect.mapError(() => storeError("saveCheckpoint", "Invalid saveCheckpoint request")),
    );

    if (envelope.checkpoint.schemaVersion !== 1) {
      return yield* CheckpointRejected.make({
        threadId: envelope.checkpoint.threadId,
        reason: "unsupported-version",
      });
    }
  },
);

const makeThreadStore = Effect.gen(function* () {
  const maxThreads = yield* ThreadCapacity;
  const crypto = yield* Crypto.Crypto;
  const state = yield* Ref.make<MemoryState>({ threads: new Map() });
  const updates = yield* PubSub.sliding<void>(1);

  yield* Effect.addFinalizer(() => PubSub.shutdown(updates));

  const materialize: ThreadStore["Service"]["materialize"] = Effect.fn(
    "MemoryThreadStore.materialize",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ThreadMaterialization, "materialize", unvalidated);

      const decision = yield* Ref.modify(
        state,
        (current): readonly [MaterializeDecision, MemoryState] => {
          const existing = current.threads.get(request.threadId);

          if (existing !== undefined) {
            if (request.producerEpoch < existing.producerEpoch) {
              return [
                {
                  _tag: "failure",
                  error: FenceRejected.make({
                    threadId: request.threadId,
                    actualEpoch: existing.producerEpoch,
                    attemptedEpoch: request.producerEpoch,
                  }),
                },
                current,
              ];
            }
            if (request.producerEpoch === existing.producerEpoch) {
              return [{ _tag: "success" }, current];
            }
            const threads = new Map(current.threads);

            threads.set(request.threadId, {
              ...existing,
              producerEpoch: request.producerEpoch,
            });

            return [{ _tag: "success" }, { threads }];
          }
          if (current.threads.size >= maxThreads) {
            return [
              {
                _tag: "failure",
                error: storeError("materialize", `In-memory thread limit ${maxThreads} exceeded`),
              },
              current,
            ];
          }
          const threads = new Map(current.threads);

          threads.set(request.threadId, {
            producerEpoch: request.producerEpoch,
            tailSequence: ZERO_CANONICAL_SEQUENCE,
            tailDigest: EMPTY_TAIL_DIGEST,
            unverifiedWorkerInputs: new Set(),
            byId: new Map(),
            workerRecords: new Map(),
            peerCount: 0,
            runInputs: new Map(),
            prepared: new Map(),
            outstanding: new Map(),
            records: [],
            recordIds: new Set(),
            batches: new Map(),
            tailDigests: new Map([[ZERO_CANONICAL_SEQUENCE, EMPTY_TAIL_DIGEST]]),
            checkpoints: new Map(),
          });

          return [{ _tag: "success" }, { threads }];
        },
      );

      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const append: ThreadStore["Service"]["append"] = Effect.fn("MemoryThreadStore.append")(
    (unvalidated) =>
      Effect.gen(function* () {
        const request = yield* validate(FencedAppendRequest, "append", unvalidated);

        const digest = yield* digestCanonicalBatch(request.expectedTailDigest, request.batch).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((error) => storeError("append", error.message, error)),
        );

        const decision = yield* Effect.uninterruptible(
          Ref.modify(state, (current): readonly [AppendDecision, MemoryState] => {
            const thread = current.threads.get(request.threadId);

            if (thread === undefined) {
              return [
                {
                  _tag: "failure",
                  error: ThreadNotMaterialized.make({
                    threadId: request.threadId,
                  }),
                },
                current,
              ];
            }
            if (request.producerEpoch !== thread.producerEpoch) {
              return [
                {
                  _tag: "failure",
                  error: FenceRejected.make({
                    threadId: request.threadId,
                    actualEpoch: thread.producerEpoch,
                    attemptedEpoch: request.producerEpoch,
                  }),
                },
                current,
              ];
            }
            const previous = thread.batches.get(request.batch.batchId);

            if (previous !== undefined) {
              if (previous.digest !== digest) {
                return [
                  {
                    _tag: "failure",
                    error: AppendConflict.make({
                      threadId: request.threadId,
                      batchId: request.batch.batchId,
                      reason: "batch-digest",
                    }),
                  },
                  current,
                ];
              }

              return [
                {
                  _tag: "success",
                  result: AppendResult.make({
                    firstSequence: previous.result.firstSequence,
                    lastSequence: previous.result.lastSequence,
                    tailDigest: previous.result.tailDigest,
                    replayed: true,
                  }),
                  records: [],
                },
                current,
              ];
            }
            if (
              request.expectedTailSequence !== thread.tailSequence ||
              request.expectedTailDigest !== thread.tailDigest
            ) {
              return [
                {
                  _tag: "failure",
                  error: AppendConflict.make({
                    threadId: request.threadId,
                    batchId: request.batch.batchId,
                    reason: "tail",
                    actualTailSequence: thread.tailSequence,
                    actualTailDigest: thread.tailDigest,
                  }),
                },
                current,
              ];
            }
            if (thread.records.length + request.batch.records.length > MAX_RECORDS_PER_THREAD) {
              return [
                {
                  _tag: "failure",
                  error: storeError(
                    "append",
                    `In-memory record limit ${MAX_RECORDS_PER_THREAD} exceeded`,
                  ),
                },
                current,
              ];
            }

            const batchRecordIds = new Set<RecordId>();

            for (const record of request.batch.records) {
              if (thread.recordIds.has(record.recordId) || batchRecordIds.has(record.recordId)) {
                return [
                  {
                    _tag: "failure",
                    error: AppendConflict.make({
                      threadId: request.threadId,
                      batchId: request.batch.batchId,
                      reason: "record-identity",
                    }),
                  },
                  current,
                ];
              }
              batchRecordIds.add(record.recordId);
            }

            const records = request.batch.records.map((record, index) => {
              const sequence = decodeCanonicalSequence(thread.tailSequence + index + 1);

              return CanonicalRecordEnvelope.make({
                threadId: request.threadId,
                batchId: request.batch.batchId,
                sequence,
                offset: observationOffset(request.threadId, sequence),
                record,
              });
            });

            const lastSequence = decodeCanonicalSequence(thread.tailSequence + records.length);

            const result = AppendResult.make({
              firstSequence: decodeCanonicalSequence(thread.tailSequence + 1),
              lastSequence,
              tailDigest: digest,
              replayed: false,
            });

            const batches = new Map(thread.batches);

            batches.set(request.batch.batchId, { digest, result });
            const recordIds = new Set(thread.recordIds);

            for (const recordId of batchRecordIds) recordIds.add(recordId);
            const tailDigests = new Map(thread.tailDigests);

            tailDigests.set(lastSequence, digest);
            const threads = new Map(current.threads);

            const unverifiedWorkerInputs = new Set(thread.unverifiedWorkerInputs);
            let peerCount = thread.peerCount;
            const workerRecords = new Map(thread.workerRecords);
            const byId = new Map(thread.byId);
            const runInputs = new Map(thread.runInputs);
            const prepared = new Map(thread.prepared);
            const outstanding = new Map(thread.outstanding);

            for (const entry of records) {
              byId.set(entry.record.recordId, entry);
              const payload = entry.record.payload;

              if (payload._tag === "PeerMessagePrepared") peerCount++;

              const workerKey =
                payload._tag === "SubtreeBudgetReserved"
                  ? `subtree:${payload.sourceSubmissionId ?? ""}`
                  : payload._tag === "SubagentJoined"
                    ? `joined:${payload.runId}`
                    : [
                          "ThreadCreated",
                          "WorkerOriginRecorded",
                          "SubagentLineageRecorded",
                          "WorkerInputRequested",
                          "WorkerInputCompleted",
                        ].includes(payload._tag)
                      ? "worker"
                      : undefined;

              if (workerKey !== undefined)
                workerRecords.set(workerKey, [...(workerRecords.get(workerKey) ?? []), entry]);

              if (
                payload._tag === "UserInputRecorded" &&
                payload.kind === "user" &&
                payload.runId !== undefined
              )
                runInputs.set(payload.runId, runInputs.has(payload.runId) ? null : entry);
              if (
                payload._tag === "ToolCallPrepared" ||
                payload._tag === "ToolCallUnknown" ||
                payload._tag === "ToolCallSettled"
              ) {
                const key = JSON.stringify([payload.runId, payload.toolCallId]);

                if (payload._tag === "ToolCallPrepared") prepared.set(key, payload);
                if (payload._tag === "ToolCallSettled") outstanding.delete(key);
                else outstanding.set(key, entry);
              }
              if (payload._tag === "WorkerInputRequested")
                outstanding.set(`worker:${payload.admission.messageId}`, entry);
              if (payload._tag === "WorkerInputCompleted") {
                if (payload.effectsResolved) {
                  outstanding.delete(`worker:${payload.messageId}`);
                  unverifiedWorkerInputs.delete(payload.messageId);
                } else unverifiedWorkerInputs.add(payload.messageId);
              }
            }
            threads.set(request.threadId, {
              ...thread,
              byId,
              workerRecords,
              peerCount,
              runInputs,
              prepared,
              outstanding,
              unverifiedWorkerInputs,
              tailSequence: lastSequence,
              tailDigest: digest,
              records: [...thread.records, ...records],
              recordIds,
              batches,
              tailDigests,
            });

            return [{ _tag: "success", result, records }, { threads }];
          }).pipe(
            Effect.tap((decision) =>
              decision._tag === "success" && decision.records.length > 0
                ? PubSub.publish(updates, undefined)
                : Effect.void,
            ),
          ),
        );

        if (decision._tag === "failure") return yield* decision.error;

        return decision.result;
      }),
  );

  const nativeReads: ThreadNativeReads = {
    countPeerMessages: Effect.fn("MemoryThreadStore.countPeerMessages")(function* (request) {
      yield* validate(ThreadPeerCountRequest, "countPeerMessages", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      return Math.min(thread.peerCount, request.limit);
    }),
    readWorkerState: Effect.fn("MemoryThreadStore.readWorkerState")(function* (request) {
      yield* validate(ThreadWorkerStateRequest, "readWorkerState", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      const records = [
        ...(thread.workerRecords.get("worker") ?? []),
        ...(thread.workerRecords.get(`subtree:${request.sourceSubmissionId ?? ""}`) ?? []),
        ...(request.sourceSubmissionId === undefined
          ? []
          : (thread.workerRecords.get(`joined:${runIdForSubmission(request.sourceSubmissionId)}`) ??
            [])),
      ].sort((a, b) => a.sequence - b.sequence);

      if (records.length > request.limit)
        return yield* storeError("readWorkerState", "Worker record limit exceeded");

      return {
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        tailDigest: thread.tailDigest,
        records,
      };
    }),
    readWorkerInputsPage: Effect.fn("MemoryThreadStore.readWorkerInputsPage")(function* (request) {
      yield* validate(ThreadWorkerInputsPageRequest, "readWorkerInputsPage", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      const entries = [...thread.outstanding.values()]
        .filter(
          (entry) =>
            entry.sequence > (request.afterSequence ?? 0) &&
            entry.record.payload._tag === "WorkerInputRequested",
        )
        .sort((a, b) => a.sequence - b.sequence)
        .slice(0, request.limit + 1);

      const page = entries.slice(0, request.limit);

      return {
        inputs: page.flatMap((entry) =>
          entry.record.payload._tag === "WorkerInputRequested" ? [entry.record.payload] : [],
        ),
        next: entries.length > request.limit ? (page.at(-1)?.sequence ?? null) : null,
      };
    }),
    getRecord: Effect.fn("MemoryThreadStore.getRecord")(function* (request) {
      yield* validate(ThreadRecordRequest, "getRecord", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      return Option.fromUndefinedOr(thread.byId.get(request.recordId));
    }),
    getRunInput: Effect.fn("MemoryThreadStore.getRunInput")(function* (request) {
      yield* validate(ThreadRunInputRequest, "getRunInput", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      const input = thread.runInputs.get(request.runId);

      if (input === null) return yield* storeError("getRunInput", "Ambiguous original Run input");

      return Option.fromUndefinedOr(input);
    }),
    readOutstanding: Effect.fn("MemoryThreadStore.readOutstanding")(function* (request) {
      yield* validate(ThreadOutstandingRequest, "readOutstanding", request);
      const thread = yield* findThread(yield* Ref.get(state), request.threadId);

      if (thread.outstanding.size > request.limit)
        return yield* storeError("readOutstanding", "Outstanding record limit exceeded");
      const operations: Array<ThreadOutstanding["operations"][number]> = [];
      const workerInputs: Array<ThreadOutstanding["workerInputs"][number]> = [];

      for (const entry of thread.outstanding.values()) {
        const payload = entry.record.payload;

        if (payload._tag === "WorkerInputRequested") {
          workerInputs.push(payload);
          continue;
        }
        if (payload._tag !== "ToolCallPrepared" && payload._tag !== "ToolCallUnknown")
          return yield* storeError("readOutstanding", "Unexpected indexed record");
        const prepared = thread.prepared.get(JSON.stringify([payload.runId, payload.toolCallId]));
        const input = thread.runInputs.get(payload.runId)?.record.payload;

        if (
          prepared === undefined ||
          input?._tag !== "UserInputRecorded" ||
          input.submissionId === undefined
        )
          return yield* storeError("readOutstanding", "Missing canonical preparation or Run input");
        operations.push({
          submissionId: input.submissionId,
          prepared,
          state: payload._tag === "ToolCallUnknown" ? "unknown" : "prepared",
        });
      }

      return {
        complete: thread.unverifiedWorkerInputs.size === 0,
        threadId: request.threadId,
        throughSequence: thread.tailSequence,
        operations,
        workerInputs,
      };
    }),
  };

  const readSnapshot = Effect.fn("MemoryThreadStore.readSnapshot")(
    (threadId: ThreadId, afterSequence: CanonicalSequence | undefined, limit: number) =>
      Ref.get(state).pipe(
        Effect.flatMap((current) => findThread(current, threadId)),
        Effect.map((thread) => {
          // Append assigns gap-free sequences starting at 1, so the exclusive cursor is an index.
          const start = afterSequence ?? ZERO_CANONICAL_SEQUENCE;

          return thread.records.slice(start, start + limit);
        }),
      ),
  );

  const read: ThreadStore["Service"]["read"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadRead, "read", unvalidated);
        const records = yield* readSnapshot(request.threadId, request.afterSequence, request.limit);

        return Stream.fromIterable(records);
      }),
    );

  const observe: ThreadStore["Service"]["observe"] = (unvalidated) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const request = yield* validate(ThreadObservation, "observe", unvalidated);
        const afterSequence = yield* offsetSequence(request.threadId, request.afterOffset);

        return Stream.unwrap(
          Effect.gen(function* () {
            const subscription = yield* PubSub.subscribe(updates);

            const initial = yield* readSnapshot(
              request.threadId,
              afterSequence,
              MAX_RECORDS_PER_THREAD,
            );

            const highWater =
              initial.length === 0 ? afterSequence : (initial.at(-1)?.sequence ?? afterSequence);

            const live = Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(
              Stream.mapAccumEffect(
                () => highWater,
                (lastSequence) =>
                  readSnapshot(request.threadId, lastSequence, MAX_RECORDS_PER_THREAD).pipe(
                    Effect.map(
                      (records) => [records.at(-1)?.sequence ?? lastSequence, records] as const,
                    ),
                  ),
              ),
            );

            return Stream.fromIterable(initial).pipe(Stream.concat(live));
          }),
        );
      }),
    );

  const exportThread: ThreadStore["Service"]["export"] = Effect.fn("MemoryThreadStore.export")(
    (unvalidated) =>
      Effect.gen(function* () {
        const request = yield* validate(ThreadExportRequest, "export", unvalidated);

        const thread = yield* Ref.get(state).pipe(
          Effect.flatMap((current) => findThread(current, request.threadId)),
        );

        return ThreadExport.make({
          format: "effect-agent/thread@1",
          threadId: request.threadId,
          tailSequence: thread.tailSequence,
          tailDigest: thread.tailDigest,
          records: thread.records,
        });
      }),
  );

  const inspectTail: ThreadStore["Service"]["inspectTail"] = Effect.fn(
    "MemoryThreadStore.inspectTail",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ThreadTailRequest, "inspectTail", unvalidated);

      const thread = yield* Ref.get(state).pipe(
        Effect.flatMap((current) => findThread(current, request.threadId)),
      );

      return ThreadTail.make({
        threadId: request.threadId,
        tailSequence: thread.tailSequence,
        tailDigest: thread.tailDigest,
        producerEpoch: thread.producerEpoch,
      });
    }),
  );

  const saveCheckpoint: ThreadCheckpoints["save"] = Effect.fn("MemoryThreadStore.saveCheckpoint")(
    (unvalidated) =>
      Effect.gen(function* () {
        yield* validateCheckpointVersion(unvalidated);
        const request = yield* validate(SaveCheckpointRequest, "saveCheckpoint", unvalidated);

        const decision = yield* Ref.modify(
          state,
          (current): readonly [CheckpointDecision, MemoryState] => {
            const checkpoint = request.checkpoint;
            const thread = current.threads.get(checkpoint.threadId);

            if (thread === undefined) {
              return [
                {
                  _tag: "failure",
                  error: ThreadNotMaterialized.make({
                    threadId: checkpoint.threadId,
                  }),
                },
                current,
              ];
            }
            if (checkpoint.throughSequence > thread.tailSequence) {
              return [
                {
                  _tag: "failure",
                  error: CheckpointRejected.make({
                    threadId: checkpoint.threadId,
                    reason: "ahead-of-tail",
                  }),
                },
                current,
              ];
            }
            if (thread.tailDigests.get(checkpoint.throughSequence) !== checkpoint.tailDigest) {
              return [
                {
                  _tag: "failure",
                  error: CheckpointRejected.make({
                    threadId: checkpoint.threadId,
                    reason: "digest-mismatch",
                  }),
                },
                current,
              ];
            }
            if (
              !thread.checkpoints.has(checkpoint.throughSequence) &&
              thread.checkpoints.size >= MAX_CHECKPOINTS_PER_THREAD
            ) {
              return [
                {
                  _tag: "failure",
                  error: storeError(
                    "saveCheckpoint",
                    `In-memory checkpoint limit ${MAX_CHECKPOINTS_PER_THREAD} exceeded`,
                  ),
                },
                current,
              ];
            }
            const checkpoints = new Map(thread.checkpoints);

            checkpoints.set(checkpoint.throughSequence, checkpoint);
            const threads = new Map(current.threads);

            threads.set(checkpoint.threadId, { ...thread, checkpoints });

            return [{ _tag: "success" }, { threads }];
          },
        );

        if (decision._tag === "failure") return yield* decision.error;
      }),
  );

  const loadCheckpoint: ThreadCheckpoints["load"] = Effect.fn("MemoryThreadStore.loadCheckpoint")(
    (unvalidated) =>
      Effect.gen(function* () {
        const request = yield* validate(LoadCheckpointRequest, "loadCheckpoint", unvalidated);

        const thread = yield* Ref.get(state).pipe(
          Effect.flatMap((current) => findThread(current, request.threadId)),
        );

        const maximum = request.atOrBeforeSequence ?? thread.tailSequence;
        let selected: ThreadCheckpoint | undefined;

        for (const [sequence, checkpoint] of thread.checkpoints) {
          if (
            sequence <= maximum &&
            (selected === undefined || sequence > selected.throughSequence)
          ) {
            selected = checkpoint;
          }
        }
        if (
          selected !== undefined &&
          thread.tailDigests.get(selected.throughSequence) !== selected.tailDigest
        ) {
          return yield* CheckpointRejected.make({
            threadId: request.threadId,
            reason: "digest-mismatch",
          });
        }

        return Option.fromNullishOr(selected);
      }),
  );

  const saveRecoveryCheckpoint: ThreadRecoveryCheckpoints["save"] = Effect.fn(
    "MemoryThreadStore.saveRecoveryCheckpoint",
  )(function* (unvalidated) {
    const request = yield* validate(
      SaveRecoveryCheckpointRequest,
      "saveRecoveryCheckpoint",
      unvalidated,
    );

    const decision = yield* Ref.modify(
      state,
      (
        current,
      ): readonly [
        CheckpointDecision | { readonly _tag: "failure"; readonly error: FenceRejected },
        MemoryState,
      ] => {
        const checkpoint = request.checkpoint;
        const thread = current.threads.get(checkpoint.threadId);

        if (thread === undefined)
          return [
            {
              _tag: "failure",
              error: ThreadNotMaterialized.make({ threadId: checkpoint.threadId }),
            },
            current,
          ];
        if (thread.producerEpoch !== request.producerEpoch)
          return [
            {
              _tag: "failure",
              error: FenceRejected.make({
                threadId: checkpoint.threadId,
                actualEpoch: thread.producerEpoch,
                attemptedEpoch: request.producerEpoch,
              }),
            },
            current,
          ];
        if (checkpoint.throughSequence > thread.tailSequence)
          return [
            {
              _tag: "failure",
              error: CheckpointRejected.make({
                threadId: checkpoint.threadId,
                reason: "ahead-of-tail",
              }),
            },
            current,
          ];
        if (thread.tailDigests.get(checkpoint.throughSequence) !== checkpoint.tailDigest)
          return [
            {
              _tag: "failure",
              error: CheckpointRejected.make({
                threadId: checkpoint.threadId,
                reason: "digest-mismatch",
              }),
            },
            current,
          ];
        if ((thread.recoveryCheckpoint?.throughSequence ?? -1) > checkpoint.throughSequence)
          return [{ _tag: "success" }, current];
        const threads = new Map(current.threads);

        threads.set(checkpoint.threadId, { ...thread, recoveryCheckpoint: checkpoint });

        return [{ _tag: "success" }, { threads }];
      },
    );

    if (decision._tag === "failure") return yield* decision.error;
  });

  const loadRecoveryCheckpoint: ThreadRecoveryCheckpoints["load"] = Effect.fn(
    "MemoryThreadStore.loadRecoveryCheckpoint",
  )(function* (unvalidated) {
    const request = yield* validate(LoadCheckpointRequest, "loadRecoveryCheckpoint", unvalidated);

    const thread = yield* Ref.get(state).pipe(
      Effect.flatMap((current) => findThread(current, request.threadId)),
    );

    const checkpoint = thread.recoveryCheckpoint;

    if (
      checkpoint === undefined ||
      checkpoint.throughSequence > (request.atOrBeforeSequence ?? thread.tailSequence)
    )
      return Option.none();
    if (thread.tailDigests.get(checkpoint.throughSequence) !== checkpoint.tailDigest)
      return yield* CheckpointRejected.make({
        threadId: request.threadId,
        reason: "digest-mismatch",
      });

    return Option.some(checkpoint);
  });

  return ThreadStore.of({
    nativeReads,
    materialize,
    append,
    read,
    observe,
    export: exportThread,
    inspectTail,
    checkpoints: { save: saveCheckpoint, load: loadCheckpoint },
    recoveryCheckpoints: { save: saveRecoveryCheckpoint, load: loadRecoveryCheckpoint },
  });
});

/**
 * In-memory canonical Thread persistence. Durable accepted work is served by the separate
 * SubmissionLedger port; this Layer deliberately provides only the ThreadStore.
 */
export const MemoryThreadStoreLive = Layer.effect(ThreadStore, makeThreadStore);

/** Configure a finite retained Thread capacity. Invalid construction options throw immediately. */
export const memoryThreadStoreLayer = (options: { readonly maxThreads?: number } = {}) =>
  MemoryThreadStoreLive.pipe(
    Layer.provide(
      Layer.succeed(ThreadCapacity)(
        Schema.decodeSync(
          Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(65_536)),
        )(options.maxThreads ?? MAX_THREADS),
      ),
    ),
  );

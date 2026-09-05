import { ThreadId, RunId } from "@effect-agent/core/Identifiers";
import { Clock, Context, DateTime, Effect, Encoding, Layer, Schema, SynchronizedRef } from "effect";
import { Prompt } from "effect/unstable/ai";

/** Bound applied to one text projection before it can enter a model context. */
export const ThreadText = Schema.String.check(Schema.isMaxLength(64 * 1024));
const MAX_THREAD_MESSAGES = 1_024;
const MAX_THREAD_CONTENT_BYTES = 4 * 1024 * 1024;
const MAX_EPHEMERAL_THREADS = 256;
const MAX_EPHEMERAL_CONTENT_BYTES = 64 * 1024 * 1024;

/** One append-only native Effect AI message in an ephemeral thread. */
export class ThreadMessage extends Schema.Class<ThreadMessage>(
  "@effect-agent/capabilities/ThreadMessage",
)({
  threadId: ThreadId,
  sequence: Schema.Natural,
  runId: Schema.optionalKey(RunId),
  message: Prompt.Message,
  encodedBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_THREAD_CONTENT_BYTES)),
  timestamp: Schema.DateTimeUtcFromString,
}) {}

/** Immutable view suitable as exact official Prompt history for a subsequent Run. */
export class ThreadSnapshot extends Schema.Class<ThreadSnapshot>(
  "@effect-agent/capabilities/ThreadSnapshot",
)({
  version: Schema.Literal(1),
  threadId: ThreadId,
  nextSequence: Schema.Natural,
  contentBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_THREAD_CONTENT_BYTES)),
  messages: Schema.Array(ThreadMessage).check(Schema.isMaxLength(MAX_THREAD_MESSAGES)),
}) {}

/** Portable snapshot of ephemeral state. It is not a durable canonical log. */
export class ThreadExport extends Schema.Class<ThreadExport>(
  "@effect-agent/capabilities/ThreadExport",
)({
  format: Schema.Literal("effect-agent/ephemeral-thread@1"),
  exportedAt: Schema.DateTimeUtcFromString,
  snapshot: ThreadSnapshot,
}) {}

/** Native message accepted by the append-only thread. */
export class ThreadAppend extends Schema.Class<ThreadAppend>(
  "@effect-agent/capabilities/ThreadAppend",
)({
  runId: Schema.optionalKey(RunId),
  message: Prompt.Message,
}) {}

/** The requested ephemeral Thread has not been created in this process Scope. */
export class ThreadNotFound extends Schema.TaggedError<ThreadNotFound>()("ThreadNotFound", {
  threadId: ThreadId,
}) {}

/** A process-local Thread store reached an explicit count or encoded-content bound. */
export class ThreadLimitExceeded extends Schema.TaggedError<ThreadLimitExceeded>()(
  "ThreadLimitExceeded",
  {
    threadId: ThreadId,
    limit: Schema.Literals(["messages", "content-bytes", "threads", "store-content-bytes"]),
    limitValue: Schema.Natural,
    observedValue: Schema.Natural,
  },
) {}

/** Engine history was not an append-only extension of the official snapshot. */
export class ThreadHistoryDiverged extends Schema.TaggedError<ThreadHistoryDiverged>()(
  "ThreadHistoryDiverged",
  { threadId: ThreadId, message: Schema.String },
) {}

/** Native Prompt encoding failed before a bounded state mutation. */
export class ThreadEncodingError extends Schema.TaggedError<ThreadEncodingError>()(
  "ThreadEncodingError",
  { threadId: ThreadId, message: Schema.String },
) {}

export type ThreadError =
  | ThreadNotFound
  | ThreadLimitExceeded
  | ThreadHistoryDiverged
  | ThreadEncodingError;

/** Reconstruct exact Effect AI Prompt history, including tools, reasoning, files, and options. */
export const threadPrompt = (snapshot: ThreadSnapshot): Prompt.Prompt =>
  Prompt.fromMessages(snapshot.messages.map((entry) => entry.message));

/**
 * Advanced process-local state for incremental interactive history. Updates remain visible even
 * when a Run later fails; this service has no successful-Run commit boundary or durable recovery.
 * Use ThreadHistory with a memory store for ordinary successful-run retention.
 * Scope closure releases all in-memory state.
 */
export class EphemeralThreads extends Context.Service<
  EphemeralThreads,
  {
    readonly create: (threadId: ThreadId) => Effect.Effect<ThreadSnapshot, ThreadLimitExceeded>;
    readonly append: (
      threadId: ThreadId,
      message: ThreadAppend,
    ) => Effect.Effect<ThreadSnapshot, ThreadNotFound | ThreadLimitExceeded | ThreadEncodingError>;
    /**
     * Record an engine-emitted full history only when it is an append-only
     * extension of the exact official Prompt already stored. The entire
     * suffix commits in one transaction or not at all: concurrent writers are
     * serialized, a writer whose history no longer extends the committed
     * official history fails with ThreadHistoryDiverged, and a limit
     * failure inside the suffix records nothing. Earlier updates remain committed if the Run
     * later fails or is interrupted; this transaction covers one update, not the whole Run.
     */
    readonly recordHistory: (
      threadId: ThreadId,
      runId: RunId,
      history: Prompt.Prompt,
    ) => Effect.Effect<ThreadSnapshot, ThreadError>;
    readonly snapshot: (threadId: ThreadId) => Effect.Effect<ThreadSnapshot, ThreadNotFound>;
    readonly export: (threadId: ThreadId) => Effect.Effect<ThreadExport, ThreadNotFound>;
  }
>()("@effect-agent/capabilities/EphemeralThreads") {}

const utf8Bytes = (value: string): number => Encoding.encodeHex(value).length / 2;

const encodeMessage = (
  threadId: ThreadId,
  message: Prompt.Message,
): Effect.Effect<string, ThreadEncodingError> =>
  Schema.encodeEffect(Prompt.Message)(message).pipe(
    Effect.map((encoded) => JSON.stringify(encoded)),
    Effect.mapError((error) =>
      ThreadEncodingError.make({
        threadId,
        message: `Could not encode native Effect AI message: ${error.message}`,
      }),
    ),
  );

const findSnapshot = (
  state: ReadonlyMap<ThreadId, ThreadSnapshot>,
  threadId: ThreadId,
): Effect.Effect<ThreadSnapshot, ThreadNotFound> => {
  const snapshot = state.get(threadId);

  return snapshot === undefined
    ? Effect.fail(ThreadNotFound.make({ threadId }))
    : Effect.succeed(snapshot);
};

const totalStoreBytes = (threads: ReadonlyMap<ThreadId, ThreadSnapshot>): number => {
  let total = 0;

  for (const snapshot of threads.values()) total += snapshot.contentBytes;

  return total;
};

const appendEncoded = Effect.fn("EphemeralThreads.appendEncoded")(function* (
  threads: ReadonlyMap<ThreadId, ThreadSnapshot>,
  current: ThreadSnapshot,
  append: ThreadAppend,
  encoded: string,
  timestamp: DateTime.Utc,
) {
  const threadId = current.threadId;

  if (current.messages.length >= MAX_THREAD_MESSAGES) {
    return yield* ThreadLimitExceeded.make({
      threadId,
      limit: "messages",
      limitValue: MAX_THREAD_MESSAGES,
      observedValue: current.messages.length + 1,
    });
  }
  const messageBytes = utf8Bytes(encoded);
  const contentBytes = current.contentBytes + messageBytes;

  if (contentBytes > MAX_THREAD_CONTENT_BYTES) {
    return yield* ThreadLimitExceeded.make({
      threadId,
      limit: "content-bytes",
      limitValue: MAX_THREAD_CONTENT_BYTES,
      observedValue: contentBytes,
    });
  }
  const storeBytes = totalStoreBytes(threads) + messageBytes;

  if (storeBytes > MAX_EPHEMERAL_CONTENT_BYTES) {
    return yield* ThreadLimitExceeded.make({
      threadId,
      limit: "store-content-bytes",
      limitValue: MAX_EPHEMERAL_CONTENT_BYTES,
      observedValue: storeBytes,
    });
  }

  const message = ThreadMessage.make({
    threadId,
    sequence: current.nextSequence,
    ...(append.runId === undefined ? {} : { runId: append.runId }),
    message: append.message,
    encodedBytes: messageBytes,
    timestamp,
  });

  const next = ThreadSnapshot.make({
    version: current.version,
    threadId: current.threadId,
    nextSequence: current.nextSequence + 1,
    contentBytes,
    messages: [...current.messages, message],
  });

  return [next, new Map(threads).set(threadId, next)] as const;
});

/** Layer whose state is scoped to the consumer; it intentionally has no persistence semantics. */
export const EphemeralThreadsLive = Layer.effect(
  EphemeralThreads,
  Effect.gen(function* () {
    const state = yield* SynchronizedRef.make<ReadonlyMap<ThreadId, ThreadSnapshot>>(new Map());

    return EphemeralThreads.of({
      create: (threadId) =>
        SynchronizedRef.modifyEffect(
          state,
          Effect.fn(function* (threads) {
            const existing = threads.get(threadId);

            if (existing !== undefined) return [existing, threads] as const;
            if (threads.size >= MAX_EPHEMERAL_THREADS) {
              return yield* ThreadLimitExceeded.make({
                threadId,
                limit: "threads",
                limitValue: MAX_EPHEMERAL_THREADS,
                observedValue: threads.size + 1,
              });
            }

            const created = ThreadSnapshot.make({
              version: 1,
              threadId,
              nextSequence: 0,
              contentBytes: 0,
              messages: [],
            });

            return [created, new Map(threads).set(threadId, created)] as const;
          }),
        ),
      append: Effect.fn("EphemeralThreads.append")(function* (threadId, message) {
        const encoded = yield* encodeMessage(threadId, message.message);
        const timestamp = DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));

        return yield* SynchronizedRef.modifyEffect(
          state,
          Effect.fn(function* (threads) {
            const current = yield* findSnapshot(threads, threadId);

            return yield* appendEncoded(threads, current, message, encoded, timestamp);
          }),
        );
      }),
      recordHistory: Effect.fn("EphemeralThreads.recordHistory")(
        function* (threadId, historyRunId, history) {
          const incoming = yield* Effect.forEach(history.content, (message) =>
            encodeMessage(threadId, message).pipe(Effect.map((encoded) => ({ message, encoded }))),
          );

          // Publish the complete suffix only after verification and every bounded append succeed.
          return yield* SynchronizedRef.modifyEffect(
            state,
            Effect.fn(function* (threads) {
              const current = yield* findSnapshot(threads, threadId);

              const currentEncoded = yield* Effect.forEach(current.messages, (entry) =>
                encodeMessage(threadId, entry.message),
              );

              if (
                incoming.length < currentEncoded.length ||
                currentEncoded.some((encoded, index) => encoded !== incoming[index]?.encoded)
              ) {
                return yield* ThreadHistoryDiverged.make({
                  threadId,
                  message: "Engine history is not an append-only extension of official history",
                });
              }
              const timestamp = DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));

              let snapshot = current;
              let next = threads;

              for (const entry of incoming.slice(currentEncoded.length)) {
                [snapshot, next] = yield* appendEncoded(
                  next,
                  snapshot,
                  ThreadAppend.make({ runId: historyRunId, message: entry.message }),
                  entry.encoded,
                  timestamp,
                );
              }

              return [snapshot, next] as const;
            }),
          );
        },
      ),
      snapshot: (threadId) =>
        SynchronizedRef.get(state).pipe(Effect.flatMap((all) => findSnapshot(all, threadId))),
      export: (threadId) =>
        Effect.gen(function* () {
          const snapshot = yield* findSnapshot(yield* SynchronizedRef.get(state), threadId);

          return ThreadExport.make({
            format: "effect-agent/ephemeral-thread@1",
            exportedAt: DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis)),
            snapshot,
          });
        }),
    });
  }),
);

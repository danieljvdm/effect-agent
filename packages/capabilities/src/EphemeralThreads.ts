import { ThreadId, RunId } from "@effect-agent/core/Identifiers";
import { Clock, Context, DateTime, Effect, Encoding, Layer, Ref, Schema } from "effect";
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

type AppendResult =
  | { readonly _tag: "not-found" }
  | {
      readonly _tag: "failure";
      readonly error: ThreadNotFound | ThreadLimitExceeded | ThreadEncodingError;
    }
  | { readonly _tag: "success"; readonly value: ThreadSnapshot };
type CreateResult =
  | { readonly _tag: "failure"; readonly error: ThreadLimitExceeded }
  | { readonly _tag: "success"; readonly value: ThreadSnapshot };
type RecordHistoryResult =
  | { readonly _tag: "stale" }
  | {
      readonly _tag: "failure";
      readonly error: ThreadNotFound | ThreadLimitExceeded | ThreadEncodingError;
    }
  | { readonly _tag: "success"; readonly value: ThreadSnapshot };

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

const appendEncoded = (
  threads: ReadonlyMap<ThreadId, ThreadSnapshot>,
  threadId: ThreadId,
  append: ThreadAppend,
  encoded: string,
  timestamp: DateTime.Utc,
): readonly [AppendResult, ReadonlyMap<ThreadId, ThreadSnapshot>] => {
  const current = threads.get(threadId);

  if (current === undefined) return [{ _tag: "not-found" }, threads];
  if (current.messages.length >= MAX_THREAD_MESSAGES) {
    return [
      {
        _tag: "failure",
        error: ThreadLimitExceeded.make({
          threadId,
          limit: "messages",
          limitValue: MAX_THREAD_MESSAGES,
          observedValue: current.messages.length + 1,
        }),
      },
      threads,
    ];
  }
  const messageBytes = utf8Bytes(encoded);
  const contentBytes = current.contentBytes + messageBytes;

  if (contentBytes > MAX_THREAD_CONTENT_BYTES) {
    return [
      {
        _tag: "failure",
        error: ThreadLimitExceeded.make({
          threadId,
          limit: "content-bytes",
          limitValue: MAX_THREAD_CONTENT_BYTES,
          observedValue: contentBytes,
        }),
      },
      threads,
    ];
  }
  const storeBytes = totalStoreBytes(threads) + messageBytes;

  if (storeBytes > MAX_EPHEMERAL_CONTENT_BYTES) {
    return [
      {
        _tag: "failure",
        error: ThreadLimitExceeded.make({
          threadId,
          limit: "store-content-bytes",
          limitValue: MAX_EPHEMERAL_CONTENT_BYTES,
          observedValue: storeBytes,
        }),
      },
      threads,
    ];
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

  return [{ _tag: "success", value: next }, new Map(threads).set(threadId, next)];
};

/**
 * Commit a verified engine suffix in one transaction: either every suffix
 * message extends official history or nothing is recorded. Snapshots are
 * immutable and replaced wholesale, so identity against the verified base
 * detects any concurrent commit before a single message can interleave.
 */
const commitHistorySuffix = (
  threads: ReadonlyMap<ThreadId, ThreadSnapshot>,
  threadId: ThreadId,
  base: ThreadSnapshot,
  suffix: ReadonlyArray<{ readonly append: ThreadAppend; readonly encoded: string }>,
  timestamp: DateTime.Utc,
): readonly [RecordHistoryResult, ReadonlyMap<ThreadId, ThreadSnapshot>] => {
  const current = threads.get(threadId);

  if (current === undefined) {
    return [{ _tag: "failure", error: ThreadNotFound.make({ threadId }) }, threads];
  }
  if (current !== base) return [{ _tag: "stale" }, threads];
  let next = threads;
  let snapshot = current;

  for (const entry of suffix) {
    const [result, updated] = appendEncoded(next, threadId, entry.append, entry.encoded, timestamp);

    if (result._tag === "not-found") {
      return [{ _tag: "failure", error: ThreadNotFound.make({ threadId }) }, threads];
    }
    if (result._tag === "failure") {
      return [{ _tag: "failure", error: result.error }, threads];
    }
    snapshot = result.value;
    next = updated;
  }

  return [{ _tag: "success", value: snapshot }, next];
};

/** Layer whose state is scoped to the consumer; it intentionally has no persistence semantics. */
export const EphemeralThreadsLive = Layer.effect(
  EphemeralThreads,
  Effect.gen(function* () {
    const state = yield* Ref.make<ReadonlyMap<ThreadId, ThreadSnapshot>>(new Map());

    const append = (
      threadId: ThreadId,
      message: ThreadAppend,
    ): Effect.Effect<ThreadSnapshot, ThreadNotFound | ThreadLimitExceeded | ThreadEncodingError> =>
      Effect.gen(function* () {
        const encoded = yield* encodeMessage(threadId, message.message);
        const timestamp = DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));

        const result = yield* Ref.modify(state, (threads) =>
          appendEncoded(threads, threadId, message, encoded, timestamp),
        );

        if (result._tag === "not-found") {
          return yield* ThreadNotFound.make({ threadId });
        }
        if (result._tag === "failure") return yield* result.error;

        return result.value;
      });

    return EphemeralThreads.of({
      create: (threadId) =>
        Effect.gen(function* () {
          const result = yield* Ref.modify(
            state,
            (threads): readonly [CreateResult, ReadonlyMap<ThreadId, ThreadSnapshot>] => {
              const existing = threads.get(threadId);

              if (existing !== undefined) {
                return [{ _tag: "success" as const, value: existing }, threads] as const;
              }
              if (threads.size >= MAX_EPHEMERAL_THREADS) {
                return [
                  {
                    _tag: "failure" as const,
                    error: ThreadLimitExceeded.make({
                      threadId,
                      limit: "threads",
                      limitValue: MAX_EPHEMERAL_THREADS,
                      observedValue: threads.size + 1,
                    }),
                  },
                  threads,
                ] as const;
              }

              const created = ThreadSnapshot.make({
                version: 1,
                threadId,
                nextSequence: 0,
                contentBytes: 0,
                messages: [],
              });

              return [
                { _tag: "success" as const, value: created },
                new Map(threads).set(threadId, created),
              ] as const;
            },
          );

          if (result._tag === "failure") return yield* result.error;

          return result.value;
        }),
      append,
      recordHistory: (threadId, historyRunId, history) =>
        Effect.gen(function* () {
          const incoming = yield* Effect.forEach(history.content, (message) =>
            encodeMessage(threadId, message).pipe(Effect.map((encoded) => ({ message, encoded }))),
          );

          const attempt: Effect.Effect<ThreadSnapshot, ThreadError> = Effect.gen(function* () {
            const current = yield* Ref.get(state).pipe(
              Effect.flatMap((all) => findSnapshot(all, threadId)),
            );

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

            const suffix = incoming.slice(currentEncoded.length).map((entry) => ({
              append: ThreadAppend.make({ runId: historyRunId, message: entry.message }),
              encoded: entry.encoded,
            }));

            const result = yield* Ref.modify(state, (threads) =>
              commitHistorySuffix(threads, threadId, current, suffix, timestamp),
            );

            // Another writer committed after verification: re-verify against the new base.
            if (result._tag === "stale") return yield* attempt;
            if (result._tag === "failure") return yield* result.error;

            return result.value;
          });

          return yield* attempt;
        }),
      snapshot: (threadId) =>
        Ref.get(state).pipe(Effect.flatMap((all) => findSnapshot(all, threadId))),
      export: (threadId) =>
        Effect.gen(function* () {
          const snapshot = yield* findSnapshot(yield* Ref.get(state), threadId);

          return ThreadExport.make({
            format: "effect-agent/ephemeral-thread@1",
            exportedAt: DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis)),
            snapshot,
          });
        }),
    });
  }),
);

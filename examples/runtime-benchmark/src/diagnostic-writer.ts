import { hrtime } from "node:process";
import { DatabaseSync } from "node:sqlite";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Effect, FileSystem, Schema } from "effect";

/** Private subprocess protocol. The observer never releases a lock on its own event loop. */
export const WriterOptions = Schema.Struct({
  filename: Schema.String,
  directory: Schema.String,
  holdMs: Schema.Literals([0, 25, 100]),
});

const Millis = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));

/** Both processes use the same Node executable and host/kernel monotonic clock, never performance.now. */
export const nodeMonotonicNanos = Effect.sync(() => hrtime.bigint());

export const WriterResult = Schema.Struct({
  clock: Schema.Literal("node-hrtime-same-host"),
  acquiredNanos: Schema.BigIntFromString,
  releaseStartedNanos: Schema.BigIntFromString,
  releasedNanos: Schema.BigIntFromString,
  heldMs: Millis,
  lockMs: Millis,
}).check(
  Schema.makeFilter(
    (result) =>
      result.acquiredNanos <= result.releaseStartedNanos &&
      result.releaseStartedNanos <= result.releasedNanos,
  ),
);

/** Conservative overlap: the lock is definitely held between these two boundary readings. */
export const writerOverlap = (
  writer: typeof WriterResult.Type,
  observer: { readonly startedNanos: bigint; readonly finishedNanos: bigint },
) => {
  const start =
    writer.acquiredNanos > observer.startedNanos ? writer.acquiredNanos : observer.startedNanos;

  const end =
    writer.releaseStartedNanos < observer.finishedNanos
      ? writer.releaseStartedNanos
      : observer.finishedNanos;

  return {
    overlapMs: end > start ? Number(end - start) / 1e6 : 0,
    heldAtStart:
      writer.acquiredNanos <= observer.startedNanos &&
      observer.startedNanos < writer.releaseStartedNanos,
    finishedWhileHeld:
      writer.acquiredNanos < observer.finishedNanos &&
      observer.finishedNanos < writer.releaseStartedNanos,
  };
};

class WriterError extends Schema.TaggedError<WriterError>()("WriterError", {
  cause: Schema.Defect(),
}) {}

export const runWriter = Effect.fn("diagnostic.writer")(function* (
  options: typeof WriterOptions.Type,
) {
  const fs = yield* FileSystem.FileSystem;

  // Foreign SQLite boundary: this process owns the connection and the entire transaction.
  const database = yield* Effect.acquireRelease(
    Effect.try({
      try: () => new DatabaseSync(options.filename),
      catch: (cause) => WriterError.make({ cause }),
    }),
    (connection) => Effect.sync(() => connection.close()),
  );

  let acquired = 0n;
  let started = 0n;
  let releaseStarted = 0n;
  let released = 0n;

  yield* Effect.gen(function* () {
    yield* Effect.acquireRelease(
      Effect.try({
        try: () => database.exec("BEGIN IMMEDIATE"),
        catch: (cause) => WriterError.make({ cause }),
      }),
      () =>
        Effect.sync(() => {
          releaseStarted = hrtime.bigint();
          database.exec("ROLLBACK");
          released = hrtime.bigint();
        }),
    );
    acquired = yield* nodeMonotonicNanos;
    yield* fs.writeFileString(`${options.directory}/ready`, "ready");
    while (!(yield* fs.exists(`${options.directory}/go`))) yield* Effect.sleep("1 millis");
    started = yield* nodeMonotonicNanos;
    yield* Effect.sleep(options.holdMs);
  }).pipe(Effect.scoped);
  // heldMs excludes readiness/gating; lockMs includes that setup. Both end after rollback.
  const heldMs = Number(released - started) / 1e6;
  const lockMs = Number(released - acquired) / 1e6;

  yield* fs.writeFileString(
    `${options.directory}/result.json`,
    yield* Schema.encodeEffect(Schema.fromJsonString(WriterResult))({
      clock: "node-hrtime-same-host",
      acquiredNanos: acquired,
      releaseStartedNanos: releaseStarted,
      releasedNanos: released,
      heldMs,
      lockMs,
    }),
  );
});

if (import.meta.main)
  NodeRuntime.runMain(
    Effect.gen(function* () {
      const options = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WriterOptions))(
        yield* Config.string("RUNTIME_DIAGNOSTIC_WRITER"),
      );

      yield* runWriter(options);
    }).pipe(Effect.scoped, Effect.timeout("10 seconds"), Effect.provide(NodeServices.layer)),
  );

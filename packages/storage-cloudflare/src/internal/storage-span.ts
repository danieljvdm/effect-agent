import { Effect, Predicate, Result } from "effect";

type Tagged = { readonly _tag: string; readonly reason?: unknown };

export const isAppendContention = (error: Tagged): boolean =>
  error._tag === "DoFenceRejected" ||
  error._tag === "FenceRejected" ||
  ((error._tag === "DoAppendConflict" || error._tag === "AppendConflict") &&
    error.reason === "tail");

/** Expected refusals remain typed at the port, but are outcomes of the storage operation. */
export const storageResult = <A, E extends Tagged, R>(
  effect: Effect.Effect<A, E, R>,
  expected: (error: E) => boolean,
) =>
  effect.pipe(
    Effect.map(Result.succeed),
    Effect.catchIf(expected, (error) => Effect.succeed(Result.fail(error))),
  );

/** Only error discriminants enter telemetry; messages, SQL, identities and payloads do not. */
export const annotateStorageError = (error: Tagged) =>
  Effect.annotateCurrentSpan({
    "error.type": error._tag,
    ...(Predicate.hasProperty(error, "cause") &&
    Predicate.hasProperty(error.cause, "_tag") &&
    typeof error.cause._tag === "string"
      ? { "error.cause.type": error.cause._tag }
      : {}),
  });

export const withStorageSpan =
  (name: string, expected: (error: Tagged) => boolean) =>
  <A, E extends Tagged, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    storageResult(effect, expected).pipe(
      Effect.tap((result) =>
        Result.isFailure(result)
          ? Effect.annotateCurrentSpan({ "storage.outcome": result.failure._tag })
          : Effect.void,
      ),
      Effect.tapError(annotateStorageError),
      Effect.withSpan(name),
      Effect.flatMap(Effect.fromResult),
    );

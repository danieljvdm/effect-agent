import { Effect } from "effect";

/** Late acquisition can finish after the owner's Scope has already closed. */
export const makeCleanup = Effect.fn("makeCleanup")(function* (
  closeSession: (sessionId: string) => Effect.Effect<void>,
) {
  let sessionId: string | undefined;

  const terminate = Effect.gen(function* () {
    if (sessionId === undefined) return "unconfirmed";
    yield* closeSession(sessionId);

    return "confirmed";
  });

  const close = yield* Effect.cached(terminate);

  return {
    close,
    acquired: Effect.fn("acquired")(function* (id: string) {
      sessionId = id;

      return yield* terminate;
    }),
  };
});

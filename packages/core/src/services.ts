import { Context, Crypto, Effect, Layer, Schema } from "effect";

import { ConversationId, RunId, TurnId } from "./identifiers.ts";

/** Replaceable authority for creating runtime identities, including deterministic test IDs. */
export class IdGenerator extends Context.Service<
  IdGenerator,
  {
    /** Create the identity for a new conversation. */
    readonly nextConversationId: Effect.Effect<ConversationId>;
    /** Create the identity for a new run. */
    readonly nextRunId: Effect.Effect<RunId>;
    /** Create the identity for a new model turn. */
    readonly nextTurnId: Effect.Effect<TurnId>;
  }
>()("@effect-agent/core/IdGenerator") {
  /**
   * Default identity authority derived from Effect's platform-neutral Crypto
   * port. Composition roots provide the host Crypto implementation; tests
   * that need deterministic identities may replace either port.
   */
  static readonly layer: Layer.Layer<IdGenerator, never, Crypto.Crypto> = Layer.effect(
    IdGenerator,
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const randomUuid = crypto.randomUUIDv4.pipe(Effect.orDie);
      return {
        nextConversationId: randomUuid.pipe(
          Effect.map((uuid) => Schema.decodeSync(ConversationId)(`conversation-${uuid}`)),
        ),
        nextRunId: randomUuid.pipe(Effect.map((uuid) => Schema.decodeSync(RunId)(`run-${uuid}`))),
        nextTurnId: randomUuid.pipe(
          Effect.map((uuid) => Schema.decodeSync(TurnId)(`turn-${uuid}`)),
        ),
      };
    }),
  );
}

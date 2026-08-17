import { Context, Crypto, Effect, Layer, PlatformError, Schema } from "effect";

import { ConversationId, RunId, TurnId } from "./identifiers.ts";

/** The host Crypto authority could not produce a runtime identity. */
export class IdGenerationError extends Schema.TaggedError<IdGenerationError>()(
  "IdGenerationError",
  {
    operation: Schema.Literal("randomUUIDv4"),
    reasonTag: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
    message: Schema.String.check(Schema.isMaxLength(1_000)),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

const idGenerationError = (error: PlatformError.PlatformError): IdGenerationError =>
  IdGenerationError.make({
    operation: "randomUUIDv4",
    reasonTag: error.reason._tag,
    message: error.message.slice(0, 1_000),
    cause: error,
  });

/** Replaceable authority for creating runtime identities, including deterministic test IDs. */
export class IdGenerator extends Context.Service<
  IdGenerator,
  {
    /** Create the identity for a new conversation. */
    readonly nextConversationId: Effect.Effect<ConversationId, IdGenerationError>;
    /** Create the identity for a new run. */
    readonly nextRunId: Effect.Effect<RunId, IdGenerationError>;
    /** Create the identity for a new model turn. */
    readonly nextTurnId: Effect.Effect<TurnId, IdGenerationError>;
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
      const randomUuid = crypto.randomUUIDv4.pipe(Effect.mapError(idGenerationError));
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

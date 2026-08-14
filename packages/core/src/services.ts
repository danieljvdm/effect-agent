import { Context, Effect, Layer, Schema } from "effect";

import { ConversationId, RunId, TurnId } from "./identifiers.ts";

/**
 * Web Crypto is a standard runtime global on every supported platform
 * (Node.js >= 20, edge workers, browsers). Core compiles without platform
 * type libraries, so the one standard member used here is declared locally
 * instead of importing DOM or Node types.
 */
declare const crypto: { readonly randomUUID: () => string };

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
   * Default identity authority backed by Web Crypto's `randomUUID`, which is
   * available without a platform import on Node.js >= 20, edge workers, and
   * browsers. Tests that need deterministic identities replace this Layer.
   */
  static readonly layer: Layer.Layer<IdGenerator> = Layer.succeed(IdGenerator, {
    nextConversationId: Effect.sync(() => `conversation-${crypto.randomUUID()}`).pipe(
      Effect.map(Schema.decodeSync(ConversationId)),
    ),
    nextRunId: Effect.sync(() => `run-${crypto.randomUUID()}`).pipe(
      Effect.map(Schema.decodeSync(RunId)),
    ),
    nextTurnId: Effect.sync(() => `turn-${crypto.randomUUID()}`).pipe(
      Effect.map(Schema.decodeSync(TurnId)),
    ),
  });
}

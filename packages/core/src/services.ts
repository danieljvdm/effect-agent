import { Context, Effect, Layer, Schema } from "effect";
import { IdGenerator as EffectAiIdGenerator } from "effect/unstable/ai";

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
   * Default identity authority backed by Effect AI's built-in generator. Its
   * Effect Random dependency is fiber-local and can be seeded in tests, so core
   * neither reaches into a runtime global nor selects a platform adapter.
   */
  static readonly layer: Layer.Layer<IdGenerator> = Layer.succeed(IdGenerator, {
    nextConversationId: EffectAiIdGenerator.defaultIdGenerator
      .generateId()
      .pipe(Effect.map((id) => Schema.decodeSync(ConversationId)(`conversation-${id}`))),
    nextRunId: EffectAiIdGenerator.defaultIdGenerator
      .generateId()
      .pipe(Effect.map((id) => Schema.decodeSync(RunId)(`run-${id}`))),
    nextTurnId: EffectAiIdGenerator.defaultIdGenerator
      .generateId()
      .pipe(Effect.map((id) => Schema.decodeSync(TurnId)(`turn-${id}`))),
  });
}

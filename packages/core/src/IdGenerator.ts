import { Context, Effect, Layer, Schema } from "effect";
import { IdGenerator as EffectAiIdGenerator } from "effect/unstable/ai";

import { ThreadId, RunId, TurnId } from "./Identifiers.ts";

/** Replaceable authority for creating runtime identities, including deterministic test IDs. */
export class IdGenerator extends Context.Service<
  IdGenerator,
  {
    /** Create the identity for a new thread. */
    readonly nextThreadId: Effect.Effect<ThreadId>;
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
    nextThreadId: EffectAiIdGenerator.defaultIdGenerator
      .generateId()
      .pipe(Effect.map((id) => Schema.decodeSync(ThreadId)(`thread-${id}`))),
    nextRunId: EffectAiIdGenerator.defaultIdGenerator
      .generateId()
      .pipe(Effect.map((id) => Schema.decodeSync(RunId)(`run-${id}`))),
    nextTurnId: EffectAiIdGenerator.defaultIdGenerator
      .generateId()
      .pipe(Effect.map((id) => Schema.decodeSync(TurnId)(`turn-${id}`))),
  });
}

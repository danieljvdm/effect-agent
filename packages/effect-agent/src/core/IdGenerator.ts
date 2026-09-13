import { Context, Effect, Layer, Schema } from "effect";
import { IdGenerator as EffectAiIdGenerator } from "effect/unstable/ai";

import { ThreadId, RunId, TurnId } from "./Identifiers.ts";

/** Replaceable authority for creating runtime identities, including deterministic test IDs. */
export interface Service {
  /** Create the identity for a new thread. */
  readonly nextThreadId: Effect.Effect<ThreadId>;
  /** Create the identity for a new run. */
  readonly nextRunId: Effect.Effect<RunId>;
  /** Create the identity for a new model turn. */
  readonly nextTurnId: Effect.Effect<TurnId>;
}

const defaultGenerator: Service = {
  nextThreadId: EffectAiIdGenerator.defaultIdGenerator
    .generateId()
    .pipe(Effect.map((id) => Schema.decodeSync(ThreadId)(`thread-${id}`))),
  nextRunId: EffectAiIdGenerator.defaultIdGenerator
    .generateId()
    .pipe(Effect.map((id) => Schema.decodeSync(RunId)(`run-${id}`))),
  nextTurnId: EffectAiIdGenerator.defaultIdGenerator
    .generateId()
    .pipe(Effect.map((id) => Schema.decodeSync(TurnId)(`turn-${id}`))),
};

/**
 * Runtime identities default to Effect AI's generator without requiring a Layer.
 * Randomness is fiber-local and can be seeded in tests. Override this reference
 * with Layer.succeed/Layer.effect or Effect.provideService for custom identities.
 * The default holds only Effects: it allocates no mutable state or resources.
 */
export const IdGenerator = Context.Reference<Service>("@effect-agent/core/IdGenerator", {
  defaultValue: () => defaultGenerator,
});

/** Explicitly select the default generator, replacing any enclosing override. */
export const layer: Layer.Layer<never> = Layer.succeed(IdGenerator, defaultGenerator);

import { Effect, Layer, Schema } from "effect";
import { IdGenerator } from "effect-agent/id-generator";
import { RunId, ThreadId, TurnId } from "effect-agent/identifiers";
import { IdGenerator as EffectAiIdGenerator } from "effect/unstable/ai";

/** Supply the same identity authority to current code and releases predating default IDs. */
export const BenchmarkIdsLive = Layer.succeed(IdGenerator, {
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

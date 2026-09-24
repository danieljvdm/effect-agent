import { Context, Effect, Schema, SchemaGetter } from "effect";
import type { Tool } from "effect/unstable/ai";
import { Toolkit } from "effect/unstable/ai";

import * as Agent from "../../src/core/Agent.ts";
import * as AgentUpdates from "../../src/core/AgentUpdates.ts";
import { IdempotencyKey } from "../../src/core/Receipt.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<A extends true> = A;
class Encoder extends Context.Service<Encoder, string>()("update-types/Encoder") {}
class Decoder extends Context.Service<Decoder, string>()("update-types/Decoder") {}

const finding = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformEffect((value) => Effect.as(Decoder, value)),
    encode: SchemaGetter.transformEffect((value) => Effect.as(Encoder, value)),
  }),
);

const updates = Schema.TaggedStruct("Finding", { finding });

const researcher = Agent.make("researcher", {
  input: Schema.String,
  updates,
  output: Schema.String,
  instructions: "Research and report useful findings.",
  toolkit: Toolkit.empty,
});

const plain = Agent.make("plain", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer.",
  toolkit: Toolkit.empty,
});

const contextualDefinition = <A extends Agent.AnyDefinition>(definition: A) => definition;

const contextualPlain = contextualDefinition(
  Agent.make("contextual-plain", {
    input: Schema.String,
    output: Schema.String,
    instructions: () => Decoder,
    toolkit: Toolkit.empty,
  }),
);

const contextualProofs: [
  Assert<Equal<Agent.Update<typeof contextualPlain>, never>>,
  Assert<Equal<Agent.DefinitionRequirements<typeof contextualPlain>, Decoder>>,
] = [true, true];

const emitted = AgentUpdates.emit(
  researcher,
  { _tag: "Finding", finding: "Rosebank" },
  {
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("finding-1"),
  },
);

const decode = (update: AgentUpdates.Update) => AgentUpdates.decode(researcher, update);

const proofs: [
  Assert<Equal<Agent.Update<typeof researcher>, typeof updates.Type>>,
  Assert<Equal<Agent.Update<typeof plain>, never>>,
  Assert<
    Equal<
      Tool.Parameters<typeof researcher.toolkit.tools.emit_update>,
      { readonly value: typeof updates.Type }
    >
  >,
  Assert<Equal<Agent.DefinitionRequirements<typeof researcher>, Encoder | Decoder>>,
  Assert<Equal<Effect.Services<typeof emitted>, AgentUpdates.Emitter | Encoder>>,
  Assert<Equal<Effect.Error<typeof emitted>, AgentUpdates.UpdateError>>,
  Assert<Equal<Effect.Success<ReturnType<typeof decode>>, typeof updates.Type>>,
  Assert<Equal<Effect.Services<ReturnType<typeof decode>>, Decoder>>,
] = [true, true, true, true, true, true, true, true];

const wrongValue = () => {
  // @ts-expect-error The update has its own schema, independent of final output.
  return AgentUpdates.emit(researcher, "final answer", {
    idempotencyKey: Schema.decodeSync(IdempotencyKey)("wrong"),
  });
};

export const verifyPreservesUpdateCodecsAndSuppliesTheNativeUpdateHandlerInternally = () => {
  void proofs.every(Boolean);
  void contextualProofs.every(Boolean);
  void typeof wrongValue;
};

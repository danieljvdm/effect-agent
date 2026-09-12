import * as Agent from "@effect-agent/core/Agent";
import { NodeCrypto } from "@effect/platform-node";
import { expect, expectTypeOf, it } from "@effect/vitest";
import { type Crypto, Effect, Layer, Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";

import { compileRegistrations } from "../src/AgentRegistration.ts";
import { digestDefinitions, type DigestError } from "../src/Digest.ts";
import { DefinitionDigestInput } from "../src/Records.ts";

const definitions = DefinitionDigestInput.make({ agent: "v1", model: "v1", tools: [] });

const model = Layer.effectContext<Agent.ModelServices, never, never>(
  Effect.die("Registration must not acquire a model"),
);

const agent = <Updates extends Schema.Top>(updates: Updates) =>
  Agent.make("updating-agent", {
    input: Schema.String,
    output: Schema.String,
    instructions: "Answer",
    toolkit: Toolkit.empty,
    updates,
  });

it.effect("pins update wire contracts while preserving registrations without updates", () =>
  Effect.gen(function* () {
    const legacyAgent = Agent.make("legacy-agent", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Answer",
      toolkit: Toolkit.empty,
    });

    const legacy = yield* compileRegistrations([{ agent: legacyAgent, model, definitions }]);

    expect(legacy[0]?.digests).toEqual(yield* digestDefinitions(definitions));

    const first = yield* compileRegistrations([
      { agent: agent(Schema.Struct({ finding: Schema.String })), model, definitions },
    ]);

    const changed = yield* compileRegistrations([
      { agent: agent(Schema.Struct({ finding: Schema.Number })), model, definitions },
    ]);

    expect(first[0]?.digests.agent).not.toEqual(changed[0]?.digests.agent);
    expect(first[0]?.digests.model).toEqual(changed[0]?.digests.model);
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

it("keeps update registration errors and requirements inferred", () => {
  const registration = compileRegistrations([{ agent: agent(Schema.String), model, definitions }]);

  expectTypeOf<Effect.Error<typeof registration>>().toEqualTypeOf<DigestError>();
  expectTypeOf<Effect.Services<typeof registration>>().toEqualTypeOf<Crypto.Crypto>();
});

import { NodeCrypto } from "@effect/platform-node";
import { expect, expectTypeOf, it } from "@effect/vitest";
import { type Crypto, Effect, Layer, Schema } from "effect";
import * as Agent from "effect-agent/agent";
import * as Subagent from "effect-agent/subagent";
import { Toolkit } from "effect/unstable/ai";

import { compileRegistrations } from "../../src/durable/AgentRegistration.ts";
import { digestDefinitions, type DigestError } from "../../src/durable/Digest.ts";
import { DefinitionDigestInput } from "../../src/durable/Records.ts";

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

    const expected = yield* digestDefinitions(definitions);

    expect(legacy[0]?.digests).toMatchObject({
      ...expected,
      replay: { agent: expected.agent, tools: {} },
    });

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

it.effect(
  "preserves the predecessor fingerprint and captured descriptor for standard reports",
  () =>
    Effect.gen(function* () {
      const child = Agent.make("child", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Research",
        toolkit: Toolkit.empty,
      });

      const declaration = Subagent.make("research", { target: child });
      const background = Subagent.background(declaration, { list: true, reportToParent: true });

      const parent = Agent.make("parent", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Delegate",
        toolkit: background.toolkit,
      });

      const entry = DefinitionDigestInput.make({
        agent: "v1",
        model: "v1",
        tools: ["research_list"],
      });

      const registered = yield* compileRegistrations([
        { agent: parent, model, definitions: entry },
      ]).pipe(Effect.provide(background.layer));

      const predecessor = yield* digestDefinitions(
        DefinitionDigestInput.make({
          ...entry,
          agent: {
            declaration: "v1",
            backgroundReporting: [
              {
                delegationId: "research",
                targetAgentId: "child",
                mode: "standard",
                destinationDelegationId: null,
              },
            ],
          },
        }),
      );

      expect(registered[0]?.digests).toMatchObject(predecessor);
      expect(registered[0]?.digests.replay?.agent).toBe(predecessor.agent);
      expect(registered[0]?.reporting).toHaveLength(1);
      expect(registered[0]?.reporting?.[0]?.target).toBe(child);
    }).pipe(Effect.provide(NodeCrypto.layer)),
);

it("keeps update registration errors and requirements inferred", () => {
  const registration = compileRegistrations([{ agent: agent(Schema.String), model, definitions }]);

  expectTypeOf<Effect.Error<typeof registration>>().toEqualTypeOf<DigestError>();
  expectTypeOf<Effect.Services<typeof registration>>().toEqualTypeOf<Crypto.Crypto>();
});

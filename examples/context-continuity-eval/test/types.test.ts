import * as ContextTools from "@effect-agent/capabilities/ContextTools";
import * as MemoryNotes from "@effect-agent/capabilities/MemoryNotes";
import * as Agent from "@effect-agent/core/Agent";
import { type IdGenerator } from "@effect-agent/core/IdGenerator";
import { MemoryKey, type MemoryReader, type MemoryWriter } from "@effect-agent/core/MemoryStore";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { type ContextHistory } from "@effect-agent/engine/ContextHistory";
import { type ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import type { OpenAiClient } from "@effect/ai-openai";
import { OpenAiLanguageModel } from "@effect/ai-openai";
import { Effect, Ref } from "effect";
import { type IdGenerator as AiIdGenerator } from "effect/unstable/ai";
import { expectTypeOf, it } from "vite-plus/test";

import { cloudflareDefinition, cloudflareModelSettings } from "../src/cloudflare-contracts.ts";
import { notesNamespace } from "../src/host-evidence.ts";
import { makeLiveClient } from "../src/live-model.ts";
import { manifestLayer } from "../src/pressure.ts";

it("preserves native model, history and durable note requirements in the pressure composition", () => {
  const notes = MemoryNotes.layer({
    key: MemoryKey.make({
      namespace: notesNamespace.make({ threadId: "type-test" }),
      id: "working-notes",
    }),
    locator: "memory://type-test",
    attributions: [
      {
        originId: "type-test",
        speaker: "Agent",
        observers: [],
        locator: "memory://type-test",
        activityAt: null,
        interpretation: "private notes",
      },
    ],
    scopes: [],
  });

  const run = AgentRuntime.run(
    Agent.withModel(
      cloudflareDefinition,
      OpenAiLanguageModel.model("gpt-6-astra", cloudflareModelSettings),
    ),
    "continue",
  ).pipe(
    Effect.provide(ContextTools.layer),
    Effect.provide(manifestLayer(16_000)),
    Effect.provide(notes),
  );

  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
    | OpenAiClient.OpenAiClient
    | ContextHistory
    | MemoryReader
    | MemoryWriter
    | AiIdGenerator.IdGenerator
    | IdGenerator
    | ThreadHistory
  >();
  expectTypeOf<Effect.Error<typeof run>>().toEqualTypeOf<
    AgentRuntime.AgentRuntimeFailure<typeof cloudflareDefinition>
  >();

  const client = Effect.gen(function* () {
    return yield* makeLiveClient({
      model: "gpt-6-astra",
      maxCostMicrousd: 10_000_000,
      phase: yield* Ref.make(0),
      audit: () => Effect.void,
    });
  });

  expectTypeOf<Effect.Services<typeof client>>().toEqualTypeOf<OpenAiClient.OpenAiClient>();
  expectTypeOf<Effect.Error<typeof client>>().toEqualTypeOf<never>();
});

import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Redacted } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import type { Models } from "./agents.ts";

/** Resolve credentials at host acquisition; only model identity enters registration digests. */
export const openAiModels = Effect.gen(function* () {
  const apiKey = yield* Config.nonEmptyString("OPENAI_API_KEY").pipe(Config.map(Redacted.make));

  const name = yield* Config.nonEmptyString("OPENAI_MODEL").pipe(
    Config.withDefault("gpt-4.1-mini"),
  );

  const config = { store: false, max_output_tokens: 1024 } as const;
  const client = OpenAiClient.layer({ apiKey }).pipe(Layer.provide(FetchHttpClient.layer));
  const model = OpenAiLanguageModel.model(name, config).pipe(Layer.provide(client));

  return {
    models: {
      scout: model,
      builderA: model,
      builderB: model,
      coordinator: model,
      advisor: model,
    } satisfies Models,
    modelVersion: JSON.stringify({ provider: "openai", name, ...config }),
  };
});

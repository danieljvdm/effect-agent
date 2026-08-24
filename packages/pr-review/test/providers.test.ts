import type { OpenAiSchema } from "@effect/ai-openai";
import { OpenAiClient } from "@effect/ai-openai";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Ref } from "effect";
import { LanguageModel } from "effect/unstable/ai";
import { HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { describeReviewModel, EFFORT_ALIASES, makeOpenAiReviewModel } from "../src/index.ts";

const makeResponse = (): OpenAiSchema.Response => ({
  id: "resp_test123",
  object: "response",
  created_at: 0,
  model: "gpt-5.6-sol",
  output: [],
  usage: null,
  error: null,
  incomplete_details: null,
});

describe("makeOpenAiReviewModel", () => {
  it.effect("applies reasoning headroom and Fast mode to OpenAI requests", () =>
    Effect.gen(function* () {
      const requests = yield* Ref.make<ReadonlyArray<typeof OpenAiSchema.CreateResponse.Encoded>>(
        [],
      );
      const request = HttpClientRequest.post("https://api.openai.com/v1/responses");
      const response = HttpClientResponse.fromWeb(request, new Response(null, { status: 200 }));
      const clientLayer = Layer.succeed(
        OpenAiClient.OpenAiClient,
        OpenAiClient.OpenAiClient.of({
          client: undefined as never,
          createResponse: (options) =>
            Ref.update(requests, (captured) => [...captured, options]).pipe(
              Effect.as([makeResponse(), response] as const),
            ),
          createResponseStream: () => Effect.die(new Error("unexpected streaming request")),
          createEmbedding: () => Effect.die(new Error("unexpected embedding request")),
        }),
      );

      yield* LanguageModel.generateText({ prompt: "Review this change." }).pipe(
        Effect.provide(
          makeOpenAiReviewModel(undefined, EFFORT_ALIASES.high, "fast").pipe(
            Layer.provide(clientLayer),
          ),
        ),
      );

      const [captured] = yield* Ref.get(requests);
      expect(captured?.max_output_tokens).toBe(32_000);
      expect(captured?.reasoning).toEqual({ effort: "high" });
      expect(captured?.service_tier).toBe("fast");
    }),
  );

  it("includes Fast mode in the fingerprint-bearing model description", () => {
    expect(describeReviewModel("openai", undefined, EFFORT_ALIASES.high, "fast")).toBe(
      "openai/gpt-5.6-sol (effort high, service tier fast)",
    );
  });
});

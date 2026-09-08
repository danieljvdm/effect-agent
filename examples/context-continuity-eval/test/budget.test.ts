import { OpenAiClient } from "@effect/ai-openai";
import { Effect, Exit, Layer, Redacted, Ref } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { expect, it } from "vite-plus/test";

import { type ModelUsage } from "../src/contracts.ts";
import { makeLiveClient } from "../src/live-model.ts";
import { RequestAuditSink } from "../src/request-audit.ts";

it.each(["ceiling", "remaining", "unresolved"] as const)(
  "refuses inference and retry for %s spending",
  async (mode) => {
    let preflights = 0;
    let inferences = 0;

    const http = HttpClient.make((request) => {
      if (!request.url.endsWith("/input_tokens")) {
        inferences++;

        return Effect.die("No inference should be dispatched");
      }
      preflights++;

      return Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          Response.json({ object: "response.input_tokens", input_tokens: 16_000 }),
        ),
      );
    });

    const initial: ModelUsage = {
      calls: 30,
      completedCalls: mode === "unresolved" ? 29 : 30,
      inputTokens: 100_000,
      outputTokens: 100_000,
      maxInputTokens: 16_000,
      estimatedCostMicrousd: 9_900_000,
      reservedCostMicrousd: mode === "unresolved" ? 100_000 : 0,
      returnedModels: ["gpt-6-astra"],
    };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const phase = yield* Ref.make(4);

        const live = yield* makeLiveClient({
          model: "gpt-6-astra",
          maxCostMicrousd: mode === "ceiling" ? 10_000_001 : 10_000_000,
          initialUsage: initial,
          phase,
        });

        const request = {
          model: "gpt-6-astra",
          store: false,
          service_tier: "default",
          max_output_tokens: 4096,
          input: "hello",
        } as const;

        const first = yield* live.client.createResponseStream(request).pipe(Effect.exit);
        const second = yield* live.client.createResponseStream(request).pipe(Effect.exit);

        return { first, second, usage: yield* live.snapshot };
      }).pipe(
        Effect.provide(
          Layer.merge(
            Layer.succeed(RequestAuditSink, RequestAuditSink.of({ write: () => Effect.void })),
            OpenAiClient.layer({ apiKey: Redacted.make("test") }).pipe(
              Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
            ),
          ),
        ),
      ),
    );

    expect(Exit.isFailure(result.first)).toBe(true);
    expect(Exit.isFailure(result.second)).toBe(true);
    expect(inferences).toBe(0);
    expect(preflights).toBe(mode === "remaining" ? 1 : 0);
    expect(result.usage).toEqual(initial);
  },
);

import { it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Redacted } from "effect";
import { TestClock } from "effect/testing";
import { FetchHttpClient } from "effect/unstable/http";
import { expect } from "vite-plus/test";

import { validateOpenAiKey } from "../src/server/credentials.ts";

const secret = Redacted.make("sk-fixture-validation-PRIVATE");

it.effect("releases the validation request and refuses provider redirects", () =>
  Effect.gen(function* () {
    for (const status of [200, 302, 401, 429]) {
      const signals: AbortSignal[] = [];

      const result = yield* validateOpenAiKey(secret).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.Fetch, async (url, init) => {
          expect(url instanceof Request ? url.url : url.toString()).toBe(
            "https://api.openai.com/v1/models",
          );
          expect(init?.redirect).toBe("manual");
          if (init?.signal) signals.push(init.signal);

          return new Response("PRIVATE provider details", {
            status,
            headers: { location: "https://untrusted.test" },
          });
        }),
        Effect.result,
      );

      expect(result._tag).toBe(status === 200 ? "Success" : "Failure");
      expect(JSON.stringify(result)).not.toContain("PRIVATE");
      expect(signals).toHaveLength(1);
      expect(signals[0]?.aborted).toBe(true);
    }
  }),
);

it.effect("times out and cancels validation without retaining a provider error or credential", () =>
  Effect.gen(function* () {
    for (const mode of ["timeout", "interrupt"] as const) {
      const started = yield* Deferred.make<void>();
      const signals: AbortSignal[] = [];

      const operation = validateOpenAiKey(secret).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.Fetch, async (_url, init) => {
          if (init?.signal) signals.push(init.signal);
          Deferred.doneUnsafe(started, Effect.void);

          return new Promise<Response>((_resolve, reject) =>
            init?.signal?.addEventListener("abort", () => reject(new Error("PRIVATE"))),
          );
        }),
      );

      const fiber = yield* operation.pipe(Effect.forkChild);

      yield* Deferred.await(started);
      if (mode === "timeout") {
        yield* TestClock.adjust("16 seconds");
        const result = yield* Fiber.join(fiber).pipe(Effect.result);

        expect(result._tag).toBe("Failure");
        expect(JSON.stringify(result)).not.toContain("PRIVATE");
      } else yield* Fiber.interrupt(fiber);
      expect(signals[0]?.aborted).toBe(true);
    }
  }),
);

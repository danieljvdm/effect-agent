import { describe, expect, it } from "vite-plus/test";

import { ConfigProvider, Effect } from "effect";

import { runOpenAiDemoOnServer } from "./run-openai.server";

describe("OpenAI demo server runner", () => {
  it("fails before network access when its server credential is absent", async () => {
    const outcome = await Effect.runPromise(
      runOpenAiDemoOnServer({ request: "Plan a review-only London trip." }).pipe(
        Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
        Effect.match({
          onFailure: (error) => ({ _tag: "Failure" as const, error }),
          onSuccess: () => ({ _tag: "Success" as const }),
        }),
      ),
    );

    expect(outcome._tag).toBe("Failure");
    if (outcome._tag === "Failure") {
      expect(outcome.error._tag).toBe("ConfigError");
      expect(outcome.error.message).toContain("OPENAI_API_KEY");
    }
  });
});

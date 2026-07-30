import { describe, expect, it } from "vite-plus/test";

import { ConfigProvider, Effect, Layer } from "effect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";

import { DemoRunRpcServerLayer } from "./run-rpc.server";

describe("OpenAI demo RPC server", () => {
  it("fails before network access when its server credential is absent", async () => {
    const outcome = await Effect.runPromise(
      Layer.build(DemoRunRpcServerLayer.pipe(Layer.provide(HttpRouter.layer))).pipe(
        Effect.scoped,
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

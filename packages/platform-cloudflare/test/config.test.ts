import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CloudflarePlatformConfigError,
  cloudflareDurableRuntimeConfigFromOptions,
} from "../src/index.ts";

const baseOptions = {
  deploymentId: "cf-config-test",
  producerPrefix: "cf-config-producer",
} as const;

describe("Cloudflare durable runtime database plan", () => {
  it("defaults conservatively to the Free-plan cap", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const config = yield* cloudflareDurableRuntimeConfigFromOptions(baseOptions);
        expect(config.databasePlan).toBe("free");
        expect(config.limits.maxDatabaseBytes).toBe(900_000_000);
      }),
    );
  });

  it("rejects a Free-plan ceiling above the real deployment cap", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const failure = yield* cloudflareDurableRuntimeConfigFromOptions({
          ...baseOptions,
          databasePlan: "free",
          maxDatabaseBytes: 1_000_000_001,
        }).pipe(Effect.flip);
        expect(failure).toBeInstanceOf(CloudflarePlatformConfigError);
        expect(failure.message).toContain("exceeds the free plan cap 1000000000");
      }),
    );
  });

  it("accepts the explicit Paid-plan default under its 10 GB cap", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const config = yield* cloudflareDurableRuntimeConfigFromOptions({
          ...baseOptions,
          databasePlan: "paid",
        });
        expect(config.databasePlan).toBe("paid");
        expect(config.limits.maxDatabaseBytes).toBe(9_000_000_000);
      }),
    );
  });
});

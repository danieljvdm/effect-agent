import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";

/**
 * WP0 probe 3: `@effect/vitest`'s `it.effect` (which provides the TestClock
 * and TestConsole environment) works inside the workers pool. The shared
 * conformance suites drive lease expiry through `TestClock`, so this is the
 * path WP1's in-workerd conformance runs depend on.
 */
describe("@effect/vitest it.effect inside the workers pool (WP0 probe 3)", () => {
  it.effect("provides a TestClock that drives virtual time inside workerd", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(Effect.as(Effect.sleep(Duration.seconds(30)), "woke"));
      yield* TestClock.adjust("31 seconds");
      const outcome = yield* Fiber.join(fiber);
      expect(outcome).toBe("woke");
    }),
  );
});

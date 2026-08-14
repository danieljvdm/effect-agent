import { Duration, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

/**
 * WP0 probe 3 fallback: `TestClock.layer()` is constructible standalone inside
 * workerd, without `@effect/vitest`. This is the manual path the conformance
 * runs would use if `it.effect` ever regresses inside the pool.
 */
describe("manual TestClock.layer() inside the workers pool (WP0 probe 3 fallback)", () => {
  it("drives virtual time through an explicitly provided TestClock layer", async () => {
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          Effect.as(Effect.sleep(Duration.seconds(30)), "woke"),
        );
        yield* TestClock.adjust("31 seconds");
        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(TestClock.layer())),
    );

    expect(outcome).toBe("woke");
  });
});

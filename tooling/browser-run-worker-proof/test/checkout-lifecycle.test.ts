import { assert, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Ref } from "effect";
import { TestClock } from "effect/testing";

import { withRetirement } from "../src/checkout-lifecycle.ts";

it.effect(
  "retires a partially provisioned stage on success, failure, defect, timeout and interruption without replay",
  () =>
    Effect.gen(function* () {
      for (const outcome of ["success", "failure", "defect", "timeout", "interruption"] as const) {
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const entered = yield* Deferred.make<void>();

        const provisionAndRun = Effect.gen(function* () {
          yield* Ref.update(events, (all) => [...all, "resource-created", "dispatch"]);
          yield* Deferred.succeed(entered, undefined);
          if (outcome === "failure") return yield* Effect.fail("expected rejection");
          if (outcome === "defect") return yield* Effect.die("unexpected defect");
          if (outcome === "timeout" || outcome === "interruption") return yield* Effect.never;
        });

        const fiber = yield* withRetirement(
          outcome === "timeout"
            ? provisionAndRun.pipe(Effect.timeout("1 second"))
            : provisionAndRun,
          Ref.update(events, (all) => [...all, "session-closed", "stage-destroyed"]),
        ).pipe(Effect.forkChild);

        yield* Deferred.await(entered);
        if (outcome === "timeout") yield* TestClock.adjust("1 second");
        if (outcome === "interruption") yield* Fiber.interrupt(fiber);
        const result = yield* Fiber.await(fiber);

        assert.strictEqual(Exit.isSuccess(result), outcome === "success");
        assert.deepStrictEqual(yield* Ref.get(events), [
          "resource-created",
          "dispatch",
          "session-closed",
          "stage-destroyed",
        ]);
      }
    }),
);

it.effect("fails the gate when retirement fails even after a successful purchase", () =>
  Effect.gen(function* () {
    const result = yield* withRetirement(
      Effect.succeed("purchase passed"),
      Effect.fail("session closure unconfirmed"),
    ).pipe(Effect.exit);

    assert.isTrue(Exit.isFailure(result));
    if (Exit.isFailure(result))
      assert.include(Cause.pretty(result.cause), "session closure unconfirmed");
  }),
);

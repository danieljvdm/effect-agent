import { describe, expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Option } from "effect";
import { TestClock } from "effect/testing";

import {
  ThreadProjectionError,
  ThreadProjectionMaintenance,
  drainDue,
} from "../src/ThreadProjectionMaintenance.ts";

describe("bounded Thread projection maintenance", () => {
  it.effect("runs one due batch and never postpones or loops over backlog", () =>
    Effect.gen(function* () {
      let deadline: Option.Option<number> = Option.none();
      let batches = 0;

      const service = ThreadProjectionMaintenance.of({
        applyCommitted: () => Effect.void,
        pendingDeadline: Effect.sync(() => deadline),
        drain: Effect.sync(() => {
          batches++;
        }),
      });

      const run = drainDue.pipe(Effect.provideService(ThreadProjectionMaintenance, service));

      yield* TestClock.setTime(1_000);
      expect(yield* run).toBe(false);
      deadline = Option.some(1_001);
      expect(yield* run).toBe(false);
      yield* TestClock.adjust(1);
      expect(yield* run).toBe(true);
      expect(batches).toBe(1);
      expect(yield* run).toBe(true);
      expect(batches).toBe(2);
    }),
  );

  it.effect.each(["failure", "defect", "interruption", "timeout"] as const)(
    "preserves %s and releases scoped batch resources",
    (mode) =>
      Effect.gen(function* () {
        let released = false;
        const failure = ThreadProjectionError.make({ operation: "drain", message: "retry" });

        const drain = Effect.gen(function* () {
          yield* Effect.acquireRelease(Effect.void, () =>
            Effect.sync(() => {
              released = true;
            }),
          );
          if (mode === "failure") return yield* failure;
          if (mode === "defect") return yield* Effect.die("projection defect");
          if (mode === "interruption") return yield* Effect.interrupt;

          return yield* Effect.never.pipe(
            Effect.timeoutOrElse({
              duration: 0,
              orElse: () => failure,
            }),
          );
        }).pipe(Effect.scoped);

        const exit = yield* drainDue.pipe(
          Effect.provideService(ThreadProjectionMaintenance, {
            applyCommitted: () => Effect.void,
            pendingDeadline: Effect.succeed(Option.some(0)),
            drain,
          }),
          Effect.exit,
        );

        expect(released).toBe(true);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          if (mode === "interruption") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
          else if (mode === "defect") expect(Cause.hasDies(exit.cause)).toBe(true);
          else expect(Cause.findErrorOption(exit.cause)).toEqual(Option.some(failure));
        }
      }),
  );
});

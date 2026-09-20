import { NodeFileSystem } from "@effect/platform-node";
import { assert, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, FileSystem, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";

import { policy, Report } from "../src/checkout-contract.ts";
import { retirementPlan, withRetirement, writeReportSnapshot } from "../src/checkout-lifecycle.ts";

it("requires closure evidence before retiring an absent owner", () => {
  for (const [status, cleanup, closed, expected] of [
    [200, "pending", [false], "close"],
    [200, "browsers-closed", [false], "close"],
    [404, "pending", [true, true], "destroy"],
    [404, "pending", [true, false], "blocked"],
    [404, "failed", [true, false], "blocked"],
    [404, "browsers-closed", [false], "destroy"],
    [404, "confirmed", [false], "destroy"],
    [404, "pending", [], "destroy"],
    [403, "confirmed", [true], "blocked"],
    [500, "browsers-closed", [true], "blocked"],
  ] as const)
    assert.strictEqual(retirementPlan(status, cleanup, closed), expected);
});

it.effect("report publication survives faults around each filesystem mutation", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped();

    const previous = Report.make({
      version: 1,
      model: "previous-model",
      sourceCommit: "test",
      dirty: false,
      repetitions: 1,
      profile: "automated",
      bindingProof: true,
      suiteFailure: null,
      configuration: policy,
      results: [],
      completed: 0,
      attempted: 0,
      completionRate: 0,
      cleanup: "pending",
      providerCompatibility: "not-established",
    });

    const next = Report.make({ ...previous, model: "next-model", cleanup: "browsers-closed" });

    for (const point of ["before-write", "after-write", "before-rename", "after-rename"]) {
      const path = `${directory}/${point}.json`;

      yield* writeReportSnapshot(path, previous);

      const result = yield* writeReportSnapshot(path, next).pipe(
        Effect.provideService(
          FileSystem.FileSystem,
          FileSystem.FileSystem.of({
            ...fs,
            writeFileString: (target, content, options) =>
              Effect.gen(function* () {
                if (point === "before-write") return yield* Effect.die(point);
                yield* fs.writeFileString(target, content, options);
                if (point === "after-write") return yield* Effect.die(point);
              }),
            rename: (source, target) =>
              Effect.gen(function* () {
                if (point === "before-rename") return yield* Effect.die(point);
                yield* fs.rename(source, target);
                if (point === "after-rename") return yield* Effect.die(point);
              }),
          }),
        ),
        Effect.exit,
      );

      assert.isTrue(Exit.isFailure(result));

      const saved = yield* fs
        .readFileString(path)
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Report))));

      assert.deepStrictEqual(saved, point === "after-rename" ? next : previous);
      yield* writeReportSnapshot(path, next);
      assert.isFalse(yield* fs.exists(`${path}.tmp`));
      assert.strictEqual(
        yield* fs.readFileString(path),
        yield* Schema.encodeEffect(Schema.fromJsonString(Report))(next),
      );
    }
  }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
);

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

import { NodeFileSystem } from "@effect/platform-node";
import { assert, it } from "@effect/vitest";
import { Effect, Exit, Fiber, FileSystem, Schema } from "effect";
import { TestClock } from "effect/testing";

import { policy, Report } from "../src/checkout-contract.ts";
import { CheckoutReport, makeCaseAdmission, withRetirement } from "../src/checkout-lifecycle.ts";

const emptyReport = Report.make({
  version: 1,
  model: "test-model",
  sourceCommit: "test",
  dirty: false,
  repetitions: 2,
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

it.effect(
  "serializes concurrent report changes without losing failures or racing publication",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const path = `${directory}/report.json`;

      yield* Effect.gen(function* () {
        const reports = yield* CheckoutReport;

        yield* reports.update(() => emptyReport);
        yield* Effect.forEach(
          Array.from({ length: 2 }, (_, index) => index),
          (index) =>
            reports.update((previous) => {
              if (previous === undefined) throw new Error("Report must be initialized");
              const passed = index !== 1;
              const completed = previous.completed + Number(passed);
              const attempted = previous.attempted + 1;

              return {
                ...previous,
                attempted,
                completed,
                completionRate: completed / attempted,
                results: [
                  ...previous.results,
                  {
                    key: `case-${index}`,
                    flow: "embedded-card",
                    scenario: "success",
                    passed,
                    failure: passed ? null : "declined",
                    evidence: null,
                  },
                ],
              };
            }),
          { concurrency: 2 },
        );

        const saved = yield* fs
          .readFileString(path)
          .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Report))));

        assert.strictEqual(saved.attempted, 2);
        assert.strictEqual(saved.completed, 1);
        assert.strictEqual(new Set(saved.results.map((result) => result.key)).size, 2);
        assert.strictEqual(
          saved.results.find((result) => result.key === "case-1")?.failure,
          "declined",
        );
        assert.deepStrictEqual(yield* reports.get, saved);
      }).pipe(
        Effect.provide(CheckoutReport.layer(path)),
        Effect.provideService(
          FileSystem.FileSystem,
          FileSystem.FileSystem.of({
            ...fs,
            writeFileString: (target, content, options) =>
              Effect.gen(function* () {
                yield* Effect.yieldNow;
                yield* fs.writeFileString(target, content, options);
              }),
          }),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
);

it.effect("joins interrupted concurrent cases before retiring the shared stage", () =>
  Effect.gen(function* () {
    {
      const started: Array<number> = [];
      const closed: Array<number> = [];
      let retired = false;

      const admit = yield* makeCaseAdmission(1_000);

      const batch = Effect.forEach(
        [0, 1, 2],
        (index) =>
          Effect.gen(function* () {
            yield* admit;
            started.push(index);

            return yield* Effect.never;
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (started.includes(index)) closed.push(index);
              }),
            ),
          ),
        { concurrency: 2 },
      );

      const fiber = yield* withRetirement(
        batch,
        Effect.sync(() => {
          assert.deepStrictEqual([...closed].sort(), [0, 1]);
          retired = true;
        }),
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust("1 second");
      assert.deepStrictEqual(started, [0, 1]);
      yield* Fiber.interrupt(fiber);

      assert.isTrue(Exit.isFailure(yield* Fiber.await(fiber)));
      assert.isTrue(retired);
      assert.deepStrictEqual(started, [0, 1]);
    }
  }),
);

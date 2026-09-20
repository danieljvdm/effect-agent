import { NodeFileSystem } from "@effect/platform-node";
import { assert, it } from "@effect/vitest";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, FileSystem, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";

import { policy, Report } from "../src/checkout-contract.ts";
import {
  CheckoutReport,
  retirementPlan,
  runCheckoutCases,
  withRetirement,
  writeReportSnapshot,
} from "../src/checkout-lifecycle.ts";

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
      let writers = 0;
      let peakWriters = 0;

      yield* Effect.gen(function* () {
        const reports = yield* CheckoutReport;

        yield* reports.update(() => emptyReport);
        yield* Effect.forEach(
          Array.from({ length: 12 }, (_, index) => index),
          (index) =>
            reports.update((previous) => {
              if (previous === undefined) throw new Error("Report must be initialized");
              const passed = index !== 7;
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
          { concurrency: 4 },
        );

        const saved = yield* fs
          .readFileString(path)
          .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Report))));

        assert.strictEqual(peakWriters, 1);
        assert.strictEqual(saved.attempted, 12);
        assert.strictEqual(saved.completed, 11);
        assert.strictEqual(new Set(saved.results.map((result) => result.key)).size, 12);
        assert.strictEqual(
          saved.results.find((result) => result.key === "case-7")?.failure,
          "declined",
        );
        assert.deepStrictEqual(yield* reports.get, saved);
        assert.isFalse(yield* fs.exists(`${path}.tmp`));
      }).pipe(
        Effect.provide(CheckoutReport.layer(path)),
        Effect.provideService(
          FileSystem.FileSystem,
          FileSystem.FileSystem.of({
            ...fs,
            writeFileString: (target, content, options) =>
              Effect.gen(function* () {
                writers++;
                peakWriters = Math.max(peakWriters, writers);
                yield* Effect.yieldNow;
                yield* fs.writeFileString(target, content, options);
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    writers--;
                  }),
                ),
              ),
          }),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
);

it.effect("admits four overlapping cases with paced starts and retains an expected failure", () =>
  Effect.gen(function* () {
    const starts: Array<readonly [number, number]> = [];
    const closed: Array<number> = [];
    let active = 0;
    let peak = 0;
    let retired = false;

    const fiber = yield* withRetirement(
      runCheckoutCases(
        [0, 1, 2, 3, 4],
        (index) =>
          Effect.gen(function* () {
            starts.push([index, yield* Clock.currentTimeMillis]);
            active++;
            peak = Math.max(peak, active);
            yield* Effect.sleep("10 seconds");
            if (index === 1) return yield* Effect.fail("expected decline");

            return index;
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                active--;
                closed.push(index);
              }),
            ),
            Effect.exit,
          ),
        { concurrency: 4, startIntervalMillis: 1_000 },
      ),
      Effect.sync(() => {
        assert.strictEqual(active, 0);
        retired = true;
      }),
    ).pipe(Effect.forkChild);

    yield* TestClock.adjust("3 seconds");
    assert.deepStrictEqual(starts, [
      [0, 0],
      [1, 1_000],
      [2, 2_000],
      [3, 3_000],
    ]);
    assert.strictEqual(active, 4);
    yield* TestClock.adjust("6 seconds");
    assert.strictEqual(starts.length, 4);
    assert.isFalse(retired);
    yield* TestClock.adjust("11 seconds");
    const results = yield* Fiber.join(fiber);

    assert.strictEqual(peak, 4);
    assert.deepStrictEqual(starts[4], [4, 10_000]);
    assert.deepStrictEqual(results.map(Exit.isSuccess), [true, false, true, true, true]);
    assert.deepStrictEqual(closed, [0, 1, 2, 3, 4]);
    assert.isTrue(retired);
  }),
);

it.effect("joins interrupted concurrent cases before retiring the shared stage", () =>
  Effect.gen(function* () {
    for (const outcome of ["failure", "defect", "timeout", "interruption"] as const) {
      const release = yield* Deferred.make<void>();
      const started: Array<number> = [];
      const closed: Array<number> = [];
      let retired = false;

      const batch = runCheckoutCases(
        [0, 1, 2, 3, 4],
        (index) =>
          Effect.gen(function* () {
            started.push(index);
            yield* Deferred.await(release);
            if (index === 0 && outcome === "failure") return yield* Effect.fail("broken report");
            if (index === 0 && outcome === "defect") return yield* Effect.die("broken runtime");

            return yield* Effect.never;
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                closed.push(index);
              }),
            ),
          ),
        { concurrency: 4, startIntervalMillis: 1_000 },
      );

      const fiber = yield* withRetirement(
        outcome === "timeout" ? batch.pipe(Effect.timeout("4 seconds")) : batch,
        Effect.sync(() => {
          assert.deepStrictEqual([...closed].sort(), [0, 1, 2, 3]);
          retired = true;
        }),
      ).pipe(Effect.forkChild);

      yield* TestClock.adjust("3 seconds");
      assert.deepStrictEqual(started, [0, 1, 2, 3]);
      if (outcome === "interruption") yield* Fiber.interrupt(fiber);
      else if (outcome === "timeout") yield* TestClock.adjust("1 second");
      else yield* Deferred.succeed(release, undefined);

      assert.isTrue(Exit.isFailure(yield* Fiber.await(fiber)));
      assert.isTrue(retired);
      assert.deepStrictEqual(started, [0, 1, 2, 3]);
    }
  }),
);

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

import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Deferred, Effect, Exit, Fiber, FileSystem, Layer, Schema } from "effect";
import { TestClock } from "effect/testing";
import { expect, it } from "vite-plus/test";

import {
  compareDiagnostics,
  DiagnosticReport,
  renderDiagnosticReport,
} from "../../../scripts/runtime-diagnostics.ts";
import { BenchmarkError } from "../src/contracts.ts";
import {
  completeDiagnosticBatch,
  DIAGNOSTIC_SIZES,
  DiagnosticProgress,
  DiagnosticWorkerReport,
  MAX_DIAGNOSTIC_MARKS,
  type DiagnosticWorkerOptions,
} from "../src/diagnostic-contracts.ts";
import { DiagnosticLedgerSeeds } from "../src/diagnostic-ledger.ts";
import {
  diagnosticCases,
  DiagnosticRunner,
  runDiagnosticWorker,
} from "../src/diagnostic-worker.ts";

const services = DiagnosticLedgerSeeds.layer.pipe(
  Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
);

const passed = Effect.gen(function* () {
  const progress = yield* DiagnosticProgress;

  yield* progress.phase("operation");
  yield* progress.mark({ name: "work", elapsedMs: 0 });
  yield* progress.phase("verification");

  return { totalMs: 0, metrics: [], counters: [] };
});

it("runs every case, alternates case order, and rejects incomplete result matrices", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();

      const options: DiagnosticWorkerOptions = {
        output: `${directory}/worker.json`,
        warmups: 1,
        samples: 1,
        timeoutMs: 1_000,
      };

      yield* runDiagnosticWorker(options).pipe(
        Effect.provideService(DiagnosticRunner, { run: () => passed }),
      );

      const report = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(DiagnosticWorkerReport),
      )(yield* fs.readFileString(options.output));

      expect(DIAGNOSTIC_SIZES).toEqual({ cohorts: 2, warmups: 2, samples: 5 });
      expect(completeDiagnosticBatch(report, options, diagnosticCases)).toBe(true);
      expect(report.samples.slice(0, diagnosticCases.length).map(({ case: name }) => name)).toEqual(
        diagnosticCases.map(({ name }) => name),
      );
      expect(report.samples.slice(diagnosticCases.length).map(({ case: name }) => name)).toEqual(
        [...diagnosticCases].reverse().map(({ name }) => name),
      );
      expect(
        completeDiagnosticBatch(
          { ...report, samples: report.samples.slice(1) },
          options,
          diagnosticCases,
        ),
      ).toBe(false);
      expect(
        completeDiagnosticBatch(
          { ...report, samples: [...report.samples.slice(1), report.samples[1]!] },
          options,
          diagnosticCases,
        ),
      ).toBe(false);
      expect(
        completeDiagnosticBatch(
          { ...report, samples: report.samples.map((sample) => ({ ...sample, result: null })) },
          options,
          diagnosticCases,
        ),
      ).toBe(false);
      expect(
        completeDiagnosticBatch(
          {
            ...report,
            samples: report.samples.map((sample) => ({ ...sample, phase: "operation" })),
          },
          options,
          diagnosticCases,
        ),
      ).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it.each(["failure", "defect", "timeout"] as const)(
  "retains a sample %s and its finalized phase before later successes",
  async (kind) => {
    let closed = false;

    await Effect.runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped();
        const entered = yield* Deferred.make<void>();

        const options: DiagnosticWorkerOptions = {
          output: `${directory}/worker.json`,
          warmups: 0,
          samples: 1,
          timeoutMs: 10,
        };

        const fiber = yield* runDiagnosticWorker(options).pipe(
          Effect.provideService(DiagnosticRunner, {
            run: (workload) =>
              workload.name !== diagnosticCases[0]!.name
                ? passed
                : Effect.gen(function* () {
                    const progress = yield* DiagnosticProgress;

                    yield* progress.phase("operation");
                    yield* Effect.acquireRelease(Effect.void, () =>
                      Effect.sync(() => {
                        closed = true;
                      }),
                    );
                    yield* progress.mark({ name: "entered", elapsedMs: 0 });
                    yield* Deferred.succeed(entered, undefined);
                    if (kind === "failure")
                      return yield* BenchmarkError.make({ message: "expected fixture failure" });
                    if (kind === "defect") return yield* Effect.die("fixture defect");

                    return yield* Effect.never;
                  }).pipe(Effect.scoped),
          }),
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        if (kind === "timeout") yield* TestClock.adjust(options.timeoutMs);
        const result = yield* Fiber.await(fiber);

        const report = yield* Schema.decodeUnknownEffect(
          Schema.fromJsonString(DiagnosticWorkerReport),
        )(yield* fs.readFileString(options.output));

        expect(Exit.isFailure(result)).toBe(true);
        expect(closed).toBe(true);
        expect(report.samples[0]).toMatchObject({
          status: "failed",
          phase: "operation",
          result: null,
          marks: [{ name: "entered", elapsedMs: 0 }],
        });
        expect(report.samples[0]?.failure).toContain(
          kind === "failure"
            ? "expected fixture failure"
            : kind === "defect"
              ? "fixture defect"
              : "TimeoutError",
        );
        expect(report.samples).toHaveLength(diagnosticCases.length);
        expect(report.samples.slice(1).map(({ status }) => status)).toEqual(
          diagnosticCases.slice(1).map(() => "passed"),
        );
        expect(completeDiagnosticBatch(report, options, diagnosticCases)).toBe(false);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(services, TestClock.layer()))),
    );
  },
);

it("retains interrupted sample evidence and closes resources", async () => {
  let closed = false;

  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const entered = yield* Deferred.make<void>();

      const options: DiagnosticWorkerOptions = {
        output: `${directory}/worker.json`,
        warmups: 0,
        samples: 1,
        timeoutMs: 1_000,
      };

      const fiber = yield* Effect.forkChild(
        runDiagnosticWorker(options).pipe(
          Effect.provideService(DiagnosticRunner, {
            run: () =>
              Effect.gen(function* () {
                const progress = yield* DiagnosticProgress;

                yield* progress.phase("operation");
                yield* Effect.acquireRelease(Effect.void, () =>
                  Effect.sync(() => {
                    closed = true;
                  }),
                );
                yield* progress.mark({ name: "waiting", elapsedMs: 0 });
                yield* Deferred.succeed(entered, undefined);

                return yield* Effect.never;
              }).pipe(Effect.scoped),
          }),
        ),
      );

      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);

      const report = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(DiagnosticWorkerReport),
      )(yield* fs.readFileString(options.output));

      expect(closed).toBe(true);
      expect(report.failure).not.toBeNull();
      expect(report.samples).toHaveLength(1);
      expect(report.samples[0]?.marks).toEqual([{ name: "waiting", elapsedMs: 0 }]);
      expect(report.samples[0]?.status).toBe("failed");
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it("bounds phase marks and preserves the accepted prefix", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();

      const options: DiagnosticWorkerOptions = {
        output: `${directory}/worker.json`,
        warmups: 0,
        samples: 1,
        timeoutMs: 1_000,
      };

      yield* runDiagnosticWorker(options).pipe(
        Effect.provideService(DiagnosticRunner, {
          run: (workload) =>
            workload.name !== diagnosticCases[0]!.name
              ? passed
              : Effect.gen(function* () {
                  const progress = yield* DiagnosticProgress;

                  for (let index = 0; index <= MAX_DIAGNOSTIC_MARKS; index++)
                    yield* progress.mark({ name: `mark-${index}`, elapsedMs: index });

                  return { totalMs: 0, metrics: [], counters: [] };
                }),
        }),
        Effect.exit,
      );

      const report = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(DiagnosticWorkerReport),
      )(yield* fs.readFileString(options.output));

      expect(report.samples[0]?.status).toBe("failed");
      expect(report.samples[0]?.marks).toHaveLength(MAX_DIAGNOSTIC_MARKS);
      expect(report.samples[0]?.marks.at(-1)?.name).toBe(`mark-${MAX_DIAGNOSTIC_MARKS - 1}`);
      expect(report.samples[0]?.failure).toContain("mark limit exceeded");
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it("retains controller setup failure and refuses to overwrite the evidence", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();

      const options = {
        root: directory,
        base: directory,
        output: `${directory}/report`,
        requireClean: false,
      };

      yield* compareDiagnostics(options).pipe(Effect.exit);
      const text = yield* fs.readFileString(`${options.output}/report.json`);

      const report = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(DiagnosticReport))(
        text,
      );

      expect(report.phase).toBe("setup");
      expect(report.failure).toContain("require clean");
      expect(report.batches).toEqual([]);
      expect(renderDiagnosticReport(report)).toContain("0/4 complete");
      const second = yield* compareDiagnostics(options).pipe(Effect.exit);

      expect(Exit.isFailure(second)).toBe(true);
      expect(yield* fs.readFileString(`${options.output}/report.json`)).toBe(text);
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

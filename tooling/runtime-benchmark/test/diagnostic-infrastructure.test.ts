import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Deferred, Effect, Exit, Fiber, FileSystem, Layer, Schema } from "effect";
import { expect, it } from "vite-plus/test";

import { diagnosticCases } from "../src/diagnostic-cases.ts";
import {
  completeDiagnosticBatch,
  DiagnosticProgress,
  DiagnosticWorkerOptions,
  DiagnosticWorkerReport,
} from "../src/diagnostic-contracts.ts";
import { DiagnosticLedgerSeeds } from "../src/diagnostic-ledger.ts";
import { DiagnosticRunner, runDiagnosticWorker } from "../src/diagnostic-worker.ts";
import { BenchmarkIdsLive } from "../src/ids.ts";

const services = DiagnosticLedgerSeeds.layer.pipe(
  Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
);

it("completes selected workloads in fixture order regardless of request order", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();

      const options = Schema.decodeSync(DiagnosticWorkerOptions)({
        output: `${directory}/worker.json`,
        cases: ["history-single", "history-unchanged"],
        warmups: 0,
        samples: 1,
        timeoutMs: 1_000,
      });

      const exit = yield* Effect.exit(runDiagnosticWorker(options));

      const report = yield* Schema.decodeEffect(Schema.fromJsonString(DiagnosticWorkerReport))(
        yield* fs.readFileString(options.output),
      );

      expect(report.cases).toEqual(["history-unchanged", "history-single"]);
      expect(report.samples.map(({ status }) => status)).toEqual(["passed", "passed"]);
      expect(Exit.isSuccess(exit)).toBe(true);
      const workloads = diagnosticCases.filter(({ name }) => report.cases?.includes(name));

      expect(
        completeDiagnosticBatch(
          report,
          { ...options, cases: ["history-single", "history-single"] },
          workloads,
        ),
      ).toBe(false);
      expect(
        completeDiagnosticBatch(
          { ...report, cases: ["history-single", "history-unchanged"] },
          options,
          workloads,
        ),
      ).toBe(false);
      expect(
        completeDiagnosticBatch(
          { ...report, samples: report.samples.slice(1) },
          options,
          workloads,
        ),
      ).toBe(false);
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.mergeAll(services, DiagnosticRunner.layer, BenchmarkIdsLive)),
    ),
  );
});

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

      const report = yield* Schema.decodeEffect(Schema.fromJsonString(DiagnosticWorkerReport))(
        yield* fs.readFileString(options.output),
      );

      expect(closed).toBe(true);
      expect(report.failure).not.toBeNull();
      expect(report.samples).toHaveLength(1);
      expect(report.samples[0]?.status).toBe("failed");
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

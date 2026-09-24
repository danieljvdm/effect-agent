import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Deferred, Effect, Fiber, FileSystem, Layer, Schema } from "effect";
import { expect, it } from "vite-plus/test";

import {
  DiagnosticProgress,
  DiagnosticWorkerReport,
  type DiagnosticWorkerOptions,
} from "../src/diagnostic-contracts.ts";
import { DiagnosticLedgerSeeds } from "../src/diagnostic-ledger.ts";
import { DiagnosticRunner, runDiagnosticWorker } from "../src/diagnostic-worker.ts";

const services = DiagnosticLedgerSeeds.layer.pipe(
  Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
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

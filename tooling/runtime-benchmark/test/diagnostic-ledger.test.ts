import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { expect, it } from "vite-plus/test";

import { DiagnosticProgress, type DiagnosticMark } from "../src/diagnostic-contracts.ts";
import {
  DiagnosticLedgerSeeds,
  DiagnosticLedgerWaitForWriterRelease,
  ledgerCases,
  runLedgerCase,
} from "../src/diagnostic-ledger.ts";

const services = DiagnosticLedgerSeeds.layer.pipe(
  Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
);

const silent = DiagnosticProgress.of({ phase: () => Effect.void, mark: () => Effect.void });

it("retains a successful sample with an explicitly missed writer window", async () => {
  const workload = ledgerCases.find(({ name }) => name === "ledger-replay-writer-25")!;

  const result = await Effect.runPromise(
    runLedgerCase(workload).pipe(
      Effect.provideService(DiagnosticLedgerWaitForWriterRelease, true),
      Effect.provideService(DiagnosticProgress, silent),
      Effect.provide(services),
    ),
  );

  expect(result.metrics.find(({ name }) => name === "writerObserverOverlap")?.value).toBe(0);
  expect(result.counters.find(({ name }) => name === "noWriterOverlap")?.value).toBe(1);
  expect(result.counters.find(({ name }) => name === "observations")?.value).toBe(1);
}, 10_000);

it("closes the external writer after observer interruption", async () => {
  const marks: Array<DiagnosticMark> = [];

  await Effect.runPromise(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const workload = ledgerCases.find(({ name }) => name === "ledger-replay-writer-100")!;

      const operation = runLedgerCase(workload).pipe(
        Effect.provideService(DiagnosticProgress, {
          ...silent,
          mark: (mark) =>
            Effect.gen(function* () {
              marks.push(mark);
              if (mark.name !== "writer.acquired") return;
              yield* Deferred.succeed(entered, undefined);

              return yield* Effect.never;
            }),
        }),
      );

      const fiber = yield* operation.pipe(Effect.forkChild);

      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(services), Effect.timeout("10 seconds")),
  );
  expect(marks.filter(({ name }) => name === "writer.scope.closed")).toHaveLength(1);
}, 15_000);

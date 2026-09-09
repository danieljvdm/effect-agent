import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { expect, it } from "vite-plus/test";

import { BenchmarkError } from "../src/contracts.ts";
import {
  DiagnosticProgress,
  DiagnosticResult,
  type DiagnosticMark,
} from "../src/diagnostic-contracts.ts";
import {
  DiagnosticLedgerSeeds,
  DiagnosticLedgerWaitForWriterRelease,
  ledgerCases,
  runLedgerCase,
} from "../src/diagnostic-ledger.ts";
import { writerOverlap, WriterResult } from "../src/diagnostic-writer.ts";

const services = DiagnosticLedgerSeeds.layer.pipe(
  Layer.provideMerge(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
);

const silent = DiagnosticProgress.of({ phase: () => Effect.void, mark: () => Effect.void });

it("retries after a committed seed failure, measures all public workloads, and reuses only the closed successful seed", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      let committed = false;

      const failed = yield* runLedgerCase(ledgerCases[0]!).pipe(
        Effect.provideService(DiagnosticProgress, {
          ...silent,
          mark: (mark) =>
            Effect.gen(function* () {
              if (mark.name !== "seed.firstSettlement.committed") return;
              committed = true;

              return yield* BenchmarkError.make({
                message: "fail after first committed settlement",
              });
            }),
        }),
        Effect.exit,
      );

      expect(committed).toBe(true);
      expect(Exit.isFailure(failed)).toBe(true);
      expect(Exit.isFailure(failed) ? Cause.pretty(failed.cause) : "").toContain(
        "fail after first committed settlement",
      );

      for (const workload of ledgerCases) {
        const marks: Array<DiagnosticMark> = [];

        const result = yield* runLedgerCase(workload).pipe(
          Effect.provideService(DiagnosticProgress, {
            ...silent,
            mark: (mark) =>
              Effect.sync(() => {
                marks.push(mark);
              }),
          }),
        );

        expect(Schema.is(DiagnosticResult)(result)).toBe(true);
        expect(result.metrics.find(({ name }) => name === "setup")?.value).toBeGreaterThan(0);

        const counters = Object.fromEntries(
          result.counters.map(({ name, value }) => [name, value]),
        );

        if (workload.parameters.mode === 0) {
          expect(counters.unfinished).toBe(workload.parameters.unfinished);
          expect(counters.sqlStatements).toBe(
            Math.floor(workload.parameters.unfinished! / 256) + 1,
          );
          expect(counters.indexedPlans).toBe(counters.sqlStatements);
          expect(counters.sortedPlans).toBe(0);
          expect(marks.some(({ name }) => name.startsWith("plan."))).toBe(true);
        } else {
          expect(counters.observations).toBe(workload.parameters.repeats);
          expect(counters.sqlStatements).toBeGreaterThanOrEqual(counters.observations!);
          if (workload.parameters.mode === 1) {
            expect(counters.sqlWriteStatements).toBe(3);
            expect(counters.sqlStatements).toBe(6);
          } else if (workload.parameters.mode === 2)
            expect(counters.sqlStatements).toBe(counters.observations);
          if (workload.parameters.holdMs! >= 0) {
            expect(marks.filter(({ name }) => name === "writer.scope.closed")).toHaveLength(1);
            expect(
              result.metrics.find(({ name }) => name === "writerHold")?.value,
            ).toBeGreaterThanOrEqual(workload.parameters.holdMs! * 0.8);
            expect(counters.noWriterOverlap).toBe(
              Number(counters.observationsWithWriterOverlap === 0),
            );
            expect(marks.filter(({ name }) => name.startsWith("node-hrtime."))).toHaveLength(5);
          }
        }
      }

      const second = yield* runLedgerCase(ledgerCases[0]!).pipe(
        Effect.provideService(DiagnosticProgress, silent),
      );

      expect(second.metrics.find(({ name }) => name === "seedSetup")?.value).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(services), Effect.timeout("100 seconds")),
  );
}, 110_000);

it("rejects modified inventory without creating a seed", async () => {
  const result = await Effect.runPromiseExit(
    runLedgerCase({ ...ledgerCases[0]!, parameters: { settled: 1_000_000 } }).pipe(
      Effect.provideService(DiagnosticProgress, silent),
      Effect.provide(services),
    ),
  );

  expect(Exit.isFailure(result)).toBe(true);
  expect(Exit.isFailure(result) ? Cause.pretty(result.cause) : "").toContain("Unknown or modified");
});

it.each([
  { started: 5, finished: 15, overlap: 5, heldAtStart: false, finishedWhileHeld: true },
  { started: 12, finished: 18, overlap: 6, heldAtStart: true, finishedWhileHeld: true },
  { started: 15, finished: 25, overlap: 5, heldAtStart: true, finishedWhileHeld: false },
  { started: 21, finished: 30, overlap: 0, heldAtStart: false, finishedWhileHeld: false },
  { started: 0, finished: 10, overlap: 0, heldAtStart: false, finishedWhileHeld: false },
])(
  "classifies the complete observer interval $started–$finished without requiring release before completion",
  (sample) => {
    const writer = Schema.decodeUnknownSync(WriterResult)({
      clock: "node-hrtime-same-host",
      acquiredNanos: "10000000",
      releaseStartedNanos: "20000000",
      releasedNanos: "21000000",
      heldMs: 11,
      lockMs: 11,
    });

    expect(
      writerOverlap(writer, {
        startedNanos: BigInt(sample.started) * 1_000_000n,
        finishedNanos: BigInt(sample.finished) * 1_000_000n,
      }),
    ).toEqual({
      overlapMs: sample.overlap,
      heldAtStart: sample.heldAtStart,
      finishedWhileHeld: sample.finishedWhileHeld,
    });
  },
);

it("retains a successful sample with an explicitly missed writer window", async () => {
  const workload = ledgerCases.find(({ name }) => name === "ledger-replay-writer-25")!;

  const result = await Effect.runPromise(
    runLedgerCase(workload).pipe(
      Effect.provideService(DiagnosticLedgerWaitForWriterRelease, true),
      Effect.provideService(DiagnosticProgress, silent),
      Effect.provide(services),
    ),
  );

  expect(Schema.is(DiagnosticResult)(result)).toBe(true);
  expect(result.metrics.find(({ name }) => name === "writerObserverOverlap")?.value).toBe(0);
  expect(result.counters.find(({ name }) => name === "noWriterOverlap")?.value).toBe(1);
  expect(result.counters.find(({ name }) => name === "observationsStartingWithWriter")?.value).toBe(
    0,
  );
  expect(result.counters.find(({ name }) => name === "observations")?.value).toBe(1);
}, 10_000);

it.each(["failure", "defect", "timeout", "interrupt"] as const)(
  "closes the external writer after observer %s",
  async (mode) => {
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
                if (mode === "failure")
                  return yield* BenchmarkError.make({ message: "expected observer failure" });
                if (mode === "defect") return yield* Effect.die("expected observer defect");
                if (mode === "timeout")
                  return yield* Effect.never.pipe(
                    Effect.timeout("5 millis"),
                    Effect.mapError(() =>
                      BenchmarkError.make({ message: "expected observer timeout" }),
                    ),
                  );

                return yield* Effect.never;
              }),
          }),
        );

        const fiber = yield* operation.pipe(Effect.forkChild);

        yield* Deferred.await(entered);
        if (mode === "interrupt") yield* Fiber.interrupt(fiber);
        expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
      }).pipe(Effect.scoped, Effect.provide(services), Effect.timeout("10 seconds")),
    );
    expect(marks.filter(({ name }) => name === "writer.scope.closed")).toHaveLength(1);
  },
  15_000,
);

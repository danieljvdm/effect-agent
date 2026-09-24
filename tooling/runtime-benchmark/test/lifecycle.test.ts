import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { Clock, Deferred, Effect, Exit, Fiber, FileSystem, Layer, Schema } from "effect";
import { expect, it } from "vite-plus/test";

import { subprocess } from "../../../scripts/runtime-benchmark.ts";
import { casesFor, completeBatch, WorkerReport, type Sample } from "../src/contracts.ts";
import { BenchmarkProgress } from "../src/evidence.ts";
import { BenchmarkRunner, runSample, SeedInitializerLive } from "../src/fixture.ts";
import { SeedInitializer, SeedTemplates } from "../src/seeds.ts";
import { runWorker } from "../src/worker.ts";

const services = Layer.mergeAll(
  NodeServices.layer,
  SeedInitializerLive.pipe(Layer.provideMerge(NodeCrypto.layer)),
  BenchmarkProgress.silent,
);

const sample = (ordinal: number): Sample => ({
  case: "small-run",
  ordinal,
  warmup: false,
  totalMs: 1,
  attemptMs: 3,
  setupMs: 1,
  failurePhase: null,
  modelEntryMs: 0.5,
  checkpointCreationMs: null,
  retainedPromptMessages: 0,
  modelCalls: 1,
  finalizers: 1,
  toolCalls: 0,
  outputBytes: 15,
  status: "passed",
  failure: null,
});

it("keeps phase evidence writes outside the operation clock", async () => {
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const clock = yield* Clock.Clock;
      let nanos = 1n;

      return yield* runSample(casesFor("smoke")[0]!, 0, false).pipe(
        Effect.provide(SeedTemplates.layer),
        Effect.provideService(BenchmarkProgress, {
          record: () =>
            Effect.sync(() => {
              nanos += 100_000_000n;
            }),
        }),
        Effect.provideService(Clock.Clock, {
          currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe(),
          currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
          currentTimeMillis: clock.currentTimeMillis,
          currentTimeNanos: clock.currentTimeNanos,
          sleep: (duration) => clock.sleep(duration),
          monotonicTimeNanosUnsafe: () => nanos,
          monotonicTimeNanos: Effect.sync(() => nanos),
        }),
      );
    }).pipe(Effect.provide(services)),
  );

  expect(result.failure).toBeNull();
  expect(result.totalMs).toBe(0);
});

it("isolates sample mutations from the closed seed and subsequent copies", async () => {
  let template = "";

  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const initializer = Layer.effect(
        SeedInitializer,
        Effect.gen(function* () {
          const seedFs = yield* FileSystem.FileSystem;

          return SeedInitializer.of({
            initialize: ({ filename }) =>
              Effect.gen(function* () {
                template = filename;
                yield* seedFs.writeFileString(filename, "seed");
              }).pipe(Effect.orDie),
          });
        }),
      );

      yield* Effect.gen(function* () {
        const seeds = yield* SeedTemplates;
        const directory = yield* fs.makeTempDirectoryScoped();

        yield* seeds.copy({ kind: "history", records: 16, filename: `${directory}/one` });
        yield* fs.writeFileString(`${directory}/one`, "measured mutation");
        yield* seeds.copy({ kind: "history", records: 16, filename: `${directory}/two` });
        expect(yield* fs.readFileString(`${directory}/two`)).toBe("seed");
        expect(yield* fs.readFileString(template)).toBe("seed");
      }).pipe(Effect.provide(SeedTemplates.layer.pipe(Layer.provide(initializer))), Effect.scoped);
    }).pipe(Effect.provide(services)),
  );
});

it("persists completed samples and the failure when the worker is interrupted", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const entered = yield* Deferred.make<void>();

      const options = {
        profile: "smoke" as const,
        cold: true,
        warmups: 0,
        samples: 2,
        output: `${directory}/worker.json`,
      };

      const runner: typeof runSample = (workload, ordinal, warmup) =>
        Effect.gen(function* () {
          if (ordinal === 0) return sample(ordinal);
          const progress = yield* BenchmarkProgress;

          yield* progress
            .record({
              case: workload.name,
              ordinal,
              warmup,
              phase: "operation",
              elapsedMs: 7,
            })
            .pipe(Effect.orDie);
          yield* Deferred.succeed(entered, undefined);

          return yield* Effect.never;
        });

      const fiber = yield* Effect.forkChild(
        runWorker(options).pipe(Effect.provideService(BenchmarkRunner, { run: runner })),
      );

      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);

      const report = yield* Schema.decodeEffect(Schema.fromJsonString(WorkerReport))(
        yield* fs.readFileString(options.output),
      );

      expect(report.samples).toEqual([sample(0)]);
      expect(report.failure).not.toBeNull();
      expect(completeBatch(report, options)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it("writes child output before completion and kills an interrupted child", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const log = `${directory}/child.log`;

      const fiber = yield* Effect.forkChild(
        subprocess(
          process.execPath,
          ["-e", "console.log(JSON.stringify({pid:process.pid})); setInterval(() => {}, 1000)"],
          directory,
          {},
          log,
        ),
      );

      yield* Effect.gen(function* () {
        while (!(yield* fs.exists(log)) || !(yield* fs.readFileString(log)).includes("pid"))
          yield* Effect.sleep("10 millis");
      }).pipe(Effect.timeout("5 seconds"));

      const child = yield* Schema.decodeEffect(
        Schema.fromJsonString(Schema.Struct({ pid: Schema.Int })),
      )(yield* fs.readFileString(log));

      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(() => process.kill(child.pid, 0)).toThrow(/ESRCH/);
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

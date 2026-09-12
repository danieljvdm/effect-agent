import { NodeCrypto, NodeServices } from "@effect/platform-node";
import {
  Clock,
  type Crypto,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Schema,
} from "effect";
import { expect, expectTypeOf, it } from "vite-plus/test";

import { MAX_SUBPROCESS_OUTPUT_BYTES, subprocess } from "../../../scripts/runtime-benchmark.ts";
import {
  BenchmarkError,
  casesFor,
  completeBatch,
  WorkerReport,
  type Sample,
} from "../src/contracts.ts";
import { BenchmarkProgress, writeEvidence } from "../src/evidence.ts";
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
  expect(result.setupMs).toBe(200);
  expect(result.attemptMs).toBe(300);
});

it("preserves the previous complete report when replacement is interrupted", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const filename = `${directory}/report.json`;

      yield* writeEvidence(filename, '{"complete":1}');

      const result = yield* writeEvidence(filename, '{"complete":2}').pipe(
        Effect.provideService(FileSystem.FileSystem, { ...fs, rename: () => Effect.interrupt }),
        Effect.exit,
      );

      expect(Exit.isFailure(result)).toBe(true);
      expect(yield* fs.readFileString(filename)).toBe('{"complete":1}');
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it("retains a failed sample and later successes while rejecting the worker", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();

      const options = {
        profile: "smoke" as const,
        cold: true,
        warmups: 0,
        samples: 2,
        output: `${directory}/worker.json`,
      };

      const runner: typeof runSample = (_workload, ordinal) =>
        Effect.succeed(
          ordinal === 0
            ? {
                ...sample(ordinal),
                status: "failed",
                failure: "expected failure",
                failurePhase: "operation",
              }
            : sample(ordinal),
        );

      const result = yield* runWorker(options).pipe(
        Effect.provideService(BenchmarkRunner, { run: runner }),
        Effect.exit,
      );

      expect(Exit.isFailure(result)).toBe(true);

      const report = yield* Schema.decodeEffect(Schema.fromJsonString(WorkerReport))(
        yield* fs.readFileString(options.output),
      );

      expect(report.samples.map((entry) => entry.status)).toEqual(["failed", "passed"]);
      expect(report.active).toBeNull();
      expect(report.failure).toContain("Benchmark correctness assertions failed");
      expect(completeBatch(report, options)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it("preserves a worker report when its seed cache cannot be acquired", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();

      const options = {
        profile: "smoke" as const,
        cold: true,
        warmups: 0,
        samples: 1,
        output: `${directory}/worker.json`,
      };

      const result = yield* runWorker(options).pipe(
        Effect.provide(BenchmarkRunner.layer),
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          makeTempDirectoryScoped: () => Effect.die("seed cache unavailable"),
        }),
        Effect.exit,
      );

      expect(Exit.isFailure(result)).toBe(true);

      const report = yield* Schema.decodeEffect(Schema.fromJsonString(WorkerReport))(
        yield* fs.readFileString(options.output),
      );

      expect(report.samples).toEqual([]);
      expect(report.active).toBeNull();
      expect(report.failure).toContain("seed cache unavailable");
      expect(completeBatch(report, options)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it("clones closed templates once per key without sharing sample mutations and removes its files", async () => {
  let template = "";

  const observed = await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let initializations = 0;

      const initializer = Layer.effect(
        SeedInitializer,
        Effect.gen(function* () {
          const seedFs = yield* FileSystem.FileSystem;

          return SeedInitializer.of({
            initialize: ({ filename }) =>
              Effect.gen(function* () {
                initializations++;
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
        expect(initializations).toBe(1);
        yield* seeds.copy({ kind: "ledger", records: 16, filename: `${directory}/three` });
        expect(initializations).toBe(2);
      }).pipe(Effect.provide(SeedTemplates.layer.pipe(Layer.provide(initializer))), Effect.scoped);

      return yield* fs.exists(template);
    }).pipe(Effect.provide(services)),
  );

  expect(observed).toBe(false);
});

it.each(["wal", "shm"])(
  "rejects a seed with a remaining SQLite %s and never publishes a copy",
  async (sidecar) => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped();
        const destination = `${directory}/copy`;

        const initializer = Layer.effect(
          SeedInitializer,
          Effect.gen(function* () {
            const seedFs = yield* FileSystem.FileSystem;

            return SeedInitializer.of({
              initialize: ({ filename }) =>
                Effect.gen(function* () {
                  yield* seedFs.writeFileString(filename, "seed");
                  yield* seedFs.writeFileString(`${filename}-${sidecar}`, "pending state");
                }).pipe(Effect.orDie),
            });
          }),
        );

        const result = yield* Effect.gen(function* () {
          const seeds = yield* SeedTemplates;

          yield* seeds.copy({ kind: "history", records: 16, filename: destination });
        }).pipe(Effect.provide(SeedTemplates.layer.pipe(Layer.provide(initializer))), Effect.exit);

        expect(Exit.isFailure(result)).toBe(true);
        expect(yield* fs.exists(destination)).toBe(false);
      }).pipe(Effect.scoped, Effect.provide(services)),
    );
  },
);

it("uses a fresh template path after interrupted initialization and closes its resources", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      let closed = false;
      let abandoned = "";

      const initializer = Layer.effect(
        SeedInitializer,
        Effect.gen(function* () {
          const seedFs = yield* FileSystem.FileSystem;

          return SeedInitializer.of({
            initialize: ({ filename }) =>
              Effect.gen(function* () {
                if (abandoned === "") {
                  abandoned = filename;
                  yield* seedFs.writeFileString(filename, "incomplete");
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      closed = true;
                    }),
                  );

                  return yield* Effect.interrupt;
                }
                expect(filename).not.toBe(abandoned);
                expect(yield* seedFs.exists(filename)).toBe(false);
                yield* seedFs.writeFileString(filename, "complete");
              }).pipe(Effect.scoped, Effect.orDie),
          });
        }),
      );

      yield* Effect.gen(function* () {
        const seeds = yield* SeedTemplates;

        const first = yield* seeds
          .copy({ kind: "history", records: 16, filename: `${directory}/one` })
          .pipe(Effect.exit);

        expect(Exit.isFailure(first)).toBe(true);
        expect(closed).toBe(true);
        yield* seeds.copy({ kind: "history", records: 16, filename: `${directory}/two` });
        expect(yield* fs.readFileString(`${directory}/two`)).toBe("complete");
      }).pipe(Effect.provide(SeedTemplates.layer.pipe(Layer.provide(initializer))));
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it("runs fresh durable and recovery samples on isolated copies while rebuilding every checkpoint", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      for (const workload of casesFor("smoke").filter((entry) =>
        ["durable", "recovery", "ledger"].includes(entry.kind),
      )) {
        for (const ordinal of [0, 1]) {
          const result = yield* runSample(workload, ordinal, false);

          expect(result.failure).toBeNull();
          expect(result.status).toBe("passed");
          expect(result.checkpointCreationMs === null).toBe(workload.kind !== "recovery");
          expect(result.modelCalls).toBe(result.finalizers);
          expect(result.attemptMs).toBeGreaterThanOrEqual(result.setupMs + result.totalMs);
        }
      }
    }).pipe(Effect.provide(SeedTemplates.layer), Effect.scoped, Effect.provide(services)),
  );
}, 30_000);

it.each(["failure", "defect", "timeout"] as const)(
  "retains setup %s with elapsed time and finalization",
  async (failure) => {
    let closed = false;
    const workload = casesFor("smoke").find((entry) => entry.name === "durable-fresh-16")!;

    const result = await Effect.runPromise(
      runSample(workload, 0, false, { timeout: "10 millis" }).pipe(
        Effect.provideService(SeedTemplates, {
          copy: () =>
            Effect.gen(function* () {
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  closed = true;
                }),
              );
              if (failure === "failure")
                return yield* BenchmarkError.make({ message: "seed failed" });
              if (failure === "defect") return yield* Effect.die("seed defect");

              return yield* Effect.never;
            }).pipe(Effect.scoped),
        }),
        Effect.provide(services),
      ),
    );

    expect(result.status).toBe("failed");
    expect(result.failurePhase).toBe("setup");
    expect(result.failure).not.toBeNull();
    expect(result.totalMs).toBe(0);
    expect(result.attemptMs).toBeGreaterThan(0);
    expect(closed).toBe(true);
  },
);

it("persists the active phase and completed samples when the worker is interrupted", async () => {
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
      expect(report.active).toMatchObject({
        case: "small-run",
        ordinal: 1,
        phase: "operation",
        elapsedMs: 7,
      });
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

it("caps combined stdout and stderr, retains their prefix, and terminates a chatty child", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const log = `${directory}/child.log`;

      // Each stream is below the cap. Only their combined output exceeds it.
      const childScript = `
      require('node:fs').writeFileSync('child.json', JSON.stringify({pid:process.pid}));
      process.stdout.write('retained prefix\\n');
      process.stdout.write(Buffer.alloc(5 * 1024 * 1024, 'o'), () => {
        process.stderr.write(Buffer.alloc(5 * 1024 * 1024, 'e'));
      });
      setInterval(() => {}, 1000);
    `;

      const failure = yield* subprocess(
        process.execPath,
        ["-e", childScript],
        directory,
        {},
        log,
      ).pipe(Effect.flip, Effect.timeout("10 seconds"));

      expect(failure._tag).toBe("BenchmarkError");
      expect(failure.message).toContain(`exceeded ${MAX_SUBPROCESS_OUTPUT_BYTES} bytes`);
      const retained = yield* fs.readFile(log);

      expect(retained.byteLength).toBe(MAX_SUBPROCESS_OUTPUT_BYTES);
      const text = new TextDecoder().decode(retained);

      expect(text.startsWith("retained prefix\n")).toBe(true);
      expect(text).toContain("oooo");
      expect(text).toContain("eeee");

      const child = yield* Schema.decodeEffect(
        Schema.fromJsonString(Schema.Struct({ pid: Schema.Int })),
      )(yield* fs.readFileString(`${directory}/child.json`));

      expect(() => process.kill(child.pid, 0)).toThrow(/ESRCH/);
    }).pipe(Effect.scoped, Effect.provide(services)),
  );
});

it("keeps sample and worker requirements visible in Effect", () => {
  expectTypeOf<
    Extract<Effect.Services<ReturnType<typeof runSample>>, BenchmarkProgress | SeedTemplates>
  >().toEqualTypeOf<BenchmarkProgress | SeedTemplates>();
  expectTypeOf<
    Extract<Effect.Services<ReturnType<typeof runWorker>>, BenchmarkRunner | SeedInitializer>
  >().toEqualTypeOf<BenchmarkRunner | SeedInitializer>();
  expectTypeOf<Layer.Services<typeof SeedTemplates.layer>>()
    .extract<SeedInitializer>()
    .toEqualTypeOf<SeedInitializer>();
  expectTypeOf<Layer.Services<typeof SeedInitializerLive>>().toEqualTypeOf<Crypto.Crypto>();
  expectTypeOf<Effect.Error<ReturnType<typeof runSample>>>().toEqualTypeOf<never>();
});

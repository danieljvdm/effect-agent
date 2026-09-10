import process from "node:process";

import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Config, Effect, Exit, FileSystem, Layer, Schema } from "effect";

import {
  BenchmarkError,
  casesFor,
  FIXTURE_VERSION,
  WorkerOptions,
  WorkerReport,
  type Sample,
  type SampleProgress,
} from "./contracts.js";
import { BenchmarkProgress, writeEvidence } from "./evidence.js";
import { BenchmarkRunner, SeedInitializerLive } from "./fixture.js";
import { SeedTemplates } from "./seeds.js";

/** The worker owns report persistence and the lifetime of its shared seed cache. */
export const runWorker = Effect.fn("benchmark.runWorker")(function* (
  options: typeof WorkerOptions.Type,
) {
  const samples: Array<Sample> = [];
  let active: SampleProgress | null = null;
  let failure: string | null = null;

  const workloads = options.cold
    ? casesFor(options.profile).slice(0, 1)
    : casesFor(options.profile);

  const persist = () =>
    writeEvidence(
      options.output,
      Schema.encodeSync(Schema.fromJsonString(WorkerReport))({
        fixture: FIXTURE_VERSION,
        profile: options.profile,
        runtime: process.version,
        platform: process.platform,
        architecture: process.arch,
        active,
        failure,
        samples,
      }),
    );

  const progressLayer = Layer.effect(
    BenchmarkProgress,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      return BenchmarkProgress.of({
        record: Effect.fn("benchmark.recordProgress")(function* (progress) {
          active = progress;
          yield* persist().pipe(Effect.provideService(FileSystem.FileSystem, fs));
        }),
      });
    }),
  );

  yield* Effect.gen(function* () {
    yield* persist();
    // Acquire the cache after the first report so acquisition failures leave evidence, too.
    yield* Effect.gen(function* () {
      const runner = yield* BenchmarkRunner;

      for (let index = 0; index < options.warmups + options.samples; index++) {
        const ordered = index % 2 === 0 ? workloads : [...workloads].reverse();

        for (const workload of ordered) {
          active = {
            case: workload.name,
            ordinal: index,
            warmup: index < options.warmups,
            phase: "setup",
            elapsedMs: 0,
          };
          yield* persist();
          samples.push(yield* runner.run(workload, index, index < options.warmups));
          active = null;
          // Keep partial failures and slow samples even when a later child is interrupted.
          yield* persist();
        }
      }
    }).pipe(Effect.provide(Layer.merge(SeedTemplates.layer, progressLayer)));
    if (samples.some((sample) => sample.status === "failed"))
      return yield* BenchmarkError.make({
        message: "Benchmark correctness assertions failed; see raw samples",
      });
  }).pipe(
    Effect.scoped,
    Effect.onExit((exit) => {
      if (Exit.isFailure(exit)) failure = Cause.pretty(exit.cause);

      return persist();
    }),
  );
});

if (import.meta.main)
  NodeRuntime.runMain(
    Effect.gen(function* () {
      const options = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WorkerOptions))(
        yield* Config.string("RUNTIME_BENCHMARK_OPTIONS"),
      );

      yield* runWorker(options).pipe(
        Effect.provide(Layer.merge(BenchmarkRunner.layer, SeedInitializerLive)),
      );
    }).pipe(Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer))),
  );

import process from "node:process";

import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Effect, FileSystem, Layer, Schema } from "effect";

import {
  BenchmarkError,
  casesFor,
  FIXTURE_VERSION,
  WorkerOptions,
  WorkerReport,
  type Sample,
} from "./contracts.js";
import { runSample } from "./fixture.js";

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  const options = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(WorkerOptions))(
    yield* Config.string("RUNTIME_BENCHMARK_OPTIONS"),
  );

  const samples: Array<Sample> = [];

  const workloads = options.cold
    ? casesFor(options.profile).slice(0, 1)
    : casesFor(options.profile);

  const persist = () =>
    fs.writeFileString(
      options.output,
      Schema.encodeSync(Schema.fromJsonString(WorkerReport))({
        fixture: FIXTURE_VERSION,
        profile: options.profile,
        runtime: process.version,
        platform: process.platform,
        architecture: process.arch,
        samples,
      }),
    );

  yield* persist();
  for (let index = 0; index < options.warmups + options.samples; index++) {
    const ordered = index % 2 === 0 ? workloads : [...workloads].reverse();

    for (const workload of ordered) {
      samples.push(yield* runSample(workload, index, index < options.warmups));
      // Keep partial failures and slow samples even when a later child is interrupted.
      yield* persist();
    }
  }
  if (samples.some((sample) => sample.status === "failed"))
    return yield* BenchmarkError.make({
      message: "Benchmark correctness assertions failed; see raw samples",
    });
}).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)));

NodeRuntime.runMain(program);

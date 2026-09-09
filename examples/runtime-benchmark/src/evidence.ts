import { Context, Effect, FileSystem, Layer } from "effect";
import type { PlatformError } from "effect/PlatformError";

import type { SampleProgress } from "./contracts.js";

/** Phase persistence belongs to the worker; samples only report their timing boundaries. */
export class BenchmarkProgress extends Context.Service<
  BenchmarkProgress,
  {
    readonly record: (progress: SampleProgress) => Effect.Effect<void, PlatformError>;
  }
>()("runtime-benchmark/BenchmarkProgress") {
  static readonly silent = Layer.succeed(BenchmarkProgress, { record: () => Effect.void });
}

/** A killed writer leaves the previous complete JSON document available to artifact readers. */
export const writeEvidence = Effect.fn("benchmark.writeEvidence")(function* (
  filename: string,
  contents: string,
) {
  const fs = yield* FileSystem.FileSystem;

  yield* fs.writeFileString(`${filename}.tmp`, contents);
  yield* fs.rename(`${filename}.tmp`, filename);
});

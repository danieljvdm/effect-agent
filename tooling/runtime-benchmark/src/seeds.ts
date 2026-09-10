import { Context, Effect, FileSystem, Layer, Path, Schema } from "effect";
import type { PlatformError } from "effect/PlatformError";

import type { BenchmarkError } from "./contracts.js";
import { check } from "./contracts.js";

export const SeedRequest = Schema.Struct({
  kind: Schema.Literals(["history", "ledger"]),
  records: Schema.Natural,
  filename: Schema.String,
});

export type SeedRequest = typeof SeedRequest.Type;

/** The initializer must finish closing every database owner before returning. */
export class SeedInitializer extends Context.Service<
  SeedInitializer,
  {
    readonly initialize: (request: SeedRequest) => Effect.Effect<void, BenchmarkError>;
  }
>()("runtime-benchmark/SeedInitializer") {}

export class SeedTemplates extends Context.Service<
  SeedTemplates,
  {
    readonly copy: (request: SeedRequest) => Effect.Effect<void, PlatformError | BenchmarkError>;
  }
>()("runtime-benchmark/SeedTemplates") {
  /** Worker-local, revision-local templates, removed when their Layer closes. */
  static readonly layer = Layer.effect(
    SeedTemplates,
    Effect.gen(function* () {
      const initializer = yield* SeedInitializer;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "runtime-benchmark-seeds-" });
      const templates = new Map<string, string>();
      let attempts = 0;

      const copy = Effect.fn("benchmark.copySeed")(function* (request: SeedRequest) {
        const key = `${request.kind}-${request.records}`;
        let source = templates.get(key);

        if (source === undefined) {
          const candidate = path.join(directory, `${attempts++}.sqlite`);

          yield* initializer.initialize({ ...request, filename: candidate });
          // Never copy a live SQLite database or omit committed pages still held in a WAL.
          yield* check(
            !(yield* fs.exists(`${candidate}-wal`)) && !(yield* fs.exists(`${candidate}-shm`)),
            "Seed database is not closed: SQLite WAL/SHM sidecars remain",
          );
          templates.set(key, candidate);
          source = candidate;
        }
        yield* fs.copyFile(source, request.filename);
      });

      return SeedTemplates.of({ copy });
    }),
  );
}

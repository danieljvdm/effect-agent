import { Effect, FileSystem, Path } from "effect";
import type { PlatformError } from "effect/PlatformError";

import type { BenchmarkError } from "./contracts.js";
import { check } from "./contracts.js";

export interface SeedTemplates {
  readonly copy: <E, R>(
    key: string,
    destination: string,
    initialize: (filename: string) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, E | PlatformError | BenchmarkError, R>;
}

/** Worker-local, revision-local templates. initialize must finish closing every database owner. */
export const makeSeedTemplates = Effect.fn("benchmark.makeSeedTemplates")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "runtime-benchmark-seeds-" });
  const templates = new Map<string, string>();
  let attempts = 0;

  const copy: SeedTemplates["copy"] = Effect.fn("benchmark.copySeed")(function* <E, R>(
    key: string,
    destination: string,
    initialize: (filename: string) => Effect.Effect<void, E, R>,
  ) {
    let source = templates.get(key);

    if (source === undefined) {
      const candidate = path.join(directory, `${attempts++}.sqlite`);

      yield* initialize(candidate);
      // Never copy a live SQLite database or omit committed pages still held in a WAL.
      yield* check(
        !(yield* fs.exists(`${candidate}-wal`)) && !(yield* fs.exists(`${candidate}-shm`)),
        "Seed database is not closed: SQLite WAL/SHM sidecars remain",
      );
      templates.set(key, candidate);
      source = candidate;
    }
    yield* fs.copyFile(source, destination);
  });

  return { copy } satisfies SeedTemplates;
});

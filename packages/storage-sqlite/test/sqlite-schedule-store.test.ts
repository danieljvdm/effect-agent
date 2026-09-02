import { scheduleStoreConformanceCases } from "@effect-agent/thread/testing";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Effect, FileSystem, Layer } from "effect";

import {
  scheduleStoreLayer,
  SqliteStorageConfig,
  SqliteStorageConfigValue,
  SqliteStorageFailpoint,
} from "../src/index.ts";

const testLayer = (filename: string) =>
  scheduleStoreLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(SqliteStorageConfig)(
          SqliteStorageConfigValue.make({
            observationPollInterval: 1,
            busyTimeout: 5_000,
            ownershipLeaseDuration: 30_000,
            verifyOnOpen: false,
          }),
        ),
        SqliteStorageFailpoint.layer,
        SqliteClient.layer({ filename }),
      ),
    ),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-schedule-sqlite-",
      });

      return yield* use(`${directory}/schedules.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SqliteScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((filename) =>
        conformanceCase.run.pipe(Effect.provide(testLayer(filename))),
      ),
    );
  }
});

import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "@effect-agent/thread/testing";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Effect, FileSystem, Layer } from "effect";

import {
  SqliteStorageConfig,
  SqliteStorageConfigValue,
  SqliteStorageFailpoint,
  subscriptionStoreLayer,
} from "../src/index.ts";

const testLayer = (filename: string) =>
  subscriptionStoreLayer(subscriptionConformancePartition).pipe(
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
        prefix: "effect-agent-subscription-sqlite-",
      });

      return yield* use(`${directory}/subscriptions.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SqliteSubscriptionStore", () => {
  for (const testCase of subscriptionStoreConformanceCases) {
    it.effect(testCase.name, () =>
      withTemporaryDatabase((filename) => testCase.run.pipe(Effect.provide(testLayer(filename)))),
    );
  }
});

import { MessageDeliveryStore } from "@effect-agent/thread/MessageDelivery";
import {
  makeMessageDeliveryFixture,
  messageDeliveryStoreConformanceCases,
} from "@effect-agent/thread/testing/MessageDeliveryStoreConformance";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer } from "effect";

import { messageDeliveryStoreLayer } from "../src/SqliteMessageDeliveryStore.ts";
import { SqliteStorageFailpoint } from "../src/SqliteStorageFailpoint.ts";
import { storageConfigLayer } from "../src/SqliteThreadStore.ts";

const storeLayer = (filename: string) =>
  messageDeliveryStoreLayer().pipe(
    Layer.provide([
      SqliteClient.layer({ filename }),
      storageConfigLayer({ filename }),
      SqliteStorageFailpoint.layer,
    ]),
  );

for (const testCase of messageDeliveryStoreConformanceCases) {
  it.effect(testCase.name, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "message-conformance-" });

        yield* testCase.run.pipe(Effect.provide(storeLayer(`${directory}/messages.sqlite`)));
      }),
    ).pipe(Effect.provide([NodeFileSystem.layer, NodeCrypto.layer])),
  );
}

it.effect("rediscovers an interrupted delivery lease after closing and reopening SQLite", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "message-reopen-" });
      const filename = `${directory}/messages.sqlite`;
      const record = yield* makeMessageDeliveryFixture();

      const oldClaim = yield* Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;

        yield* store.insert(record);

        return yield* store.change(record.key, { _tag: "Claim", nowMillis: 0, expectedVersion: 1 });
      }).pipe(Effect.provide(storeLayer(filename)));

      yield* Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;

        expect(yield* store.get(record.key)).toEqual(oldClaim);
        expect(yield* store.due(99, 10)).toEqual([]);
        expect(yield* store.due(100, 10)).toEqual([record.key]);

        const recovered = yield* store.change(record.key, {
          _tag: "Claim",
          nowMillis: 100,
          expectedVersion: oldClaim.version,
        });

        expect(recovered.envelope).toEqual(record.envelope);
        expect(recovered.version).toBe(oldClaim.version + 1);
      }).pipe(Effect.provide(storeLayer(filename)));
    }),
  ).pipe(Effect.provide([NodeFileSystem.layer, NodeCrypto.layer])),
);

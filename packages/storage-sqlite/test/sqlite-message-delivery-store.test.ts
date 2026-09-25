import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer } from "effect";
import { lifecyclePublicationLayer } from "effect-agent/lifecycle-publication";
import { MessageDeliveryStore, readPending } from "effect-agent/message-delivery";
import {
  makeMessageDeliveryFixture,
  messageDeliveryStoreConformanceCases,
} from "effect-agent/testing/message-delivery-store-conformance";

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

// A native admission may survive its caller. A public record must be retried
// from that exact retained envelope after a lost publication acknowledgement.
it.effect("retains exact lifecycle delivery through reopen and lost acknowledgement", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "lifecycle-reopen-" });
      const filename = `${directory}/messages.sqlite`;
      const record = yield* makeMessageDeliveryFixture();

      const layer = messageDeliveryStoreLayer().pipe(
        Layer.provide(lifecyclePublicationLayer),
        Layer.provide([
          SqliteClient.layer({ filename }),
          storageConfigLayer({ filename }),
          SqliteStorageFailpoint.layer,
        ]),
      );

      const publication = yield* Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;

        yield* store.insert(record);
        if (store.lifecyclePublications === undefined)
          return yield* Effect.fail("Native lifecycle publication is unavailable");
        const pending = yield* store.lifecyclePublications.pending(0, 32);

        expect(pending).toHaveLength(1);
        expect(pending[0]?.fact).toEqual({
          _tag: "DeliveryRetained",
          key: record.key,
          envelope: record.envelope,
          createdAtMillis: record.createdAtMillis,
        });

        return pending[0]!;
      }).pipe(Effect.provide(layer));

      yield* Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;

        if (store.lifecyclePublications === undefined)
          return yield* Effect.fail("Native lifecycle publication is unavailable");
        expect(yield* store.get(record.key)).toEqual(record);
        expect(yield* store.lifecyclePublications.pending(0, 32)).toEqual([publication]);
        yield* store.lifecyclePublications.acknowledge(publication);
        yield* store.insert(record);
        expect(yield* store.lifecyclePublications.pending(0, 32)).toEqual([]);
      }).pipe(Effect.provide(layer));
    }),
  ).pipe(Effect.provide([NodeFileSystem.layer, NodeCrypto.layer])),
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
        expect(yield* readPending({ ownerThreadId: record.key.ownerThreadId, limit: 1 })).toEqual([
          oldClaim,
        ]);
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

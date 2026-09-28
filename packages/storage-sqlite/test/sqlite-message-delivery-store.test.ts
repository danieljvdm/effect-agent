import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, Option } from "effect";
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
it.effect(
  "retains ordered lifecycle batches, retry budgets and exact receipts through reopen",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "lifecycle-reopen-" });
        const filename = `${directory}/messages.sqlite`;
        const record = yield* makeMessageDeliveryFixture();
        const second = yield* makeMessageDeliveryFixture("second");
        const later = yield* makeMessageDeliveryFixture("later");
        const other = yield* makeMessageDeliveryFixture("other", "other-owner");

        const layer = messageDeliveryStoreLayer().pipe(
          Layer.provide(lifecyclePublicationLayer),
          Layer.provide([
            SqliteClient.layer({ filename }),
            storageConfigLayer({ filename }),
            SqliteStorageFailpoint.layer,
          ]),
        );

        const batch = yield* Effect.gen(function* () {
          const store = yield* MessageDeliveryStore;

          yield* store.insert(record);
          yield* store.insert(second);
          const publications = store.lifecyclePublications;

          if (publications === undefined) return yield* Effect.fail("Missing lifecycle storage");
          const pending = yield* publications.pending(0, 1);

          // Regression: c68edc7a selected only the oldest fact, never an owner batch.
          expect(pending).toMatchObject([[{ ordinal: 1 }, { ordinal: 2 }]]);
          const selected = pending[0]!;

          expect(selected[0].fact).toEqual({
            _tag: "DeliveryRetained",
            key: record.key,
            envelope: record.envelope,
            createdAtMillis: 0,
          });
          expect(yield* publications.claim(selected, 0, 10)).toBe(true);
          expect(yield* publications.pending(1_009, 1)).toEqual([]);

          return selected;
        }).pipe(Effect.provide(layer));

        yield* Effect.gen(function* () {
          const store = yield* MessageDeliveryStore;
          const publications = store.lifecyclePublications;

          if (publications === undefined) return yield* Effect.fail("Missing lifecycle storage");
          expect(yield* store.get(record.key)).toEqual(record);
          expect(yield* publications.pending(1_010, 1)).toEqual([batch]);

          let now = 1_010;

          // The first dispatch survived reopen without its acknowledgement. Seven remain.
          for (const delay of [2_000, 4_000, 8_000, 16_000, 32_000, 60_000, null]) {
            expect(yield* publications.claim(batch, now, 10)).toBe(true);
            expect(yield* publications.claim(batch, now, 10)).toBe(false);
            if (delay !== null) {
              now += 10 + delay;
              expect(yield* publications.pendingDeadline).toEqual(Option.some(now));
              expect(yield* publications.pending(now - 1, 1)).toEqual([]);
              expect(yield* publications.pending(now, 1)).toEqual([batch]);
            }
          }
          expect(yield* publications.pendingDeadline).toEqual(Option.none());
          yield* store.insert(later);
          yield* store.insert(other);
          const unrelated = yield* publications.pending(Number.MAX_SAFE_INTEGER, 10);

          expect(unrelated.map((entry) => entry[0].ownerThreadId)).toEqual([
            other.key.ownerThreadId,
          ]);
          yield* publications.acknowledge(unrelated[0]!);

          // Operator repair restores the parked prefix, including facts retained since parking.
          yield* publications.retryParked(record.key.ownerThreadId, now);
          expect((yield* publications.pending(now, 1))[0]).toHaveLength(3);

          const changed = [
            batch[0],
            { ...batch[1]!, createdAt: batch[0].createdAt, id: "changed" },
          ] as const;

          expect(yield* publications.acknowledge(changed).pipe(Effect.flip)).toMatchObject({
            reason: "conflict",
          });
          expect((yield* publications.pending(now, 1))[0]).toHaveLength(3);
          yield* publications.acknowledge(batch);
          yield* publications.acknowledge(batch);
          yield* store.insert(record);
          const remaining = yield* publications.pending(now, 1);

          expect(remaining).toMatchObject([[{ ordinal: 3 }]]);
          yield* publications.acknowledge(remaining[0]!);
          expect(yield* publications.pending(now, 1)).toEqual([]);
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

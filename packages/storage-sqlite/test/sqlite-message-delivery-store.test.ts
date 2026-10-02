import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import { ThreadId } from "@yielded/agent/identifiers";
import {
  drainLifecyclePublications,
  lifecyclePublicationLayer,
  LifecyclePublication,
  type LifecyclePublicationBatch,
  LifecyclePublicationError,
  LifecyclePublicationHandler,
  type LifecyclePublicationStorage,
} from "@yielded/agent/lifecycle-publication";
import { MessageDeliveryStore, readPending } from "@yielded/agent/message-delivery";
import {
  makeMessageDeliveryFixture,
  messageDeliveryStoreConformanceCases,
} from "@yielded/agent/testing/message-delivery-store-conformance";
import { DateTime, Effect, FileSystem, Layer, Option } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import { messageDeliveryStoreLayer } from "../src/SqliteMessageDeliveryStore.ts";
import { SqliteStorageFailpoint } from "../src/SqliteStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/SqliteSubmissionLedger.ts";
import { storageConfigLayer, threadStoreLayer } from "../src/SqliteThreadStore.ts";

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
          Layer.provide([storageConfigLayer({ filename }), SqliteStorageFailpoint.layer]),
          Layer.provideMerge(SqliteClient.layer({ filename })),
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
          // beta.161 operator repair moved the deadline and deleted the retry row.
          // That supported reset must receive its full fresh attempt budget after upgrade.
          const sql = yield* SqlClient;

          yield* sql`DELETE FROM effect_agent_lifecycle_publication_retries WHERE id = ${batch[0].id}`;
          expect(yield* publications.claim(batch, now, 10)).toBe(true);
          expect(yield* publications.pendingDeadline).toEqual(Option.some(now + 1_010));
          now += 1_010;

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

// Requested wave acknowledgement seam: a conflicting later owner must not consume the
// earlier owner's publication debt. Existing single-owner reopen proof cannot force this.
it.effect("rolls back a lifecycle acknowledgement wave with a conflicting owner", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "lifecycle-wave-" });

      yield* Effect.gen(function* () {
        const record = yield* makeMessageDeliveryFixture();
        const other = yield* makeMessageDeliveryFixture("other", "other-owner");
        const store = yield* MessageDeliveryStore;

        yield* store.insert(record);
        yield* store.insert(other);
        const publications = store.lifecyclePublications;

        if (publications === undefined) return yield* Effect.fail("Missing lifecycle storage");
        const pending = yield* publications.pending(0, 2);
        const changed = [pending[0]!, [{ ...pending[1]![0], id: "changed" }] as const];
        const acknowledgeMany = publications.acknowledgeMany;

        expect(
          yield* (
            acknowledgeMany === undefined
              ? Effect.forEach(changed, publications.acknowledge)
              : acknowledgeMany(changed)
          ).pipe(Effect.flip),
        ).toMatchObject({ reason: "conflict" });
        expect(yield* publications.pending(0, 2)).toEqual(pending);
      }).pipe(
        Effect.provide(
          storeLayer(`${directory}/messages.sqlite`).pipe(Layer.provide(lifecyclePublicationLayer)),
        ),
      );
    }),
  ).pipe(Effect.provide([NodeCrypto.layer, NodeFileSystem.layer])),
);

// Custom ports may return more owners than SQL's limit. Preserve the documented
// acknowledgement bound and completed-batch cleanup for those larger drains.
it.effect("bounds custom acknowledgement waves after success and interruption", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "lifecycle-custom-wave-" });

      yield* Effect.gen(function* () {
        const record = yield* makeMessageDeliveryFixture();

        const batches = Array.from({ length: 102 }, (_, index): LifecyclePublicationBatch => {
          const ownerThreadId = ThreadId.make(`owner-${index}`);

          return [
            LifecyclePublication.make({
              id: `publication-${index}`,
              ownerThreadId,
              ordinal: 1,
              createdAt: DateTime.makeUnsafe(0),
              fact: {
                _tag: "DeliveryRetained",
                key: { ...record.key, ownerThreadId },
                envelope: record.envelope,
                createdAtMillis: 0,
              },
            }),
          ];
        });

        for (const interrupted of [false, true]) {
          const acknowledged: Array<ReadonlyArray<LifecyclePublicationBatch>> = [];
          const published: Array<string> = [];

          const storage: LifecyclePublicationStorage = {
            pending: (_, limit) => Effect.succeed(batches.slice(0, limit)),
            claim: () => Effect.succeed(true),
            acknowledge: () => Effect.die("Expected bounded wave acknowledgement"),
            acknowledgeMany: (wave) =>
              wave.length > 100
                ? Effect.fail(LifecyclePublicationError.make({ reason: "capacity" }))
                : Effect.sync(() => {
                    acknowledged.push(wave);
                  }),
            pendingDeadline: Effect.succeed(Option.none()),
            retryParked: () => Effect.void,
          };

          const exit = yield* drainLifecyclePublications(
            storage,
            10_000,
            interrupted ? 102 : 101,
          ).pipe(
            Effect.provideService(LifecyclePublicationHandler, {
              publish: (batch) =>
                batch[0].id === "publication-101"
                  ? Effect.interrupt
                  : Effect.sync(() => {
                      published.push(batch[0].id);
                    }),
            }),
            Effect.exit,
          );

          expect(acknowledged.map((wave) => wave.length)).toEqual([100, 1]);
          expect(acknowledged.flat().map((batch) => batch[0].id)).toEqual(published);
          expect(exit._tag).toBe(interrupted ? "Failure" : "Success");
        }
      }).pipe(
        Effect.provide(
          Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
            Layer.provide([
              SqliteClient.layer({ filename: `${directory}/messages.sqlite` }),
              storageConfigLayer({ filename: `${directory}/messages.sqlite` }),
              SqliteStorageFailpoint.layer,
            ]),
          ),
        ),
      );
    }),
  ).pipe(Effect.provide([NodeCrypto.layer, NodeFileSystem.layer])),
);

// Cancellation during a later dispatch must still acknowledge the earlier committed
// batch, while retaining the interrupted owner's exact debt.
it.effect("acknowledges completed lifecycle batches when a later owner interrupts", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "lifecycle-interrupt-" });

      yield* Effect.gen(function* () {
        const record = yield* makeMessageDeliveryFixture();
        const other = yield* makeMessageDeliveryFixture("other", "other-owner");
        const store = yield* MessageDeliveryStore;

        yield* store.insert(record);
        yield* store.insert(other);
        const publications = store.lifecyclePublications;

        if (publications === undefined) return yield* Effect.fail("Missing lifecycle storage");
        const pending = yield* publications.pending(0, 2);

        yield* drainLifecyclePublications(publications).pipe(
          Effect.provideService(LifecyclePublicationHandler, {
            publish: (batch) =>
              batch[0].ownerThreadId === pending[0]![0].ownerThreadId
                ? Effect.void
                : Effect.interrupt,
          }),
          Effect.exit,
        );
        expect(yield* publications.pending(Number.MAX_SAFE_INTEGER, 2)).toEqual([pending[1]]);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(messageDeliveryStoreLayer(), threadStoreLayer, submissionLedgerLayer).pipe(
            Layer.provide(lifecyclePublicationLayer),
            Layer.provide([
              SqliteClient.layer({ filename: `${directory}/messages.sqlite` }),
              storageConfigLayer({ filename: `${directory}/messages.sqlite` }),
              SqliteStorageFailpoint.layer,
            ]),
          ),
        ),
      );
    }),
  ).pipe(Effect.provide([NodeCrypto.layer, NodeFileSystem.layer])),
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

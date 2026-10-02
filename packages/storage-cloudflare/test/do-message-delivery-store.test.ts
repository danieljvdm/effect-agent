import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import {
  makeSqlLifecyclePublication,
  type SqlLifecyclePublicationInput,
} from "@yielded/agent-storage-sql/sql-lifecycle-publication";
import { makeSqlMessageDeliveryStore } from "@yielded/agent-storage-sql/sql-message-delivery-store";
import {
  SqlStorageProgress,
  type SqlStorageProgressKind,
} from "@yielded/agent-storage-sql/sql-storage-progress";
import { lifecyclePublicationLayer } from "@yielded/agent/lifecycle-publication";
import { MessageDeliveryRecord } from "@yielded/agent/message-delivery";
import { SqlStorageOwner } from "@yielded/agent/sql-memory-store";
import {
  makeMessageDeliveryFixture,
  messageDeliveryStoreConformanceCases,
} from "@yielded/agent/testing/message-delivery-store-conformance";
import { DateTime, Effect, Layer, Option, Result, Schema, Tracer } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { expect, it } from "vite-plus/test";

import { doMessageDeliveryStoreLayer } from "../src/DoMessageDeliveryStore.ts";
import { DoStorageConfig, DoStorageConfigValue } from "../src/DoStorageConfig.ts";
import { DoStorageFailpoint } from "../src/DoStorageFailpoint.ts";
import { initializeDoJournal } from "../src/internal/do-journal.ts";
import { withThreadStorage } from "./harness.ts";

const storeLayer = (storage: DurableObjectStorage) =>
  doMessageDeliveryStoreLayer().pipe(
    Layer.provide([
      SqliteClient.layer({ storage }),
      Layer.succeed(
        DoStorageConfig,
        DoStorageConfigValue.make({
          observationPollInterval: 1,
          ownershipLeaseDuration: 30_000,
          maxStoredValueBytes: 1_900_000,
          verifyOnOpen: false,
        }),
      ),
      DoStorageFailpoint.layer,
    ]),
  );

const publicationInput = (
  record: MessageDeliveryRecord,
  millis = 0,
): SqlLifecyclePublicationInput => ({
  ownerThreadId: record.key.ownerThreadId,
  createdAt: DateTime.makeUnsafe(millis),
  fact: {
    _tag: "DeliveryRetained",
    key: record.key,
    envelope: record.envelope,
    createdAtMillis: record.createdAtMillis,
  },
});

// The exclusive-owner summary must include parked/future heads, survive rollback,
// and fall back to SQL rather than treating a bounded prefix as a complete inventory.
it("reuses complete lifecycle heads and rebuilds them after rollback", () => {
  const spans: Array<Tracer.Span> = [];

  const tracer = Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options);

      spans.push(span);

      return span;
    },
  });

  return withThreadStorage(`lifecycle-heads-${crypto.randomUUID()}`, (storage) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const journal = yield* initializeDoJournal(sql, () => Effect.void, 1_900_000);

      const lifecycle = yield* makeSqlLifecyclePublication().pipe(
        Effect.provideService(SqlStorageOwner, journal.owner),
      );

      if (lifecycle === undefined) return yield* Effect.fail("Missing lifecycle storage");
      const parked = yield* makeMessageDeliveryFixture("parked", "parked");
      const later = yield* makeMessageDeliveryFixture("later", "parked");
      const active = yield* makeMessageDeliveryFixture("active", "active");
      const future = yield* makeMessageDeliveryFixture("future", "\uE000");
      const astral = yield* makeMessageDeliveryFixture("astral", "\u{10000}");

      yield* journal.state.transaction(
        lifecycle.retainMany([
          publicationInput(parked),
          publicationInput(active, 1),
          publicationInput(astral, 500_000),
          publicationInput(future, 500_000),
        ]),
      );
      const parkedBatch = (yield* lifecycle.storage.pending(0, 1))[0]!;
      let now = 0;

      for (const delay of [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, null]) {
        expect(yield* lifecycle.storage.claim(parkedBatch, now, 10)).toBe(true);
        if (delay !== null) now += delay + 10;
      }
      yield* journal.state.transaction(lifecycle.retainMany([publicationInput(later)]));
      const pending = yield* lifecycle.storage.pending(1_000_000, 10);

      expect(pending.map((batch) => batch[0].ownerThreadId)).toEqual([
        active.key.ownerThreadId,
        future.key.ownerThreadId,
        astral.key.ownerThreadId,
      ]);
      expect(yield* lifecycle.storage.pendingDeadline).toEqual(Option.some(1));
      spans.length = 0;
      expect(yield* lifecycle.storage.pending(1_000_000, 10)).toEqual(pending);
      expect(yield* lifecycle.storage.pendingDeadline).toEqual(Option.some(1));

      const warmQueries = spans
        .filter((span) => span.name === "sql.execute")
        .map((span) => span.attributes.get("db.query.text"));

      yield* journal.state
        .transaction(
          lifecycle.storage.acknowledge(pending[0]!).pipe(Effect.andThen(Effect.fail("rollback"))),
        )
        .pipe(Effect.result);
      expect(yield* lifecycle.storage.pending(1_000_000, 10)).toEqual(pending);
      expect(yield* lifecycle.storage.pendingDeadline).toEqual(Option.some(1));

      // The 129th owner is the earliest due head; it must survive the bounded read.
      const overflow = yield* Effect.forEach(
        Array.from({ length: 129 }, (_, index) => index),
        (index) => makeMessageDeliveryFixture("overflow", `z-${String(index).padStart(3, "0")}`),
      );

      yield* journal.state.transaction(
        lifecycle.retainMany(
          overflow.map((record, index) => publicationInput(record, index === 128 ? 0 : 2_000_000)),
        ),
      );
      expect((yield* lifecycle.storage.pending(0, 1))[0]![0].ownerThreadId).toBe(
        overflow[128]!.key.ownerThreadId,
      );
      expect(yield* lifecycle.storage.pendingDeadline).toEqual(Option.some(0));
      expect(warmQueries).toEqual([]);
    }).pipe(
      Effect.provide([
        SqliteClient.layer({ storage }),
        BrowserCrypto.layer,
        lifecyclePublicationLayer.pipe(Layer.provide(BrowserCrypto.layer)),
      ]),
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.withTracerEnabled(true),
    ),
  );
});

// Ack must renew actual progress with a future head, but clearing the final timed
// head must not renew a claimed lane. Fresh producer enrollment remains independent.
it("enrolls lifecycle acknowledgement progress only while timed heads remain", () => {
  const progress: Array<SqlStorageProgressKind> = [];

  return withThreadStorage(`lifecycle-ack-progress-${crypto.randomUUID()}`, (storage) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const journal = yield* initializeDoJournal(sql, () => Effect.void, 1_900_000);

      const lifecycle = yield* makeSqlLifecyclePublication().pipe(
        Effect.provideService(SqlStorageOwner, journal.owner),
      );

      if (lifecycle === undefined) return yield* Effect.fail("Missing lifecycle storage");
      const active = yield* makeMessageDeliveryFixture("active", "active");
      const future = yield* makeMessageDeliveryFixture("future", "future");

      yield* journal.state.transaction(
        lifecycle.retainMany([publicationInput(active), publicationInput(future, 500_000)]),
      );
      const batch = (yield* lifecycle.storage.pending(0, 1))[0]!;

      progress.length = 0;
      yield* lifecycle.storage.acknowledge(batch);
      expect(progress).toEqual(["lifecycle-ack"]);
      expect(yield* lifecycle.storage.pendingDeadline).toEqual(Option.some(500_000));
      const last = (yield* lifecycle.storage.pending(500_000, 1))[0]!;

      progress.length = 0;
      yield* lifecycle.storage.acknowledge(last);
      expect(progress).toEqual([]);
      expect(yield* lifecycle.storage.pendingDeadline).toEqual(Option.none());
      yield* journal.state.transaction(lifecycle.retainMany([publicationInput(active)]));
      expect(progress).toEqual(["lifecycle"]);
      expect(yield* lifecycle.storage.pending(0, 1)).toHaveLength(1);
    }).pipe(
      Effect.provide([
        SqliteClient.layer({ storage }),
        BrowserCrypto.layer,
        lifecyclePublicationLayer.pipe(Layer.provide(BrowserCrypto.layer)),
      ]),
      Effect.provideService(SqlStorageProgress, {
        committed: (kind) =>
          Effect.sync(() => {
            progress.push(kind);
          }),
      }),
    ),
  );
});

// Regression: 405916b0 shared decoded pending rows without their persisted byte bound.
// Single-adapter conformance cannot detect a second adapter bypassing its smaller limit.
it("applies the consuming adapter's stored-value bound to a shared pending view", () =>
  withThreadStorage(`message-view-limit-${crypto.randomUUID()}`, (storage) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const journal = yield* initializeDoJournal(sql, () => Effect.void, 1_900_000);
      const record = yield* makeMessageDeliveryFixture();
      const text = Schema.encodeSync(Schema.fromJsonString(MessageDeliveryRecord))(record);
      const size = new TextEncoder().encode(text).byteLength;

      // Transaction decorators retain the physical owner's cache identity.
      const owner = () => ({
        identity: journal.state,
        read: journal.state.read,
        transaction: journal.state.transaction,
        invalidators: journal.state.invalidators,
      });

      const higher = yield* makeSqlMessageDeliveryStore(undefined, {
        maxStoredValueBytes: size + 1,
      }).pipe(Effect.provideService(SqlStorageOwner, owner()));

      const lower = yield* makeSqlMessageDeliveryStore(undefined, {
        maxStoredValueBytes: size - 1,
      }).pipe(Effect.provideService(SqlStorageOwner, owner()));

      // Seed the other adapter's empty view before the source commit.
      yield* lower.list({ ownerThreadId: record.key.ownerThreadId, pendingOnly: true, limit: 1 });
      yield* higher.insert(record);
      yield* higher.list({ ownerThreadId: record.key.ownerThreadId, pendingOnly: true, limit: 1 });
      const direct = yield* lower.get(record.key).pipe(Effect.result);

      const cached = yield* lower
        .list({ ownerThreadId: record.key.ownerThreadId, pendingOnly: true, limit: 1 })
        .pipe(Effect.result);

      expect(Result.isFailure(direct) && direct.failure.operation).toBe("stored-value-bytes");
      expect(Result.isFailure(cached) && cached.failure.operation).toBe("stored-value-bytes");
    }).pipe(Effect.provide([SqliteClient.layer({ storage }), BrowserCrypto.layer])),
  ));

for (const [index, testCase] of messageDeliveryStoreConformanceCases.entries()) {
  it(String(testCase.name), () =>
    expect(
      withThreadStorage(`message-conformance-${index}`, (storage) =>
        testCase.run.pipe(Effect.provide([storeLayer(storage), BrowserCrypto.layer])),
      ),
    ).resolves.toBeUndefined(),
  );
}

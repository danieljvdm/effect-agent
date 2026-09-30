import { makeSqlMessageDeliveryStore } from "@effect-agent/storage-sql/sql-message-delivery-store";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer, Result, Schema } from "effect";
import { MessageDeliveryRecord } from "effect-agent/message-delivery";
import { SqlStorageOwner } from "effect-agent/sql-memory-store";
import {
  makeMessageDeliveryFixture,
  messageDeliveryStoreConformanceCases,
} from "effect-agent/testing/message-delivery-store-conformance";
import * as SqlClient from "effect/unstable/sql/SqlClient";
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

      const higher = yield* makeSqlMessageDeliveryStore(undefined, {
        maxStoredValueBytes: size + 1,
      }).pipe(Effect.provideService(SqlStorageOwner, journal.state));

      const lower = yield* makeSqlMessageDeliveryStore(undefined, {
        maxStoredValueBytes: size - 1,
      }).pipe(Effect.provideService(SqlStorageOwner, journal.state));

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

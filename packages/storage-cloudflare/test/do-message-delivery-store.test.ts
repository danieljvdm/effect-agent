import { MessageDeliveryStore } from "@effect-agent/thread/MessageDelivery";
import {
  makeMessageDeliveryFixture,
  messageDeliveryStoreConformanceCases,
} from "@effect-agent/thread/testing/MessageDeliveryStoreConformance";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer } from "effect";
import { expect, it } from "vite-plus/test";

import { doMessageDeliveryStoreLayer } from "../src/DoMessageDeliveryStore.ts";
import { DoStorageConfig, DoStorageConfigValue } from "../src/DoStorageConfig.ts";
import { DoStorageFailpoint } from "../src/DoStorageFailpoint.ts";
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

for (const [index, testCase] of messageDeliveryStoreConformanceCases.entries()) {
  it(String(testCase.name), () =>
    expect(
      withThreadStorage(`message-conformance-${index}`, (storage) =>
        testCase.run.pipe(Effect.provide([storeLayer(storage), BrowserCrypto.layer])),
      ),
    ).resolves.toBeUndefined(),
  );
}

it("reconstructs pending obligations from Thread Object SQL with no active ledger work", () =>
  expect(
    withThreadStorage("message-reopen", (storage) =>
      Effect.gen(function* () {
        const record = yield* makeMessageDeliveryFixture();

        yield* Effect.gen(function* () {
          const store = yield* MessageDeliveryStore;

          yield* store.insert(record);
        }).pipe(Effect.provide(storeLayer(storage)));
        yield* Effect.gen(function* () {
          const store = yield* MessageDeliveryStore;

          expect(yield* store.get(record.key)).toEqual(record);
          expect(yield* store.due(0, 10)).toEqual([record.key]);
          expect(yield* store.nextDeadline(record.key.ownerThreadId)).toBe(0);
        }).pipe(Effect.provide(storeLayer(storage)));
      }).pipe(Effect.provide(BrowserCrypto.layer)),
    ),
  ).resolves.toBeUndefined());

import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer } from "effect";
import { messageDeliveryStoreConformanceCases } from "effect-agent/testing/message-delivery-store-conformance";
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

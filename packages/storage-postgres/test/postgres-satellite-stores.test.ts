import * as PostgresMessageDeliveryStore from "@effect-agent/storage-postgres/postgres-message-delivery-store";
import * as PostgresScheduleStore from "@effect-agent/storage-postgres/postgres-schedule-store";
import {
  PostgresStorageConfig,
  PostgresStorageConfigValue,
} from "@effect-agent/storage-postgres/postgres-storage-config";
import { PostgresStorageFailpoint } from "@effect-agent/storage-postgres/postgres-storage-failpoint";
import * as PostgresSubscriptionStore from "@effect-agent/storage-postgres/postgres-subscription-store";
import { NodeCrypto } from "@effect/platform-node";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { messageDeliveryStoreConformanceCases } from "effect-agent/testing/message-delivery-store-conformance";
import { scheduleStoreConformanceCases } from "effect-agent/testing/schedule-store-conformance";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "effect-agent/testing/subscription-store-conformance";

import { clientLayer, withTemporaryDatabase } from "./harness.ts";

const storageServices = (url: string) =>
  Layer.mergeAll(
    Layer.succeed(PostgresStorageConfig)(
      PostgresStorageConfigValue.make({
        observationPollInterval: 1,
        lockTimeout: 5_000,
        ownershipLeaseDuration: 30_000,
        verifyOnOpen: false,
        schema: "public",
      }),
    ),
    PostgresStorageFailpoint.layer,
    clientLayer(url),
  );

describe("PostgresScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(
          Effect.provide(PostgresScheduleStore.layer.pipe(Layer.provide(storageServices(url)))),
        ),
      ),
    );
  }
});

describe("PostgresSubscriptionStore", () => {
  for (const conformanceCase of subscriptionStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(
          Effect.provide(
            PostgresSubscriptionStore.layer(subscriptionConformancePartition).pipe(
              Layer.provide(storageServices(url)),
            ),
          ),
        ),
      ),
    );
  }
});

describe("PostgresMessageDeliveryStore", () => {
  for (const conformanceCase of messageDeliveryStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(
          Effect.provide(
            Layer.mergeAll(
              PostgresMessageDeliveryStore.layer().pipe(Layer.provide(storageServices(url))),
              NodeCrypto.layer,
            ),
          ),
        ),
      ),
    );
  }
});

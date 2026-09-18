import { messageDeliveryStoreLayer } from "@effect-agent/storage-postgres/postgres-message-delivery-store";
import { scheduleStoreLayer } from "@effect-agent/storage-postgres/postgres-schedule-store";
import { storageClientLayer } from "@effect-agent/storage-postgres/postgres-storage-client";
import {
  PostgresStorageConfig,
  PostgresStorageConfigValue,
} from "@effect-agent/storage-postgres/postgres-storage-config";
import { PostgresStorageFailpoint } from "@effect-agent/storage-postgres/postgres-storage-failpoint";
import { subscriptionStoreLayer } from "@effect-agent/storage-postgres/postgres-subscription-store";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Redacted } from "effect";
import { messageDeliveryStoreConformanceCases } from "effect-agent/testing/message-delivery-store-conformance";
import { scheduleStoreConformanceCases } from "effect-agent/testing/schedule-store-conformance";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "effect-agent/testing/subscription-store-conformance";

const adminUrl =
  process.env.EFFECT_AGENT_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:55432/effect_agent";

const databaseUrl = (database: string) => {
  const url = new URL(adminUrl);

  url.pathname = `/${database}`;

  return url.toString();
};

let databaseCounter = 0;

/** A database per case, for the reason given in the thread store's suite. */
const withTemporaryDatabase = <A, E>(
  use: (url: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.gen(function* () {
    databaseCounter = databaseCounter + 1;
    const database = `effect_agent_sat_${process.pid}_${databaseCounter}`;

    yield* Effect.gen(function* () {
      const sql = yield* PgClient.PgClient;

      return yield* sql.unsafe(`CREATE DATABASE ${database}`);
    }).pipe(
      Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })),
      Effect.orDie,
    );

    return yield* use(databaseUrl(database));
  });

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
    storageClientLayer({ url: Redacted.make(url) }),
  );

describe("PostgresScheduleStore", () => {
  for (const conformanceCase of scheduleStoreConformanceCases) {
    it.effect(conformanceCase.name, () =>
      withTemporaryDatabase((url) =>
        conformanceCase.run.pipe(
          Effect.provide(scheduleStoreLayer.pipe(Layer.provide(storageServices(url)))),
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
            subscriptionStoreLayer(subscriptionConformancePartition).pipe(
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
              messageDeliveryStoreLayer().pipe(Layer.provide(storageServices(url))),
              NodeCrypto.layer,
            ),
          ),
        ),
      ),
    );
  }
});

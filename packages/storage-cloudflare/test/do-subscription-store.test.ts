import { SubscriptionError, SubscriptionStore } from "@effect-agent/thread";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "@effect-agent/thread/testing";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer } from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import { DoSubscriptionTransaction, doSubscriptionStoreLayer } from "../src/index.ts";
import { withScheduleStorage } from "./harness.ts";

let objectCounter = 0;

describe("Durable Object SubscriptionStore conformance", () => {
  for (const testCase of subscriptionStoreConformanceCases) {
    // oxlint-disable-next-line vitest/valid-title -- exported contract cases own their names
    it(String(testCase.name), () =>
      expect(
        withScheduleStorage(`subscription-store-${objectCounter++}`, (storage) =>
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            const transaction = DoSubscriptionTransaction.of({
              run: (body) =>
                sql
                  .withTransaction(body(() => Effect.void))
                  .pipe(
                    Effect.catchTag("SqlError", () =>
                      SubscriptionError.make({ reason: "storage", code: "test-transaction" }),
                    ),
                  ),
            });
            const dependencies = Layer.merge(
              Layer.succeed(SqlClientService.SqlClient)(sql),
              Layer.succeed(DoSubscriptionTransaction)(transaction),
            );
            yield* testCase.run.pipe(
              Effect.provide(
                doSubscriptionStoreLayer(subscriptionConformancePartition).pipe(
                  Layer.provide(dependencies),
                ),
              ),
            );
          }).pipe(Effect.provide(Layer.merge(SqliteClient.layer({ storage }), TestClock.layer()))),
        ),
      ).resolves.toBeUndefined(),
    );
  }

  it("rejects the Conversation-era subscription version without mutation", () =>
    expect(
      withScheduleStorage("subscription-store-conversation-era-version", (storage) =>
        Effect.gen(function* () {
          const sql = yield* SqlClientService.SqlClient;
          yield* sql`
            CREATE TABLE effect_agent_subscription_store_state (
              singleton INTEGER PRIMARY KEY NOT NULL,
              storage_version INTEGER NOT NULL,
              alarm_generation INTEGER NOT NULL
            )
          `.withoutTransform;
          for (const table of [
            "effect_agent_subscription_sequences",
            "effect_agent_subscriptions",
            "effect_agent_subscription_events",
            "effect_agent_subscription_deliveries",
          ]) {
            yield* sql.unsafe(`CREATE TABLE ${table} (sentinel TEXT)`);
          }
          yield* sql`
            INSERT INTO effect_agent_subscription_store_state (
              singleton, storage_version, alarm_generation
            ) VALUES (1, 1, 9)
          `.withoutTransform;

          const transaction = DoSubscriptionTransaction.of({
            run: (body) =>
              sql
                .withTransaction(body(() => Effect.void))
                .pipe(
                  Effect.catchTag("SqlError", () =>
                    SubscriptionError.make({ reason: "storage", code: "test-transaction" }),
                  ),
                ),
          });
          const dependencies = Layer.merge(
            Layer.succeed(SqlClientService.SqlClient)(sql),
            Layer.succeed(DoSubscriptionTransaction)(transaction),
          );
          const failure = yield* SubscriptionStore.pipe(
            Effect.provide(
              doSubscriptionStoreLayer(subscriptionConformancePartition).pipe(
                Layer.provide(dependencies),
              ),
            ),
            Effect.flip,
          );
          expect(failure).toMatchObject({
            _tag: "SubscriptionError",
            reason: "corrupt",
            code: "incompatible subscription storage version 1; expected 2",
          });

          const state = yield* sql<Record<string, unknown>>`
            SELECT storage_version, alarm_generation
            FROM effect_agent_subscription_store_state
          `;
          expect(state).toEqual([{ storage_version: 1, alarm_generation: 9 }]);
        }).pipe(Effect.provide(Layer.merge(SqliteClient.layer({ storage }), TestClock.layer()))),
      ),
    ).resolves.toBeUndefined());
});

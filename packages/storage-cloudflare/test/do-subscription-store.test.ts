import { SubscriptionError } from "@effect-agent/session";
import {
  subscriptionConformancePartition,
  subscriptionStoreConformanceCases,
} from "@effect-agent/session/testing";
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
          }).pipe(
            Effect.provide(SqliteClient.layer({ storage })),
            Effect.provide(TestClock.layer()),
          ),
        ),
      ).resolves.toBeUndefined(),
    );
  }
});

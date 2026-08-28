import { ScheduleStorageError } from "@effect-agent/session";
import { scheduleStoreConformanceCases } from "@effect-agent/session/testing";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import { DoScheduleTransaction, scheduleStoreLayer } from "../src/index.ts";
import { withScheduleStorage } from "./harness.ts";

let objectCounter = 0;

describe("Durable Object ScheduleStore conformance", () => {
  for (const testCase of scheduleStoreConformanceCases) {
    // oxlint-disable-next-line vitest/valid-title -- exported contract cases own their names
    it(String(testCase.name), () => {
      return expect(
        withScheduleStorage(`schedule-store-${objectCounter++}`, (storage) =>
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            const transaction = DoScheduleTransaction.of({
              run: (body) =>
                sql.withTransaction(body(() => Effect.void)).pipe(
                  Effect.catchTag("SqlError", () =>
                    ScheduleStorageError.make({
                      operation: "test ScheduleStore transaction",
                      reason: "unavailable",
                    }),
                  ),
                ),
            });
            const dependencies = Layer.merge(
              Layer.succeed(SqlClientService.SqlClient)(sql),
              Layer.succeed(DoScheduleTransaction)(transaction),
            );
            yield* testCase.run.pipe(
              Effect.provide(scheduleStoreLayer.pipe(Layer.provide(dependencies))),
            );
          }).pipe(Effect.provide(SqliteClient.layer({ storage }))),
        ),
      ).resolves.toBeUndefined();
    });
  }
});

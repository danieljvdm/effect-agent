import { ScheduleStorageError, ScheduleStore } from "@effect-agent/thread";
import { scheduleStoreConformanceCases } from "@effect-agent/thread/testing";
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

  it("rejects the Conversation-era schedule version without mutation", () =>
    expect(
      withScheduleStorage("schedule-store-conversation-era-version", (storage) =>
        Effect.gen(function* () {
          const sql = yield* SqlClientService.SqlClient;
          yield* sql`
            CREATE TABLE effect_agent_schedule_store_state (
              singleton INTEGER PRIMARY KEY NOT NULL,
              storage_version INTEGER NOT NULL,
              alarm_generation INTEGER NOT NULL
            )
          `.withoutTransform;
          yield* sql`
            CREATE TABLE effect_agent_schedules (
              schedule_id TEXT PRIMARY KEY NOT NULL
            )
          `.withoutTransform;
          yield* sql`
            INSERT INTO effect_agent_schedule_store_state (
              singleton, storage_version, alarm_generation
            ) VALUES (1, 1, 7)
          `.withoutTransform;

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
          const failure = yield* ScheduleStore.pipe(
            Effect.provide(scheduleStoreLayer.pipe(Layer.provide(dependencies))),
            Effect.flip,
          );
          expect(failure).toMatchObject({
            _tag: "ScheduleStorageError",
            reason: "corrupt",
          });
          expect(failure.operation).toContain("incompatible storage version 1; expected 2");

          const state = yield* sql<Record<string, unknown>>`
            SELECT storage_version, alarm_generation
            FROM effect_agent_schedule_store_state
          `;
          expect(state).toEqual([{ storage_version: 1, alarm_generation: 7 }]);
        }).pipe(Effect.provide(SqliteClient.layer({ storage }))),
      ),
    ).resolves.toBeUndefined());
});

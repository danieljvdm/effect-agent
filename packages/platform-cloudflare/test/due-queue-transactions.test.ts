import { SqliteClient } from "@effect/sql-sqlite-do";
import { runInDurableObject } from "cloudflare:test";
import { Effect, Exit, Layer } from "effect";
import { DurableObject } from "effect-cf";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { expect, it } from "vite-plus/test";

import { instrumentedStorage } from "../../../test/fixtures/instrumented-storage.ts";
import { ThreadMutationGate } from "../src/Alarm.ts";
import { DurableObjectContext } from "../src/CloudflareBindings.ts";
import * as DueQueue from "../src/internal/due-queue.ts";
import { stubFor } from "./harness.ts";

// Requested scheduler seam: one write per changed lane in a source transaction.
// Real SQLite is needed to distinguish speculative scheduling from committed recovery state.
it("coalesces source intent and reads it once even above the warm cache limit", () =>
  runInDurableObject(stubFor(`queue-coalesce-${crypto.randomUUID()}`), (instance, state) =>
    instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const context = yield* DurableObjectContext;
        const queries: Array<string> = [];
        const future = Date.now() + 86_400_000;
        const storage = instrumentedStorage(context.ctx.storage, (query) => queries.push(query));

        const ctx = new Proxy(context.ctx, {
          get(target, property) {
            if (property === "storage") return storage;
            const value = Reflect.get(target, property, target);

            return typeof value === "function" ? value.bind(target) : value;
          },
        });

        yield* Effect.gen(function* () {
          const gate = yield* ThreadMutationGate;
          const sql = yield* SqlClient;

          for (let index = 0; index < 129; index++)
            storage.sql.exec(
              "INSERT INTO platform_cloudflare_due_queue (id, revision, dueAt, stalls) VALUES (?, 0, NULL, 0)",
              `test:retained:${index}`,
            );
          DueQueue.invalidate(storage);
          queries.length = 0;
          yield* sql.withTransaction(
            Effect.gen(function* () {
              yield* gate.schedule("test:coalesced", future + 3_000, 1n);
              yield* gate.schedule("test:coalesced", future + 1_000, 3n);
              yield* gate.schedule("test:coalesced", future + 2_000, 2n);
              yield* gate.schedule("test:coalesced", future + 4_000, 4n);
              expect(
                DueQueue.make(storage)
                  .read()
                  .find((row) => row.id === "test:coalesced"),
              ).toMatchObject({ dueAt: future + 1_000, progressKey: "4" });
              yield* gate.schedule("test:coalesced", future + 500);
              expect(
                DueQueue.make(storage)
                  .read()
                  .find((row) => row.id === "test:coalesced"),
              ).toMatchObject({ dueAt: future + 500, progressKey: "4" });
            }),
          );
          expect(queries.filter((query) => /^(INSERT|UPDATE)/.test(query))).toHaveLength(1);
          expect(queries.filter((query) => query.startsWith("SELECT"))).toHaveLength(1);
          DueQueue.invalidate(storage);
          expect(
            DueQueue.make(storage)
              .read()
              .find((row) => row.id === "test:coalesced"),
          ).toMatchObject({ dueAt: future + 500, progressKey: "4" });
        }).pipe(
          Effect.provide(Layer.fresh(ThreadMutationGate.layer)),
          Effect.provide(SqliteClient.layer({ storage })),
          Effect.provideService(DurableObjectContext, { ...context, ctx }),
        );
      }).pipe(Effect.ensuring(Effect.promise(() => state.storage.deleteAlarm()))),
    ),
  ));

// Requested rollback/crash seam: source facts and their due intent commit together.
// A caught child rollback must preserve the parent's unflushed intent.
it("discards aborted intent and preserves parent intent across child rollback", () =>
  runInDurableObject(stubFor(`queue-rollback-${crypto.randomUUID()}`), (instance, state) =>
    instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const gate = yield* ThreadMutationGate;
        const sql = yield* SqlClient;
        const queue = DueQueue.make(state.storage);
        const future = Date.now() + 86_400_000;

        for (const abort of [Effect.fail("source failed"), Effect.interrupt]) {
          const result = yield* sql
            .withTransaction(
              gate.schedule("test:aborted", future + 100, 9n).pipe(Effect.andThen(abort)),
            )
            .pipe(Effect.exit);

          expect(Exit.isFailure(result)).toBe(true);
          expect(queue.read().find((row) => row.id === "test:aborted")).toBeUndefined();
        }
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* gate.schedule("test:parent", future + 700, 5n);
            yield* sql
              .withTransaction(
                Effect.gen(function* () {
                  yield* gate.schedule("test:parent", future + 100, 6n);
                  yield* gate.schedule("test:child", future + 100, 1n);

                  return yield* Effect.fail("child failed");
                }),
              )
              .pipe(Effect.exit);
            expect(queue.read().find((row) => row.id === "test:parent")).toMatchObject({
              dueAt: future + 700,
              progressKey: "5",
            });
            expect(queue.read().find((row) => row.id === "test:child")).toBeUndefined();
          }),
        );
        DueQueue.invalidate(state.storage);
        expect(queue.read().find((row) => row.id === "test:parent")).toMatchObject({
          dueAt: future + 700,
          progressKey: "5",
        });
        expect(queue.read().find((row) => row.id === "test:child")).toBeUndefined();
      }).pipe(Effect.ensuring(Effect.promise(() => state.storage.deleteAlarm()))),
    ),
  ));

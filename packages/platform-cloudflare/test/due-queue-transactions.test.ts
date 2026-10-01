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

// The library's empty required-publication handler has no debt to enroll.
// Custom handlers still use the publication and retry-barrier workflows.
it("keeps an idle default publication lane idle across source progress", () =>
  runInDurableObject(stubFor(`empty-publication-${crypto.randomUUID()}`), (instance, state) =>
    instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const gate = yield* ThreadMutationGate;
        const queue = DueQueue.make(state.storage);
        const row = queue.read().find((lane) => lane.id === DueQueue.Publication);

        if (row === undefined) return yield* Effect.fail("Missing publication lane");
        yield* Effect.promise(() => queue.transaction(async () => queue.complete(row, null)));
        const idle = queue.read().find((lane) => lane.id === DueQueue.Publication);

        yield* gate.withMutation(
          gate.withTransaction(gate.recordProgress([DueQueue.Native, DueQueue.Publication])),
        );
        expect(queue.read().find((lane) => lane.id === DueQueue.Publication)).toEqual(idle);
        expect(queue.read().find((lane) => lane.id === DueQueue.Native)?.state).toBe("pending");
      }).pipe(Effect.ensuring(Effect.promise(() => state.storage.deleteAlarm()))),
    ),
  ));

// Requested scheduler seam: one write for changed lanes in a source transaction.
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
          const client = yield* SqlClient;
          const originalTransaction = client.withTransaction;

          yield* Effect.gen(function* () {
            const gate = yield* ThreadMutationGate;
            const sql = yield* SqlClient;

            expect(sql.withTransaction).toBe(originalTransaction);
            for (let index = 0; index < 129; index++)
              storage.sql.exec(
                "INSERT INTO platform_cloudflare_due_queue (id, revision, dueAt, stalls) VALUES (?, 0, NULL, 0)",
                `test:retained:${index}`,
              );
            DueQueue.invalidate(storage);
            queries.length = 0;
            yield* gate.withTransaction(
              Effect.gen(function* () {
                // Prearming must commit before work, outside the source transaction.
                let ranMutation = false;

                yield* gate
                  .withMutation(
                    Effect.sync(() => {
                      ranMutation = true;
                    }),
                  )
                  .pipe(Effect.exit);
                expect(ranMutation).toBe(false);
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
                yield* gate.schedule("test:second", future + 700, 1n);
                expect(
                  DueQueue.make(storage)
                    .read()
                    .find((row) => row.id === "test:coalesced"),
                ).toMatchObject({ dueAt: future + 500, progressKey: "4" });
              }),
            );
            expect(queries.filter((query) => /\b(INSERT|UPDATE)\b/.test(query))).toHaveLength(1);
            expect(queries.filter((query) => query.startsWith("SELECT"))).toHaveLength(1);
            DueQueue.invalidate(storage);
            expect(
              DueQueue.make(storage)
                .read()
                .find((row) => row.id === "test:second"),
            ).toMatchObject({ dueAt: future + 700, progressKey: "1" });
            expect(
              DueQueue.make(storage)
                .read()
                .find((row) => row.id === "test:coalesced"),
            ).toMatchObject({ dueAt: future + 500, progressKey: "4" });
          }).pipe(Effect.provide(Layer.fresh(ThreadMutationGate.layer)));
        }).pipe(
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
          const result = yield* gate
            .withTransaction(
              gate.schedule("test:aborted", future + 100, 9n).pipe(Effect.andThen(abort)),
            )
            .pipe(Effect.exit);

          expect(Exit.isFailure(result)).toBe(true);
          expect(queue.read().find((row) => row.id === "test:aborted")).toBeUndefined();
        }
        yield* gate.withTransaction(
          Effect.gen(function* () {
            yield* gate.schedule("test:parent", future + 700, 5n);
            yield* gate
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
            // An unwrapped child cannot leave intent in its parent's buffer after rollback.
            yield* sql
              .withTransaction(
                gate
                  .schedule("test:unwrapped-child", future + 50, 1n)
                  .pipe(Effect.andThen(Effect.fail("unwrapped child failed"))),
              )
              .pipe(Effect.exit);
            expect(queue.read().find((row) => row.id === "test:unwrapped-child")).toBeUndefined();
            yield* sql
              .withTransaction(
                gate
                  .withTransaction(gate.schedule("test:grandchild", future + 50, 1n))
                  .pipe(Effect.andThen(Effect.fail("intervening child failed"))),
              )
              .pipe(Effect.exit);
            expect(queue.read().find((row) => row.id === "test:grandchild")).toBeUndefined();
          }),
        );
        yield* sql
          .withTransaction(
            gate
              .withTransaction(gate.schedule("test:raw-parent", future + 50, 1n))
              .pipe(Effect.andThen(Effect.fail("raw parent failed"))),
          )
          .pipe(Effect.exit);
        expect(queue.read().find((row) => row.id === "test:raw-parent")).toBeUndefined();
        yield* gate.withTransaction(gate.schedule("test:stale", future + 900, 1n));

        const conflict = yield* gate
          .withTransaction(
            Effect.gen(function* () {
              yield* gate.schedule("test:atomic-new", future + 100, 1n);
              yield* gate.schedule("test:stale", future + 100, 2n);
              // A revision changed outside the buffered view rejects the entire source commit.
              yield* sql`UPDATE platform_cloudflare_due_queue SET revision = revision + 1 WHERE id = ${"test:stale"}`;
            }),
          )
          .pipe(Effect.exit);

        expect(Exit.isFailure(conflict)).toBe(true);
        DueQueue.invalidate(state.storage);
        expect(queue.read().find((row) => row.id === "test:atomic-new")).toBeUndefined();
        expect(queue.read().find((row) => row.id === "test:stale")).toMatchObject({
          dueAt: future + 900,
          progressKey: "1",
          revision: 1,
        });
        expect(queue.read().find((row) => row.id === "test:parent")).toMatchObject({
          dueAt: future + 700,
          progressKey: "5",
        });
        expect(queue.read().find((row) => row.id === "test:child")).toBeUndefined();
      }).pipe(Effect.ensuring(Effect.promise(() => state.storage.deleteAlarm()))),
    ),
  ));

import { SqliteClient } from "@effect/sql-sqlite-do";
import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import type { ProbeDurableObject } from "./probe-worker.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      PROBE: DurableObjectNamespace<ProbeDurableObject>;
    }
  }
}

class ProbeRollback extends Schema.TaggedError<ProbeRollback>()("ProbeRollback", {}) {}

/**
 * WP0 probe 1: the pinned `@effect/sql-sqlite-do` client executes DDL/DML and
 * storage-backed `withTransaction` (commit and rollback) against a real
 * SQLite-backed Durable Object's storage inside workerd.
 */
const sqlProbe = Effect.gen(function* () {
  const sql = yield* SqlClientService.SqlClient;

  yield* sql`CREATE TABLE IF NOT EXISTS wp0_probe (id INTEGER PRIMARY KEY, label TEXT NOT NULL)`;
  yield* sql`INSERT INTO wp0_probe (id, label) VALUES (${1}, ${"plain-dml"})`;

  yield* sql.withTransaction(sql`INSERT INTO wp0_probe (id, label) VALUES (${2}, ${"committed"})`);

  const rollbackFailure = yield* sql
    .withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO wp0_probe (id, label) VALUES (${3}, ${"rolled-back"})`;
        return yield* ProbeRollback.make();
      }),
    )
    .pipe(Effect.flip);

  const rows = yield* sql<{
    id: number;
    label: string;
  }>`SELECT id, label FROM wp0_probe ORDER BY id`;

  return { rollbackFailure, rows };
});

describe("@effect/sql-sqlite-do inside a SQLite-backed Durable Object (WP0 probe 1)", () => {
  it("executes DDL/DML and storage-backed withTransaction with commit and rollback", async () => {
    const stub = env.PROBE.get(env.PROBE.idFromName("wp0-sql-probe"));

    const result = await runInDurableObject(stub, (_instance, state) =>
      Effect.runPromise(
        sqlProbe.pipe(Effect.provide(SqliteClient.layer({ storage: state.storage }))),
      ),
    );

    expect(result.rollbackFailure._tag).toBe("ProbeRollback");
    expect(result.rows).toEqual([
      { id: 1, label: "plain-dml" },
      { id: 2, label: "committed" },
    ]);
  });

  // The 0.21.x pool (vitest 4 line) dropped the old `isolatedStorage` option:
  // Durable Object storage is SHARED across tests within a run unless a test
  // explicitly calls `reset()`. Conformance harnesses must therefore mint a
  // unique Durable Object name per case (matching how the SQLite suites mint a
  // temporary database file per case) or reset between tests.
  it("shares DO storage across tests in a run: first write", async () => {
    const stub = env.PROBE.get(env.PROBE.idFromName("wp0-isolation-probe"));
    await runInDurableObject(stub, async (_instance, state) => {
      await state.storage.put("wp0:isolation", "written-by-earlier-test");
    });
  });

  it("shares DO storage across tests in a run: later read observes it", async () => {
    const stub = env.PROBE.get(env.PROBE.idFromName("wp0-isolation-probe"));
    const observed = await runInDurableObject(stub, (_instance, state) =>
      state.storage.get<string>("wp0:isolation"),
    );
    expect(observed).toBe("written-by-earlier-test");
  });

  it("delivers Durable Object alarms through runDurableObjectAlarm (WP3 harness lever)", async () => {
    const stub = env.PROBE.get(env.PROBE.idFromName("wp0-alarm-probe"));

    await runInDurableObject(stub, async (_instance, state) => {
      await state.storage.setAlarm(Date.now() + 60_000);
    });

    // Direct at-least-once delivery: the first call runs the scheduled alarm,
    // the second reports that none remains scheduled.
    expect(await runDurableObjectAlarm(stub)).toBe(true);
    expect(await runDurableObjectAlarm(stub)).toBe(false);

    const fires = await runInDurableObject(stub, (_instance, state) =>
      state.storage.get<number>("wp0:alarm-fires"),
    );
    expect(fires).toBe(1);
  });
});

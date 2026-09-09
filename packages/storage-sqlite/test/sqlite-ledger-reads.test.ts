import { SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, type Crypto } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  ledgerReadCases,
  reserveReadFixture,
} from "../../../test/fixtures/ledger-read-contracts.ts";
import { SqliteStorageFailpointError } from "../src/SqliteStorageError.ts";
import { SqliteStorageFailpoint } from "../src/SqliteStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/SqliteSubmissionLedger.ts";
import { storageConfigLayer } from "../src/SqliteThreadStore.ts";

const withFixture = <A, E>(
  effect: Effect.Effect<A, E, SubmissionLedger | SqlClient.SqlClient | Crypto.Crypto>,
  options?: {
    readonly reserved?: () => void;
    readonly failpoint?: (point: string) => Effect.Effect<void, SqliteStorageFailpointError>;
  },
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "ledger-reads-" });
      const filename = `${directory}/state.sqlite`;

      return yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        const observed = new Proxy(sql, {
          get(target, key, receiver) {
            if (key === "reserve")
              return sql.reserve.pipe(Effect.tap(() => Effect.sync(() => options?.reserved?.())));

            return Reflect.get(target, key, receiver);
          },
        });

        const deps = Layer.mergeAll(
          Layer.succeed(SqlClient.SqlClient)(observed),
          storageConfigLayer({ filename, busyTimeout: 0 }),
          NodeCrypto.layer,
          options?.failpoint === undefined
            ? SqliteStorageFailpoint.layer
            : Layer.succeed(SqliteStorageFailpoint)({ hit: options.failpoint }),
        );

        return yield* effect.pipe(
          Effect.provide(submissionLedgerLayer.pipe(Layer.provideMerge(deps))),
        );
      }).pipe(Effect.provide(SqliteClient.layer({ filename })));
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

describe("SQLite ledger read contracts", () => {
  for (const test of ledgerReadCases) it.effect(test.name, () => withFixture(test.run));

  it.effect("replays finalized state while another connection holds BEGIN IMMEDIATE", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "ledger-read-lock-" });
        const filename = `${directory}/state.sqlite`;

        const deps = Layer.mergeAll(
          SqliteClient.layer({ filename }),
          storageConfigLayer({ filename, busyTimeout: 0 }),
          NodeCrypto.layer,
          SqliteStorageFailpoint.layer,
        );

        yield* Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const request = yield* reserveReadFixture("lock-reader");
          const settled = yield* ledger.finalizeSettlement(request);

          yield* Effect.gen(function* () {
            const writer = yield* SqlClient.SqlClient;

            yield* Effect.acquireRelease(writer`BEGIN IMMEDIATE`, () =>
              writer`ROLLBACK`.pipe(Effect.orDie),
            );
            yield* writer`UPDATE effect_agent_settlement_reservations SET finalized_at='2040-01-01T00:00:00.000Z' WHERE submission_id=${request.submissionId}`;
            expect(yield* ledger.finalizeSettlement(request)).toEqual(settled);
          }).pipe(Effect.scoped, Effect.provide(SqliteClient.layer({ filename })));
          expect(yield* ledger.finalizeSettlement(request)).toEqual(settled);
          // The subsequent genuine finalization must still acquire and release the writer.
          yield* ledger.finalizeSettlement(yield* reserveReadFixture("after-lock"));
        }).pipe(Effect.provide(submissionLedgerLayer.pipe(Layer.provideMerge(deps))));
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  );

  it.effect(
    "adds one read probe to active finalization and takes no writer reservation on replay",
    () => {
      let reservations = 0;

      return withFixture(
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const request = yield* reserveReadFixture("writer-budget");

          reservations = 0;
          yield* ledger.finalizeSettlement(request);
          expect(reservations).toBe(1);
          reservations = 0;
          yield* ledger.finalizeSettlement(request);
          expect(reservations).toBe(0);
        }),
        {
          reserved: () => {
            reservations++;
          },
        },
      );
    },
  );

  for (const point of [
    "ledger:finalize-settlement:before",
    "ledger:finalize-settlement:after",
  ] as const) {
    it.effect(`preserves ${point} on already settled replay`, () => {
      let armed = false;
      let hits = 0;

      return withFixture(
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const request = yield* reserveReadFixture(point);
          const settled = yield* ledger.finalizeSettlement(request);

          armed = true;
          expect(yield* ledger.finalizeSettlement(request).pipe(Effect.result)).toMatchObject({
            _tag: "Failure",
            failure: {
              _tag: "LedgerError",
              cause: { _tag: "SqliteStorageFailpointError", location: point },
            },
          });
          expect(hits).toBe(1);
          armed = false;
          expect(yield* ledger.finalizeSettlement(request)).toEqual(settled);
        }),
        {
          failpoint: (location) => {
            if (!armed || location !== point) return Effect.void;
            hits++;

            return SqliteStorageFailpointError.make({ location: point });
          },
        },
      );
    });
  }
});

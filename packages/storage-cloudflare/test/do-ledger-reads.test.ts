import { SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer, type Crypto } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  ledgerReadCases,
  reserveReadFixture,
} from "../../../test/fixtures/ledger-read-contracts.ts";
import { DoStorageFailpointError } from "../src/DoStorageError.ts";
import { DoStorageFailpoint } from "../src/DoStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/DoSubmissionLedger.ts";
import { storageConfigLayer } from "../src/DoThreadStore.ts";
import { withThreadStorage } from "./harness.ts";

let nextId = 0;

const withFixture = <A, E>(
  effect: Effect.Effect<A, E, SubmissionLedger | SqlClient.SqlClient | Crypto.Crypto>,
  options?: {
    readonly transaction?: () => void;
    readonly failpoint?: (point: string) => Effect.Effect<void, DoStorageFailpointError>;
  },
) =>
  withThreadStorage(`ledger-reads-${nextId++}`, (storage) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const observed = new Proxy(sql, {
        get(target, key, receiver) {
          if (key === "withTransaction")
            return <A, E, R>(body: Effect.Effect<A, E, R>) =>
              Effect.sync(() => options?.transaction?.()).pipe(
                Effect.andThen(sql.withTransaction(body)),
              );

          return Reflect.get(target, key, receiver);
        },
      });

      const deps = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient)(observed),
        storageConfigLayer({ storage }),
        BrowserCrypto.layer,
        options?.failpoint === undefined
          ? DoStorageFailpoint.layer
          : Layer.succeed(DoStorageFailpoint)({ hit: options.failpoint }),
      );

      return yield* effect.pipe(
        Effect.provide(submissionLedgerLayer.pipe(Layer.provideMerge(deps))),
      );
    }).pipe(Effect.provide(SqliteClient.layer({ storage }))),
  );

describe("Durable Object ledger read contracts", () => {
  for (const test of ledgerReadCases) it(test.name, () => withFixture(test.run));

  it("uses no storage transaction for settled replay and preserves active finalization", () => {
    let transactions = 0;

    return withFixture(
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const request = yield* reserveReadFixture("transaction-budget");

        transactions = 0;
        yield* ledger.finalizeSettlement(request);
        expect(transactions).toBe(1);
        transactions = 0;
        yield* ledger.finalizeSettlement(request);
        expect(transactions).toBe(0);
      }),
      {
        transaction: () => {
          transactions++;
        },
      },
    );
  });

  for (const point of [
    "ledger:finalize-settlement:before",
    "ledger:finalize-settlement:after",
  ] as const) {
    it(`preserves ${point} on already settled replay`, () => {
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
              cause: { _tag: "DoStorageFailpointError", location: point },
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

            return DoStorageFailpointError.make({ location: point });
          },
        },
      );
    });
  }
});

import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer, type Crypto } from "effect";
import type { SubmissionLedger } from "effect-agent/submission-ledger";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { describe, it } from "vite-plus/test";

import { ledgerReadCases } from "../../../test/fixtures/ledger-read-contracts.ts";
import { DoStorageFailpoint } from "../src/DoStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/DoSubmissionLedger.ts";
import { storageConfigLayer } from "../src/DoThreadStore.ts";
import { withThreadStorage } from "./harness.ts";

let nextId = 0;

const withFixture = <A, E>(
  effect: Effect.Effect<A, E, SubmissionLedger | SqlClient.SqlClient | Crypto.Crypto>,
) =>
  withThreadStorage(`ledger-reads-${nextId++}`, (storage) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const deps = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient)(sql),
        storageConfigLayer({ storage }),
        BrowserCrypto.layer,
        DoStorageFailpoint.layer,
      );

      return yield* effect.pipe(
        Effect.provide(submissionLedgerLayer.pipe(Layer.provideMerge(deps))),
      );
    }).pipe(Effect.provide(SqliteClient.layer({ storage }))),
  );

describe("Durable Object ledger read contracts", () => {
  for (const test of ledgerReadCases) it(test.name, () => withFixture(test.run));
});

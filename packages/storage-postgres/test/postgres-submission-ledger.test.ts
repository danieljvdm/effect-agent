import { ledgerLayer } from "@effect-agent/storage-postgres/postgres-submission-ledger";
import { NodeCrypto } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Redacted } from "effect";
import { submissionLedgerConformanceCases } from "effect-agent/testing/submission-ledger-conformance";

const adminUrl =
  process.env.EFFECT_AGENT_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:55432/effect_agent";

const databaseUrl = (database: string) => {
  const url = new URL(adminUrl);

  url.pathname = `/${database}`;

  return url.toString();
};

let databaseCounter = 0;

/** A database per case, for the reason given in the thread store's suite. */
const withTemporaryDatabase = <A, E>(
  use: (url: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.gen(function* () {
    databaseCounter = databaseCounter + 1;
    const database = `effect_agent_ledger_${process.pid}_${databaseCounter}`;

    yield* Effect.gen(function* () {
      const sql = yield* PgClient.PgClient;

      return yield* sql.unsafe(`CREATE DATABASE ${database}`);
    }).pipe(
      Effect.provide(PgClient.layer({ url: Redacted.make(adminUrl), maxConnections: 1 })),
      Effect.orDie,
    );

    return yield* use(databaseUrl(database));
  });

describe("PostgresSubmissionLedger", () => {
  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((url) =>
          conformanceCase.run.pipe(
            Effect.provide(
              Layer.mergeAll(
                ledgerLayer({ client: { url: Redacted.make(url) } }),
                NodeCrypto.layer,
              ),
            ),
          ),
        ),
      );
    }
  });
});

import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, type Crypto } from "effect";
import { SubmissionLedger } from "effect-agent/submission-ledger";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  ledgerReadCases,
  reserveReadFixture,
} from "../../../test/fixtures/ledger-read-contracts.ts";
import { SqliteStorageFailpoint } from "../src/SqliteStorageFailpoint.ts";
import { submissionLedgerLayer } from "../src/SqliteSubmissionLedger.ts";
import { storageConfigLayer } from "../src/SqliteThreadStore.ts";

const withFixture = <A, E>(
  effect: Effect.Effect<A, E, SubmissionLedger | SqlClient.SqlClient | Crypto.Crypto>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "ledger-reads-" });
      const filename = `${directory}/state.sqlite`;

      return yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        const deps = Layer.mergeAll(
          Layer.succeed(SqlClient.SqlClient)(sql),
          storageConfigLayer({ filename, busyTimeout: 0 }),
          NodeCrypto.layer,
          SqliteStorageFailpoint.layer,
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
});

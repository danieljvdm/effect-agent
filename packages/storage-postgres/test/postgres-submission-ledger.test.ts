import * as PostgresStorage from "@effect-agent/storage-postgres/postgres-storage";
import { NodeCrypto } from "@effect/platform-node";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, Redacted } from "effect";
import { submissionLedgerConformanceCases } from "effect-agent/testing/submission-ledger-conformance";

import { withTemporaryDatabase } from "./harness.ts";

describe("PostgresSubmissionLedger", () => {
  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((url) =>
          conformanceCase.run.pipe(
            Effect.provide(
              Layer.mergeAll(
                PostgresStorage.make({ client: { url: Redacted.make(url) } }).submissionLedger,
                NodeCrypto.layer,
              ),
            ),
          ),
        ),
      );
    }
  });
});

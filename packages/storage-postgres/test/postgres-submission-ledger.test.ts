import * as PostgresSubmissionLedger from "@effect-agent/storage-postgres/postgres-submission-ledger";
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
                PostgresSubmissionLedger.layer({ client: { url: Redacted.make(url) } }),
                NodeCrypto.layer,
              ),
            ),
          ),
        ),
      );
    }
  });
});

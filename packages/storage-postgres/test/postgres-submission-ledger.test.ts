import { NodeCrypto } from "@effect/platform-node";
import { describe, it } from "@effect/vitest";
import { Effect, Layer, String } from "effect";
import { submissionLedgerConformanceCases } from "effect-agent/testing/submission-ledger-conformance";

import { storage as makeStorage, withTemporaryDatabase } from "./harness.ts";

describe("PostgresSubmissionLedger", () => {
  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it.effect(conformanceCase.name, () =>
        withTemporaryDatabase((url) =>
          conformanceCase.run.pipe(
            Effect.provide(
              Layer.mergeAll(
                makeStorage(
                  url,
                  { schema: "select" },
                  {
                    startupParameters: { search_path: "pg_catalog" },
                    transformQueryNames: String.snakeToCamel,
                    transformResultNames: String.snakeToCamel,
                  },
                ).submissionLedger,
                NodeCrypto.layer,
              ),
            ),
          ),
        ),
      );
    }
  });
});

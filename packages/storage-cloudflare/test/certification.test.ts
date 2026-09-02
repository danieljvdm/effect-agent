import {
  CERTIFICATION_SCENARIOS,
  TIER2_UNREACHED_LOCATIONS,
  certifyDurableAdapters,
  tier2NeverFiredLocations,
} from "@effect-agent/testing/certification";
import {
  CertificationReport,
  threadStoreConformanceCases,
  submissionLedgerConformanceCases,
} from "@effect-agent/thread/testing";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  DoStorageFailpoint,
  threadStoreLayer,
  storageConfigLayer,
  submissionLedgerLayer,
} from "../src/index.ts";
import { withThreadStorage } from "./harness.ts";

// ---------------------------------------------------------------------------
// P7 WP2 — certification runner for the Cloudflare Durable Object adapter pair (DC storage),
// executed IN-WORKERD against a real SQLite-backed Durable Object's storage (STORE-013,
// D-P6-7: this package's tests run under `vitest run` with the workers pool).
//
// Both ports share ONE SqlClient over the same `ctx.storage` (ADR-0011 D7's "same file" rule
// transposed to one object's private database). One certification run is shared by every test
// in this file. Workerd cannot write files, so PRINT_REPORT can emit the encoded result when
// local inspection is needed.
//
// Tier 3 is discharged by citation: the real loss levers for this adapter are the committed
// eviction (`ctx.abort()`), cross-DO subagent, and Miniflare-restart suites.
// ---------------------------------------------------------------------------

/** Flip to true to log the Schema-encoded certificate (workerd cannot write files). */
const PRINT_REPORT = false;

/** ThreadStore and SubmissionLedger sharing ONE SqlClient over one DO's storage. */
const combinedAdapters = (storage: DurableObjectStorage) =>
  Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        storageConfigLayer({ storage, observationPollInterval: 1 }),
        DoStorageFailpoint.layer,
        SqliteClient.layer({ storage }),
        BrowserCrypto.layer,
      ),
    ),
  );

const TIER3_EVIDENCE = [
  "packages/platform-cloudflare/test/eviction.test.ts",
  "packages/platform-cloudflare/test/subagents-cross-do.test.ts",
  "packages/platform-cloudflare/test/restart/travel-planner-restart.test.ts",
] as const;

let cachedPromise: Promise<CertificationReport> | undefined;

const certified = (): Promise<CertificationReport> => {
  cachedPromise ??= withThreadStorage("wp2-certification", (storage) =>
    Effect.gen(function* () {
      // The same combined Layer instance is passed for BOTH ports: Layer memoization builds
      // it once, so ledger and store share one SqlClient over one Durable Object database.
      const adapters = combinedAdapters(storage);

      const report = yield* certifyDurableAdapters({
        adapter: { name: "@effect-agent/storage-cloudflare" },
        submissionLedger: adapters,
        threadStore: adapters,
        tierThreeEvidence: TIER3_EVIDENCE,
      });

      if (PRINT_REPORT) {
        const encoded = yield* Schema.encodeEffect(CertificationReport)(report).pipe(Effect.orDie);

        console.log(JSON.stringify(encoded, null, 2));
      }

      return report;
    }).pipe(Effect.provide(BrowserCrypto.layer)),
  );

  return cachedPromise;
};

const CERTIFICATION_TIMEOUT = 240_000;

describe("TEST-004 STORE-010 STORE-013 adapter certification — storage-cloudflare (DC, in-workerd)", () => {
  it(
    "TIER1: all SubmissionLedger and ThreadStore contract cases pass",
    async () => {
      const report = await certified();
      const ledgerCases = report.tier1.filter((result) => result.suite === "submission-ledger");
      const storeCases = report.tier1.filter((result) => result.suite === "thread-store");

      expect(ledgerCases).toHaveLength(submissionLedgerConformanceCases.length);
      expect(storeCases).toHaveLength(threadStoreConformanceCases.length);
      expect(report.tier1.filter((result) => result.status !== "passed")).toEqual([]);
    },
    CERTIFICATION_TIMEOUT,
  );

  it(
    "TIER2: every coordinator failpoint leaves a classifiable state and re-drive converges",
    async () => {
      const report = await certified();

      expect(report.tier2).toHaveLength(34 * CERTIFICATION_SCENARIOS.length);
      expect(report.tier2.filter((row) => row.status === "failed")).toEqual([]);
      expect(report.tier2.every((row) => row.digestChainVerified)).toBe(true);
      for (const scenario of CERTIFICATION_SCENARIOS) {
        expect(report.tier2.some((row) => row.scenario === scenario && row.failpointFired)).toBe(
          true,
        );
      }
      expect(tier2NeverFiredLocations(report.tier2)).toEqual([...TIER2_UNREACHED_LOCATIONS].sort());
    },
    CERTIFICATION_TIMEOUT,
  );

  it(
    "the certification report round-trips its Schema and names the adapter identity and durability claim",
    async () => {
      const report = await certified();

      const encoded = await Effect.runPromise(
        Schema.encodeEffect(CertificationReport)(report).pipe(Effect.orDie),
      );

      const decoded = await Effect.runPromise(
        Schema.decodeUnknownEffect(CertificationReport)(encoded).pipe(Effect.orDie),
      );

      expect(decoded.format).toBe("effect-agent/certification@2");
      expect(decoded.fullyCertified).toBe(false);
      expect(decoded.adapter.name).toBe("@effect-agent/storage-cloudflare");
      expect(decoded.adapter.durability).toBe("durable-cloudflare");
      expect(decoded.ok).toBe(true);
    },
    CERTIFICATION_TIMEOUT,
  );

  it(
    "TIER3: the real eviction/restart loss levers are discharged by the committed DC suites",
    async () => {
      const report = await certified();

      expect(report.tier3.status).toBe("recorded-evidence");
      expect(report.tier3.evidence).toEqual(TIER3_EVIDENCE);
      expect(report.tier3.cases).toEqual([]);
    },
    CERTIFICATION_TIMEOUT,
  );
});

import {
  CertificationReport,
  conversationStoreConformanceCases,
  submissionLedgerConformanceCases,
} from "@effect-agent/session/testing";
import {
  SqliteStorageFailpoint,
  conversationStoreLayer,
  storageConfigLayer,
  submissionLedgerLayer,
} from "@effect-agent/storage-sqlite";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, FileSystem, Layer, Schema } from "effect";

import {
  CERTIFICATION_SCENARIOS,
  TIER2_UNREACHED_LOCATIONS,
  certifyDurableAdapters,
  tier2NeverFiredLocations,
} from "../src/index.ts";
import { maybeWriteReport } from "./certification-report-io.ts";

// ---------------------------------------------------------------------------
// P7 WP2 — certification runner for the Node/SQLite adapter pair (DN storage).
//
// The plan (§1) places this runner beside the adapter under test
// (`packages/storage-sqlite/test/certification.test.ts`); it lives here instead because vp's
// task graph rejects the dev-edge cycle storage-sqlite → testing → storage-sqlite — the same
// constraint that moved `durable-runtime.test.ts` into this package (commit c106b53). The
// candidate Layers below ARE the storage-sqlite adapters over one temporary database file,
// with BOTH ports sharing one SqlClient (the ADR-0011 "same file" rule).
//
// Tier 3 is discharged by citation: the real process-kill loss lever for this adapter is the
// committed platform-node crash matrix (TEST-005), which drives these same Layers through
// worker kills at every durable boundary.
// ---------------------------------------------------------------------------

/** ConversationStore and SubmissionLedger sharing ONE SqlClient over one database file. */
const combinedAdapters = (filename: string) =>
  Layer.mergeAll(conversationStoreLayer, submissionLedgerLayer).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        storageConfigLayer({ filename, observationPollInterval: 1 }),
        SqliteStorageFailpoint.layer,
        SqliteClient.layer({ filename }),
        NodeCrypto.layer,
      ),
    ),
  );

const TIER3_EVIDENCE = [
  "packages/platform-node/test/crash/crash.test.ts",
  "packages/platform-node/test/crash/crash-subagents.test.ts",
] as const;

let cached: CertificationReport | undefined;

const certified = Effect.gen(function* () {
  if (cached !== undefined) return cached;
  const report = yield* Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-certification-sqlite-",
      });
      // The same combined Layer instance is passed for BOTH ports: Layer memoization builds
      // it once, so ledger and store share one SqlClient over one database file.
      const adapters = combinedAdapters(`${directory}/certification.sqlite`);
      return yield* certifyDurableAdapters({
        adapter: { name: "@effect-agent/storage-sqlite" },
        submissionLedger: adapters,
        conversationStore: adapters,
        tierThreeEvidence: TIER3_EVIDENCE,
      });
    }),
  ).pipe(Effect.provide([NodeFileSystem.layer, NodeCrypto.layer]));
  yield* maybeWriteReport("storage-sqlite", report);
  cached = report;
  return report;
});

describe("TEST-004 STORE-010 adapter certification — storage-sqlite (DN)", () => {
  it.effect(
    "TIER1: all SubmissionLedger and ConversationStore contract cases pass",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        const ledgerCases = report.tier1.filter((result) => result.suite === "submission-ledger");
        const storeCases = report.tier1.filter((result) => result.suite === "conversation-store");
        expect(ledgerCases).toHaveLength(submissionLedgerConformanceCases.length);
        expect(storeCases).toHaveLength(conversationStoreConformanceCases.length);
        expect(report.tier1.filter((result) => result.status !== "passed")).toEqual([]);
      }),
    300_000,
  );

  it.effect(
    "TIER2: every coordinator failpoint leaves a classifiable state and re-drive converges",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        expect(report.tier2).toHaveLength(31 * CERTIFICATION_SCENARIOS.length);
        expect(report.tier2.filter((row) => row.status === "failed")).toEqual([]);
        expect(report.tier2.every((row) => row.digestChainVerified)).toBe(true);
        for (const scenario of CERTIFICATION_SCENARIOS) {
          expect(report.tier2.some((row) => row.scenario === scenario && row.failpointFired)).toBe(
            true,
          );
        }
        expect(tier2NeverFiredLocations(report.tier2)).toEqual(
          [...TIER2_UNREACHED_LOCATIONS].sort(),
        );
      }),
    300_000,
  );

  it.effect(
    "the certification report round-trips its Schema and names the adapter identity and durability claim",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        const encoded = yield* Schema.encodeEffect(CertificationReport)(report);
        const decoded = yield* Schema.decodeUnknownEffect(CertificationReport)(encoded);
        expect(decoded.format).toBe("effect-agent/certification@1");
        expect(decoded.adapter.name).toBe("@effect-agent/storage-sqlite");
        expect(decoded.adapter.durability).toBe("durable-node");
        expect(decoded.ok).toBe(true);
      }),
    300_000,
  );

  it.effect(
    "TIER3: the real process-kill loss lever is discharged by the committed crash matrix",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        expect(report.tier3.status).toBe("recorded-evidence");
        expect(report.tier3.evidence).toEqual(TIER3_EVIDENCE);
        expect(report.tier3.cases).toEqual([]);
      }),
    300_000,
  );
});

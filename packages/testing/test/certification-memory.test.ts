import {
  CertificationCaseResult,
  CertificationReport,
  conversationStoreConformanceCases,
  submissionLedgerConformanceCases,
} from "@effect-agent/session/testing";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import {
  CERTIFICATION_SCENARIOS,
  TIER2_UNREACHED_LOCATIONS,
  certifyDurableAdapters,
  resolveTierThree,
  tier2NeverFiredLocations,
} from "../src/index.ts";
import { maybeWriteReport } from "./certification-report-io.ts";

// ---------------------------------------------------------------------------
// P7 WP2 — certification runner for the in-memory REFERENCE adapter pair.
//
// The plan (§1) places this runner beside the adapter under test
// (`packages/storage-memory/test/certification.test.ts`); it lives here instead because vp's
// task graph rejects the dev-edge cycle storage-memory → testing → storage-memory — the same
// constraint that moved `durable-runtime.test.ts` into this package (commit c106b53). The
// candidate Layers below ARE the storage-memory adapters; nothing about the certification
// weakens.
//
// One certification run is shared by every test in this file (a certificate is one run's
// verdict). Set EFFECT_AGENT_CERTIFICATION_OUT to write the encoded report locally.
// ---------------------------------------------------------------------------

let cached: CertificationReport | undefined;

const certified = Effect.gen(function* () {
  if (cached !== undefined) return cached;
  const report = yield* certifyDurableAdapters({
    adapter: { name: "@effect-agent/storage-memory" },
    submissionLedger: MemorySubmissionLedgerLive,
    conversationStore: MemoryConversationStoreLive,
  }).pipe(Effect.provide(NodeCrypto.layer));
  yield* maybeWriteReport("storage-memory", report);
  cached = report;
  return report;
});

describe("TEST-004 STORE-010 adapter certification — storage-memory reference (Tier 3 N/A)", () => {
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
    120_000,
  );

  it.effect(
    "TIER2: every coordinator failpoint leaves a classifiable state and re-drive converges",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        // Full sweep: every location armed in every scenario shape.
        expect(report.tier2).toHaveLength(31 * CERTIFICATION_SCENARIOS.length);
        expect(report.tier2.filter((row) => row.status === "failed")).toEqual([]);
        // Every cell (fired or clean) verified with a FULLY recomputed digest chain.
        expect(report.tier2.every((row) => row.digestChainVerified)).toBe(true);
        // Each scenario shape had its own faults actually fire.
        for (const scenario of CERTIFICATION_SCENARIOS) {
          expect(report.tier2.some((row) => row.scenario === scenario && row.failpointFired)).toBe(
            true,
          );
        }
        // The never-fired set is EXACTLY the documented abort/operator-path exceptions —
        // scoped coverage stated honestly, and pinned so it cannot silently grow.
        expect(tier2NeverFiredLocations(report.tier2)).toEqual(
          [...TIER2_UNREACHED_LOCATIONS].sort(),
        );
      }),
    120_000,
  );

  it.effect(
    "the certification report round-trips its Schema and names the adapter identity and durability claim",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        const encoded = yield* Schema.encodeEffect(CertificationReport)(report);
        const decoded = yield* Schema.decodeUnknownEffect(CertificationReport)(encoded);
        expect(decoded.format).toBe("effect-agent/certification@1");
        expect(decoded.adapter.name).toBe("@effect-agent/storage-memory");
        expect(decoded.adapter.durability).toBe("non-durable");
        expect(decoded.ok).toBe(true);
        expect(decoded.tier1).toHaveLength(report.tier1.length);
        expect(decoded.tier2).toHaveLength(report.tier2.length);
      }),
    120_000,
  );

  it.effect(
    "TIER3: the non-durable reference adapter records not-applicable, never a silent claim",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        expect(report.tier3.status).toBe("not-applicable");
        expect(report.tier3.cases).toEqual([]);
        expect(report.tier3.evidence).toEqual([]);
      }),
    120_000,
  );

  it.effect("TIER3: resolveTierThree maps lever, citations, and absence honestly", () =>
    Effect.gen(function* () {
      const exercised = yield* resolveTierThree("durable-node", {
        crashLever: Effect.succeed([
          CertificationCaseResult.make({
            suite: "real-loss",
            name: "kill/reopen designated row",
            status: "passed",
          }),
        ]),
      });
      expect(exercised.status).toBe("exercised");
      expect(exercised.cases).toHaveLength(1);

      const recorded = yield* resolveTierThree("durable-node", {
        tierThreeEvidence: ["packages/platform-node/test/crash/crash.test.ts"],
      });
      expect(recorded.status).toBe("recorded-evidence");

      const notExercised = yield* resolveTierThree("durable-cloudflare", {});
      expect(notExercised.status).toBe("not-exercised");
      expect(notExercised.detail).toContain("NOT discharged");

      const notApplicable = yield* resolveTierThree("non-durable", {
        tierThreeEvidence: ["ignored"],
      });
      expect(notApplicable.status).toBe("not-applicable");
    }),
  );
});

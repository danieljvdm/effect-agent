import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { memoryThreadStoreLayer } from "@effect-agent/storage-memory/memory-thread-store";
import {
  CERTIFICATION_SCENARIOS,
  TIER2_UNREACHED_LOCATIONS,
  certifyDurableAdapters,
  tier2NeverFiredLocations,
} from "@effect-agent/testing/certification";
import { DurableRuntimeFailpointLocation } from "@effect-agent/thread/durable-failpoint";
import {
  CertificationReport,
  type CertificationScenario,
} from "@effect-agent/thread/testing/certification";
import { submissionLedgerConformanceCases } from "@effect-agent/thread/testing/submission-ledger-conformance";
import { threadStoreConformanceCases } from "@effect-agent/thread/testing/thread-store-conformance";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

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
    threadStore: memoryThreadStoreLayer({ maxThreads: 512 }),
  }).pipe(Effect.provide(NodeCrypto.layer));

  yield* maybeWriteReport("storage-memory", report);
  cached = report;

  return report;
});

// Independent expectations for the fixture protocols, never inputs to discovery. A lost
// path in one shape must fail even if another shape still fires that location globally.
const runLocations: ReadonlyArray<DurableRuntimeFailpointLocation> = [
  "submit:after-admit",
  "submit:after-materialize",
  "claim:after-claim",
  "input:after-canonical-append",
  "run:before-start-append",
  "run:after-start-append",
  "turn:after-canonical-append",
  "terminalize:after-reserve",
  "terminalize:after-canonical-append",
];

const toolLocations: ReadonlyArray<DurableRuntimeFailpointLocation> = [
  "turn:after-response-append",
  "tools:before-prepared-append",
  "tools:after-prepared-append",
];

const ordinaryToolLocations: ReadonlyArray<DurableRuntimeFailpointLocation> = [
  ...runLocations,
  ...toolLocations,
  "turn:after-results-append",
];

const expectedPaths: Record<
  CertificationScenario,
  ReadonlyArray<DurableRuntimeFailpointLocation>
> = {
  plain: runLocations,
  "uncertain-tool": ordinaryToolLocations,
  "durable-steps": [...ordinaryToolLocations, "step:after-step-append"],
  approval: [...ordinaryToolLocations, "approval:after-request-append", "approval:after-suspend"],
  join: [...runLocations, "join:after-claim", "join:after-canonical-append"],
  // Attached delegation settles siblings at suspension and joins through the child protocol.
  delegation: [
    ...runLocations,
    ...toolLocations,
    "subagent:after-reserve",
    "subagent:after-request-append",
    "subagent:after-admit",
    "subagent:after-child-ready",
    "subagent:after-start-append",
    "subagent:after-sibling-settle",
    "subagent:after-suspend",
    "subagent:before-join-append",
    "subagent:after-join-append",
    "subagent:after-release-pending",
    "subagent:after-release",
  ],
};

describe("TEST-004 STORE-010 adapter certification — storage-memory reference (Tier 3 N/A)", () => {
  it.effect(
    "TIER1: all SubmissionLedger and ThreadStore contract cases pass",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;
        const ledgerCases = report.tier1.filter((result) => result.suite === "submission-ledger");
        const storeCases = report.tier1.filter((result) => result.suite === "thread-store");

        expect(ledgerCases).toHaveLength(submissionLedgerConformanceCases.length);
        expect(storeCases).toHaveLength(threadStoreConformanceCases.length);
        expect(report.tier1.filter((result) => result.status !== "passed")).toEqual([]);
      }),
    120_000,
  );

  it.effect(
    "TIER2: every coordinator failpoint leaves a classifiable state and re-drive converges",
    () =>
      Effect.gen(function* () {
        const report = yield* certified;

        // Every pair remains accounted for, including shared clean-path results.
        expect(report.tier2.map(({ scenario, location }) => [scenario, location])).toEqual(
          CERTIFICATION_SCENARIOS.flatMap((scenario) =>
            DurableRuntimeFailpointLocation.literals.map((location) => [scenario, location]),
          ),
        );
        expect(report.tier2.filter((row) => row.status === "failed")).toEqual([]);
        // Every cell (fired or clean) verified with a FULLY recomputed digest chain.
        expect(report.tier2.every((row) => row.digestChainVerified)).toBe(true);
        for (const scenario of CERTIFICATION_SCENARIOS) {
          expect(
            report.tier2
              .filter((row) => row.scenario === scenario && row.failpointFired)
              .map((row) => row.location)
              .sort(),
          ).toEqual([...expectedPaths[scenario]].sort());
        }
        // The never-fired set is EXACTLY the documented paths covered by separate suites —
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
        const decoded = yield* Schema.decodeEffect(CertificationReport)(encoded);

        expect(decoded.format).toBe("effect-agent/certification@2");
        expect(decoded.fullyCertified).toBe(false);
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
});

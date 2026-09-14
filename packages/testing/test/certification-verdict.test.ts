import { resolveTierThree } from "@effect-agent/testing/certification";
import { describe, expect, it } from "@effect/vitest";
import { DateTime, Effect } from "effect";
import {
  CertificationCaseResult,
  CertificationSweepResult,
  CertificationTierThreeReport,
  CertifiedAdapterIdentity,
} from "effect-agent/testing/certification";

import { makeCertificationReport } from "../src/internal/certification-report.ts";

const passedLoss = CertificationCaseResult.make({
  suite: "real-loss",
  name: "supplied process-loss result",
  status: "passed",
});

const failedLoss = CertificationCaseResult.make({ ...passedLoss, status: "failed" });

const ledgerCase = CertificationCaseResult.make({
  suite: "submission-ledger",
  name: "ledger contract result",
  status: "passed",
});

const storeCase = CertificationCaseResult.make({
  suite: "thread-store",
  name: "store contract result",
  status: "passed",
});

const sweep = CertificationSweepResult.make({
  scenario: "plain",
  location: "submit:after-admit",
  failpointFired: true,
  status: "converged",
  digestChainVerified: true,
});

const results = {
  generatedAt: DateTime.makeUnsafe(0),
  tier1: [ledgerCase, storeCase],
  tier2: [
    sweep,
    CertificationSweepResult.make({
      ...sweep,
      location: "step:after-step-append",
      failpointFired: false,
      status: "not-triggered",
    }),
  ],
};

interface VerdictCase {
  readonly name: string;
  readonly durability: CertifiedAdapterIdentity["durability"];
  readonly cases?: ReadonlyArray<CertificationCaseResult>;
  readonly evidence?: ReadonlyArray<string>;
  readonly leverRuns: number;
  readonly expected: {
    readonly ok: boolean;
    readonly fullyCertified: boolean;
    readonly tier3: { readonly status: CertificationTierThreeReport["status"] };
  };
}

// These are supplied result records, not fake adapters. Full certification remains in the
// adapter suites, including a failed lever that must change the runner's final verdict.
const verdicts: ReadonlyArray<VerdictCase> = [
  {
    name: "missing lever",
    durability: "durable-node",
    leverRuns: 0,
    expected: { ok: true, fullyCertified: false, tier3: { status: "not-exercised" } },
  },
  {
    name: "empty lever",
    durability: "durable-node",
    cases: [],
    leverRuns: 1,
    expected: { ok: true, fullyCertified: false, tier3: { status: "not-exercised" } },
  },
  {
    name: "passing real loss",
    durability: "durable-node",
    cases: [passedLoss],
    leverRuns: 1,
    expected: { ok: true, fullyCertified: true, tier3: { status: "exercised" } },
  },
  {
    name: "a failed loss among passing cases",
    durability: "durable-node",
    cases: [passedLoss, failedLoss],
    leverRuns: 1,
    expected: { ok: false, fullyCertified: false, tier3: { status: "exercised" } },
  },
  {
    name: "citations without execution",
    durability: "durable-node",
    evidence: ["test/crash.test.ts"],
    leverRuns: 0,
    expected: { ok: true, fullyCertified: false, tier3: { status: "recorded-evidence" } },
  },
  {
    name: "empty lever takes precedence over citations",
    durability: "durable-node",
    cases: [],
    evidence: ["test/crash.test.ts"],
    leverRuns: 1,
    expected: { ok: true, fullyCertified: false, tier3: { status: "not-exercised" } },
  },
  {
    name: "passing conformance cannot substitute for real loss",
    durability: "durable-node",
    cases: [passedLoss, storeCase],
    leverRuns: 1,
    expected: { ok: true, fullyCertified: false, tier3: { status: "exercised" } },
  },
  {
    name: "non-durable adapters do not execute a loss lever",
    durability: "non-durable",
    cases: [failedLoss],
    evidence: ["ignored"],
    leverRuns: 0,
    expected: { ok: true, fullyCertified: false, tier3: { status: "not-applicable" } },
  },
  {
    name: "passing Cloudflare real loss",
    durability: "durable-cloudflare",
    cases: [passedLoss],
    leverRuns: 1,
    expected: { ok: true, fullyCertified: true, tier3: { status: "exercised" } },
  },
];

describe("Certification verdicts", () => {
  it.effect.each(verdicts)("distinguishes passed checks from complete coverage: $name", (row) =>
    Effect.gen(function* () {
      let leverRuns = 0;

      const cases = row.cases;

      const tier3 = yield* resolveTierThree(row.durability, {
        tierThreeEvidence: row.evidence,
        crashLever:
          cases === undefined
            ? undefined
            : Effect.sync(() => {
                leverRuns++;

                return cases;
              }),
      });

      const report = makeCertificationReport({
        ...results,
        adapter: CertifiedAdapterIdentity.make({ name: "candidate", durability: row.durability }),
        tier3,
      });

      expect(report).toMatchObject(row.expected);
      expect(leverRuns).toBe(row.leverRuns);
      expect(tier3.cases).toEqual(row.leverRuns === 0 ? [] : row.cases);
      expect(tier3.evidence).toEqual(row.durability === "non-durable" ? [] : (row.evidence ?? []));
    }),
  );

  it.each(["submission-ledger", "thread-store", "coordinator"] as const)(
    "a failed %s check prevents certification even when every other tier passes",
    (failure) => {
      const report = makeCertificationReport({
        ...results,
        adapter: CertifiedAdapterIdentity.make({ name: "candidate", durability: "durable-node" }),
        tier1: results.tier1.map((row) =>
          row.suite === failure ? CertificationCaseResult.make({ ...row, status: "failed" }) : row,
        ),
        tier2:
          failure === "coordinator"
            ? [CertificationSweepResult.make({ ...sweep, status: "failed" })]
            : results.tier2,
        tier3: CertificationTierThreeReport.make({
          status: "exercised",
          evidence: [],
          cases: [passedLoss],
        }),
      });

      expect(report.ok).toBe(false);
      expect(report.fullyCertified).toBe(false);
    },
  );
});

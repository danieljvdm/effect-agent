import { CertificationReport } from "@effect-agent/thread/testing/Certification";

/** Fold executed results without acquiring adapters or claiming coverage from citations. */
export const makeCertificationReport = (
  results: Pick<CertificationReport, "adapter" | "generatedAt" | "tier1" | "tier2" | "tier3">,
): CertificationReport => {
  const { adapter, tier1, tier2, tier3 } = results;

  const ok =
    tier1.every((result) => result.status === "passed") &&
    tier2.every((result) => result.status !== "failed") &&
    tier3.cases.every((result) => result.status === "passed");

  return CertificationReport.make({
    format: "effect-agent/certification@2",
    ...results,
    ok,
    fullyCertified:
      ok &&
      adapter.durability !== "non-durable" &&
      tier3.status === "exercised" &&
      tier3.cases.length > 0 &&
      tier3.cases.every((result) => result.suite === "real-loss"),
  });
};

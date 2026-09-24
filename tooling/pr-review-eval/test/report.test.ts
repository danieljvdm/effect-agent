import { fileURLToPath } from "node:url";

import {
  ReviewFinding,
  ReviewOutcome,
  ReviewReport,
  type ReviewSeverity,
} from "@effect-agent/pr-review/review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { DateTime, Effect, Schema } from "effect";

import type { EvalObservationSetDigest } from "../src/index.ts";
import {
  CURRENT_RUNNER_VERSION,
  digestObservationSet,
  EvalCase,
  EvalDefectId,
  EvalExpectedDefect,
  EvalFindingJudgment,
  EvalJudgmentSet,
  EvalObservation,
  EvalTrialFailed,
  EvalTrialSucceeded,
  EvalVariantConfiguration,
  EvalSuite,
  loadEvalSuite,
  makeQualityReport,
} from "../src/index.ts";

const fixturePath = fileURLToPath(new URL("../fixtures/smoke-suite.json", import.meta.url));
const at = (millis: number) => DateTime.toUtc(DateTime.makeUnsafe(millis));

const configuration = (id: string, model = "scripted-eval") =>
  EvalVariantConfiguration.make({
    id,
    reviewerProfile: `${id}-v1`,
    provider: "openai",
    model,
    reasoningEffort: "medium",
    compaction: "prune",
    contextTokenLimit: 128_000,
    maxOutputTokens: 8_000,
    strictJsonSchema: true,
    store: false,
    maxCostMicrousd: 2_500_000,
    budgetPolicy: "input-size-v1",
  });

const finding = (title: string, severity: ReviewSeverity = "blocking"): ReviewFinding =>
  ReviewFinding.make({
    path: "src/read.ts",
    line: 1,
    severity,
    category: "correctness",
    title,
    body: `${title}.`,
  });

const succeeded = (
  findings: ReadonlyArray<ReviewFinding>,
  cost?: number,
  coverage: Pick<ReviewOutcome, "incomplete" | "exhausted" | "compactions" | "research"> = {},
) =>
  EvalTrialSucceeded.make({
    outcome: ReviewOutcome.make({
      ...coverage,
      report: ReviewReport.make({
        summary: findings.length === 0 ? "No findings." : "Review findings.",
        findings,
      }),
      turns: 1,
      usage: {
        inputTokens: 10,
        uncachedInputTokens: 8,
        cachedInputTokens: 2,
        cacheWriteInputTokens: 0,
        outputTokens: 3,
        ...(cost === undefined ? {} : { estimatedCostMicrousd: cost }),
      },
    }),
  });

const observation = (
  evalCase: EvalCase,
  variant: EvalVariantConfiguration,
  trial: number,
  result: EvalTrialSucceeded | EvalTrialFailed,
) =>
  EvalObservation.make({
    version: 1,
    runnerVersion: CURRENT_RUNNER_VERSION,
    caseId: evalCase.id,
    caseVersion: evalCase.version,
    inputDigest: evalCase.inputDigest,
    variant,
    trial,
    recordedAt: at(trial * 1_000),
    elapsedMillis: trial * 100,
    result,
  });

const judgment = (
  evalCase: EvalCase,
  variant: EvalVariantConfiguration,
  trial: number,
  findingIndex: number,
  label: "matches-expected" | "new-valid" | "invalid" | "unclear",
  matchedDefectIds: ReadonlyArray<(typeof evalCase.expectedDefects)[number]["id"]> = [],
) =>
  EvalFindingJudgment.make({
    version: 1,
    caseId: evalCase.id,
    caseVersion: evalCase.version,
    inputDigest: evalCase.inputDigest,
    variantId: variant.id,
    trial,
    findingIndex,
    label,
    matchedDefectIds,
    rationale: `Source judgment: ${label}.`,
    adjudicator: "maintainer",
  });

const judgmentSet = (
  observationSetDigest: EvalObservationSetDigest,
  judgments: ReadonlyArray<EvalFindingJudgment>,
) =>
  EvalJudgmentSet.make({
    version: 1,
    observationSetDigest,
    judgments,
  });

describe("PR-review eval quality report", () => {
  it.effect(
    "keeps incomplete trials out of clean controls while retaining adjudicated findings",
    () =>
      Effect.gen(function* () {
        const suite = yield* loadEvalSuite(fixturePath);
        const known = suite.cases.find((evalCase) => evalCase.kind === "known-defects");
        const clean = suite.cases.find((evalCase) => evalCase.kind === "clean-control");

        if (known === undefined || clean === undefined) throw new Error("Missing eval fixtures");
        const defectId = known.expectedDefects[0]?.id;

        if (defectId === undefined) throw new Error("Missing expected blocker");
        const variant = configuration("partial");

        const observations = [
          observation(
            known,
            variant,
            1,
            succeeded([finding("Recorded blocker")], 5, {
              incomplete: true,
              research: {
                delegations: 3,
                started: 2,
                completed: 1,
                failed: 1,
                interrupted: 0,
                incomplete: 1,
              },
              compactions: [
                {
                  kind: "clear-tool-results",
                  turn: 1,
                  tokensBeforeEstimate: 36_000,
                  tokensAfterEstimate: 33_000,
                },
                {
                  kind: "rollover",
                  turn: 1,
                  tokensBeforeEstimate: 33_000,
                  tokensAfterEstimate: 10_000,
                },
              ],
            }),
          ),
          observation(
            known,
            variant,
            2,
            succeeded([], 3, {
              compactions: [],
              research: {
                delegations: 0,
                started: 0,
                completed: 0,
                failed: 0,
                interrupted: 0,
                incomplete: 0,
              },
            }),
          ),
          observation(clean, variant, 1, succeeded([], undefined, { exhausted: "cost" })),
          observation(
            clean,
            variant,
            2,
            EvalTrialFailed.make({
              errorTag: "AiError",
              message: "Unavailable",
              estimatedCostMicrousd: 1,
            }),
          ),
        ];

        const judgments = judgmentSet(yield* digestObservationSet(observations), [
          judgment(known, variant, 1, 0, "matches-expected", [defectId]),
        ]);

        const report = yield* makeQualityReport(suite, observations, 2, judgments);
        const result = report.variants[0];

        expect(result?.resources).toMatchObject({
          incompleteTrials: 2,
          succeededTrials: 1,
        });
        expect(result?.firstTrialFindings.valid).toBe(1);
        expect(result?.cleanControls).toMatchObject({ passed: 0, total: 1 });
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("counts missed important defects even when a later trial finds them all", () =>
    Effect.gen(function* () {
      const loaded = yield* loadEvalSuite(fixturePath);
      const original = loaded.cases.find((evalCase) => evalCase.kind === "known-defects");

      expect(original).toBeDefined();
      if (original === undefined) return;
      const first = original.expectedDefects[0];

      expect(first).toBeDefined();
      if (first === undefined) return;

      const known = EvalCase.make({
        ...original,
        expectedDefects: [
          EvalExpectedDefect.make({ ...first, severity: "important" }),
          EvalExpectedDefect.make({
            ...first,
            id: Schema.decodeSync(EvalDefectId)("second-important-defect"),
            severity: "important",
            invariant: "A second independent supported operation returns the wrong result.",
          }),
        ],
      });

      const suite = EvalSuite.make({ version: 1, cases: [known] });
      const variant = configuration("misses-important");

      const observations = [
        observation(known, variant, 1, succeeded([])),
        observation(
          known,
          variant,
          2,
          succeeded([finding("First defect", "important"), finding("Second defect", "important")]),
        ),
      ];

      const judgments = judgmentSet(
        yield* digestObservationSet(observations),
        known.expectedDefects.map((defect, index) =>
          judgment(known, variant, 2, index, "matches-expected", [defect.id]),
        ),
      );

      const report = yield* makeQualityReport(suite, observations, 2, judgments);
      const result = report.variants[0];

      expect(result?.defectRecall).toEqual({ numerator: 0, denominator: 2, status: "measured" });
      expect(result?.allTrialFindings.valid).toBe(2);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

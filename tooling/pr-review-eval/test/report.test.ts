import { fileURLToPath } from "node:url";

import {
  ReviewFinding,
  ReviewFollowUp,
  ReviewOutcome,
  ReviewReport,
  ReviewRequest,
  ReviewResolution,
  type ReviewSeverity,
} from "@effect-agent/pr-review/review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { DateTime, Effect, Schema } from "effect";

import type { EvalObservationSetDigest, EvalOracleSetDigest } from "../src/index.ts";
import {
  CURRENT_RUNNER_VERSION,
  digestEvalSuiteOracles,
  digestObservationSet,
  digestReviewRequest,
  EvalCase,
  EvalDefectId,
  EvalExpectedDefect,
  EvalFindingJudgment,
  EvalInputDigest,
  EvalJudgmentSet,
  EvalObservation,
  EvalTrialFailed,
  EvalTrialSucceeded,
  EvalVariantConfiguration,
  EvalSuite,
  loadEvalSuite,
  makeQualityReport,
  renderQualityReport,
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
    serviceTier: "default",
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
  coverage: Pick<
    ReviewOutcome,
    "incomplete" | "exhausted" | "compactions" | "research" | "resolutions"
  > = {},
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
  oracleSetDigest: EvalOracleSetDigest,
  observationSetDigest: EvalObservationSetDigest,
  judgments: ReadonlyArray<EvalFindingJudgment>,
) =>
  EvalJudgmentSet.make({
    version: 2,
    oracleSetDigest,
    observationSetDigest,
    judgments,
  });

describe("PR-review eval quality report", () => {
  it.effect("rejects mixed repository sources across variants of one case", () =>
    Effect.gen(function* () {
      const loaded = yield* loadEvalSuite(fixturePath);
      const evalCase = loaded.cases[0];

      if (evalCase === undefined) throw new Error("Missing eval fixture");

      const suite = EvalSuite.make({ version: 1, cases: [evalCase] });
      const baseline = configuration("baseline-source");
      const candidate = configuration("candidate-source");

      const withGitSource = (variant: EvalVariantConfiguration, digit: string) =>
        EvalObservation.make({
          ...observation(evalCase, variant, 1, succeeded([])),
          repositorySource: {
            mode: "pinned-git",
            digest: Schema.decodeSync(EvalInputDigest)(digit.repeat(64)),
          },
        });

      const error = yield* Effect.flip(
        makeQualityReport(suite, [withGitSource(baseline, "1"), withGitSource(candidate, "2")], 1),
      );

      expect(error.message).toContain("repository source");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects stale judgments after defect promotion or a changed resolution oracle", () =>
    Effect.gen(function* () {
      const loaded = yield* loadEvalSuite(fixturePath);
      const known = loaded.cases.find((evalCase) => evalCase.kind === "known-defects");

      if (known === undefined) throw new Error("Missing known-defect fixture");

      const request = ReviewRequest.make({
        ...known.request,
        followUps: ["fixed", "open"].map((id) =>
          ReviewFollowUp.make({ id, description: `Recheck ${id} against the source.` }),
        ),
      });

      const original = Schema.decodeSync(EvalCase)({
        ...Schema.encodeSync(EvalCase)(known),
        kind: "unadjudicated",
        inputDigest: yield* digestReviewRequest(request),
        request: Schema.encodeSync(ReviewRequest)(request),
        expectedDefects: [],
        expectedResolvedFollowUpIds: ["fixed"],
        expectedUnresolvedFollowUpIds: ["open"],
      });

      const variant = configuration("adjudicated-before-corpus-update");
      const observations = [observation(original, variant, 1, succeeded([finding("Valid bug")]))];
      const originalSuite = EvalSuite.make({ version: 1, cases: [original] });

      const judgments = judgmentSet(
        yield* digestEvalSuiteOracles(originalSuite),
        yield* digestObservationSet(observations),
        [judgment(original, variant, 1, 0, "new-valid")],
      );

      const originalReport = yield* makeQualityReport(originalSuite, observations, 1, judgments);

      expect(originalReport.unmappedValidFindings).toHaveLength(1);

      const promoted = EvalCase.make({
        ...original,
        kind: "known-defects",
        expectedDefects: known.expectedDefects,
      });

      const repartitioned = EvalCase.make({
        ...original,
        expectedResolvedFollowUpIds: original.expectedUnresolvedFollowUpIds,
        expectedUnresolvedFollowUpIds: original.expectedResolvedFollowUpIds,
      });

      for (const changed of [promoted, repartitioned]) {
        const changedSuite = EvalSuite.make({ version: 1, cases: [changed] });
        const unjudgedReport = yield* makeQualityReport(changedSuite, observations, 1);

        expect(unjudgedReport.oracleSetDigest).not.toBe(originalReport.oracleSetDigest);

        const error = yield* Effect.flip(
          makeQualityReport(changedSuite, observations, 1, judgments),
        );

        expect(error.message).toContain("oracle");
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("scores first-pass resolutions and preserves unresolved feedback across retries", () =>
    Effect.gen(function* () {
      const loaded = yield* loadEvalSuite(fixturePath);
      const original = loaded.cases[0];

      if (original === undefined) throw new Error("Missing eval fixture");

      const followUps = ["fixed-a", "fixed-b", "open-a", "open-b"].map((id) =>
        ReviewFollowUp.make({ id, description: `Review ${id} against the head revision.` }),
      );

      const request = ReviewRequest.make({ ...original.request, followUps });

      const candidate = EvalCase.make({
        ...original,
        kind: "unadjudicated",
        expectedDefects: [],
        request,
        inputDigest: yield* digestReviewRequest(request),
      });

      const evalCase = Schema.decodeSync(EvalCase)({
        ...Schema.encodeSync(EvalCase)(candidate),
        expectedResolvedFollowUpIds: ["fixed-a", "fixed-b"],
        expectedUnresolvedFollowUpIds: ["open-a", "open-b"],
      });

      const suite = EvalSuite.make({ version: 1, cases: [evalCase] });

      const resolution = (id: string) =>
        ReviewResolution.make({ id, evidence: `The current source addresses ${id}.` });

      const inaccurate = configuration("inaccurate-first-pass");
      const incomplete = configuration("incomplete-first-pass");

      const observations = [
        observation(
          evalCase,
          inaccurate,
          1,
          succeeded([], undefined, { resolutions: [resolution("fixed-a"), resolution("open-a")] }),
        ),
        observation(
          evalCase,
          inaccurate,
          2,
          succeeded([], undefined, { resolutions: [resolution("fixed-a"), resolution("fixed-b")] }),
        ),
        observation(evalCase, incomplete, 1, succeeded([], undefined, { incomplete: true })),
        observation(
          evalCase,
          incomplete,
          2,
          succeeded([], undefined, { resolutions: [resolution("fixed-a"), resolution("fixed-b")] }),
        ),
      ];

      const report = yield* makeQualityReport(suite, observations, 2);

      expect(
        report.variants.find(({ configuration }) => configuration.id === inaccurate.id),
      ).toMatchObject({
        resolutionRecall: { numerator: 1, denominator: 2, status: "measured" },
        unresolvedRetention: { numerator: 1, denominator: 2, status: "measured" },
        resolutionCases: { complete: 0, incomplete: 1, unresolved: 0, total: 1 },
        cases: [
          {
            resolutionStatus: "incomplete",
            missedResolvedFollowUpIds: ["fixed-b"],
            erroneouslyResolvedFollowUpIds: ["open-a"],
          },
        ],
      });
      expect(
        report.variants.find(({ configuration }) => configuration.id === incomplete.id),
      ).toMatchObject({
        resolutionRecall: { numerator: 0, denominator: 2, status: "measured" },
        unresolvedRetention: { numerator: 0, denominator: 2, status: "measured" },
        resolutionCases: { complete: 0, incomplete: 1, unresolved: 0, total: 1 },
        cases: [
          {
            resolutionStatus: "incomplete",
            missedResolvedFollowUpIds: ["fixed-a", "fixed-b"],
            erroneouslyResolvedFollowUpIds: [],
          },
        ],
      });
      expect(renderQualityReport(report)).toContain(
        "resolution-recall 1/2; unresolved-retention 1/2",
      );
      expect(report.version).toBe(7);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

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

        const judgments = judgmentSet(
          yield* digestEvalSuiteOracles(suite),
          yield* digestObservationSet(observations),
          [judgment(known, variant, 1, 0, "matches-expected", [defectId])],
        );

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
        yield* digestEvalSuiteOracles(suite),
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

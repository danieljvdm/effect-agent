import { fileURLToPath } from "node:url";

import {
  ReviewFinding,
  ReviewOutcome,
  ReviewReport,
  type ReviewSeverity,
} from "@effect-agent/pr-review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { DateTime, Effect, FileSystem, Schema } from "effect";

import {
  CURRENT_RUNNER_VERSION,
  digestObservationSet,
  EvalFindingJudgment,
  EvalInputDigest,
  EvalJudgmentSet,
  EvalObservation,
  EvalObservationSetDigest,
  EvalQualityReport,
  EvalTrialFailed,
  EvalTrialSucceeded,
  EvalVariantConfiguration,
  loadJudgmentSet,
  loadObservationFile,
  loadEvalSuite,
  makeQualityReport,
  preflightObservationOutput,
  writeObservations,
  writeQualityReport,
  type EvalCase,
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
    maxOutputTokens: 8_000,
    strictJsonSchema: true,
    store: false,
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

const succeeded = (findings: ReadonlyArray<ReviewFinding>, cost?: number) =>
  EvalTrialSucceeded.make({
    outcome: ReviewOutcome.make({
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
    rationale: `Human judgment: ${label}.`,
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
  it.effect("pins first-pass quality while keeping later trials diagnostic", () =>
    Effect.gen(function* () {
      const suite = yield* loadEvalSuite(fixturePath);
      const known = suite.cases.find((evalCase) => evalCase.kind === "known-defects");
      const clean = suite.cases.find((evalCase) => evalCase.kind === "clean-control");
      expect(known).toBeDefined();
      expect(clean).toBeDefined();
      if (known === undefined || clean === undefined) return;
      const defectId = known.expectedDefects[0]?.id;
      expect(defectId).toBeDefined();
      if (defectId === undefined) return;

      const current = configuration("current");
      const candidate = configuration("candidate");
      const currentFirstUnjudged = finding("Unjudged non-blocker", "important");
      const currentFirstInvalid = finding("Speculative cleanup", "nit");
      const repeatedBlocker = finding("Optional value is dereferenced");
      const candidateBlocker = finding("Missing undefined guard");
      const candidateNovel = finding("Another valid regression", "important");
      const candidateUnclear = finding("Ambiguous edge case", "nit");

      const observations = [
        observation(known, current, 1, succeeded([currentFirstUnjudged, currentFirstInvalid], 5)),
        observation(known, current, 2, succeeded([repeatedBlocker, repeatedBlocker])),
        observation(clean, current, 1, succeeded([])),
        observation(
          clean,
          current,
          2,
          EvalTrialFailed.make({ errorTag: "RateLimitError", message: "Try later" }),
        ),
        observation(
          known,
          candidate,
          1,
          succeeded([candidateBlocker, candidateNovel, candidateUnclear], 7),
        ),
        observation(known, candidate, 2, succeeded([candidateBlocker], 7)),
        observation(
          clean,
          candidate,
          1,
          EvalTrialFailed.make({ errorTag: "TimeoutError", message: "Timed out" }),
        ),
        observation(clean, candidate, 2, succeeded([], 2)),
      ];
      const digest = yield* digestObservationSet(observations);
      const judgments = judgmentSet(digest, [
        judgment(known, current, 1, 1, "invalid"),
        judgment(known, current, 2, 0, "matches-expected", [defectId]),
        judgment(known, current, 2, 1, "matches-expected", [defectId]),
        judgment(known, candidate, 1, 0, "matches-expected", [defectId]),
        judgment(known, candidate, 1, 1, "new-valid"),
        judgment(known, candidate, 1, 2, "unclear"),
        judgment(known, candidate, 2, 0, "matches-expected", [defectId]),
      ]);

      const report = yield* makeQualityReport(suite, observations, judgments);
      expect(report.variants).toHaveLength(2);
      const currentReport = report.variants.find(
        (variant) => variant.configuration.id === "current",
      );
      const candidateReport = report.variants.find(
        (variant) => variant.configuration.id === "candidate",
      );
      expect(currentReport).toBeDefined();
      expect(candidateReport).toBeDefined();
      if (currentReport === undefined || candidateReport === undefined) return;

      expect(currentReport.blockerRecall).toMatchObject({
        numerator: 0,
        denominator: 1,
        status: "measured",
      });
      expect(currentReport.blockerCases).toMatchObject({
        complete: 0,
        incomplete: 1,
        unresolved: 0,
        total: 1,
      });
      expect(currentReport.laterOnlyBlockingDefects).toHaveLength(1);
      expect(currentReport.laterOnlyBlockingDefects[0]).toMatchObject({
        defectId,
        firstFoundTrial: 2,
      });
      expect(currentReport.firstTrialFindings).toMatchObject({
        valid: 0,
        invalid: 1,
        unclear: 0,
        unjudged: 1,
      });
      expect(currentReport.allTrialFindings).toMatchObject({
        valid: 2,
        invalid: 1,
        unjudged: 1,
      });
      expect(currentReport.cleanControls).toMatchObject({ passed: 1, total: 1 });
      expect(currentReport.resources).toMatchObject({
        attemptedTrials: 4,
        succeededTrials: 3,
        failedTrials: 1,
        costedSucceededTrials: 1,
        uncostedSucceededTrials: 2,
        estimatedCostMicrousd: 5,
        inputTokens: 30,
        outputTokens: 9,
        elapsedMillis: 600,
      });
      expect(currentReport.resources.failuresByTag).toEqual([
        { errorTag: "RateLimitError", count: 1 },
      ]);

      expect(candidateReport.blockerRecall).toMatchObject({
        numerator: 1,
        denominator: 1,
        status: "measured",
      });
      expect(candidateReport.blockerCases.complete).toBe(1);
      expect(candidateReport.cleanControls).toMatchObject({ passed: 0, total: 1 });
      expect(candidateReport.firstTrialFindings.precision.status).toBe("unresolved");
      expect(candidateReport.firstTrialFindings).toMatchObject({
        valid: 2,
        invalid: 0,
        unclear: 1,
        unjudged: 0,
      });
      expect(candidateReport.firstTrialFailures).toBe(1);
      expect(candidateReport.resources).toMatchObject({
        failedTrials: 1,
        inputTokens: 30,
        outputTokens: 9,
        costedSucceededTrials: 3,
        estimatedCostMicrousd: 16,
      });
      expect(report.unjudgedFindings).toHaveLength(1);
      expect(report.unmappedValidFindings).toHaveLength(1);
      const currentKnown = currentReport.cases.find((entry) => entry.caseId === known.id);
      const currentClean = currentReport.cases.find((entry) => entry.caseId === clean.id);
      const candidateClean = candidateReport.cases.find((entry) => entry.caseId === clean.id);
      expect(currentKnown?.resources).toMatchObject({
        attemptedTrials: 2,
        succeededTrials: 2,
        inputTokens: 20,
        outputTokens: 6,
      });
      expect(currentKnown?.blockerStatus).toBe("incomplete");
      expect(currentClean?.cleanControlPassed).toBe(true);
      expect(currentClean?.resources.failedTrials).toBe(1);
      expect(candidateClean?.firstTrialFailureTag).toBe("TimeoutError");
      expect(
        Schema.decodeSync(EvalQualityReport)(Schema.encodeSync(EvalQualityReport)(report)),
      ).toEqual(report);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects incompatible observation and judgment identities", () =>
    Effect.gen(function* () {
      const suite = yield* loadEvalSuite(fixturePath);
      const known = suite.cases[0];
      const clean = suite.cases[1];
      expect(known).toBeDefined();
      expect(clean).toBeDefined();
      if (known === undefined || clean === undefined) return;
      const current = configuration("current");
      const knownObservation = observation(known, current, 1, succeeded([]));
      const cleanObservation = observation(clean, current, 1, succeeded([]));
      const observations = [knownObservation, cleanObservation];
      const digest = yield* digestObservationSet(observations);

      const wrongDigest = Schema.decodeSync(EvalInputDigest)("0".repeat(64));
      expect(
        Schema.decodeUnknownOption(EvalObservation)({
          ...Schema.encodeSync(EvalObservation)(knownObservation),
          caseVersion: 2,
        })._tag,
      ).toBe("None");
      const incompatibleCase = [
        EvalObservation.make({ ...knownObservation, inputDigest: wrongDigest }),
        cleanObservation,
      ];
      expect((yield* Effect.result(makeQualityReport(suite, incompatibleCase)))._tag).toBe(
        "Failure",
      );

      const incompatibleConfig = [
        knownObservation,
        EvalObservation.make({
          ...cleanObservation,
          variant: configuration("current", "different-model"),
        }),
      ];
      expect((yield* Effect.result(makeQualityReport(suite, incompatibleConfig)))._tag).toBe(
        "Failure",
      );

      const staleJudgments = judgmentSet(
        Schema.decodeSync(EvalObservationSetDigest)("f".repeat(64)),
        [],
      );
      expect(
        (yield* Effect.result(makeQualityReport(suite, observations, staleJudgments)))._tag,
      ).toBe("Failure");

      const report = yield* makeQualityReport(suite, observations, judgmentSet(digest, []));
      expect(report.caseSet).toHaveLength(2);
      expect(report.variants).toHaveLength(1);

      const subsetReport = yield* makeQualityReport(suite, [knownObservation]);
      expect(subsetReport.caseSet.map((identity) => identity.id)).toEqual([known.id]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("round-trips judgment and report files without a model call", () =>
    Effect.gen(function* () {
      const suite = yield* loadEvalSuite(fixturePath);
      const known = suite.cases[0];
      const clean = suite.cases[1];
      expect(known).toBeDefined();
      expect(clean).toBeDefined();
      if (known === undefined || clean === undefined) return;
      const current = configuration("current");
      const observations = [
        observation(known, current, 1, succeeded([])),
        observation(clean, current, 1, succeeded([])),
      ];
      const judgments = judgmentSet(yield* digestObservationSet(observations), []);
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-report-" });
      const observationsPath = `${directory}/observations.jsonl`;
      const judgmentsPath = `${directory}/judgments.json`;
      const reportPath = `${directory}/report.json`;

      yield* writeObservations(observationsPath, observations);
      yield* fs.writeFileString(
        judgmentsPath,
        Schema.encodeSync(Schema.fromJsonString(EvalJudgmentSet))(judgments),
      );
      const loadedObservations = yield* loadObservationFile(observationsPath);
      const loadedJudgments = yield* loadJudgmentSet(judgmentsPath);
      const report = yield* makeQualityReport(suite, loadedObservations, loadedJudgments);
      yield* preflightObservationOutput(reportPath);
      yield* writeQualityReport(reportPath, report);

      expect(
        Schema.decodeSync(Schema.fromJsonString(EvalQualityReport))(
          yield* fs.readFileString(reportPath),
        ),
      ).toEqual(report);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

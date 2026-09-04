import {
  ReviewCandidate,
  ReviewDiagnostics,
  ReviewFinding,
  ReviewOutcome,
  ReviewReport,
} from "@effect-agent/pr-review/Review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { type Crypto, Effect, Path, Schema, Stream } from "effect";

import {
  digestEvalOracle,
  digestObservation,
  digestObservationSet,
  digestRepositorySnapshot,
  digestReviewRequest,
  digestText,
  EvalCase,
  EvalCaseId,
  EvalObservation,
  EvalRepositoryFile,
  EvalRepositorySnapshot,
  EvalReviewerFailure,
  EvalSuite,
  EvalTrialSucceeded,
  EvalVariantConfiguration,
  EvalFindingJudgment,
  EvalJudgmentSet,
  freezeEvalRun,
  freezeEvalSuite,
  loadEvalSuite,
  makeCurrentOpenAiVariant,
  makeQualityReport,
  rescoreEvalObservations,
  runEvalSuite,
  validateFrozenRun,
  type EvalVariant,
} from "../src/index.ts";

const fixture = Effect.gen(function* () {
  const path = yield* Path.Path;

  const loaded = yield* loadEvalSuite(
    yield* path.fromFileUrl(new URL("../fixtures/smoke-suite.json", import.meta.url)),
  );

  const cases = yield* Effect.forEach(loaded.cases, (evalCase, index) =>
    Effect.gen(function* () {
      const change = evalCase.request.changes[0];

      if (change === undefined) return yield* Effect.die("Missing smoke fixture patch");

      const snapshot = EvalRepositorySnapshot.make({
        version: 1,
        digest: evalCase.inputDigest,
        files: [
          EvalRepositoryFile.make({
            path: change.path,
            revision: "base",
            content: change.patch.split("\n")[1]?.slice(1) ?? "",
          }),
          EvalRepositoryFile.make({
            path: change.path,
            revision: "head",
            content: change.patch.split("\n")[2]?.slice(1) ?? "",
          }),
        ],
      });

      return EvalCase.make({
        ...evalCase,
        id: Schema.decodeSync(EvalCaseId)(index === 0 ? "z-first" : "a-second"),
        repository: EvalRepositorySnapshot.make({
          ...snapshot,
          digest: yield* digestRepositorySnapshot(snapshot),
        }),
        oracleVersion: 1,
        split: "development",
        relatedGroup: `smoke-${index}`,
      });
    }),
  );

  const variant = yield* makeCurrentOpenAiVariant({
    id: "baseline",
    strategy: "baseline",
    reviewerRevision: "a".repeat(40),
    model: "gpt-5.6-terra",
    reasoningEffort: "high",
  });

  return { suite: EvalSuite.make({ version: 1, cases }), configuration: variant.configuration };
});

const scriptedVariant = (configuration: EvalVariantConfiguration): EvalVariant<Crypto.Crypto> => ({
  configuration,
  review: (request) =>
    Effect.gen(function* () {
      const digest = yield* digestReviewRequest(request).pipe(Effect.orDie);

      const finding = ReviewFinding.make({
        path: "src/read.ts",
        line: 1,
        severity: "blocking",
        category: "correctness",
        title: "Retained original claim",
        body: "This synthetic claim must remain available for independent judgment.",
      });

      const candidate = ReviewCandidate.make({
        id: `${digest}:1`,
        requestDigest: digest,
        baseRevision: request.baseRevision,
        headRevision: request.headRevision,
        batch: 0,
        finding,
        disposition: "not-requested",
        publication: "published",
        evidence: [],
      });

      return ReviewOutcome.make({
        report: ReviewReport.make({ summary: "Completed", findings: [finding] }),
        diagnostics: ReviewDiagnostics.make({
          strategy: "baseline",
          requestDigest: digest,
          discovery: "complete",
          verification: "not-requested",
          patchesSupplied: 1,
          candidates: [candidate],
          activity: [],
          droppedActivityCount: 0,
          droppedCandidateCount: 0,
          stages: [],
        }),
        turns: 1,
        usage: {
          inputTokens: 0,
          uncachedInputTokens: 0,
          cachedInputTokens: 0,
          cacheWriteInputTokens: 0,
          outputTokens: 0,
        },
      });
    }),
});

describe("frozen baseline runs", () => {
  it.effect("reports heldout baseline false blockers without a paired rollout claim", () =>
    Effect.gen(function* () {
      const { suite, configuration } = yield* fixture;
      const clean = suite.cases.find((evalCase) => evalCase.kind === "clean-control");

      if (clean === undefined) return yield* Effect.die("Missing clean control");

      const frozen = yield* freezeEvalRun(
        EvalSuite.make({
          version: 1,
          cases: [EvalCase.make({ ...clean, split: "heldout" })],
        }),
        configuration,
        "heldout-baseline",
        1,
      );

      const rows = yield* runEvalSuite(frozen, [scriptedVariant(configuration)], {
        trials: 1,
        concurrency: 1,
        caseIds: [],
      }).pipe(Stream.runCollect);

      const first = rows[0];

      if (first === undefined) return yield* Effect.die("Missing first trial");

      const report = yield* makeQualityReport(
        frozen,
        rows,
        1,
        EvalJudgmentSet.make({
          version: 1,
          observationSetDigest: yield* digestObservationSet(rows),
          judgments: [
            EvalFindingJudgment.make({
              version: 1,
              caseId: clean.id,
              caseVersion: 1,
              inputDigest: clean.inputDigest,
              variantId: configuration.id,
              trial: 1,
              candidateId: `${clean.inputDigest}:1`,
              observationDigest: yield* digestObservation(first),
              oracleDigest: yield* digestEvalOracle(clean),
              label: "invalid",
              matchedDefectIds: [],
              rationale: "Synthetic false claim in the known clean control.",
              adjudicator: "test-source-audit",
              adjudicatorKind: "agent",
            }),
          ],
        }),
      );

      expect(report.rollout).toMatchObject({
        decision: "not-applicable",
        basis: "frozen-baseline-run",
        heldoutCases: 1,
        baselineFalseBlockers: 1,
      });
      expect(report.rollout.verifiedFalseBlockers).toBeUndefined();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "freezes a single split, retains failed trials and original claims in case-then-trial order",
    () =>
      Effect.gen(function* () {
        const { suite, configuration } = yield* fixture;
        const frozen = yield* freezeEvalRun(suite, configuration, "baseline-run", 2);

        expect(frozen.frozenRun).toMatchObject({
          trials: 2,
          plannedRuns: 4,
          maximumCostMicrousd: 3_999_996,
        });
        let calls = 0;
        const variant = scriptedVariant(configuration);

        const observations = yield* runEvalSuite(
          frozen,
          [
            {
              ...variant,
              review: (request, trial) =>
                Effect.suspend(() =>
                  ++calls === 1
                    ? Effect.fail(
                        EvalReviewerFailure.make({
                          errorTag: "Unavailable",
                          message: "Original first trial failed",
                        }),
                      )
                    : variant.review(request, trial),
                ),
            },
          ],
          { trials: 2, concurrency: 1, caseIds: [] },
        ).pipe(Stream.runCollect);

        expect(
          observations.map((row) => [row.caseId, row.trial, row.sequence, row.result._tag]),
        ).toEqual([
          ["z-first", 1, 0, "Failed"],
          ["z-first", 2, 1, "Succeeded"],
          ["a-second", 1, 2, "Succeeded"],
          ["a-second", 2, 3, "Succeeded"],
        ]);
        expect(new Set(observations.map((row) => row.cacheNamespace)).size).toBe(4);
        expect(new Set(observations.map((row) => row.runId)).size).toBe(1);
        expect(observations.every((row) => row.frozenRunDigest === frozen.frozenRun?.digest)).toBe(
          true,
        );
        const report = yield* makeQualityReport(frozen, observations, 2);

        expect(report.frozenRunDigest).toBe(frozen.frozenRun?.digest);
        expect(report.rollout).toMatchObject({
          decision: "not-applicable",
          basis: "frozen-baseline-run",
        });
        expect(report.variants[0]?.resources.failedTrials).toBe(1);
        expect(report.unjudgedFindings).toHaveLength(3);
        expect(
          report.unjudgedFindings.every(
            (claim) => claim.candidate?.finding.title === "Retained original claim",
          ),
        ).toBe(true);
        expect(
          (yield* rescoreEvalObservations(frozen, suite, observations, "unsupported").pipe(
            Effect.flip,
          ))._tag,
        ).toBe("EvalConfigurationError");
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "binds full ordered cases and bounds liability without weakening the paired freeze",
    () =>
      Effect.gen(function* () {
        const { suite, configuration } = yield* fixture;
        const frozen = yield* freezeEvalRun(suite, configuration, "identity", 2);
        const first = frozen.cases[0];

        if (first === undefined || first.repository === undefined)
          return yield* Effect.die("Missing fixture");

        const changedSource = EvalRepositorySnapshot.make({
          ...first.repository,
          files: first.repository.files.map((file) =>
            EvalRepositoryFile.make({ ...file, content: `${file.content}\n` }),
          ),
        });

        const changedOracle = EvalCase.make({ ...first, oracleVersion: 2 });
        const { oracleDigest: _oracleDigest, ...withoutOracleDigest } = first;

        for (const changed of [
          EvalSuite.make({ ...frozen, cases: [...frozen.cases].reverse() }),
          EvalSuite.make({
            ...frozen,
            cases: [
              EvalCase.make({ ...first, provenance: "Changed provenance" }),
              ...frozen.cases.slice(1),
            ],
          }),
          EvalSuite.make({
            ...frozen,
            cases: [EvalCase.make(withoutOracleDigest), ...frozen.cases.slice(1)],
          }),
          EvalSuite.make({
            ...frozen,
            cases: [
              EvalCase.make({
                ...changedOracle,
                oracleDigest: yield* digestEvalOracle(changedOracle),
              }),
              ...frozen.cases.slice(1),
            ],
          }),
          EvalSuite.make({
            ...frozen,
            cases: [
              EvalCase.make({
                ...first,
                repository: EvalRepositorySnapshot.make({
                  ...changedSource,
                  digest: yield* digestRepositorySnapshot(changedSource),
                }),
              }),
              ...frozen.cases.slice(1),
            ],
          }),
        ]) {
          expect((yield* validateFrozenRun(changed).pipe(Effect.flip)).message).toContain(
            "identity changed",
          );
        }

        const six = EvalSuite.make({
          version: 1,
          cases: Array.from({ length: 6 }, (_, index) =>
            EvalCase.make({ ...first, id: Schema.decodeSync(EvalCaseId)(`case-${index}`) }),
          ),
        });

        expect(
          (yield* freezeEvalRun(six, configuration, "limit", 20)).frozenRun?.maximumCostMicrousd,
        ).toBe(119_999_880);

        const seven = EvalSuite.make({
          ...six,
          cases: [
            ...six.cases,
            EvalCase.make({ ...first, id: Schema.decodeSync(EvalCaseId)("seventh") }),
          ],
        });

        expect(
          (yield* freezeEvalRun(seven, configuration, "too-large", 20).pipe(Effect.flip))._tag,
        ).toBe("EvalConfigurationError");
        for (const trials of [0, 21])
          expect(
            (yield* freezeEvalRun(suite, configuration, "bad-trials", trials).pipe(Effect.flip))
              ._tag,
          ).toBe("EvalConfigurationError");

        const verified = yield* makeCurrentOpenAiVariant({
          id: "verified",
          strategy: "verified",
          reviewerRevision: "a".repeat(40),
        });

        expect(
          (yield* freezeEvalRun(suite, verified.configuration, "not-baseline", 2).pipe(Effect.flip))
            ._tag,
        ).toBe("EvalConfigurationError");
        expect(
          (yield* freezeEvalSuite(
            frozen,
            [configuration, verified.configuration],
            "not-paired",
          ).pipe(Effect.flip))._tag,
        ).toBe("EvalConfigurationError");

        const leaked = EvalSuite.make({
          ...suite,
          cases: suite.cases.map((evalCase, index) =>
            EvalCase.make({
              ...evalCase,
              relatedGroup: "shared",
              split: index === 0 ? "development" : "heldout",
            }),
          ),
        });

        expect(
          (yield* freezeEvalRun(leaked, configuration, "leaked", 2).pipe(Effect.flip)).message,
        ).toContain("crosses");
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects partial execution before invoking the reviewer", () =>
    Effect.gen(function* () {
      const { suite, configuration } = yield* fixture;
      const frozen = yield* freezeEvalRun(suite, configuration, "execution", 2);
      let calls = 0;

      const variant: EvalVariant<never> = {
        configuration,
        review: () =>
          Effect.sync(() => {
            calls++;
          }).pipe(Effect.andThen(Effect.die("Must not run"))),
      };

      for (const options of [
        { trials: 1, concurrency: 1, caseIds: [] },
        { trials: 2, concurrency: 2, caseIds: [] },
        { trials: 2, concurrency: 1, caseIds: [Schema.decodeSync(EvalCaseId)("z-first")] },
      ])
        expect(
          (yield* runEvalSuite(frozen, [variant], options).pipe(Stream.runCollect, Effect.flip))
            ._tag,
        ).toBe("EvalConfigurationError");
      expect(
        (yield* runEvalSuite(
          frozen,
          [
            {
              ...variant,
              configuration: EvalVariantConfiguration.make({
                ...configuration,
                reasoningEffort: "low",
              }),
            },
          ],
          { trials: 2, concurrency: 1, caseIds: [] },
        ).pipe(Stream.runCollect, Effect.flip))._tag,
      ).toBe("EvalConfigurationError");
      expect(calls).toBe(0);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "rejects incomplete, duplicated, reordered, retagged or candidate-stripped observations",
    () =>
      Effect.gen(function* () {
        const { suite, configuration } = yield* fixture;
        const frozen = yield* freezeEvalRun(suite, configuration, "report", 2);

        const rows = yield* runEvalSuite(frozen, [scriptedVariant(configuration)], {
          trials: 2,
          concurrency: 1,
          caseIds: [],
        }).pipe(Stream.runCollect);

        const first = rows[0];
        const second = rows[1];

        if (
          first === undefined ||
          second === undefined ||
          first.result._tag !== "Succeeded" ||
          first.result.outcome.diagnostics === undefined
        )
          return yield* Effect.die("Missing returned trial");
        const outcome = first.result.outcome;
        const diagnostics = first.result.outcome.diagnostics;
        const { diagnostics: _diagnostics, ...withoutDiagnostics } = outcome;
        const changedRun = "another-run";

        const corrupted: ReadonlyArray<ReadonlyArray<EvalObservation>> = [
          rows.slice(1),
          [first, first, ...rows.slice(2)],
          [second, first, ...rows.slice(2)],
          [EvalObservation.make({ ...first, sequence: 1 }), ...rows.slice(1)],
          [
            first,
            EvalObservation.make({ ...second, cacheNamespace: first.cacheNamespace }),
            ...rows.slice(2),
          ],
          [
            EvalObservation.make({
              ...first,
              runId: changedRun,
              cacheNamespace: yield* digestText(`${changedRun}:${first.caseId}:baseline:1`),
            }),
            ...rows.slice(1),
          ],
          [
            EvalObservation.make({
              ...first,
              variant: EvalVariantConfiguration.make({ ...configuration, reasoningEffort: "low" }),
            }),
            ...rows.slice(1),
          ],
          [
            EvalObservation.make({
              ...first,
              result: EvalTrialSucceeded.make({ outcome: ReviewOutcome.make(withoutDiagnostics) }),
            }),
            ...rows.slice(1),
          ],
          [
            EvalObservation.make({
              ...first,
              result: EvalTrialSucceeded.make({
                outcome: ReviewOutcome.make({
                  ...outcome,
                  diagnostics: ReviewDiagnostics.make({ ...diagnostics, candidates: [] }),
                }),
              }),
            }),
            ...rows.slice(1),
          ],
        ];

        for (const observations of corrupted)
          expect((yield* makeQualityReport(frozen, observations, 2).pipe(Effect.flip))._tag).toBe(
            "EvalReportError",
          );
        expect((yield* makeQualityReport(frozen, rows, 3).pipe(Effect.flip))._tag).toBe(
          "EvalReportError",
        );
        expect((yield* makeQualityReport(suite, rows, 2).pipe(Effect.flip)).message).toContain(
          "original frozen run manifest",
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

import {
  ReviewFinding,
  ReviewOutcome,
  ReviewReport,
  type ReviewSeverity,
  ReviewCandidate,
  ReviewDiagnostics,
  ReviewEvidence,
  ReviewRequest,
} from "@effect-agent/pr-review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { DateTime, Effect, FileSystem, Path, Schema, Stream } from "effect";

import {
  CURRENT_RUNNER_VERSION,
  digestObservationSet,
  EvalCase,
  EvalCaseId,
  EvalDefectId,
  EvalEvidence,
  EvalExpectedDefect,
  EvalFindingJudgment,
  EvalInputDigest,
  EvalJudgmentSet,
  EvalObservation,
  EvalObservationSetDigest,
  EvalQualityReport,
  EvalTrialFailed,
  EvalTrialSucceeded,
  EvalVariantConfiguration,
  EvalSuite,
  loadJudgmentSet,
  loadObservationFiles,
  loadEvalSuite,
  makeQualityReport,
  renderQualityReport,
  writeObservations,
  writeQualityReport,
  digestEvalOracle,
  digestObservation,
  digestRepositorySnapshot,
  digestReviewRequest,
  EvalRepositorySnapshot,
  EvalRepositoryFile,
  freezeEvalSuite,
  makeCurrentOpenAiVariant,
  runEvalSuite,
  rescoreEvalObservations,
  type EvalVariant,
} from "../src/index.ts";

const loadFixture = Effect.gen(function* () {
  const path = yield* Path.Path;

  return yield* loadEvalSuite(
    yield* path.fromFileUrl(new URL("../fixtures/smoke-suite.json", import.meta.url)),
  );
});

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

const succeeded = (
  findings: ReadonlyArray<ReviewFinding>,
  cost?: number,
  coverage: Pick<ReviewOutcome, "incomplete" | "exhausted"> = {},
  reservedCostMicrousd?: number,
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
        ...(reservedCostMicrousd === undefined ? {} : { reservedCostMicrousd }),
      },
    }),
  });

const observation = (
  evalCase: EvalCase,
  variant: EvalVariantConfiguration,
  trial: number,
  result: EvalTrialSucceeded | EvalTrialFailed,
  elapsedMillis = trial * 100,
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
    elapsedMillis,
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
  it.effect("preserves explicit agent provenance and treats historical names as unknown", () =>
    Effect.gen(function* () {
      const original = (yield* loadFixture).cases[0];

      if (original === undefined) return yield* Effect.die("Missing fixture");
      const suite = EvalSuite.make({ version: 1, cases: [original] });
      const variant = configuration("provenance");

      const observations = [
        observation(
          original,
          variant,
          1,
          succeeded([
            finding("Legacy judgment"),
            finding("Agent judgment"),
            finding("Human judgment"),
            finding("Unknown judgment"),
          ]),
        ),
      ];

      const supplied = judgmentSet(yield* digestObservationSet(observations), [
        EvalFindingJudgment.make({
          ...judgment(original, variant, 1, 0, "invalid"),
          adjudicator: "human-reviewer",
        }),
        EvalFindingJudgment.make({
          ...judgment(original, variant, 1, 1, "invalid"),
          adjudicator: "maintainer",
          adjudicatorKind: "agent",
        }),
        EvalFindingJudgment.make({
          ...judgment(original, variant, 1, 2, "invalid"),
          adjudicator: "agent-looking-name",
          adjudicatorKind: "human",
        }),
        EvalFindingJudgment.make({
          ...judgment(original, variant, 1, 3, "invalid"),
          adjudicatorKind: "unknown",
        }),
      ]);

      const decoded = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(EvalJudgmentSet))(
        Schema.encodeSync(Schema.fromJsonString(EvalJudgmentSet))(supplied),
      );

      expect(decoded.judgments[0]?.adjudicatorKind).toBeUndefined();
      expect(decoded.judgments[1]?.adjudicatorKind).toBe("agent");
      const report = yield* makeQualityReport(suite, observations, 1, decoded);

      expect(report.judgmentProvenance).toEqual({ agent: 1, human: 1, unknown: 2 });
      expect(renderQualityReport(report)).toContain(
        "Judgment provenance: 1 agent, 1 human, 2 unknown (all trials).",
      );

      const { judgmentProvenance: _provenance, ...historical } =
        Schema.encodeSync(EvalQualityReport)(report);

      expect(
        renderQualityReport(yield* Schema.decodeUnknownEffect(EvalQualityReport)(historical)),
      ).toContain("Judgment provenance: unknown (not recorded).");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "separates verification losses, rejects stale oracles, and rescores both strategies",
    () =>
      Effect.gen(function* () {
        const loaded = yield* loadFixture;
        const known = loaded.cases.find((evalCase) => evalCase.kind === "known-defects");
        const clean = loaded.cases.find((evalCase) => evalCase.kind === "clean-control");

        if (known === undefined || clean === undefined)
          return yield* Effect.die("Missing fixtures");

        const source = EvalRepositorySnapshot.make({
          version: 1,
          digest: known.inputDigest,
          files: [
            EvalRepositoryFile.make({
              path: "src/read.ts",
              revision: "base",
              content: "export const read = (value?: string) => value?.length ?? 0;",
            }),
            EvalRepositoryFile.make({
              path: "src/read.ts",
              revision: "head",
              content: "export const read = (value?: string) => value.length;",
            }),
          ],
        });

        const repository = EvalRepositorySnapshot.make({
          ...source,
          digest: yield* digestRepositorySnapshot(source),
        });

        const suite = EvalSuite.make({
          version: 1,
          cases: [
            EvalCase.make({
              ...known,
              repository,
              oracleVersion: 1,
              split: "heldout",
              relatedGroup: "known",
            }),
            EvalCase.make({
              ...clean,
              repository,
              oracleVersion: 1,
              split: "heldout",
              relatedGroup: "clean",
            }),
            EvalCase.make({
              ...clean,
              id: Schema.decodeSync(EvalCaseId)("development-clean"),
              repository,
              oracleVersion: 1,
              split: "development",
              relatedGroup: "development",
            }),
          ],
        });

        const baseline = yield* makeCurrentOpenAiVariant({
          id: "baseline",
          strategy: "baseline",
          reviewerRevision: "a".repeat(40),
        });

        const verified = yield* makeCurrentOpenAiVariant({
          id: "verified",
          strategy: "verified",
          reviewerRevision: "a".repeat(40),
        });

        const frozen = yield* freezeEvalSuite(
          suite,
          [baseline.configuration, verified.configuration],
          "comparison",
        );

        const variants: Array<EvalVariant<never>> = [baseline, verified].map((variant) => ({
          configuration: variant.configuration,
          review: (request) =>
            Effect.sync(() => {
              const isKnown = request.changes[0]?.patch === known.request.changes[0]?.patch;
              const claim = finding(isKnown ? "Optional read crashes" : "Invented blocker");
              const strategy = variant.configuration.strategy ?? "baseline";
              const digest = isKnown ? known.inputDigest : clean.inputDigest;

              const candidate = ReviewCandidate.make({
                id: `${digest}:1`,
                requestDigest: digest,
                baseRevision: request.baseRevision,
                headRevision: request.headRevision,
                batch: 0,
                finding: claim,
                disposition: strategy === "baseline" ? "not-requested" : "refuted",
                publication: strategy === "baseline" ? "published" : "suppressed",
                reason: "Claim assessed independently in this test",
                evidence: [
                  ReviewEvidence.make({
                    kind: "diff",
                    revision: request.headRevision,
                    path: claim.path,
                    startLine: 1,
                    endLine: 1,
                  }),
                ],
              });

              return ReviewOutcome.make({
                ...succeeded([]).outcome,
                report: ReviewReport.make({
                  summary: "Completed",
                  findings: strategy === "baseline" ? [claim] : [],
                }),
                diagnostics: ReviewDiagnostics.make({
                  strategy,
                  requestDigest: digest,
                  discovery: "complete",
                  verification: strategy === "baseline" ? "not-requested" : "complete",
                  patchesSupplied: 1,
                  candidates: [candidate],
                  activity: [],
                  droppedActivityCount: 0,
                  droppedCandidateCount: 0,
                  stages: [],
                }),
              });
            }),
        }));

        const observations = yield* runEvalSuite(frozen, variants, {
          trials: 3,
          concurrency: 1,
          caseIds: [],
        }).pipe(Stream.runCollect);

        const unjudged = yield* makeQualityReport(frozen, observations, 3);

        expect(unjudged.version).toBe(5);
        expect(
          unjudged.variants.map((variant) => [
            variant.discoveryMisses,
            variant.unresolvedDiscoveryMisses,
          ]),
        ).toEqual([
          [0, 1],
          [0, 1],
        ]);
        expect(unjudged.unjudgedFindings).toHaveLength(18);
        expect(
          unjudged.unjudgedFindings.every(
            (entry) => entry.candidateId !== undefined && entry.observationDigest !== undefined,
          ),
        ).toBe(true);

        const adjudicate = (rows: ReadonlyArray<EvalObservation>) =>
          Effect.forEach(
            rows,
            Effect.fn(function* (row) {
              const evalCase = frozen.cases.find((entry) => entry.id === row.caseId);

              if (evalCase === undefined) return yield* Effect.die("Missing case");

              return EvalFindingJudgment.make({
                version: 1,
                caseId: row.caseId,
                caseVersion: 1,
                inputDigest: row.inputDigest,
                variantId: row.variant.id,
                trial: row.trial,
                candidateId: `${row.inputDigest}:1`,
                observationDigest: yield* digestObservation(row),
                oracleDigest: yield* digestEvalOracle(evalCase),
                label: evalCase.kind === "known-defects" ? "matches-expected" : "invalid",
                matchedDefectIds: evalCase.expectedDefects.map((defect) => defect.id),
                rationale: "Independent source-based oracle",
                adjudicator: "maintainer",
              });
            }),
          );

        const judgments = yield* adjudicate(observations);

        const report = yield* makeQualityReport(
          frozen,
          observations,
          3,
          judgmentSet(yield* digestObservationSet(observations), judgments),
        );

        const result = report.variants.find(
          (variant) => variant.configuration.strategy === "verified",
        );

        expect(result).toMatchObject({
          discoveryMisses: 0,
          unresolvedDiscoveryMisses: 0,
          validCandidatesRefuted: 1,
          validCandidatesWithheld: 1,
          validBlockingCandidatesRefuted: 1,
          blockerRecall: { numerator: 0, denominator: 1 },
        });
        expect(report.rollout).toMatchObject({
          decision: "experimental",
          baselineFalseBlockers: 1,
          verifiedFalseBlockers: 0,
        });
        expect(report.rollout.reasons).toContain(
          "Verification wrongly refuted a valid blocking candidate.",
        );

        const supported = observations.map((row) => {
          if (
            row.caseId !== known.id ||
            row.variant.strategy !== "verified" ||
            row.result._tag !== "Succeeded" ||
            row.result.outcome.diagnostics === undefined
          )
            return row;

          const candidates = row.result.outcome.diagnostics.candidates.map((candidate) =>
            ReviewCandidate.make({
              ...candidate,
              disposition: "supported",
              publication: "published",
            }),
          );

          return EvalObservation.make({
            ...row,
            result: EvalTrialSucceeded.make({
              outcome: ReviewOutcome.make({
                ...row.result.outcome,
                report: ReviewReport.make({
                  summary: "Supported original candidate",
                  findings: candidates.map((candidate) => candidate.finding),
                }),
                diagnostics: ReviewDiagnostics.make({
                  ...row.result.outcome.diagnostics,
                  candidates,
                }),
              }),
            }),
          });
        });

        const eligible = yield* makeQualityReport(
          frozen,
          supported,
          3,
          judgmentSet(yield* digestObservationSet(supported), yield* adjudicate(supported)),
        );

        expect(eligible.rollout.decision).toBe("eligible");

        const supportedDigest = yield* digestObservationSet(supported);
        const supportedJudgments = yield* adjudicate(supported);

        for (const severity of ["important", "nit", "blocking", undefined] as const) {
          const independentlyJudged = supportedJudgments.map((entry) =>
            entry.caseId === known.id && entry.trial === 1
              ? EvalFindingJudgment.make({
                  ...entry,
                  label: "new-valid",
                  ...(severity === undefined ? {} : { severity }),
                  matchedDefectIds: [],
                })
              : entry,
          );

          const calibrated = yield* makeQualityReport(
            frozen,
            supported,
            3,
            judgmentSet(supportedDigest, independentlyJudged),
          );

          const overstated = severity === "important" || severity === "nit" ? 1 : 0;

          for (const variant of calibrated.variants) {
            const knownCase = variant.cases.find((entry) => entry.caseId === known.id);

            expect(knownCase?.firstTrialBlockingFindings).toEqual({
              aligned: severity === "blocking" ? 1 : 0,
              overstated,
              unresolved: severity === undefined ? 1 : 0,
              precision: {
                numerator: severity === "blocking" ? 1 : 0,
                denominator: severity === undefined ? 0 : 1,
                status: severity === undefined ? "unresolved" : "measured",
              },
            });
            expect(knownCase?.blockerRecall.numerator).toBe(0);
            expect(knownCase?.unresolvedRepeatedTrialInstability).toBe(true);
          }
          expect(calibrated.unmappedValidFindings).toHaveLength(2);
          expect(calibrated.rollout).toMatchObject({
            decision: "experimental",
            baselineFalseBlockers: 1 + overstated,
            verifiedFalseBlockers: overstated,
          });
          expect(calibrated.rollout.reasons).toContain(
            "New valid defects require a versioned oracle correction and rescoring both strategies.",
          );
        }

        const unresolved = supported.map((row) => {
          if (
            row.caseId !== known.id ||
            row.variant.strategy !== "verified" ||
            row.result._tag !== "Succeeded" ||
            row.result.outcome.diagnostics === undefined
          )
            return row;

          return EvalObservation.make({
            ...row,
            result: EvalTrialSucceeded.make({
              outcome: ReviewOutcome.make({
                ...row.result.outcome,
                incomplete: true,
                diagnostics: ReviewDiagnostics.make({
                  ...row.result.outcome.diagnostics,
                  verification: "incomplete",
                  candidates: row.result.outcome.diagnostics.candidates.map((candidate) =>
                    ReviewCandidate.make({
                      ...candidate,
                      disposition: "unresolved",
                      publication: "unverified",
                    }),
                  ),
                }),
              }),
            }),
          });
        });

        const withheld = yield* makeQualityReport(
          frozen,
          unresolved,
          3,
          judgmentSet(yield* digestObservationSet(unresolved), yield* adjudicate(unresolved)),
        );

        expect(
          withheld.variants.find((variant) => variant.configuration.strategy === "verified"),
        ).toMatchObject({
          validCandidatesRefuted: 0,
          validCandidatesWithheld: 1,
          discoveryMisses: 0,
        });
        expect(withheld.rollout.decision).toBe("experimental");

        const novelJudgments = judgments.map((entry) =>
          entry.caseId === clean.id && entry.variantId === "verified" && entry.trial === 1
            ? EvalFindingJudgment.make({
                ...entry,
                label: "new-valid",
                severity: "blocking",
                matchedDefectIds: [],
              })
            : entry,
        );

        const novel = yield* makeQualityReport(
          frozen,
          observations,
          3,
          judgmentSet(yield* digestObservationSet(observations), novelJudgments),
        );

        expect(
          novel.variants.find((variant) => variant.configuration.strategy === "verified")
            ?.validBlockingCandidatesRefuted,
        ).toBe(2);
        expect(novel.unmappedValidFindings).toHaveLength(1);
        expect(novel.rollout.reasons).toContain(
          "New valid defects require a versioned oracle correction and rescoring both strategies.",
        );

        const corrected = EvalSuite.make({
          version: 1,
          cases: frozen.cases.map((evalCase) => {
            if (evalCase.kind !== "known-defects") return evalCase;
            const { oracleDigest: _digest, ...fields } = evalCase;

            return EvalCase.make({
              ...fields,
              oracleVersion: 2,
              expectedDefects: evalCase.expectedDefects.map((defect) =>
                EvalExpectedDefect.make({ ...defect, severity: "important" }),
              ),
            });
          }),
        });

        expect(
          (yield* makeQualityReport(corrected, observations, 3).pipe(Effect.result))._tag,
        ).toBe("Failure");

        const rebound = yield* rescoreEvalObservations(
          frozen,
          corrected,
          observations,
          "oracle-correction",
        );

        expect(rebound.observations).toHaveLength(18);
        expect(rebound.observations[0]?.previousObservationDigest).toBe(
          yield* digestObservation(observations[0]!),
        );
        expect(rebound.observations.map((row) => row.result)).toEqual(
          observations.map((row) => row.result),
        );
        expect(
          (yield* makeQualityReport(rebound.suite, rebound.observations, 3)).rollout.decision,
        ).toBe("experimental");
        expect(
          (yield* rescoreEvalObservations(
            frozen,
            corrected,
            observations.filter((row) => row.variant.id === "baseline"),
            "partial-correction",
          ).pipe(Effect.result))._tag,
        ).toBe("Failure");
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "keeps failed retained candidates unresolved until judgment while counting empty-trial misses",
    () =>
      Effect.gen(function* () {
        const loaded = yield* loadFixture;
        const known = loaded.cases.find((evalCase) => evalCase.kind === "known-defects");

        if (known === undefined) return yield* Effect.die("Missing known-defect fixture");
        const defectId = known.expectedDefects[0]?.id;

        if (defectId === undefined) return yield* Effect.die("Missing expected defect");
        const empty = EvalCase.make({ ...known, id: Schema.decodeSync(EvalCaseId)("empty-trial") });
        const suite = EvalSuite.make({ version: 1, cases: [known, empty] });
        const variant = configuration("retained-failure");
        const claim = finding("Optional value is dereferenced");

        const candidate = ReviewCandidate.make({
          id: `${known.inputDigest}:1`,
          requestDigest: known.inputDigest,
          baseRevision: known.request.baseRevision,
          headRevision: known.request.headRevision,
          batch: 0,
          finding: claim,
          disposition: "not-requested",
          publication: "published",
          evidence: [],
        });

        const first = observation(
          known,
          variant,
          1,
          EvalTrialFailed.make({
            errorTag: "ProviderUnavailable",
            message: "Stopped after recording the candidate",
            diagnostics: ReviewDiagnostics.make({
              strategy: "baseline",
              requestDigest: known.inputDigest,
              discovery: "failed",
              verification: "not-requested",
              patchesSupplied: 1,
              candidates: [candidate],
              activity: [],
              droppedActivityCount: 0,
              droppedCandidateCount: 0,
              stages: [],
            }),
          }),
        );

        const observations = [
          first,
          observation(known, variant, 2, succeeded([claim])),
          observation(empty, variant, 1, succeeded([])),
          observation(empty, variant, 2, succeeded([])),
        ];

        const digest = yield* digestObservationSet(observations);
        const laterJudgment = judgment(known, variant, 2, 0, "matches-expected", [defectId]);

        const pending = yield* makeQualityReport(
          suite,
          observations,
          2,
          judgmentSet(digest, [laterJudgment]),
        );

        const pendingVariant = pending.variants[0];

        expect(pendingVariant).toMatchObject({
          discoveryMisses: 1,
          unresolvedDiscoveryMisses: 1,
          firstTrialFailures: 1,
          repeatedTrialInstability: 1,
          unresolvedRepeatedTrialInstability: 0,
        });
        expect(
          pendingVariant?.cases.find((evalCase) => evalCase.caseId === known.id),
        ).toMatchObject({
          discoveryMisses: 0,
          unresolvedDiscoveryMisses: 1,
          blockerDetection: { status: "unresolved" },
          blockerRecall: { status: "unresolved" },
          laterOnlyBlockingDefects: [],
          firstTrialFailureTag: "ProviderUnavailable",
          repeatedTrialInstability: true,
          unresolvedRepeatedTrialInstability: false,
        });
        expect(
          pendingVariant?.cases.find((evalCase) => evalCase.caseId === empty.id),
        ).toMatchObject({
          discoveryMisses: 1,
          unresolvedDiscoveryMisses: 0,
          blockerDetection: { status: "measured" },
        });
        expect(renderQualityReport(pending)).toContain(
          "discovery-misses 1 confirmed; 1 awaiting judgment",
        );

        const firstJudgment = EvalFindingJudgment.make({
          version: 1,
          caseId: known.id,
          caseVersion: 1,
          inputDigest: known.inputDigest,
          variantId: variant.id,
          trial: 1,
          candidateId: candidate.id,
          observationDigest: yield* digestObservation(first),
          oracleDigest: yield* digestEvalOracle(known),
          label: "unclear",
          matchedDefectIds: [],
          rationale: "The first candidate still requires independent judgment",
          adjudicator: "maintainer",
        });

        const unclear = yield* makeQualityReport(
          suite,
          observations,
          2,
          judgmentSet(digest, [laterJudgment, firstJudgment]),
        );

        expect(unclear.variants[0]).toMatchObject({
          discoveryMisses: 1,
          unresolvedDiscoveryMisses: 1,
          laterOnlyBlockingDefects: [],
        });

        const invalid = yield* makeQualityReport(
          suite,
          observations,
          2,
          judgmentSet(digest, [
            laterJudgment,
            EvalFindingJudgment.make({ ...firstJudgment, label: "invalid" }),
          ]),
        );

        expect(invalid.variants[0]).toMatchObject({
          discoveryMisses: 2,
          unresolvedDiscoveryMisses: 0,
          firstTrialFailures: 1,
          laterOnlyBlockingDefects: [{ caseId: known.id, defectId, firstFoundTrial: 2 }],
        });

        const matched = yield* makeQualityReport(
          suite,
          observations,
          2,
          judgmentSet(digest, [
            laterJudgment,
            EvalFindingJudgment.make({
              ...firstJudgment,
              label: "matches-expected",
              matchedDefectIds: [defectId],
            }),
          ]),
        );

        expect(matched.variants[0]).toMatchObject({
          discoveryMisses: 1,
          unresolvedDiscoveryMisses: 0,
          firstTrialFailures: 1,
          laterOnlyBlockingDefects: [],
        });
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not infer repeated-trial instability from missing judgments", () =>
    Effect.gen(function* () {
      const loaded = yield* loadFixture;
      const known = loaded.cases.find((evalCase) => evalCase.kind === "known-defects");

      if (known === undefined) return yield* Effect.die("Missing known-defect fixture");
      const defectId = known.expectedDefects[0]?.id;

      if (defectId === undefined) return yield* Effect.die("Missing expected defect");
      const suite = EvalSuite.make({ version: 1, cases: [known] });
      const variant = configuration("partial-judgments");

      const observations = [
        observation(known, variant, 1, succeeded([finding("Optional value is dereferenced")])),
        observation(known, variant, 2, succeeded([finding("Another claimed regression")])),
      ];

      const digest = yield* digestObservationSet(observations);
      const first = judgment(known, variant, 1, 0, "matches-expected", [defectId]);

      const pending = yield* makeQualityReport(
        suite,
        observations,
        2,
        judgmentSet(digest, [first]),
      );

      expect(pending.variants[0]).toMatchObject({
        repeatedTrialInstability: 0,
        unresolvedRepeatedTrialInstability: 1,
      });
      expect(pending.variants[0]?.cases[0]).toMatchObject({
        repeatedTrialInstability: false,
        unresolvedRepeatedTrialInstability: true,
      });
      expect(renderQualityReport(pending)).toContain(
        "unstable-cases 0 confirmed; 1 awaiting judgment",
      );

      const stable = yield* makeQualityReport(
        suite,
        observations,
        2,
        judgmentSet(digest, [
          first,
          judgment(known, variant, 2, 0, "matches-expected", [defectId]),
        ]),
      );

      expect(stable.variants[0]).toMatchObject({
        repeatedTrialInstability: 0,
        unresolvedRepeatedTrialInstability: 0,
      });

      const changed = yield* makeQualityReport(
        suite,
        observations,
        2,
        judgmentSet(digest, [first, judgment(known, variant, 2, 0, "invalid")]),
      );

      expect(changed.variants[0]).toMatchObject({
        repeatedTrialInstability: 1,
        unresolvedRepeatedTrialInstability: 0,
      });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("never passes clean controls with pending or excluded patches", () =>
    Effect.gen(function* () {
      const loaded = yield* loadFixture;
      const clean = loaded.cases.find((evalCase) => evalCase.kind === "clean-control");

      if (clean === undefined) return yield* Effect.die("Missing fixture");

      const excludedRequest = ReviewRequest.make({
        ...clean.request,
        unreviewedPaths: ["src/excluded.ts"],
      });

      const excluded = EvalCase.make({
        ...clean,
        id: Schema.decodeSync(EvalCaseId)("excluded"),
        request: excludedRequest,
        inputDigest: yield* digestReviewRequest(excludedRequest),
      });

      const suite = EvalSuite.make({ version: 1, cases: [clean, excluded] });
      const variant = configuration("coverage");

      const report = yield* makeQualityReport(
        suite,
        [
          observation(
            clean,
            variant,
            1,
            EvalTrialSucceeded.make({
              outcome: ReviewOutcome.make({
                ...succeeded([]).outcome,
                pendingPaths: ["src/read.ts"],
              }),
            }),
          ),
          observation(excluded, variant, 1, succeeded([])),
        ],
        1,
      );

      expect(report.variants[0]?.cleanControls).toEqual({ passed: 0, total: 2 });
      expect(report.variants[0]?.resources.incompleteTrials).toBe(2);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
  it.effect("pins first-pass quality while keeping later trials diagnostic", () =>
    Effect.gen(function* () {
      const loadedSuite = yield* loadFixture;
      const loadedKnown = loadedSuite.cases.find((evalCase) => evalCase.kind === "known-defects");
      const clean = loadedSuite.cases.find((evalCase) => evalCase.kind === "clean-control");

      expect(loadedKnown).toBeDefined();
      expect(clean).toBeDefined();
      if (loadedKnown === undefined || clean === undefined) return;
      const importantDefectId = Schema.decodeSync(EvalDefectId)("important-follow-up");

      const known = EvalCase.make({
        ...loadedKnown,
        expectedDefects: [
          ...loadedKnown.expectedDefects,
          EvalExpectedDefect.make({
            id: importantDefectId,
            severity: "important",
            invariant: "A secondary issue is actionable but does not block merge.",
            evidence: [
              EvalEvidence.make({
                path: "src/read.ts",
                line: 1,
                description: "Synthetic evidence for severity calibration.",
              }),
            ],
          }),
        ],
      });

      const suite = EvalSuite.make({
        ...loadedSuite,
        cases: loadedSuite.cases.map((evalCase) => (evalCase.id === known.id ? known : evalCase)),
      });

      const defectId = known.expectedDefects[0]?.id;

      expect(defectId).toBeDefined();
      if (defectId === undefined) return;

      const current = configuration("current");
      const candidate = configuration("candidate");
      const currentFirstUnjudged = finding("Unjudged non-blocker", "important");
      const currentFirstInvalid = finding("Speculative cleanup");
      const repeatedBlocker = finding("Optional value is dereferenced");
      const candidateBlocker = finding("Missing undefined guard");
      const candidateOverstated = finding("Important issue promoted to blocker");
      const candidateNovel = finding("Another valid regression", "important");
      const candidateUnclear = finding("Ambiguous edge case");

      const observations = [
        observation(known, current, 1, succeeded([currentFirstUnjudged, currentFirstInvalid], 5)),
        observation(known, current, 2, succeeded([repeatedBlocker, repeatedBlocker])),
        observation(clean, current, 1, succeeded([])),
        observation(
          clean,
          current,
          2,
          EvalTrialFailed.make({
            errorTag: "RateLimitError",
            message: "Try later",
            estimatedCostMicrousd: 11,
          }),
        ),
        observation(
          known,
          candidate,
          1,
          succeeded([candidateBlocker, candidateOverstated, candidateNovel, candidateUnclear], 7),
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
        judgment(known, candidate, 1, 1, "matches-expected", [importantDefectId]),
        judgment(known, candidate, 1, 2, "new-valid"),
        judgment(known, candidate, 1, 3, "unclear"),
        judgment(known, candidate, 2, 0, "matches-expected", [defectId]),
      ]);

      const report = yield* makeQualityReport(suite, observations, 2, judgments);

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
      expect(currentReport.blockerDetection).toMatchObject({
        numerator: 0,
        denominator: 1,
        status: "unresolved",
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
      expect(currentReport.firstTrialBlockingFindings).toMatchObject({
        aligned: 0,
        overstated: 1,
        unresolved: 0,
        precision: { numerator: 0, denominator: 1, status: "measured" },
      });
      expect(currentReport.cleanControls).toMatchObject({ passed: 1, total: 1 });
      expect(currentReport.resources).toMatchObject({
        attemptedTrials: 4,
        succeededTrials: 3,
        failedTrials: 1,
        costedSucceededTrials: 1,
        uncostedSucceededTrials: 2,
        estimatedCostMicrousd: 16,
        costedFailedTrials: 1,
        reservationKnownTrials: 0,
        reservationUnknownTrials: 4,
        inputTokens: 30,
        outputTokens: 9,
        elapsedMillis: 600,
      });
      expect(currentReport.resources.reservedCostMicrousd).toBeUndefined();
      expect(renderQualityReport(report)).toContain(
        "outstanding reservations unavailable (4 trials unknown)",
      );
      expect(currentReport.resources.failuresByTag).toEqual([
        { errorTag: "RateLimitError", count: 1 },
      ]);

      expect(candidateReport.blockerRecall).toMatchObject({
        numerator: 1,
        denominator: 1,
        status: "measured",
      });
      expect(candidateReport.blockerDetection).toMatchObject({
        numerator: 1,
        denominator: 1,
        status: "measured",
      });
      expect(candidateReport.blockerCases.complete).toBe(1);
      expect(candidateReport.cleanControls).toMatchObject({ passed: 0, total: 1 });
      expect(candidateReport.firstTrialFindings.precision.status).toBe("unresolved");
      expect(candidateReport.firstTrialFindings).toMatchObject({
        valid: 3,
        invalid: 0,
        unclear: 1,
        unjudged: 0,
      });
      expect(candidateReport.firstTrialBlockingFindings).toMatchObject({
        aligned: 1,
        overstated: 1,
        unresolved: 1,
        precision: { numerator: 1, denominator: 2, status: "unresolved" },
      });
      expect(candidateReport.firstTrialFailures).toBe(1);
      expect(candidateReport.resources).toMatchObject({
        failedTrials: 1,
        inputTokens: 30,
        outputTokens: 9,
        costedSucceededTrials: 3,
        costedFailedTrials: 0,
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

  it.effect(
    "separates settled estimates, reservations, and elapsed time across all returned trial outcomes",
    () =>
      Effect.gen(function* () {
        const suite = yield* loadFixture;
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
            succeeded([finding("Recorded blocker")], 5, { incomplete: true }, 70_000),
            201,
          ),
          observation(known, variant, 2, succeeded([], 3, {}, 0), 1_000),
          observation(clean, variant, 1, succeeded([], undefined, { exhausted: "cost" })),
          observation(
            clean,
            variant,
            2,
            EvalTrialFailed.make({
              errorTag: "AiError",
              message: "Unavailable",
              estimatedCostMicrousd: 1,
              reservedCostMicrousd: 80_000,
            }),
            1,
          ),
        ];

        const judgments = judgmentSet(yield* digestObservationSet(observations), [
          judgment(known, variant, 1, 0, "matches-expected", [defectId]),
        ]);

        const report = yield* makeQualityReport(suite, observations, 2, judgments);
        const result = report.variants[0];

        expect(result?.resources).toMatchObject({
          attemptedTrials: 4,
          succeededTrials: 1,
          incompleteTrials: 2,
          failedTrials: 1,
          costedSucceededTrials: 1,
          costedIncompleteTrials: 1,
          uncostedIncompleteTrials: 1,
          costedFailedTrials: 1,
          estimatedCostMicrousd: 9,
          reservedCostMicrousd: 150_000,
          reservationKnownTrials: 3,
          reservationUnknownTrials: 1,
          inputTokens: 30,
          outputTokens: 9,
          elapsedMillis: 1_302,
          medianElapsedMillis: 150.5,
        });
        expect(result?.blockerRecall).toMatchObject({
          numerator: 1,
          denominator: 1,
          status: "measured",
        });
        expect(result?.firstTrialFindings.valid).toBe(1);
        expect(result?.allTrialFindings.valid).toBe(1);
        expect(result?.blockerCases).toMatchObject({ complete: 0, incomplete: 1, total: 1 });
        expect(result?.cleanControls).toMatchObject({ passed: 0, total: 1 });
        expect(result?.cases.find((entry) => entry.caseId === known.id)?.resources).toMatchObject({
          succeededTrials: 1,
          incompleteTrials: 1,
          failedTrials: 0,
          reservedCostMicrousd: 70_000,
          reservationKnownTrials: 2,
          reservationUnknownTrials: 0,
          elapsedMillis: 1_201,
          medianElapsedMillis: 600.5,
        });
        expect(result?.cases.find((entry) => entry.caseId === clean.id)?.resources).toMatchObject({
          elapsedMillis: 101,
          medianElapsedMillis: 50.5,
        });
        expect(result?.cases.find((entry) => entry.caseId === clean.id)?.cleanControlPassed).toBe(
          false,
        );
        expect(renderQualityReport(report)).toContain("incomplete 2/4; failures 1/4");
        expect(renderQualityReport(report)).toContain(
          "settled estimate 9µUSD (1 succeeded + 1 incomplete + 1 failed; 1 unknown)",
        );
        expect(renderQualityReport(report)).toContain(
          "outstanding reservations 150000µUSD (3 trials known; 1 unknown)",
        );
        expect(renderQualityReport(report)).toContain(
          "local trial elapsed: total 1302ms, median 150.5ms (all returned trials)",
        );
        expect(report.version).toBe(5);
        expect(
          Schema.decodeSync(EvalQualityReport)(Schema.encodeSync(EvalQualityReport)(report)),
        ).toEqual(report);
        const encoded = Schema.encodeSync(EvalQualityReport)(report);

        const historical = yield* Schema.decodeUnknownEffect(EvalQualityReport)({
          ...encoded,
          variants: encoded.variants.map((variant) => {
            const { medianElapsedMillis: _median, ...resources } = variant.resources;

            return { ...variant, resources };
          }),
        });

        expect(renderQualityReport(historical)).toContain(
          "local trial elapsed: total 1302ms, median unknown (all returned trials)",
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("separates blocker detection from merge-gating severity", () =>
    Effect.gen(function* () {
      const suite = yield* loadFixture;
      const known = suite.cases.find((evalCase) => evalCase.kind === "known-defects");

      expect(known).toBeDefined();
      if (known === undefined) return;
      const defectId = known.expectedDefects[0]?.id;

      expect(defectId).toBeDefined();
      if (defectId === undefined) return;

      const variant = configuration("underclassified");

      const observations = [
        observation(
          known,
          variant,
          1,
          succeeded([finding("Detected blocker labeled important", "important")]),
        ),
      ];

      const digest = yield* digestObservationSet(observations);

      const judgments = judgmentSet(digest, [
        judgment(known, variant, 1, 0, "matches-expected", [defectId]),
      ]);

      const report = yield* makeQualityReport(
        EvalSuite.make({ ...suite, cases: [known] }),
        observations,
        1,
        judgments,
      );

      const variantReport = report.variants[0];

      expect(variantReport?.blockerDetection).toMatchObject({
        numerator: 1,
        denominator: 1,
        status: "measured",
      });
      expect(variantReport?.blockerRecall).toMatchObject({
        numerator: 0,
        denominator: 1,
        status: "measured",
      });
      expect(variantReport?.firstTrialBlockingFindings.precision.status).toBe("not-applicable");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects incompatible observation and judgment identities", () =>
    Effect.gen(function* () {
      const suite = yield* loadFixture;
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

      expect((yield* Effect.result(makeQualityReport(suite, incompatibleCase, 1)))._tag).toBe(
        "Failure",
      );

      const incompatibleConfig = [
        knownObservation,
        EvalObservation.make({
          ...cleanObservation,
          variant: configuration("current", "different-model"),
        }),
      ];

      expect((yield* Effect.result(makeQualityReport(suite, incompatibleConfig, 1)))._tag).toBe(
        "Failure",
      );

      const staleJudgments = judgmentSet(
        Schema.decodeSync(EvalObservationSetDigest)("f".repeat(64)),
        [],
      );

      expect(
        (yield* Effect.result(makeQualityReport(suite, observations, 1, staleJudgments)))._tag,
      ).toBe("Failure");

      const report = yield* makeQualityReport(suite, observations, 1, judgmentSet(digest, []));

      expect(report.caseSet).toHaveLength(2);
      expect(report.variants).toHaveLength(1);

      expect((yield* Effect.result(makeQualityReport(suite, observations, 5)))._tag).toBe(
        "Failure",
      );
      expect((yield* Effect.result(makeQualityReport(suite, [knownObservation], 1)))._tag).toBe(
        "Failure",
      );

      const subsetReport = yield* makeQualityReport(
        EvalSuite.make({ ...suite, cases: [known] }),
        [knownObservation],
        1,
      );

      expect(subsetReport.caseSet.map((identity) => identity.id)).toEqual([known.id]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("round-trips judgment and report files without a model call", () =>
    Effect.gen(function* () {
      const suite = yield* loadFixture;
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

      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-review-report-" });
      const observationsPath = `${directory}/observations.jsonl`;
      const candidateObservationsPath = `${directory}/candidate-observations.jsonl`;
      const judgmentsPath = `${directory}/judgments.json`;
      const reportPath = `${directory}/report.json`;

      yield* writeObservations(observationsPath, Stream.fromIterable(observations));

      const candidateObservations = observations.map((entry) =>
        EvalObservation.make({ ...entry, variant: configuration("candidate") }),
      );

      yield* writeObservations(
        candidateObservationsPath,
        Stream.fromIterable(candidateObservations),
      );

      const loadedObservations = yield* loadObservationFiles([
        observationsPath,
        candidateObservationsPath,
      ]);

      expect(loadedObservations).toEqual([...observations, ...candidateObservations]);
      const judgments = judgmentSet(yield* digestObservationSet(loadedObservations), []);

      yield* fs.writeFileString(
        judgmentsPath,
        Schema.encodeSync(Schema.fromJsonString(EvalJudgmentSet))(judgments),
      );
      const loadedJudgments = yield* loadJudgmentSet(judgmentsPath);
      const report = yield* makeQualityReport(suite, loadedObservations, 1, loadedJudgments);

      expect(report.variants).toHaveLength(2);
      yield* writeQualityReport(reportPath, report);

      expect(
        Schema.decodeSync(Schema.fromJsonString(EvalQualityReport))(
          yield* fs.readFileString(reportPath),
        ),
      ).toEqual(report);

      const emptyPath = `${directory}/interrupted-candidate.jsonl`;

      yield* fs.writeFileString(emptyPath, "");
      expect(
        yield* loadObservationFiles([observationsPath, emptyPath]).pipe(Effect.result),
      ).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "EvalDataError", operation: "read observations" },
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Layer, Redacted, Ref, Schema } from "effect";
import { Agent, AgentRuntime, IdGenerator, UsageBudgetLimits } from "effect-agent";
import { Tool } from "effect/unstable/ai";

import {
  CandidateAssessment,
  ChangedFile,
  annotatePatch,
  executeFanOutReview,
  fanOutInputCoverage,
  fanOutReviewBudgetLimits,
  fanOutReviewerProfile,
  FanOutReviewerProfile,
  findingAnchorInUnitEvidence,
  fileReviewEvidenceChunks,
  FileReviewer,
  FileReviewBrief,
  FileReviewEvidence,
  FileReviewReport,
  FileReviewToolkit,
  makeFileReviewerDefinition,
  MAX_FILE_REVIEW_TOOL_CALLS,
  MAX_PATCH_CHARS,
  MAX_REPORTED_UNASSIGNED_EVIDENCE_SHARDS,
  MAX_REVIEW_UNITS,
  MAX_UNIT_FILES,
  planReviewUnits,
  PullRequestMetadata,
  rankAndDedupeFindings,
  ReviewFinding,
  ReviewExecutionContext,
  type ReviewPassMisbehaved,
  type ReviewPublicationPlan,
  ReviewStateAuthenticator,
  type ReviewRiskCategory,
  WalkthroughEntry,
  webCryptoReviewStateAuthenticatorLayer,
  defaultFileReviewerPolicy,
} from "../src/index.ts";
import {
  collectingReviewPublisherLayer,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  makeOfflineFileReviewerModel,
  type OfflineUnitScript,
} from "../src/testing.ts";

const requiresNothing = <A, E>(effect: Effect.Effect<A, E, never>): Effect.Effect<A, E, never> =>
  effect;

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;
type EffectError<T> = T extends Effect.Effect<infer _A, infer E, infer _R> ? E : never;

const FIXTURE_SHA = "fedcba9876543210fedcba9876543210fedcba98";
const MISSION_MARKER = "coordinator-dossier-91x";

const patchFor = (name: string): string =>
  [
    "@@ -1,3 +1,4 @@",
    ` export const ${name}Base = 1;`,
    `-export const ${name}Mode = "old";`,
    `+export const ${name}Mode = "new";`,
    `+export const ${name}Enabled = true;`,
    ` export const ${name}Value = ${name}Base;`,
  ].join("\n");

const fixtureFile = (
  path: string,
  name: string,
  options: { readonly additions?: number; readonly deletions?: number } = {},
): FixtureFile =>
  FixtureFile.make({
    file: ChangedFile.make({
      path,
      status: "modified",
      additions: options.additions ?? 2,
      deletions: options.deletions ?? 1,
      patch: patchFor(name),
    }),
    headContent: [
      `export const ${name}Base = 1;`,
      `export const ${name}Mode = "new";`,
      `export const ${name}Enabled = true;`,
      `export const ${name}Value = ${name}Base;`,
    ].join("\n"),
  });

const highRiskFixture = FixturePullRequest.make({
  metadata: PullRequestMetadata.make({
    repository: "acme/ingress",
    number: 110,
    title: "Authenticate and publish work-order events",
    body: `Regression fixture; ${MISSION_MARKER} stays with the host.`,
    baseRef: "main",
    baseSha: "0123456789abcdef0123456789abcdef01234567",
    headRef: "work-order-ingress",
    headSha: FIXTURE_SHA,
    totalChangedFiles: 2,
  }),
  files: [
    fixtureFile("examples/pr-work-order-ingress/src/authenticate.ts", "authenticate"),
    fixtureFile("examples/pr-work-order-ingress/src/parse-event.ts", "publishWebhook"),
  ],
});

const highRiskFiles = highRiskFixture.files.map((entry) => entry.file);
const highRiskPlan = planReviewUnits(highRiskFiles, { totalChangedFiles: 2 });
const highRiskUnit = highRiskPlan.units[0]!;
const generalPass = highRiskPlan.discoveryPasses.find((pass) => pass.perspective === "general")!;
const specialistPass = highRiskPlan.discoveryPasses.find(
  (pass) => pass.perspective === "risk-specialist",
)!;
const verificationWorkId = `${highRiskUnit.unitId}-verification`;

const supportedFinding = ReviewFinding.make({
  path: "examples/pr-work-order-ingress/src/authenticate.ts",
  startLine: 2,
  endLine: 2,
  severity: "important",
  category: "security",
  title: "Authentication mode accepts an unverified caller",
  body: "The new mode reaches the authenticated path without validating the caller identity.",
});

const unsupportedFinding = ReviewFinding.make({
  path: "examples/pr-work-order-ingress/src/authenticate.ts",
  startLine: 3,
  endLine: 3,
  severity: "important",
  category: "correctness",
  title: "The feature flag always disables authentication",
  body: "The candidate claims the true literal disables the path, but the bounded evidence does not support that behavior.",
});

/** The host assigns candidate IDs by kept-finding position, 1-based. */
const candidateId = (passId: string, index: number): string =>
  `${passId}:finding:${String(index).padStart(3, "0")}`;

const discoveryReport = (
  pass: typeof generalPass,
  findings: ReadonlyArray<ReviewFinding>,
): FileReviewReport =>
  FileReviewReport.make({
    phase: "discovery",
    workId: pass.passId,
    unitId: pass.unitId,
    findings,
    concerns: [],
    fileSummaries:
      pass.perspective === "general"
        ? pass.paths.map((path) =>
            WalkthroughEntry.make({ path, summary: `Reviews the bounded change in ${path}.` }),
          )
        : [],
    assessments: [],
  });

const verificationReport = (assessments: ReadonlyArray<CandidateAssessment>): FileReviewReport =>
  FileReviewReport.make({
    phase: "verification",
    workId: verificationWorkId,
    unitId: highRiskUnit.unitId,
    findings: [],
    concerns: [],
    fileSummaries: [],
    assessments,
  });

const report = (workId: string, value: FileReviewReport): OfflineUnitScript => ({
  workId,
  outcomes: [{ _tag: "report", report: value }],
});

/** Run the host-scheduled pipeline offline over the fixture pull request. */
const runOfflineFanOut = (script: {
  readonly children: ReadonlyArray<OfflineUnitScript>;
  readonly fixture?: FixturePullRequest | undefined;
  readonly maxFindings?: number | undefined;
  readonly limits?: UsageBudgetLimits | undefined;
  readonly retry?:
    | {
        readonly paths: ReadonlyArray<string>;
        readonly stages: ReadonlyArray<"discovery" | "specialist" | "verification">;
      }
    | undefined;
}) =>
  Effect.gen(function* () {
    const fixture = script.fixture ?? highRiskFixture;
    const reviewFiles = fixture.files.map((entry) => entry.file);
    const children = yield* makeOfflineFileReviewerModel(script.children);
    const childBinding = Agent.withModel(FileReviewer, children.model);
    const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
    const stateAuthenticator = yield* Effect.gen(function* () {
      return yield* ReviewStateAuthenticator;
    }).pipe(
      Effect.provide(
        webCryptoReviewStateAuthenticatorLayer(Redacted.make("offline-assurance-secret")),
      ),
    );
    const program = executeFanOutReview(childBinding, {
      post: true,
      applyVerdict: false,
      limits: script.limits ?? fanOutReviewBudgetLimits,
      ...(script.maxFindings === undefined ? {} : { maxFindings: script.maxFindings }),
      signature: () => "offline-assurance-profile-v3",
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          fixturePullRequestSourceLayer(fixture),
          collectingReviewPublisherLayer(published),
          IdGenerator.layer,
          NodeCrypto.layer,
        ),
      ),
      Effect.provideService(ReviewExecutionContext, {
        mode: script.retry === undefined ? "full" : "incremental",
        reason:
          script.retry === undefined
            ? "offline full-diff assurance fixture"
            : "offline leftover-pass retry fixture",
        files: reviewFiles,
        affectedPaths: script.retry === undefined ? reviewFiles.map((file) => file.path) : [],
        retryPaths: script.retry?.paths ?? [],
        retryStages: script.retry?.stages ?? [],
        totalFiles: reviewFiles.length,
        baselineSha: undefined,
        priorState: undefined,
        profileFingerprint: "a".repeat(64),
        stateAuthenticator,
      }),
      Effect.scoped,
    );
    const outcome = yield* requiresNothing(program);
    return {
      outcome,
      published: yield* Ref.get(published),
      childCalls: yield* children.calls,
      childPrompts: yield* children.prompts,
    };
  });

describe("fan-out review contract", () => {
  const discoveryBrief = FileReviewBrief.make({
    phase: "discovery",
    workId: generalPass.passId,
    unitId: generalPass.unitId,
    paths: generalPass.paths,
    evidenceShardIds: generalPass.evidenceShardIds,
    perspective: generalPass.perspective,
    riskCategories: generalPass.riskCategories,
    candidates: [],
    evidence: [
      FileReviewEvidence.make({
        shardId: "shard-0001",
        path: generalPass.paths[0]!,
        status: "modified",
        reviewMode: "diff",
        ordinal: 1,
        total: 1,
        annotatedPatch: "@@ -1 +1 @@\nR1 + export const value = 1;",
      }),
    ],
  });

  it("spells the finding shape and committable-suggestion rule out to discovery workers", () => {
    const instructions = FileReviewer.instructions(discoveryBrief);
    expect(instructions).toContain(
      '"suggestion": <string, OPTIONAL: replacement source code for exactly lines startLine..endLine, ready to commit>',
    );
    expect(instructions).toContain(
      "full replacement source for every line in the range and nothing else",
    );
    // The rendered final-output contract carries the same rule with the schema,
    // so every consumer of ReviewFinding shows it to the model.
    const outputSchema = JSON.stringify(Tool.getJsonSchemaFromSchema(FileReviewReport));
    expect(outputSchema).toContain("never prose describing the change");
  });

  it("requires verifiers to settle every carried suggestion exactly", () => {
    const instructions = FileReviewer.instructions(
      FileReviewBrief.make({
        ...discoveryBrief,
        phase: "verification",
        workId: verificationWorkId,
        perspective: "candidate-verification",
        candidates: [
          {
            _tag: "FindingCandidate",
            candidateId: candidateId(specialistPass.passId, 1),
            workId: specialistPass.passId,
            unitId: specialistPass.unitId,
            finding: supportedFinding,
            evidencePaths: [supportedFinding.path],
          },
        ],
      }),
    );
    expect(instructions).toContain('"suggestion": <"committable" | "not-committable"');
    expect(instructions).toContain(
      "full replacement source for exactly lines startLine..endLine and nothing else",
    );
    const outputSchema = JSON.stringify(Tool.getJsonSchemaFromSchema(FileReviewReport));
    expect(outputSchema).toContain(
      "Required exactly when the candidate finding carries a suggestion",
    );
  });

  it("configures evidence-only children with the packaged bounded policy", () => {
    expect(Object.keys(FileReviewToolkit.tools)).toEqual([]);
    expect(MAX_FILE_REVIEW_TOOL_CALLS).toBe(1);
    expect(defaultFileReviewerPolicy.maxToolCalls).toBe(1);
    expect(defaultFileReviewerPolicy.onExhaustion).toBe("fail");
    expect(Duration.toMillis(defaultFileReviewerPolicy.maxDuration)).toBe(6 * 60_000);
  });

  it("pins the exact capability and non-guarantee profile", () => {
    const decoded = Schema.decodeUnknownSync(FanOutReviewerProfile)(
      Schema.encodeSync(FanOutReviewerProfile)(fanOutReviewerProfile),
    );
    expect(decoded).toEqual(fanOutReviewerProfile);
    expect(fanOutReviewerProfile).toMatchObject({
      hostScheduledPasses: true,
      failedPassesRetriedOnceThenCarried: true,
      hostOwnedRiskClassification: true,
      redundantHighRiskDiscovery: true,
      independentCandidateVerification: true,
      defectAbsenceProven: false,
      exactlyOnceExternalEffects: false,
    });
  });

  it.effect("selects scripted child outcomes by an exact work-ID marker", () =>
    Effect.gen(function* () {
      const prefixReport = FileReviewReport.make({
        phase: "discovery",
        workId: "unit-001",
        unitId: "unit-001",
        findings: [],
        concerns: [],
        fileSummaries: [],
        assessments: [],
      });
      const exactReport = FileReviewReport.make({
        ...prefixReport,
        workId: "unit-001-general",
      });
      const scripted = yield* makeOfflineFileReviewerModel([
        report(prefixReport.workId, prefixReport),
        report(exactReport.workId, exactReport),
      ]);
      const binding = Agent.withModel(FileReviewer, scripted.model);
      const output = yield* AgentRuntime.run(
        binding,
        FileReviewBrief.make({
          phase: "discovery",
          workId: exactReport.workId,
          unitId: exactReport.unitId,
          paths: ["src/example.ts"],
          evidenceShardIds: ["shard-0001"],
          perspective: "general",
          riskCategories: [],
          candidates: [],
          evidence: [
            FileReviewEvidence.make({
              shardId: "shard-0001",
              path: "src/example.ts",
              status: "modified",
              reviewMode: "diff",
              ordinal: 1,
              total: 1,
              annotatedPatch: "@@ -1 +1 @@\nR1 + export const value = 1;",
            }),
          ],
        }),
      ).pipe(Effect.provide(IdGenerator.layer), Effect.scoped);

      expect(output.output.workId).toBe(exactReport.workId);
    }),
  );

  it("carries shared guidance to every worker", () => {
    const child = makeFileReviewerDefinition({ guidance: "Architecture first." });
    expect(child.instructions(discoveryBrief)).toContain("Architecture first.");
  });
});

describe("deterministic input and risk planning", () => {
  it("assigns all 41 PR #110-style paths despite the old 24-call flat bound", () => {
    const files = Array.from({ length: 41 }, (_, index) =>
      ChangedFile.make({
        path: `src/routine/file-${String(index + 1).padStart(2, "0")}.ts`,
        status: "modified",
        additions: 2,
        deletions: 1,
        patch: patchFor(`routine${index + 1}`),
      }),
    );
    const plan = planReviewUnits(files, { totalChangedFiles: files.length });
    const assigned = plan.units.flatMap((unit) => unit.paths);

    expect(assigned).toHaveLength(41);
    expect([...assigned].sort()).toEqual(files.map((file) => file.path).sort());
    expect(plan.undiffablePaths).toEqual([]);
    expect(plan.unassignedPaths).toEqual([]);
    expect(plan.units.length).toBeLessThanOrEqual(MAX_REVIEW_UNITS);
    expect(plan.units.every((unit) => unit.paths.length <= MAX_UNIT_FILES)).toBe(true);
  });

  it("classifies high-risk scope in host code and assigns fresh specialist discovery", () => {
    const risks = new Set<ReviewRiskCategory>(highRiskUnit.riskCategories);
    expect(risks.has("authentication-authorization")).toBe(true);
    expect(risks.has("external-side-effects")).toBe(true);
    expect(highRiskPlan.discoveryPasses.map((pass) => pass.perspective)).toEqual([
      "general",
      "risk-specialist",
    ]);
    expect(specialistPass.riskCategories).toEqual(highRiskUnit.riskCategories);
  });

  it("covers every required high-risk category with deterministic host rules", () => {
    const file = ChangedFile.make({
      path: "src/auth/security-boundary.ts",
      status: "modified",
      additions: 1,
      deletions: 1,
      patch:
        "@@ -1 +1 @@\n-const previous = true;\n+authorizeBearerCredentialInsideSandboxTransactionWithSemaphoreThenPublish();",
    });
    const plan = planReviewUnits([file], { totalChangedFiles: 1 });
    expect(plan.units[0]?.riskCategories).toEqual([
      "authentication-authorization",
      "security-boundary",
      "persistence-durability",
      "concurrency",
      "credential-handling",
      "external-side-effects",
    ]);
    expect(plan.discoveryPasses.map((pass) => pass.perspective)).toEqual([
      "general",
      "risk-specialist",
    ]);
  });

  it("assigns independent specialist discovery when keyword classification is silent", () => {
    const file = ChangedFile.make({
      path: "src/domain/policy.ts",
      status: "modified",
      additions: 1,
      deletions: 1,
      patch: "@@ -1 +1 @@\n-export const mode = 1;\n+export const mode = 2;",
    });
    const plan = planReviewUnits([file], { totalChangedFiles: 1 });
    expect(plan.units[0]?.riskCategories).toEqual([]);
    expect(plan.discoveryPasses.map((pass) => pass.perspective)).toEqual([
      "general",
      "risk-specialist",
    ]);
    expect(plan.discoveryPasses[1]?.riskCategories).toEqual([]);
  });

  it("keeps undiffable and over-capacity paths as fail-closed coverage gaps", () => {
    const binary = ChangedFile.make({
      path: "assets/logo.png",
      status: "added",
      additions: 0,
      deletions: 0,
    });
    const many = Array.from({ length: 120 }, (_, index) =>
      ChangedFile.make({
        path: `src/wide/file-${String(index + 1).padStart(3, "0")}.ts`,
        status: "modified",
        additions: 1,
        deletions: 0,
        patch: patchFor("wide"),
      }),
    );
    const files = [...many, binary];
    const plan = planReviewUnits(files, { totalChangedFiles: 121 });
    const assigned = plan.units.flatMap((unit) => unit.paths);

    expect(plan.undiffablePaths).toEqual(["assets/logo.png"]);
    expect([...assigned, ...plan.unassignedPaths].sort()).toEqual(
      many.map((file) => file.path).sort(),
    );

    const inputCoverage = fanOutInputCoverage({
      plan,
      files,
      totalFiles: files.length,
      anchorFiles: files,
      totalAnchorFiles: files.length,
    });
    // Fail-closed: an unreviewable binary keeps input coverage incomplete for
    // as long as it is part of the pull request; ignore globs are the
    // deliberate way to exclude it. The capacity overflow is a gap too.
    expect(inputCoverage.status).toBe("incomplete");
    expect(inputCoverage.undiffablePaths).toEqual(["assets/logo.png"]);
    expect(inputCoverage.unassignedPaths).toEqual([...plan.unassignedPaths].sort());
    expect(inputCoverage.reasons.join("\n")).toContain("assets/logo.png");
  });

  it("bounds overflow identifiers while preserving the exact shard count and every path", () => {
    const files = Array.from({ length: 100 }, (_, index) =>
      ChangedFile.make({
        path: `src/overflow/file-${String(index + 1).padStart(3, "0")}.ts`,
        status: "added",
        additions: 1,
        deletions: 0,
        patch: `@@ -0,0 +1 @@\n+${"x".repeat(61_000)}`,
      }),
    );
    const plan = planReviewUnits(files, { totalChangedFiles: files.length });
    const assignedShardCount = plan.units.reduce(
      (total, unit) => total + unit.evidenceShards.length,
      0,
    );
    const generatedShardCount = files.reduce(
      (total, file) => total + fileReviewEvidenceChunks(file).length,
      0,
    );

    expect(plan.unassignedEvidenceShardCount).toBe(generatedShardCount - assignedShardCount);
    expect(plan.unassignedEvidenceShardCount).toBeGreaterThan(
      MAX_REPORTED_UNASSIGNED_EVIDENCE_SHARDS,
    );
    expect(plan.unassignedEvidenceShardIds).toHaveLength(MAX_REPORTED_UNASSIGNED_EVIDENCE_SHARDS);
    expect(
      [...new Set([...plan.units.flatMap((unit) => unit.paths), ...plan.unassignedPaths])].sort(),
    ).toEqual(files.map((file) => file.path).sort());

    const inputCoverage = fanOutInputCoverage({
      plan,
      files,
      totalFiles: files.length,
      anchorFiles: files,
      totalAnchorFiles: files.length,
    });
    expect(inputCoverage.status).toBe("incomplete");
    expect(inputCoverage.unassignedPaths).toEqual([...plan.unassignedPaths].sort());
    expect(inputCoverage.reasons.join("\n")).toContain(
      `${plan.unassignedEvidenceShardCount} deterministic evidence shard(s)`,
    );
  });

  it("splits an oversized path into complete bounded evidence shards", () => {
    const oversized = ChangedFile.make({
      path: "src/oversized.ts",
      status: "modified",
      additions: 1,
      deletions: 0,
      patch: `@@ -1 +1 @@\n+${"x".repeat(61_000)}`,
    });
    const plan = planReviewUnits([oversized], { totalChangedFiles: 1 });
    const chunks = fileReviewEvidenceChunks(oversized);
    const shards = plan.units.flatMap((unit) => unit.evidenceShards);
    expect(plan.units.flatMap((unit) => unit.paths)).toEqual([oversized.path]);
    expect(chunks.length).toBeGreaterThan(1);
    expect(chunks.every((chunk) => chunk.annotatedPatch.length <= MAX_PATCH_CHARS)).toBe(true);
    expect(chunks.map((chunk) => chunk.annotatedPatch).join("")).toBe(
      annotatePatch(oversized.patch ?? ""),
    );
    expect(shards.map((shard) => shard.ordinal)).toEqual(
      Array.from({ length: chunks.length }, (_, index) => index + 1),
    );
    expect(shards.every((shard) => shard.total === chunks.length)).toBe(true);
    expect(plan.partialEvidencePaths).toEqual([]);
    expect(plan.unassignedEvidenceShardIds).toEqual([]);

    const inputCoverage = fanOutInputCoverage({
      plan,
      files: [oversized],
      totalFiles: 1,
      anchorFiles: [oversized],
      totalAnchorFiles: 1,
    });
    expect(inputCoverage.status).toBe("complete");
    expect(inputCoverage.assignedPaths).toEqual([oversized.path]);
    expect(inputCoverage.partialPaths).toEqual([]);
    expect(inputCoverage.reasons).toEqual([]);
  });

  it("binds finding anchors to the exact shards assigned to their unit", () => {
    const lines = Array.from(
      { length: 130 },
      (_, index) => `+export const value${index + 1} = "${"x".repeat(5_500)}";`,
    );
    const oversized = ChangedFile.make({
      path: "src/multi-unit.ts",
      status: "added",
      additions: lines.length,
      deletions: 0,
      patch: [`@@ -0,0 +1,${lines.length} @@`, ...lines].join("\n"),
    });
    const plan = planReviewUnits([oversized], { totalChangedFiles: 1 });
    const first = plan.units[0];
    const last = plan.units.at(-1);
    expect(plan.units.length).toBeGreaterThan(1);
    expect(first).toBeDefined();
    expect(last).toBeDefined();
    if (first === undefined || last === undefined) return;

    const findingAt = (line: number) =>
      ReviewFinding.make({
        path: oversized.path,
        startLine: line,
        endLine: line,
        severity: "important",
        title: `Defect at line ${line}`,
        body: "The assigned evidence demonstrates this defect.",
      });

    expect(findingAnchorInUnitEvidence(findingAt(1), first, [oversized])).toBe(true);
    expect(findingAnchorInUnitEvidence(findingAt(130), first, [oversized])).toBe(false);
    expect(findingAnchorInUnitEvidence(findingAt(1), last, [oversized])).toBe(false);
    expect(findingAnchorInUnitEvidence(findingAt(130), last, [oversized])).toBe(true);
  });

  it("is deterministic regardless of source order", () => {
    expect(planReviewUnits([...highRiskFiles].reverse(), { totalChangedFiles: 2 })).toEqual(
      highRiskPlan,
    );
  });
});

describe("host-scheduled discovery and verification pipeline", () => {
  const successfulDiscovery: ReadonlyArray<OfflineUnitScript> = [
    report(generalPass.passId, discoveryReport(generalPass, [])),
    report(
      specialistPass.passId,
      discoveryReport(specialistPass, [supportedFinding, unsupportedFinding]),
    ),
  ];
  const settledVerification = report(
    verificationWorkId,
    verificationReport([
      CandidateAssessment.make({
        candidateId: candidateId(specialistPass.passId, 1),
        disposition: "confirmed",
        rationale: "The changed authentication mode is reachable without an identity check.",
      }),
      CandidateAssessment.make({
        candidateId: candidateId(specialistPass.passId, 2),
        disposition: "rejected",
        rationale: "The evidence contains no disabling branch and does not establish the claim.",
      }),
    ]),
  );

  it.effect(
    "finds a defect missed by general discovery and publishes only verifier-confirmed candidates",
    () =>
      Effect.gen(function* () {
        const result = yield* runOfflineFanOut({
          children: [...successfulDiscovery, settledVerification],
        });

        expect(result.childCalls).toBe(3);
        expect(result.outcome.inputCoverage.status).toBe("complete");
        expect(result.outcome.assurance).toMatchObject({
          status: "settled",
          requiredGeneralDiscoveryPasses: 1,
          completedGeneralDiscoveryPasses: 1,
          requiredSpecialistPasses: 1,
          completedSpecialistPasses: 1,
          requiredVerificationPasses: 1,
          completedVerificationPasses: 1,
          discoveredCandidates: 2,
          confirmedCandidates: 1,
          rejectedCandidates: 1,
          unsettledCandidates: 0,
          discardedInvalidFindings: 0,
        });
        expect(result.outcome.review.findings).toEqual([supportedFinding]);
        expect(result.outcome.review.findings).not.toContainEqual(unsupportedFinding);
        expect(result.outcome.review.verdict).toBe("comment");
        expect(result.outcome.unreviewedPaths).toEqual([]);
        expect(result.outcome.turns).toBeGreaterThan(0);
        expect(result.outcome.plan.comments.map((comment) => comment.path)).toEqual([
          supportedFinding.path,
        ]);
        expect(result.outcome.plan.body).toContain("**Review assurance:** settled");
        expect(result.outcome.state?.reviewedHeadSha).toBe(FIXTURE_SHA);
        expect(result.outcome.state?.settled).toBe(true);
        expect(result.outcome.state?.unreviewedPaths).toEqual([]);
        expect(result.outcome.plan.body).toContain("effect-agent-pr-review state-v2:");
        expect(result.published).toHaveLength(1);

        const generalPrompt = result.childPrompts.find((prompt) =>
          prompt.includes(generalPass.passId),
        );
        const specialistPrompt = result.childPrompts.find(
          (prompt) =>
            prompt.includes(specialistPass.passId) &&
            !prompt.includes(candidateId(specialistPass.passId, 1)),
        );
        const verifierPrompt = result.childPrompts.find((prompt) =>
          prompt.includes(verificationWorkId),
        );
        expect(generalPrompt).toBeDefined();
        expect(specialistPrompt).toBeDefined();
        expect(verifierPrompt).toContain(candidateId(specialistPass.passId, 1));
        expect(verifierPrompt).toContain("authenticateMode");
        expect(verifierPrompt).toContain("publishWebhookMode");
        // Children receive host-planned evidence only, never the PR mission.
        for (const prompt of result.childPrompts) expect(prompt).not.toContain(MISSION_MARKER);
      }),
  );

  it.effect("verifies an equivalent cross-pass candidate once and publishes it once", () =>
    Effect.gen(function* () {
      const result = yield* runOfflineFanOut({
        children: [
          report(generalPass.passId, discoveryReport(generalPass, [supportedFinding])),
          report(specialistPass.passId, discoveryReport(specialistPass, [supportedFinding])),
          report(
            verificationWorkId,
            verificationReport([
              CandidateAssessment.make({
                candidateId: candidateId(generalPass.passId, 1),
                disposition: "confirmed",
                rationale: "The bounded evidence confirms the authentication defect once.",
              }),
            ]),
          ),
        ],
      });

      expect(result.outcome.assurance).toMatchObject({
        status: "settled",
        discoveredCandidates: 1,
        confirmedCandidates: 1,
        rejectedCandidates: 0,
      });
      expect(result.outcome.review.findings).toEqual([supportedFinding]);
      expect(result.outcome.plan.comments).toHaveLength(1);
    }),
  );

  it.effect("settles an empty host plan without spawning a review child", () =>
    Effect.gen(function* () {
      const emptyFixture = FixturePullRequest.make({
        metadata: PullRequestMetadata.make({
          repository: "acme/empty",
          number: 1,
          title: "No reviewable changes",
          body: "",
          baseRef: "main",
          baseSha: "0123456789abcdef0123456789abcdef01234567",
          headRef: "empty",
          headSha: FIXTURE_SHA,
          totalChangedFiles: 0,
        }),
        files: [],
      });
      const result = yield* runOfflineFanOut({ fixture: emptyFixture, children: [] });

      expect(result.childCalls).toBe(0);
      expect(result.outcome.inputCoverage.status).toBe("complete");
      expect(result.outcome.assurance).toMatchObject({
        status: "settled",
        requiredGeneralDiscoveryPasses: 0,
        requiredSpecialistPasses: 0,
        requiredVerificationPasses: 0,
      });
      expect(result.outcome.review.verdict).toBe("approve");
      expect(result.outcome.turns).toBe(0);
      expect(result.outcome.state?.settled).toBe(true);
    }),
  );

  // The incident regression (issue #131): one invalid anchor used to reject
  // the whole pass as FileReviewWorkRejected, which froze the continuity
  // baseline and reopened the entire post-baseline scope on every push.
  it.effect("discards an invalid-anchor finding without failing its pass or the baseline", () =>
    Effect.gen(function* () {
      const invalidAnchor = ReviewFinding.make({
        ...supportedFinding,
        startLine: 99,
        endLine: 99,
        title: "Invalid new-version anchor",
      });
      const result = yield* runOfflineFanOut({
        children: [
          report(generalPass.passId, discoveryReport(generalPass, [])),
          report(
            specialistPass.passId,
            discoveryReport(specialistPass, [invalidAnchor, supportedFinding]),
          ),
          report(
            verificationWorkId,
            verificationReport([
              CandidateAssessment.make({
                // The invalid anchor was discarded, so the kept finding is 001.
                candidateId: candidateId(specialistPass.passId, 1),
                disposition: "confirmed",
                rationale: "The changed mode reaches the authenticated path unchecked.",
              }),
            ]),
          ),
        ],
      });

      expect(result.outcome.assurance).toMatchObject({
        status: "settled",
        discardedInvalidFindings: 1,
        discoveredCandidates: 1,
        confirmedCandidates: 1,
      });
      expect(result.outcome.review.findings).toEqual([supportedFinding]);
      expect(result.outcome.unreviewedPaths).toEqual([]);
      expect(result.outcome.state?.settled).toBe(true);
    }),
  );

  it.effect("retries a misdirected child report once and settles on the retry", () =>
    Effect.gen(function* () {
      const mismatched = FileReviewReport.make({
        ...discoveryReport(specialistPass, [supportedFinding]),
        workId: generalPass.passId,
      });
      const result = yield* runOfflineFanOut({
        children: [
          report(generalPass.passId, discoveryReport(generalPass, [])),
          {
            workId: specialistPass.passId,
            outcomes: [
              { _tag: "report", report: mismatched },
              { _tag: "report", report: discoveryReport(specialistPass, [supportedFinding]) },
            ],
          },
          report(
            verificationWorkId,
            verificationReport([
              CandidateAssessment.make({
                candidateId: candidateId(specialistPass.passId, 1),
                disposition: "confirmed",
                rationale: "Confirmed on the retried, correctly addressed pass.",
              }),
            ]),
          ),
        ],
      });

      expect(result.childCalls).toBe(4);
      expect(result.outcome.assurance.status).toBe("settled");
      expect(result.outcome.assurance.failedPasses).toEqual([]);
      expect(result.outcome.review.findings).toEqual([supportedFinding]);
      expect(result.outcome.state?.settled).toBe(true);
    }),
  );

  // The second incident regression: a pass that stays failed no longer
  // freezes continuity — the baseline advances and the unit's paths are
  // carried as retryable scope for the next run.
  it.effect("carries a persistently failed pass forward instead of freezing the baseline", () =>
    Effect.gen(function* () {
      const result = yield* runOfflineFanOut({
        children: [
          report(generalPass.passId, discoveryReport(generalPass, [])),
          { workId: specialistPass.passId, outcomes: [{ _tag: "malformed-output" }] },
        ],
      });

      expect(result.outcome.inputCoverage.status).toBe("complete");
      expect(result.outcome.assurance.status).toBe("incomplete");
      expect(result.outcome.assurance.failedPasses).toContainEqual(
        expect.objectContaining({
          workId: specialistPass.passId,
          stage: "specialist",
          errorTag: expect.stringContaining("AgentOutputError"),
        }),
      );
      expect(result.outcome.coverage.status).toBe("incomplete");
      expect(result.outcome.unreviewedPaths).toEqual([...highRiskUnit.paths].sort());
      // Continuity ADVANCES with the gap carried explicitly.
      expect(result.outcome.state?.reviewedHeadSha).toBe(FIXTURE_SHA);
      expect(result.outcome.state?.settled).toBe(false);
      expect(result.outcome.state?.unreviewedPaths).toEqual([...highRiskUnit.paths].sort());
      expect(result.outcome.state?.unreviewedPasses).toMatchObject([
        {
          stage: "specialist",
          paths: highRiskUnit.paths,
        },
      ]);
      expect(result.outcome.plan.body).toContain("Unsettled review passes");
      expect(result.outcome.plan.body).toContain("do not change code to satisfy this section");
      expect(result.outcome.plan.event).toBe("COMMENT");
    }),
  );

  it.effect("retries only the failed leftover pass without a second general discovery", () =>
    Effect.gen(function* () {
      const result = yield* runOfflineFanOut({
        retry: { paths: [...highRiskUnit.paths], stages: ["specialist"] },
        children: [
          report(specialistPass.passId, discoveryReport(specialistPass, [supportedFinding])),
          report(
            verificationWorkId,
            verificationReport([
              CandidateAssessment.make({
                candidateId: candidateId(specialistPass.passId, 1),
                disposition: "confirmed",
                rationale: "Confirmed on the leftover specialist retry.",
              }),
            ]),
          ),
        ],
      });

      expect(result.childCalls).toBe(2);
      expect(result.outcome.assurance).toMatchObject({
        status: "settled",
        requiredGeneralDiscoveryPasses: 0,
        completedGeneralDiscoveryPasses: 0,
        requiredSpecialistPasses: 1,
        completedSpecialistPasses: 1,
      });
      expect(result.outcome.review.findings).toEqual([supportedFinding]);
      expect(result.outcome.unreviewedPaths).toEqual([]);
      expect(result.outcome.state?.unreviewedPasses).toEqual([]);
    }),
  );

  it.effect("never retries budget exhaustion and still advances continuity", () =>
    Effect.gen(function* () {
      const result = yield* runOfflineFanOut({
        children: [
          report(generalPass.passId, discoveryReport(generalPass, [])),
          report(specialistPass.passId, discoveryReport(specialistPass, [])),
        ],
        // The first child's turn-seam consumption trips the shared budget, so
        // every scheduled pass fails with BudgetExceeded. A retry would fail
        // identically, so the pipeline must not double the child calls.
        limits: UsageBudgetLimits.make({ maxInputTokens: 1 }),
      });

      // At least one child ran into the budget; no pass spent the retry.
      expect(result.childCalls).toBeGreaterThanOrEqual(1);
      expect(result.childCalls).toBeLessThanOrEqual(2);
      expect(result.outcome.assurance.status).toBe("incomplete");
      expect(result.outcome.assurance.failedPasses.map((pass) => pass.workId).sort()).toEqual(
        [generalPass.passId, specialistPass.passId].sort(),
      );
      for (const pass of result.outcome.assurance.failedPasses) {
        expect(pass.errorTag).toBe("BudgetExceeded");
      }
      // Continuity still advances; the whole unit is carried for retry.
      expect(result.outcome.state?.reviewedHeadSha).toBe(FIXTURE_SHA);
      expect(result.outcome.state?.settled).toBe(false);
      expect(result.outcome.state?.unreviewedPaths).toEqual([...highRiskUnit.paths].sort());
    }),
  );

  it.effect("leaves every candidate unsettled when independent verification fails", () =>
    Effect.gen(function* () {
      const result = yield* runOfflineFanOut({
        children: [
          ...successfulDiscovery,
          { workId: verificationWorkId, outcomes: [{ _tag: "malformed-output" }] },
        ],
      });

      expect(result.outcome.assurance).toMatchObject({
        status: "incomplete",
        requiredVerificationPasses: 1,
        completedVerificationPasses: 0,
        discoveredCandidates: 2,
        confirmedCandidates: 0,
        unsettledCandidates: 2,
      });
      expect(result.outcome.assurance.failedPasses).toContainEqual(
        expect.objectContaining({
          workId: verificationWorkId,
          stage: "verification",
          errorTag: expect.stringContaining("AgentOutputError"),
        }),
      );
      expect(result.outcome.review.findings).toEqual([]);
      expect(result.outcome.plan.comments).toEqual([]);
      expect(result.outcome.plan.event).toBe("COMMENT");
      expect(result.outcome.state?.settled).toBe(false);
      expect(result.outcome.state?.unreviewedPaths).toEqual([...highRiskUnit.paths].sort());
    }),
  );

  const committableSuggestionFinding = ReviewFinding.make({
    ...supportedFinding,
    suggestion: 'export const authenticateMode = "verified";',
  });
  const proseSuggestionFinding = ReviewFinding.make({
    path: supportedFinding.path,
    startLine: 3,
    endLine: 3,
    severity: "important",
    category: "correctness",
    title: "The enable flag ships without a rollout guard",
    body: "The new flag enables the mode unconditionally in every environment.",
    suggestion: "Move the flag behind the environment rollout guard before enabling it.",
  });
  const suggestionDiscovery: ReadonlyArray<OfflineUnitScript> = [
    report(generalPass.passId, discoveryReport(generalPass, [])),
    report(
      specialistPass.passId,
      discoveryReport(specialistPass, [committableSuggestionFinding, proseSuggestionFinding]),
    ),
  ];

  it.effect("publishes a suggestion only on an exact committable settlement", () =>
    Effect.gen(function* () {
      const result = yield* runOfflineFanOut({
        children: [
          ...suggestionDiscovery,
          report(
            verificationWorkId,
            verificationReport([
              CandidateAssessment.make({
                candidateId: candidateId(specialistPass.passId, 1),
                disposition: "confirmed",
                suggestion: "committable",
                rationale: "The replacement is the exact committable source for line 2.",
              }),
              CandidateAssessment.make({
                candidateId: candidateId(specialistPass.passId, 2),
                disposition: "confirmed",
                suggestion: "not-committable",
                rationale: "The suggestion text is advice prose, not replacement source.",
              }),
            ]),
          ),
        ],
      });

      const { suggestion: _prose, ...strippedProse } = proseSuggestionFinding;
      expect(result.outcome.assurance).toMatchObject({
        status: "settled",
        confirmedCandidates: 2,
        rejectedCandidates: 0,
        unsettledCandidates: 0,
      });
      expect(result.outcome.review.findings).toEqual([
        committableSuggestionFinding,
        ReviewFinding.make(strippedProse),
      ]);
      const committableComment = result.outcome.plan.comments.find((comment) =>
        comment.body.includes(committableSuggestionFinding.title),
      );
      const strippedComment = result.outcome.plan.comments.find((comment) =>
        comment.body.includes(proseSuggestionFinding.title),
      );
      expect(committableComment?.body).toContain("```suggestion");
      expect(strippedComment?.body).toBeDefined();
      expect(strippedComment?.body).not.toContain("```suggestion");
    }),
  );

  it.effect("treats an unsettled carried suggestion as a failed pass after its retry", () =>
    Effect.gen(function* () {
      const inexact = verificationReport([
        CandidateAssessment.make({
          candidateId: candidateId(specialistPass.passId, 1),
          disposition: "confirmed",
          rationale: "Confirmed without settling the carried suggestion.",
        }),
        CandidateAssessment.make({
          candidateId: candidateId(specialistPass.passId, 2),
          disposition: "confirmed",
          suggestion: "not-committable",
          rationale: "The suggestion text is advice prose, not replacement source.",
        }),
      ]);
      const result = yield* runOfflineFanOut({
        children: [
          ...suggestionDiscovery,
          { workId: verificationWorkId, outcomes: [{ _tag: "report", report: inexact }] },
        ],
      });

      expect(result.outcome.assurance.status).toBe("incomplete");
      expect(result.outcome.assurance.failedPasses).toContainEqual(
        expect.objectContaining({
          workId: verificationWorkId,
          stage: "verification",
          errorTag: "ReviewPassMisbehaved",
        }),
      );
      expect(result.outcome.review.findings).toEqual([]);
      expect(result.outcome.plan.comments).toEqual([]);
      expect(result.outcome.state?.settled).toBe(false);
      expect(result.outcome.state?.unreviewedPaths).toEqual([...highRiskUnit.paths].sort());
    }),
  );

  it.effect("keeps an undiffable path fail-closed: incomplete, carried, never skippable", () =>
    Effect.gen(function* () {
      const binaryFixture = FixturePullRequest.make({
        metadata: PullRequestMetadata.make({
          ...highRiskFixture.metadata,
          totalChangedFiles: 3,
        }),
        files: [
          ...highRiskFixture.files,
          FixtureFile.make({
            file: ChangedFile.make({
              path: "assets/logo.png",
              status: "added",
              additions: 0,
              deletions: 0,
            }),
          }),
        ],
      });
      const result = yield* runOfflineFanOut({
        fixture: binaryFixture,
        children: [
          report(generalPass.passId, discoveryReport(generalPass, [])),
          report(specialistPass.passId, discoveryReport(specialistPass, [])),
        ],
      });

      // An unreviewable change must never authorize a green check, and the
      // carry keeps it in scope even after it leaves the incremental delta.
      expect(result.outcome.inputCoverage.status).toBe("incomplete");
      expect(result.outcome.inputCoverage.undiffablePaths).toEqual(["assets/logo.png"]);
      expect(result.outcome.inputCoverage.reasons.join("\n")).toContain("assets/logo.png");
      expect(result.outcome.assurance.status).toBe("settled");
      expect(result.outcome.unreviewedPaths).toContain("assets/logo.png");
      expect(result.outcome.state?.settled).toBe(false);
      expect(result.outcome.state?.unreviewedPaths).toContain("assets/logo.png");
      expect(result.outcome.plan.body).toContain("Incomplete input coverage");
    }),
  );
});

describe("deterministic finding merge", () => {
  it("keeps the most severe duplicate anchor and ranks deterministically", () => {
    const duplicate = ReviewFinding.make({ ...supportedFinding, severity: "nit" });
    const blocking = ReviewFinding.make({ ...unsupportedFinding, severity: "blocking" });
    expect(
      rankAndDedupeFindings([duplicate, unsupportedFinding, blocking, supportedFinding]),
    ).toEqual([blocking, supportedFinding]);
  });
});

type OfflineFanOutProgram = ReturnType<typeof runOfflineFanOut>;
type OfflineFanOutServices = Effect.Services<OfflineFanOutProgram>;
type OfflineFanOutFailure = EffectError<OfflineFanOutProgram>;
type OfflineFanOutRequirementsProof = Assert<Equal<OfflineFanOutServices, never>>;
type PassMisbehaviorContainedProof = Assert<
  Equal<Extract<OfflineFanOutFailure, ReviewPassMisbehaved>, never>
>;

describe("fan-out Effect channels", () => {
  it("keeps provided requirements empty and expected pass failures contained", () => {
    const noRequirements: OfflineFanOutRequirementsProof = true;
    const passFailuresAreResults: PassMisbehaviorContainedProof = true;
    expect([noRequirements, passFailuresAreResults]).toEqual([true, true]);
  });
});

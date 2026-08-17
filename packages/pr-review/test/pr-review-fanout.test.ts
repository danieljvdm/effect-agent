import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Layer, Redacted, Ref, Schema } from "effect";
import { Agent, IdGenerator, RunEvent, SubagentReservationsMemoryLive } from "effect-agent";
import { Tool } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

import {
  CandidateAssessment,
  ChangedFile,
  CodeReview,
  DelegateFileReview,
  executeReview,
  assessReviewPipeline,
  fanOutHandlersLayer,
  FanOutCoordinatorToolkitLayer,
  fanOutReviewBudgetLimits,
  fanOutReviewerProfile,
  FanOutReviewer,
  FanOutReviewerProfile,
  FileReviewer,
  FileReviewReport,
  FileReviewRequest,
  FileReviewToolkit,
  FileReviewToolkitLayer,
  FileReviewUnitFailed,
  FileReviewUnitResult,
  type FileReviewWorkRejected,
  FindingCandidate,
  ListReviewUnits,
  makeFanOutReviewInstructions,
  makeFanOutReviewSuite,
  MAX_FILE_REVIEW_TOOL_CALLS,
  MAX_REVIEW_UNITS,
  MAX_UNIT_FILES,
  planReviewUnits,
  PullRequestMetadata,
  rankAndDedupeFindings,
  ReviewFinding,
  ReviewExecutionContext,
  ReviewMission,
  type ReviewPublicationPlan,
  ReviewStateAuthenticator,
  type ReviewRiskCategory,
  WalkthroughEntry,
  webCryptoReviewStateAuthenticatorLayer,
  defaultFileReviewerPolicy,
  fileReviewPolicy,
} from "../src/index.ts";
import {
  collectingReviewPublisherLayer,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  makeOfflineFanOutCoordinatorModel,
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
    body: `Regression fixture; ${MISSION_MARKER} stays with the coordinator.`,
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

const candidateFor = (finding: ReviewFinding, index: number): FindingCandidate =>
  FindingCandidate.make({
    candidateId: `${specialistPass.passId}:finding:${String(index).padStart(3, "0")}`,
    workId: specialistPass.passId,
    unitId: specialistPass.unitId,
    finding,
    evidencePaths: [finding.path],
  });

const supportedCandidate = candidateFor(supportedFinding, 1);
const unsupportedCandidate = candidateFor(unsupportedFinding, 2);

const discoveryRequest = (pass: typeof generalPass): FileReviewRequest =>
  FileReviewRequest.make({
    phase: "discovery",
    workId: pass.passId,
    unitId: pass.unitId,
    paths: pass.paths,
    perspective: pass.perspective,
    riskCategories: pass.riskCategories,
    candidates: [],
  });

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

const verificationRequest = FileReviewRequest.make({
  phase: "verification",
  workId: `${highRiskUnit.unitId}-verification`,
  unitId: highRiskUnit.unitId,
  paths: highRiskUnit.paths,
  perspective: "candidate-verification",
  riskCategories: highRiskUnit.riskCategories,
  candidates: [supportedCandidate, unsupportedCandidate],
});

const verificationReport = FileReviewReport.make({
  phase: "verification",
  workId: verificationRequest.workId,
  unitId: verificationRequest.unitId,
  findings: [],
  concerns: [],
  fileSummaries: [],
  assessments: [
    CandidateAssessment.make({
      candidateId: supportedCandidate.candidateId,
      disposition: "confirmed",
      rationale: "The changed authentication mode is reachable without an identity check.",
    }),
    CandidateAssessment.make({
      candidateId: unsupportedCandidate.candidateId,
      disposition: "rejected",
      rationale: "The evidence contains no disabling branch and does not establish the claim.",
    }),
  ],
});

const coordinatorReview = CodeReview.make({
  summary: "General and specialist discovery completed; candidate verification settled.",
  verdict: "comment",
  findings: [unsupportedFinding],
  concerns: [],
});

const runOfflineFanOut = (script: {
  readonly discoveryReports: ReadonlyArray<OfflineUnitScript>;
  readonly verificationReports?: ReadonlyArray<OfflineUnitScript> | undefined;
  readonly verificationCalls?: ReadonlyArray<FileReviewRequest> | undefined;
  readonly review?: CodeReview | undefined;
}) =>
  Effect.gen(function* () {
    const coordinator = yield* makeOfflineFanOutCoordinatorModel({
      discoveryCalls: highRiskPlan.discoveryPasses.map(discoveryRequest),
      verificationCalls: script.verificationCalls ?? [],
      review: script.review ?? coordinatorReview,
    });
    const children = yield* makeOfflineFileReviewerModel([
      ...script.discoveryReports,
      ...(script.verificationReports ?? []),
    ]);
    const parentBinding = Agent.withModel(FanOutReviewer, coordinator.model);
    const childBinding = Agent.withModel(FileReviewer, children.model);
    const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
    const stateAuthenticator = yield* Effect.gen(function* () {
      return yield* ReviewStateAuthenticator;
    }).pipe(
      Effect.provide(
        webCryptoReviewStateAuthenticatorLayer(Redacted.make("offline-assurance-secret")),
      ),
    );
    const sourceLayer = fixturePullRequestSourceLayer(highRiskFixture);
    const childSupportLayer = Layer.mergeAll(
      FileReviewToolkitLayer,
      SubagentReservationsMemoryLive,
      IdGenerator.layer,
    ).pipe(Layer.provideMerge(sourceLayer));
    const program = executeReview(parentBinding, {
      post: true,
      applyVerdict: false,
      limits: fanOutReviewBudgetLimits,
      reviewShape: "fan-out",
      signature: () => "offline-assurance-profile-v2",
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          FanOutCoordinatorToolkitLayer.pipe(Layer.provideMerge(sourceLayer)),
          fanOutHandlersLayer(childBinding).pipe(Layer.provide(childSupportLayer)),
          collectingReviewPublisherLayer(published),
          IdGenerator.layer,
        ),
      ),
      Effect.provideService(ReviewExecutionContext, {
        mode: "full",
        reason: "offline full-diff assurance fixture",
        files: highRiskFiles,
        affectedPaths: highRiskFiles.map((file) => file.path),
        totalFiles: highRiskFiles.length,
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
      coordinatorCalls: yield* coordinator.calls,
      coordinatorPrompts: yield* coordinator.prompts,
      childCalls: yield* children.calls,
      childPrompts: yield* children.prompts,
    };
  });

describe("fan-out review contract", () => {
  const mission = ReviewMission.make({
    repository: "acme/ingress",
    number: 110,
    title: "Authenticate ingress",
    body: "",
    baseRef: "main",
    headRef: "work-order-ingress",
    changedFileCount: 2,
  });

  it("requires discovery, exact independent verification, and honest non-exhaustive prose", () => {
    const instructions = makeFanOutReviewInstructions({
      guidance: "Review architecture first.",
      maxFindings: 7,
    })(mission);
    expect(instructions).toContain("Review architecture first.");
    expect(instructions).toContain("EVERY discoveryPass");
    expect(instructions).toContain("EVERY candidate copied byte-for-byte");
    expect(instructions).toContain("never an exhaustive or defect-free review");
    expect(instructions).toContain("publication cap is 7");
  });

  it("uses evidence-only children with bounded structured concurrency", () => {
    expect(Object.keys(FileReviewToolkit.tools)).toEqual([]);
    expect(MAX_FILE_REVIEW_TOOL_CALLS).toBe(1);
    expect(defaultFileReviewerPolicy.maxToolCalls).toBe(1);
    expect(fileReviewPolicy.maxToolCalls).toBe(1);
    expect(fileReviewPolicy.maxConcurrency).toBe(4);
    expect(Duration.toMillis(defaultFileReviewerPolicy.maxDuration)).toBe(6 * 60_000);
  });

  it("pins the exact capability and non-guarantee profile", () => {
    const decoded = Schema.decodeUnknownSync(FanOutReviewerProfile)(
      Schema.encodeSync(FanOutReviewerProfile)(fanOutReviewerProfile),
    );
    expect(decoded).toEqual(fanOutReviewerProfile);
    expect(fanOutReviewerProfile).toMatchObject({
      hostOwnedRiskClassification: true,
      redundantHighRiskDiscovery: true,
      independentCandidateVerification: true,
      defectAbsenceProven: false,
      exactlyOnceExternalEffects: false,
    });
  });

  it("keeps coordinator tools compatible with strict provider object schemas", () => {
    for (const tool of [ListReviewUnits, DelegateFileReview]) {
      const jsonSchema = Tool.getJsonSchema(tool, { transformer: toCodecOpenAI });
      expect(jsonSchema.type).toBe("object");
      expect(jsonSchema.anyOf).toBeUndefined();
    }
  });

  it("carries shared guidance to both coordinator and workers", () => {
    const suite = makeFanOutReviewSuite({ guidance: "Architecture first.", maxFindings: 7 });
    expect(suite.parent.instructions(mission)).toContain("Architecture first.");
    const brief = {
      ...discoveryRequest(generalPass),
      evidence: [],
    };
    expect(suite.child.instructions(brief)).toContain("Architecture first.");
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

  it("reports undiffable and over-capacity paths instead of dropping them", () => {
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
    const plan = planReviewUnits([...many, binary], { totalChangedFiles: 121 });
    const assigned = plan.units.flatMap((unit) => unit.paths);

    expect(plan.undiffablePaths).toEqual(["assets/logo.png"]);
    expect([...assigned, ...plan.unassignedPaths].sort()).toEqual(
      many.map((file) => file.path).sort(),
    );
  });

  it("assigns but explicitly marks truncated diff evidence as partial input", () => {
    const oversized = ChangedFile.make({
      path: "src/oversized.ts",
      status: "modified",
      additions: 1,
      deletions: 0,
      patch: `@@ -1 +1 @@\n+${"x".repeat(61_000)}`,
    });
    const plan = planReviewUnits([oversized], { totalChangedFiles: 1 });
    expect(plan.units.flatMap((unit) => unit.paths)).toEqual([oversized.path]);
    expect(plan.partialEvidencePaths).toEqual([oversized.path]);

    const assessment = assessReviewPipeline({
      shape: "fan-out",
      files: [oversized],
      totalFiles: 1,
      anchorFiles: [oversized],
      totalAnchorFiles: 1,
      events: [],
    });
    expect(assessment.inputCoverage.status).toBe("incomplete");
    expect(assessment.inputCoverage.assignedPaths).toEqual([oversized.path]);
    expect(assessment.inputCoverage.partialPaths).toEqual([oversized.path]);
    expect(assessment.inputCoverage.reasons).toContain(
      `model-visible diff evidence was truncated (1): ${oversized.path}`,
    );
  });

  it("is deterministic regardless of source order", () => {
    expect(planReviewUnits([...highRiskFiles].reverse(), { totalChangedFiles: 2 })).toEqual(
      highRiskPlan,
    );
  });
});

describe("offline discovery and verification pipeline", () => {
  const successfulDiscovery: ReadonlyArray<OfflineUnitScript> = [
    {
      workId: generalPass.passId,
      outcome: { _tag: "report", report: discoveryReport(generalPass, []) },
    },
    {
      workId: specialistPass.passId,
      outcome: {
        _tag: "report",
        report: discoveryReport(specialistPass, [supportedFinding, unsupportedFinding]),
      },
    },
  ];

  it.effect(
    "finds a defect missed by general discovery and publishes only verifier-confirmed candidates",
    () =>
      Effect.gen(function* () {
        const result = yield* runOfflineFanOut({
          discoveryReports: successfulDiscovery,
          verificationCalls: [verificationRequest],
          verificationReports: [
            {
              workId: verificationRequest.workId,
              outcome: { _tag: "report", report: verificationReport },
            },
          ],
        });

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
        });
        expect(result.outcome.review.findings).toEqual([supportedFinding]);
        expect(result.outcome.review.findings).not.toContainEqual(unsupportedFinding);
        expect(result.outcome.plan.comments.map((comment) => comment.path)).toEqual([
          supportedFinding.path,
        ]);
        expect(result.outcome.plan.body).toContain("**Review assurance:** settled");
        expect(result.outcome.state?.reviewedHeadSha).toBe(FIXTURE_SHA);
        expect(result.outcome.plan.body).toContain("effect-agent-pr-review state-v1:");
        expect(result.published).toHaveLength(1);

        const generalPrompt = result.childPrompts.find((prompt) =>
          prompt.includes(generalPass.passId),
        );
        const specialistPrompt = result.childPrompts.find(
          (prompt) =>
            prompt.includes(specialistPass.passId) &&
            !prompt.includes(supportedCandidate.candidateId),
        );
        const verifierPrompt = result.childPrompts.find((prompt) =>
          prompt.includes(verificationRequest.workId),
        );
        expect(generalPrompt).toBeDefined();
        expect(specialistPrompt).toBeDefined();
        expect(verifierPrompt).toContain(supportedCandidate.candidateId);
        expect(verifierPrompt).toContain("authenticateMode");
        expect(verifierPrompt).not.toContain("publishWebhookMode");
        for (const prompt of result.childPrompts) expect(prompt).not.toContain(MISSION_MARKER);
      }),
  );

  it.effect("makes failed specialist discovery visible and prevents settled assurance", () =>
    Effect.gen(function* () {
      const result = yield* runOfflineFanOut({
        discoveryReports: [
          successfulDiscovery[0]!,
          { workId: specialistPass.passId, outcome: { _tag: "malformed-output" } },
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
      expect(result.outcome.state).toBeUndefined();
      expect(result.outcome.plan.body).toContain("Incomplete review assurance");
      expect(result.outcome.plan.event).toBe("COMMENT");
    }),
  );

  it.effect("leaves every candidate unsettled when independent verification fails", () =>
    Effect.gen(function* () {
      const result = yield* runOfflineFanOut({
        discoveryReports: successfulDiscovery,
        verificationCalls: [verificationRequest],
        verificationReports: [
          { workId: verificationRequest.workId, outcome: { _tag: "malformed-output" } },
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
          workId: verificationRequest.workId,
          stage: "verification",
          errorTag: expect.stringContaining("AgentOutputError"),
        }),
      );
      expect(result.outcome.review.findings).toEqual([]);
      expect(result.outcome.plan.comments).toEqual([]);
      expect(result.outcome.plan.event).toBe("COMMENT");
      expect(result.outcome.state).toBeUndefined();
    }),
  );

  it("surfaces typed policy exhaustion as a failed specialist pass", () => {
    const generalRequest = discoveryRequest(generalPass);
    const specialistRequest = discoveryRequest(specialistPass);
    const base = {
      eventVersion: 1,
      runId: "run-assurance-exhaustion",
      conversationId: "conversation-assurance-exhaustion",
      agentId: "pr-fanout-reviewer",
      timestamp: "2026-08-17T20:00:00.000Z",
      providerExecuted: false,
    } as const;
    const events = [
      Schema.decodeUnknownSync(RunEvent)({
        ...base,
        _tag: "ToolCallDeclared",
        sequence: 1,
        toolCallId: "general-call",
        toolName: "delegate_file_review",
        parameters: Schema.encodeSync(FileReviewRequest)(generalRequest),
      }),
      Schema.decodeUnknownSync(RunEvent)({
        ...base,
        _tag: "ToolCallSucceeded",
        sequence: 2,
        toolCallId: "general-call",
        toolName: "delegate_file_review",
        result: Schema.encodeSync(FileReviewUnitResult)(
          FileReviewUnitResult.make({
            phase: "discovery",
            workId: generalPass.passId,
            unitId: generalPass.unitId,
            candidates: [],
            fileSummaries: [],
            assessments: [],
          }),
        ),
      }),
      Schema.decodeUnknownSync(RunEvent)({
        ...base,
        _tag: "ToolCallDeclared",
        sequence: 3,
        toolCallId: "specialist-call",
        toolName: "delegate_file_review",
        parameters: Schema.encodeSync(FileReviewRequest)(specialistRequest),
      }),
      Schema.decodeUnknownSync(RunEvent)({
        ...base,
        _tag: "ToolCallSucceeded",
        sequence: 4,
        toolCallId: "specialist-call",
        toolName: "delegate_file_review",
        result: Schema.encodeSync(FileReviewUnitFailed)(
          FileReviewUnitFailed.make({
            childErrorTag: "AgentPolicyError",
            message: "review worker exceeded its bounded turn budget",
          }),
        ),
      }),
    ];
    const assessment = assessReviewPipeline({
      shape: "fan-out",
      files: highRiskFiles,
      totalFiles: 2,
      anchorFiles: highRiskFiles,
      totalAnchorFiles: 2,
      events,
    });

    expect(defaultFileReviewerPolicy.onExhaustion).toBe("fail");
    expect(assessment.assurance.status).toBe("incomplete");
    expect(assessment.assurance.failedPasses).toContainEqual(
      expect.objectContaining({
        workId: specialistPass.passId,
        stage: "specialist",
        errorTag: "FileReviewUnitFailed:AgentPolicyError",
      }),
    );
    expect(assessment.coverage.status).toBe("incomplete");
  });
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
type UnitFailureContainedProof = Assert<
  Equal<Extract<OfflineFanOutFailure, FileReviewUnitFailed>, never>
>;
type WorkRejectionContainedProof = Assert<
  Equal<Extract<OfflineFanOutFailure, FileReviewWorkRejected>, never>
>;

describe("fan-out Effect channels", () => {
  it("keeps provided requirements empty and expected child failures contained", () => {
    const noRequirements: OfflineFanOutRequirementsProof = true;
    const unitFailuresAreResults: UnitFailureContainedProof = true;
    const workRejectionsAreResults: WorkRejectionContainedProof = true;
    expect([noRequirements, unitFailuresAreResults, workRejectionsAreResults]).toEqual([
      true,
      true,
      true,
    ]);
  });
});

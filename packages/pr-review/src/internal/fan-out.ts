import { Effect, Schema } from "effect";
import {
  Agent,
  AgentPolicy,
  AgentRuntime,
  ToolResultBounds,
  type BudgetAdapterError,
  type BudgetExceeded,
  type RunBudgetHook,
  type RuntimeBinding,
} from "effect-agent";
import { Toolkit } from "effect/unstable/ai";

import { anchorViolation } from "./anchors.ts";
import { boundedListReason, FailedReviewPass, ReviewAssurance } from "./coverage.ts";
import { ChangedFileStatus, ChangedPath, type ChangedFile } from "./diff.ts";
import {
  CodeReview,
  fileReviewEvidenceChunks,
  MAX_PATCH_CHARS,
  MAX_WALKTHROUGH_SUMMARY_CHARS,
  REVIEW_TOOL_RESULT_MAX_BYTES,
  ReviewConcern,
  ReviewFinding,
  WalkthroughEntry,
} from "./review-agent.ts";
import {
  MAX_REVIEW_UNITS,
  MAX_UNIT_EVIDENCE_SHARDS,
  MAX_UNIT_FILES,
  findingAnchorInUnitEvidence,
  planReviewUnits,
  rankAndDedupeConcerns,
  rankAndDedupeFindings,
  ReviewEvidenceShardId,
  ReviewPassId,
  ReviewRiskCategory,
  ReviewUnitId,
  type ReviewDiscoveryPass,
  type ReviewUnit,
  type ReviewUnitPlan,
} from "./review-units.ts";

// ---------------------------------------------------------------------------
// The assured fan-out reviewer is a bounded, deterministic three-stage
// pipeline scheduled ENTIRELY by host code:
//
//   host plan -> independent discovery passes -> independent verification
//
// `planReviewUnits` is a pure function, so dispatch is plain Effect structured
// concurrency over its exact work list — there is no coordinator model, no
// delegation tool, and therefore no prompt-compliance failure mode. A pass
// that fails (child fault, malformed output) is retried once; a pass that
// still fails is recorded and its unit's paths are carried forward as
// retryable unreviewed scope instead of freezing the run's continuity
// baseline. A finding whose anchor is invalid is discarded and counted —
// never a reason to reject the whole pass.
// ---------------------------------------------------------------------------

/** One discovery pass returns at most this many anchored candidates. */
export const MAX_CHILD_FINDINGS = 6;

/** One discovery pass returns at most this many non-anchored candidates. */
export const MAX_CHILD_CONCERNS = 3;

/** Every unit receives independent general and specialist discovery passes. */
export const MAX_UNIT_CANDIDATES = (MAX_CHILD_FINDINGS + MAX_CHILD_CONCERNS) * 2;

/**
 * General + specialist discovery for every unit, then one verifier per unit.
 * The one-retry budget doubles the worst-case child Run count, but the
 * schedule itself never exceeds this bound.
 */
export const MAX_REVIEW_CHILDREN = MAX_REVIEW_UNITS * 3;

/** Bounded structured concurrency across units; passes inside a unit are sequential. */
export const REVIEW_UNIT_CONCURRENCY = 4;

/** Structural minimum for a child that exposes no tools. */
export const MAX_FILE_REVIEW_TOOL_CALLS = 1;

export const ReviewWorkPhase = Schema.Literals(["discovery", "verification"]);
export type ReviewWorkPhase = typeof ReviewWorkPhase.Type;

export const ReviewWorkPerspective = Schema.Literals([
  "general",
  "risk-specialist",
  "candidate-verification",
]);
export type ReviewWorkPerspective = typeof ReviewWorkPerspective.Type;

export const ReviewCandidateId = Schema.NonEmptyString.check(Schema.isMaxLength(96));

export class FindingCandidate extends Schema.TaggedClass<FindingCandidate>()("FindingCandidate", {
  candidateId: ReviewCandidateId,
  workId: ReviewPassId,
  unitId: ReviewUnitId,
  finding: ReviewFinding,
  evidencePaths: Schema.Array(ChangedPath)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(1)),
}) {}

export class ConcernCandidate extends Schema.TaggedClass<ConcernCandidate>()("ConcernCandidate", {
  candidateId: ReviewCandidateId,
  workId: ReviewPassId,
  unitId: ReviewUnitId,
  concern: ReviewConcern,
  evidencePaths: Schema.Array(ChangedPath)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(3)),
}) {}

export const ReviewCandidate = Schema.Union([FindingCandidate, ConcernCandidate]);
export type ReviewCandidate = typeof ReviewCandidate.Type;

/** Deterministic host equivalence for claims repeated across discovery passes. */
export const reviewCandidateSubjectKey = (candidate: ReviewCandidate): string =>
  candidate._tag === "FindingCandidate"
    ? `finding:${JSON.stringify(Schema.encodeSync(ReviewFinding)(candidate.finding))}`
    : `concern:${JSON.stringify(Schema.encodeSync(ReviewConcern)(candidate.concern))}`;

export class CandidateAssessment extends Schema.Class<CandidateAssessment>(
  "@effect-agent/pr-review/CandidateAssessment",
)({
  candidateId: ReviewCandidateId,
  disposition: Schema.Literals(["confirmed", "rejected"]),
  /**
   * Exact suggestion settlement: required when the candidate finding carries
   * a suggestion, forbidden otherwise. Untrusted child output cannot publish
   * a GitHub replacement block by prompt compliance alone — the host keeps a
   * confirmed finding's suggestion only on an exact "committable" settlement.
   */
  suggestion: Schema.optionalKey(
    Schema.Literals(["committable", "not-committable"]).annotate({
      description:
        'Required exactly when the candidate finding carries a suggestion: "committable" only when its text is the full replacement source for exactly lines startLine..endLine and nothing else. Forbidden for candidates without a suggestion.',
    }),
  ),
  rationale: Schema.NonEmptyString.check(Schema.isMaxLength(600)),
}) {}

/**
 * Exact suggestion settlement shape: a carried suggestion must be settled and
 * nothing else may be. A verification report that violates it is treated as a
 * misbehaving pass and retried within the pass budget.
 */
export const assessmentSettlesSuggestionExactly = (
  assessment: CandidateAssessment,
  candidate: ReviewCandidate,
): boolean =>
  candidate._tag === "FindingCandidate" && candidate.finding.suggestion !== undefined
    ? assessment.suggestion !== undefined
    : assessment.suggestion === undefined;

/**
 * Fail-closed publication of a confirmed finding: only an exact "committable"
 * settlement keeps the suggestion; anything else publishes the finding with
 * the suggestion stripped so unverified text can never become a one-click
 * GitHub replacement block.
 */
export const confirmedFindingForPublication = (
  assessment: CandidateAssessment,
  candidate: FindingCandidate,
): ReviewFinding => {
  if (candidate.finding.suggestion === undefined || assessment.suggestion === "committable") {
    return candidate.finding;
  }
  const { suggestion: _stripped, ...finding } = candidate.finding;
  return ReviewFinding.make(finding);
};

/**
 * Concern candidates need explicit paths internally to bind the claim to
 * scheduled evidence. The verifier receives the complete bounded unit so it
 * can use neighboring evidence to falsify the claim. The public ReviewConcern
 * remains path-free after the host confirms and projects it.
 */
export class DiscoveredConcern extends Schema.Class<DiscoveredConcern>(
  "@effect-agent/pr-review/DiscoveredConcern",
)({
  concern: ReviewConcern,
  evidencePaths: Schema.Array(ChangedPath)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(3)),
}) {}

const UnitPaths = Schema.Array(ChangedPath)
  .check(Schema.isMinLength(1))
  .check(Schema.isMaxLength(MAX_UNIT_FILES));

const RiskCategories = Schema.Array(ReviewRiskCategory).check(Schema.isMaxLength(6));
const Candidates = Schema.Array(ReviewCandidate).check(Schema.isMaxLength(MAX_UNIT_CANDIDATES));
const EvidenceShardIds = Schema.Array(ReviewEvidenceShardId)
  .check(Schema.isMinLength(1))
  .check(Schema.isMaxLength(MAX_UNIT_EVIDENCE_SHARDS));

/** One complete host-selected evidence shard supplied to a review child. */
export class FileReviewEvidence extends Schema.Class<FileReviewEvidence>(
  "@effect-agent/pr-review/FileReviewEvidence",
)({
  shardId: ReviewEvidenceShardId,
  path: ChangedPath,
  status: ChangedFileStatus,
  reviewMode: Schema.Literals(["diff", "content", "unavailable"]),
  ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  total: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  annotatedPatch: Schema.String.check(Schema.isMaxLength(MAX_PATCH_CHARS)),
}) {}

/** Host-prepared child input with complete bounded diff/content evidence. */
export class FileReviewBrief extends Schema.Class<FileReviewBrief>(
  "@effect-agent/pr-review/FileReviewBrief",
)({
  phase: ReviewWorkPhase,
  workId: ReviewPassId,
  unitId: ReviewUnitId,
  paths: UnitPaths,
  evidenceShardIds: EvidenceShardIds,
  perspective: ReviewWorkPerspective,
  riskCategories: RiskCategories,
  /** Empty for discovery; the exact discovered set for unit verification. */
  candidates: Candidates,
  evidence: Schema.Array(FileReviewEvidence)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(MAX_UNIT_EVIDENCE_SHARDS)),
}) {}

/** Child output; phase-inapplicable collections must be empty. */
export class FileReviewReport extends Schema.Class<FileReviewReport>(
  "@effect-agent/pr-review/FileReviewReport",
)({
  phase: ReviewWorkPhase,
  workId: ReviewPassId,
  unitId: ReviewUnitId,
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(MAX_CHILD_FINDINGS)),
  concerns: Schema.Array(DiscoveredConcern).check(Schema.isMaxLength(MAX_CHILD_CONCERNS)),
  fileSummaries: Schema.Array(WalkthroughEntry).check(Schema.isMaxLength(MAX_UNIT_FILES)),
  assessments: Schema.Array(CandidateAssessment).check(Schema.isMaxLength(MAX_UNIT_CANDIDATES)),
}) {}

/**
 * A structurally valid child report that does not answer the scheduled pass:
 * wrong identity, phase-inapplicable fields, or an inexact assessment set.
 * Retried once like any other pass fault, because it is model misbehavior,
 * not evidence about the code under review.
 */
export class ReviewPassMisbehaved extends Schema.TaggedError<ReviewPassMisbehaved>()(
  "ReviewPassMisbehaved",
  {
    workId: ReviewPassId,
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(600)),
  },
) {}

export interface FanOutInstructionOptions {
  readonly guidance?: string | ReadonlyArray<string> | undefined;
}

const staticGuidanceLines = (
  guidance: string | ReadonlyArray<string> | undefined,
): ReadonlyArray<string> => {
  if (guidance === undefined) return [];
  const lines = typeof guidance === "string" ? [guidance] : guidance;
  return lines.filter((line) => line.length > 0);
};

const evidenceInstructions = [
  "The host placed complete bounded review evidence shards in the input evidence array. Treat every shard as required input; ordinal/total identifies multi-shard paths.",
  "You have no tools and cannot roam outside this evidence. If it is insufficient for a candidate, reject or omit that candidate rather than guessing.",
  "A diff marks new-version anchors as R<number>; only those lines may anchor findings. B/H content evidence is non-anchorable.",
];

/** Discovery and verification instructions share one child definition. */
export const makeFileReviewerInstructions =
  (options: FanOutInstructionOptions = {}) =>
  (brief: FileReviewBrief): string => {
    const common = [
      `You are an attached review worker for ${brief.workId} in host-planned unit ${brief.unitId}: ${brief.paths.join(", ")}.`,
      ...staticGuidanceLines(options.guidance),
      ...evidenceInstructions,
    ];
    if (brief.phase === "verification") {
      return [
        ...common,
        "Independently verify every candidate in the input. You did not receive another reviewer's transcript or reasoning; use only the candidate claim and bounded evidence.",
        "The evidence array contains the complete bounded unit, including neighboring changed code that may confirm or falsify a locally plausible claim.",
        "For each candidate, try to falsify it first. Confirm only when the cited behavior is supported and actionable. Reject unsupported, speculative, duplicate, or non-actionable candidates.",
        'Return ONLY JSON with phase "verification", the exact workId/unitId, empty findings/concerns/fileSummaries arrays, and exactly one assessment per candidateId. Each assessment is {"candidateId": <exact id>, "disposition": <"confirmed" | "rejected">, "suggestion": <"committable" | "not-committable", present exactly when the candidate finding carries a suggestion>, "rationale": <bounded evidence-based reason>}. Never add or omit an id.',
        'Settle every carried suggestion independently of the claim: answer "committable" only when its text is the full replacement source for exactly lines startLine..endLine and nothing else — it compiles in context and preserves the finding\'s intent, never prose describing a change. Otherwise answer "not-committable"; the host then publishes the confirmed finding without its suggestion. Omit the assessment "suggestion" field for candidates without one.',
      ].join("\n");
    }
    const focus =
      brief.perspective === "risk-specialist"
        ? brief.riskCategories.length > 0
          ? `This is a fresh specialist discovery pass. Concentrate on these host-classified risks without relying on another pass: ${brief.riskCategories.join(", ")}.`
          : "This is a fresh specialist discovery pass. The host found no keyword-classified category, so independently scrutinize authentication/authorization, security boundaries, durability, concurrency, credentials, and external side effects rather than treating classification silence as low risk."
        : "This is the general discovery pass. Review broadly for correctness, security, concurrency, resource, API, and error-handling defects.";
    return [
      ...common,
      focus,
      "The discovery evidence array contains every complete shard in the unit. Review every entry and every shard of a multi-shard path. A later independent verifier, not you, decides which candidates publish.",
      "When a non-anchored concern depends on one or more unit files, list 1-3 exact evidencePaths to bind the claim to scheduled evidence.",
      `Return ONLY JSON with phase "discovery", the exact workId/unitId, up to ${MAX_CHILD_FINDINGS} findings, up to ${MAX_CHILD_CONCERNS} concerns shaped as {concern, evidencePaths}, one factual file summary per path (<= ${MAX_WALKTHROUGH_SUMMARY_CHARS} chars), and an empty assessments array. Empty candidate arrays are valid; do not invent defects.`,
      'Each finding is {"path": <a unit file path>, "startLine": <integer, an R-marked new-file line>, "endLine": <integer, >= startLine, same file, R-marked>, "severity": <"blocking" | "important" | "nit">, "category": <OPTIONAL problem-kind label>, "title": <string, <= 120 chars>, "body": <string, why it matters and what to do>, "suggestion": <string, OPTIONAL: replacement source code for exactly lines startLine..endLine, ready to commit>}.',
      'Include "suggestion" only when you are confident the replacement compiles and preserves intent; its text must contain the full replacement source for every line in the range and nothing else — never prose describing the change, which belongs in "body".',
    ].join("\n");
  };

export const fileReviewerInstructions = makeFileReviewerInstructions();

export const FileReviewToolkit = Toolkit.empty;

export const defaultFileReviewerPolicy = AgentPolicy.make({
  maxTurns: 6,
  maxToolCalls: MAX_FILE_REVIEW_TOOL_CALLS,
  maxDuration: "6 minutes",
  toolConcurrency: 2,
  repeatedFailureLimit: 6,
  tokenBudget: 200_000,
  contextTokenLimit: 150_000,
  toolResultBounds: ToolResultBounds.make({ maxBytes: REVIEW_TOOL_RESULT_MAX_BYTES }),
  // Discovery or verification that exhausts is unsettled work, never a
  // schema-valid partial that can contribute to a green assurance claim.
  onExhaustion: "fail",
});

export const makeFileReviewerDefinition = (options: FanOutInstructionOptions = {}) =>
  Agent.define("pr-review-worker", {
    input: FileReviewBrief,
    output: FileReviewReport,
    instructions: makeFileReviewerInstructions(options),
    toolkit: FileReviewToolkit,
    policy: defaultFileReviewerPolicy,
    description:
      "Perform one bounded discovery or independent candidate-verification pass over host-supplied pull-request evidence.",
    metadata: { deploymentClass: "E", surface: "read-only", stage: "discovery-verification" },
  });

export const FileReviewer = makeFileReviewerDefinition();

/** The exact child binding shape the host pipeline schedules. */
export type FileReviewerBinding<Provider, ModelProvides, ModelRequires> = RuntimeBinding<
  typeof FileReviewBrief,
  typeof FileReviewReport,
  ReturnType<typeof makeFileReviewerInstructions>,
  Toolkit.Tools<typeof FileReviewToolkit>,
  Provider,
  ModelProvides,
  ModelRequires
>;

// ---------------------------------------------------------------------------
// Host pipeline.
// ---------------------------------------------------------------------------

/** Everything one settled fan-out pipeline run produced, before publication. */
export interface FanOutPipelineOutcome {
  readonly review: CodeReview;
  readonly assurance: ReviewAssurance;
  readonly plan: ReviewUnitPlan;
  /** Paths of units with an unsettled pass — retryable scope for the next run. */
  readonly unreviewedPaths: ReadonlyArray<string>;
  /** Total settled child turns across every scheduled pass. */
  readonly turns: number;
}

export interface FanOutPipelineInput {
  readonly files: ReadonlyArray<ChangedFile>;
  readonly anchorFiles: ReadonlyArray<ChangedFile>;
  readonly totalChangedFiles: number;
  readonly maxFindings?: number | undefined;
  /** Shared run budget observed by every child pass. */
  readonly budget?: RunBudgetHook<BudgetExceeded | BudgetAdapterError> | undefined;
}

const candidateOrdinal = (index: number): string => String(index + 1).padStart(3, "0");

/** Rebuild one unit's complete evidence from the same snapshot the plan used. */
const unitEvidence = (
  unit: ReviewUnit,
  files: ReadonlyArray<ChangedFile>,
): Effect.Effect<ReadonlyArray<FileReviewEvidence>> =>
  Effect.gen(function* () {
    const byPath = new Map(files.map((file) => [file.path, file] as const));
    const evidence: Array<FileReviewEvidence> = [];
    for (const shard of unit.evidenceShards) {
      const file = byPath.get(shard.path);
      const chunk =
        file === undefined ? undefined : fileReviewEvidenceChunks(file)[shard.ordinal - 1];
      if (file === undefined || chunk === undefined) {
        // The plan and this evidence derive from the same immutable snapshot
        // via the same pure function; a mismatch is a host defect, not input.
        return yield* Effect.die(
          new Error(`planned evidence shard has no source: ${shard.shardId} (${shard.path})`),
        );
      }
      evidence.push(
        FileReviewEvidence.make({
          shardId: shard.shardId,
          path: shard.path,
          status: file.status,
          reviewMode: chunk.reviewMode,
          ordinal: shard.ordinal,
          total: shard.total,
          annotatedPatch: chunk.annotatedPatch,
        }),
      );
    }
    return evidence;
  });

interface SettledPass {
  readonly report: FileReviewReport;
  readonly turns: number;
}

type PassOutcome =
  | ({ readonly _tag: "settled" } & SettledPass)
  | { readonly _tag: "failed"; readonly errorTag: string };

const misbehaved = (workId: string, reason: string) =>
  ReviewPassMisbehaved.make({ workId, reason: reason.slice(0, 600) });

/** Validate that a verification report assesses exactly the scheduled candidates. */
const validateVerificationReport = (
  brief: FileReviewBrief,
  report: FileReviewReport,
): ReviewPassMisbehaved | undefined => {
  if (report.findings.length > 0 || report.concerns.length > 0 || report.fileSummaries.length > 0) {
    return misbehaved(brief.workId, "verification output contained discovery-only fields");
  }
  const expectedById = new Map(
    brief.candidates.map((candidate) => [candidate.candidateId, candidate] as const),
  );
  const assessedIds = new Set<string>();
  for (const assessment of report.assessments) {
    const candidate = expectedById.get(assessment.candidateId);
    if (candidate === undefined || assessedIds.has(assessment.candidateId)) {
      return misbehaved(brief.workId, "verification output did not assess the exact candidate set");
    }
    if (!assessmentSettlesSuggestionExactly(assessment, candidate)) {
      return misbehaved(
        brief.workId,
        "verification output did not settle suggestion publication exactly",
      );
    }
    assessedIds.add(assessment.candidateId);
  }
  if (assessedIds.size !== expectedById.size) {
    return misbehaved(brief.workId, "verification output did not assess the exact candidate set");
  }
  return undefined;
};

/**
 * Run one scheduled pass: execute the child, decode its report, and enforce
 * the pass contract. Any typed fault — child failure, malformed or misdirected
 * output — is retried once; budget exhaustion is terminal because a retry
 * would fail the same way. The settled outcome is a value either way, so one
 * flaky pass can never fail the whole pipeline.
 */
const runReviewPass = <Provider, ModelProvides, ModelRequires>(
  binding: FileReviewerBinding<Provider, ModelProvides, ModelRequires>,
  brief: FileReviewBrief,
  budget: RunBudgetHook<BudgetExceeded | BudgetAdapterError> | undefined,
) =>
  Effect.gen(function* () {
    const result = yield* AgentRuntime.run(binding, brief, {
      ...(budget === undefined ? {} : { budget }),
      estimateCostMicrousd: () => Effect.succeed(500),
    });
    const report = yield* Schema.decodeUnknownEffect(FileReviewReport)(result.output).pipe(
      Effect.mapError((error) =>
        misbehaved(brief.workId, `child report failed to decode: ${error.message}`),
      ),
    );
    if (
      report.phase !== brief.phase ||
      report.workId !== brief.workId ||
      report.unitId !== brief.unitId
    ) {
      return yield* misbehaved(
        brief.workId,
        "child report identity does not match the scheduled pass",
      );
    }
    if (brief.phase === "verification") {
      const violation = validateVerificationReport(brief, report);
      if (violation !== undefined) return yield* violation;
    } else if (report.assessments.length > 0) {
      return yield* misbehaved(
        brief.workId,
        "discovery output contained verification-only assessments",
      );
    }
    return { report, turns: result.turns } satisfies SettledPass;
  }).pipe(
    Effect.scoped,
    Effect.retry({ times: 1, while: (error) => error._tag !== "BudgetExceeded" }),
    Effect.map((settled): PassOutcome => ({ _tag: "settled", ...settled })),
    Effect.catch((error) =>
      Effect.succeed<PassOutcome>({ _tag: "failed", errorTag: String(error._tag).slice(0, 256) }),
    ),
  );

interface DiscoveryHarvest {
  readonly candidates: ReadonlyArray<ReviewCandidate>;
  readonly fileSummaries: ReadonlyArray<WalkthroughEntry>;
  readonly discarded: number;
}

/**
 * Keep only findings anchored inside the pass's exact assigned evidence and
 * concerns bound to unit paths. Everything else is discarded and counted —
 * an invalid anchor invalidates one claim, never the pass that produced it.
 */
const harvestDiscovery = (
  pass: ReviewDiscoveryPass,
  unit: ReviewUnit,
  files: ReadonlyArray<ChangedFile>,
  anchorFiles: ReadonlyArray<ChangedFile>,
  report: FileReviewReport,
): DiscoveryHarvest => {
  const allowed = new Set(pass.paths);
  let discarded = 0;
  const keptFindings: Array<ReviewFinding> = [];
  for (const finding of report.findings) {
    if (
      !allowed.has(finding.path) ||
      anchorViolation(finding, anchorFiles) !== undefined ||
      !findingAnchorInUnitEvidence(finding, unit, files)
    ) {
      discarded += 1;
      continue;
    }
    keptFindings.push(finding);
  }
  const keptConcerns: Array<DiscoveredConcern> = [];
  for (const candidate of report.concerns) {
    if (candidate.evidencePaths.some((path) => !allowed.has(path))) {
      discarded += 1;
      continue;
    }
    keptConcerns.push(candidate);
  }
  return {
    candidates: [
      ...keptFindings.map((finding, index) =>
        FindingCandidate.make({
          candidateId: `${pass.passId}:finding:${candidateOrdinal(index)}`,
          workId: pass.passId,
          unitId: pass.unitId,
          finding,
          evidencePaths: [finding.path],
        }),
      ),
      ...keptConcerns.map((candidate, index) =>
        ConcernCandidate.make({
          candidateId: `${pass.passId}:concern:${candidateOrdinal(index)}`,
          workId: pass.passId,
          unitId: pass.unitId,
          concern: candidate.concern,
          evidencePaths: candidate.evidencePaths,
        }),
      ),
    ],
    fileSummaries: report.fileSummaries.filter((entry) => allowed.has(entry.path)),
    discarded,
  };
};

interface UnitReviewOutcome {
  readonly failedPasses: ReadonlyArray<FailedReviewPass>;
  readonly discoveredCandidates: number;
  readonly confirmed: ReadonlyArray<{
    readonly assessment: CandidateAssessment;
    readonly candidate: ReviewCandidate;
  }>;
  readonly rejectedCandidates: number;
  readonly unsettledCandidates: number;
  readonly discardedFindings: number;
  readonly walkthrough: ReadonlyArray<WalkthroughEntry>;
  readonly turns: number;
  readonly completedGeneralPasses: number;
  readonly completedSpecialistPasses: number;
  readonly requiredVerificationPasses: number;
  readonly completedVerificationPasses: number;
  readonly unreviewedPaths: ReadonlyArray<string>;
}

const reviewUnit = <Provider, ModelProvides, ModelRequires>(
  binding: FileReviewerBinding<Provider, ModelProvides, ModelRequires>,
  unit: ReviewUnit,
  passes: ReadonlyArray<ReviewDiscoveryPass>,
  input: FanOutPipelineInput,
) =>
  Effect.gen(function* () {
    const evidence = yield* unitEvidence(unit, input.files);
    const failedPasses: Array<FailedReviewPass> = [];
    const candidates: Array<ReviewCandidate> = [];
    const subjects = new Set<string>();
    const walkthrough: Array<WalkthroughEntry> = [];
    let discardedFindings = 0;
    let turns = 0;
    let completedGeneralPasses = 0;
    let completedSpecialistPasses = 0;
    for (const pass of passes) {
      const stage = pass.perspective === "risk-specialist" ? "specialist" : "discovery";
      const brief = FileReviewBrief.make({
        phase: "discovery",
        workId: pass.passId,
        unitId: pass.unitId,
        paths: pass.paths,
        evidenceShardIds: pass.evidenceShardIds,
        perspective: pass.perspective,
        riskCategories: pass.riskCategories,
        candidates: [],
        evidence,
      });
      const outcome = yield* runReviewPass(binding, brief, input.budget);
      if (outcome._tag === "failed") {
        failedPasses.push(
          FailedReviewPass.make({ workId: pass.passId, stage, errorTag: outcome.errorTag }),
        );
        continue;
      }
      turns += outcome.turns;
      if (stage === "specialist") {
        completedSpecialistPasses += 1;
      } else {
        completedGeneralPasses += 1;
      }
      const harvest = harvestDiscovery(pass, unit, input.files, input.anchorFiles, outcome.report);
      discardedFindings += harvest.discarded;
      if (pass.perspective === "general") walkthrough.push(...harvest.fileSummaries);
      for (const candidate of harvest.candidates) {
        const subject = reviewCandidateSubjectKey(candidate);
        if (subjects.has(subject)) continue;
        subjects.add(subject);
        candidates.push(candidate);
      }
    }
    const confirmed: Array<{
      readonly assessment: CandidateAssessment;
      readonly candidate: ReviewCandidate;
    }> = [];
    let rejectedCandidates = 0;
    let unsettledCandidates = 0;
    let completedVerificationPasses = 0;
    const requiredVerificationPasses = candidates.length > 0 ? 1 : 0;
    if (candidates.length > 0) {
      const workId = `${unit.unitId}-verification`;
      const brief = FileReviewBrief.make({
        phase: "verification",
        workId,
        unitId: unit.unitId,
        paths: unit.paths,
        evidenceShardIds: unit.evidenceShards.map((shard) => shard.shardId),
        perspective: "candidate-verification",
        riskCategories: unit.riskCategories,
        candidates,
        evidence,
      });
      const outcome = yield* runReviewPass(binding, brief, input.budget);
      if (outcome._tag === "failed") {
        unsettledCandidates = candidates.length;
        failedPasses.push(
          FailedReviewPass.make({ workId, stage: "verification", errorTag: outcome.errorTag }),
        );
      } else {
        turns += outcome.turns;
        completedVerificationPasses = 1;
        const byId = new Map(candidates.map((candidate) => [candidate.candidateId, candidate]));
        for (const assessment of outcome.report.assessments) {
          const candidate = byId.get(assessment.candidateId);
          if (candidate === undefined) continue;
          if (assessment.disposition === "confirmed") {
            confirmed.push({ assessment, candidate });
          } else {
            rejectedCandidates += 1;
          }
        }
      }
    }
    return {
      failedPasses,
      discoveredCandidates: candidates.length,
      confirmed,
      rejectedCandidates,
      unsettledCandidates,
      discardedFindings,
      walkthrough,
      turns,
      completedGeneralPasses,
      completedSpecialistPasses,
      requiredVerificationPasses,
      completedVerificationPasses,
      unreviewedPaths: failedPasses.length > 0 ? unit.paths : [],
    } satisfies UnitReviewOutcome;
  });

const countNoun = (count: number, noun: string): string =>
  `${count} ${noun}${count === 1 ? "" : "s"}`;

const composeSummary = (plan: ReviewUnitPlan, assurance: ReviewAssurance): string => {
  const requiredDiscovery =
    assurance.requiredGeneralDiscoveryPasses + assurance.requiredSpecialistPasses;
  const completedDiscovery =
    assurance.completedGeneralDiscoveryPasses + assurance.completedSpecialistPasses;
  const parts = [
    `Reviewed ${countNoun(plan.totalFiles, "changed file")} across ${countNoun(plan.units.length, "bounded unit")}: ${completedDiscovery}/${requiredDiscovery} discovery and ${assurance.completedVerificationPasses}/${assurance.requiredVerificationPasses} verification pass(es) settled; ${assurance.confirmedCandidates} of ${countNoun(assurance.discoveredCandidates, "discovered candidate")} confirmed by independent verification.`,
  ];
  if (assurance.failedPasses.length > 0) {
    parts.push(
      `${countNoun(assurance.failedPasses.length, "pass")} did not settle; the affected paths are carried forward and retried on the next run. This is a reviewer-side gap, not a code defect.`,
    );
  }
  if (assurance.discardedInvalidFindings > 0) {
    parts.push(
      `${countNoun(assurance.discardedInvalidFindings, "candidate")} discarded for anchors or paths outside the assigned evidence.`,
    );
  }
  if (plan.undiffablePaths.length > 0) {
    parts.push(
      `${countNoun(plan.undiffablePaths.length, "path")} had no reviewable textual evidence and keep input coverage incomplete; exclude such paths with ignore globs when that is intended.`,
    );
  }
  if (plan.unassignedPaths.length > 0 || plan.unassignedEvidenceShardCount > 0) {
    parts.push(
      "The changeset exceeded the bounded fan-out capacity; unassigned scope is reported under input coverage.",
    );
  }
  parts.push(
    "No configured pipeline can prove absence of defects; this describes settled work only.",
  );
  return parts.join(" ").slice(0, 4_000);
};

/**
 * Run the complete host-scheduled fan-out pipeline over one selected
 * changeset snapshot: plan, independent discovery, exact verification, and a
 * deterministic host-composed CodeReview from verifier-confirmed candidates
 * only. The verdict is derived from confirmed severities, never model prose.
 */
export const runFanOutReview = <Provider, ModelProvides, ModelRequires>(
  binding: FileReviewerBinding<Provider, ModelProvides, ModelRequires>,
  input: FanOutPipelineInput,
) =>
  Effect.gen(function* () {
    const plan = planReviewUnits(input.files, { totalChangedFiles: input.totalChangedFiles });
    const passesByUnit = new Map<string, Array<ReviewDiscoveryPass>>();
    for (const pass of plan.discoveryPasses) {
      const passes = passesByUnit.get(pass.unitId) ?? [];
      passes.push(pass);
      passesByUnit.set(pass.unitId, passes);
    }
    const outcomes = yield* Effect.forEach(
      plan.units,
      (unit) => reviewUnit(binding, unit, passesByUnit.get(unit.unitId) ?? [], input),
      { concurrency: REVIEW_UNIT_CONCURRENCY },
    );
    const failedPasses = outcomes.flatMap((outcome) => outcome.failedPasses);
    const unsettledCandidates = outcomes.reduce(
      (total, outcome) => total + outcome.unsettledCandidates,
      0,
    );
    const reasons: Array<string> = [];
    if (failedPasses.length > 0) {
      reasons.push(
        boundedListReason(
          "configured review passes did not settle",
          failedPasses.map((pass) => `${pass.workId} (${pass.errorTag})`),
        ),
      );
    }
    if (unsettledCandidates > 0) {
      reasons.push(
        `${unsettledCandidates} discovered candidate(s) did not receive exact verification`,
      );
    }
    const requiredSpecialistPasses = plan.discoveryPasses.filter(
      (pass) => pass.perspective === "risk-specialist",
    ).length;
    const confirmed = outcomes.flatMap((outcome) => outcome.confirmed);
    const assurance = ReviewAssurance.make({
      status: reasons.length === 0 ? "settled" : "incomplete",
      requiredGeneralDiscoveryPasses: plan.discoveryPasses.length - requiredSpecialistPasses,
      completedGeneralDiscoveryPasses: outcomes.reduce(
        (total, outcome) => total + outcome.completedGeneralPasses,
        0,
      ),
      requiredSpecialistPasses,
      completedSpecialistPasses: outcomes.reduce(
        (total, outcome) => total + outcome.completedSpecialistPasses,
        0,
      ),
      requiredVerificationPasses: outcomes.reduce(
        (total, outcome) => total + outcome.requiredVerificationPasses,
        0,
      ),
      completedVerificationPasses: outcomes.reduce(
        (total, outcome) => total + outcome.completedVerificationPasses,
        0,
      ),
      discoveredCandidates: outcomes.reduce(
        (total, outcome) => total + outcome.discoveredCandidates,
        0,
      ),
      confirmedCandidates: confirmed.length,
      rejectedCandidates: outcomes.reduce(
        (total, outcome) => total + outcome.rejectedCandidates,
        0,
      ),
      unsettledCandidates,
      discardedInvalidFindings: outcomes.reduce(
        (total, outcome) => total + outcome.discardedFindings,
        0,
      ),
      failedPasses,
      reasons,
    });
    const findings = rankAndDedupeFindings(
      confirmed.flatMap(({ assessment, candidate }) =>
        candidate._tag === "FindingCandidate"
          ? [confirmedFindingForPublication(assessment, candidate)]
          : [],
      ),
    );
    const concerns = rankAndDedupeConcerns(
      confirmed.flatMap(({ candidate }) =>
        candidate._tag === "ConcernCandidate" ? [candidate.concern] : [],
      ),
    );
    const walkthrough = outcomes.flatMap((outcome) => outcome.walkthrough);
    const blocking =
      findings.some((finding) => finding.severity === "blocking") ||
      concerns.some((concern) => concern.severity === "blocking");
    const review = CodeReview.make({
      summary: composeSummary(plan, assurance),
      verdict: blocking
        ? "request-changes"
        : findings.length > 0 || concerns.length > 0
          ? "comment"
          : "approve",
      findings,
      ...(concerns.length === 0 ? {} : { concerns }),
      ...(walkthrough.length === 0 ? {} : { walkthrough }),
    });
    return {
      review,
      assurance,
      plan,
      // Everything not fully reviewed this run and still part of the pull
      // request carries forward, so the baseline can advance without ever
      // moving unreviewed scope behind a green check: failed units retry,
      // whole overflow files review in later installments, and partial or
      // undiffable files keep the check fail-closed until they are reviewed,
      // removed, or explicitly ignored.
      unreviewedPaths: [
        ...new Set([
          ...outcomes.flatMap((outcome) => outcome.unreviewedPaths),
          ...plan.unassignedPaths,
          ...plan.partialEvidencePaths,
          ...plan.undiffablePaths,
        ]),
      ].sort(),
      turns: outcomes.reduce((total, outcome) => total + outcome.turns, 0),
    } satisfies FanOutPipelineOutcome;
  });

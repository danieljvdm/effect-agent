import { Effect, Layer, Schema } from "effect";
import {
  Agent,
  AgentPolicy,
  Subagent,
  SubagentPolicy,
  SubagentRuntime,
  ToolExecutionClass,
  ToolResultBounds,
  type RuntimeBinding,
} from "effect-agent";
import { Tool, Toolkit } from "effect/unstable/ai";

import { anchorViolation } from "./anchors.ts";
import { ChangedFileStatus, ChangedPath } from "./diff.ts";
import {
  clampMaxFindings,
  CodeReview,
  fileReviewEvidenceChunks,
  MAX_PATCH_CHARS,
  MAX_WALKTHROUGH_SUMMARY_CHARS,
  REVIEW_TOOL_RESULT_MAX_BYTES,
  ReviewConcern,
  ReviewFinding,
  ReviewMission,
  WalkthroughEntry,
} from "./review-agent.ts";
import {
  MAX_REVIEW_UNITS,
  MAX_UNIT_EVIDENCE_SHARDS,
  MAX_UNIT_FILES,
  findingAnchorInUnitEvidence,
  planReviewUnits,
  ReviewEvidenceShardId,
  ReviewPassId,
  ReviewRiskCategory,
  ReviewUnitId,
  ReviewUnitPlan,
} from "./review-units.ts";
import { PullRequestSource, PullRequestSourceFailure } from "./source.ts";

// ---------------------------------------------------------------------------
// The assured fan-out reviewer is a bounded, deterministic three-stage
// pipeline driven through attached S1 children:
//
//   host plan -> independent discovery passes -> independent verification
//
// Host code owns partitioning, risk classification, bounded evidence, exact
// pass settlement, candidate provenance, and the final confirmed-candidate
// fold. The coordinator only schedules the declared work and writes prose.
// ---------------------------------------------------------------------------

/** One discovery pass returns at most this many anchored candidates. */
export const MAX_CHILD_FINDINGS = 6;

/** One discovery pass returns at most this many non-anchored candidates. */
export const MAX_CHILD_CONCERNS = 3;

/** Every unit receives independent general and specialist discovery passes. */
export const MAX_UNIT_CANDIDATES = (MAX_CHILD_FINDINGS + MAX_CHILD_CONCERNS) * 2;

/** General + specialist discovery for every unit, then one verifier per unit. */
export const MAX_REVIEW_CHILDREN = MAX_REVIEW_UNITS * 3;

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

export class CandidateAssessment extends Schema.Class<CandidateAssessment>(
  "@effect-agent/pr-review/CandidateAssessment",
)({
  candidateId: ReviewCandidateId,
  disposition: Schema.Literals(["confirmed", "rejected"]),
  rationale: Schema.NonEmptyString.check(Schema.isMaxLength(600)),
}) {}

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

/** Strict-object coordinator request for either discovery or verification. */
export class FileReviewRequest extends Schema.Class<FileReviewRequest>(
  "@effect-agent/pr-review/FileReviewRequest",
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
}) {}

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

/** Bounded coordinator-visible result with host-assigned candidate IDs. */
export class FileReviewUnitResult extends Schema.Class<FileReviewUnitResult>(
  "@effect-agent/pr-review/FileReviewUnitResult",
)({
  phase: ReviewWorkPhase,
  workId: ReviewPassId,
  unitId: ReviewUnitId,
  candidates: Candidates,
  fileSummaries: Schema.Array(WalkthroughEntry).check(Schema.isMaxLength(MAX_UNIT_FILES)),
  assessments: Schema.Array(CandidateAssessment).check(Schema.isMaxLength(MAX_UNIT_CANDIDATES)),
}) {}

export class FileReviewUnitFailed extends Schema.TaggedError<FileReviewUnitFailed>()(
  "FileReviewUnitFailed",
  {
    childErrorTag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
    message: Schema.String.check(Schema.isMaxLength(400)),
  },
) {}

export class FileReviewWorkRejected extends Schema.TaggedError<FileReviewWorkRejected>()(
  "FileReviewWorkRejected",
  {
    workId: ReviewPassId,
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(600)),
  },
) {}

export const FileReviewFailure = Schema.Union([FileReviewUnitFailed, FileReviewWorkRejected]);

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
        'Return ONLY JSON with phase "verification", the exact workId/unitId, empty findings/concerns/fileSummaries arrays, and exactly one assessment per candidateId. Each assessment is {"candidateId": <exact id>, "disposition": <"confirmed" | "rejected">, "rationale": <bounded evidence-based reason>}. Never add or omit an id.',
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
    ].join("\n");
  };

export const fileReviewerInstructions = makeFileReviewerInstructions();

export const FileReviewToolkit = Toolkit.empty;

/** Compatibility export: the evidence-only child has no handler requirements. */
export const FileReviewToolkitLayer = Layer.empty;

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

export const fileReviewPolicy = SubagentPolicy.make({
  maxChildren: MAX_REVIEW_CHILDREN,
  maxConcurrency: 4,
  maxTurns: 6,
  maxToolCalls: MAX_FILE_REVIEW_TOOL_CALLS,
  maxDuration: "6 minutes",
  maxResultBytes: 256 * 1024,
});

export const mapFileReviewChildFailure = (failure: {
  readonly _tag: string;
  readonly message?: string;
}): FileReviewUnitFailed =>
  FileReviewUnitFailed.make({
    childErrorTag: failure._tag,
    message: (failure.message ?? "").slice(0, 400),
  });

const sameStrings = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean =>
  left.length === right.length && left.every((value, index) => value === right[index]);

const rejectWork = (workId: string, reason: string) =>
  FileReviewWorkRejected.make({ workId, reason });

/** Validate coordinator scheduling against the current deterministic plan. */
const prepareReviewBrief = (request: FileReviewRequest) =>
  Effect.gen(function* () {
    const source = yield* PullRequestSource;
    const mapSourceFailure = (failure: PullRequestSourceFailure) =>
      rejectWork(
        request.workId,
        `pull-request source ${failure.operation} failed: ${failure.reason}`.slice(0, 600),
      );
    const files = yield* source.changedFiles.pipe(Effect.mapError(mapSourceFailure));
    const metadata = yield* source.metadata.pipe(Effect.mapError(mapSourceFailure));
    const plan = planReviewUnits(files, { totalChangedFiles: metadata.totalChangedFiles });
    const unit = plan.units.find((candidate) => candidate.unitId === request.unitId);
    if (
      unit === undefined ||
      !sameStrings(request.paths, unit.paths) ||
      !sameStrings(
        request.evidenceShardIds,
        unit.evidenceShards.map((shard) => shard.shardId),
      )
    ) {
      return yield* rejectWork(request.workId, "request does not match a host-planned unit");
    }

    if (request.phase === "discovery") {
      const pass = plan.discoveryPasses.find((candidate) => candidate.passId === request.workId);
      if (
        pass === undefined ||
        pass.unitId !== request.unitId ||
        !sameStrings(pass.paths, request.paths) ||
        !sameStrings(pass.evidenceShardIds, request.evidenceShardIds) ||
        pass.perspective !== request.perspective ||
        !sameStrings(pass.riskCategories, request.riskCategories) ||
        request.candidates.length !== 0
      ) {
        return yield* rejectWork(request.workId, "discovery request does not match the host plan");
      }
    } else {
      if (
        request.workId !== `${request.unitId}-verification` ||
        request.perspective !== "candidate-verification" ||
        !sameStrings(request.riskCategories, unit.riskCategories) ||
        request.candidates.length === 0
      ) {
        return yield* rejectWork(
          request.workId,
          "verification request does not match the host-planned unit",
        );
      }
      const candidateIds = new Set<string>();
      const allowed = new Set(unit.paths);
      for (const candidate of request.candidates) {
        if (
          candidateIds.has(candidate.candidateId) ||
          candidate.unitId !== unit.unitId ||
          candidate.evidencePaths.some((path) => !allowed.has(path)) ||
          (candidate._tag === "FindingCandidate" && !allowed.has(candidate.finding.path))
        ) {
          return yield* rejectWork(
            request.workId,
            "verification candidates are duplicated or outside the planned unit",
          );
        }
        candidateIds.add(candidate.candidateId);
      }
    }

    const byPath = new Map(files.map((file) => [file.path, file] as const));
    const evidence: Array<FileReviewEvidence> = [];
    for (const shard of unit.evidenceShards) {
      const file = byPath.get(shard.path);
      if (file === undefined) {
        return yield* rejectWork(
          request.workId,
          `planned evidence path is unavailable: ${shard.path}`,
        );
      }
      const chunks = fileReviewEvidenceChunks(file);
      const chunk = chunks[shard.ordinal - 1];
      if (
        chunk === undefined ||
        chunks.length !== shard.total ||
        chunk.annotatedPatch.length !== shard.evidenceChars
      ) {
        return yield* rejectWork(
          request.workId,
          `planned evidence shard no longer matches source: ${shard.shardId}`,
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
    return FileReviewBrief.make({ ...request, evidence });
  });

const candidateOrdinal = (index: number): string => String(index + 1).padStart(3, "0");

const projectReviewResult = (
  report: FileReviewReport,
  context: { readonly budgetExhausted: boolean },
  request: FileReviewRequest,
) => {
  if (context.budgetExhausted) {
    return Effect.fail(
      rejectWork(report.workId, "review work exhausted its budget before exact settlement"),
    );
  }
  if (
    report.phase !== request.phase ||
    report.workId !== request.workId ||
    report.unitId !== request.unitId
  ) {
    return Effect.fail(
      rejectWork(request.workId, "review output identity does not match the scheduled request"),
    );
  }
  if (report.phase === "verification") {
    if (
      report.findings.length > 0 ||
      report.concerns.length > 0 ||
      report.fileSummaries.length > 0
    ) {
      return Effect.fail(
        rejectWork(report.workId, "verification output contained discovery-only fields"),
      );
    }
    const expectedIds = new Set(request.candidates.map((candidate) => candidate.candidateId));
    const assessedIds = new Set<string>();
    for (const assessment of report.assessments) {
      if (!expectedIds.has(assessment.candidateId) || assessedIds.has(assessment.candidateId)) {
        return Effect.fail(
          rejectWork(report.workId, "verification output did not assess the exact candidate set"),
        );
      }
      assessedIds.add(assessment.candidateId);
    }
    if (assessedIds.size !== expectedIds.size) {
      return Effect.fail(
        rejectWork(report.workId, "verification output did not assess the exact candidate set"),
      );
    }
    return Effect.succeed(
      FileReviewUnitResult.make({
        phase: report.phase,
        workId: report.workId,
        unitId: report.unitId,
        candidates: [],
        fileSummaries: [],
        assessments: report.assessments,
      }),
    );
  }
  if (report.assessments.length > 0) {
    return Effect.fail(
      rejectWork(report.workId, "discovery output contained verification-only assessments"),
    );
  }
  const allowed = new Set(request.paths);
  if (
    report.findings.some((finding) => !allowed.has(finding.path)) ||
    report.concerns.some((candidate) =>
      candidate.evidencePaths.some((path) => !allowed.has(path)),
    ) ||
    report.fileSummaries.some((entry) => !allowed.has(entry.path))
  ) {
    return Effect.fail(
      rejectWork(report.workId, "discovery output referenced evidence outside the scheduled unit"),
    );
  }
  return Effect.gen(function* () {
    const source = yield* PullRequestSource;
    const mapSourceFailure = (failure: PullRequestSourceFailure) =>
      rejectWork(
        request.workId,
        `pull-request source ${failure.operation} failed: ${failure.reason}`.slice(0, 600),
      );
    const files = yield* source.changedFiles.pipe(Effect.mapError(mapSourceFailure));
    const metadata = yield* source.metadata.pipe(Effect.mapError(mapSourceFailure));
    const anchorFiles = yield* source.anchorFiles.pipe(Effect.mapError(mapSourceFailure));
    const unit = planReviewUnits(files, {
      totalChangedFiles: metadata.totalChangedFiles,
    }).units.find((candidate) => candidate.unitId === request.unitId);
    if (unit === undefined) {
      return yield* rejectWork(request.workId, "scheduled review unit is no longer available");
    }
    for (const finding of report.findings) {
      const violation = anchorViolation(finding, anchorFiles);
      if (violation !== undefined || !findingAnchorInUnitEvidence(finding, unit, files)) {
        return yield* rejectWork(
          request.workId,
          `discovery finding has no valid anchor in its assigned evidence: ${violation ?? finding.path}`,
        );
      }
    }
    const findingCandidates = report.findings.map((finding, index) =>
      FindingCandidate.make({
        candidateId: `${request.workId}:finding:${candidateOrdinal(index)}`,
        workId: request.workId,
        unitId: request.unitId,
        finding,
        evidencePaths: [finding.path],
      }),
    );
    const concernCandidates = report.concerns.map((candidate, index) =>
      ConcernCandidate.make({
        candidateId: `${request.workId}:concern:${candidateOrdinal(index)}`,
        workId: request.workId,
        unitId: request.unitId,
        concern: candidate.concern,
        evidencePaths: candidate.evidencePaths,
      }),
    );
    return FileReviewUnitResult.make({
      phase: report.phase,
      workId: report.workId,
      unitId: report.unitId,
      candidates: [...findingCandidates, ...concernCandidates],
      fileSummaries: report.fileSummaries,
      assessments: [],
    });
  });
};

const delegationDescription =
  "Run exactly one host-planned discovery or candidate-verification child. Copy every plan field and candidate verbatim; never retry failed work.";

const makeFileReviewDelegation = (child: ReturnType<typeof makeFileReviewerDefinition>) =>
  Subagent.define("delegate_file_review", {
    description: delegationDescription,
    target: child,
    parameters: FileReviewRequest,
    success: FileReviewUnitResult,
    failure: FileReviewFailure,
    failureMode: "return",
    prepareInput: prepareReviewBrief,
    projectResult: projectReviewResult,
    policy: fileReviewPolicy,
  });

export class ListReviewUnitsQuery extends Schema.Class<ListReviewUnitsQuery>(
  "@effect-agent/pr-review/ListReviewUnitsQuery",
)({
  scope: Schema.Literal("all"),
}) {}

export const ListReviewUnits = Tool.make("list_review_units", {
  description:
    "List deterministic bounded review units, explicit risk categories, every required discovery pass, and paths the pipeline cannot cover.",
  parameters: ListReviewUnitsQuery,
  success: ReviewUnitPlan,
  failure: PullRequestSourceFailure,
  failureMode: "error",
  dependencies: [PullRequestSource],
}).annotate(ToolExecutionClass, "readonly");

export const FanOutCoordinatorToolkit = Toolkit.make(ListReviewUnits);

export const FanOutCoordinatorToolkitLayer = FanOutCoordinatorToolkit.toLayer({
  list_review_units: () =>
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      const files = yield* source.changedFiles;
      const metadata = yield* source.metadata;
      return planReviewUnits(files, { totalChangedFiles: metadata.totalChangedFiles });
    }),
});

export const makeFanOutReviewInstructions =
  (options: FanOutInstructionOptions & { readonly maxFindings?: number | undefined } = {}) =>
  (mission: ReviewMission): string => {
    const maxFindings = clampMaxFindings(options.maxFindings);
    return [
      `You coordinate the bounded multi-pass review of pull request #${mission.number} ("${mission.title}") in ${mission.repository}.`,
      mission.body.length > 0 ? `Author description:\n${mission.body}` : "No author description.",
      ...staticGuidanceLines(options.guidance),
      "1. Call list_review_units exactly once.",
      '2. For EVERY discoveryPass, call delegate_file_review exactly once with phase "discovery", workId=passId, and the pass unitId/paths/evidenceShardIds/perspective/riskCategories verbatim; candidates must be []. Prefer one bounded parallel batch. Never retry.',
      '3. Group candidates returned by all successful discovery passes by unit. For every unit with at least one candidate, call delegate_file_review exactly once with phase "verification", workId "<unitId>-verification", perspective "candidate-verification", the unit paths/evidenceShardIds/riskCategories, and EVERY candidate copied byte-for-byte. Prefer one bounded parallel batch. Never retry.',
      "4. Verification is authoritative: rejected candidates must not be reported. The host independently reconstructs publishable findings from exact confirmed assessments, so do not select, rewrite, downgrade, or invent findings.",
      `5. Return ONLY CodeReview JSON. Write a concise summary of completed and failed stages. Set findings=[] and concerns=[]; the host injects exact confirmed candidates. Copy factual fileSummaries into walkthrough without invention. The host publication cap is ${maxFindings}.`,
      "No configured pipeline can prove absence of defects. Describe settled work, never an exhaustive or defect-free review.",
    ].join("\n");
  };

export const fanOutReviewInstructions = makeFanOutReviewInstructions();

export const defaultFanOutPolicy = AgentPolicy.make({
  maxTurns: 7,
  maxToolCalls: 1 + MAX_REVIEW_CHILDREN,
  maxDuration: "20 minutes",
  toolConcurrency: 4,
  repeatedFailureLimit: 3,
  tokenBudget: 400_000,
  contextTokenLimit: 150_000,
  // Coordinator exhaustion cannot become an assured result; exact stage
  // settlement, not this final prose, determines assurance.
  onExhaustion: "final-answer",
});

export interface FanOutReviewSuite {
  readonly child: ReturnType<typeof makeFileReviewerDefinition>;
  readonly parent: ReturnType<typeof makeFanOutReviewerDefinition>;
  readonly delegation: ReturnType<typeof makeFileReviewDelegation>;
}

const makeFileReviewerDefinition = (options: FanOutInstructionOptions = {}) =>
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

const delegationToolFor = (delegation: ReturnType<typeof makeFileReviewDelegation>) =>
  delegation.tool.annotate(ToolExecutionClass, "readonly");

const makeFanOutReviewerDefinition = (
  options: FanOutInstructionOptions & { readonly maxFindings?: number | undefined },
  delegation: ReturnType<typeof makeFileReviewDelegation>,
) =>
  Agent.define("pr-fanout-reviewer", {
    input: ReviewMission,
    output: CodeReview,
    instructions: makeFanOutReviewInstructions(options),
    toolkit: Toolkit.make(ListReviewUnits, delegationToolFor(delegation)),
    policy: defaultFanOutPolicy,
    description:
      "Coordinate deterministic general/specialist discovery and independent candidate verification over bounded review units.",
    metadata: {
      deploymentClass: "E",
      surface: "read-only",
      delegation: "S1-attached",
      assurance: "multi-pass",
    },
  });

export interface FanOutSuiteOptions extends FanOutInstructionOptions {
  readonly maxFindings?: number | undefined;
}

export const makeFanOutReviewSuite = (options: FanOutSuiteOptions = {}): FanOutReviewSuite => {
  const child = makeFileReviewerDefinition({ guidance: options.guidance });
  const delegation = makeFileReviewDelegation(child);
  return {
    child,
    parent: makeFanOutReviewerDefinition(options, delegation),
    delegation,
  };
};

const defaultSuite = makeFanOutReviewSuite();

export const FileReviewer = defaultSuite.child;
export const FanOutReviewer = defaultSuite.parent;
export const fileReviewDelegation = defaultSuite.delegation;
export const DelegateFileReview = delegationToolFor(fileReviewDelegation);
export const FanOutReviewToolkit = FanOutReviewer.toolkit;
export const FileReviewDelegationFailure = fileReviewDelegation.containedFailure;

export const fanOutHandlersLayerFor =
  (delegation: ReturnType<typeof makeFileReviewDelegation>) =>
  <Provider, ModelProvides, ModelRequires>(
    childBinding: RuntimeBinding<
      typeof FileReviewBrief,
      typeof FileReviewReport,
      ReturnType<typeof makeFileReviewerInstructions>,
      Toolkit.Tools<typeof FileReviewToolkit>,
      Provider,
      ModelProvides,
      ModelRequires
    >,
  ) =>
    SubagentRuntime.layer(delegation, childBinding, {
      mapChildFailure: mapFileReviewChildFailure,
    });

export const fanOutHandlersLayer = fanOutHandlersLayerFor(fileReviewDelegation);

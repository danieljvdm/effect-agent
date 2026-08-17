import { Option, Schema } from "effect";
import type { RunEvent } from "effect-agent";

import type { ChangedFile } from "./diff.ts";
import { isReviewableFile } from "./diff.ts";
import {
  FileReviewDelegationFailure,
  FileReviewRequest,
  FileReviewUnitResult,
  ReviewCandidate,
} from "./fan-out.ts";
import {
  FileDiffView,
  FileDiffQuery,
  type ReviewConcern,
  type ReviewFinding,
  type WalkthroughEntry,
} from "./review-agent.ts";
import { planReviewUnits, type ReviewDiscoveryPass } from "./review-units.ts";

// ---------------------------------------------------------------------------
// Two different claims are deliberately modeled:
//
// - input coverage: every required path was assigned bounded evidence or was
//   explicitly reported outside the pipeline's capacity;
// - review assurance: every configured discovery/specialist pass and every
//   candidate-verification batch settled exactly.
//
// Neither claims that the model found every defect.
// ---------------------------------------------------------------------------

export const ReviewShape = Schema.Literals(["flat", "fan-out"]);
export type ReviewShape = typeof ReviewShape.Type;

export class FailedReviewUnit extends Schema.Class<FailedReviewUnit>(
  "@effect-agent/pr-review/FailedReviewUnit",
)({
  unitId: Schema.NonEmptyString.check(Schema.isMaxLength(32)),
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
}) {}

/**
 * Compatibility diagnostic retained for callers that consumed the original
 * `coverage` field. New UI and state decisions use ReviewInputCoverage and
 * ReviewAssurance directly.
 */
export class ReviewCoverage extends Schema.Class<ReviewCoverage>(
  "@effect-agent/pr-review/ReviewCoverage",
)({
  status: Schema.Literals(["complete", "incomplete"]),
  requiredPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  reviewedPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  unreviewedPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  failedUnits: Schema.Array(FailedReviewUnit).check(Schema.isMaxLength(8)),
  reasons: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(1_000))).check(
    Schema.isMaxLength(32),
  ),
}) {}

export class ReviewInputCoverage extends Schema.Class<ReviewInputCoverage>(
  "@effect-agent/pr-review/ReviewInputCoverage",
)({
  status: Schema.Literals(["complete", "incomplete"]),
  requiredPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  assignedPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  /** Assigned paths whose model-visible diff was truncated by the evidence bound. */
  partialPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  unassignedPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  reasons: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(1_000))).check(
    Schema.isMaxLength(20),
  ),
}) {}

export class FailedReviewPass extends Schema.Class<FailedReviewPass>(
  "@effect-agent/pr-review/FailedReviewPass",
)({
  workId: Schema.NonEmptyString.check(Schema.isMaxLength(96)),
  stage: Schema.Literals(["discovery", "specialist", "verification"]),
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
}) {}

export class ReviewAssurance extends Schema.Class<ReviewAssurance>(
  "@effect-agent/pr-review/ReviewAssurance",
)({
  status: Schema.Literals(["settled", "incomplete", "unverified"]),
  requiredGeneralDiscoveryPasses: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  completedGeneralDiscoveryPasses: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  requiredSpecialistPasses: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  completedSpecialistPasses: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  requiredVerificationPasses: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  completedVerificationPasses: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  discoveredCandidates: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  confirmedCandidates: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  rejectedCandidates: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  unsettledCandidates: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Every failure remains visible within the coordinator's 32-call hard bound. */
  failedPasses: Schema.Array(FailedReviewPass).check(Schema.isMaxLength(64)),
  reasons: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(1_000))).check(
    Schema.isMaxLength(32),
  ),
}) {}

interface ToolTrace {
  readonly declared: Map<string, Extract<RunEvent, { readonly _tag: "ToolCallDeclared" }>>;
  readonly succeeded: Map<string, Extract<RunEvent, { readonly _tag: "ToolCallSucceeded" }>>;
  readonly failed: Map<string, Extract<RunEvent, { readonly _tag: "ToolCallFailed" }>>;
}

const toolTrace = (events: ReadonlyArray<RunEvent>): ToolTrace => {
  const declared = new Map<string, Extract<RunEvent, { readonly _tag: "ToolCallDeclared" }>>();
  const succeeded = new Map<string, Extract<RunEvent, { readonly _tag: "ToolCallSucceeded" }>>();
  const failed = new Map<string, Extract<RunEvent, { readonly _tag: "ToolCallFailed" }>>();
  for (const event of events) {
    if (event._tag === "ToolCallDeclared") declared.set(event.toolCallId, event);
    if (event._tag === "ToolCallSucceeded") succeeded.set(event.toolCallId, event);
    if (event._tag === "ToolCallFailed") failed.set(event.toolCallId, event);
  }
  return { declared, succeeded, failed };
};

const sortedUnique = (values: Iterable<string>): ReadonlyArray<string> =>
  [...new Set(values)].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));

const boundedListReason = (label: string, values: Iterable<string>): string => {
  const items = sortedUnique(values);
  const prefix = `${label} (${items.length}): `;
  let rendered = prefix;
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index] ?? "";
    const separator = index === 0 ? "" : ", ";
    const omitted = items.length - index - 1;
    const suffix = omitted === 0 ? "" : ` … (+${omitted} more)`;
    if (`${rendered}${separator}${item}${suffix}`.length > 1_000) {
      const omission = `… (+${items.length - index} more)`;
      return `${rendered.slice(0, 1_000 - omission.length)}${omission}`;
    }
    rendered = `${rendered}${separator}${item}`;
  }
  return rendered;
};

const flatInputCoverage = (
  files: ReadonlyArray<ChangedFile>,
  totalFiles: number,
  trace: ToolTrace,
): ReviewInputCoverage => {
  const requiredPaths = sortedUnique(files.map((file) => file.path));
  const assigned = new Set<string>();
  const partial = new Set<string>();
  const failedPaths = new Set<string>();
  for (const [toolCallId, declaration] of trace.declared) {
    if (declaration.toolName !== "read_file_diff") continue;
    const query = Schema.decodeUnknownOption(FileDiffQuery)(declaration.parameters);
    if (Option.isNone(query)) continue;
    const success = trace.succeeded.get(toolCallId);
    if (success !== undefined) {
      assigned.add(query.value.path);
      const view = Schema.decodeUnknownOption(FileDiffView)(success.result);
      if (Option.isSome(view) && view.value.truncated) partial.add(query.value.path);
    }
    if (trace.failed.has(toolCallId)) failedPaths.add(query.value.path);
  }
  const undiffable = files.filter((file) => !isReviewableFile(file)).map((file) => file.path);
  const unassigned = requiredPaths.filter(
    (path) => !assigned.has(path) || undiffable.includes(path) || failedPaths.has(path),
  );
  const reasons: Array<string> = [];
  if (files.length < totalFiles) {
    reasons.push(`review range exposed ${files.length} of ${totalFiles} required files`);
  }
  if (undiffable.length > 0) {
    reasons.push(
      boundedListReason("required paths have no reviewable diff or bounded text", undiffable),
    );
  }
  if (failedPaths.size > 0) reasons.push(boundedListReason("diff reads failed", failedPaths));
  if (partial.size > 0) {
    reasons.push(boundedListReason("model-visible diff evidence was truncated", partial));
  }
  if (unassigned.length > 0) {
    reasons.push(boundedListReason("required paths received no successful diff input", unassigned));
  }
  return ReviewInputCoverage.make({
    status: reasons.length === 0 ? "complete" : "incomplete",
    requiredPaths,
    assignedPaths: sortedUnique(assigned),
    partialPaths: sortedUnique(partial),
    unassignedPaths: sortedUnique(unassigned),
    reasons,
  });
};

const fanOutInputCoverage = (
  files: ReadonlyArray<ChangedFile>,
  totalFiles: number,
): ReviewInputCoverage => {
  const plan = planReviewUnits(files, { totalChangedFiles: totalFiles });
  const assignedPaths = sortedUnique(plan.units.flatMap((unit) => unit.paths));
  const unassignedPaths = sortedUnique([...plan.undiffablePaths, ...plan.unassignedPaths]);
  const reasons: Array<string> = [];
  if (plan.truncated) {
    reasons.push(`review range exposed ${files.length} of ${totalFiles} required files`);
  }
  if (plan.undiffablePaths.length > 0) {
    reasons.push(
      boundedListReason(
        "required paths have no reviewable diff or bounded text",
        plan.undiffablePaths,
      ),
    );
  }
  if (plan.partialEvidencePaths.length > 0) {
    reasons.push(
      boundedListReason("model-visible diff evidence was truncated", plan.partialEvidencePaths),
    );
  }
  if (plan.unassignedPaths.length > 0) {
    reasons.push(boundedListReason("fan-out capacity left paths unassigned", plan.unassignedPaths));
  }
  return ReviewInputCoverage.make({
    status: reasons.length === 0 ? "complete" : "incomplete",
    requiredPaths: sortedUnique(files.map((file) => file.path)),
    assignedPaths,
    partialPaths: plan.partialEvidencePaths,
    unassignedPaths,
    reasons,
  });
};

const sameStrings = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean =>
  left.length === right.length && left.every((value, index) => value === right[index]);

const candidateKey = (candidate: ReviewCandidate): string =>
  JSON.stringify(Schema.encodeSync(ReviewCandidate)(candidate));

const sameCandidates = (
  left: ReadonlyArray<ReviewCandidate>,
  right: ReadonlyArray<ReviewCandidate>,
): boolean =>
  left.length === right.length &&
  left.every((candidate, index) => {
    const corresponding = right[index];
    return corresponding !== undefined && candidateKey(candidate) === candidateKey(corresponding);
  });

const delegationDeclarations = (trace: ToolTrace) =>
  [...trace.declared].flatMap(([id, declaration]) => {
    if (declaration.toolName !== "delegate_file_review") return [];
    const request = Schema.decodeUnknownOption(FileReviewRequest)(declaration.parameters);
    return Option.isNone(request) ? [] : [{ id, request: request.value }];
  });

const failureTag = (trace: ToolTrace, id: string): string | undefined => {
  const failed = trace.failed.get(id);
  if (failed !== undefined) return failed.errorTag;
  const succeeded = trace.succeeded.get(id);
  if (succeeded === undefined) return undefined;
  const returned = Schema.decodeUnknownOption(FileReviewDelegationFailure)(succeeded.result);
  if (Option.isNone(returned)) return undefined;
  return returned.value._tag === "FileReviewUnitFailed"
    ? `${returned.value._tag}:${returned.value.childErrorTag}`
    : returned.value._tag;
};

const exactDiscoveryRequest = (request: FileReviewRequest, pass: ReviewDiscoveryPass): boolean =>
  request.phase === "discovery" &&
  request.workId === pass.passId &&
  request.unitId === pass.unitId &&
  request.perspective === pass.perspective &&
  sameStrings(request.paths, pass.paths) &&
  sameStrings(request.riskCategories, pass.riskCategories) &&
  request.candidates.length === 0;

const validCandidate = (candidate: ReviewCandidate, pass: ReviewDiscoveryPass): boolean => {
  const allowed = new Set(pass.paths);
  return (
    candidate.workId === pass.passId &&
    candidate.unitId === pass.unitId &&
    candidate.evidencePaths.length > 0 &&
    candidate.evidencePaths.every((path) => allowed.has(path)) &&
    (candidate._tag !== "FindingCandidate" || allowed.has(candidate.finding.path))
  );
};

interface AssuranceAssessment {
  readonly assurance: ReviewAssurance;
  readonly confirmedFindings: ReadonlyArray<ReviewFinding>;
  readonly confirmedConcerns: ReadonlyArray<ReviewConcern>;
  readonly walkthrough: ReadonlyArray<WalkthroughEntry>;
}

const flatAssurance = (): AssuranceAssessment => ({
  assurance: ReviewAssurance.make({
    status: "unverified",
    requiredGeneralDiscoveryPasses: 1,
    completedGeneralDiscoveryPasses: 1,
    requiredSpecialistPasses: 0,
    completedSpecialistPasses: 0,
    requiredVerificationPasses: 1,
    completedVerificationPasses: 0,
    discoveredCandidates: 0,
    confirmedCandidates: 0,
    rejectedCandidates: 0,
    unsettledCandidates: 0,
    failedPasses: [],
    reasons: [
      "flat review has no independent candidate-verification pass; use the fan-out pipeline for a settled assurance result",
    ],
  }),
  confirmedFindings: [],
  confirmedConcerns: [],
  walkthrough: [],
});

const fanOutAssurance = (
  files: ReadonlyArray<ChangedFile>,
  totalFiles: number,
  trace: ToolTrace,
): AssuranceAssessment => {
  const plan = planReviewUnits(files, { totalChangedFiles: totalFiles });
  const declarations = delegationDeclarations(trace);
  const expectedWorkIds = new Set(plan.discoveryPasses.map((pass) => pass.passId));
  const failedPasses: Array<FailedReviewPass> = [];
  const reasons: Array<string> = [];
  const candidatesByUnit = new Map<string, Array<ReviewCandidate>>();
  const walkthrough: Array<WalkthroughEntry> = [];
  let completedGeneralDiscoveryPasses = 0;
  let completedSpecialistPasses = 0;

  for (const pass of plan.discoveryPasses) {
    const matching = declarations.filter(({ request }) => exactDiscoveryRequest(request, pass));
    const stage = pass.perspective === "risk-specialist" ? "specialist" : "discovery";
    if (matching.length !== 1) {
      failedPasses.push(
        FailedReviewPass.make({
          workId: pass.passId,
          stage,
          errorTag: matching.length === 0 ? "PassNotAssigned" : "PassAssignedMultipleTimes",
        }),
      );
      continue;
    }
    const call = matching[0];
    if (call === undefined) {
      failedPasses.push(
        FailedReviewPass.make({
          workId: pass.passId,
          stage,
          errorTag: "PassLookupInvariantFailed",
        }),
      );
      continue;
    }
    const failure = failureTag(trace, call.id);
    const succeeded = trace.succeeded.get(call.id);
    const result =
      succeeded === undefined
        ? Option.none()
        : Schema.decodeUnknownOption(FileReviewUnitResult)(succeeded.result);
    if (
      failure !== undefined ||
      Option.isNone(result) ||
      result.value.phase !== "discovery" ||
      result.value.workId !== pass.passId ||
      result.value.unitId !== pass.unitId ||
      result.value.assessments.length !== 0
    ) {
      failedPasses.push(
        FailedReviewPass.make({
          workId: pass.passId,
          stage,
          errorTag: failure ?? "DiscoveryDidNotSettleExactly",
        }),
      );
      continue;
    }
    const ids = new Set<string>();
    let candidatesValid = true;
    for (const candidate of result.value.candidates) {
      if (ids.has(candidate.candidateId) || !validCandidate(candidate, pass)) {
        candidatesValid = false;
        break;
      }
      ids.add(candidate.candidateId);
    }
    if (!candidatesValid) {
      failedPasses.push(
        FailedReviewPass.make({
          workId: pass.passId,
          stage,
          errorTag: "DiscoveryCandidateMismatch",
        }),
      );
      continue;
    }
    if (stage === "specialist") {
      completedSpecialistPasses += 1;
    } else {
      completedGeneralDiscoveryPasses += 1;
    }
    const unitCandidates = candidatesByUnit.get(pass.unitId) ?? [];
    unitCandidates.push(...result.value.candidates);
    candidatesByUnit.set(pass.unitId, unitCandidates);
    if (pass.perspective === "general") {
      const allowed = new Set(pass.paths);
      walkthrough.push(...result.value.fileSummaries.filter((entry) => allowed.has(entry.path)));
    }
  }

  const confirmedCandidates: Array<ReviewCandidate> = [];
  let rejectedCandidates = 0;
  let unsettledCandidates = 0;
  let requiredVerificationPasses = 0;
  let completedVerificationPasses = 0;
  for (const unit of plan.units) {
    const candidates = candidatesByUnit.get(unit.unitId) ?? [];
    if (candidates.length === 0) continue;
    requiredVerificationPasses += 1;
    const workId = `${unit.unitId}-verification`;
    expectedWorkIds.add(workId);
    const matching = declarations.filter(
      ({ request }) =>
        request.phase === "verification" &&
        request.workId === workId &&
        request.unitId === unit.unitId &&
        request.perspective === "candidate-verification" &&
        sameStrings(request.paths, unit.paths) &&
        sameStrings(request.riskCategories, unit.riskCategories) &&
        sameCandidates(request.candidates, candidates),
    );
    if (matching.length !== 1) {
      unsettledCandidates += candidates.length;
      failedPasses.push(
        FailedReviewPass.make({
          workId,
          stage: "verification",
          errorTag:
            matching.length === 0
              ? "VerificationNotAssignedOrCandidateMismatch"
              : "VerificationAssignedMultipleTimes",
        }),
      );
      continue;
    }
    const call = matching[0];
    if (call === undefined) {
      unsettledCandidates += candidates.length;
      failedPasses.push(
        FailedReviewPass.make({
          workId,
          stage: "verification",
          errorTag: "PassLookupInvariantFailed",
        }),
      );
      continue;
    }
    const failure = failureTag(trace, call.id);
    const succeeded = trace.succeeded.get(call.id);
    const result =
      succeeded === undefined
        ? Option.none()
        : Schema.decodeUnknownOption(FileReviewUnitResult)(succeeded.result);
    if (
      failure !== undefined ||
      Option.isNone(result) ||
      result.value.phase !== "verification" ||
      result.value.workId !== workId ||
      result.value.unitId !== unit.unitId ||
      result.value.candidates.length !== 0 ||
      result.value.fileSummaries.length !== 0
    ) {
      unsettledCandidates += candidates.length;
      failedPasses.push(
        FailedReviewPass.make({
          workId,
          stage: "verification",
          errorTag: failure ?? "VerificationDidNotSettleExactly",
        }),
      );
      continue;
    }
    const expectedIds = new Set(candidates.map((candidate) => candidate.candidateId));
    const assessedIds = new Set<string>();
    const exactAssessments = result.value.assessments.every((assessment) => {
      if (!expectedIds.has(assessment.candidateId) || assessedIds.has(assessment.candidateId)) {
        return false;
      }
      assessedIds.add(assessment.candidateId);
      return true;
    });
    if (!exactAssessments || assessedIds.size !== expectedIds.size) {
      unsettledCandidates += candidates.length;
      failedPasses.push(
        FailedReviewPass.make({
          workId,
          stage: "verification",
          errorTag: "VerificationAssessmentMismatch",
        }),
      );
      continue;
    }
    completedVerificationPasses += 1;
    const byId = new Map(
      candidates.map((candidate) => [candidate.candidateId, candidate] as const),
    );
    for (const assessment of result.value.assessments) {
      if (assessment.disposition === "confirmed") {
        const candidate = byId.get(assessment.candidateId);
        if (candidate !== undefined) confirmedCandidates.push(candidate);
      } else {
        rejectedCandidates += 1;
      }
    }
  }

  const unexpected = declarations.filter(({ request }) => !expectedWorkIds.has(request.workId));
  for (const declaration of unexpected) {
    failedPasses.push(
      FailedReviewPass.make({
        workId: declaration.request.workId,
        stage: declaration.request.phase === "verification" ? "verification" : "discovery",
        errorTag: "UnexpectedPass",
      }),
    );
  }
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
  const requiredGeneralDiscoveryPasses = plan.discoveryPasses.length - requiredSpecialistPasses;
  const discoveredCandidates = [...candidatesByUnit.values()].reduce(
    (total, candidates) => total + candidates.length,
    0,
  );
  return {
    assurance: ReviewAssurance.make({
      status: reasons.length === 0 ? "settled" : "incomplete",
      requiredGeneralDiscoveryPasses,
      completedGeneralDiscoveryPasses,
      requiredSpecialistPasses,
      completedSpecialistPasses,
      requiredVerificationPasses,
      completedVerificationPasses,
      discoveredCandidates,
      confirmedCandidates: confirmedCandidates.length,
      rejectedCandidates,
      unsettledCandidates,
      failedPasses,
      reasons,
    }),
    confirmedFindings: confirmedCandidates.flatMap((candidate) =>
      candidate._tag === "FindingCandidate" ? [candidate.finding] : [],
    ),
    confirmedConcerns: confirmedCandidates.flatMap((candidate) =>
      candidate._tag === "ConcernCandidate" ? [candidate.concern] : [],
    ),
    walkthrough,
  };
};

const compatibilityCoverage = (
  inputCoverage: ReviewInputCoverage,
  assurance: ReviewAssurance,
): ReviewCoverage => {
  const assuranceIncomplete = assurance.status === "incomplete";
  const failedUnits = new Map<string, FailedReviewUnit>();
  for (const pass of assurance.failedPasses) {
    const unitId = pass.workId.slice(0, "unit-000".length);
    if (!failedUnits.has(unitId)) {
      failedUnits.set(
        unitId,
        FailedReviewUnit.make({ unitId, errorTag: `${pass.stage}:${pass.errorTag}` }),
      );
    }
  }
  return ReviewCoverage.make({
    status: inputCoverage.status === "complete" && !assuranceIncomplete ? "complete" : "incomplete",
    requiredPaths: inputCoverage.requiredPaths,
    reviewedPaths: inputCoverage.assignedPaths,
    unreviewedPaths: sortedUnique([
      ...inputCoverage.partialPaths,
      ...inputCoverage.unassignedPaths,
    ]),
    failedUnits: [...failedUnits.values()].slice(0, 8),
    reasons: [...inputCoverage.reasons, ...(assuranceIncomplete ? assurance.reasons : [])],
  });
};

export interface ReviewPipelineAssessment {
  readonly inputCoverage: ReviewInputCoverage;
  readonly assurance: ReviewAssurance;
  /** Deprecated compatibility aggregate. */
  readonly coverage: ReviewCoverage;
  readonly confirmedFindings: ReadonlyArray<ReviewFinding>;
  readonly confirmedConcerns: ReadonlyArray<ReviewConcern>;
  readonly walkthrough: ReadonlyArray<WalkthroughEntry>;
}

/** Assess one settled run without trusting coordinator prose or findings. */
export const assessReviewPipeline = (input: {
  readonly shape: ReviewShape;
  readonly files: ReadonlyArray<ChangedFile>;
  readonly totalFiles: number;
  readonly anchorFiles: ReadonlyArray<ChangedFile>;
  readonly totalAnchorFiles: number;
  readonly events: ReadonlyArray<RunEvent>;
}): ReviewPipelineAssessment => {
  const trace = toolTrace(input.events);
  let inputCoverage =
    input.shape === "fan-out"
      ? fanOutInputCoverage(input.files, input.totalFiles)
      : flatInputCoverage(input.files, input.totalFiles, trace);
  if (input.anchorFiles.length < input.totalAnchorFiles) {
    inputCoverage = ReviewInputCoverage.make({
      ...inputCoverage,
      status: "incomplete",
      reasons: [
        ...inputCoverage.reasons,
        `full pull-request anchor surface exposed ${input.anchorFiles.length} of ${input.totalAnchorFiles} required files`,
      ],
    });
  }
  const assessed =
    input.shape === "fan-out"
      ? fanOutAssurance(input.files, input.totalFiles, trace)
      : flatAssurance();
  return {
    inputCoverage,
    assurance: assessed.assurance,
    coverage: compatibilityCoverage(inputCoverage, assessed.assurance),
    confirmedFindings: assessed.confirmedFindings,
    confirmedConcerns: assessed.confirmedConcerns,
    walkthrough: assessed.walkthrough,
  };
};

/** Compatibility helper; prefer assessReviewPipeline for precise claims. */
export const assessReviewCoverage = (input: {
  readonly shape: ReviewShape;
  readonly files: ReadonlyArray<ChangedFile>;
  readonly totalFiles: number;
  readonly anchorFiles: ReadonlyArray<ChangedFile>;
  readonly totalAnchorFiles: number;
  readonly events: ReadonlyArray<RunEvent>;
}): ReviewCoverage => assessReviewPipeline(input).coverage;

/** Host-verified summaries from successful general discovery passes only. */
export const collectUnitFileSummaries = (
  events: ReadonlyArray<RunEvent>,
): ReadonlyArray<WalkthroughEntry> => {
  const trace = toolTrace(events);
  return delegationDeclarations(trace).flatMap(({ id, request }) => {
    if (request.phase !== "discovery" || request.perspective !== "general") return [];
    const success = trace.succeeded.get(id);
    if (success === undefined || trace.failed.has(id)) return [];
    const result = Schema.decodeUnknownOption(FileReviewUnitResult)(success.result);
    if (
      Option.isNone(result) ||
      result.value.phase !== "discovery" ||
      result.value.workId !== request.workId ||
      result.value.unitId !== request.unitId
    ) {
      return [];
    }
    const assigned = new Set(request.paths);
    return result.value.fileSummaries.filter((entry) => assigned.has(entry.path));
  });
};

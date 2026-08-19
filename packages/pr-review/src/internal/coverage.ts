import { Option, Schema } from "effect";
import type { RunEvent } from "effect-agent";

import type { ChangedFile } from "./diff.ts";
import { isReviewableFile } from "./diff.ts";
import { FileDiffView, FileDiffQuery } from "./review-agent.ts";
import type { ReviewUnitPlan } from "./review-units.ts";

// ---------------------------------------------------------------------------
// Two different claims are deliberately modeled:
//
// - input coverage: every required path was assigned bounded evidence or was
//   explicitly reported outside the pipeline's capacity;
// - review assurance: every scheduled discovery/specialist pass and every
//   candidate-verification pass settled.
//
// Neither claims that the model found every defect. The fan-out pipeline is
// host-scheduled (fan-out.ts), so its assurance is computed from direct pass
// results; only the flat reviewer is assessed from its Run event trace here.
// ---------------------------------------------------------------------------

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
  /**
   * Paths with neither a textual diff nor bounded base/head text (binaries,
   * oversized files). A permanent property of the changeset: reported for
   * honesty, but never a reason the status is incomplete — re-running can
   * never review them, so treating them as gaps would fail every run forever.
   */
  undiffablePaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
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

/**
 * Settlement of scheduled review work. `incomplete` means reviewer-side work
 * failed after its bounded retry — a machinery gap that is carried forward and
 * retried on the next run, never a statement about the code under review.
 * `unverified` is the flat reviewer's honest constant: one pass with no
 * independent verifier is neither settled assurance nor a failure.
 */
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
  /** Discovery claims discarded for anchors/paths outside their assigned evidence. */
  discardedInvalidFindings: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
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

/** Render a bounded, deterministic "label (n): a, b, … (+k more)" reason line. */
export const boundedListReason = (label: string, values: Iterable<string>): string => {
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

const anchorSurfaceAdjusted = (
  inputCoverage: ReviewInputCoverage,
  anchorFiles: ReadonlyArray<ChangedFile>,
  totalAnchorFiles: number,
): ReviewInputCoverage =>
  anchorFiles.length >= totalAnchorFiles
    ? inputCoverage
    : ReviewInputCoverage.make({
        ...inputCoverage,
        status: "incomplete",
        reasons: [
          ...inputCoverage.reasons,
          `full pull-request anchor surface exposed ${anchorFiles.length} of ${totalAnchorFiles} required files`,
        ],
      });

/** The flat reviewer's honest constant assurance: one pass, no verifier. */
export const flatAssurance = (): ReviewAssurance =>
  ReviewAssurance.make({
    status: "unverified",
    requiredGeneralDiscoveryPasses: 1,
    completedGeneralDiscoveryPasses: 1,
    requiredSpecialistPasses: 0,
    completedSpecialistPasses: 0,
    requiredVerificationPasses: 0,
    completedVerificationPasses: 0,
    discoveredCandidates: 0,
    confirmedCandidates: 0,
    rejectedCandidates: 0,
    unsettledCandidates: 0,
    discardedInvalidFindings: 0,
    failedPasses: [],
    reasons: [
      "flat review has no independent candidate-verification pass; use the fan-out pipeline for a settled assurance result",
    ],
  });

export interface FlatReviewAssessment {
  readonly inputCoverage: ReviewInputCoverage;
  readonly assurance: ReviewAssurance;
  /** Retryable evidence gaps (failed or missing diff reads), never undiffable paths. */
  readonly unreviewedPaths: ReadonlyArray<string>;
}

/**
 * Assess one settled flat run from its Run event trace: which required paths
 * received successful bounded diff evidence. This observes tool INPUT
 * assignment only — the host cannot know which evidence the model weighed.
 */
export const assessFlatReview = (input: {
  readonly files: ReadonlyArray<ChangedFile>;
  readonly totalFiles: number;
  readonly anchorFiles: ReadonlyArray<ChangedFile>;
  readonly totalAnchorFiles: number;
  readonly events: ReadonlyArray<RunEvent>;
}): FlatReviewAssessment => {
  const trace = toolTrace(input.events);
  const requiredPaths = sortedUnique(input.files.map((file) => file.path));
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
  const undiffable = new Set(
    input.files.filter((file) => !isReviewableFile(file)).map((file) => file.path),
  );
  const unassigned = requiredPaths.filter(
    (path) => !undiffable.has(path) && (!assigned.has(path) || failedPaths.has(path)),
  );
  const reasons: Array<string> = [];
  if (input.files.length < input.totalFiles) {
    reasons.push(
      `review range exposed ${input.files.length} of ${input.totalFiles} required files`,
    );
  }
  if (failedPaths.size > 0) reasons.push(boundedListReason("diff reads failed", failedPaths));
  if (partial.size > 0) {
    reasons.push(boundedListReason("model-visible diff evidence was truncated", partial));
  }
  if (unassigned.length > 0) {
    reasons.push(boundedListReason("required paths received no successful diff input", unassigned));
  }
  const inputCoverage = anchorSurfaceAdjusted(
    ReviewInputCoverage.make({
      status: reasons.length === 0 ? "complete" : "incomplete",
      requiredPaths,
      assignedPaths: sortedUnique(assigned),
      partialPaths: sortedUnique(partial),
      unassignedPaths: sortedUnique(unassigned),
      undiffablePaths: sortedUnique(undiffable),
      reasons,
    }),
    input.anchorFiles,
    input.totalAnchorFiles,
  );
  return {
    inputCoverage,
    assurance: flatAssurance(),
    unreviewedPaths: unassigned,
  };
};

/**
 * Input coverage of one host-scheduled fan-out plan: which required paths the
 * bounded plan actually assigned complete evidence for. Capacity overflow is
 * a real gap and is carried by the pipeline as retryable scope; undiffable
 * paths are reported informationally only.
 */
export const fanOutInputCoverage = (input: {
  readonly plan: ReviewUnitPlan;
  readonly files: ReadonlyArray<ChangedFile>;
  readonly totalFiles: number;
  readonly anchorFiles: ReadonlyArray<ChangedFile>;
  readonly totalAnchorFiles: number;
}): ReviewInputCoverage => {
  const plan = input.plan;
  const assignedPaths = sortedUnique(plan.units.flatMap((unit) => unit.paths));
  const unassignedPaths = sortedUnique(plan.unassignedPaths);
  const reasons: Array<string> = [];
  if (plan.truncated) {
    reasons.push(
      `review range exposed ${input.files.length} of ${input.totalFiles} required files`,
    );
  }
  if (plan.partialEvidencePaths.length > 0) {
    reasons.push(
      boundedListReason(
        "fan-out capacity left some deterministic evidence shards unassigned",
        plan.partialEvidencePaths,
      ),
    );
  }
  if (plan.unassignedEvidenceShardCount > 0) {
    reasons.push(
      `${plan.unassignedEvidenceShardCount} deterministic evidence shard(s) exceeded fan-out capacity`,
    );
    reasons.push(
      boundedListReason(
        `unassigned evidence shard identifier sample (${plan.unassignedEvidenceShardIds.length} of ${plan.unassignedEvidenceShardCount})`,
        plan.unassignedEvidenceShardIds,
      ),
    );
  }
  if (plan.unassignedPaths.length > 0) {
    reasons.push(boundedListReason("fan-out capacity left paths unassigned", plan.unassignedPaths));
  }
  return anchorSurfaceAdjusted(
    ReviewInputCoverage.make({
      status: reasons.length === 0 ? "complete" : "incomplete",
      requiredPaths: sortedUnique(input.files.map((file) => file.path)),
      assignedPaths,
      partialPaths: plan.partialEvidencePaths,
      unassignedPaths,
      undiffablePaths: sortedUnique(plan.undiffablePaths),
      reasons,
    }),
    input.anchorFiles,
    input.totalAnchorFiles,
  );
};

/** Compatibility aggregate over the two precise claims. */
export const compatibilityCoverage = (
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

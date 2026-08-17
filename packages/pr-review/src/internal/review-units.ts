import { Schema } from "effect";

import type { ChangedFile } from "./diff.ts";
import { ChangedPath, isReviewableFile } from "./diff.ts";
import type { FindingSeverity } from "./review-agent.ts";
import { ReviewFinding } from "./review-agent.ts";

// ---------------------------------------------------------------------------
// Pure, deterministic planning for the fan-out reviewer: group the changeset
// into bounded review units (the work one delegated child reviews) and merge
// the children's findings back into one bounded review. Both operations are
// plain functions so tests pin them directly and the coordinator's tool
// surface stays deterministic — grouping is an algorithm, not model prose.
// ---------------------------------------------------------------------------

/** The delegation fan-out bound: one parent Run spawns at most this many children. */
export const MAX_REVIEW_UNITS = 16;

/** A unit never carries more files than this, regardless of their size. */
export const MAX_UNIT_FILES = 12;

/** Soft changed-line budget per unit; a single oversized file still gets its own unit. */
export const UNIT_CHANGED_LINE_BUDGET = 1_200;

/** Flat per-file cost so many tiny files still spread across units. */
const FILE_OVERHEAD_LINES = 20;

/** The merged review never exceeds the `CodeReview` findings bound. */
export const MAX_MERGED_FINDINGS = 20;

export const ReviewUnitId = Schema.NonEmptyString.check(Schema.isMaxLength(32));

/** One bounded slice of the changeset delegated to one child reviewer. */
export class ReviewUnit extends Schema.Class<ReviewUnit>("@effect-agent/pr-review/ReviewUnit")({
  unitId: ReviewUnitId,
  paths: Schema.Array(ChangedPath)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(MAX_UNIT_FILES)),
  /** additions + deletions across the unit's files, for honest sizing. */
  changedLines: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

/** The complete deterministic fan-out plan over one changeset. */
export class ReviewUnitPlan extends Schema.Class<ReviewUnitPlan>(
  "@effect-agent/pr-review/ReviewUnitPlan",
)({
  totalFiles: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** True when the source returned fewer files than the pull request has. */
  truncated: Schema.Boolean,
  units: Schema.Array(ReviewUnit).check(Schema.isMaxLength(MAX_REVIEW_UNITS)),
  /** Changed files with neither a textual diff nor bounded base/head text. */
  undiffablePaths: Schema.Array(ChangedPath).check(Schema.isMaxLength(300)),
  /**
   * Diffable files beyond the fan-out capacity (MAX_REVIEW_UNITS units of
   * MAX_UNIT_FILES files). Never silently dropped: the coordinator must name
   * them as unreviewed in its summary.
   */
  unassignedPaths: Schema.Array(ChangedPath).check(Schema.isMaxLength(300)),
}) {}

const fileCost = (file: ChangedFile): number => {
  const contentChars =
    (file.reviewBaseContent?.length ?? 0) + (file.reviewHeadContent?.length ?? 0);
  const contentWeight = Math.ceil(contentChars / 200);
  return file.additions + file.deletions + contentWeight + FILE_OVERHEAD_LINES;
};

const unitOf = (index: number, files: ReadonlyArray<ChangedFile>): ReviewUnit =>
  ReviewUnit.make({
    unitId: `unit-${String(index + 1).padStart(3, "0")}`,
    paths: files.map((file) => file.path),
    changedLines: files.reduce((total, file) => total + file.additions + file.deletions, 0),
  });

/**
 * Group the changeset into at most `MAX_REVIEW_UNITS` review units.
 *
 * Deterministic by construction: files are ordered by path (so files sharing
 * a directory become neighbors — directory affinity without a heuristic),
 * then packed greedily in that order under the soft changed-line budget and
 * the hard per-unit file bound. Capacity is finite and explicit:
 *
 * - files without a textual diff are still delegated when the source
 *   recovered complete bounded UTF-8 base/head content. Findings from that
 *   evidence cannot anchor inline and are reported as concerns;
 * - files with neither form of textual evidence surface in
 *   `undiffablePaths` instead of laundering missing coverage;
 * - reviewable files beyond `MAX_REVIEW_UNITS` full units surface in
 *   `unassignedPaths` so the review can report them as unreviewed, never
 *   silently truncated.
 */
export const planReviewUnits = (
  files: ReadonlyArray<ChangedFile>,
  options: { readonly totalChangedFiles: number },
): ReviewUnitPlan => {
  const ordered = [...files].sort((left, right) => (left.path < right.path ? -1 : 1));
  const reviewable = ordered.filter(isReviewableFile);
  const undiffable = ordered.filter((file) => !isReviewableFile(file));

  const groups: Array<Array<ChangedFile>> = [];
  const unassigned: Array<ChangedFile> = [];
  let current: Array<ChangedFile> = [];
  let currentCost = 0;
  for (const file of reviewable) {
    const cost = fileCost(file);
    const wouldOverflow =
      current.length >= MAX_UNIT_FILES ||
      (current.length > 0 && currentCost + cost > UNIT_CHANGED_LINE_BUDGET);
    if (wouldOverflow) {
      groups.push(current);
      current = [];
      currentCost = 0;
    }
    if (groups.length >= MAX_REVIEW_UNITS) {
      unassigned.push(file);
      continue;
    }
    current.push(file);
    currentCost += cost;
  }
  if (current.length > 0 && groups.length < MAX_REVIEW_UNITS) {
    groups.push(current);
  }

  return ReviewUnitPlan.make({
    totalFiles: files.length,
    truncated: files.length < options.totalChangedFiles,
    units: groups.map((group, index) => unitOf(index, group)),
    undiffablePaths: undiffable.map((file) => file.path),
    unassignedPaths: unassigned.map((file) => file.path),
  });
};

const severityRank: Record<FindingSeverity, number> = {
  blocking: 0,
  important: 1,
  nit: 2,
};

const anchorKey = (finding: ReviewFinding): string =>
  `${finding.path} ${finding.startLine} ${finding.endLine}`;

/**
 * Merge the children's findings into one bounded, deterministic list: dedupe
 * findings sharing an anchor (path + line range) keeping the most severe —
 * and, at equal severity, the first in declaration order — then rank by
 * severity, path, and line, and cap at the `CodeReview` findings bound.
 * This is the merge policy the coordinator's instructions state in prose;
 * pinning it here keeps the policy itself deterministic and testable.
 */
export const rankAndDedupeFindings = (
  findings: ReadonlyArray<ReviewFinding>,
): ReadonlyArray<ReviewFinding> => {
  const byAnchor = new Map<string, ReviewFinding>();
  for (const finding of findings) {
    const key = anchorKey(finding);
    const existing = byAnchor.get(key);
    if (
      existing === undefined ||
      severityRank[finding.severity] < severityRank[existing.severity]
    ) {
      byAnchor.set(key, finding);
    }
  }
  return [...byAnchor.values()]
    .sort((left, right) => {
      const bySeverity = severityRank[left.severity] - severityRank[right.severity];
      if (bySeverity !== 0) return bySeverity;
      if (left.path !== right.path) return left.path < right.path ? -1 : 1;
      return left.startLine - right.startLine;
    })
    .slice(0, MAX_MERGED_FINDINGS);
};

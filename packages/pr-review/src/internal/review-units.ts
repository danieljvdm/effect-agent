import { Schema } from "effect";

import type { ChangedFile } from "./diff.ts";
import { annotatePatch, ChangedPath, isReviewableFile, renderReviewContent } from "./diff.ts";
import { MAX_PATCH_CHARS, type FindingSeverity, type ReviewFinding } from "./review-agent.ts";

// ---------------------------------------------------------------------------
// Pure, deterministic planning for the fan-out reviewer: group the changeset
// into bounded review units (the work one delegated child reviews) and merge
// the children's findings back into one bounded review. Both operations are
// plain functions so tests pin them directly and the coordinator's tool
// surface stays deterministic — grouping is an algorithm, not model prose.
// ---------------------------------------------------------------------------

/** The delegation fan-out bound: one parent Run spawns at most this many children. */
export const MAX_REVIEW_UNITS = 8;

/** A unit never carries more files than this, regardless of their size. */
export const MAX_UNIT_FILES = 12;

/** Soft changed-line budget per unit; a single oversized file still gets its own unit. */
export const UNIT_CHANGED_LINE_BUDGET = 800;

/**
 * Bound the complete model-visible evidence assigned to one child. This is a
 * character bound rather than a token estimate because it is deterministic,
 * provider-independent, and enforced before any model call.
 */
export const UNIT_EVIDENCE_CHAR_BUDGET = 240_000;

/** Maximum annotated patch characters placed in one child brief. */
export const MAX_FILE_EVIDENCE_CHARS = MAX_PATCH_CHARS;

/** Flat per-file cost so many tiny files still spread across units. */
const FILE_OVERHEAD_LINES = 20;

/** The merged review never exceeds the `CodeReview` findings bound. */
export const MAX_MERGED_FINDINGS = 20;

export const ReviewUnitId = Schema.NonEmptyString.check(Schema.isMaxLength(32));

/** High-risk surfaces that receive a fresh specialist discovery pass. */
export const ReviewRiskCategory = Schema.Literals([
  "authentication-authorization",
  "security-boundary",
  "persistence-durability",
  "concurrency",
  "credential-handling",
  "external-side-effects",
]);
export type ReviewRiskCategory = typeof ReviewRiskCategory.Type;

export const ReviewDiscoveryPerspective = Schema.Literals(["general", "risk-specialist"]);
export type ReviewDiscoveryPerspective = typeof ReviewDiscoveryPerspective.Type;

export const ReviewPassId = Schema.NonEmptyString.check(Schema.isMaxLength(64));

/** One required, independently scoped discovery attempt. */
export class ReviewDiscoveryPass extends Schema.Class<ReviewDiscoveryPass>(
  "@effect-agent/pr-review/ReviewDiscoveryPass",
)({
  passId: ReviewPassId,
  unitId: ReviewUnitId,
  paths: Schema.Array(ChangedPath)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(MAX_UNIT_FILES)),
  perspective: ReviewDiscoveryPerspective,
  /** Empty for the general pass; explicit deterministic focus for specialists. */
  riskCategories: Schema.Array(ReviewRiskCategory).check(Schema.isMaxLength(6)),
}) {}

/** One bounded slice of the changeset delegated to one child reviewer. */
export class ReviewUnit extends Schema.Class<ReviewUnit>("@effect-agent/pr-review/ReviewUnit")({
  unitId: ReviewUnitId,
  paths: Schema.Array(ChangedPath)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(MAX_UNIT_FILES)),
  /** additions + deletions across the unit's files, for honest sizing. */
  changedLines: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Complete model-visible diff/content evidence assigned to each child. */
  evidenceChars: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Host-classified reasons this unit requires redundant specialist discovery. */
  riskCategories: Schema.Array(ReviewRiskCategory).check(Schema.isMaxLength(6)),
}) {}

/** The complete deterministic fan-out plan over one changeset. */
export class ReviewUnitPlan extends Schema.Class<ReviewUnitPlan>(
  "@effect-agent/pr-review/ReviewUnitPlan",
)({
  totalFiles: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** True when the source returned fewer files than the pull request has. */
  truncated: Schema.Boolean,
  units: Schema.Array(ReviewUnit).check(Schema.isMaxLength(MAX_REVIEW_UNITS)),
  /** Exact discovery calls the coordinator must make. */
  discoveryPasses: Schema.Array(ReviewDiscoveryPass).check(
    Schema.isMaxLength(MAX_REVIEW_UNITS * 2),
  ),
  /** Changed files with neither a textual diff nor bounded base/head text. */
  undiffablePaths: Schema.Array(ChangedPath).check(Schema.isMaxLength(300)),
  /** Assigned paths whose diff evidence was necessarily truncated. */
  partialEvidencePaths: Schema.Array(ChangedPath).check(Schema.isMaxLength(300)),
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

const annotatedPatchChars = (patch: string): number => {
  const chars = annotatePatch(patch).length;
  return chars > MAX_FILE_EVIDENCE_CHARS
    ? MAX_FILE_EVIDENCE_CHARS + "\n[diff truncated]".length
    : chars;
};

const evidenceChars = (file: ChangedFile): number =>
  file.patch === undefined
    ? (renderReviewContent(file)?.length ?? 0)
    : annotatedPatchChars(file.patch);

const hasPartialEvidence = (file: ChangedFile): boolean =>
  file.patch !== undefined && annotatePatch(file.patch).length > MAX_FILE_EVIDENCE_CHARS;

const riskRules: ReadonlyArray<{
  readonly category: ReviewRiskCategory;
  readonly patterns: ReadonlyArray<RegExp>;
}> = [
  {
    category: "authentication-authorization",
    patterns: [/auth/, /authoriz/, /permission/, /principal/, /access[-_ ]?control/, /role\b/],
  },
  {
    category: "security-boundary",
    patterns: [
      /security/,
      /sandbox/,
      /untrusted/,
      /schema\.decode/,
      /validation/,
      /injection/,
      /csrf/,
      /xss/,
      /path traversal/,
    ],
  },
  {
    category: "persistence-durability",
    patterns: [
      /durab/,
      /persist/,
      /storage/,
      /database/,
      /\bsql\b/,
      /journal/,
      /ledger/,
      /checkpoint/,
      /migration/,
      /transaction/,
    ],
  },
  {
    category: "concurrency",
    patterns: [
      /concurr/,
      /semaphore/,
      /\bfiber/,
      /race/,
      /mutex/,
      /\block\b/,
      /queue/,
      /parallel/,
      /interrupt/,
    ],
  },
  {
    category: "credential-handling",
    patterns: [/credential/, /secret/, /password/, /api[-_ ]?key/, /bearer/, /hmac/, /signature/],
  },
  {
    category: "external-side-effects",
    patterns: [
      /publish/,
      /webhook/,
      /github/,
      /fetch\(/,
      /http/,
      /send[-_ ]?(email|message)/,
      /write[-_ ]?(file|record)/,
      /delete/,
      /mutation/,
      /side[-_ ]?effect/,
      /spawn/,
      /exec/,
    ],
  },
];

/**
 * Deterministic host policy for specialist assignment. It intentionally
 * favors false positives: an extra bounded pass costs work, while a missed
 * high-risk classification removes redundancy. This is not a claim that the
 * keyword policy recognizes every semantically risky change.
 */
export const classifyReviewRisks = (file: ChangedFile): ReadonlyArray<ReviewRiskCategory> => {
  const text = [
    file.path,
    file.previousPath ?? "",
    file.patch ?? "",
    file.reviewBaseContent ?? "",
    file.reviewHeadContent ?? "",
  ]
    .join("\n")
    .toLowerCase();
  return riskRules
    .filter((rule) => rule.patterns.some((pattern) => pattern.test(text)))
    .map((rule) => rule.category);
};

const unitOf = (index: number, files: ReadonlyArray<ChangedFile>): ReviewUnit =>
  ReviewUnit.make({
    unitId: `unit-${String(index + 1).padStart(3, "0")}`,
    paths: files.map((file) => file.path),
    changedLines: files.reduce((total, file) => total + file.additions + file.deletions, 0),
    evidenceChars: files.reduce((total, file) => total + evidenceChars(file), 0),
    riskCategories: [...new Set(files.flatMap((file) => classifyReviewRisks(file)))],
  });

const discoveryPassesFor = (units: ReadonlyArray<ReviewUnit>): ReadonlyArray<ReviewDiscoveryPass> =>
  units.flatMap((unit) => [
    ReviewDiscoveryPass.make({
      passId: `${unit.unitId}-general`,
      unitId: unit.unitId,
      paths: unit.paths,
      perspective: "general",
      riskCategories: [],
    }),
    ...(unit.riskCategories.length === 0
      ? []
      : [
          ReviewDiscoveryPass.make({
            passId: `${unit.unitId}-specialist`,
            unitId: unit.unitId,
            paths: unit.paths,
            perspective: "risk-specialist",
            riskCategories: unit.riskCategories,
          }),
        ]),
  ]);

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
  let currentEvidenceChars = 0;
  for (const file of reviewable) {
    const cost = fileCost(file);
    const chars = evidenceChars(file);
    const wouldOverflow =
      current.length >= MAX_UNIT_FILES ||
      (current.length > 0 &&
        (currentCost + cost > UNIT_CHANGED_LINE_BUDGET ||
          currentEvidenceChars + chars > UNIT_EVIDENCE_CHAR_BUDGET));
    if (wouldOverflow) {
      groups.push(current);
      current = [];
      currentCost = 0;
      currentEvidenceChars = 0;
    }
    if (groups.length >= MAX_REVIEW_UNITS) {
      unassigned.push(file);
      continue;
    }
    current.push(file);
    currentCost += cost;
    currentEvidenceChars += chars;
  }
  if (current.length > 0 && groups.length < MAX_REVIEW_UNITS) {
    groups.push(current);
  }

  const units = groups.map((group, index) => unitOf(index, group));
  return ReviewUnitPlan.make({
    totalFiles: files.length,
    truncated: files.length < options.totalChangedFiles,
    units,
    discoveryPasses: discoveryPassesFor(units),
    undiffablePaths: undiffable.map((file) => file.path),
    partialEvidencePaths: reviewable.filter(hasPartialEvidence).map((file) => file.path),
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

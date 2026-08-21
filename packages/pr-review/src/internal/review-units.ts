import { Schema } from "effect";

import type { ChangedFile } from "./diff.ts";
import { ChangedPath, isReviewableFile } from "./diff.ts";
import {
  fileReviewEvidenceChunks,
  type FindingSeverity,
  MAX_PATCH_CHARS,
  type ReviewConcern,
  type ReviewFinding,
} from "./review-agent.ts";

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

/**
 * Bound the complete model-visible evidence assigned to one child. This is a
 * character bound rather than a token estimate because it is deterministic,
 * provider-independent, and enforced before any model call.
 */
export const UNIT_EVIDENCE_CHAR_BUDGET = 240_000;

/** Maximum complete evidence shards placed in one child brief. */
export const MAX_UNIT_EVIDENCE_SHARDS = 12;

/**
 * Keep overflow diagnostics bounded to one plan's total assignment capacity.
 * The plan separately records the exact overflow count and every affected
 * path, so identifiers are a deterministic diagnostic sample rather than the
 * authority for whether input coverage is complete.
 */
export const MAX_REPORTED_UNASSIGNED_EVIDENCE_SHARDS = MAX_REVIEW_UNITS * MAX_UNIT_EVIDENCE_SHARDS;

/** The merged review never exceeds the `CodeReview` findings bound. */
export const MAX_MERGED_FINDINGS = 20;

export const ReviewUnitId = Schema.NonEmptyString.check(Schema.isMaxLength(32));

/** High-risk surfaces that receive an explicit specialist focus label. */
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
export const ReviewEvidenceShardId = Schema.NonEmptyString.check(Schema.isMaxLength(32));

/** One complete bounded slice of a changed path's model-visible evidence. */
export class ReviewEvidenceShard extends Schema.Class<ReviewEvidenceShard>(
  "@effect-agent/pr-review/ReviewEvidenceShard",
)({
  shardId: ReviewEvidenceShardId,
  path: ChangedPath,
  ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  total: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  evidenceChars: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).check(
    Schema.isLessThanOrEqualTo(MAX_PATCH_CHARS),
  ),
}) {}

const EvidenceShardIds = Schema.Array(ReviewEvidenceShardId)
  .check(Schema.isMinLength(1))
  .check(Schema.isMaxLength(MAX_UNIT_EVIDENCE_SHARDS));

/** One required, independently scoped discovery attempt. */
export class ReviewDiscoveryPass extends Schema.Class<ReviewDiscoveryPass>(
  "@effect-agent/pr-review/ReviewDiscoveryPass",
)({
  passId: ReviewPassId,
  unitId: ReviewUnitId,
  paths: Schema.Array(ChangedPath)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(MAX_UNIT_FILES)),
  evidenceShardIds: EvidenceShardIds,
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
  evidenceShards: Schema.Array(ReviewEvidenceShard)
    .check(Schema.isMinLength(1))
    .check(Schema.isMaxLength(MAX_UNIT_EVIDENCE_SHARDS)),
  /** additions + deletions across the unit's files, for honest sizing. */
  changedLines: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Complete model-visible diff/content evidence assigned to each child. */
  evidenceChars: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)).check(
    Schema.isLessThanOrEqualTo(UNIT_EVIDENCE_CHAR_BUDGET),
  ),
  /** Host-classified focus labels for the unit's redundant specialist pass. */
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
  /** Assigned paths with one or more evidence shards beyond plan capacity. */
  partialEvidencePaths: Schema.Array(ChangedPath).check(Schema.isMaxLength(300)),
  /** Exact number of shards beyond the bounded unit capacity. */
  unassignedEvidenceShardCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Bounded deterministic prefix of the unassigned shard identifiers. */
  unassignedEvidenceShardIds: Schema.Array(ReviewEvidenceShardId).check(
    Schema.isMaxLength(MAX_REPORTED_UNASSIGNED_EVIDENCE_SHARDS),
  ),
  /**
   * Diffable files beyond the fan-out capacity (MAX_REVIEW_UNITS units of
   * MAX_UNIT_FILES files). Never silently dropped: the coordinator must name
   * them as unreviewed in its summary.
   */
  unassignedPaths: Schema.Array(ChangedPath).check(Schema.isMaxLength(300)),
}) {}

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

/**
 * Whether every claimed finding anchor was present in the exact bounded
 * evidence shards assigned to one unit. This is stricter than checking the
 * full pull-request diff when an oversized path spans multiple units.
 */
export const findingAnchorInUnitEvidence = (
  finding: ReviewFinding,
  unit: ReviewUnit,
  files: ReadonlyArray<ChangedFile>,
): boolean => {
  const file = files.find((candidate) => candidate.path === finding.path);
  if (file?.patch === undefined || finding.endLine < finding.startLine) return false;
  const assignedOrdinals = new Set(
    unit.evidenceShards
      .filter((shard) => shard.path === finding.path)
      .map((shard) => shard.ordinal),
  );
  const visibleLines = new Set<number>();
  const chunks = fileReviewEvidenceChunks(file);
  for (let index = 0; index < chunks.length; index += 1) {
    if (!assignedOrdinals.has(index + 1)) continue;
    for (const line of chunks[index]?.annotatedPatch.split("\n") ?? []) {
      const match = /^R(\d+) /.exec(line);
      if (match?.[1] !== undefined) visibleLines.add(Number(match[1]));
    }
  }
  for (let line = finding.startLine; line <= finding.endLine; line += 1) {
    if (!visibleLines.has(line)) return false;
  }
  return true;
};

interface PlannedEvidenceShard {
  readonly shard: ReviewEvidenceShard;
  readonly file: ChangedFile;
  readonly changedLines: number;
}

const uniquePaths = (shards: ReadonlyArray<PlannedEvidenceShard>): ReadonlyArray<string> => [
  ...new Set(shards.map(({ shard }) => shard.path)),
];

const plannedEvidenceShards = (
  files: ReadonlyArray<ChangedFile>,
): ReadonlyArray<PlannedEvidenceShard> => {
  const planned: Array<PlannedEvidenceShard> = [];
  let shardIndex = 0;
  for (const file of files) {
    const chunks = fileReviewEvidenceChunks(file);
    for (let index = 0; index < chunks.length; index += 1) {
      const chunk = chunks[index];
      if (chunk === undefined) continue;
      shardIndex += 1;
      planned.push({
        shard: ReviewEvidenceShard.make({
          shardId: `shard-${String(shardIndex).padStart(4, "0")}`,
          path: file.path,
          ordinal: index + 1,
          total: chunks.length,
          evidenceChars: chunk.annotatedPatch.length,
        }),
        file,
        changedLines: index === 0 ? file.additions + file.deletions : 0,
      });
    }
  }
  return planned;
};

const unitOf = (index: number, shards: ReadonlyArray<PlannedEvidenceShard>): ReviewUnit =>
  ReviewUnit.make({
    unitId: `unit-${String(index + 1).padStart(3, "0")}`,
    paths: uniquePaths(shards),
    evidenceShards: shards.map(({ shard }) => shard),
    changedLines: shards.reduce((total, shard) => total + shard.changedLines, 0),
    evidenceChars: shards.reduce((total, { shard }) => total + shard.evidenceChars, 0),
    riskCategories: [...new Set(shards.flatMap(({ file }) => classifyReviewRisks(file)))],
  });

const discoveryPassesFor = (units: ReadonlyArray<ReviewUnit>): ReadonlyArray<ReviewDiscoveryPass> =>
  units.flatMap((unit) => [
    ReviewDiscoveryPass.make({
      passId: `${unit.unitId}-general`,
      unitId: unit.unitId,
      paths: unit.paths,
      evidenceShardIds: unit.evidenceShards.map((shard) => shard.shardId),
      perspective: "general",
      riskCategories: [],
    }),
    ReviewDiscoveryPass.make({
      passId: `${unit.unitId}-specialist`,
      unitId: unit.unitId,
      paths: unit.paths,
      evidenceShardIds: unit.evidenceShards.map((shard) => shard.shardId),
      perspective: "risk-specialist",
      riskCategories: unit.riskCategories,
    }),
  ]);

/**
 * Group the changeset into at most `MAX_REVIEW_UNITS` review units.
 *
 * Deterministic by construction: files are ordered by path (so files sharing
 * a directory become neighbors — directory affinity without a heuristic),
 * then split into complete line-bounded evidence shards and packed greedily
 * under the hard evidence and per-unit shard bounds. Capacity is finite and
 * explicit:
 *
 * - files without a textual diff are still delegated when the source
 *   recovered complete bounded UTF-8 base/head content. Findings from that
 *   evidence cannot anchor inline and are reported as concerns;
 * - files with neither form of textual evidence surface in
 *   `undiffablePaths` instead of laundering missing coverage;
 * - an oversized path spans as many deterministic shards and units as needed;
 * - shards beyond `MAX_REVIEW_UNITS` full units surface explicitly, so a path
 *   is partial only when finite plan capacity is genuinely exhausted.
 */
export const planReviewUnits = (
  files: ReadonlyArray<ChangedFile>,
  options: { readonly totalChangedFiles: number },
): ReviewUnitPlan => {
  const ordered = [...files].sort((left, right) => (left.path < right.path ? -1 : 1));
  const reviewable = ordered.filter(isReviewableFile);
  const undiffable = ordered.filter((file) => !isReviewableFile(file));

  const shards = plannedEvidenceShards(reviewable);
  const groups: Array<Array<PlannedEvidenceShard>> = [];
  const unassigned: Array<PlannedEvidenceShard> = [];
  let current: Array<PlannedEvidenceShard> = [];
  let currentEvidenceChars = 0;
  for (const shard of shards) {
    const nextPaths = new Set([...uniquePaths(current), shard.shard.path]);
    const wouldOverflow =
      current.length >= MAX_UNIT_EVIDENCE_SHARDS ||
      nextPaths.size > MAX_UNIT_FILES ||
      (current.length > 0 &&
        currentEvidenceChars + shard.shard.evidenceChars > UNIT_EVIDENCE_CHAR_BUDGET);
    if (wouldOverflow) {
      groups.push(current);
      current = [];
      currentEvidenceChars = 0;
    }
    if (groups.length >= MAX_REVIEW_UNITS) {
      unassigned.push(shard);
      continue;
    }
    current.push(shard);
    currentEvidenceChars += shard.shard.evidenceChars;
  }
  if (current.length > 0 && groups.length < MAX_REVIEW_UNITS) {
    groups.push(current);
  }

  const units = groups.map((group, index) => unitOf(index, group));
  const assignedShardIds = new Set(
    units.flatMap((unit) => unit.evidenceShards.map((shard) => shard.shardId)),
  );
  const assignedPaths = new Set(
    shards
      .filter(({ shard }) => assignedShardIds.has(shard.shardId))
      .map(({ shard }) => shard.path),
  );
  const unassignedPathsWithEvidence = new Set(unassigned.map(({ shard }) => shard.path));
  return ReviewUnitPlan.make({
    totalFiles: files.length,
    truncated: files.length < options.totalChangedFiles,
    units,
    discoveryPasses: discoveryPassesFor(units),
    undiffablePaths: undiffable.map((file) => file.path),
    partialEvidencePaths: [...unassignedPathsWithEvidence].filter((path) =>
      assignedPaths.has(path),
    ),
    unassignedEvidenceShardCount: unassigned.length,
    unassignedEvidenceShardIds: unassigned
      .slice(0, MAX_REPORTED_UNASSIGNED_EVIDENCE_SHARDS)
      .map(({ shard }) => shard.shardId),
    unassignedPaths: [...unassignedPathsWithEvidence].filter((path) => !assignedPaths.has(path)),
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

/**
 * Stable identity for one concern. The paths are part of the claim: identical
 * prose about two independent files must not collapse into one item.
 */
export const reviewConcernKey = (concern: ReviewConcern): string =>
  `${(concern.evidencePaths ?? []).join("\u0000")}\u0001${concern.title}\u0000${concern.body}`;

/**
 * The concern analogue of `rankAndDedupeFindings`: dedupe by exact scoped
 * content keeping the most severe duplicate, rank by severity, and cap at the
 * `CodeReview` concerns bound.
 */
export const rankAndDedupeConcerns = (
  concerns: ReadonlyArray<ReviewConcern>,
): ReadonlyArray<ReviewConcern> => {
  const byContent = new Map<string, ReviewConcern>();
  for (const concern of concerns) {
    const key = reviewConcernKey(concern);
    const previous = byContent.get(key);
    if (
      previous === undefined ||
      severityRank[concern.severity] < severityRank[previous.severity]
    ) {
      byContent.set(key, concern);
    }
  }
  return [...byContent.values()]
    .sort((left, right) => severityRank[left.severity] - severityRank[right.severity])
    .slice(0, 10);
};

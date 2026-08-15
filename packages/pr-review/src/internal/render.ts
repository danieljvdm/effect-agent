import { Schema } from "effect";

import type { ReviewCoverage } from "./coverage.ts";
import type { ChangedFile } from "./diff.ts";
import { commentableLines } from "./diff.ts";
import { renderFingerprintMarker } from "./fingerprint.ts";
import type { CodeReview } from "./review-agent.ts";
import { ReviewFinding, type ReviewConcern } from "./review-agent.ts";
import { renderReviewStateMarker, type ReviewScopeMode, type ReviewState } from "./review-state.ts";

// ---------------------------------------------------------------------------
// Publication planning: pure, deterministic, and fail-closed. Model output is
// untrusted input, so every finding anchor is validated against the parsed
// diff before it may become an inline comment; findings that fail validation
// are demoted into the review body instead of being dropped or trusted. This
// module is deliberately not configurable — customization widens what goes
// into a review, never what leaves it unvalidated.
// ---------------------------------------------------------------------------

export const ReviewEvent = Schema.Literals(["COMMENT", "APPROVE", "REQUEST_CHANGES"]);
export type ReviewEvent = typeof ReviewEvent.Type;

/** One inline comment exactly as the GitHub review API accepts it. */
export class ReviewCommentDraft extends Schema.Class<ReviewCommentDraft>(
  "@effect-agent/pr-review/ReviewCommentDraft",
)({
  path: Schema.NonEmptyString,
  /** The last (or only) commented line, RIGHT side of the diff. */
  line: Schema.Int.check(Schema.isGreaterThan(0)),
  /** Present only for multi-line comments; strictly less than `line`. */
  startLine: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  body: Schema.NonEmptyString,
}) {}

/** The complete, validated review ready for one GitHub reviews API call. */
export class ReviewPublicationPlan extends Schema.Class<ReviewPublicationPlan>(
  "@effect-agent/pr-review/ReviewPublicationPlan",
)({
  event: ReviewEvent,
  body: Schema.String.check(Schema.isMaxLength(60_000)),
  comments: Schema.Array(ReviewCommentDraft),
  /** Findings whose anchors failed diff validation; folded into `body`. */
  demoted: Schema.Array(ReviewFinding),
  /** The head commit the diffs were fetched at; pins the posted review. */
  commitSha: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
}) {}

const severityEmoji: Record<ReviewFinding["severity"], string> = {
  blocking: "🛑",
  important: "⚠️",
  nit: "💅",
};

const severityRank: Record<ReviewFinding["severity"], number> = {
  blocking: 0,
  important: 1,
  nit: 2,
};

const severityLabel: Record<ReviewFinding["severity"], string> = {
  blocking: `${severityEmoji.blocking} blocking`,
  important: `${severityEmoji.important} important`,
  nit: `${severityEmoji.nit} nit`,
};

/** A fence long enough that the suggestion content can never close it early. */
const suggestionFence = (suggestion: string): string => {
  let fence = "```";
  while (suggestion.includes(fence)) fence = `${fence}\``;
  return fence;
};

const renderCommentBody = (finding: ReviewFinding): string => {
  const parts = [`**[${severityLabel[finding.severity]}] ${finding.title}**`, "", finding.body];
  if (finding.suggestion !== undefined) {
    const fence = suggestionFence(finding.suggestion);
    parts.push("", `${fence}suggestion`, finding.suggestion, fence);
  }
  return parts.join("\n");
};

const renderDemoted = (finding: ReviewFinding, reason: string): string => {
  const location = `\`${finding.path}:${finding.startLine}${
    finding.endLine !== finding.startLine ? `-${finding.endLine}` : ""
  }\``;
  return `- ${location} **[${severityLabel[finding.severity]}] ${finding.title}** — ${finding.body} _(demoted: ${reason})_`;
};

const countNoun = (count: number, noun: string): string =>
  `${count} ${noun}${count === 1 ? "" : "s"}`;

/** The validated finding + concern severities, tallied for callout and event. */
const severityCounts = (
  review: CodeReview,
  carriedFindings: ReadonlyArray<ReviewFinding> = [],
  carriedConcerns: ReadonlyArray<ReviewConcern> = [],
) => {
  const severities = [
    ...review.findings.map((finding) => finding.severity),
    ...(review.concerns ?? []).map((concern) => concern.severity),
    ...carriedFindings.map((finding) => finding.severity),
    ...carriedConcerns.map((concern) => concern.severity),
  ];
  return {
    blocking: severities.filter((severity) => severity === "blocking").length,
    important: severities.filter((severity) => severity === "important").length,
    total: severities.length,
  };
};

/**
 * The opening callout: the review's overall tier, derived HOST-SIDE from the
 * validated severities (never from model prose), described by what GitHub
 * renders it as. `[!CAUTION]` is a red banner, `[!IMPORTANT]` a purple one;
 * the blockquote tiers read as informational.
 */
const renderVerdictCallout = (
  review: CodeReview,
  options: {
    readonly carriedFindings?: ReadonlyArray<ReviewFinding> | undefined;
    readonly carriedConcerns?: ReadonlyArray<ReviewConcern> | undefined;
    readonly coverage?: ReviewCoverage | undefined;
  },
): string => {
  const counts = severityCounts(review, options.carriedFindings, options.carriedConcerns);
  if (options.coverage?.status === "incomplete") {
    const suffix =
      counts.blocking > 0 ? ` It also has ${countNoun(counts.blocking, "blocking finding")}.` : "";
    return `> [!CAUTION]\n> Review coverage is incomplete — the check must not pass.${suffix}`;
  }
  if (counts.blocking > 0) {
    return `> [!CAUTION]\n> ${countNoun(counts.blocking, "blocking finding")} — do not merge before addressing ${counts.blocking === 1 ? "it" : "them"}.`;
  }
  if (counts.important > 0) {
    return `> [!IMPORTANT]\n> ${countNoun(counts.important, "important finding")} to address before merging.`;
  }
  if (counts.total > 0) {
    return "> ℹ️ Minor suggestions only — mergeable as-is.";
  }
  return review.verdict === "approve"
    ? "> ✅ No issues found."
    : "> ℹ️ No findings — see the summary.";
};

const renderConcern = (concern: ReviewConcern): string =>
  [`### ${severityEmoji[concern.severity]} ${concern.title}`, "", concern.body].join("\n");

const renderCarriedFinding = (finding: ReviewFinding): string =>
  `- \`${finding.path}:${finding.startLine}${finding.endLine === finding.startLine ? "" : `-${finding.endLine}`}\` **[${severityLabel[finding.severity]}] ${finding.title}** — ${finding.body}`;

/** HTML comments must not contain `--`; interpolated values are sanitized. */
const commentSafe = (value: string): string => value.replaceAll("--", "- -");

/**
 * The invisible staleness note addressed to whoever reads the review later —
 * a human or a downstream agent: which commit the findings were written
 * against, and that line callouts age the moment new commits land.
 */
const renderReviewMetadata = (options: {
  readonly headSha: string;
  readonly baseRef?: string | undefined;
  readonly headRef?: string | undefined;
  readonly filesVisible: number;
  readonly totalChangedFiles: number;
  readonly reviewMode?: ReviewScopeMode | undefined;
  readonly baselineSha?: string | undefined;
}): string =>
  [
    "<!-- effect-agent-pr-review metadata",
    `reviewed-head: ${commentSafe(options.headSha)}`,
    ...(options.baseRef !== undefined && options.headRef !== undefined
      ? [`base-ref: ${commentSafe(options.baseRef)}`, `head-ref: ${commentSafe(options.headRef)}`]
      : []),
    // The observation surface, not a coverage claim: the host cannot know
    // which visible files the model actually examined, and the summary is
    // where unreviewed units are named.
    `files-visible: ${options.filesVisible} of ${options.totalChangedFiles}`,
    ...(options.reviewMode === undefined ? [] : [`review-mode: ${options.reviewMode}`]),
    ...(options.baselineSha === undefined
      ? []
      : [`incremental-baseline: ${commentSafe(options.baselineSha)}`]),
    "Findings were written against the head commit above; if commits have landed",
    "since, treat file and line callouts as potentially stale and re-diff first.",
    "-->",
  ].join("\n");

/**
 * Why one finding cannot become an inline comment, or undefined when it can.
 * Exported so tests can pin each rule individually.
 */
export const anchorViolation = (
  finding: ReviewFinding,
  files: ReadonlyArray<ChangedFile>,
): string | undefined => {
  const file = files.find((candidate) => candidate.path === finding.path);
  if (file === undefined) return "path is not part of the changeset";
  if (file.patch === undefined) return "file has no textual diff";
  if (finding.endLine < finding.startLine) return "endLine precedes startLine";
  if (finding.endLine - finding.startLine + 1 > 100) return "range is implausibly large";
  const anchors = commentableLines(file.patch);
  for (let line = finding.startLine; line <= finding.endLine; line += 1) {
    if (!anchors.has(line)) return `line ${line} is not part of the diff`;
  }
  return undefined;
};

/**
 * Turn one validated review into the exact GitHub publication payload.
 * `applyVerdict: false` (the safe default) always posts a COMMENT review;
 * `true` maps the model's verdict onto APPROVE / REQUEST_CHANGES.
 */
export const planPublication = (
  review: CodeReview,
  files: ReadonlyArray<ChangedFile>,
  options: {
    readonly applyVerdict: boolean;
    /** Head commit the changeset was fetched at (pins the posted review). */
    readonly headSha: string;
    /** GitHub's changed-file total, for honest truncation reporting. */
    readonly totalChangedFiles: number;
    /** Base/head refs for the staleness metadata comment. */
    readonly baseRef?: string | undefined;
    readonly headRef?: string | undefined;
    /** Provider binding descriptor rendered into the footer. */
    readonly modelLabel?: string | undefined;
    /** Workflow-run URL rendered into the footer. */
    readonly runUrl?: string | undefined;
    /** Observed run usage rendered into the footer. */
    readonly usage?: { readonly inputTokens: number; readonly outputTokens: number } | undefined;
    /** What the usage observed: the whole run, or the coordinator only. */
    readonly usageScope?: "run" | "coordinator" | undefined;
    /**
     * Changeset fingerprint embedded invisibly in the review body so a later
     * run can skip re-reviewing an unchanged changeset.
     */
    readonly fingerprint?: string | undefined;
    /** Host-owned coverage; incomplete coverage is rendered and fails the check. */
    readonly coverage?: ReviewCoverage | undefined;
    /** Unchanged unresolved items carried from the prior successfully reviewed head. */
    readonly carriedFindings?: ReadonlyArray<ReviewFinding> | undefined;
    readonly carriedConcerns?: ReadonlyArray<ReviewConcern> | undefined;
    /** Selected review scope, made visible whenever orchestration chose it. */
    readonly reviewMode?: ReviewScopeMode | undefined;
    readonly reviewReason?: string | undefined;
    readonly baselineSha?: string | undefined;
    readonly reviewFilesVisible?: number | undefined;
    readonly reviewTotalFiles?: number | undefined;
    /** Durable state is emitted only after complete host-owned coverage. */
    readonly state?: ReviewState | undefined;
  },
): ReviewPublicationPlan => {
  const comments: Array<ReviewCommentDraft> = [];
  const demoted: Array<{ readonly finding: ReviewFinding; readonly reason: string }> = [];
  for (const finding of review.findings) {
    const violation = anchorViolation(finding, files);
    if (violation === undefined) {
      comments.push(
        ReviewCommentDraft.make({
          path: finding.path,
          line: finding.endLine,
          ...(finding.endLine > finding.startLine ? { startLine: finding.startLine } : {}),
          body: renderCommentBody(finding),
        }),
      );
    } else {
      demoted.push({ finding, reason: violation });
    }
  }

  // Rendered most-severe first so the size cap below sheds the least severe.
  const sortedConcerns = [...(review.concerns ?? [])].sort(
    (a, b) => severityRank[a.severity] - severityRank[b.severity],
  );
  const sortedDemoted = [...demoted].sort(
    (a, b) => severityRank[a.finding.severity] - severityRank[b.finding.severity],
  );

  const footerParts = ["Automated review by @effect-agent/pr-review"];
  if (options.modelLabel !== undefined) footerParts.push(options.modelLabel);
  // Usage renders only under an EXPLICIT scope: this planner cannot know
  // whether a budget snapshot observed the whole run or only a fan-out
  // coordinator, and omitting the number is honest where mislabeling is not.
  if (options.usage !== undefined && options.usageScope !== undefined) {
    const scope = options.usageScope === "coordinator" ? " (coordinator)" : "";
    footerParts.push(
      `${options.usage.inputTokens} in / ${options.usage.outputTokens} out tokens${scope}`,
    );
  }
  if (options.runUrl !== undefined) footerParts.push(`[run](${options.runUrl})`);
  footerParts.push(`reviewed at ${options.headSha.slice(0, 7)}`);
  const footer = `_${footerParts.join(" · ")}._`;

  const renderHead = (concernsKept: number, demotedKept: number, omitted: number): string => {
    const carriedFindings = options.carriedFindings ?? [];
    const carriedConcerns = options.carriedConcerns ?? [];
    const parts = [
      renderVerdictCallout(review, {
        carriedFindings,
        carriedConcerns,
        coverage: options.coverage,
      }),
    ];
    if (options.reviewMode !== undefined && options.reviewReason !== undefined) {
      parts.push(
        "",
        options.reviewMode === "incremental"
          ? `**Incremental scope:** reviewed ${options.reviewFilesVisible ?? files.length} file(s) ${options.reviewReason}. Unchanged accepted scope was preserved and not reopened.`
          : `**Full-diff scope:** ${options.reviewReason}.`,
      );
    }
    parts.push("", review.summary);
    if (options.coverage?.status === "incomplete") {
      parts.push(
        "",
        "### 🛑 Incomplete coverage",
        "",
        ...options.coverage.reasons.map((reason) => `- ${reason}`),
      );
    }
    if (carriedFindings.length > 0) {
      parts.push(
        "",
        "### Unresolved findings carried from unchanged scope",
        "",
        ...carriedFindings.map(renderCarriedFinding),
      );
    }
    if (carriedConcerns.length > 0) {
      parts.push("", "### Unresolved concerns carried to the final audit");
      for (const concern of carriedConcerns) parts.push("", renderConcern(concern));
    }
    for (const concern of sortedConcerns.slice(0, concernsKept)) {
      parts.push("", renderConcern(concern));
    }
    if (files.length < options.totalChangedFiles) {
      parts.push(
        "",
        `⚠️ Reviewed ${files.length} of ${options.totalChangedFiles} changed files — the changeset exceeded the reviewer's file bound.`,
      );
    }
    if (demotedKept > 0) {
      parts.push(
        "",
        "### Findings without a valid diff anchor",
        ...sortedDemoted
          .slice(0, demotedKept)
          .map(({ finding, reason }) => renderDemoted(finding, reason)),
      );
    }
    if (omitted > 0) {
      parts.push(
        "",
        `⚠️ ${countNoun(omitted, "review item")} omitted — the body exceeded GitHub's review size cap.`,
      );
    }
    parts.push("", footer);
    return parts.join("\n");
  };

  // The model's verdict may not contradict the reported severities (model
  // output is untrusted input): any blocking item forces REQUEST_CHANGES, a
  // review with no blocking item can never REQUEST_CHANGES, and an approval
  // is honored only when nothing blocking or important was reported — the
  // event always agrees with the callout tier. Demoted findings and concerns
  // count like anchored findings: anchor validation validates LOCATIONS, not
  // truth, so severity is equally model-claimed for all three, and counting
  // them only ever moves the event toward the closed direction.
  const counts = severityCounts(
    review,
    options.carriedFindings ?? [],
    options.carriedConcerns ?? [],
  );
  const event: ReviewEvent = !options.applyVerdict
    ? "COMMENT"
    : counts.blocking > 0
      ? "REQUEST_CHANGES"
      : review.verdict === "approve" && counts.important === 0
        ? "APPROVE"
        : "COMMENT";

  // The invisible tail (metadata + fingerprint marker) must survive the body
  // cap, so the cap reserves exactly the room it needs.
  const tail = [
    renderReviewMetadata({
      headSha: options.headSha,
      baseRef: options.baseRef,
      headRef: options.headRef,
      filesVisible: options.reviewFilesVisible ?? files.length,
      totalChangedFiles: options.reviewTotalFiles ?? options.totalChangedFiles,
      reviewMode: options.reviewMode,
      baselineSha: options.baselineSha,
    }),
    ...(options.fingerprint === undefined ? [] : [renderFingerprintMarker(options.fingerprint)]),
    ...(options.state === undefined ? [] : [renderReviewStateMarker(options.state)]),
  ].join("\n");
  const headBudget = 60_000 - tail.length - 1;

  // Shed whole trailing items — demoted bullets first (they already failed
  // validation), then concerns — instead of slicing markdown mid-block. Every
  // omission is announced, and `plan.demoted` keeps the full data regardless.
  let concernsKept = sortedConcerns.length;
  let demotedKept = sortedDemoted.length;
  let omitted = 0;
  let head = renderHead(concernsKept, demotedKept, omitted);
  while (head.length > headBudget && (demotedKept > 0 || concernsKept > 0)) {
    if (demotedKept > 0) demotedKept -= 1;
    else concernsKept -= 1;
    omitted += 1;
    head = renderHead(concernsKept, demotedKept, omitted);
  }
  // Last resort for a pathological summary; unreachable while the CodeReview
  // schema caps the summary well below the budget.
  const body = `${head.slice(0, headBudget)}\n${tail}`;

  return ReviewPublicationPlan.make({
    event,
    body,
    comments,
    demoted: demoted.map(({ finding }) => finding),
    commitSha: options.headSha,
  });
};

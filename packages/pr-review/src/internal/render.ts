import { Schema } from "effect";

import type { ChangedFile } from "./diff.ts";
import { commentableLines } from "./diff.ts";
import { FINGERPRINT_MARKER_LENGTH, renderFingerprintMarker } from "./fingerprint.ts";
import type { CodeReview } from "./review-agent.ts";
import { ReviewFinding } from "./review-agent.ts";

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

const severityLabel: Record<ReviewFinding["severity"], string> = {
  blocking: "🛑 blocking",
  important: "⚠️ important",
  nit: "💅 nit",
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

const renderDemoted = (finding: ReviewFinding): string => {
  const location = `\`${finding.path}:${finding.startLine}${
    finding.endLine !== finding.startLine ? `-${finding.endLine}` : ""
  }\``;
  return [
    `- ${location} **[${severityLabel[finding.severity]}] ${finding.title}** — ${finding.body}`,
  ].join("\n");
};

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
    /**
     * Changeset fingerprint embedded invisibly in the review body so a later
     * run can skip re-reviewing an unchanged changeset.
     */
    readonly fingerprint?: string | undefined;
  },
): ReviewPublicationPlan => {
  const comments: Array<ReviewCommentDraft> = [];
  const demoted: Array<ReviewFinding> = [];
  for (const finding of review.findings) {
    if (anchorViolation(finding, files) === undefined) {
      comments.push(
        ReviewCommentDraft.make({
          path: finding.path,
          line: finding.endLine,
          ...(finding.endLine > finding.startLine ? { startLine: finding.startLine } : {}),
          body: renderCommentBody(finding),
        }),
      );
    } else {
      demoted.push(finding);
    }
  }

  const bodyParts = [review.summary];
  if (files.length < options.totalChangedFiles) {
    bodyParts.push(
      "",
      `⚠️ Reviewed ${files.length} of ${options.totalChangedFiles} changed files — the changeset exceeded the reviewer's file bound.`,
    );
  }
  if (demoted.length > 0) {
    bodyParts.push("", "### Findings without a valid diff anchor", ...demoted.map(renderDemoted));
  }
  bodyParts.push(
    "",
    `_Automated review by @effect-agent/pr-review · reviewed at ${options.headSha.slice(0, 7)}._`,
  );

  const event: ReviewEvent = options.applyVerdict
    ? review.verdict === "approve"
      ? "APPROVE"
      : review.verdict === "request-changes"
        ? "REQUEST_CHANGES"
        : "COMMENT"
    : "COMMENT";

  // The marker must survive the body cap, so the cap reserves room for it.
  const body =
    options.fingerprint === undefined
      ? bodyParts.join("\n").slice(0, 60_000)
      : `${bodyParts.join("\n").slice(0, 60_000 - FINGERPRINT_MARKER_LENGTH - 1)}\n${renderFingerprintMarker(options.fingerprint)}`;

  return ReviewPublicationPlan.make({
    event,
    body,
    comments,
    demoted,
    commitSha: options.headSha,
  });
};

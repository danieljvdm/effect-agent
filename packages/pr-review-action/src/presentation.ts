import type {
  ReviewFinding,
  ReviewOutcome,
  ReviewReport,
  ReviewSeverity,
} from "@effect-agent/pr-review";
import { Context } from "effect";

import { reviewMarker, reviewPauseMarker } from "./selection.ts";

const severityAppearance: Record<
  ReviewSeverity,
  { readonly icon: string; readonly label: string }
> = {
  blocking: { icon: "🛑", label: "blocking" },
  important: { icon: "⚠️", label: "important" },
  nit: { icon: "💅", label: "nit" },
};

const countNoun = (count: number, noun: string): string =>
  `${String(count)} ${noun}${count === 1 ? "" : "s"}`;

const formatNumber = (value: number): string => String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ",");

const findingLabel = (finding: ReviewFinding): string => {
  const appearance = severityAppearance[finding.severity];
  return `${appearance.icon} ${appearance.label} · ${finding.category}`;
};

const severityCounts = (report: ReviewReport) => ({
  blocking: report.findings.filter((finding) => finding.severity === "blocking").length,
  important: report.findings.filter((finding) => finding.severity === "important").length,
  nit: report.findings.filter((finding) => finding.severity === "nit").length,
});

const renderFindingTally = (input: ReviewPresentationInput): string => {
  const counts = severityCounts(input.report);
  const parts = [
    ...(counts.blocking > 0 ? [`🛑 ${String(counts.blocking)} blocking`] : []),
    ...(counts.important > 0 ? [`⚠️ ${String(counts.important)} important`] : []),
    ...(counts.nit > 0 ? [`💅 ${String(counts.nit)} nit`] : []),
  ];
  if (parts.length > 0) return parts.join(" · ");
  if (!input.complete || input.exhausted !== undefined) return "None recorded · incomplete";
  return input.unresolvedChangeRequests > 0 ? "None recorded" : "✅ None";
};

const renderVerdict = (
  report: ReviewReport,
  complete: boolean,
  unresolvedChangeRequests: number,
  exhausted: ReviewOutcome["exhausted"],
): string => {
  const counts = severityCounts(report);
  if (counts.blocking > 0) {
    return `> [!CAUTION]\n> **${countNoun(counts.blocking, "blocking finding")}.** Do not merge until ${counts.blocking === 1 ? "it is" : "they are"} addressed.`;
  }
  if (exhausted !== undefined) {
    return `> [!CAUTION]\n> **Review stopped at the ${exhausted} budget.** Findings are preserved, but coverage is incomplete and this result does not clear the change.`;
  }
  if (!complete) {
    return "> [!CAUTION]\n> **Review coverage is incomplete.** Not all supplied changes were verified, so this result does not clear the change.";
  }
  if (unresolvedChangeRequests > 0) {
    return `> [!CAUTION]\n> **No new blocking finding clears ${countNoun(unresolvedChangeRequests, "earlier change request")}.** A maintainer must dismiss it explicitly after verifying the fix.`;
  }
  if (counts.important > 0) {
    return `> [!IMPORTANT]\n> **${countNoun(counts.important, "important finding")}.** Address before merging.`;
  }
  if (counts.nit > 0) {
    return `> [!NOTE]\n> **${countNoun(counts.nit, "minor finding")}.**`;
  }
  return "> [!TIP]\n> **No actionable findings.**";
};

export const renderFindingBody = (finding: ReviewFinding): string =>
  [`**[${findingLabel(finding)}] ${finding.title}**`, "", finding.body].join("\n");

const renderUnanchoredFindingText = (finding: ReviewFinding): string =>
  [`[${findingLabel(finding)}] ${finding.title}`, `Path: ${finding.path}`, "", finding.body].join(
    "\n",
  );

const fencedPlainText = (text: string): string => {
  let longestRun = 0;
  let currentRun = 0;
  for (const character of text) {
    if (character === "`") {
      currentRun += 1;
      longestRun = Math.max(longestRun, currentRun);
    } else {
      currentRun = 0;
    }
  }
  const fence = "`".repeat(Math.max(3, longestRun + 1));
  return `${fence}text\n${text}\n${fence}`;
};

export interface ReviewPresentationInput {
  readonly report: ReviewReport;
  readonly automaticReviewsRemaining: number;
  readonly scope: "full" | "incremental";
  readonly reviewedFiles: number;
  readonly unreviewedFiles: number;
  readonly ignoredFiles: number;
  readonly modelTurns: number;
  readonly complete: boolean;
  readonly exhausted?: ReviewOutcome["exhausted"];
  readonly unresolvedChangeRequests: number;
  readonly inputTokens: number;
  readonly uncachedInputTokens: number;
  readonly cachedInputTokens: number;
  readonly cacheWriteInputTokens: number;
  readonly outputTokens: number;
  readonly estimatedCost?: ReviewCostEstimate | undefined;
  readonly reservedCostMicrousd?: number;
  readonly costLimitMicrousd?: number;
  readonly headRevision: string;
}

export interface ReviewCostEstimate {
  readonly microusd: number;
  readonly label: string;
  readonly url: string;
}

const renderCoverage = (input: ReviewPresentationInput): string =>
  [
    `${String(input.reviewedFiles)} ${input.complete ? "reviewed" : "supplied"}`,
    ...(input.unreviewedFiles > 0 ? [`${String(input.unreviewedFiles)} unavailable`] : []),
    ...(input.ignoredFiles > 0 ? [`${String(input.ignoredFiles)} ignored`] : []),
  ].join(" · ");

const formatEstimatedUsd = (microusd: number): string => {
  const dollars = microusd / 1_000_000;
  const digits =
    (dollars > 0 && dollars < 0.0001) || (dollars >= 0.9999 && dollars < 1)
      ? 6
      : dollars < 1
        ? 4
        : 2;
  return `$${dollars.toFixed(digits)}`;
};

const renderInputUsage = (input: ReviewPresentationInput): string =>
  `${formatNumber(input.inputTokens)} input (${formatNumber(input.uncachedInputTokens)} uncached · ${formatNumber(input.cachedInputTokens)} cached · ${formatNumber(input.cacheWriteInputTokens)} cache write; ${(input.inputTokens === 0 ? 0 : (100 * input.cachedInputTokens) / input.inputTokens).toFixed(1)}% cache reads)`;

const renderAutomaticPause = (automaticReviewsRemaining: number): string | undefined =>
  automaticReviewsRemaining > 0
    ? undefined
    : [
        "> [!NOTE]",
        "> **Automatic reviews are paused for this pull request.**",
        "> Further pushes will not start another review. Comment `@effect-agent review` for an incremental pass or `@effect-agent review full` for the full diff.",
      ].join("\n");

export const renderReviewBody = (input: ReviewPresentationInput): string => {
  const unanchored = input.report.findings.filter((finding) => finding.line === undefined);
  const parts = [
    "## Effect Agent review",
    renderVerdict(input.report, input.complete, input.unresolvedChangeRequests, input.exhausted),
    [
      "| Scope | Files | New findings |",
      "| :-- | :-- | :-- |",
      `| **${input.scope === "full" ? "Full diff" : "Incremental"}** | ${renderCoverage(input)} | ${renderFindingTally(input)} |`,
    ].join("\n"),
  ];
  const automaticPause = renderAutomaticPause(input.automaticReviewsRemaining);
  if (automaticPause !== undefined) parts.push(automaticPause);
  parts.push("### Summary", input.report.summary);

  if (unanchored.length > 0) {
    const findingText = unanchored.map(renderUnanchoredFindingText).join("\n\n---\n\n");
    parts.push(
      [
        `### Findings without an inline anchor (${String(unanchored.length)})`,
        "",
        fencedPlainText(findingText),
      ].join("\n"),
    );
  }

  const modelLabel =
    input.modelTurns === 0 ? "No model call" : countNoun(input.modelTurns, "model call");
  const usage =
    input.modelTurns === 0
      ? ""
      : ` · ${renderInputUsage(input)} / ${formatNumber(input.outputTokens)} output tokens`;
  const estimatedCost =
    input.estimatedCost === undefined
      ? ""
      : ` · ≈ ${formatEstimatedUsd(input.estimatedCost.microusd)} at <a href="${input.estimatedCost.url}">${input.estimatedCost.label} rates</a>`;
  const pendingCost =
    (input.reservedCostMicrousd ?? 0) === 0
      ? ""
      : ` · up to ${formatEstimatedUsd(input.reservedCostMicrousd ?? 0)} awaiting usage`;
  const costLimit =
    input.costLimitMicrousd === undefined
      ? ""
      : ` · $${(input.costLimitMicrousd / 1_000_000).toFixed(6)} spending ceiling`;
  const automaticReviewStatus =
    input.automaticReviewsRemaining === 0
      ? ""
      : input.automaticReviewsRemaining === 1
        ? " · 1 automatic review remains"
        : ` · ${String(input.automaticReviewsRemaining)} automatic reviews remain`;
  const footer = `<sub>${modelLabel}${usage}${estimatedCost}${pendingCost}${costLimit} · inspected at <code>${input.headRevision.slice(0, 7)}</code>${automaticReviewStatus}</sub>`;

  parts.push(footer);
  return parts.join("\n\n");
};

export interface ReviewFailurePresentationInput {
  readonly automaticReviewsRemaining: number;
  /** Host-authored explanation; never raw provider diagnostics or model output. */
  readonly failureSummary?: string | undefined;
}

export const renderReviewFailureBody = (input: ReviewFailurePresentationInput): string => {
  const parts = [
    "## Effect Agent review",
    "> [!CAUTION]\n> The review failed before it could publish findings.",
    input.failureSummary ?? "Review preparation or a model pass failed.",
    "This attempt does not advance the baseline or clear earlier change requests.",
    "Check the Action log for details. Comment `@effect-agent review full` to retry the full diff.",
  ];
  const automaticPause = renderAutomaticPause(input.automaticReviewsRemaining);
  if (automaticPause !== undefined) parts.push(automaticPause);
  return parts.join("\n\n");
};

export interface ReviewPausePresentationInput {
  readonly automaticReviewLimit: number;
  readonly automaticAttempts: number;
  readonly lastCompletedRevision: string | undefined;
  readonly headRevision: string;
  readonly unresolvedChangeRequests: number;
}

export const renderReviewPauseBody = (input: ReviewPausePresentationInput): string => {
  const attempts =
    input.automaticAttempts === input.automaticReviewLimit
      ? `${String(input.automaticAttempts)} of ${String(input.automaticReviewLimit)} used`
      : `${String(input.automaticAttempts)} recorded · limit ${String(input.automaticReviewLimit)}`;
  const lastCompleted =
    input.lastCompletedRevision === undefined
      ? "None"
      : `<code>${input.lastCompletedRevision.slice(0, 7)}</code>`;
  const unresolved =
    input.unresolvedChangeRequests === 0
      ? []
      : [
          `> [!CAUTION]\n> **${countNoun(input.unresolvedChangeRequests, "earlier change request")} remains unresolved.** This pause notice does not clear it.`,
        ];
  return [
    "## Effect Agent review",
    [
      "> [!NOTE]",
      "> **Automatic reviews are paused for this pull request.**",
      "> The configured automatic review limit has been reached. No model call was made for this update.",
    ].join("\n"),
    ...unresolved,
    [
      "| Automatic attempts | Last completed review | Current head |",
      "| :-- | :-- | :-- |",
      `| **${attempts}** | ${lastCompleted} | <code>${input.headRevision.slice(0, 7)}</code> |`,
    ].join("\n"),
    "### Summary",
    "Further pushes will not start another automatic model review, and this pause notice will not be posted again.",
    "Comment `@effect-agent review` for another review of the latest changes, or `@effect-agent review full` for the full pull request diff.",
    `<sub>No model call · review automation paused at <code>${input.headRevision.slice(0, 7)}</code></sub>`,
  ].join("\n\n");
};

export interface ReviewPresentation {
  readonly renderFinding: (finding: ReviewFinding, headRevision: string) => string;
  readonly renderReview: (input: ReviewPresentationInput) => string;
  readonly renderFailure: (input: ReviewFailurePresentationInput) => string;
  readonly renderPause: (input: ReviewPausePresentationInput) => string;
}

export const defaultReviewPresentation: ReviewPresentation = {
  renderFinding: renderFindingBody,
  renderReview: renderReviewBody,
  renderFailure: renderReviewFailureBody,
  renderPause: renderReviewPauseBody,
};

export const ReviewPresentation: Context.Reference<ReviewPresentation> =
  Context.Reference<ReviewPresentation>("@effect-agent/pr-review-action/ReviewPresentation", {
    defaultValue: () => defaultReviewPresentation,
  });

const withTerminalMarker = (body: string, marker: string): string => {
  const visibleBody = body.trimEnd();
  return visibleBody.length === 0 ? marker : `${visibleBody}\n\n${marker}`;
};

/** Keep the trusted attempt marker outside host-replaceable presentation. */
export const withReviewMarker = (body: string, automatic: boolean, completed = true): string => {
  return withTerminalMarker(body, reviewMarker(automatic, completed));
};

/** Keep the trusted one-time pause marker outside host-replaceable presentation. */
export const withReviewPauseMarker = (body: string, automaticReviewLimit: number): string =>
  withTerminalMarker(body, reviewPauseMarker(automaticReviewLimit));

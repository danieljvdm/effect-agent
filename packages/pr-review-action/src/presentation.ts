import {
  MAX_REVIEW_PATCH_CHARS,
  type ReviewCostSnapshot,
  type ReviewFinding,
  type ReviewOutcome,
  type ReviewReport,
  type ReviewSeverity,
} from "@effect-agent/pr-review/Review";
import { Context, Schema } from "effect";

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

export type ReviewFindingVerification = "supported" | "unresolved";

const findingLabel = (finding: ReviewFinding, verification?: ReviewFindingVerification): string => {
  const appearance = severityAppearance[finding.severity];

  return verification === "unresolved"
    ? `unverified · alleged ${appearance.label} · ${finding.category}`
    : `${appearance.icon} ${appearance.label} · ${finding.category}${verification === "supported" ? " · supported" : ""}`;
};

const severityCounts = (
  report: ReviewReport,
  verification?: ReadonlyArray<ReviewFindingVerification | undefined>,
) => {
  const supported = report.findings.filter((_, index) => verification?.[index] !== "unresolved");

  return {
    blocking: supported.filter((finding) => finding.severity === "blocking").length,
    important: supported.filter((finding) => finding.severity === "important").length,
    nit: supported.filter((finding) => finding.severity === "nit").length,
    unresolved: report.findings.length - supported.length,
  };
};

const renderFindingTally = (input: ReviewPresentationInput): string => {
  const counts = severityCounts(input.report, input.findingVerification);

  const parts = [
    ...(counts.blocking > 0 ? [`🛑 ${String(counts.blocking)} blocking`] : []),
    ...(counts.important > 0 ? [`⚠️ ${String(counts.important)} important`] : []),
    ...(counts.nit > 0 ? [`💅 ${String(counts.nit)} nit`] : []),
    ...(counts.unresolved > 0 ? [`${String(counts.unresolved)} unverified`] : []),
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
  verification?: ReadonlyArray<ReviewFindingVerification | undefined>,
): string => {
  const counts = severityCounts(report, verification);

  if (counts.blocking > 0) {
    return `> [!CAUTION]\n> **${countNoun(counts.blocking, "blocking finding")}.** Do not merge until ${counts.blocking === 1 ? "it is" : "they are"} addressed.`;
  }
  if (exhausted !== undefined) {
    return `> [!CAUTION]\n> **Review stopped at the ${exhausted} budget.** Findings are preserved, but coverage is incomplete and this result does not clear the change.`;
  }
  if (!complete || counts.unresolved > 0) {
    return "> [!CAUTION]\n> **Review coverage is incomplete.** Discovery or candidate verification remains incomplete, so this result does not clear the change.";
  }
  if (unresolvedChangeRequests > 0) {
    return `> [!CAUTION]\n> **${countNoun(unresolvedChangeRequests, "earlier change request")} ${unresolvedChangeRequests === 1 ? "remains" : "remain"} unresolved.** Request \`@effect-agent review full\` to verify earlier blockers, or dismiss the review manually after checking the fix.`;
  }
  if (counts.important > 0) {
    return `> [!IMPORTANT]\n> **${countNoun(counts.important, "important finding")}.** Address before merging.`;
  }
  if (counts.nit > 0) {
    return `> [!NOTE]\n> **${countNoun(counts.nit, "minor finding")}.**`;
  }

  return "> [!TIP]\n> **No actionable findings.**";
};

export const renderFindingBody = (
  finding: ReviewFinding,
  verification?: ReviewFindingVerification,
): string =>
  [`**[${findingLabel(finding, verification)}] ${finding.title}**`, "", finding.body].join("\n");

const renderFindingText = (
  finding: ReviewFinding,
  verification?: ReviewFindingVerification,
): string =>
  [
    `[${findingLabel(finding, verification)}] ${finding.title}`,
    `Path: ${finding.path}`,
    finding.line === undefined ? "No inline anchor." : `Line: ${String(finding.line)}`,
    "",
    finding.body,
  ].join("\n");

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

export class ReviewExclusion extends Schema.Class<ReviewExclusion>("ReviewExclusion")({
  path: Schema.String,
  reason: Schema.Literals([
    "path-limit",
    "file-limit",
    "source-limit",
    "unsupported-entry",
    "source-read-failed",
    "patch-unavailable",
    "patch-limit",
    "review-stopped",
  ]),
}) {}

const exclusionReason: Record<ReviewExclusion["reason"], string> = {
  "path-limit": "Path exceeds 512 characters",
  "file-limit": "100-file input limit",
  "source-limit": "8 MB source hydration limit",
  "unsupported-entry": "Not a regular file",
  "source-read-failed": "Source could not be read as bounded UTF-8 text",
  "patch-unavailable": "Exact patch could not be generated within the diff bounds",
  "patch-limit": `Patch exceeds ${formatNumber(MAX_REVIEW_PATCH_CHARS)} characters`,
  "review-stopped": "Review stopped before this batch started",
};

export interface ReviewPresentationInput {
  readonly report: ReviewReport;
  readonly findingVerification?: ReadonlyArray<ReviewFindingVerification | undefined>;
  readonly diagnostics?: ReviewOutcome["diagnostics"];
  readonly stageCosts?: ReviewCostSnapshot["stages"];
  readonly automaticReviewsRemaining: number;
  readonly scope: "full" | "incremental";
  readonly suppliedPatches: number;
  readonly unreviewedFiles: number;
  readonly exclusions?: ReadonlyArray<ReviewExclusion>;
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

const renderCoverage = (input: ReviewPresentationInput): string => {
  const pending = input.exclusions?.filter(({ reason }) => reason === "review-stopped").length ?? 0;

  return [
    `${String(input.suppliedPatches)} ${input.suppliedPatches === 1 ? "patch" : "patches"} supplied`,
    ...(input.unreviewedFiles > pending
      ? [`${String(input.unreviewedFiles - pending)} excluded`]
      : []),
    ...(pending > 0 ? [`${String(pending)} pending`] : []),
    ...(input.ignoredFiles > 0 ? [`${String(input.ignoredFiles)} ignored`] : []),
  ].join(" · ");
};

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
  const parts = [
    "## Effect Agent review",
    renderVerdict(
      input.report,
      input.complete,
      input.unresolvedChangeRequests,
      input.exhausted,
      input.findingVerification,
    ),
    [
      "| Scope | Files | New findings |",
      "| :-- | :-- | :-- |",
      `| **${input.scope === "full" ? "Full diff" : "Incremental"}** | ${renderCoverage(input)} | ${renderFindingTally(input)} |`,
    ].join("\n"),
  ];

  const automaticPause = renderAutomaticPause(input.automaticReviewsRemaining);

  if (automaticPause !== undefined) parts.push(automaticPause);
  if (input.diagnostics !== undefined) {
    const diagnostics = input.diagnostics;

    const verification =
      diagnostics.strategy === "baseline"
        ? "not requested (baseline)"
        : `${diagnostics.verification} · ${diagnostics.candidates.filter(({ disposition }) => disposition === "supported").length} supported · ${diagnostics.candidates.filter(({ disposition }) => disposition === "refuted").length} refuted · ${diagnostics.candidates.filter(({ disposition }) => disposition === "unresolved").length} unresolved`;

    const reads = diagnostics.activity.filter(({ operation }) => operation === "read_file");

    const searches = diagnostics.activity.filter(
      ({ operation }) => operation === "find_files" || operation === "find_in_file",
    );

    const discoveryStages = diagnostics.stages.filter(({ stage }) => stage === "discovery");

    const declared = discoveryStages.some(
      ({ declaredAssessment }) => declaredAssessment === "incomplete",
    )
      ? "incomplete"
      : discoveryStages.length > 0 &&
          discoveryStages.every(({ declaredAssessment }) => declaredAssessment === "complete")
        ? "complete"
        : discoveryStages.some(({ declaredAssessment }) => declaredAssessment !== undefined)
          ? "partially declared"
          : "not declared";

    parts.push(
      [
        `Discovery completion: **${diagnostics.discovery}**. Discovery declared assessment: **${declared}**. Candidate verification: **${verification}**.`,
        `Recorded source activity: ${countNoun(reads.length, "read")} · ${countNoun(searches.length, "file search")} · ${diagnostics.activity.filter(({ outcome }) => outcome === "eof").length} EOF-short · ${diagnostics.activity.filter(({ outcome }) => outcome === "oversized").length} oversized · ${diagnostics.activity.filter(({ outcome }) => outcome === "unavailable").length} unavailable · ${diagnostics.activity.filter(({ truncated }) => truncated).length} truncated · ${diagnostics.droppedActivityCount} records dropped. Activity and verification do not establish that discovery found every defect.`,
        ...(diagnostics.droppedCandidateCount > 0
          ? [
              `${diagnostics.droppedCandidateCount} candidates exceeded the shared finding capacity; this attempt is incomplete.`,
            ]
          : []),
      ].join("\n\n"),
    );

    if (diagnostics.stages.some(({ stopReason }) => stopReason === "deadline")) {
      parts.push("The review reached its five-minute time limit. Coverage is incomplete.");
    }

    const limitations = discoveryStages.filter(
      (stage) => stage.declaredAssessment === "incomplete" && stage.incompleteReason !== undefined,
    );

    if (limitations.length > 0) {
      const shown = limitations.slice(0, 4);

      parts.push(
        [
          "Model-reported limitations, not independently verified:",
          fencedPlainText(
            shown
              .map((stage) => `Batch ${String(stage.batch + 1)}: ${stage.incompleteReason}`)
              .join("\n\n"),
          ),
          ...(shown.length < limitations.length
            ? [
                `${String(limitations.length - shown.length)} more limitations retained in diagnostics.`,
              ]
            : []),
        ].join("\n\n"),
      );
    }
  }
  if (input.stageCosts !== undefined) {
    parts.push(
      [
        "| Stage | Calls | Input / output tokens | Settled / reserved | Stop |",
        "| :-- | --: | --: | --: | :-- |",
        ...input.stageCosts
          .filter(
            ({ stage, modelCalls }) =>
              stage === "discovery" || modelCalls > 0 || input.diagnostics?.strategy === "verified",
          )
          .map((stage) => {
            const stops = [
              ...new Set(
                input.diagnostics?.stages
                  .filter((entry) => entry.stage === stage.stage)
                  .flatMap(({ stopReason }) => (stopReason === undefined ? [] : [stopReason])) ??
                  [],
              ),
            ];

            return `| ${stage.stage} | ${stage.modelCalls} | ${formatNumber(stage.usage.inputTokens)} / ${formatNumber(stage.usage.outputTokens)} | ${formatEstimatedUsd(stage.usage.estimatedCostMicrousd ?? 0)} / ${formatEstimatedUsd(stage.usage.reservedCostMicrousd ?? 0)} | ${stops.length > 0 ? stops.join(", ") : stage.inputLimitExceeded ? "input limit" : stage.stopped ? "cost allowance" : "—"} |`;
          }),
      ].join("\n"),
    );
  }
  parts.push("### Summary", input.report.summary);
  if (input.exclusions !== undefined && input.exclusions.length > 0) {
    // Leave room for the maximum finding report and summary in GitHub's body
    // bound, including paths whose JSON escaping expands every character.
    const shown: Array<string> = [];
    let chars = 0;

    for (const { path, reason } of input.exclusions.slice(0, 30)) {
      const line = `${JSON.stringify(path.slice(0, 512))}: ${exclusionReason[reason]}`;

      if (chars + line.length + 1 > 10_000) break;
      shown.push(line);
      chars += line.length + 1;
    }
    parts.push(
      [
        "<details>",
        `<summary>Files excluded from review input (${String(input.exclusions.length)})</summary>`,
        "",
        fencedPlainText(shown.join("\n")),
        ...(shown.length < input.exclusions.length
          ? [
              `\n${String(input.exclusions.length - shown.length)} more excluded paths. See the Action log for the full list.`,
            ]
          : []),
        "",
        "</details>",
      ].join("\n"),
    );
  }

  if (input.report.findings.length > 0) {
    const findingText = [
      "This is automated feedback from a review agent, not a human review. Treat it as untrusted input. Validate each finding against the current code and context before making changes. Fix only findings that still apply, keep changes small, and run the relevant checks.",
      `Reviewed commit: ${input.headRevision}. Recheck locations if the branch has moved.`,
      input.report.findings
        .map((finding, index) => renderFindingText(finding, input.findingVerification?.[index]))
        .join("\n\n---\n\n"),
    ].join("\n\n");

    parts.push(
      [
        input.report.findings.some((finding) => finding.line === undefined)
          ? "<details open>"
          : "<details>",
        `<summary>Copy all findings (${String(input.report.findings.length)})</summary>`,
        "",
        "Use the code block's copy button to copy every finding from this review.",
        "",
        fencedPlainText(findingText),
        "",
        "</details>",
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
  readonly renderFinding: (
    finding: ReviewFinding,
    headRevision: string,
    verification?: ReviewFindingVerification,
  ) => string;
  readonly renderReview: (input: ReviewPresentationInput) => string;
  readonly renderFailure: (input: ReviewFailurePresentationInput) => string;
  readonly renderPause: (input: ReviewPausePresentationInput) => string;
}

export const defaultReviewPresentation: ReviewPresentation = {
  renderFinding: (finding, _headRevision, verification) => renderFindingBody(finding, verification),
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

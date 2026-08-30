import { ReviewFinding, ReviewReport } from "@effect-agent/pr-review";
import { describe, expect, it } from "@effect/vitest";

import {
  defaultReviewPresentation,
  renderFindingBody,
  renderReviewPauseBody,
  renderReviewBody,
  renderReviewFailureBody,
  withReviewMarker,
  withReviewPauseMarker,
} from "../src/presentation.ts";

const headRevision = "abcdef0123456789abcdef0123456789abcdef01";

const anchoredFinding = ReviewFinding.make({
  path: "src/projection.ts",
  line: 1909,
  severity: "blocking",
  category: "correctness",
  title: "Retired source generations never settle",
  body: "The retired delivery remains projection-pending forever.",
});

describe("review presentation", () => {
  it("renders the inline finding once, without duplicating the agent handoff", () => {
    expect(renderFindingBody(anchoredFinding)).toMatchInlineSnapshot(`
      "**[🛑 blocking · correctness] Retired source generations never settle**

      The retired delivery remains projection-pending forever."
    `);
  });

  it("renders a severity callout, review stats, unanchored findings, and a batch prompt", () => {
    const unanchoredFinding = ReviewFinding.make({
      path: "src/auth.ts",
      severity: "important",
      category: "security",
      title: "Authorization is not enforced",
      body: "The new route accepts requests without checking the caller.",
    });
    const body = renderReviewBody({
      report: ReviewReport.make({
        summary: "The change has two actionable defects.",
        findings: [anchoredFinding, unanchoredFinding],
      }),
      automaticReviewsRemaining: 2,
      scope: "full",
      reviewedFiles: 2,
      unreviewedFiles: 1,
      ignoredFiles: 3,
      modelTurns: 5,
      complete: true,
      unresolvedChangeRequests: 0,
      inputTokens: 12_345,
      uncachedInputTokens: 10_000,
      cachedInputTokens: 2_000,
      cacheWriteInputTokens: 345,
      outputTokens: 678,
      estimatedCost: {
        microusd: 62_940,
        label: "GPT-5.6 Sol",
        url: "https://developers.openai.com/api/docs/models/gpt-5.6-sol",
      },
      headRevision,
    });

    expect(body).toContain("> [!CAUTION]\n> **1 blocking finding.** Do not merge");
    expect(body).toContain(
      "| **Full diff** | 2 reviewed · 1 unavailable · 3 ignored | 🛑 1 blocking · ⚠️ 1 important |",
    );
    expect(body).toContain("### Findings without an inline anchor (1)");
    expect(body).toContain("```text\n[⚠️ important · security] Authorization is not enforced");
    expect(body).toContain(
      '<sub>5 model turns · 12,345 input (10,000 uncached · 2,000 cached · 345 cache write) / 678 output tokens · ≈ $0.0629 at <a href="https://developers.openai.com/api/docs/models/gpt-5.6-sol">GPT-5.6 Sol rates</a> · reviewed at <code>abcdef0</code> · 2 automatic reviews remain</sub>',
    );
  });

  it("renders a clean review as a compact receipt", () => {
    const body = withReviewMarker(
      defaultReviewPresentation.renderReview({
        report: ReviewReport.make({
          summary: "No actionable defects found in the supplied diff.",
          findings: [],
        }),
        automaticReviewsRemaining: 0,
        scope: "incremental",
        reviewedFiles: 3,
        unreviewedFiles: 0,
        ignoredFiles: 2,
        modelTurns: 3,
        complete: true,
        unresolvedChangeRequests: 0,
        inputTokens: 2_267,
        uncachedInputTokens: 2_000,
        cachedInputTokens: 200,
        cacheWriteInputTokens: 67,
        outputTokens: 456,
        estimatedCost: {
          microusd: 18_188,
          label: "GPT-5.6 Sol",
          url: "https://developers.openai.com/api/docs/models/gpt-5.6-sol",
        },
        headRevision,
      }),
      true,
    );

    expect(body).toMatchInlineSnapshot(`
      "## Effect Agent review

      > [!TIP]
      > **No actionable findings.**

      | Scope | Files | New findings |
      | :-- | :-- | :-- |
      | **Incremental** | 3 reviewed · 2 ignored | ✅ None |

      > [!NOTE]
      > **Automatic reviews are paused for this pull request.**
      > Further pushes will not start another review. Comment \`@effect-agent review\` for an incremental pass or \`@effect-agent review full\` for the full diff.

      ### Summary

      No actionable defects found in the supplied diff.

      <sub>3 model turns · 2,267 input (2,000 uncached · 200 cached · 67 cache write) / 456 output tokens · ≈ $0.0182 at <a href="https://developers.openai.com/api/docs/models/gpt-5.6-sol">GPT-5.6 Sol rates</a> · reviewed at <code>abcdef0</code></sub>

      <!-- effect-agent-review:v3 automatic=true completed=true -->"
    `);
  });

  it("keeps attempt accounting outside replaceable presentation", () => {
    expect(withReviewMarker("Custom presentation", false, false)).toBe(
      "Custom presentation\n\n<!-- effect-agent-review:v3 automatic=false completed=false -->",
    );
  });

  it("renders a one-time closing review without a model call", () => {
    const body = withReviewPauseMarker(
      renderReviewPauseBody({
        automaticReviewLimit: 2,
        automaticAttempts: 2,
        lastCompletedRevision: "840c401c8e3103efcb70bd602e88b85834bd021d",
        headRevision,
        unresolvedChangeRequests: 0,
      }),
      2,
    );

    expect(body).toMatchInlineSnapshot(`
      "## Effect Agent review

      > [!NOTE]
      > **Automatic reviews are paused for this pull request.**
      > The configured automatic review limit has been reached. No model call was made for this update.

      | Automatic attempts | Last completed review | Current head |
      | :-- | :-- | :-- |
      | **2 of 2 used** | <code>840c401</code> | <code>abcdef0</code> |

      ### Summary

      Further pushes will not start another automatic model review, and this pause notice will not be posted again.

      Comment \`@effect-agent review\` for another review of the latest changes, or \`@effect-agent review full\` for the full pull request diff.

      <sub>No model call · review automation paused at <code>abcdef0</code></sub>

      <!-- effect-agent-review-pause:v1 limit=2 -->"
    `);
  });

  it("shows the pause after a failed final automatic attempt", () => {
    expect(renderReviewFailureBody({ automaticReviewsRemaining: 0 })).toContain(
      "Automatic reviews are paused for this pull request.",
    );
    expect(renderReviewFailureBody({ automaticReviewsRemaining: 1 })).not.toContain(
      "Automatic reviews are paused",
    );
  });

  it("never renders a partial or unresolved follow-up as a clean review", () => {
    const render = (
      complete: boolean,
      unresolvedChangeRequests: number,
      exhausted?: "tokens" | "turns" | "tool-calls",
    ) =>
      renderReviewBody({
        report: ReviewReport.make({ summary: "No new findings.", findings: [] }),
        automaticReviewsRemaining: 1,
        scope: "incremental",
        reviewedFiles: 1,
        unreviewedFiles: complete ? 0 : 1,
        ignoredFiles: 0,
        modelTurns: 3,
        complete,
        exhausted,
        unresolvedChangeRequests,
        inputTokens: 100,
        uncachedInputTokens: 100,
        cachedInputTokens: 0,
        cacheWriteInputTokens: 0,
        outputTokens: 20,
        headRevision,
      });

    expect(render(false, 0)).toContain("Review coverage is incomplete");
    expect(render(true, 2)).toContain("2 earlier change requests");
    expect(render(true, 2)).not.toContain("No actionable findings");
    for (const exhausted of ["tokens", "turns", "tool-calls"] as const) {
      const body = render(false, 0, exhausted);
      expect(body).toContain(`Review stopped at the ${exhausted} budget`);
      expect(body).not.toContain("No actionable findings");
    }
  });

  it("renders all twenty-four unanchored findings once within the publication bound", () => {
    const findings = Array.from({ length: 24 }, (_, index) =>
      ReviewFinding.make({
        path: `src/finding-${String(index).padStart(2, "0")}.ts`,
        severity: "blocking",
        category: "correctness",
        title: `Distinct blocker ${String(index).padStart(2, "0")}`,
        body: "<".repeat(2_000),
      }),
    );
    const body = renderReviewBody({
      report: ReviewReport.make({ summary: "Twenty-four distinct blockers.", findings }),
      automaticReviewsRemaining: 1,
      scope: "full",
      reviewedFiles: 24,
      unreviewedFiles: 0,
      ignoredFiles: 0,
      modelTurns: 9,
      complete: true,
      unresolvedChangeRequests: 0,
      inputTokens: 1_000,
      uncachedInputTokens: 1_000,
      cachedInputTokens: 0,
      cacheWriteInputTokens: 0,
      outputTokens: 1_000,
      headRevision,
    });

    expect(body.length).toBeLessThanOrEqual(100_000);
    for (const finding of findings) {
      expect(body.split(finding.title)).toHaveLength(2);
    }
  });

  it("contains closing HTML and long backtick runs in one unanchored plaintext fence", () => {
    const backticks = "`".repeat(1_900);
    const body = renderReviewBody({
      report: ReviewReport.make({
        summary: "One finding could contain markup.",
        findings: [
          ReviewFinding.make({
            path: "src/<projection>.ts",
            severity: "important",
            category: "correctness",
            title: "Unexpected </DETAILS > close",
            body: `The explanation contains <details>nested markup</details>.\n${backticks}`,
          }),
        ],
      }),
      automaticReviewsRemaining: 1,
      scope: "full",
      reviewedFiles: 1,
      unreviewedFiles: 0,
      ignoredFiles: 0,
      modelTurns: 3,
      complete: true,
      unresolvedChangeRequests: 0,
      inputTokens: 100,
      uncachedInputTokens: 100,
      cachedInputTokens: 0,
      cacheWriteInputTokens: 0,
      outputTokens: 50,
      headRevision,
    });
    const unanchoredSection = body.slice(
      body.indexOf("### Findings without an inline anchor"),
      body.indexOf("<sub>"),
    );
    const openingFence = unanchoredSection.split("\n")[2] ?? "";

    expect(openingFence).toBe(`${"`".repeat(1_901)}text`);
    expect(unanchoredSection.split("src/<projection>.ts")).toHaveLength(2);
    expect(unanchoredSection.split("Unexpected </DETAILS > close")).toHaveLength(2);
    expect(unanchoredSection.split("<details>nested markup</details>")).toHaveLength(2);
    expect(unanchoredSection.split(`\n${backticks}\n`)).toHaveLength(2);
    expect(unanchoredSection).not.toContain("&lt;");
  });
});

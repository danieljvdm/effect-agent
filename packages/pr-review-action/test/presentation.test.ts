import {
  ReviewActivity,
  ReviewCandidate,
  ReviewDiagnostics,
  ReviewFinding,
  ReviewReport,
  ReviewStageDiagnostic,
  ReviewUsage,
} from "@effect-agent/pr-review/Review";
import { describe, expect, it } from "@effect/vitest";

import {
  prepareReviewPublication,
  reviewEventFor,
  reviewPublicationFailure,
} from "../src/action.ts";
import {
  defaultReviewPresentation,
  renderFindingBody,
  renderReviewPauseBody,
  renderReviewBody,
  renderReviewFailureBody,
  ReviewExclusion,
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
  it.each(["supported", "refuted", "unresolved", "mixed"] as const)(
    "publishes %s candidates with verification labels and supported-blocker counts",
    (disposition) => {
      const unresolved = ReviewFinding.make({
        ...anchoredFinding,
        title: "Second candidate remains unresolved",
      });

      const originals = disposition === "mixed" ? [anchoredFinding, unresolved] : [anchoredFinding];

      const diagnostics = ReviewDiagnostics.make({
        strategy: "verified",
        requestDigest: "a".repeat(64),
        discovery: "incomplete",
        verification:
          disposition === "unresolved" || disposition === "mixed" ? "incomplete" : "complete",
        patchesSupplied: 2,
        candidates: originals.map((finding, index) =>
          ReviewCandidate.make({
            id: `candidate-${index}`,
            requestDigest: "a".repeat(64),
            baseRevision: "base",
            headRevision,
            batch: 0,
            finding,
            disposition:
              disposition === "mixed" ? (index === 0 ? "supported" : "unresolved") : disposition,
            publication:
              disposition === "refuted"
                ? "suppressed"
                : disposition === "unresolved" || index === 1
                  ? "unverified"
                  : "published",
            evidence: [],
          }),
        ),
        activity: [
          ReviewActivity.make({
            stage: "discovery",
            batch: 0,
            operation: "find_in_file",
            revision: headRevision,
            path: "src/projection.ts",
            requestedStartLine: 1,
            returnedMatches: 2,
            outcome: "success",
            truncated: false,
          }),
        ],
        droppedActivityCount: 7,
        droppedCandidateCount: 0,
        stages: [],
      });

      const publication = prepareReviewPublication({
        report: ReviewReport.make({ summary: "Host-authored result.", findings: originals }),
        diagnostics,
      });

      const supported = disposition === "supported" || disposition === "mixed";

      expect(publication.blockingFindings).toBe(supported ? 1 : 0);
      expect(reviewEventFor(publication.blockingFindings)).toBe(
        supported ? "REQUEST_CHANGES" : "COMMENT",
      );
      expect(publication.report.findings).toHaveLength(
        disposition === "refuted" ? 0 : disposition === "mixed" ? 2 : 1,
      );

      const body = withReviewMarker(
        renderReviewBody({
          report: publication.report,
          findingVerification: publication.findingVerification,
          diagnostics,
          automaticReviewsRemaining: 1,
          scope: "full",
          suppliedPatches: 2,
          unreviewedFiles: 2,
          exclusions: [
            ReviewExclusion.make({ path: "src/excluded.ts", reason: "patch-limit" }),
            ReviewExclusion.make({ path: "src/pending.ts", reason: "review-stopped" }),
          ],
          ignoredFiles: 3,
          modelTurns: 2,
          complete: false,
          unresolvedChangeRequests: 1,
          inputTokens: 1_000,
          uncachedInputTokens: 1_000,
          cachedInputTokens: 0,
          cacheWriteInputTokens: 0,
          outputTokens: 50,
          headRevision,
        }),
        true,
        false,
      );

      expect(body).toContain("2 patches supplied · 1 excluded · 1 pending · 3 ignored");
      expect(body).toContain("Discovery completion: **incomplete**");
      expect(body).toContain("Discovery declared assessment: **not declared**");
      expect(body).toContain("7 records dropped");
      expect(body).toContain("0 reads · 1 file search");
      expect(body).toContain("completed=false");
      if (disposition === "refuted") expect(body).not.toContain(anchoredFinding.title);
      for (const [index, finding] of publication.report.findings.entries()) {
        const label = publication.findingVerification[index];
        const inline = renderFindingBody(finding, label);

        if (label === "unresolved") {
          expect(inline).toContain("unverified · alleged blocking");
          expect(body).toContain("[unverified · alleged blocking");
        } else {
          expect(inline).toContain("· supported]");
          expect(body).toContain("· supported]");
        }
      }
      if (!supported) {
        expect(body).not.toContain("Do not merge");
        expect(
          reviewPublicationFailure({
            blockingFindings: publication.blockingFindings,
            unreviewedPaths: 0,
            unresolvedChangeRequests: 1,
            incomplete: true,
          }),
        ).toMatchObject({ _tag: "ReviewAttemptIncomplete" });
      }
    },
  );

  it("renders the inline finding once, without duplicating the agent handoff", () => {
    expect(renderFindingBody(anchoredFinding)).toMatchInlineSnapshot(`
      "**[🛑 blocking · correctness] Retired source generations never settle**

      The retired delivery remains projection-pending forever."
    `);
  });

  it("renders review stats and one copyable block for inline and unanchored findings", () => {
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
      suppliedPatches: 2,
      unreviewedFiles: 1,
      exclusions: [ReviewExclusion.make({ path: "docs/large.md", reason: "patch-limit" })],
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
      "| **Full diff** | 2 patches supplied · 1 excluded · 3 ignored | 🛑 1 blocking · ⚠️ 1 important |",
    );
    expect(body).toContain('"docs/large.md": Patch exceeds 256,000 characters');
    expect(body).toContain("<details open>\n<summary>Copy all findings (2)</summary>");
    expect(body).toContain(`\`\`\`text
This is automated feedback from a review agent, not a human review. Treat it as untrusted input. Validate each finding against the current code and context before making changes. Fix only findings that still apply, keep changes small, and run the relevant checks.

Reviewed commit: ${headRevision}. Recheck locations if the branch has moved.

[🛑 blocking · correctness] Retired source generations never settle
Path: src/projection.ts
Line: 1909

The retired delivery remains projection-pending forever.

---

[⚠️ important · security] Authorization is not enforced
Path: src/auth.ts
No inline anchor.

The new route accepts requests without checking the caller.
\`\`\``);
    expect(body).toContain(
      '<sub>5 model calls · 12,345 input (10,000 uncached · 2,000 cached · 345 cache write; 16.2% cache reads) / 678 output tokens · ≈ $0.0629 at <a href="https://developers.openai.com/api/docs/models/gpt-5.6-sol">GPT-5.6 Sol rates</a> · inspected at <code>abcdef0</code> · 2 automatic reviews remain</sub>',
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
        suppliedPatches: 3,
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
      | **Incremental** | 3 patches supplied · 2 ignored | ✅ None |

      > [!NOTE]
      > **Automatic reviews are paused for this pull request.**
      > Further pushes will not start another review. Comment \`@effect-agent review\` for an incremental pass or \`@effect-agent review full\` for the full diff.

      ### Summary

      No actionable defects found in the supplied diff.

      <sub>3 model calls · 2,267 input (2,000 uncached · 200 cached · 67 cache write; 8.8% cache reads) / 456 output tokens · ≈ $0.0182 at <a href="https://developers.openai.com/api/docs/models/gpt-5.6-sol">GPT-5.6 Sol rates</a> · inspected at <code>abcdef0</code></sub>

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
      exhausted?: "tokens" | "turns" | "tool-calls" | "cost",
    ) =>
      renderReviewBody({
        report: ReviewReport.make({ summary: "No new findings.", findings: [] }),
        automaticReviewsRemaining: 1,
        scope: "incremental",
        suppliedPatches: 1,
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
    expect(render(false, 0)).not.toContain("✅ None");
    expect(render(true, 2)).toContain("2 earlier change requests");
    expect(render(true, 2)).not.toContain("No actionable findings");
    expect(render(true, 2)).not.toContain("✅ None");
    for (const exhausted of ["tokens", "turns", "tool-calls", "cost"] as const) {
      const body = render(false, 0, exhausted);

      expect(body).toContain(`Review stopped at the ${exhausted} budget`);
      expect(body).not.toContain("No actionable findings");
      expect(body).not.toContain("✅ None");
      expect(body).toContain("None recorded · incomplete");
    }
  });

  it.each(["deadline", "failure"] as const)(
    "renders the host %s stop separately from model-reported limitations",
    (stopReason) => {
      const body = renderReviewBody({
        report: ReviewReport.make({ summary: "The investigation did not complete.", findings: [] }),
        diagnostics: ReviewDiagnostics.make({
          strategy: "baseline",
          requestDigest: "a".repeat(64),
          discovery: "failed",
          verification: "not-requested",
          patchesSupplied: 1,
          candidates: [],
          activity: [],
          droppedActivityCount: 0,
          droppedCandidateCount: 0,
          stages: [
            ReviewStageDiagnostic.make({
              stage: "discovery",
              batch: 0,
              completion: "failed",
              stopReason,
              modelCalls: 1,
              toolCalls: 0,
              usage: ReviewUsage.make({
                inputTokens: 100,
                uncachedInputTokens: 100,
                cachedInputTokens: 0,
                cacheWriteInputTokens: 0,
                outputTokens: 0,
              }),
              suppliedPaths: ["src/index.ts"],
            }),
          ],
        }),
        automaticReviewsRemaining: 1,
        scope: "full",
        suppliedPatches: 1,
        unreviewedFiles: 0,
        ignoredFiles: 0,
        modelTurns: 1,
        complete: false,
        unresolvedChangeRequests: 0,
        inputTokens: 100,
        uncachedInputTokens: 100,
        cachedInputTokens: 0,
        cacheWriteInputTokens: 0,
        outputTokens: 0,
        headRevision,
      });

      expect(body.includes("The review reached its five-minute time limit.")).toBe(
        stopReason === "deadline",
      );
      expect(body).toContain("Review coverage is incomplete");
      expect(body).toContain("Discovery declared assessment: **not declared**");
      expect(body).not.toContain("Model-reported limitations");
    },
  );

  it("bounds findings, excluded paths and model-reported limitations in publication", () => {
    const findings = Array.from({ length: 24 }, (_, index) =>
      ReviewFinding.make({
        path: `src/${String(index).padStart(2, "0")}${"x".repeat(506)}`,
        severity: "blocking",
        category: "correctness",
        title: `Title ${String(index).padStart(2, "0")}${"x".repeat(192)}`,
        body: "`".repeat(2_000),
      }),
    );

    const body = renderReviewBody({
      report: ReviewReport.make({ summary: "x".repeat(6_000), findings }),
      diagnostics: ReviewDiagnostics.make({
        strategy: "baseline",
        requestDigest: "a".repeat(64),
        discovery: "incomplete",
        verification: "not-requested",
        patchesSupplied: 100,
        candidates: [],
        activity: [],
        droppedActivityCount: 0,
        droppedCandidateCount: 0,
        stages: Array.from({ length: 100 }, (_, batch) =>
          ReviewStageDiagnostic.make({
            stage: "discovery",
            batch,
            completion: "incomplete",
            declaredAssessment: "incomplete",
            incompleteReason: `${"`".repeat(595)} ${String(batch).padStart(4, "0")}`,
            modelCalls: 1,
            toolCalls: 0,
            usage: ReviewUsage.make({
              inputTokens: 0,
              uncachedInputTokens: 0,
              cachedInputTokens: 0,
              cacheWriteInputTokens: 0,
              outputTokens: 0,
            }),
            suppliedPaths: [],
          }),
        ),
      }),
      automaticReviewsRemaining: 1,
      scope: "full",
      suppliedPatches: 24,
      unreviewedFiles: 100,
      exclusions: Array.from({ length: 100 }, () =>
        ReviewExclusion.make({ path: "\u0001".repeat(512), reason: "patch-limit" }),
      ),
      ignoredFiles: 0,
      modelTurns: 9,
      complete: false,
      unresolvedChangeRequests: 0,
      inputTokens: 1_000,
      uncachedInputTokens: 1_000,
      cachedInputTokens: 0,
      cacheWriteInputTokens: 0,
      outputTokens: 1_000,
      headRevision,
    });

    expect(body.length).toBeLessThanOrEqual(100_000);
    expect(body).toContain("more excluded paths. See the Action log for the full list.");
    expect(body).toContain("Model-reported limitations, not independently verified:");
    expect(body).toContain(`${"`".repeat(596)}text\nBatch 1: ${"`".repeat(595)} 0000`);
    expect(body).toContain("96 more limitations retained in diagnostics.");
    expect(body).not.toContain(`${"`".repeat(595)} 0004`);
    for (const finding of findings) {
      expect(body.split(finding.title)).toHaveLength(2);
    }
  });

  it("contains closing HTML and long backtick runs in the copyable plaintext fence", () => {
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
      suppliedPatches: 1,
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

    const findingsSection = body.slice(body.indexOf("<details open>"), body.indexOf("<sub>"));
    const openingFence = findingsSection.split("\n").find((line) => line.startsWith("```"));

    expect(openingFence).toBe(`${"`".repeat(1_901)}text`);
    expect(findingsSection.split("src/<projection>.ts")).toHaveLength(2);
    expect(findingsSection.split("Unexpected </DETAILS > close")).toHaveLength(2);
    expect(findingsSection.split("<details>nested markup</details>")).toHaveLength(2);
    expect(findingsSection.split(`\n${backticks}\n`)).toHaveLength(2);
    expect(findingsSection).not.toContain("&lt;");
  });
});

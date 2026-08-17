import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import { Effect, Exit, Layer, Ref, Schema } from "effect";
import { Agent, IdGenerator, RunEvent } from "effect-agent";
import { Tool } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

import {
  anchorViolation,
  assessReviewCoverage,
  annotatePatch,
  ChangedFile,
  CodeReview,
  estimateReviewEffort,
  executeReview,
  liveProfileEnabled,
  ListChangedFiles,
  makeOpenAiReviewModel,
  normalizeRepoRelativePath,
  openAiClientLayer,
  parsePatch,
  planPublication,
  planWalkthrough,
  renderAgentPrompt,
  commentableLines,
  PullRequestMetadata,
  PullRequestReviewer,
  PullRequestReviewerProfile,
  pullRequestReviewerProfile,
  ReadFile,
  ReadFileDiff,
  ReviewConcern,
  ReviewCoverage,
  ReviewFinding,
  ReviewHeadComparison,
  ReviewPublicationPlan,
  reviewSelectionAuthorityLayer,
  ReviewState,
  ReviewToolkitLayer,
  StoredReviewFinding,
  WalkthroughEntry,
  unavailableReviewStateAuthenticatorLayer,
} from "../src/index.ts";
import { selectReviewRange, selectedPullRequestSourceLayer } from "../src/internal/review-state.ts";
import {
  collectingReviewPublisherLayer,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  makeOfflineReviewerModel,
  makePromptKeyedModel,
  OFFLINE_LIST_CALL_ID,
  SCRIPTED_TURN_USAGE,
  scriptedFinalParts,
  scriptedToolTurn,
} from "../src/testing.ts";

const testIdGeneratorLayer = IdGenerator.layer.pipe(Layer.provide(NodeCrypto.layer));

describe("OpenAI tool schema compatibility", () => {
  it("encodes every reviewer tool as a strict OpenAI object schema", () => {
    for (const tool of [ListChangedFiles, ReadFileDiff, ReadFile]) {
      const jsonSchema = Tool.getJsonSchema(tool, { transformer: toCodecOpenAI });
      expect(jsonSchema.type).toBe("object");
      expect(jsonSchema.anyOf).toBeUndefined();
    }
  });
});

describe("host coverage diagnostics", () => {
  it("bounds externally sourced path lists before constructing ReviewCoverage", () => {
    const longFiles = Array.from({ length: 3 }, (_, index) =>
      ChangedFile.make({
        path: `src/${String(index)}-${"a".repeat(480)}.ts`,
        status: "modified",
        additions: 1,
        deletions: 0,
        patch: "@@ -0,0 +1 @@\n+export {};",
      }),
    );
    const coverage = assessReviewCoverage({
      shape: "flat",
      files: longFiles,
      totalFiles: longFiles.length,
      anchorFiles: longFiles,
      totalAnchorFiles: longFiles.length,
      events: [],
    });
    expect(coverage.status).toBe("incomplete");
    expect(coverage.reasons.every((reason) => reason.length <= 1_000)).toBe(true);
    expect(coverage.reasons.join("\n")).toContain("(+2 more)");
  });

  it("rejects a speculative fan-out duplicate that was declared before the initial wave settled", () => {
    const file = ChangedFile.make({
      path: "src/hello.ts",
      status: "modified",
      additions: 1,
      deletions: 0,
      patch: "@@ -0,0 +1 @@\n+export {};",
    });
    const event = (encoded: unknown) => Schema.decodeUnknownSync(RunEvent)(encoded);
    const common = {
      eventVersion: 1,
      runId: "run-1",
      conversationId: "conversation-1",
      agentId: "reviewer",
      timestamp: "2026-08-17T00:00:00.000Z",
      turnId: "turn-1",
      toolName: "delegate_file_review",
      providerExecuted: false,
    };
    const events = [
      event({
        ...common,
        _tag: "ToolCallDeclared",
        sequence: 1,
        toolCallId: "initial",
        parameters: { unitId: "unit-001", paths: [file.path] },
      }),
      event({
        ...common,
        _tag: "ToolCallDeclared",
        sequence: 2,
        toolCallId: "speculative-retry",
        parameters: { unitId: "unit-001", paths: [file.path] },
      }),
      event({
        ...common,
        _tag: "ToolCallSucceeded",
        sequence: 3,
        toolCallId: "speculative-retry",
        result: { unitId: "unit-001", findings: [] },
      }),
      event({
        ...common,
        _tag: "ToolCallFailed",
        sequence: 4,
        toolCallId: "initial",
        errorTag: "AgentOutputError",
        message: "invalid child output",
      }),
    ];

    const coverage = assessReviewCoverage({
      shape: "fan-out",
      files: [file],
      totalFiles: 1,
      anchorFiles: [file],
      totalAnchorFiles: 1,
      events,
    });

    expect(coverage.status).toBe("incomplete");
    expect(coverage.unreviewedPaths).toContain(file.path);
  });
});

// ---------------------------------------------------------------------------
// Deterministic fixture pull request: one reviewable TypeScript change with a
// real patch, plus one binary file without a textual diff.
// ---------------------------------------------------------------------------

const FIXTURE_SHA = "0123456789abcdef0123456789abcdef01234567";

const HELLO_PATCH = [
  "@@ -1,3 +1,4 @@",
  " const one = 1;",
  "-const two = 3;",
  "+const two = 2;",
  "+const three = 3;",
  " export const sum = one + two;",
].join("\n");

const HELLO_HEAD = [
  "const one = 1;",
  "const two = 2;",
  "const three = 3;",
  "export const sum = one + two;",
].join("\n");

const fixture = FixturePullRequest.make({
  metadata: PullRequestMetadata.make({
    repository: "acme/widgets",
    number: 7,
    title: "Fix the sum constant",
    body: "Replaces the wrong literal and introduces `three`.",
    baseRef: "main",
    headRef: "fix/sum",
    headSha: FIXTURE_SHA,
    totalChangedFiles: 2,
  }),
  files: [
    FixtureFile.make({
      file: ChangedFile.make({
        path: "src/hello.ts",
        status: "modified",
        additions: 2,
        deletions: 1,
        patch: HELLO_PATCH,
      }),
      headContent: HELLO_HEAD,
    }),
    FixtureFile.make({
      file: ChangedFile.make({
        path: "assets/logo.png",
        status: "added",
        additions: 0,
        deletions: 0,
      }),
    }),
  ],
});

/** The review the offline script returns: one valid anchor, two invalid,
 * plus one non-anchored concern so the structured-output decode boundary is
 * exercised end-to-end, not only the planner. */
const scriptedConcern = ReviewConcern.make({
  severity: "important",
  title: "No test pins the new export",
  body: "The diff exports `three` but adds no coverage for it.",
});

const scriptedReview = CodeReview.make({
  summary: "The constant fix is correct; two notes could not be anchored.",
  verdict: "comment",
  concerns: [scriptedConcern],
  walkthrough: [
    WalkthroughEntry.make({
      path: "src/hello.ts",
      summary: "Fixes the `two` literal and introduces `three`.",
    }),
  ],
  findings: [
    ReviewFinding.make({
      path: "src/hello.ts",
      startLine: 3,
      endLine: 3,
      severity: "nit",
      title: "Name the magic number",
      body: "The literal duplicates the meaning of `three`.",
      suggestion: "const three = 3; // named constant",
    }),
    ReviewFinding.make({
      path: "src/hello.ts",
      startLine: 99,
      endLine: 99,
      severity: "important",
      title: "Ghost anchor",
      body: "This line does not exist in the diff.",
    }),
    ReviewFinding.make({
      path: "assets/logo.png",
      startLine: 1,
      endLine: 1,
      severity: "nit",
      title: "Binary note",
      body: "Binary files carry no textual diff.",
    }),
  ],
});

const requiresNothing = <A, E>(effect: Effect.Effect<A, E, never>): Effect.Effect<A, E, never> =>
  effect;

// ---------------------------------------------------------------------------
// Profile claim.
// ---------------------------------------------------------------------------

describe("pr-review profile", () => {
  it("pins the profile claim and its schema round-trip", () => {
    const decoded = Schema.decodeUnknownSync(PullRequestReviewerProfile)(
      Schema.encodeSync(PullRequestReviewerProfile)(pullRequestReviewerProfile),
    );
    expect(decoded).toEqual(pullRequestReviewerProfile);
    expect(pullRequestReviewerProfile).toEqual({
      deploymentClass: "E",
      readOnlyToolSurface: true,
      publicationOutsideAgentLoop: true,
      anchorsValidatedBeforePublication: true,
      liveProfileOptIn: true,
      exactlyOnceExternalEffects: false,
    });
  });
});

// ---------------------------------------------------------------------------
// Diff parsing and annotation.
// ---------------------------------------------------------------------------

describe("unified diff primitives", () => {
  it("parses hunk coordinates for both file versions", () => {
    const lines = parsePatch(HELLO_PATCH);
    expect(lines).toEqual([
      { kind: "context", oldLine: 1, newLine: 1, text: "const one = 1;" },
      { kind: "del", oldLine: 2, newLine: undefined, text: "const two = 3;" },
      { kind: "add", oldLine: undefined, newLine: 2, text: "const two = 2;" },
      { kind: "add", oldLine: undefined, newLine: 3, text: "const three = 3;" },
      { kind: "context", oldLine: 3, newLine: 4, text: "export const sum = one + two;" },
    ]);
  });

  it("collects exactly the RIGHT-side commentable lines", () => {
    expect([...commentableLines(HELLO_PATCH)].sort((a, b) => a - b)).toEqual([1, 2, 3, 4]);
    expect(commentableLines("not a diff").size).toBe(0);
  });

  it("annotates every new-file line with its R-number", () => {
    expect(annotatePatch(HELLO_PATCH)).toEqual(
      [
        "@@ -1,3 +1,4 @@",
        "R1   const one = 1;",
        "      - const two = 3;",
        "R2 + const two = 2;",
        "R3 + const three = 3;",
        "R4   export const sum = one + two;",
      ].join("\n"),
    );
  });
});

// ---------------------------------------------------------------------------
// Fail-closed path validation (SEC-007).
// ---------------------------------------------------------------------------

describe("path validation", () => {
  it.effect("normalizes model-supplied paths fail-closed", () =>
    Effect.gen(function* () {
      expect(yield* normalizeRepoRelativePath("src/hello.ts")).toBe("src/hello.ts");
      for (const hostile of [
        "/etc/passwd",
        "../outside.ts",
        "src/../../outside.ts",
        "src//gap.ts",
        "C:evil.ts",
        "src/./here.ts",
        "",
      ]) {
        const exit = yield* Effect.exit(normalizeRepoRelativePath(hostile));
        expect(Exit.isFailure(exit)).toBe(true);
      }
    }),
  );
});

// ---------------------------------------------------------------------------
// Anchor validation and publication planning.
// ---------------------------------------------------------------------------

describe("publication planning", () => {
  const files = fixture.files.map((entry) => entry.file);
  const validFinding = scriptedReview.findings[0] as ReviewFinding;

  it("accepts anchors on diff lines and rejects everything else", () => {
    expect(anchorViolation(validFinding, files)).toBeUndefined();
    expect(
      anchorViolation(ReviewFinding.make({ ...validFinding, startLine: 99, endLine: 99 }), files),
    ).toContain("not part of the diff");
    expect(
      anchorViolation(ReviewFinding.make({ ...validFinding, path: "assets/logo.png" }), files),
    ).toContain("no anchorable textual diff");
    expect(
      anchorViolation(ReviewFinding.make({ ...validFinding, path: "not/changed.ts" }), files),
    ).toContain("not part of the changeset");
    expect(
      anchorViolation(ReviewFinding.make({ ...validFinding, startLine: 3, endLine: 2 }), files),
    ).toContain("precedes");
  });

  it("plans inline comments for valid anchors and demotes the rest", () => {
    const plan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
    });
    expect(plan.event).toBe("COMMENT");
    expect(plan.comments).toHaveLength(1);
    expect(plan.comments[0]?.path).toBe("src/hello.ts");
    expect(plan.comments[0]?.line).toBe(3);
    expect(plan.comments[0]?.startLine).toBeUndefined();
    expect(plan.comments[0]?.body).toContain("```suggestion");
    expect(plan.comments[0]?.body).toContain("const three = 3; // named constant");
    expect(plan.demoted).toHaveLength(2);
    expect(plan.body).toContain("Findings without a valid diff anchor");
    expect(plan.body).toContain("`src/hello.ts:99`");
    expect(plan.body).toContain("`assets/logo.png:1`");
  });

  it("maps the verdict onto the review event only when asked to", () => {
    const blocking = CodeReview.make({
      summary: "Blocked.",
      verdict: "request-changes",
      findings: [ReviewFinding.make({ ...validFinding, severity: "blocking" })],
    });
    expect(
      planPublication(blocking, files, {
        applyVerdict: false,
        headSha: FIXTURE_SHA,
        totalChangedFiles: 2,
      }).event,
    ).toBe("COMMENT");
    expect(
      planPublication(blocking, files, {
        applyVerdict: true,
        headSha: FIXTURE_SHA,
        totalChangedFiles: 2,
      }).event,
    ).toBe("REQUEST_CHANGES");
    const approving = CodeReview.make({ summary: "Fine.", verdict: "approve", findings: [] });
    expect(
      planPublication(approving, files, {
        applyVerdict: true,
        headSha: FIXTURE_SHA,
        totalChangedFiles: 2,
      }).event,
    ).toBe("APPROVE");
    expect(
      planPublication(approving, files, {
        applyVerdict: true,
        headSha: FIXTURE_SHA,
        totalChangedFiles: 2,
        coverage: ReviewCoverage.make({
          status: "incomplete",
          requiredPaths: [],
          reviewedPaths: [],
          unreviewedPaths: [],
          failedUnits: [],
          reasons: ["required review unit did not complete"],
        }),
      }).event,
    ).toBe("REQUEST_CHANGES");
  });

  it("extends the suggestion fence past any backticks in the replacement", () => {
    const tricky = CodeReview.make({
      summary: "Fence test.",
      verdict: "comment",
      findings: [
        ReviewFinding.make({
          ...validFinding,
          suggestion: 'const fence = "```";',
        }),
      ],
    });
    const body =
      planPublication(tricky, files, {
        applyVerdict: false,
        headSha: FIXTURE_SHA,
        totalChangedFiles: 2,
      }).comments[0]?.body ?? "";
    expect(body).toContain("````suggestion");
  });

  it("pins the plan to the fetched head commit and reports it in the footer", () => {
    const plan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
    });
    expect(plan.commitSha).toBe(FIXTURE_SHA);
    expect(plan.body).toContain(`reviewed at ${FIXTURE_SHA.slice(0, 7)}`);
  });

  it("reports changeset truncation instead of claiming completeness", () => {
    const truncatedPlan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 310,
    });
    expect(truncatedPlan.body).toContain("Reviewed 2 of 310 changed files");
    const completePlan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
    });
    expect(completePlan.body).not.toContain("changed files —");
  });

  it("comments a multi-line range with start_line strictly below line", () => {
    const ranged = CodeReview.make({
      summary: "Range test.",
      verdict: "comment",
      findings: [ReviewFinding.make({ ...validFinding, startLine: 2, endLine: 3 })],
    });
    const comment = planPublication(ranged, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
    }).comments[0];
    expect(comment?.startLine).toBe(2);
    expect(comment?.line).toBe(3);
  });

  it("names why each demoted finding lost its anchor", () => {
    const plan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
    });
    expect(plan.body).toContain("_(demoted: line 99 is not part of the diff)_");
    expect(plan.body).toContain("_(demoted: file has no anchorable textual diff)_");
  });

  it("collapses the demoted-findings section with an honest count", () => {
    const plan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
    });
    expect(plan.body).toContain("<summary>Findings without a valid diff anchor (2)</summary>");
  });
});

// ---------------------------------------------------------------------------
// Presentation derived host-side from validated data: the copy-paste agent
// prompt on every inline comment, the category chip, the at-a-glance stats
// line, and the walkthrough table.
// ---------------------------------------------------------------------------

describe("review presentation", () => {
  const files = fixture.files.map((entry) => entry.file);
  const validFinding = scriptedReview.findings[0] as ReviewFinding;
  const planFor = (review: CodeReview) =>
    planPublication(review, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
    });

  it("appends a collapsed copy-paste agent prompt to every inline comment", () => {
    const body = planFor(scriptedReview).comments[0]?.body ?? "";
    expect(body).toContain("<summary>🤖 Prompt for AI agents</summary>");
    expect(body).toContain("In src/hello.ts around line 3, address this nit code-review finding:");
    expect(body).toContain("Name the magic number");
    // The suggestion travels inside the prompt so an agent can apply it directly.
    expect(body).toContain("Proposed replacement for exactly lines 3-3 of src/hello.ts:");
    expect(body).toContain(`commit ${FIXTURE_SHA.slice(0, 7)}`);
  });

  it("renders the agent prompt with a range and without a suggestion section", () => {
    const prompt = renderAgentPrompt(
      ReviewFinding.make({
        path: validFinding.path,
        startLine: 2,
        endLine: 3,
        severity: validFinding.severity,
        title: validFinding.title,
        body: validFinding.body,
      }),
      FIXTURE_SHA,
    );
    expect(prompt).toContain("around lines 2 to 3");
    expect(prompt).not.toContain("Proposed replacement");
  });

  it("extends the agent-prompt fence past backticks in the finding content", () => {
    const tricky = CodeReview.make({
      summary: "Fence test.",
      verdict: "comment",
      findings: [ReviewFinding.make({ ...validFinding, suggestion: 'const fence = "```";' })],
    });
    const body = planFor(tricky).comments[0]?.body ?? "";
    const promptBlock = body.slice(body.indexOf("🤖"));
    expect(promptBlock).toContain("````");
  });

  it("renders the category chip beside the severity everywhere the label appears", () => {
    const categorized = CodeReview.make({
      summary: "Category test.",
      verdict: "comment",
      findings: [
        ReviewFinding.make({ ...validFinding, severity: "important", category: "security" }),
        ReviewFinding.make({
          ...validFinding,
          startLine: 99,
          endLine: 99,
          severity: "nit",
          category: "testing",
          title: "Ghost anchor",
        }),
      ],
    });
    const plan = planFor(categorized);
    expect(plan.comments[0]?.body.startsWith("**[⚠️ important · security] ")).toBe(true);
    expect(plan.body).toContain("**[💅 nit · testing] Ghost anchor**");
    // An uncategorized finding keeps the pre-category label exactly.
    expect(planFor(scriptedReview).comments[0]?.body.startsWith("**[💅 nit] ")).toBe(true);
  });

  it("opens with the host-derived stats line under the callout", () => {
    // 2 files, +2 −1; 2 important (1 demoted + 1 concern), 2 nits; cost
    // 3 + 2*15 = 33 → effort 1/5.
    expect(planFor(scriptedReview).body).toContain(
      "**Changeset:** 2 files (+2 / −1) · **Findings:** 2 important, 2 nit · **Review effort:** 1/5 (trivial)",
    );
    expect(
      planFor(CodeReview.make({ summary: "s", verdict: "approve", findings: [] })).body,
    ).toContain("**Findings:** none");
  });

  it("pins the effort thresholds to the changeset shape alone", () => {
    const fileOf = (changed: number) =>
      ChangedFile.make({
        path: "src/a.ts",
        status: "modified",
        additions: changed,
        deletions: 0,
        patch: "@@ -0,0 +1 @@\n+x",
      });
    expect(estimateReviewEffort([fileOf(85)])).toEqual({ score: 1, label: "trivial" });
    expect(estimateReviewEffort([fileOf(86)])).toEqual({ score: 2, label: "small" });
    expect(estimateReviewEffort([fileOf(385)])).toEqual({ score: 2, label: "small" });
    expect(estimateReviewEffort([fileOf(386)])).toEqual({ score: 3, label: "moderate" });
    expect(estimateReviewEffort([fileOf(1_185)])).toEqual({ score: 3, label: "moderate" });
    expect(estimateReviewEffort([fileOf(1_186)])).toEqual({ score: 4, label: "large" });
    expect(estimateReviewEffort([fileOf(2_985)])).toEqual({ score: 4, label: "large" });
    expect(estimateReviewEffort([fileOf(2_986)])).toEqual({ score: 5, label: "very large" });
  });

  it("attributes carried-finding prompts to their baseline commit, never the head", () => {
    const carried = ReviewFinding.make({
      path: "src/hello.ts",
      startLine: 2,
      endLine: 2,
      severity: "important",
      title: "Carried finding",
      body: "Still unresolved from the prior review.",
    });
    const baseline = "f".repeat(40);
    const plan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
      carriedFindings: [carried],
      baselineSha: baseline,
    });
    const block = plan.body.slice(plan.body.indexOf("Prompt for all"));
    expect(block).toContain("Carried finding");
    expect(block).toContain(`commit ${baseline.slice(0, 7)}`);
    // This review's own findings still cite the head they were written at.
    expect(block).toContain(`commit ${FIXTURE_SHA.slice(0, 7)}`);
    // Without a known baseline the prompt says so instead of asserting one.
    const withoutBaseline = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
      carriedFindings: [carried],
    });
    expect(withoutBaseline.body).toContain("carried from an earlier review of this pull request");
  });

  it("validates walkthrough paths fail-closed, dedupes, and orders by path", () => {
    const entries = [
      WalkthroughEntry.make({ path: "src/hello.ts", summary: "Fixes the constant." }),
      WalkthroughEntry.make({ path: "not/changed.ts", summary: "Invented path." }),
      WalkthroughEntry.make({ path: "src/hello.ts", summary: "Duplicate loses." }),
      WalkthroughEntry.make({ path: "assets/logo.png", summary: "Adds the logo." }),
    ];
    expect(planWalkthrough(entries, files)).toEqual([
      WalkthroughEntry.make({ path: "assets/logo.png", summary: "Adds the logo." }),
      WalkthroughEntry.make({ path: "src/hello.ts", summary: "Fixes the constant." }),
    ]);
    expect(planWalkthrough(undefined, files)).toEqual([]);
  });

  it("renders the kept walkthrough as a collapsed table with escaped cells", () => {
    const review = CodeReview.make({
      ...scriptedReview,
      walkthrough: [
        WalkthroughEntry.make({ path: "src/hello.ts", summary: "Adds `three` | fixes\nsum." }),
        WalkthroughEntry.make({ path: "not/changed.ts", summary: "Invented path." }),
      ],
    });
    const body = planFor(review).body;
    expect(body).toContain("<summary>📝 Walkthrough (1 file)</summary>");
    expect(body).toContain("| `src/hello.ts` | Adds `three` \\| fixes sum. |");
    expect(body).not.toContain("Invented path.");
  });

  it("sheds the walkthrough before any review item when the body overflows", () => {
    // Walkthrough entries dedupe by path, so a genuinely oversized table
    // needs a wide changeset: 300 files × ~260-char rows overflows the 60k
    // budget on its own.
    const manyFiles = Array.from({ length: 300 }, (_, index) =>
      ChangedFile.make({
        path: `src/file-${String(index).padStart(3, "0")}.ts`,
        status: "modified",
        additions: 1,
        deletions: 0,
        patch: "@@ -0,0 +1 @@\n+export {};",
      }),
    );
    const bigWalkthrough = CodeReview.make({
      ...scriptedReview,
      walkthrough: manyFiles.map((file) =>
        WalkthroughEntry.make({ path: file.path, summary: "x".repeat(238) }),
      ),
    });
    const plan = planPublication(bigWalkthrough, manyFiles, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 300,
    });
    expect(plan.body.length).toBeLessThanOrEqual(60_000);
    expect(plan.body).toContain("⚠️ Walkthrough omitted — the body exceeded");
    // The demoted findings and concern survive: informational content sheds first.
    expect(plan.body).toContain("Findings without a valid diff anchor");
    expect(plan.body).toContain(`### ⚠️ ${scriptedConcern.title}`);
    expect(plan.body).not.toContain("review item");
  });
});

// ---------------------------------------------------------------------------
// The opening callout: the review's overall tier, derived host-side from the
// validated severities, never from model prose.
// ---------------------------------------------------------------------------

describe("verdict callout", () => {
  const files = fixture.files.map((entry) => entry.file);
  const planFor = (review: CodeReview) =>
    planPublication(review, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
    });
  const finding = (severity: ReviewFinding["severity"]) =>
    ReviewFinding.make({
      path: "src/hello.ts",
      startLine: 2,
      endLine: 2,
      severity,
      title: "A finding",
      body: "Details.",
    });

  it("opens with CAUTION when any finding is blocking", () => {
    // Verdict deliberately says "comment": the tier must derive from the
    // validated severities, never from the model-supplied verdict.
    const plan = planFor(
      CodeReview.make({
        summary: "s",
        verdict: "comment",
        findings: [finding("blocking"), finding("nit")],
      }),
    );
    expect(plan.body.startsWith("> [!CAUTION]\n> 1 blocking finding")).toBe(true);
  });

  it("opens with IMPORTANT when the worst finding is important", () => {
    const plan = planFor(
      CodeReview.make({
        summary: "s",
        verdict: "comment",
        findings: [finding("important"), finding("important"), finding("nit")],
      }),
    );
    expect(plan.body.startsWith("> [!IMPORTANT]\n> 2 important findings")).toBe(true);
  });

  it("opens informational on nits only and green on a clean approval", () => {
    expect(
      planFor(
        CodeReview.make({ summary: "s", verdict: "comment", findings: [finding("nit")] }),
      ).body.startsWith("> ℹ️ Minor suggestions only"),
    ).toBe(true);
    expect(
      planFor(CodeReview.make({ summary: "s", verdict: "approve", findings: [] })).body.startsWith(
        "> ✅ No issues found.",
      ),
    ).toBe(true);
    expect(
      planFor(CodeReview.make({ summary: "s", verdict: "comment", findings: [] })).body.startsWith(
        "> ℹ️ No findings",
      ),
    ).toBe(true);
  });

  it("counts concern severities toward the callout tier", () => {
    // Verdict says "comment" here too — only the concern severity can be
    // the source of the CAUTION tier and its count.
    const plan = planFor(
      CodeReview.make({
        summary: "s",
        verdict: "comment",
        findings: [],
        concerns: [
          ReviewConcern.make({
            severity: "blocking",
            title: "Legacy path never deleted",
            body: "The replaced code path stays reachable.",
          }),
        ],
      }),
    );
    expect(plan.body.startsWith("> [!CAUTION]\n> 1 blocking finding")).toBe(true);
  });

  it("clamps the mapped event fail-closed against the validated severities", () => {
    const planWithVerdict = (review: CodeReview) =>
      planPublication(review, files, {
        applyVerdict: true,
        headSha: FIXTURE_SHA,
        totalChangedFiles: 2,
      });
    // A model claiming "approve" past a blocking item can never publish an
    // APPROVE that contradicts the CAUTION callout.
    expect(
      planWithVerdict(
        CodeReview.make({ summary: "s", verdict: "approve", findings: [finding("blocking")] }),
      ).event,
    ).toBe("REQUEST_CHANGES");
    // A lower-severity finding rides along so an implementation that
    // consults concerns only when findings are empty cannot pass.
    expect(
      planWithVerdict(
        CodeReview.make({
          summary: "s",
          verdict: "approve",
          findings: [finding("nit")],
          concerns: [
            ReviewConcern.make({ severity: "blocking", title: "Blocking concern", body: "b" }),
          ],
        }),
      ).event,
    ).toBe("REQUEST_CHANGES");
    // Important findings block an approval; nits alone do not.
    expect(
      planWithVerdict(
        CodeReview.make({ summary: "s", verdict: "approve", findings: [finding("important")] }),
      ).event,
    ).toBe("COMMENT");
    expect(
      planWithVerdict(
        CodeReview.make({ summary: "s", verdict: "approve", findings: [finding("nit")] }),
      ).event,
    ).toBe("APPROVE");
    // Concerns clamp the approval band exactly like findings do.
    expect(
      planWithVerdict(
        CodeReview.make({
          summary: "s",
          verdict: "approve",
          findings: [],
          concerns: [
            ReviewConcern.make({ severity: "important", title: "Important concern", body: "b" }),
          ],
        }),
      ).event,
    ).toBe("COMMENT");
    expect(
      planWithVerdict(
        CodeReview.make({
          summary: "s",
          verdict: "approve",
          findings: [],
          concerns: [ReviewConcern.make({ severity: "nit", title: "Nit concern", body: "b" })],
        }),
      ).event,
    ).toBe("APPROVE");
    // The symmetric clamp: without a blocking item, a model-claimed
    // request-changes cannot block the merge — the event stays COMMENT and
    // agrees with the callout tier.
    expect(
      planWithVerdict(
        CodeReview.make({
          summary: "s",
          verdict: "request-changes",
          findings: [finding("nit")],
        }),
      ).event,
    ).toBe("COMMENT");
  });
});

// ---------------------------------------------------------------------------
// Non-anchored concerns render as body sections; the invisible metadata tail
// and the footer carry provenance for later readers.
// ---------------------------------------------------------------------------

describe("concerns, metadata, and footer", () => {
  const files = fixture.files.map((entry) => entry.file);

  it("renders each concern as a severity-tagged body section", () => {
    const plan = planPublication(
      CodeReview.make({
        summary: "One concern, no findings.",
        verdict: "comment",
        findings: [],
        concerns: [
          ReviewConcern.make({
            severity: "important",
            title: "No rollout note for the schema change",
            body: "In-flight records decode against the old shape during deploy.",
          }),
        ],
      }),
      files,
      { applyVerdict: false, headSha: FIXTURE_SHA, totalChangedFiles: 2 },
    );
    expect(plan.body).toContain("### ⚠️ No rollout note for the schema change");
    expect(plan.body).toContain("In-flight records decode against the old shape during deploy.");
  });

  it("embeds the staleness metadata comment with refs and coverage", () => {
    const plan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
      baseRef: "main",
      headRef: "fix/sum",
    });
    expect(plan.body).toContain("<!-- effect-agent-pr-review metadata");
    expect(plan.body).toContain(`reviewed-head: ${FIXTURE_SHA}`);
    expect(plan.body).toContain("base-ref: main");
    expect(plan.body).toContain("head-ref: fix/sum");
    expect(plan.body).toContain("files-visible: 2 of 2");
    expect(plan.body).toContain("potentially stale");
  });

  it("neutralizes a ref that would terminate the metadata comment", () => {
    // Refs are external data; a `-->` inside EITHER ref must not close the
    // HTML comment early and leak or spoof the provenance block.
    const hostile = "feat/x-->y";
    const cases = [
      { baseRef: hostile, headRef: "fix/sum", label: "base-ref" },
      { baseRef: "main", headRef: hostile, label: "head-ref" },
    ] as const;
    for (const { baseRef, headRef, label } of cases) {
      const plan = planPublication(scriptedReview, files, {
        applyVerdict: false,
        headSha: FIXTURE_SHA,
        totalChangedFiles: 2,
        baseRef,
        headRef,
        fingerprint: "c".repeat(64),
      });
      expect(plan.body).not.toContain(hostile);
      expect(plan.body).toContain(`${label}: feat/x- ->y`);
      // The block stayed intact: its later lines are still inside the
      // comment, and the only terminator is the host-generated one.
      const metadataStart = plan.body.indexOf("<!-- effect-agent-pr-review metadata");
      const metadataEnd = plan.body.indexOf("-->", metadataStart);
      // An unterminated comment must fail here, not slip through slice(-1).
      expect(metadataStart).toBeGreaterThanOrEqual(0);
      expect(metadataEnd).toBeGreaterThan(metadataStart);
      const block = plan.body.slice(metadataStart, metadataEnd);
      expect(block).toContain(`${label}: feat/x- ->y`);
      expect(block).toContain("files-visible: 2 of 2");
      expect(block).toContain("potentially stale");
    }
  });

  it("renders model, usage, and run link into the footer in order", () => {
    const plan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
      modelLabel: "openai/gpt-5.6-sol (effort high)",
      usage: { inputTokens: 1234, outputTokens: 56 },
      usageScope: "run",
      runUrl: "https://github.com/acme/widgets/actions/runs/42",
    });
    expect(plan.body).toContain(
      "_Automated review by @effect-agent/pr-review · openai/gpt-5.6-sol (effort high) · " +
        "1234 in / 56 out tokens · [run](https://github.com/acme/widgets/actions/runs/42) · " +
        `reviewed at ${FIXTURE_SHA.slice(0, 7)}._`,
    );
  });

  it("sheds whole low-severity items under the size cap instead of slicing markdown", () => {
    // Every item's body ends in a distinct sentinel so a mid-item slice —
    // which would keep a title while cutting its body — cannot pass.
    const ghostBody = (index: number) =>
      `${"x".repeat(1_960)} GHOST-END-${String(index).padStart(2, "0")}`;
    const concernBody = (name: string) => `${"x".repeat(1_960)} CONCERN-END-${name}`;
    const oversized = CodeReview.make({
      summary: "Oversized body test.",
      verdict: "comment",
      // 20 unanchorable findings (~40k chars demoted) + 10 concerns (~20k
      // chars) exceed the 60k cap; demoted bullets shed first.
      findings: Array.from({ length: 20 }, (_, index) =>
        ReviewFinding.make({
          path: "src/hello.ts",
          startLine: 99,
          endLine: 99,
          severity: "important",
          title: `Ghost ${String(index).padStart(2, "0")}`,
          body: ghostBody(index),
        }),
      ),
      concerns: [
        ReviewConcern.make({
          severity: "blocking",
          title: "Keep me first",
          body: concernBody("keep"),
        }),
        ...Array.from({ length: 8 }, (_, index) =>
          ReviewConcern.make({
            severity: "important",
            title: `Concern ${index}`,
            body: concernBody(String(index)),
          }),
        ),
        ReviewConcern.make({ severity: "nit", title: "Nit concern", body: concernBody("nit") }),
      ],
    });
    const plan = planPublication(oversized, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
      fingerprint: "a".repeat(64),
    });
    expect(plan.body.length).toBeLessThanOrEqual(60_000);
    // Whole items were shed and announced; demoted bullets go first (they
    // already failed validation), so EVERY concern survives complete.
    expect(plan.body).toContain("omitted — the body exceeded GitHub's review size cap");
    expect(plan.body).toContain("### 🛑 Keep me first");
    for (const name of ["keep", "0", "1", "2", "3", "4", "5", "6", "7", "nit"]) {
      expect(plan.body).toContain(`CONCERN-END-${name}`);
    }
    // Every demoted item is either present in full or wholly absent — a
    // title without its end sentinel would mean a mid-item slice.
    let kept = 0;
    for (let index = 0; index < 20; index += 1) {
      const hasTitle = plan.body.includes(`Ghost ${String(index).padStart(2, "0")}`);
      expect(plan.body.includes(`GHOST-END-${String(index).padStart(2, "0")}`)).toBe(hasTitle);
      if (hasTitle) kept += 1;
    }
    expect(kept).toBeGreaterThan(0);
    expect(kept).toBeLessThan(20);
    expect(plan.body).toContain("_Automated review by @effect-agent/pr-review");
    expect(plan.body).toContain(`<!-- effect-agent-pr-review fingerprint=sha256:${"a".repeat(64)}`);
    // The plan's data is complete regardless of what the body could hold.
    expect(plan.demoted).toHaveLength(20);
  });

  it("labels coordinator-scoped usage honestly", () => {
    const plan = planPublication(scriptedReview, files, {
      applyVerdict: false,
      headSha: FIXTURE_SHA,
      totalChangedFiles: 2,
      usage: { inputTokens: 10, outputTokens: 2 },
      usageScope: "coordinator",
    });
    expect(plan.body).toContain("10 in / 2 out tokens (coordinator)");
  });
});

// ---------------------------------------------------------------------------
// Offline end-to-end: scripted model over the real toolkit, source port, and
// publication path — the exact wiring the CLI uses, minus GitHub and the
// live model.
// ---------------------------------------------------------------------------

layer(reviewSelectionAuthorityLayer)("offline review run", (it) => {
  it.effect("reviews the fixture pull request end-to-end and publishes the validated plan", () =>
    Effect.gen(function* () {
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: "src/hello.ts",
        readPath: "src/hello.ts",
        review: scriptedReview,
      });
      const binding = Agent.withModel(PullRequestReviewer, scripted.model);
      const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);

      const program = executeReview(binding, { post: true, applyVerdict: false }).pipe(
        Effect.provide(
          Layer.mergeAll(
            ReviewToolkitLayer.pipe(Layer.provideMerge(fixturePullRequestSourceLayer(fixture))),
            collectingReviewPublisherLayer(published),
            reviewSelectionAuthorityLayer,
            testIdGeneratorLayer,
            unavailableReviewStateAuthenticatorLayer("offline review test"),
          ),
        ),
        Effect.scoped,
      );
      const outcome = yield* requiresNothing(program);

      // The terminal JSON decoded into the scripted review exactly — the
      // concern survived the engine's structured-output boundary, not just
      // the planner's trusted input.
      expect(outcome.review).toEqual(scriptedReview);
      expect(outcome.review.concerns).toEqual([scriptedConcern]);
      expect(outcome.plan.body).toContain(`### ⚠️ ${scriptedConcern.title}`);
      // The walkthrough crossed the same boundary and rendered as a table row.
      expect(outcome.plan.body).toContain(
        "| `src/hello.ts` | Fixes the `two` literal and introduces `three`. |",
      );
      // list -> diff -> read -> final: four model turns, four prompts.
      expect(outcome.turns).toBe(4);
      expect(yield* scripted.calls).toBe(4);

      // The instructions carried the mission framing into the first prompt.
      const prompts = yield* scripted.prompts;
      expect(prompts[0]).toContain("pull request #7");
      expect(prompts[0]).toContain("acme/widgets");

      // Anchor validation split the findings exactly as planned.
      expect(outcome.plan.comments).toHaveLength(1);
      expect(outcome.plan.demoted).toHaveLength(2);

      // The run budget's observed usage aggregates EVERY turn exactly: the
      // scripted model attaches a fixed usage to each of its four turns.
      expect(outcome.usage?.inputTokens).toBe(4 * SCRIPTED_TURN_USAGE.inputTokens);
      expect(outcome.usage?.outputTokens).toBe(4 * SCRIPTED_TURN_USAGE.outputTokens);

      // Publication went through the collecting publisher exactly once.
      const plans = yield* Ref.get(published);
      expect(plans).toHaveLength(1);
      expect(plans[0]?.event).toBe("COMMENT");
      expect(outcome.published?.url).toBe("memory://review/1");
      expect(outcome.published?.inlineComments).toBe(1);

      // The dry-run print path encodes cleanly.
      const encoded = yield* Schema.encodeEffect(ReviewPublicationPlan)(outcome.plan);
      expect(typeof encoded.body).toBe("string");
    }),
  );

  it.effect("publishes nothing on a dry run", () =>
    Effect.gen(function* () {
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: "src/hello.ts",
        readPath: "src/hello.ts",
        review: scriptedReview,
      });
      const binding = Agent.withModel(PullRequestReviewer, scripted.model);
      const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
      const outcome = yield* executeReview(binding, { post: false, applyVerdict: false }).pipe(
        Effect.provide(
          Layer.mergeAll(
            ReviewToolkitLayer.pipe(Layer.provideMerge(fixturePullRequestSourceLayer(fixture))),
            collectingReviewPublisherLayer(published),
            testIdGeneratorLayer,
            unavailableReviewStateAuthenticatorLayer("offline review test"),
          ),
        ),
        Effect.scoped,
      );
      expect(outcome.published).toBeUndefined();
      expect(yield* Ref.get(published)).toHaveLength(0);
    }),
  );

  it.effect("survives one parallel batch of out-of-scope read probes", () =>
    Effect.gen(function* () {
      // A live model may probe several paths outside the review scope in ONE
      // batch — e.g. files the PR description names outside an incremental
      // delta — before any refusal is visible to it. The refusals are typed
      // tool RESULTS, so more sibling probes than the engine's default
      // 3-consecutive-failure limit must not abort the run.
      const PROBE_PREFIX = "probe-";
      const scripted = yield* makePromptKeyedModel("pr-review-probing", (promptJson) => {
        if (promptJson.includes(`${PROBE_PREFIX}1`)) {
          return scriptedFinalParts(JSON.stringify(Schema.encodeSync(CodeReview)(scriptedReview)));
        }
        if (promptJson.includes(OFFLINE_LIST_CALL_ID)) {
          return scriptedToolTurn(
            ...[1, 2, 3, 4].map(
              (index) =>
                ({
                  type: "tool-call",
                  id: `${PROBE_PREFIX}${index}`,
                  name: "read_file_diff",
                  params: { path: `src/not-in-changeset-${index}.ts` },
                  providerExecuted: false,
                }) as const,
            ),
          );
        }
        return scriptedToolTurn({
          type: "tool-call",
          id: OFFLINE_LIST_CALL_ID,
          name: "list_changed_files",
          params: { scope: "all" },
          providerExecuted: false,
        });
      });
      const binding = Agent.withModel(PullRequestReviewer, scripted.model);
      const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
      const outcome = yield* executeReview(binding, { post: false, applyVerdict: false }).pipe(
        Effect.provide(
          Layer.mergeAll(
            ReviewToolkitLayer.pipe(Layer.provideMerge(fixturePullRequestSourceLayer(fixture))),
            collectingReviewPublisherLayer(published),
            testIdGeneratorLayer,
            unavailableReviewStateAuthenticatorLayer("offline review test"),
          ),
        ),
        Effect.scoped,
      );
      // list -> probe batch (all refused) -> final: the run settles normally.
      expect(outcome.review).toEqual(scriptedReview);
      expect(outcome.turns).toBe(3);
    }),
  );

  it.effect("carries unresolved findings from unchanged scope without re-reviewing it", () =>
    Effect.gen(function* () {
      const baseSha = "1".repeat(40);
      const reviewedHeadSha = "2".repeat(40);
      const headSha = "3".repeat(40);
      const profileFingerprint = "a".repeat(64);
      const unchangedFile = ChangedFile.make({
        path: "src/unchanged.ts",
        status: "modified",
        additions: 1,
        deletions: 1,
        patch: "@@ -1 +1 @@\n-old\n+unchanged",
      });
      const correctiveFile = ChangedFile.make({
        path: "src/corrective.ts",
        status: "modified",
        additions: 1,
        deletions: 1,
      });
      // The compare payload has no patch. The selected-source decorator must
      // enrich it from the current full source without invalidating the
      // authenticated incremental selection's stable identity.
      const enrichedCorrectiveFile = ChangedFile.make({
        ...correctiveFile,
        reviewBaseContent: "before",
        reviewHeadContent: "after",
      });
      const metadata = PullRequestMetadata.make({
        repository: "acme/widgets",
        number: 30,
        title: "Correct one finding",
        body: "",
        baseRef: "main",
        baseSha,
        headRef: "fix/review",
        headSha,
        totalChangedFiles: 2,
      });
      const priorFinding = StoredReviewFinding.make({
        path: "src/unchanged.ts",
        startLine: 1,
        endLine: 1,
        severity: "blocking",
        title: "Unchanged blocker",
        body: "This remains active until its path changes.",
      });
      const priorState = ReviewState.make({
        version: 1,
        repository: metadata.repository,
        pullRequestNumber: metadata.number,
        baseRef: metadata.baseRef,
        baseSha,
        headRef: metadata.headRef,
        reviewedHeadSha,
        profileFingerprint,
        acceptedScopeFingerprint: "b".repeat(64),
        reviewedPathCount: 2,
        unresolvedFindings: [priorFinding],
        unresolvedConcerns: [],
        lastReviewMode: "full",
      });
      const selection = yield* selectReviewRange({
        requestedMode: "incremental",
        current: metadata,
        fullFiles: [unchangedFile, correctiveFile],
        profileFingerprint,
        priorState,
        comparison: ReviewHeadComparison.make({
          status: "ahead",
          baseSha: reviewedHeadSha,
          headSha,
          mergeBaseSha: reviewedHeadSha,
          files: [correctiveFile],
          truncated: false,
        }),
      });
      const currentReview = CodeReview.make({
        summary: "The corrective delta introduces no new findings.",
        verdict: "approve",
        findings: [],
      });
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: correctiveFile.path,
        readPath: correctiveFile.path,
        review: currentReview,
      });
      const binding = Agent.withModel(PullRequestReviewer, scripted.model);
      const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
      const fullSource = fixturePullRequestSourceLayer(
        FixturePullRequest.make({
          metadata,
          files: [
            FixtureFile.make({ file: unchangedFile, headContent: "unchanged" }),
            FixtureFile.make({ file: enrichedCorrectiveFile, headContent: "after" }),
          ],
        }),
      );
      const selectedSource = selectedPullRequestSourceLayer(selection).pipe(
        Layer.provide(fullSource),
      );
      const outcome = yield* executeReview(binding, {
        post: false,
        applyVerdict: false,
        reviewShape: "flat",
        selection,
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            ReviewToolkitLayer.pipe(Layer.provideMerge(selectedSource)),
            collectingReviewPublisherLayer(published),
            testIdGeneratorLayer,
            unavailableReviewStateAuthenticatorLayer("offline review test"),
          ),
        ),
        Effect.scoped,
      );

      expect(selection.files.map((file) => file.path)).toEqual(["src/corrective.ts"]);
      expect(outcome.coverage.status).toBe("complete");
      expect(outcome.activeFindings.map((finding) => finding.title)).toEqual([priorFinding.title]);
      expect(outcome.plan.body).toContain("Unresolved findings carried from unchanged scope");
      expect(outcome.plan.body).toContain(priorFinding.title);
      expect((yield* scripted.prompts).join("\n")).not.toContain("src/unchanged.ts");
    }),
  );

  it.effect(
    "reports a full source truncated by the host bound as incomplete instead of rejecting it",
    () =>
      Effect.gen(function* () {
        const file = ChangedFile.make({
          path: "src/visible.ts",
          status: "modified",
          additions: 1,
          deletions: 1,
          patch: "@@ -1 +1 @@\n-old\n+new",
        });
        const metadata = PullRequestMetadata.make({
          repository: "acme/widgets",
          number: 32,
          title: "Bounded source",
          body: "",
          baseRef: "main",
          baseSha: "1".repeat(40),
          headRef: "fix/bounded-source",
          headSha: "2".repeat(40),
          // The adapter's visible list is bounded below GitHub's total.
          totalChangedFiles: 2,
        });
        const selection = yield* selectReviewRange({
          requestedMode: "final",
          current: metadata,
          fullFiles: [file],
          profileFingerprint: "a".repeat(64),
          priorState: undefined,
          comparison: undefined,
        });
        const scripted = yield* makeOfflineReviewerModel({
          diffPath: file.path,
          readPath: file.path,
          review: CodeReview.make({
            summary: "one visible file",
            verdict: "approve",
            findings: [],
          }),
        });
        const outcome = yield* executeReview(Agent.withModel(PullRequestReviewer, scripted.model), {
          post: false,
          applyVerdict: false,
          selection,
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              ReviewToolkitLayer.pipe(
                Layer.provideMerge(
                  fixturePullRequestSourceLayer(
                    FixturePullRequest.make({
                      metadata,
                      files: [FixtureFile.make({ file, headContent: "new" })],
                    }),
                  ),
                ),
              ),
              collectingReviewPublisherLayer(
                yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]),
              ),
              testIdGeneratorLayer,
              unavailableReviewStateAuthenticatorLayer("bounded source test"),
            ),
          ),
          Effect.scoped,
        );

        expect(outcome.coverage.status).toBe("incomplete");
        expect(outcome.plan.body).toContain("Reviewed 1 of 2 changed files");
      }),
  );

  it.effect(
    "rejects a sealed selection when a current rename has the same path but different ancestry",
    () =>
      Effect.gen(function* () {
        const baseSha = "1".repeat(40);
        const headSha = "2".repeat(40);
        const stale = ChangedFile.make({
          path: "src/new.ts",
          previousPath: "src/old-a.ts",
          status: "renamed",
          additions: 1,
          deletions: 1,
          patch: "@@ -1 +1 @@\n-old\n+new",
        });
        const current = ChangedFile.make({ ...stale, previousPath: "src/old-b.ts" });
        const metadata = PullRequestMetadata.make({
          repository: "acme/widgets",
          number: 31,
          title: "Rename source",
          body: "",
          baseRef: "main",
          baseSha,
          headRef: "fix/rename",
          headSha,
          totalChangedFiles: 1,
        });
        const selection = yield* selectReviewRange({
          requestedMode: "final",
          current: metadata,
          fullFiles: [stale],
          profileFingerprint: "a".repeat(64),
          priorState: undefined,
          comparison: undefined,
        });
        const scripted = yield* makeOfflineReviewerModel({
          diffPath: current.path,
          readPath: current.path,
          review: CodeReview.make({ summary: "unused", verdict: "approve", findings: [] }),
        });
        const error = yield* executeReview(Agent.withModel(PullRequestReviewer, scripted.model), {
          post: false,
          applyVerdict: false,
          selection,
        }).pipe(
          Effect.flip,
          Effect.provide(
            Layer.mergeAll(
              ReviewToolkitLayer.pipe(
                Layer.provideMerge(
                  fixturePullRequestSourceLayer(
                    FixturePullRequest.make({
                      metadata,
                      files: [FixtureFile.make({ file: current, headContent: "new" })],
                    }),
                  ),
                ),
              ),
              collectingReviewPublisherLayer(
                yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]),
              ),
              testIdGeneratorLayer,
              unavailableReviewStateAuthenticatorLayer("rename test"),
            ),
          ),
        );
        expect(error).toMatchObject({
          _tag: "ReviewSelectionViolation",
          reason: "review selection evidence does not match the model-visible source range",
        });
        expect(yield* scripted.prompts).toEqual([]);
      }),
  );
});

// ---------------------------------------------------------------------------
// Opt-in live profile: the SAME definition bound to a real OpenAI model
// over the fixture pull request, behind the explicit environment gate.
// Ordinary gates make zero network calls and need zero credentials.
// ---------------------------------------------------------------------------

const liveEnabled = liveProfileEnabled(process.env, "OPENAI_API_KEY");

describe.skipIf(!liveEnabled)("pr-review live profile (opt-in)", () => {
  it.live(
    "reviews the fixture pull request with a live OpenAI model",
    () =>
      Effect.gen(function* () {
        const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
        const binding = Agent.withModel(PullRequestReviewer, makeOpenAiReviewModel());
        const outcome = yield* executeReview(binding, {
          post: false,
          applyVerdict: false,
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              ReviewToolkitLayer.pipe(Layer.provideMerge(fixturePullRequestSourceLayer(fixture))),
              collectingReviewPublisherLayer(published),
              openAiClientLayer,
              reviewSelectionAuthorityLayer,
              testIdGeneratorLayer,
              unavailableReviewStateAuthenticatorLayer("live review test"),
            ),
          ),
          Effect.scoped,
        );
        // The output contract held against a real model; anchors that survived
        // validation must reference the only diff in the fixture.
        expect(outcome.review.summary.length).toBeGreaterThan(0);
        for (const comment of outcome.plan.comments) {
          expect(comment.path).toBe("src/hello.ts");
          expect([1, 2, 3, 4]).toContain(comment.line);
        }
        expect(yield* Ref.get(published)).toHaveLength(0);
      }),
    300_000,
  );
});

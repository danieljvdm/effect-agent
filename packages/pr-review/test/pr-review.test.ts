import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Layer, Ref, Schema } from "effect";
import { Agent, IdGenerator } from "effect-agent";
import { Tool } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

import {
  anchorViolation,
  annotatePatch,
  ChangedFile,
  CodeReview,
  executeReview,
  liveProfileEnabled,
  ListChangedFiles,
  makeOpenAiReviewModel,
  normalizeRepoRelativePath,
  openAiClientLayer,
  parsePatch,
  planPublication,
  commentableLines,
  PullRequestMetadata,
  PullRequestReviewer,
  PullRequestReviewerProfile,
  pullRequestReviewerProfile,
  ReadFile,
  ReadFileDiff,
  ReviewConcern,
  ReviewFinding,
  ReviewPublicationPlan,
  ReviewToolkitLayer,
} from "../src/index.ts";
import {
  collectingReviewPublisherLayer,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  makeOfflineReviewerModel,
  SCRIPTED_TURN_USAGE,
} from "../src/testing.ts";

describe("OpenAI tool schema compatibility", () => {
  it("encodes every reviewer tool as a strict OpenAI object schema", () => {
    for (const tool of [ListChangedFiles, ReadFileDiff, ReadFile]) {
      const jsonSchema = Tool.getJsonSchema(tool, { transformer: toCodecOpenAI });
      expect(jsonSchema.type).toBe("object");
      expect(jsonSchema.anyOf).toBeUndefined();
    }
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
    ).toContain("no textual diff");
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
    expect(plan.body).toContain("_(demoted: file has no textual diff)_");
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
    // already failed validation), so every concern survives complete.
    expect(plan.body).toContain("omitted — the body exceeded GitHub's review size cap");
    expect(plan.body).toContain("### 🛑 Keep me first");
    expect(plan.body).toContain("CONCERN-END-keep");
    expect(plan.body).toContain("CONCERN-END-nit");
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

describe("offline review run", () => {
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
            IdGenerator.layer,
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
            IdGenerator.layer,
          ),
        ),
        Effect.scoped,
      );
      expect(outcome.published).toBeUndefined();
      expect(yield* Ref.get(published)).toHaveLength(0);
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
              IdGenerator.layer,
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

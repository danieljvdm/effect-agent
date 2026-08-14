import { Agent, IdGenerator } from "@effect-agent/core";
import { OpenAiClient } from "@effect/ai-openai";
import { describe, expect, it } from "@effect/vitest";
import { Config, Effect, Exit, Layer, Ref, Schema } from "effect";
import { Tool } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";
import { FetchHttpClient } from "effect/unstable/http";

import {
  anchorViolation,
  annotatePatch,
  ChangedFile,
  CodeReview,
  collectingReviewPublisherLayer,
  commentableLines,
  executeReview,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  liveReviewProfileEnabled,
  ListChangedFiles,
  makeOpenAiReviewer,
  makeOfflineReviewerModel,
  normalizeRepoRelativePath,
  parsePatch,
  planPublication,
  PullRequestMetadata,
  PullRequestReviewer,
  PullRequestReviewerProfile,
  ReadFile,
  ReadFileDiff,
  pullRequestReviewerProfile,
  ReviewFinding,
  ReviewPublicationPlan,
  ReviewToolkitLayer,
} from "../src/index.ts";

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
    headSha: "0123456789abcdef0123456789abcdef01234567",
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

/** The review the offline script returns: one valid anchor, two invalid. */
const scriptedReview = CodeReview.make({
  summary: "The constant fix is correct; two notes could not be anchored.",
  verdict: "comment",
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
    const plan = planPublication(scriptedReview, files, { applyVerdict: false });
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
      findings: [],
    });
    expect(planPublication(blocking, files, { applyVerdict: false }).event).toBe("COMMENT");
    expect(planPublication(blocking, files, { applyVerdict: true }).event).toBe("REQUEST_CHANGES");
    const approving = CodeReview.make({ summary: "Fine.", verdict: "approve", findings: [] });
    expect(planPublication(approving, files, { applyVerdict: true }).event).toBe("APPROVE");
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
    const body = planPublication(tricky, files, { applyVerdict: false }).comments[0]?.body ?? "";
    expect(body).toContain("````suggestion");
  });

  it("comments a multi-line range with start_line strictly below line", () => {
    const ranged = CodeReview.make({
      summary: "Range test.",
      verdict: "comment",
      findings: [ReviewFinding.make({ ...validFinding, startLine: 2, endLine: 3 })],
    });
    const comment = planPublication(ranged, files, { applyVerdict: false }).comments[0];
    expect(comment?.startLine).toBe(2);
    expect(comment?.line).toBe(3);
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

      // The terminal JSON decoded into the scripted review exactly.
      expect(outcome.review).toEqual(scriptedReview);
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

const liveEnabled = liveReviewProfileEnabled(process.env);

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

describe.skipIf(!liveEnabled)("pr-review live profile (opt-in)", () => {
  it.live(
    "reviews the fixture pull request with a live OpenAI model",
    () =>
      Effect.gen(function* () {
        const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
        const outcome = yield* executeReview(makeOpenAiReviewer(), {
          post: false,
          applyVerdict: false,
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              ReviewToolkitLayer.pipe(Layer.provideMerge(fixturePullRequestSourceLayer(fixture))),
              collectingReviewPublisherLayer(published),
              OpenAiClientLayer,
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

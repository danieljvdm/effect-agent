import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import type { Crypto } from "effect";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import type { IdGenerator } from "effect-agent";
import { ToolExecutionClass } from "effect-agent";
import { LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";

import {
  ChangedFile,
  CodeReview,
  compileIgnoreGlobs,
  enforceFindingsBound,
  fullReviewExecutionContextLayer,
  noReviewAdjudicationHost,
  PrReview,
  PullRequestMetadata,
  type PullRequestSource,
  ReviewAdjudicationHost,
  type ReviewExecutionContext,
  ReviewFinding,
  type ReviewPublicationPlan,
  type ReviewPublisher,
} from "../src/index.ts";
import {
  collectingReviewPublisherLayer,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  makeOfflineReviewerModel,
  makePromptKeyedModel,
  scriptedFinalParts,
  scriptedToolTurn,
} from "../src/testing.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

// ---------------------------------------------------------------------------
// Fixture: one reviewable file plus one lockfile the ignore tests exclude.
// ---------------------------------------------------------------------------

const FIXTURE_SHA = "abcdefabcdefabcdefabcdefabcdefabcdefabcd";

const HELLO_PATCH = [
  "@@ -1,3 +1,4 @@",
  " const one = 1;",
  "-const two = 3;",
  "+const two = 2;",
  "+const three = 3;",
  " export const sum = one + two;",
].join("\n");

const LOCK_PATCH = ["@@ -1,1 +1,1 @@", "-lockfile v1", "+lockfile v2"].join("\n");

const fixture = FixturePullRequest.make({
  metadata: PullRequestMetadata.make({
    repository: "acme/widgets",
    number: 9,
    title: "Bump a constant and the lockfile",
    body: "",
    baseRef: "main",
    headRef: "chore/bump",
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
      headContent:
        "const one = 1;\nconst two = 2;\nconst three = 3;\nexport const sum = one + two;",
    }),
    FixtureFile.make({
      file: ChangedFile.make({
        path: "deps/bun.lock",
        status: "modified",
        additions: 1,
        deletions: 1,
        patch: LOCK_PATCH,
      }),
    }),
  ],
});

const nitFinding = ReviewFinding.make({
  path: "src/hello.ts",
  startLine: 3,
  endLine: 3,
  severity: "nit",
  title: "Name the magic number",
  body: "The literal duplicates the meaning of `three`.",
});

const importantFinding = ReviewFinding.make({
  path: "src/hello.ts",
  startLine: 2,
  endLine: 2,
  severity: "important",
  title: "Wrong constant restored",
  body: "The replacement value looks unintended.",
});

const singleFindingReview = CodeReview.make({
  summary: "One small note.",
  verdict: "comment",
  findings: [nitFinding],
});

// ---------------------------------------------------------------------------
// Glob semantics.
// ---------------------------------------------------------------------------

describe("compileIgnoreGlobs", () => {
  it("matches the documented vocabulary and nothing more", () => {
    const ignored = compileIgnoreGlobs(["**/*.lock", "dist/**", "docs/*.md", "exact.txt"]);
    expect(ignored("deps/bun.lock")).toBe(true);
    expect(ignored("bun.lock")).toBe(true);
    expect(ignored("dist/index.mjs")).toBe(true);
    expect(ignored("dist/nested/deep.mjs")).toBe(true);
    expect(ignored("docs/readme.md")).toBe(true);
    expect(ignored("exact.txt")).toBe(true);
    // `*` never crosses a separator; literals never glob.
    expect(ignored("docs/nested/readme.md")).toBe(false);
    expect(ignored("src/hello.ts")).toBe(false);
    expect(ignored("distX/file.mjs")).toBe(false);
    expect(compileIgnoreGlobs([])("anything")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// The host-side findings bound.
// ---------------------------------------------------------------------------

describe("enforceFindingsBound", () => {
  it("keeps a compliant review untouched and trims an oversized one by severity", () => {
    const compliant = CodeReview.make({
      summary: "ok",
      verdict: "comment",
      findings: [nitFinding, importantFinding],
    });
    expect(enforceFindingsBound(compliant, 2)).toBe(compliant);
    const trimmed = enforceFindingsBound(compliant, 1);
    expect(trimmed.findings).toHaveLength(1);
    expect(trimmed.findings[0]?.severity).toBe("important");
  });
});

// ---------------------------------------------------------------------------
// Factory behavior over the real run path (offline scripted model).
// ---------------------------------------------------------------------------

const runFactoryReviewer = <E, R>(
  run: (options?: { readonly post?: boolean }) => Effect.Effect<unknown, E, R>,
) =>
  Effect.gen(function* () {
    const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
    const outcome = yield* run({ post: true }).pipe(
      Effect.provideService(ReviewAdjudicationHost, noReviewAdjudicationHost),
      Effect.provide(fullReviewExecutionContextLayer("offline factory full review")),
      Effect.provide(
        Layer.merge(
          fixturePullRequestSourceLayer(fixture),
          collectingReviewPublisherLayer(published),
        ),
      ),
    );
    return { outcome, published: yield* Ref.get(published) };
  });

layer(NodeCrypto.layer)("PrReview.make", (it) => {
  it.effect("injects guidance between the mission framing and the intact contract", () =>
    Effect.gen(function* () {
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: "src/hello.ts",
        readPath: "src/hello.ts",
        review: singleFindingReview,
      });
      const reviewer = PrReview.make({
        model: scripted.model,
        guidance: (mission) => [
          `This is an Effect codebase (reviewing PR #${mission.number}).`,
          "Flag any public API returning a naked Promise.",
        ],
        maxFindings: 5,
      });
      yield* runFactoryReviewer(reviewer.run);
      const prompts = yield* scripted.prompts;
      const first = prompts[0] ?? "";
      // Guidance present, mission-aware, and positioned before the procedure.
      expect(first).toContain("This is an Effect codebase (reviewing PR #9).");
      expect(first).toContain("Flag any public API returning a naked Promise.");
      // The machine contract survives every customization.
      expect(first).toContain("Report at most 5 findings");
      expect(first).toContain("Then return ONLY a JSON object");
      expect(first).toContain("R-numbers from read_file_diff");
    }),
  );

  it.effect("removes ignored files from the reviewer's observation surface", () =>
    Effect.gen(function* () {
      // The scripted review carries a finding on the ignored lockfile; the
      // fail-closed planner must demote it because the file is invisible.
      const lockFinding = ReviewFinding.make({
        path: "deps/bun.lock",
        startLine: 1,
        endLine: 1,
        severity: "nit",
        title: "Lockfile note",
        body: "Should never anchor: the file is ignored.",
      });
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: "src/hello.ts",
        readPath: "src/hello.ts",
        review: CodeReview.make({
          summary: "Reviewed with an ignored lockfile.",
          verdict: "comment",
          findings: [nitFinding, lockFinding],
        }),
      });
      const reviewer = PrReview.make({
        model: scripted.model,
        ignore: ["**/*.lock"],
      });
      const { outcome, published } = yield* runFactoryReviewer(reviewer.run);

      // The committed list_changed_files result never mentioned the lockfile.
      const prompts = yield* scripted.prompts;
      for (const prompt of prompts) {
        expect(prompt).not.toContain("bun.lock");
      }

      // The lockfile finding failed anchor validation (not in the changeset).
      const plan = published[0];
      expect(plan?.comments.map((comment) => comment.path)).toEqual(["src/hello.ts"]);
      expect(plan?.demoted.map((finding) => finding.path)).toEqual(["deps/bun.lock"]);
      expect(outcome).toBeDefined();
    }),
  );

  it.effect("enforces the configured findings bound on the way out", () =>
    Effect.gen(function* () {
      const scripted = yield* makeOfflineReviewerModel({
        diffPath: "src/hello.ts",
        readPath: "src/hello.ts",
        review: CodeReview.make({
          summary: "Two findings, one allowed.",
          verdict: "comment",
          findings: [nitFinding, importantFinding],
        }),
      });
      const reviewer = PrReview.make({ model: scripted.model, maxFindings: 1 });
      const { published } = yield* runFactoryReviewer(reviewer.run);
      const plan = published[0];
      // The most severe finding survived the host-side trim.
      expect(plan?.comments).toHaveLength(1);
      expect(plan?.comments[0]?.body).toContain("Wrong constant restored");
      expect(plan?.demoted).toHaveLength(0);
    }),
  );

  it("rejects extra tools that are not declared read-only", () => {
    const writeTool = Tool.make("write_stuff", {
      description: "A tool that is not read-only.",
      parameters: Schema.Struct({ scope: Schema.Literal("all") }),
      success: Schema.Struct({ done: Schema.Boolean }),
    });
    const typesModel = Model.make(
      "scripted",
      "factory-types",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.empty,
        }),
      ),
    );
    expect(() => PrReview.make({ model: typesModel, extraTools: [writeTool] })).toThrow(
      /read-only/,
    );
  });

  it.effect("merges a read-only extra tool whose handler the caller provides", () =>
    Effect.gen(function* () {
      const GUIDELINES = "Prefer typed errors over thrown exceptions.";
      const ReadGuidelines = Tool.make("read_review_guidelines", {
        description: "Read the team review guidelines.",
        parameters: Schema.Struct({ scope: Schema.Literal("all") }),
        success: Schema.Struct({ guidelines: Schema.String }),
      }).annotate(ToolExecutionClass, "readonly");
      const guidelinesLayer = Toolkit.make(ReadGuidelines).toLayer({
        read_review_guidelines: () => Effect.succeed({ guidelines: GUIDELINES }),
      });

      const scripted = yield* makePromptKeyedModel("factory-extra-tool", (promptJson) => {
        if (promptJson.includes("guidelines-1")) {
          return scriptedFinalParts(
            JSON.stringify(Schema.encodeSync(CodeReview)(singleFindingReview)),
          );
        }
        return scriptedToolTurn({
          type: "tool-call",
          id: "guidelines-1",
          name: "read_review_guidelines",
          params: { scope: "all" },
          providerExecuted: false,
        });
      });

      const reviewer = PrReview.make({
        model: scripted.model,
        extraTools: [ReadGuidelines],
      });
      const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
      const outcome = yield* reviewer
        .run({ post: true })
        .pipe(
          Effect.provideService(ReviewAdjudicationHost, noReviewAdjudicationHost),
          Effect.provide(fullReviewExecutionContextLayer("offline extra-tool full review")),
          Effect.provide(
            Layer.mergeAll(
              fixturePullRequestSourceLayer(fixture),
              collectingReviewPublisherLayer(published),
              guidelinesLayer,
            ),
          ),
        );

      // The extra tool executed through the merged toolkit and its result was
      // committed to the conversation the model saw.
      const prompts = yield* scripted.prompts;
      expect(prompts[1]).toContain(GUIDELINES);
      expect(outcome.review).toEqual(singleFindingReview);
      expect(yield* Ref.get(published)).toHaveLength(1);
    }),
  );
});

// ---------------------------------------------------------------------------
// E/R compile proofs for the factory surface (AGENTS.md change discipline):
// framework plumbing is provided internally, real dependencies stay visible.
// ---------------------------------------------------------------------------

const typesModel = Model.make(
  "scripted",
  "factory-proof-model",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: () => Stream.empty,
    }),
  ),
);

const ProofTool = Tool.make("proof_extra", {
  description: "Type-proof extra tool.",
  parameters: Schema.Struct({ scope: Schema.Literal("all") }),
  success: Schema.Struct({ value: Schema.String }),
}).annotate(ToolExecutionClass, "readonly");

const plainRun = PrReview.make({ model: typesModel }).run();
const extraRun = PrReview.make({ model: typesModel, extraTools: [ProofTool] }).run();
const fanOutRun = PrReview.makeFanOut({ model: typesModel }).run();

type ServicesOf<T> = T extends Effect.Effect<infer _A, infer _E, infer R> ? R : never;

type PlainServices = ServicesOf<typeof plainRun>;
type ExtraServices = ServicesOf<typeof extraRun>;
type FanOutServices = ServicesOf<typeof fanOutRun>;

// The source, publisher, adjudication host, execution context, and Crypto stay
// visible on every shape.
type PlainSourceProof = Assert<Equal<Extract<PlainServices, PullRequestSource>, PullRequestSource>>;
type PlainPublisherProof = Assert<Equal<Extract<PlainServices, ReviewPublisher>, ReviewPublisher>>;
type PlainAdjudicationHostProof = Assert<
  Equal<Extract<PlainServices, ReviewAdjudicationHost>, ReviewAdjudicationHost>
>;
type PlainExecutionContextProof = Assert<
  Equal<Extract<PlainServices, ReviewExecutionContext>, ReviewExecutionContext>
>;
type PlainCryptoProof = Assert<Equal<Extract<PlainServices, Crypto.Crypto>, Crypto.Crypto>>;
type FanOutCryptoProof = Assert<Equal<Extract<FanOutServices, Crypto.Crypto>, Crypto.Crypto>>;
type FanOutSourceProof = Assert<
  Equal<Extract<FanOutServices, PullRequestSource>, PullRequestSource>
>;
type FanOutAdjudicationHostProof = Assert<
  Equal<Extract<FanOutServices, ReviewAdjudicationHost>, ReviewAdjudicationHost>
>;
type FanOutExecutionContextProof = Assert<
  Equal<Extract<FanOutServices, ReviewExecutionContext>, ReviewExecutionContext>
>;
// Framework plumbing is satisfied internally.
type PlainIdGeneratorExcludedProof = Assert<Equal<Extract<PlainServices, IdGenerator>, never>>;
type FanOutIdGeneratorExcludedProof = Assert<Equal<Extract<FanOutServices, IdGenerator>, never>>;
// An extra tool's handler is the caller's dependency, visible in `R` — and
// absent when no extra tool is configured.
type ExtraHandlerProof = Assert<
  Equal<
    Extract<ExtraServices, Tool.HandlersFor<{ proof_extra: typeof ProofTool }>>,
    Tool.HandlersFor<{ proof_extra: typeof ProofTool }>
  >
>;
type PlainHandlerExcludedProof = Assert<
  Equal<Extract<PlainServices, Tool.HandlersFor<{ proof_extra: typeof ProofTool }>>, never>
>;

describe("factory type proofs", () => {
  it("keeps ports and extra handlers visible while hiding framework plumbing", () => {
    const plainSourceProof: PlainSourceProof = true;
    const plainPublisherProof: PlainPublisherProof = true;
    const plainAdjudicationHostProof: PlainAdjudicationHostProof = true;
    const plainExecutionContextProof: PlainExecutionContextProof = true;
    const plainCryptoProof: PlainCryptoProof = true;
    const fanOutCryptoProof: FanOutCryptoProof = true;
    const fanOutSourceProof: FanOutSourceProof = true;
    const fanOutAdjudicationHostProof: FanOutAdjudicationHostProof = true;
    const fanOutExecutionContextProof: FanOutExecutionContextProof = true;
    const plainIdGeneratorExcludedProof: PlainIdGeneratorExcludedProof = true;
    const fanOutIdGeneratorExcludedProof: FanOutIdGeneratorExcludedProof = true;
    const extraHandlerProof: ExtraHandlerProof = true;
    const plainHandlerExcludedProof: PlainHandlerExcludedProof = true;
    expect([
      plainSourceProof,
      plainPublisherProof,
      plainAdjudicationHostProof,
      plainExecutionContextProof,
      plainCryptoProof,
      fanOutCryptoProof,
      fanOutSourceProof,
      fanOutAdjudicationHostProof,
      fanOutExecutionContextProof,
      plainIdGeneratorExcludedProof,
      fanOutIdGeneratorExcludedProof,
      extraHandlerProof,
      plainHandlerExcludedProof,
    ]).toEqual([true, true, true, true, true, true, true, true, true, true, true, true, true]);
  });
});

// ---------------------------------------------------------------------------
// Containment of bad read requests: a model asking for an out-of-changeset
// path gets a typed refusal it can correct — the review continues (the live
// regression: PR #26's third review died on exactly this).
// ---------------------------------------------------------------------------

layer(NodeCrypto.layer)("out-of-changeset reads", (it) => {
  it.effect("refuses the read as a result and completes the review", () =>
    Effect.gen(function* () {
      const scripted = yield* makePromptKeyedModel("bad-read-reviewer", (promptJson) => {
        if (promptJson.includes("bad-read-1")) {
          // The refusal is model-visible: the violation reached the prompt.
          expect(promptJson).toContain("ReviewInputViolation");
          expect(promptJson).toContain("not part of this pull request's changeset");
          return scriptedFinalParts(
            JSON.stringify(Schema.encodeSync(CodeReview)(singleFindingReview)),
          );
        }
        return scriptedToolTurn({
          type: "tool-call",
          id: "bad-read-1",
          name: "read_file",
          params: { path: "src/internal/not-in-changeset.ts" },
          providerExecuted: false,
        });
      });
      const reviewer = PrReview.make({ model: scripted.model });
      const { outcome, published } = yield* runFactoryReviewer(reviewer.run);
      expect(outcome).toBeDefined();
      expect(published).toHaveLength(1);
      expect(published[0]?.comments).toHaveLength(1);
    }),
  );
});

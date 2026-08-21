import type { ReviewPublicationPlan } from "@effect-agent/pr-review";
import {
  ChangedFile,
  CodeReview,
  fullReviewExecutionContextLayer,
  noReviewAdjudicationHost,
  PullRequestMetadata,
  ReviewAdjudicationHost,
  ReviewFinding,
} from "@effect-agent/pr-review";
import {
  collectingReviewPublisherLayer,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  makePromptKeyedModel,
  scriptedFinalParts,
  scriptedToolTurn,
} from "@effect-agent/pr-review/testing";
import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";

import {
  makeExampleReviewer,
  ReadReviewConventionsLayer,
  REVIEW_CONVENTIONS,
} from "../src/index.ts";

// ---------------------------------------------------------------------------
// One offline end-to-end run of the customized reviewer: the guidance, the
// extra conventions tool, and the lockfile ignore all observable through the
// real run path — no network, no credentials.
// ---------------------------------------------------------------------------

const HELLO_PATCH = [
  "@@ -1,2 +1,2 @@",
  " const one = 1;",
  "-export const load = () => fetch(url).then((r) => r.json());",
  "+export const load = () => fetch(url).then((response) => response.json());",
].join("\n");

const fixture = FixturePullRequest.make({
  metadata: PullRequestMetadata.make({
    repository: "acme/widgets",
    number: 42,
    title: "Refactor the loader",
    body: "",
    baseRef: "main",
    headRef: "refactor/loader",
    headSha: "1234567890abcdef1234567890abcdef12345678",
    totalChangedFiles: 2,
  }),
  files: [
    FixtureFile.make({
      file: ChangedFile.make({
        path: "src/loader.ts",
        status: "modified",
        additions: 1,
        deletions: 1,
        patch: HELLO_PATCH,
      }),
    }),
    FixtureFile.make({
      file: ChangedFile.make({
        path: "bun.lock",
        status: "modified",
        additions: 3,
        deletions: 3,
        patch: "@@ -1,1 +1,1 @@\n-v1\n+v2",
      }),
    }),
  ],
});

const scriptedReview = CodeReview.make({
  summary: "The loader still returns a naked Promise.",
  verdict: "comment",
  findings: [
    ReviewFinding.make({
      path: "src/loader.ts",
      startLine: 2,
      endLine: 2,
      severity: "important",
      title: "Public API returns a naked Promise",
      body: "Per the repository conventions, public async operations return Effect.",
    }),
  ],
});

const runReviewCli = Effect.fn("exampleReviewerTest.runReviewCli")(function* (
  args: ReadonlyArray<string>,
) {
  const child = yield* ChildProcess.make("bun", ["src/cli.ts", ...args], {
    cwd: ".",
    env: {
      GITHUB_EVENT_PATH: "",
      GITHUB_REPOSITORY: "",
      OPENAI_API_KEY: "",
    },
    extendEnv: true,
    stderr: "pipe",
    stdout: "pipe",
  });
  const [stdout, stderr, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.stdout)),
    Stream.mkString(Stream.decodeText(child.stderr)),
    child.exitCode,
  ]);
  return { stdout, stderr, exitCode: Number(exitCode) } as const;
});

describe("example reviewer", () => {
  it.effect("runs the customized reviewer end-to-end offline", () =>
    Effect.gen(function* () {
      const scripted = yield* makePromptKeyedModel("example-reviewer-offline", (promptJson) => {
        if (promptJson.includes("conventions-1")) {
          return scriptedFinalParts(JSON.stringify(Schema.encodeSync(CodeReview)(scriptedReview)));
        }
        return scriptedToolTurn({
          type: "tool-call",
          id: "conventions-1",
          name: "read_review_conventions",
          params: { scope: "all" },
          providerExecuted: false,
        });
      });

      const reviewer = makeExampleReviewer(scripted.model);
      const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
      const outcome = yield* reviewer
        .run({ post: true })
        .pipe(
          Effect.provideService(ReviewAdjudicationHost, noReviewAdjudicationHost),
          Effect.provide(
            fullReviewExecutionContextLayer("offline example full review").pipe(
              Layer.provideMerge(
                Layer.mergeAll(
                  fixturePullRequestSourceLayer(fixture),
                  collectingReviewPublisherLayer(published),
                  ReadReviewConventionsLayer,
                  NodeCrypto.layer,
                ),
              ),
            ),
          ),
        );

      const prompts = yield* scripted.prompts;
      // The guidance made it into the instructions, mission-aware.
      expect(prompts[0]).toContain("This repository is an Effect codebase");
      expect(prompts[0]).toContain("Consult read_review_conventions");
      // The extra tool executed and its result was committed.
      expect(prompts[1]).toContain(REVIEW_CONVENTIONS.split("\n")[1]);
      // The ignored lockfile never reached the model.
      for (const prompt of prompts) {
        expect(prompt).not.toContain("bun.lock");
      }
      // The validated plan published the anchored finding.
      expect(outcome.review).toEqual(scriptedReview);
      expect(outcome.plan.comments.map((comment) => comment.path)).toEqual(["src/loader.ts"]);
      expect(yield* Ref.get(published)).toHaveLength(1);
    }),
  );

  it.effect("the real CLI exposes help and reports operational failures", () =>
    Effect.gen(function* () {
      const help = yield* runReviewCli(["--help"]);
      expect(help.exitCode).toBe(0);
      expect(help.stdout).toContain("USAGE");
      expect(help.stdout).toContain("--post");

      const failure = yield* runReviewCli([]);
      expect(failure.exitCode).not.toBe(0);
      expect(`${failure.stdout}\n${failure.stderr}`).toContain("No pull request to review");
      expect(`${failure.stdout}\n${failure.stderr}`).not.toContain("src/command.ts");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

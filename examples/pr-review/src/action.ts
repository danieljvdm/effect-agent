import {
  isCommentableLine,
  makeReviewer,
  ReviewChange,
  ReviewFinding,
  ReviewReport,
  ReviewRequest,
  type ReviewOutcome,
} from "@effect-agent/pr-review";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Console, Effect, Exit, FileSystem, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { type ChangedFile, makeGitHubClient } from "./github.ts";
import { reviewMarker, selectReview } from "./selection.ts";

const MAX_REVIEW_PATCH_CHARS = 320_000;
const MAX_PATCH_CHARS = 80_000;
const MAX_REVIEW_FILES = 100;

export class ActionConfigurationError extends Schema.TaggedError<ActionConfigurationError>()(
  "ActionConfigurationError",
  { message: Schema.String },
) {}

export class BlockingFindings extends Schema.TaggedError<BlockingFindings>()("BlockingFindings", {
  count: Schema.Int,
}) {}

const writeOutputs = Effect.fn("writeReviewOutputs")(function* (
  entries: ReadonlyArray<readonly [string, string | number]>,
) {
  const outputPath = yield* Config.string("GITHUB_OUTPUT").pipe(Config.withDefault(""));
  if (outputPath.length === 0) return;
  const fs = yield* FileSystem.FileSystem;
  yield* fs.writeFileString(
    outputPath,
    `${entries.map(([key, value]) => `${key}=${String(value)}`).join("\n")}\n`,
    { flag: "a" },
  );
});

const skip = Effect.fn("skipReview")(function* (reason: string) {
  yield* Console.log(`PR review skipped: ${reason}`);
  yield* writeOutputs([
    ["skipped", "true"],
    ["reason", reason],
    ["input-tokens", 0],
    ["output-tokens", 0],
    ["blocking-findings", 0],
  ]);
});

const matchesIgnore = (path: string, rawPattern: string): boolean => {
  const pattern = rawPattern.trim().replace(/^\.\//, "");
  if (pattern.length === 0) return false;
  if (path === pattern) return true;
  if (pattern.endsWith("/**")) {
    const prefix = pattern.slice(0, -3).replace(/\/$/, "");
    return path === prefix || path.startsWith(`${prefix}/`);
  }
  if (pattern.startsWith("**/")) {
    const suffix = pattern.slice(3);
    return suffix.startsWith("*.")
      ? path.endsWith(suffix.slice(1))
      : path === suffix || path.endsWith(`/${suffix}`);
  }
  return false;
};

interface ReviewSurface {
  readonly changes: ReadonlyArray<ReviewChange>;
  readonly unreviewedPaths: ReadonlyArray<string>;
  readonly ignoredPaths: ReadonlyArray<string>;
}

export const prepareReviewSurface = (
  files: ReadonlyArray<ChangedFile>,
  ignore: ReadonlyArray<string>,
): ReviewSurface => {
  const changes: Array<ReviewChange> = [];
  const unreviewedPaths: Array<string> = [];
  const ignoredPaths: Array<string> = [];
  let patchChars = 0;
  for (const file of files) {
    if (ignore.some((pattern) => matchesIgnore(file.path, pattern))) {
      ignoredPaths.push(file.path);
      continue;
    }
    const patch = file.patch;
    if (
      patch === undefined ||
      patch.length === 0 ||
      patch.length > MAX_PATCH_CHARS ||
      file.path.length > 512 ||
      changes.length >= MAX_REVIEW_FILES ||
      patchChars + patch.length > MAX_REVIEW_PATCH_CHARS
    ) {
      unreviewedPaths.push(file.path);
      continue;
    }
    changes.push(ReviewChange.make({ path: file.path, patch }));
    patchChars += patch.length;
  }
  return { changes, unreviewedPaths, ignoredPaths };
};

const reanchorToFullPullRequest = (
  files: ReadonlyArray<ChangedFile>,
  report: ReviewReport,
): ReviewReport => {
  const patches = new Map(files.map((file) => [file.path, file.patch] as const));
  return ReviewReport.make({
    summary: report.summary,
    findings: report.findings.flatMap((finding) => {
      if (!patches.has(finding.path)) return [];
      const patch = patches.get(finding.path);
      const line =
        finding.line !== undefined && patch !== undefined && isCommentableLine(patch, finding.line)
          ? finding.line
          : undefined;
      return [
        ReviewFinding.make({
          path: finding.path,
          ...(line === undefined ? {} : { line }),
          severity: finding.severity,
          title: finding.title,
          body: finding.body,
        }),
      ];
    }),
  });
};

const renderBody = (input: {
  readonly report: ReviewReport;
  readonly automatic: boolean;
  readonly scope: "full" | "incremental";
  readonly reviewedFiles: number;
  readonly unreviewedFiles: number;
  readonly ignoredFiles: number;
  readonly inputTokens: number;
  readonly outputTokens: number;
}): string => {
  const unanchored = input.report.findings.filter((finding) => finding.line === undefined);
  const sections = ["## PR review", input.report.summary];
  if (unanchored.length > 0) {
    sections.push(
      "### Findings without an inline anchor",
      ...unanchored.map(
        (finding) =>
          `**${finding.severity}: ${finding.title}** — \`${finding.path}\`\n\n${finding.body}`,
      ),
    );
  }
  sections.push(
    `Scope: ${input.scope}; ${String(input.reviewedFiles)} file(s) reviewed, ${String(input.unreviewedFiles)} unavailable or outside the input bound, ${String(input.ignoredFiles)} ignored.`,
    `Usage: ${String(input.inputTokens)} input / ${String(input.outputTokens)} output tokens.`,
    reviewMarker(input.automatic),
  );
  return sections.join("\n\n");
};

const openAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export const reviewActionProgram = Effect.gen(function* () {
  const repository = yield* Config.nonEmptyString("GITHUB_REPOSITORY");
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository)) {
    return yield* ActionConfigurationError.make({
      message: `Invalid GITHUB_REPOSITORY: ${repository}`,
    });
  }
  const pullRequestNumber = yield* Config.schema(
    Schema.Int.check(Schema.isGreaterThan(0)),
    "PR_REVIEW_PULL_REQUEST",
  );
  const token = yield* Config.redacted("GITHUB_TOKEN");
  const reviewAuthor = yield* Config.nonEmptyString("PR_REVIEW_AUTHOR").pipe(
    Config.withDefault("github-actions[bot]"),
  );
  const mode = yield* Config.literals(["auto", "incremental", "full"], "PR_REVIEW_MODE").pipe(
    Config.withDefault("auto"),
  );
  const expectedHead = yield* Config.string("PR_REVIEW_EXPECTED_HEAD").pipe(Config.withDefault(""));
  const modelName = yield* Config.nonEmptyString("PR_REVIEW_MODEL").pipe(
    Config.withDefault("gpt-5.6-sol"),
  );
  const effort = yield* Config.literals(
    ["low", "medium", "high", "xhigh"],
    "PR_REVIEW_EFFORT",
  ).pipe(Config.withDefault("medium"));
  const guidanceFile = yield* Config.string("PR_REVIEW_GUIDANCE_FILE").pipe(Config.withDefault(""));
  const ignore = (yield* Config.string("PR_REVIEW_IGNORE").pipe(Config.withDefault("")))
    .split(",")
    .map((pattern) => pattern.trim())
    .filter((pattern) => pattern.length > 0);
  const apiUrl = yield* Config.nonEmptyString("GITHUB_API_URL").pipe(
    Config.withDefault("https://api.github.com"),
  );

  const github = yield* makeGitHubClient({
    repository,
    pullRequest: pullRequestNumber,
    token,
    apiUrl,
  });
  const pull = yield* github.getPullRequest;
  if (pull.draft) return yield* skip("draft-pull-request");
  if (expectedHead.length > 0 && pull.headRevision !== expectedHead) {
    return yield* skip("stale-event-head");
  }

  const history = yield* github.listReviews;
  const selection = selectReview({
    mode,
    currentHead: pull.headRevision,
    reviewAuthor,
    history,
  });
  if (selection._tag === "skip") return yield* skip(selection.reason);

  const fullFiles = yield* github.listFiles;
  let scopedFiles: ReadonlyArray<ChangedFile> = fullFiles;
  let actualScope = selection.scope;
  if (selection.scope === "incremental" && selection.baseRevision !== undefined) {
    const comparison = yield* github.compareFiles(selection.baseRevision, pull.headRevision).pipe(
      Effect.map((value) => ({ _tag: "success" as const, value })),
      Effect.catch((error) =>
        Console.warn(
          `Incremental comparison failed; reviewing the full diff: ${error.reason}`,
        ).pipe(Effect.as({ _tag: "failure" as const })),
      ),
    );
    if (
      comparison._tag === "success" &&
      comparison.value.status === "ahead" &&
      comparison.value.files.length < 300
    ) {
      scopedFiles = comparison.value.files;
    } else {
      actualScope = "full";
    }
  }

  const surface = prepareReviewSurface(scopedFiles, ignore);
  const fs = yield* FileSystem.FileSystem;
  const guidance =
    guidanceFile.length === 0
      ? undefined
      : (yield* fs.readFileString(guidanceFile)).slice(0, 20_000);

  let outcome: ReviewOutcome | undefined;
  let report: ReviewReport;
  if (surface.changes.length === 0) {
    report = ReviewReport.make({
      summary:
        surface.ignoredPaths.length === scopedFiles.length
          ? "No changed files matched the configured review scope."
          : "No textual patch fit within the review input bound.",
      findings: [],
    });
  } else {
    const reviewer = makeReviewer({
      model: OpenAiLanguageModel.model(modelName, {
        max_output_tokens: 12_000,
        store: false,
        strictJsonSchema: true,
        reasoning: { effort },
      }),
      ...(guidance === undefined ? {} : { guidance }),
    });
    const reviewExit = yield* reviewer
      .review(
        ReviewRequest.make({
          title: pull.title.slice(0, 1_000),
          description: pull.description.slice(0, 20_000),
          baseRevision: selection.baseRevision ?? pull.baseRevision,
          headRevision: pull.headRevision,
          changes: surface.changes,
          unreviewedPaths: surface.unreviewedPaths
            .filter((path) => path.length <= 512)
            .slice(0, 300),
        }),
      )
      .pipe(Effect.provide(openAiClientLayer), Effect.exit);
    if (Exit.isFailure(reviewExit)) {
      const reviewUrl = yield* github.publishReview({
        commitId: pull.headRevision,
        body: [
          "## PR review",
          "The reviewer did not produce a schema-valid report. No findings were published.",
          reviewMarker(selection.automatic, false),
        ].join("\n\n"),
        comments: [],
      });
      yield* writeOutputs([
        ["skipped", "false"],
        ["reason", "review-failed"],
        ["blocking-findings", 0],
        ["review-url", reviewUrl],
      ]);
      return yield* Effect.failCause(reviewExit.cause);
    }
    outcome = reviewExit.value;
    report = reanchorToFullPullRequest(fullFiles, outcome.report);
  }

  const inputTokens = outcome?.usage.inputTokens ?? 0;
  const outputTokens = outcome?.usage.outputTokens ?? 0;
  const body = renderBody({
    report,
    automatic: selection.automatic,
    scope: actualScope,
    reviewedFiles: surface.changes.length,
    unreviewedFiles: surface.unreviewedPaths.length,
    ignoredFiles: surface.ignoredPaths.length,
    inputTokens,
    outputTokens,
  });
  const reviewUrl = yield* github.publishReview({
    commitId: pull.headRevision,
    body,
    comments: report.findings.flatMap((finding) =>
      finding.line === undefined
        ? []
        : [
            {
              path: finding.path,
              line: finding.line,
              body: `**${finding.severity}: ${finding.title}**\n\n${finding.body}`,
            },
          ],
    ),
  });
  const blocking = report.findings.filter((finding) => finding.severity === "blocking").length;
  yield* writeOutputs([
    ["skipped", "false"],
    ["reason", selection.reason],
    ["input-tokens", inputTokens],
    ["output-tokens", outputTokens],
    ["blocking-findings", blocking],
    ["review-url", reviewUrl],
  ]);
  yield* Console.log(`Posted PR review: ${reviewUrl}`);
  if (blocking > 0) return yield* BlockingFindings.make({ count: blocking });
});

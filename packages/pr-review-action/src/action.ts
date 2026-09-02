import {
  isCommentableLine,
  makeReviewer,
  MAX_REVIEW_PATCH_CHARS,
  ReviewChange,
  ReviewContextError,
  ReviewFileList,
  ReviewFinding,
  type ReviewOutcome,
  ReviewReport,
  type ReviewResolution,
  ReviewRepository,
  ReviewRequest,
  ReviewSource,
} from "@effect-agent/pr-review";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import {
  Cause,
  Config,
  ConfigProvider,
  Console,
  Context,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Result,
  Schema,
} from "effect";

import {
  type ChangedFile,
  GitHubApiFailure,
  isBinaryAssetPath,
  makeExactPatch,
  makeGitHubClient,
  type RepositorySnapshot,
  type StaleReviewHead,
} from "./github.ts";
import {
  type ReviewCostEstimate,
  ReviewExclusion,
  ReviewPresentation,
  withReviewMarker,
  withReviewPauseMarker,
} from "./presentation.ts";
import {
  makeReviewOpenAi,
  REVIEW_COST_LIMIT_MICROUSD,
  reviewCostEstimator,
  reviewModelPricing,
} from "./review-openai.ts";
import {
  reviewModeFromCommand,
  selectReview,
  unresolvedChangeRequestCount,
  unresolvedChangeRequests as selectUnresolvedChangeRequests,
} from "./selection.ts";

export { estimateGpt56CostMicrousd } from "./review-openai.ts";

const MAX_REVIEW_FILES = 100;
const MAX_GENERATED_CLASSIFICATIONS = 100;
const MAX_HYDRATED_SOURCE_BYTES = 8_000_000;

const ACTION_INPUT_BY_CONFIG: Readonly<Record<string, string>> = {
  OPENAI_API_KEY: "INPUT_OPENAI-API-KEY",
  GITHUB_TOKEN: "INPUT_GITHUB-TOKEN",
  PR_REVIEW_PULL_REQUEST: "INPUT_PULL-REQUEST",
  PR_REVIEW_AUTHOR: "INPUT_REVIEW-AUTHOR",
  PR_REVIEW_MODE: "INPUT_MODE",
  PR_REVIEW_COMMAND: "INPUT_COMMAND",
  PR_REVIEW_COMMENT_ID: "INPUT_COMMENT-ID",
  PR_REVIEW_AUTOMATIC_LIMIT: "INPUT_AUTOMATIC-REVIEW-LIMIT",
  PR_REVIEW_EXPECTED_HEAD: "INPUT_EXPECTED-HEAD",
  PR_REVIEW_MODEL: "INPUT_MODEL",
  PR_REVIEW_EFFORT: "INPUT_EFFORT",
  PR_REVIEW_GUIDANCE_FILE: "INPUT_GUIDANCE-FILE",
  PR_REVIEW_IGNORE: "INPUT_IGNORE",
};

/** Prefer local environment configuration, then read the matching GitHub Action input. */
export const withActionInputs = (
  provider: ConfigProvider.ConfigProvider,
): ConfigProvider.ConfigProvider =>
  ConfigProvider.orElse(
    provider,
    ConfigProvider.mapInput(provider, (path) =>
      path.map((segment) =>
        typeof segment === "string" ? (ACTION_INPUT_BY_CONFIG[segment] ?? segment) : segment,
      ),
    ),
  );

export class ActionConfigurationError extends Schema.TaggedError<ActionConfigurationError>()(
  "ActionConfigurationError",
  { message: Schema.String },
) {}

export class BlockingFindings extends Schema.TaggedError<BlockingFindings>()("BlockingFindings", {
  count: Schema.Int,
}) {}

export class UnresolvedChangeRequests extends Schema.TaggedError<UnresolvedChangeRequests>()(
  "UnresolvedChangeRequests",
  { count: Schema.Int },
) {}

export class IncompleteReview extends Schema.TaggedError<IncompleteReview>()("IncompleteReview", {
  unreviewedPaths: Schema.Int,
}) {}

export class ReviewAttemptIncomplete extends Schema.TaggedError<ReviewAttemptIncomplete>()(
  "ReviewAttemptIncomplete",
  {},
) {}

export class IncrementalScopeUnavailable extends Schema.TaggedError<IncrementalScopeUnavailable>()(
  "IncrementalScopeUnavailable",
  {
    priorMergeBase: Schema.String,
    currentMergeBase: Schema.String,
  },
) {}

export const reviewPublicationFailure = (input: {
  readonly blockingFindings: number;
  readonly unreviewedPaths: number;
  readonly unresolvedChangeRequests: number;
  readonly exhausted?: ReviewOutcome["exhausted"];
  readonly incomplete?: boolean;
}):
  | BlockingFindings
  | IncompleteReview
  | UnresolvedChangeRequests
  | ReviewAttemptIncomplete
  | undefined => {
  if (input.blockingFindings > 0) return BlockingFindings.make({ count: input.blockingFindings });
  if (input.unreviewedPaths > 0) {
    return IncompleteReview.make({ unreviewedPaths: input.unreviewedPaths });
  }
  if (input.exhausted !== undefined || input.incomplete === true)
    return ReviewAttemptIncomplete.make({});
  if (input.unresolvedChangeRequests > 0) {
    return UnresolvedChangeRequests.make({ count: input.unresolvedChangeRequests });
  }

  return undefined;
};

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

/** Count stale attempts only against the inspected commit, without publishing their findings. */
export const publishHeadBoundReview = Effect.fn("publishHeadBoundReview")(function* (
  publish: Effect.Effect<string, GitHubApiFailure | StaleReviewHead>,
  staleAttempt: {
    readonly publish: (input: {
      readonly commitId: string;
      readonly body: string;
    }) => Effect.Effect<string, GitHubApiFailure>;
    readonly automatic: boolean;
    readonly failureSummary?: string | undefined;
  },
) {
  return yield* publish.pipe(
    Effect.tapErrorTag(
      "StaleReviewHead",
      Effect.fn(function* (failure) {
        yield* Console.log(
          `PR review publication stopped: inspected ${failure.inspectedHead}, current head ${failure.currentHead}. Recording an incomplete attempt on the inspected commit only.`,
        );

        const reviewUrl = yield* staleAttempt.publish({
          commitId: failure.inspectedHead,
          body: withReviewMarker(
            [
              "## Effect Agent review",
              "> [!CAUTION]\n> This attempt is incomplete because the pull request moved to a newer commit.",
              staleAttempt.failureSummary ?? "No findings were published from this attempt.",
              "This notice records the attempt on the inspected commit. The newer commit still needs its own review.",
            ].join("\n\n"),
            staleAttempt.automatic,
            false,
          ),
        });

        yield* writeOutputs([
          ["skipped", "false"],
          ["reason", "stale-review-head"],
          ["review-url", reviewUrl],
        ]);
      }),
    ),
  );
});

const skip = Effect.fn("skipReview")(function* (
  reason: string,
  reviewUrl?: string,
  unresolvedChangeRequests = 0,
) {
  yield* Console.log(
    reviewUrl === undefined ? `PR review skipped: ${reason}` : `Posted PR review: ${reviewUrl}`,
  );
  yield* writeOutputs([
    ["skipped", "true"],
    ["reason", reason],
    ["input-tokens", 0],
    ["uncached-input-tokens", 0],
    ["cached-input-tokens", 0],
    ["cache-write-input-tokens", 0],
    ["output-tokens", 0],
    ["estimated-cost-usd", "0.000000"],
    ["reserved-cost-usd", "0.000000"],
    ["cost-limit-usd", (REVIEW_COST_LIMIT_MICROUSD / 1_000_000).toFixed(6)],
    ["blocking-findings", 0],
    ["unresolved-change-requests", unresolvedChangeRequests],
    ...(reviewUrl === undefined ? [] : [["review-url", reviewUrl] as const]),
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

// Spend limited review capacity on implementation and configuration before prose
// and documentation assets. Preserve alphabetical order within each group.
const documentationPath = (path: string): boolean =>
  /(^|\/)(docs?|\.changeset)(\/|$)|\.(md|mdx|rst|txt|adoc)$/i.test(path);

/** The host classifies paths at a trusted revision, never at a PR-controlled head. */
export class GeneratedFileClassification extends Context.Service<
  GeneratedFileClassification,
  { readonly isGenerated: (path: string) => Effect.Effect<boolean, GitHubApiFailure> }
>()("@effect-agent/pr-review-action/GeneratedFileClassification") {}

/** Hydrate exact patches in implementation-first order; the reviewer batches large input. */
export const hydrateExactChanges = Effect.fn("hydrateExactChanges")(function* (input: {
  readonly files: ReadonlyArray<ChangedFile>;
  readonly changedPaths: ReadonlyArray<string>;
  readonly base: RepositorySnapshot;
  readonly head: RepositorySnapshot;
  readonly ignore: ReadonlyArray<string>;
}) {
  const classification = yield* GeneratedFileClassification;
  const metadata = new Map(input.files.map((file) => [file.path, file] as const));
  const activeRenames = new Map<string, ChangedFile>();

  for (const file of input.files) {
    const previousPath = file.previousPath;

    if (
      file.status === "renamed" &&
      previousPath !== undefined &&
      // A rename across binary/text formats is a textual addition or deletion,
      // not one ignored change. Leave its two tree paths as separate candidates.
      (isBinaryAssetPath(previousPath) === isBinaryAssetPath(file.path) ||
        [previousPath, file.path].some((path) =>
          input.ignore.some((pattern) => matchesIgnore(path, pattern)),
        )) &&
      input.base.entry(previousPath) !== undefined &&
      input.base.entry(file.path) === undefined &&
      input.head.entry(previousPath) === undefined &&
      input.head.entry(file.path) !== undefined
    ) {
      activeRenames.set(previousPath, file);
      activeRenames.set(file.path, file);
    }
  }
  const candidates = new Map<string, { readonly file: ChangedFile; readonly basePath: string }>();

  for (const changedPath of [...new Set(input.changedPaths)].sort()) {
    const renamed = activeRenames.get(changedPath);
    const path = renamed?.path ?? changedPath;

    const file =
      renamed ??
      metadata.get(path) ??
      ({
        path,
        status: "modified",
        additions: 0,
        deletions: 0,
        patch: undefined,
      } satisfies ChangedFile);

    candidates.set(path, { file, basePath: renamed?.previousPath ?? path });
  }

  const changes: Array<ReviewChange> = [];
  const unreviewedPaths: Array<string> = [];
  const ignoredPaths: Array<string> = [];
  const exclusions: Array<ReviewExclusion> = [];
  const unavailablePaths = new Set<string>();

  const exclude = (
    paths: Array<string>,
    file: ChangedFile,
    basePath: string,
    reason?: ReviewExclusion["reason"],
  ) => {
    paths.push(file.path);
    if (reason !== undefined) exclusions.push(ReviewExclusion.make({ path: file.path, reason }));
    // Input capacity does not revoke source access. Ignore rules and genuinely
    // unreadable entries still exclude both sides of a rename from source tools.
    if (reason === undefined || reason === "unsupported-entry" || reason === "source-read-failed") {
      unavailablePaths.add(file.path).add(basePath);
    }
  };

  let admittedPaths = 0;
  let classificationAttempts = 0;
  let hydratedSourceBytes = 0;

  for (const { file, basePath } of [...candidates.values()].sort(
    (left, right) =>
      Number(documentationPath(left.file.path)) - Number(documentationPath(right.file.path)) ||
      (left.file.path < right.file.path ? -1 : left.file.path > right.file.path ? 1 : 0),
  )) {
    const ignored = [file.path, ...(basePath === file.path ? [] : [basePath])].some(
      (path) =>
        isBinaryAssetPath(path) || input.ignore.some((pattern) => matchesIgnore(path, pattern)),
    );

    if (ignored) {
      exclude(ignoredPaths, file, basePath);
      continue;
    }
    if (file.path.length > 512 || admittedPaths >= MAX_REVIEW_FILES) {
      exclude(
        unreviewedPaths,
        file,
        basePath,
        file.path.length > 512 ? "path-limit" : "file-limit",
      );
      continue;
    }
    const beforeEntry = input.base.entry(basePath);
    const afterEntry = input.head.entry(file.path);

    if (
      (beforeEntry !== undefined &&
        (beforeEntry.type !== "blob" || beforeEntry.mode === "120000")) ||
      (afterEntry !== undefined && (afterEntry.type !== "blob" || afterEntry.mode === "120000"))
    ) {
      admittedPaths += 1;
      exclude(unreviewedPaths, file, basePath, "unsupported-entry");
      continue;
    }
    if (
      basePath === file.path &&
      beforeEntry !== undefined &&
      (afterEntry === undefined || beforeEntry.mode === afterEntry.mode) &&
      classificationAttempts < MAX_GENERATED_CLASSIFICATIONS
    ) {
      classificationAttempts += 1;
      if (yield* classification.isGenerated(file.path)) {
        exclude(ignoredPaths, file, basePath);
        continue;
      }
    }
    admittedPaths += 1;

    const sourceSizesKnown =
      (beforeEntry === undefined || beforeEntry.size !== undefined) &&
      (afterEntry === undefined || afterEntry.size !== undefined);

    const estimatedSourceBytes = (beforeEntry?.size ?? 0) + (afterEntry?.size ?? 0);

    if (
      hydratedSourceBytes >= MAX_HYDRATED_SOURCE_BYTES ||
      (sourceSizesKnown && hydratedSourceBytes + estimatedSourceBytes > MAX_HYDRATED_SOURCE_BYTES)
    ) {
      exclude(unreviewedPaths, file, basePath, "source-limit");
      continue;
    }

    const contents = yield* Effect.all(
      {
        before:
          beforeEntry === undefined
            ? Effect.succeed("")
            : input.base
                .readTextFile(basePath)
                .pipe(Effect.catchTag("BinaryBlob", () => Effect.succeed(undefined))),
        after:
          afterEntry === undefined
            ? Effect.succeed("")
            : input.head
                .readTextFile(file.path)
                .pipe(Effect.catchTag("BinaryBlob", () => Effect.succeed(undefined))),
      },
      { concurrency: 2 },
    ).pipe(Effect.result);

    if (Result.isFailure(contents)) {
      hydratedSourceBytes += estimatedSourceBytes;
      exclude(unreviewedPaths, file, basePath, "source-read-failed");
      continue;
    }
    // Both reads have settled, so a binary side cannot hide a real read failure.
    // Preserve any textual side as an addition/deletion instead of dropping it.
    const beforeBinary = contents.success.before === undefined;
    const afterBinary = contents.success.after === undefined;

    if (
      (beforeBinary || afterBinary) &&
      (beforeBinary || beforeEntry === undefined) &&
      (afterBinary || afterEntry === undefined)
    ) {
      hydratedSourceBytes += estimatedSourceBytes;
      exclude(ignoredPaths, file, basePath);
      continue;
    }
    const before = contents.success.before ?? "";
    const after = contents.success.after ?? "";
    const path = afterBinary ? basePath : file.path;

    if (basePath !== file.path) {
      if (beforeBinary) unavailablePaths.add(basePath);
      if (afterBinary) unavailablePaths.add(file.path);
    }
    hydratedSourceBytes += sourceSizesKnown ? estimatedSourceBytes : before.length + after.length;
    if (hydratedSourceBytes > MAX_HYDRATED_SOURCE_BYTES) {
      exclude(unreviewedPaths, file, basePath, "source-limit");
      continue;
    }

    const patch =
      basePath === file.path &&
      !beforeBinary &&
      !afterBinary &&
      before === after &&
      beforeEntry !== undefined &&
      afterEntry !== undefined &&
      beforeEntry.mode !== afterEntry.mode
        ? [
            `diff --git a/${file.path} b/${file.path}`,
            `old mode ${beforeEntry.mode}`,
            `new mode ${afterEntry.mode}`,
          ].join("\n")
        : makeExactPatch({
            path,
            basePath: beforeBinary || afterBinary ? path : basePath,
            headPath: path,
            baseRevision: input.base.revision,
            headRevision: input.head.revision,
            before,
            after,
          });

    const exactPatch =
      patch !== undefined && basePath !== file.path && !beforeBinary && !afterBinary
        ? [
            `diff --git a/${basePath} b/${file.path}`,
            `rename from ${basePath}`,
            `rename to ${file.path}`,
            patch,
          ].join("\n")
        : patch;

    if (
      path.length > 512 ||
      exactPatch === undefined ||
      exactPatch.length === 0 ||
      exactPatch.length > MAX_REVIEW_PATCH_CHARS
    ) {
      exclude(
        unreviewedPaths,
        file,
        basePath,
        path.length > 512
          ? "path-limit"
          : exactPatch !== undefined && exactPatch.length > MAX_REVIEW_PATCH_CHARS
            ? "patch-limit"
            : "patch-unavailable",
      );
      continue;
    }
    changes.push(ReviewChange.make({ path, patch: exactPatch }));
  }

  return { changes, unreviewedPaths, ignoredPaths, unavailablePaths, exclusions };
});

const reviewContextFailure = (message: string): ReviewContextError =>
  ReviewContextError.make({ message });

type ReviewReadFileInput = Parameters<ReviewRepository["Service"]["readFile"]>[0];
type ReviewFindFilesInput = Parameters<ReviewRepository["Service"]["findFiles"]>[0];

/** Bind model context reads to the exact verified base and head trees. */
export const makeReviewRepository = (input: {
  readonly base: RepositorySnapshot;
  readonly head: RepositorySnapshot;
  readonly ignore: ReadonlyArray<string>;
  readonly unavailablePaths: ReadonlySet<string>;
}): ReviewRepository["Service"] => {
  const snapshot = (revision: "base" | "head") => (revision === "base" ? input.base : input.head);

  const outsideScope = (path: string) =>
    isBinaryAssetPath(path) ||
    input.unavailablePaths.has(path) ||
    input.ignore.some((pattern) => matchesIgnore(path, pattern));

  const isReadableEntry = (entry: ReturnType<RepositorySnapshot["entry"]>) =>
    entry?.type === "blob" && entry.mode !== "120000";

  const readFile = Effect.fn("ReviewRepository.readFile")(function* (request: ReviewReadFileInput) {
    if (outsideScope(request.path)) {
      return yield* reviewContextFailure(
        "The requested path is outside this review's source scope.",
      );
    }
    const selected = snapshot(request.revision);

    if (!isReadableEntry(selected.entry(request.path))) {
      return yield* reviewContextFailure(
        "Text source is unavailable for the requested path and revision.",
      );
    }

    const content = yield* selected
      .readTextFile(request.path)
      .pipe(
        Effect.mapError(() =>
          reviewContextFailure(
            "Text source could not be read for the requested path and revision.",
          ),
        ),
      );

    return yield* ReviewSource.fromText(request, content);
  });

  const findFiles = (request: ReviewFindFilesInput) => {
    const selected = snapshot(request.revision);

    const matches = selected.paths
      .filter(
        (path) =>
          path.length <= 512 &&
          !outsideScope(path) &&
          isReadableEntry(selected.entry(path)) &&
          path.includes(request.query),
      )
      .sort();

    return Effect.succeed(
      ReviewFileList.make({ paths: matches.slice(0, 100), truncated: matches.length > 100 }),
    );
  };

  return ReviewRepository.of({ readFile, findFiles });
};

export const reviewEventFor = (blockingFindings: number): "COMMENT" | "REQUEST_CHANGES" =>
  blockingFindings > 0 ? "REQUEST_CHANGES" : "COMMENT";

const reanchorToFullPullRequest = (
  files: ReadonlyArray<ChangedFile>,
  report: ReviewReport,
): ReviewReport => {
  const patches = new Map(files.map((file) => [file.path, file.patch] as const));

  return ReviewReport.make({
    summary: report.summary,
    findings: report.findings.flatMap((finding) => {
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
          category: finding.category,
          title: finding.title,
          body: finding.body,
        }),
      ];
    }),
  });
};

export const reviewActionProgram = Effect.gen(function* () {
  const presentation = yield* ReviewPresentation;
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

  const configuredMode = yield* Config.literals(
    ["auto", "incremental", "full"],
    "PR_REVIEW_MODE",
  ).pipe(Config.withDefault("auto"));

  const command = yield* Config.string("PR_REVIEW_COMMAND").pipe(Config.withDefault(""));
  const mode = command.trim().length === 0 ? configuredMode : reviewModeFromCommand(command);

  if (mode === undefined) {
    return yield* skip("unsupported-review-command");
  }

  const commentId = yield* Config.schema(Schema.Natural, "PR_REVIEW_COMMENT_ID").pipe(
    Config.withDefault(0),
  );

  if (command.trim().length > 0 && commentId === 0) {
    return yield* ActionConfigurationError.make({
      message: "comment-id is required for a manual review command",
    });
  }

  const automaticReviewLimit = yield* Config.schema(
    Schema.Natural,
    "PR_REVIEW_AUTOMATIC_LIMIT",
  ).pipe(Config.withDefault(2));

  const expectedHead = yield* Config.string("PR_REVIEW_EXPECTED_HEAD").pipe(Config.withDefault(""));

  const modelName = yield* Config.nonEmptyString("PR_REVIEW_MODEL").pipe(
    Config.withDefault("gpt-5.6-sol"),
  );

  const effort = yield* Config.literals(
    ["low", "medium", "high", "xhigh"],
    "PR_REVIEW_EFFORT",
  ).pipe(Config.withDefault("xhigh"));

  const guidanceFile = yield* Config.string("PR_REVIEW_GUIDANCE_FILE").pipe(Config.withDefault(""));

  const ignore = (yield* Config.string("PR_REVIEW_IGNORE").pipe(Config.withDefault("")))
    .split(",")
    .map((pattern) => pattern.trim())
    .filter((pattern) => pattern.length > 0);

  const apiUrl = yield* Config.nonEmptyString("GITHUB_API_URL").pipe(
    Config.withDefault("https://api.github.com"),
  );

  const graphqlUrl = yield* Config.nonEmptyString("GITHUB_GRAPHQL_URL").pipe(Config.option);

  const github = yield* makeGitHubClient({
    repository,
    pullRequest: pullRequestNumber,
    token,
    apiUrl,
    graphqlUrl: Option.getOrUndefined(graphqlUrl),
  });

  if (commentId > 0) yield* github.acknowledgeComment(commentId);
  const pull = yield* github.getPullRequest;

  if (pull.draft) return yield* skip("draft-pull-request");
  if (expectedHead.length > 0 && pull.headRevision !== expectedHead) {
    return yield* skip("stale-event-head");
  }

  const history = yield* github.listReviews;
  let unresolvedChangeRequests = unresolvedChangeRequestCount({ reviewAuthor, history });

  const selection = selectReview({
    mode,
    currentHead: pull.headRevision,
    reviewAuthor,
    automaticReviewLimit,
    history,
  });

  if (selection._tag === "skip") {
    yield* skip(selection.reason, undefined, unresolvedChangeRequests);
    if (selection.reason === "head-review-incomplete") {
      return yield* ReviewAttemptIncomplete.make({});
    }
    if (unresolvedChangeRequests > 0) {
      return yield* UnresolvedChangeRequests.make({ count: unresolvedChangeRequests });
    }

    return;
  }
  if (selection._tag === "pause") {
    const reviewUrl = yield* github.publishReview({
      commitId: pull.headRevision,
      event: "COMMENT",
      body: withReviewPauseMarker(
        presentation.renderPause({
          automaticReviewLimit: selection.automaticReviewLimit,
          automaticAttempts: selection.automaticAttempts,
          lastCompletedRevision: selection.lastCompletedRevision,
          headRevision: pull.headRevision,
          unresolvedChangeRequests,
        }),
        selection.automaticReviewLimit,
      ),
      comments: [],
    });

    yield* skip(selection.reason, reviewUrl, unresolvedChangeRequests);
    if (unresolvedChangeRequests > 0) {
      return yield* UnresolvedChangeRequests.make({ count: unresolvedChangeRequests });
    }

    return;
  }

  let scope = selection.scope;

  const attemptExit = yield* Effect.gen(function* () {
    const fullFiles = yield* github.listFiles;
    const currentMergeBase = yield* github.getMergeBase(pull.baseRevision, pull.headRevision);

    let reviewBase =
      scope === "incremental" && selection.baseRevision !== undefined
        ? selection.baseRevision
        : currentMergeBase;

    if (scope === "incremental") {
      const priorMergeBase = yield* github.getMergeBase(pull.baseRevision, reviewBase);

      if (priorMergeBase !== currentMergeBase) {
        if (!selection.automatic) {
          return yield* IncrementalScopeUnavailable.make({
            priorMergeBase,
            currentMergeBase,
          });
        }
        scope = "full";
        reviewBase = currentMergeBase;
        yield* Effect.logInfo("Reviewing the full diff after the merge base changed");
      }
    }
    const comparison = yield* github.compareTrees(reviewBase, pull.headRevision);

    // A completed incremental baseline is still PR-controlled. Only the merge
    // base with the target branch may authorize automatic generated exclusions.
    const generatedAt = yield* Effect.cached(
      reviewBase === currentMergeBase
        ? Effect.succeed(comparison.base)
        : github.readTreeSnapshot(currentMergeBase),
    );

    const surface = yield* hydrateExactChanges({
      files: fullFiles,
      changedPaths: comparison.changedPaths,
      base: comparison.base,
      head: comparison.head,
      ignore,
    }).pipe(
      Effect.provide(
        Layer.succeed(GeneratedFileClassification, {
          isGenerated: (path) =>
            generatedAt.pipe(Effect.flatMap((snapshot) => github.isGenerated(snapshot, path))),
        }),
      ),
    );

    const reviewRepository = makeReviewRepository({
      base: comparison.base,
      head: comparison.head,
      ignore,
      unavailablePaths: surface.unavailablePaths,
    });

    const fs = yield* FileSystem.FileSystem;

    const guidance =
      guidanceFile.length === 0
        ? undefined
        : (yield* fs.readFileString(guidanceFile)).slice(0, 20_000);

    if (surface.changes.length === 0) {
      const resolutions: ReadonlyArray<ReviewResolution> = [];

      return {
        resolutions,
        followUps: [],
        surface,
        modelTurns: 0,
        exhausted: undefined,
        incomplete: false,
        inputTokens: 0,
        uncachedInputTokens: 0,
        cachedInputTokens: 0,
        cacheWriteInputTokens: 0,
        outputTokens: 0,
        estimatedCostMicrousd: undefined,
        reservedCostMicrousd: 0,
        report: ReviewReport.make({
          summary:
            scope === "incremental" &&
            surface.ignoredPaths.length === 0 &&
            surface.unreviewedPaths.length === 0
              ? "No pull-request files changed since the last completed review."
              : surface.ignoredPaths.length > 0 && surface.unreviewedPaths.length === 0
                ? "No changed files matched the configured review scope."
                : "No textual patch fit within the review input bound.",
          findings: [],
        }),
      };
    }

    const followUps = yield* github.loadReviewFollowUps({
      reviewAuthor,
      history,
      scope,
      changedPaths: new Set(surface.changes.map(({ path }) => path)),
    });

    const provider = yield* makeReviewOpenAi({
      client: yield* OpenAiClient.make({ apiKey: yield* Config.redacted("OPENAI_API_KEY") }),
      model: modelName,
      cacheKey: `pr-review-v2:${pull.headRevision}`,
    });

    const reviewer = makeReviewer({
      model: OpenAiLanguageModel.model(modelName, {
        max_output_tokens: 32_000,
        store: false,
        service_tier: "default",
        strictJsonSchema: true,
        reasoning: { effort },
      }),
      estimateCostMicrousd: reviewCostEstimator(modelName),
      costControl: provider.costControl,
      ...(guidance === undefined ? {} : { guidance }),
    });

    const result = yield* reviewer
      .review(
        ReviewRequest.make({
          title: pull.title.slice(0, 1_000),
          description: pull.description.slice(0, 20_000),
          baseRevision: reviewBase,
          headRevision: pull.headRevision,
          scope,
          changes: surface.changes,
          unreviewedPaths: surface.unreviewedPaths
            .filter((path) => path.length <= 512)
            .slice(0, 300),
          followUps,
        }),
      )
      .pipe(
        Effect.provideService(ReviewRepository, reviewRepository),
        Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
        Effect.onExit(() =>
          provider.costControl.snapshot.pipe(
            Effect.flatMap((snapshot) =>
              Effect.logInfo("Review accounting totals", {
                modelCalls: snapshot.modelCalls,
                costLimited: snapshot.stopped,
                inputLimited: snapshot.inputLimitExceeded === true,
                ...snapshot.usage,
                costLimitMicrousd: REVIEW_COST_LIMIT_MICROUSD,
              }),
            ),
          ),
        ),
      );

    const pending = new Set(result.pendingPaths ?? []);

    surface.unreviewedPaths.push(...pending);
    surface.exclusions.push(
      ...[...pending].map((path) => ReviewExclusion.make({ path, reason: "review-stopped" })),
    );

    return {
      resolutions: result.resolutions ?? [],
      followUps,
      surface: {
        ...surface,
        changes: surface.changes.filter((change) => !pending.has(change.path)),
      },
      modelTurns: result.turns,
      exhausted: result.exhausted,
      incomplete: result.incomplete === true,
      inputTokens: result.usage.inputTokens,
      uncachedInputTokens: result.usage.uncachedInputTokens,
      cachedInputTokens: result.usage.cachedInputTokens,
      cacheWriteInputTokens: result.usage.cacheWriteInputTokens,
      outputTokens: result.usage.outputTokens,
      estimatedCostMicrousd: result.usage.estimatedCostMicrousd,
      reservedCostMicrousd: result.usage.reservedCostMicrousd ?? 0,
      report: reanchorToFullPullRequest(fullFiles, result.report),
    };
  }).pipe(Effect.exit);

  if (Exit.isFailure(attemptExit)) {
    const failureSummary = attemptExit.cause.reasons
      .flatMap((reason) => {
        if (!Cause.isFailReason(reason)) return [];
        const failure = reason.error;

        switch (failure._tag) {
          case "BudgetExceeded":
            return [
              `Review budget exceeded (${failure.limit}): observed ${String(failure.observedValue)}, limit ${String(failure.limitValue)}.`,
            ];
          case "IncrementalScopeUnavailable":
            return [
              "The merge base changed. Request a full review before incremental reviews can resume.",
            ];
          case "GitHubApiFailure":
            return ["A GitHub repository request failed."];
          default:
            return [];
        }
      })
      .at(0);

    yield* Console.error(
      `PR review attempt failed${failureSummary === undefined ? "" : `: ${failureSummary}`}`,
    );
    yield* Effect.logError("Review failure", {
      failureTypes: attemptExit.cause.reasons.flatMap((reason) =>
        Cause.isFailReason(reason) ? [reason.error._tag] : [reason._tag],
      ),
    });

    const reviewUrl = yield* publishHeadBoundReview(
      github.publishReview({
        commitId: pull.headRevision,
        event: "COMMENT",
        body: withReviewMarker(
          presentation.renderFailure({
            automaticReviewsRemaining: selection.automaticReviewsRemaining,
            failureSummary,
          }),
          selection.automatic,
          false,
        ),
        comments: [],
      }),
      { publish: github.publishAttemptMarker, automatic: selection.automatic, failureSummary },
    ).pipe(Effect.catchTag("StaleReviewHead", () => Effect.failCause(attemptExit.cause)));

    yield* writeOutputs([
      ["skipped", "false"],
      ["reason", "review-failed"],
      ["blocking-findings", 0],
      ["unresolved-change-requests", unresolvedChangeRequests],
      ["review-url", reviewUrl],
    ]);

    return yield* Effect.failCause(attemptExit.cause);
  }

  const {
    surface,
    modelTurns,
    inputTokens,
    uncachedInputTokens,
    cachedInputTokens,
    cacheWriteInputTokens,
    outputTokens,
    estimatedCostMicrousd,
    reservedCostMicrousd,
    report,
    exhausted,
    incomplete,
    resolutions,
    followUps,
  } = attemptExit.value;

  const complete = surface.unreviewedPaths.length === 0 && exhausted === undefined && !incomplete;

  const pricing = reviewModelPricing(modelName);

  const estimatedCost: ReviewCostEstimate | undefined =
    estimatedCostMicrousd === undefined || pricing === undefined
      ? undefined
      : {
          microusd: estimatedCostMicrousd,
          label: pricing.label,
          url: pricing.url,
        };

  const blocking = report.findings.filter((finding) => finding.severity === "blocking").length;

  // Only positive verification from a complete, nonblocking pass can retire prior feedback.
  // Dismissals retain the inspected commit and evidence even if later publication fails.
  if (complete && blocking === 0 && resolutions.length > 0) {
    const owned = selectUnresolvedChangeRequests({ reviewAuthor, history });

    yield* publishHeadBoundReview(
      Effect.gen(function* () {
        for (const resolution of resolutions) {
          const review = owned.find(({ id }) => String(id) === resolution.id);
          const followUp = followUps.find(({ id }) => id === resolution.id);

          if (review === undefined || followUp === undefined) {
            return yield* GitHubApiFailure.make({
              operation: "dismiss review",
              reason: "Resolution identified an unowned review",
            });
          }
          yield* github.dismissReview({
            review,
            followUp,
            reviewAuthor,
            commitId: pull.headRevision,
            evidence: resolution.evidence,
          });
          yield* Effect.logInfo("Dismissed addressed review", {
            reviewId: review.id,
            headRevision: pull.headRevision,
          });
        }
        unresolvedChangeRequests = unresolvedChangeRequestCount({
          reviewAuthor,
          history: yield* github.listReviews,
        });

        return "";
      }).pipe(
        Effect.tapErrorTag("GitHubApiFailure", () =>
          github.publishAttemptMarker({
            commitId: pull.headRevision,
            body: withReviewMarker(
              presentation.renderFailure({
                automaticReviewsRemaining: selection.automaticReviewsRemaining,
                failureSummary:
                  "GitHub could not confirm dismissal of the verified reviews. Check the review timeline and request a full review to retry.",
              }),
              selection.automatic,
              false,
            ),
          }),
        ),
      ),
      { publish: github.publishAttemptMarker, automatic: selection.automatic },
    );
  }

  const body = withReviewMarker(
    presentation.renderReview({
      report,
      automaticReviewsRemaining: selection.automaticReviewsRemaining,
      scope,
      reviewedFiles: surface.changes.length,
      unreviewedFiles: surface.unreviewedPaths.length,
      exclusions: surface.exclusions,
      ignoredFiles: surface.ignoredPaths.length,
      modelTurns,
      complete,
      exhausted,
      unresolvedChangeRequests,
      inputTokens,
      uncachedInputTokens,
      cachedInputTokens,
      cacheWriteInputTokens,
      outputTokens,
      estimatedCost,
      reservedCostMicrousd,
      costLimitMicrousd: REVIEW_COST_LIMIT_MICROUSD,
      headRevision: pull.headRevision,
    }),
    selection.automatic,
    complete,
  );

  const reviewUrl = yield* publishHeadBoundReview(
    github.publishReview({
      commitId: pull.headRevision,
      event: reviewEventFor(blocking),
      body,
      comments: report.findings.flatMap((finding) =>
        finding.line === undefined
          ? []
          : [
              {
                path: finding.path,
                line: finding.line,
                body: presentation.renderFinding(finding, pull.headRevision),
              },
            ],
      ),
    }),
    { publish: github.publishAttemptMarker, automatic: selection.automatic },
  );

  yield* writeOutputs([
    ["skipped", "false"],
    [
      "reason",
      scope === selection.scope
        ? selection.reason
        : "automatic full review after merge-base change",
    ],
    ["input-tokens", inputTokens],
    ["uncached-input-tokens", uncachedInputTokens],
    ["cached-input-tokens", cachedInputTokens],
    ["cache-write-input-tokens", cacheWriteInputTokens],
    ["output-tokens", outputTokens],
    ["reserved-cost-usd", (reservedCostMicrousd / 1_000_000).toFixed(6)],
    ["cost-limit-usd", (REVIEW_COST_LIMIT_MICROUSD / 1_000_000).toFixed(6)],
    [
      "estimated-cost-usd",
      estimatedCost === undefined ? "" : (estimatedCost.microusd / 1_000_000).toFixed(6),
    ],
    ["blocking-findings", blocking],
    ["unresolved-change-requests", unresolvedChangeRequests],
    ["review-url", reviewUrl],
  ]);
  yield* Console.log(`Posted PR review: ${reviewUrl}`);
  for (const exclusion of surface.exclusions) {
    yield* Effect.logInfo("Review input excluded", {
      path: exclusion.path,
      reason: exclusion.reason,
    });
  }

  const publicationFailure = reviewPublicationFailure({
    blockingFindings: blocking,
    unreviewedPaths: surface.unreviewedPaths.length,
    unresolvedChangeRequests,
    exhausted,
    incomplete,
  });

  if (publicationFailure !== undefined) return yield* publicationFailure;
});

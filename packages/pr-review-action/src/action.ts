import {
  isCommentableLine,
  makeReviewer,
  ReviewChange,
  ReviewContextError,
  ReviewFileList,
  ReviewFinding,
  ReviewReport,
  ReviewRepository,
  ReviewRequest,
  ReviewSource,
  type RunCostEstimator,
} from "@effect-agent/pr-review";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import {
  Cause,
  Config,
  ConfigProvider,
  Console,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Schema,
} from "effect";
import type { Response } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import {
  type ChangedFile,
  type GitHubApiFailure,
  makeExactPatch,
  makeGitHubClient,
  type RepositorySnapshot,
  type StaleReviewHead,
} from "./github.ts";
import {
  type ReviewCostEstimate,
  ReviewPresentation,
  withReviewMarker,
  withReviewPauseMarker,
} from "./presentation.ts";
import { reviewModeFromCommand, selectReview, unresolvedChangeRequestCount } from "./selection.ts";

const MAX_REVIEW_PATCH_CHARS = 120_000;
const MAX_PATCH_CHARS = 80_000;
const MAX_REVIEW_FILES = 100;
const MAX_HYDRATED_SOURCE_BYTES = 4_000_000;

interface Gpt56Pricing {
  readonly label: string;
  readonly url: string;
  readonly inputRateHundredths: number;
  readonly cachedInputRateHundredths: number;
  readonly cacheWriteRateHundredths: number;
  readonly outputRateHundredths: number;
}

const GPT_56_SOL_PRICING: Gpt56Pricing = {
  label: "GPT-5.6 Sol",
  url: "https://developers.openai.com/api/docs/models/gpt-5.6-sol",
  inputRateHundredths: 400,
  cachedInputRateHundredths: 40,
  cacheWriteRateHundredths: 500,
  outputRateHundredths: 2_000,
};

const GPT_56_PRICING: Readonly<Record<string, Gpt56Pricing>> = {
  "gpt-5.6": GPT_56_SOL_PRICING,
  "gpt-5.6-sol": GPT_56_SOL_PRICING,
  "gpt-5.6-terra": {
    label: "GPT-5.6 Terra",
    url: "https://developers.openai.com/api/docs/models/gpt-5.6-terra",
    inputRateHundredths: 200,
    cachedInputRateHundredths: 20,
    cacheWriteRateHundredths: 250,
    outputRateHundredths: 1_200,
  },
  "gpt-5.6-luna": {
    label: "GPT-5.6 Luna",
    url: "https://developers.openai.com/api/docs/models/gpt-5.6-luna",
    inputRateHundredths: 20,
    cachedInputRateHundredths: 2,
    cacheWriteRateHundredths: 25,
    outputRateHundredths: 120,
  },
};

const GPT_56_PRICING_VERSION = "openai-api-2026-08-25";

/** Estimate standard-tier GPT-5.6 text cost, rounded up to the nearest microdollar. */
export const estimateGpt56CostMicrousd = (
  model: string,
  usage: Response.Usage,
): number | undefined => {
  const pricing = GPT_56_PRICING[model];
  if (pricing === undefined) return undefined;
  const cacheRead = usage.inputTokens.cacheRead ?? 0;
  const inputTotal = usage.inputTokens.total ?? (usage.inputTokens.uncached ?? 0) + cacheRead;
  const uncached = Math.max(0, inputTotal - cacheRead);
  const cacheWrite = Math.min(uncached, usage.inputTokens.cacheWrite ?? 0);
  const standardInput = uncached - cacheWrite;
  const output = usage.outputTokens.total ?? 0;
  const costHundredths =
    standardInput * pricing.inputRateHundredths +
    cacheRead * pricing.cachedInputRateHundredths +
    cacheWrite * pricing.cacheWriteRateHundredths +
    output * pricing.outputRateHundredths;
  return Math.ceil(costHundredths / 100);
};

const gpt56CostEstimator = (model: string): RunCostEstimator | undefined => {
  const pricing = GPT_56_PRICING[model];
  if (pricing === undefined) return undefined;
  return (usage) =>
    Effect.succeed({
      costMicrousd: estimateGpt56CostMicrousd(model, usage) ?? 0,
      pricingVersion: GPT_56_PRICING_VERSION,
    });
};

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

type ReviewPublication =
  | { readonly _tag: "published"; readonly reviewUrl: string }
  | {
      readonly _tag: "stale";
      readonly reviewUrl: string;
      readonly failure: StaleReviewHead;
    };

const staleAttemptBody = (automatic: boolean): string =>
  withReviewMarker(
    [
      "## Effect Agent review",
      "",
      "> [!CAUTION]",
      "> **The review result was discarded because the pull request changed during publication.**",
      "> This attempt is incomplete and does not clear the change.",
    ].join("\n"),
    automatic,
    false,
  );

/** Recover only a known pre-POST head mismatch with a neutral marker on GitHub's current head. */
export const publishHeadBoundReview = Effect.fn("publishHeadBoundReview")(function* (input: {
  readonly publish: Effect.Effect<string, GitHubApiFailure | StaleReviewHead>;
  readonly publishCurrentHeadAttemptMarker: (
    body: string,
  ) => Effect.Effect<string, GitHubApiFailure>;
  readonly automatic: boolean;
}): Effect.fn.Return<ReviewPublication, GitHubApiFailure> {
  return yield* input.publish.pipe(
    Effect.map((reviewUrl) => ({ _tag: "published", reviewUrl }) as const),
    Effect.catchTag("StaleReviewHead", (failure) =>
      input
        .publishCurrentHeadAttemptMarker(staleAttemptBody(input.automatic))
        .pipe(Effect.map((reviewUrl) => ({ _tag: "stale", reviewUrl, failure }) as const)),
    ),
  );
});

export const reviewPublicationFailure = (input: {
  readonly blockingFindings: number;
  readonly unreviewedPaths: number;
  readonly unresolvedChangeRequests: number;
}): BlockingFindings | IncompleteReview | UnresolvedChangeRequests | undefined => {
  if (input.blockingFindings > 0) return BlockingFindings.make({ count: input.blockingFindings });
  if (input.unreviewedPaths > 0) {
    return IncompleteReview.make({ unreviewedPaths: input.unreviewedPaths });
  }
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

interface ReviewSurface {
  readonly changes: ReadonlyArray<ReviewChange>;
  readonly unreviewedPaths: ReadonlyArray<string>;
  readonly ignoredPaths: ReadonlyArray<string>;
}

export const reviewUnavailablePaths = (
  files: ReadonlyArray<ChangedFile>,
  surface: ReviewSurface,
): ReadonlySet<string> => {
  const paths = new Set([...surface.unreviewedPaths, ...surface.ignoredPaths]);
  for (const file of files) {
    if (file.previousPath !== undefined && paths.has(file.path)) paths.add(file.previousPath);
  }
  return paths;
};

/** Include both sides of provider-declared renames in exact tree comparison. */
export const reviewCandidatePaths = (
  files: ReadonlyArray<ChangedFile>,
  additionalPaths: ReadonlyArray<string> = [],
): ReadonlyArray<string> =>
  [
    ...new Set([
      ...files.flatMap((file) =>
        file.previousPath === undefined ? [file.path] : [file.previousPath, file.path],
      ),
      ...additionalPaths,
    ]),
  ].sort();

/** Admit sorted metadata before hydrating exact baseline-to-head patches. */
export const hydrateExactChanges = Effect.fn("hydrateExactChanges")(function* (input: {
  readonly files: ReadonlyArray<ChangedFile>;
  readonly changedPaths: ReadonlyArray<string>;
  readonly base: RepositorySnapshot;
  readonly head: RepositorySnapshot;
  readonly ignore: ReadonlyArray<string>;
}) {
  const metadata = new Map(input.files.map((file) => [file.path, file] as const));
  const activeRenames = new Map(
    input.files.flatMap((file) => {
      const previousPath = file.previousPath;
      return file.status === "renamed" &&
        previousPath !== undefined &&
        input.base.entry(previousPath) !== undefined &&
        input.base.entry(file.path) === undefined &&
        input.head.entry(previousPath) === undefined &&
        input.head.entry(file.path) !== undefined
        ? [[previousPath, file] as const]
        : [];
    }),
  );
  const activeRenamesByCurrentPath = new Map(
    [...activeRenames.values()].map((file) => [file.path, file] as const),
  );
  const candidates = new Map<string, { readonly file: ChangedFile; readonly basePath: string }>();
  for (const changedPath of [...new Set(input.changedPaths)].sort()) {
    const renamed = activeRenames.get(changedPath) ?? activeRenamesByCurrentPath.get(changedPath);
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
  let admittedPaths = 0;
  let patchChars = 0;
  let patchBudgetExhausted = false;
  let hydratedSourceBytes = 0;
  let sourceBudgetExhausted = false;
  for (const { file, basePath } of [...candidates.values()].sort((left, right) =>
    left.file.path < right.file.path ? -1 : left.file.path > right.file.path ? 1 : 0,
  )) {
    const ignored = [file.path, ...(basePath === file.path ? [] : [basePath])].some((path) =>
      input.ignore.some((pattern) => matchesIgnore(path, pattern)),
    );
    if (ignored) {
      ignoredPaths.push(file.path);
      continue;
    }
    if (
      file.path.length > 512 ||
      admittedPaths >= MAX_REVIEW_FILES ||
      patchBudgetExhausted ||
      sourceBudgetExhausted
    ) {
      unreviewedPaths.push(file.path);
      continue;
    }
    admittedPaths += 1;
    const beforeEntry = input.base.entry(basePath);
    const afterEntry = input.head.entry(file.path);
    if (
      (beforeEntry !== undefined && beforeEntry.type !== "blob") ||
      (afterEntry !== undefined && afterEntry.type !== "blob")
    ) {
      unreviewedPaths.push(file.path);
      continue;
    }
    const sourceSizesKnown =
      (beforeEntry === undefined || beforeEntry.size !== undefined) &&
      (afterEntry === undefined || afterEntry.size !== undefined);
    const estimatedSourceBytes = (beforeEntry?.size ?? 0) + (afterEntry?.size ?? 0);
    if (
      sourceSizesKnown &&
      hydratedSourceBytes + estimatedSourceBytes > MAX_HYDRATED_SOURCE_BYTES
    ) {
      unreviewedPaths.push(file.path);
      sourceBudgetExhausted = true;
      continue;
    }
    const contents = yield* Effect.all(
      {
        before: beforeEntry === undefined ? Effect.succeed("") : input.base.readTextFile(basePath),
        after: afterEntry === undefined ? Effect.succeed("") : input.head.readTextFile(file.path),
      },
      { concurrency: 2 },
    ).pipe(Effect.exit);
    if (Exit.isFailure(contents)) {
      hydratedSourceBytes += estimatedSourceBytes;
      unreviewedPaths.push(file.path);
      continue;
    }
    hydratedSourceBytes += sourceSizesKnown
      ? estimatedSourceBytes
      : contents.value.before.length + contents.value.after.length;
    if (hydratedSourceBytes > MAX_HYDRATED_SOURCE_BYTES) {
      unreviewedPaths.push(file.path);
      sourceBudgetExhausted = true;
      continue;
    }
    const patch =
      basePath === file.path &&
      contents.value.before === contents.value.after &&
      beforeEntry !== undefined &&
      afterEntry !== undefined &&
      beforeEntry.mode !== afterEntry.mode
        ? [
            `diff --git a/${file.path} b/${file.path}`,
            `old mode ${beforeEntry.mode}`,
            `new mode ${afterEntry.mode}`,
          ].join("\n")
        : makeExactPatch({
            path: file.path,
            basePath,
            headPath: file.path,
            baseRevision: input.base.revision,
            headRevision: input.head.revision,
            before: contents.value.before,
            after: contents.value.after,
          });
    const exactPatch =
      patch !== undefined && basePath !== file.path
        ? [
            `diff --git a/${basePath} b/${file.path}`,
            `rename from ${basePath}`,
            `rename to ${file.path}`,
            patch,
          ].join("\n")
        : patch;
    if (
      exactPatch === undefined ||
      exactPatch.length === 0 ||
      exactPatch.length > MAX_PATCH_CHARS
    ) {
      unreviewedPaths.push(file.path);
      continue;
    }
    if (patchChars + exactPatch.length > MAX_REVIEW_PATCH_CHARS) {
      unreviewedPaths.push(file.path);
      patchBudgetExhausted = true;
      continue;
    }
    changes.push(ReviewChange.make({ path: file.path, patch: exactPatch }));
    patchChars += exactPatch.length;
    if (patchChars === MAX_REVIEW_PATCH_CHARS) patchBudgetExhausted = true;
  }
  return { changes, unreviewedPaths, ignoredPaths } satisfies ReviewSurface;
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

const openAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

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

  const github = yield* makeGitHubClient({
    repository,
    pullRequest: pullRequestNumber,
    token,
    apiUrl,
  });
  if (commentId > 0) yield* github.acknowledgeComment(commentId);
  const pull = yield* github.getPullRequest;
  if (pull.draft) return yield* skip("draft-pull-request");
  if (expectedHead.length > 0 && pull.headRevision !== expectedHead) {
    return yield* skip("stale-event-head");
  }

  const history = yield* github.listReviews;
  const unresolvedChangeRequests = unresolvedChangeRequestCount({ reviewAuthor, history });
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

  const reviewUrlOrFailStale = Effect.fn("reviewUrlOrFailStale")(function* (
    publication: ReviewPublication,
  ) {
    if (publication._tag === "published") return publication.reviewUrl;
    yield* writeOutputs([
      ["skipped", "false"],
      ["reason", "stale-review-head"],
      ["blocking-findings", 0],
      ["unresolved-change-requests", unresolvedChangeRequests],
      ["review-url", publication.reviewUrl],
    ]);
    return yield* publication.failure;
  });

  const attemptExit = yield* Effect.gen(function* () {
    const fullFiles = yield* github.listFiles;
    const currentMergeBase = yield* github.getMergeBase(pull.baseRevision, pull.headRevision);
    const reviewBase =
      selection.scope === "incremental" && selection.baseRevision !== undefined
        ? selection.baseRevision
        : currentMergeBase;
    let candidatePaths = reviewCandidatePaths(fullFiles);
    if (selection.scope === "incremental") {
      const priorMergeBase = yield* github.getMergeBase(pull.baseRevision, reviewBase);
      if (priorMergeBase !== currentMergeBase) {
        return yield* IncrementalScopeUnavailable.make({
          priorMergeBase,
          currentMergeBase,
        });
      }
      const priorPullDelta = yield* github.compareTrees(priorMergeBase, reviewBase);
      candidatePaths = reviewCandidatePaths(fullFiles, priorPullDelta.changedPaths);
    }
    const comparison = yield* github.compareTrees(reviewBase, pull.headRevision, candidatePaths);
    const surface = yield* hydrateExactChanges({
      files: fullFiles,
      changedPaths: comparison.changedPaths,
      base: comparison.base,
      head: comparison.head,
      ignore,
    });
    const unavailablePaths = reviewUnavailablePaths(fullFiles, surface);
    const reviewRepository = makeReviewRepository({
      base: comparison.base,
      head: comparison.head,
      ignore,
      unavailablePaths,
    });
    const fs = yield* FileSystem.FileSystem;
    const guidance =
      guidanceFile.length === 0
        ? undefined
        : (yield* fs.readFileString(guidanceFile)).slice(0, 20_000);

    if (surface.changes.length === 0) {
      return {
        surface,
        modelTurns: 0,
        inputTokens: 0,
        uncachedInputTokens: 0,
        cachedInputTokens: 0,
        cacheWriteInputTokens: 0,
        outputTokens: 0,
        estimatedCostMicrousd: undefined,
        report: ReviewReport.make({
          summary:
            selection.scope === "incremental" &&
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

    const estimateCostMicrousd = gpt56CostEstimator(modelName);
    const reviewer = makeReviewer({
      model: OpenAiLanguageModel.model(modelName, {
        max_output_tokens: 32_000,
        store: false,
        strictJsonSchema: true,
        reasoning: { effort },
      }),
      ...(estimateCostMicrousd === undefined ? {} : { estimateCostMicrousd }),
      ...(guidance === undefined ? {} : { guidance }),
    });
    const result = yield* reviewer
      .review(
        ReviewRequest.make({
          title: pull.title.slice(0, 1_000),
          description: pull.description.slice(0, 20_000),
          baseRevision: reviewBase,
          headRevision: pull.headRevision,
          scope: selection.scope,
          changes: surface.changes,
          unreviewedPaths: surface.unreviewedPaths
            .filter((path) => path.length <= 512)
            .slice(0, 300),
        }),
      )
      .pipe(
        Effect.provideService(ReviewRepository, reviewRepository),
        Effect.provide(openAiClientLayer),
      );
    return {
      surface,
      modelTurns: result.turns,
      inputTokens: result.usage.inputTokens,
      uncachedInputTokens: result.usage.uncachedInputTokens,
      cachedInputTokens: result.usage.cachedInputTokens,
      cacheWriteInputTokens: result.usage.cacheWriteInputTokens,
      outputTokens: result.usage.outputTokens,
      estimatedCostMicrousd: result.usage.estimatedCostMicrousd,
      report: reanchorToFullPullRequest(fullFiles, result.report),
    };
  }).pipe(Effect.exit);
  if (Exit.isFailure(attemptExit)) {
    yield* Console.error(`PR review attempt failed:\n${Cause.pretty(attemptExit.cause)}`);
    const publication = yield* publishHeadBoundReview({
      publish: github.publishReview({
        commitId: pull.headRevision,
        event: "COMMENT",
        body: withReviewMarker(
          presentation.renderFailure({
            automaticReviewsRemaining: selection.automaticReviewsRemaining,
          }),
          selection.automatic,
          false,
        ),
        comments: [],
      }),
      publishCurrentHeadAttemptMarker: github.publishCurrentHeadAttemptMarker,
      automatic: selection.automatic,
    });
    const reviewUrl = yield* reviewUrlOrFailStale(publication).pipe(
      Effect.catchTag("StaleReviewHead", () => Effect.failCause(attemptExit.cause)),
    );
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
    report,
  } = attemptExit.value;
  const complete = surface.unreviewedPaths.length === 0;

  const pricing = GPT_56_PRICING[modelName];
  const estimatedCost: ReviewCostEstimate | undefined =
    estimatedCostMicrousd === undefined || pricing === undefined
      ? undefined
      : {
          microusd: estimatedCostMicrousd,
          label: pricing.label,
          url: pricing.url,
        };

  const blocking = report.findings.filter((finding) => finding.severity === "blocking").length;
  const body = withReviewMarker(
    presentation.renderReview({
      report,
      automaticReviewsRemaining: selection.automaticReviewsRemaining,
      scope: selection.scope,
      reviewedFiles: surface.changes.length,
      unreviewedFiles: surface.unreviewedPaths.length,
      ignoredFiles: surface.ignoredPaths.length,
      modelTurns,
      complete,
      unresolvedChangeRequests,
      inputTokens,
      uncachedInputTokens,
      cachedInputTokens,
      cacheWriteInputTokens,
      outputTokens,
      estimatedCost,
      headRevision: pull.headRevision,
    }),
    selection.automatic,
    complete,
  );
  const publication = yield* publishHeadBoundReview({
    publish: github.publishReview({
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
    publishCurrentHeadAttemptMarker: github.publishCurrentHeadAttemptMarker,
    automatic: selection.automatic,
  });
  const reviewUrl = yield* reviewUrlOrFailStale(publication);
  yield* writeOutputs([
    ["skipped", "false"],
    ["reason", selection.reason],
    ["input-tokens", inputTokens],
    ["uncached-input-tokens", uncachedInputTokens],
    ["cached-input-tokens", cachedInputTokens],
    ["cache-write-input-tokens", cacheWriteInputTokens],
    ["output-tokens", outputTokens],
    [
      "estimated-cost-usd",
      estimatedCost === undefined ? "" : (estimatedCost.microusd / 1_000_000).toFixed(6),
    ],
    ["blocking-findings", blocking],
    ["unresolved-change-requests", unresolvedChangeRequests],
    ["review-url", reviewUrl],
  ]);
  yield* Console.log(`Posted PR review: ${reviewUrl}`);
  const publicationFailure = reviewPublicationFailure({
    blockingFindings: blocking,
    unreviewedPaths: surface.unreviewedPaths.length,
    unresolvedChangeRequests,
  });
  if (publicationFailure !== undefined) return yield* publicationFailure;
});

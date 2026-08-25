import {
  isCommentableLine,
  makeReviewer,
  ReviewChange,
  ReviewFinding,
  ReviewReport,
  ReviewRequest,
  type ReviewOutcome,
  type ReviewSeverity,
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
import type { RunCostEstimator } from "effect-agent";
import type { Response } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import { type ChangedFile, makeGitHubClient } from "./github.ts";
import { type ReviewCostEstimate, ReviewPresentation, withReviewMarker } from "./presentation.ts";
import { selectReview } from "./selection.ts";

const MAX_REVIEW_PATCH_CHARS = 320_000;
const MAX_PATCH_CHARS = 80_000;
const MAX_REVIEW_FILES = 100;
const TARGET_SHARD_PATCH_CHARS = 80_000;
const MAX_MERGED_FINDINGS = 12;
export const MAX_REVIEW_SHARDS = 4;

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
    ["uncached-input-tokens", 0],
    ["cached-input-tokens", 0],
    ["cache-write-input-tokens", 0],
    ["output-tokens", 0],
    ["estimated-cost-usd", "0.000000"],
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

/** Partition one admitted diff into a small, deterministic, size-balanced wave. */
export const shardReviewChanges = (
  changes: ReadonlyArray<ReviewChange>,
): ReadonlyArray<ReadonlyArray<ReviewChange>> => {
  if (changes.length === 0) return [];
  const totalChars = changes.reduce((total, change) => total + change.patch.length, 0);
  const shardCount = Math.min(
    MAX_REVIEW_SHARDS,
    changes.length,
    Math.max(1, Math.ceil(totalChars / TARGET_SHARD_PATCH_CHARS)),
  );
  const shards: Array<{
    chars: number;
    changes: Array<{ readonly index: number; readonly change: ReviewChange }>;
  }> = Array.from({ length: shardCount }, () => ({
    chars: 0,
    changes: [],
  }));
  const largestFirst = changes
    .map((change, index) => ({ change, index }))
    .sort((left, right) => right.change.patch.length - left.change.patch.length);

  for (const item of largestFirst) {
    let target = shards[0];
    if (target === undefined) break;
    for (const shard of shards) {
      if (shard.chars < target.chars) target = shard;
    }
    target.changes.push(item);
    target.chars += item.change.patch.length;
  }

  return shards.map((shard) =>
    shard.changes.sort((left, right) => left.index - right.index).map(({ change }) => change),
  );
};

/** Run one bounded parallel wave. Each Effect remains a one-turn reviewer invocation. */
export const runReviewWave = Effect.fn("runReviewWave")(function* <A, E, R>(
  reviews: ReadonlyArray<Effect.Effect<A, E, R>>,
): Effect.fn.Return<ReadonlyArray<A>, E, R> {
  return yield* Effect.all(reviews, { concurrency: MAX_REVIEW_SHARDS });
});

export const mergeReviewOutcomes = (
  outcomes: ReadonlyArray<ReviewOutcome>,
): {
  readonly report: ReviewReport;
  readonly inputTokens: number;
  readonly uncachedInputTokens: number;
  readonly cachedInputTokens: number;
  readonly cacheWriteInputTokens: number;
  readonly outputTokens: number;
  readonly estimatedCostMicrousd: number | undefined;
} => {
  const severityOrder: Record<ReviewSeverity, number> = { blocking: 0, important: 1, nit: 2 };
  const findings = outcomes
    .flatMap((outcome) => outcome.report.findings)
    .sort((left, right) => severityOrder[left.severity] - severityOrder[right.severity])
    .slice(0, MAX_MERGED_FINDINGS);
  const summary =
    outcomes.length === 1
      ? (outcomes[0]?.report.summary ?? "Review completed without a summary.")
      : outcomes
          .map(
            (outcome, index) =>
              `Shard ${String(index + 1)}:\n${outcome.report.summary.slice(0, 1_400)}`,
          )
          .join("\n\n");
  return {
    report: ReviewReport.make({ summary, findings }),
    inputTokens: outcomes.reduce((total, outcome) => total + outcome.usage.inputTokens, 0),
    uncachedInputTokens: outcomes.reduce(
      (total, outcome) => total + outcome.usage.uncachedInputTokens,
      0,
    ),
    cachedInputTokens: outcomes.reduce(
      (total, outcome) => total + outcome.usage.cachedInputTokens,
      0,
    ),
    cacheWriteInputTokens: outcomes.reduce(
      (total, outcome) => total + outcome.usage.cacheWriteInputTokens,
      0,
    ),
    outputTokens: outcomes.reduce((total, outcome) => total + outcome.usage.outputTokens, 0),
    estimatedCostMicrousd: outcomes.every(
      (outcome) => outcome.usage.estimatedCostMicrousd !== undefined,
    )
      ? outcomes.reduce((total, outcome) => total + (outcome.usage.estimatedCostMicrousd ?? 0), 0)
      : undefined,
  };
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
  const mode = yield* Config.literals(["auto", "incremental", "full"], "PR_REVIEW_MODE").pipe(
    Config.withDefault("auto"),
  );
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
    automaticReviewLimit,
    history,
  });
  if (selection._tag === "skip") return yield* skip(selection.reason);

  const fullFiles = yield* github.listFiles;
  let scopedFiles: ReadonlyArray<ChangedFile> = fullFiles;
  let actualScope = selection.scope;
  let reviewBaseRevision = pull.baseRevision;
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
      reviewBaseRevision = selection.baseRevision;
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

  let shardCount = 0;
  let inputTokens = 0;
  let uncachedInputTokens = 0;
  let cachedInputTokens = 0;
  let cacheWriteInputTokens = 0;
  let outputTokens = 0;
  let estimatedCostMicrousd: number | undefined;
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
    const shards = shardReviewChanges(surface.changes);
    shardCount = shards.length;
    const estimateCostMicrousd = gpt56CostEstimator(modelName);
    const reviewer = makeReviewer({
      model: OpenAiLanguageModel.model(modelName, {
        max_output_tokens: 8_000,
        store: false,
        strictJsonSchema: true,
        reasoning: { effort },
      }),
      ...(estimateCostMicrousd === undefined ? {} : { estimateCostMicrousd }),
      ...(guidance === undefined ? {} : { guidance }),
    });
    const unreviewedPaths = surface.unreviewedPaths
      .filter((path) => path.length <= 512)
      .slice(0, 300);
    const reviewExit = yield* runReviewWave(
      shards.map((changes) =>
        reviewer.review(
          ReviewRequest.make({
            title: pull.title.slice(0, 1_000),
            description: pull.description.slice(0, 20_000),
            baseRevision: reviewBaseRevision,
            headRevision: pull.headRevision,
            changes,
            unreviewedPaths,
          }),
        ),
      ),
    ).pipe(Effect.provide(openAiClientLayer), Effect.exit);
    if (Exit.isFailure(reviewExit)) {
      yield* Console.error(`PR review wave failed:\n${Cause.pretty(reviewExit.cause)}`);
      const reviewUrl = yield* github.publishReview({
        commitId: pull.headRevision,
        body: withReviewMarker(
          presentation.renderFailure({
            automaticReviewsRemaining: selection.automaticReviewsRemaining,
          }),
          selection.automatic,
          false,
        ),
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
    const merged = mergeReviewOutcomes(reviewExit.value);
    inputTokens = merged.inputTokens;
    uncachedInputTokens = merged.uncachedInputTokens;
    cachedInputTokens = merged.cachedInputTokens;
    cacheWriteInputTokens = merged.cacheWriteInputTokens;
    outputTokens = merged.outputTokens;
    estimatedCostMicrousd = merged.estimatedCostMicrousd;
    report = reanchorToFullPullRequest(fullFiles, merged.report);
  }

  const pricing = GPT_56_PRICING[modelName];
  const estimatedCost: ReviewCostEstimate | undefined =
    estimatedCostMicrousd === undefined || pricing === undefined
      ? undefined
      : {
          microusd: estimatedCostMicrousd,
          label: pricing.label,
          url: pricing.url,
        };

  const body = withReviewMarker(
    presentation.renderReview({
      report,
      automatic: selection.automatic,
      automaticReviewsRemaining: selection.automaticReviewsRemaining,
      scope: actualScope,
      reviewedFiles: surface.changes.length,
      unreviewedFiles: surface.unreviewedPaths.length,
      ignoredFiles: surface.ignoredPaths.length,
      shards: shardCount,
      inputTokens,
      uncachedInputTokens,
      cachedInputTokens,
      cacheWriteInputTokens,
      outputTokens,
      estimatedCost,
      headRevision: pull.headRevision,
    }),
    selection.automatic,
  );
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
              body: presentation.renderFinding(finding, pull.headRevision),
            },
          ],
    ),
  });
  const blocking = report.findings.filter((finding) => finding.severity === "blocking").length;
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
    ["review-url", reviewUrl],
  ]);
  yield* Console.log(`Posted PR review: ${reviewUrl}`);
  if (blocking > 0) return yield* BlockingFindings.make({ count: blocking });
});

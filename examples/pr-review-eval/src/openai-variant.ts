import {
  makeReviewOpenAi,
  reviewCostLimitMicrousd,
  reviewMaxCostUsd,
  reviewModel,
  reviewReasoningEffort,
} from "@effect-agent/pr-review-action/review-openai";
import {
  makeReviewer,
  ReviewCompaction,
  ReviewContextTokenLimit,
  type ReviewRequest,
} from "@effect-agent/pr-review/Review";
import { type ReviewRepository } from "@effect-agent/pr-review/ReviewRepository";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Option, Schema } from "effect";
import { AiError } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import { EvalReviewerFailure, EvalVariantConfiguration, type EvalVariantId } from "./contracts.ts";
import { digestText } from "./corpus.ts";
import { type EvalVariant } from "./runner.ts";

const ReviewerErrorView = Schema.Struct({
  _tag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  message: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(4_096))),
});

const reviewerFailure = (error: unknown, estimatedCostMicrousd?: number): EvalReviewerFailure => {
  // Provider messages and Tool parameters may contain source or credentials.
  const diagnostic = AiError.isAiError(error)
    ? {
        errorTag: `AiError/${error.reason._tag}`,
        message: `AI failure; retryable=${String(error.reason.isRetryable)}`,
      }
    : Option.match(Schema.decodeUnknownOption(ReviewerErrorView)(error), {
        onNone: () => ({
          errorTag: "ReviewInvocationFailure",
          message: "Reviewer invocation failed without a bounded typed diagnostic",
        }),
        onSome: (view) => ({ errorTag: view._tag, message: view.message?.trim() || view._tag }),
      });

  return EvalReviewerFailure.make({
    ...diagnostic,
    ...(estimatedCostMicrousd === undefined ? {} : { estimatedCostMicrousd }),
  });
};

export const openAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export interface CurrentOpenAiVariantOptions {
  readonly id: EvalVariantId;
  readonly guidance?: string | undefined;
}

export const makeCurrentOpenAiVariant = Effect.fn("PrReviewEval.makeCurrentOpenAiVariant")(
  function* (options: CurrentOpenAiVariantOptions) {
    const maxCostUsd = yield* reviewMaxCostUsd;
    const model = yield* reviewModel;
    const reasoningEffort = yield* reviewReasoningEffort;

    const compaction = yield* Config.schema(ReviewCompaction, "PR_REVIEW_COMPACTION").pipe(
      Config.withDefault("rollover"),
    );

    const contextTokenLimit = yield* Config.schema(
      ReviewContextTokenLimit,
      "PR_REVIEW_CONTEXT_TOKENS",
    ).pipe(Config.withDefault(48_000));

    const researchConcurrency = yield* Config.schema(
      Schema.Literals([0, 1, 2]),
      "PR_REVIEW_RESEARCH_CONCURRENCY",
    ).pipe(Config.withDefault(0));

    const trimmedGuidance = options.guidance?.trim();
    const effectiveGuidance = trimmedGuidance === "" ? undefined : trimmedGuidance;

    const guidanceDigest =
      effectiveGuidance === undefined ? undefined : yield* digestText(effectiveGuidance);

    const configuration = EvalVariantConfiguration.make({
      id: options.id,
      reviewerProfile: "repository-review",
      provider: "openai",
      model,
      reasoningEffort,
      compaction,
      contextTokenLimit,
      ...(researchConcurrency === 0
        ? {}
        : { research: { concurrency: researchConcurrency, maxOutputTokens: 4_000 } }),
      maxOutputTokens: 32_000,
      strictJsonSchema: true,
      store: false,
      maxCostMicrousd: Math.floor(maxCostUsd * 1_000_000),
      budgetPolicy: "input-size-v1",
      ...(guidanceDigest === undefined ? {} : { guidanceDigest }),
    });

    const modelLayer = (maxOutputTokens: number) =>
      OpenAiLanguageModel.model(configuration.model, {
        max_output_tokens: maxOutputTokens,
        reasoning: { effort: configuration.reasoningEffort },
        store: configuration.store,
        service_tier: "default",
        strictJsonSchema: configuration.strictJsonSchema,
      });

    return {
      configuration,
      review: Effect.fn("PrReviewEval.review")(function* (request: ReviewRequest) {
        // Allocate the shipping ledger per invocation, including concurrent/repeated trials.
        const provider = yield* makeReviewOpenAi({
          model: configuration.model,
          cacheKey: `pr-review:${request.headRevision}`,
          costLimitMicrousd: reviewCostLimitMicrousd(request, maxCostUsd),
        }).pipe(Effect.mapError((error) => reviewerFailure(error)));

        const reviewer = makeReviewer({
          model: modelLayer(configuration.maxOutputTokens),
          costControl: provider.costControl,
          compaction: configuration.compaction,
          contextTokenLimit: configuration.contextTokenLimit,
          ...(configuration.research === undefined
            ? {}
            : {
                research: {
                  model: modelLayer(configuration.research.maxOutputTokens),
                  concurrency: configuration.research.concurrency,
                },
              }),
          ...(effectiveGuidance === undefined ? {} : { guidance: effectiveGuidance }),
        });

        return yield* reviewer.review(request).pipe(
          Effect.provideService(OpenAiClient.OpenAiClient, provider.client),
          Effect.catch((error) =>
            provider.costControl.snapshot.pipe(
              Effect.flatMap((snapshot) =>
                Effect.fail(reviewerFailure(error, snapshot.usage.estimatedCostMicrousd)),
              ),
            ),
          ),
        );
      }),
    } satisfies EvalVariant<OpenAiClient.OpenAiClient | ReviewRepository>;
  },
);

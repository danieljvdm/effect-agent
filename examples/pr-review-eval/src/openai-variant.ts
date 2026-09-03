import {
  makeReviewOpenAi,
  REVIEW_COST_LIMIT_MICROUSD,
  reviewCostEstimator,
} from "@effect-agent/pr-review-action/review-openai";
import { makeReviewer, type ReviewRequest } from "@effect-agent/pr-review/Review";
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
    const trimmedGuidance = options.guidance?.trim();
    const effectiveGuidance = trimmedGuidance === "" ? undefined : trimmedGuidance;

    const guidanceDigest =
      effectiveGuidance === undefined ? undefined : yield* digestText(effectiveGuidance);

    const configuration = EvalVariantConfiguration.make({
      id: options.id,
      reviewerProfile: "diff-review-v5-capped",
      provider: "openai",
      model: "gpt-5.6-sol",
      reasoningEffort: "xhigh",
      maxOutputTokens: 32_000,
      strictJsonSchema: true,
      store: false,
      costLimitMicrousd: REVIEW_COST_LIMIT_MICROUSD,
      ...(guidanceDigest === undefined ? {} : { guidanceDigest }),
    });

    return {
      configuration,
      review: Effect.fn("PrReviewEval.review")(function* (request: ReviewRequest) {
        // Allocate the shipping ledger per invocation, including concurrent/repeated trials.
        const provider = yield* makeReviewOpenAi({
          client: yield* OpenAiClient.OpenAiClient,
          model: configuration.model,
          cacheKey: `pr-review-v2:${request.headRevision}`,
        }).pipe(Effect.mapError((error) => reviewerFailure(error)));

        const reviewer = makeReviewer({
          model: OpenAiLanguageModel.model(configuration.model, {
            max_output_tokens: configuration.maxOutputTokens,
            reasoning: { effort: configuration.reasoningEffort },
            store: configuration.store,
            service_tier: "default",
            strictJsonSchema: configuration.strictJsonSchema,
          }),
          estimateCostMicrousd: reviewCostEstimator(configuration.model),
          costControl: provider.costControl,
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

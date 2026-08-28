import { type ReviewRepository, makeReviewer, type ReviewRequest } from "@effect-agent/pr-review";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Option, Ref, Schema } from "effect";
import { AiError, type Response } from "effect/unstable/ai";
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

const gpt56SolCost = (usage: Response.Usage) => {
  const cacheRead = usage.inputTokens.cacheRead ?? 0;
  const inputTotal = usage.inputTokens.total ?? (usage.inputTokens.uncached ?? 0) + cacheRead;
  const uncached = Math.max(0, inputTotal - cacheRead);
  const cacheWrite = Math.min(uncached, usage.inputTokens.cacheWrite ?? 0);
  const standardInput = uncached - cacheWrite;
  // GPT-5.6 Sol standard rates published 2026-08-25: $4/$0.40/$5/$20 per M tokens.
  const microusd = Math.ceil(
    (standardInput * 400 +
      cacheRead * 40 +
      cacheWrite * 500 +
      (usage.outputTokens.total ?? 0) * 2_000) /
      100,
  );
  return Effect.succeed({ costMicrousd: microusd, pricingVersion: "openai-api-2026-08-25" });
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
      reviewerProfile: "source-review-v4",
      provider: "openai",
      model: "gpt-5.6-sol",
      reasoningEffort: "xhigh",
      maxOutputTokens: 32_000,
      strictJsonSchema: true,
      store: false,
      ...(guidanceDigest === undefined ? {} : { guidanceDigest }),
    });
    return {
      configuration,
      review: Effect.fn("PrReviewEval.review")(function* (request: ReviewRequest) {
        // This Ref belongs to one review invocation, so concurrent eval jobs cannot share cost.
        const accountedCost = yield* Ref.make<number | undefined>(undefined);
        const reviewer = makeReviewer({
          model: OpenAiLanguageModel.model(configuration.model, {
            max_output_tokens: configuration.maxOutputTokens,
            reasoning: { effort: configuration.reasoningEffort },
            store: configuration.store,
            strictJsonSchema: configuration.strictJsonSchema,
          }),
          estimateCostMicrousd: (usage) =>
            gpt56SolCost(usage).pipe(
              Effect.tap((estimate) =>
                Ref.update(accountedCost, (total) => (total ?? 0) + estimate.costMicrousd),
              ),
            ),
          ...(effectiveGuidance === undefined ? {} : { guidance: effectiveGuidance }),
        });
        return yield* reviewer
          .review(request)
          .pipe(
            Effect.catch((error) =>
              Ref.get(accountedCost).pipe(
                Effect.flatMap((cost) => Effect.fail(reviewerFailure(error, cost))),
              ),
            ),
          );
      }),
    } satisfies EvalVariant<OpenAiClient.OpenAiClient | ReviewRepository>;
  },
);

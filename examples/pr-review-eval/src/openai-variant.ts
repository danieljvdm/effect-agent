import {
  type ReviewRepository,
  type ReviewDiagnosticsSink,
  type ReviewStrategy,
  makeReviewer,
  type ReviewRequest,
  reviewInstructions,
  REVIEW_VERIFICATION_INSTRUCTIONS,
  REVIEW_LIMITS,
} from "@effect-agent/pr-review";
import {
  makeReviewOpenAi,
  REVIEW_COST_LIMIT_MICROUSD,
  reviewCostEstimator,
  REVIEW_DISCOVERY_COST_LIMIT_MICROUSD,
  REVIEW_VERIFICATION_RESERVE_MICROUSD,
  REVIEW_MAX_INPUT_TOKENS,
  REVIEW_MAX_OUTPUT_TOKENS,
  REVIEW_PRICING_VERSION,
  REVIEW_PRICING_VALID_UNTIL,
  REVIEW_CACHE_POLICY,
  reviewModelPricing,
} from "@effect-agent/pr-review-action/review-openai";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Crypto, Effect, Layer, Option, Schema } from "effect";
import { AiError } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import {
  EvalConfigurationError,
  EvalEffectiveConfiguration,
  type EvalReasoningEffort,
  EvalReviewerFailure,
  EvalVariantConfiguration,
  type EvalVariantId,
} from "./contracts.ts";
import { digestText } from "./corpus.ts";
import { type EvalTrialContext, type EvalVariant } from "./runner.ts";

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
  readonly model?: string;
  readonly reasoningEffort?: EvalReasoningEffort;
  readonly strategy?: ReviewStrategy;
  readonly reviewerRevision?: string;
}

export const makeCurrentOpenAiVariant = Effect.fn("PrReviewEval.makeCurrentOpenAiVariant")(
  function* (options: CurrentOpenAiVariantOptions) {
    const trimmedGuidance = options.guidance?.trim();
    const effectiveGuidance = trimmedGuidance === "" ? undefined : trimmedGuidance;

    const guidanceDigest = yield* digestText(effectiveGuidance ?? "");
    const strategy = options.strategy ?? "baseline";
    const model = options.model ?? "gpt-5.6-sol";
    const pricing = reviewModelPricing(model);

    if (pricing === undefined)
      return yield* EvalConfigurationError.make({
        message: "Missing pricing for the configured reviewer model",
      });

    const configuration = EvalVariantConfiguration.make({
      id: options.id,
      reviewerProfile: "diff-review-v5-capped",
      provider: "openai",
      model,
      reasoningEffort: options.reasoningEffort ?? "xhigh",
      maxOutputTokens: REVIEW_MAX_OUTPUT_TOKENS,
      strictJsonSchema: true,
      store: false,
      costLimitMicrousd: REVIEW_COST_LIMIT_MICROUSD,
      guidanceDigest,
      strategy,
      effective: EvalEffectiveConfiguration.make({
        ...(options.reviewerRevision === undefined
          ? {}
          : { reviewerRevision: options.reviewerRevision }),
        discoveryPromptDigest: yield* digestText(reviewInstructions(effectiveGuidance)),
        verificationPromptDigest: yield* digestText(
          `${REVIEW_VERIFICATION_INSTRUCTIONS}${effectiveGuidance === undefined ? "" : `\n\nRepository guidance:\n${effectiveGuidance}`}`,
        ),
        serviceTier: "default",
        discoveryLimitMicrousd:
          strategy === "verified"
            ? REVIEW_DISCOVERY_COST_LIMIT_MICROUSD
            : REVIEW_COST_LIMIT_MICROUSD,
        verificationReserveMicrousd:
          strategy === "verified" ? REVIEW_VERIFICATION_RESERVE_MICROUSD : 0,
        maxInputTokens: REVIEW_MAX_INPUT_TOKENS,
        maxTurns: REVIEW_LIMITS.costAdmittedMaxTurns,
        maxToolCalls: REVIEW_LIMITS.maxToolCalls,
        maxDurationMillis: REVIEW_LIMITS.maxDurationMs,
        contextTokenLimit: REVIEW_LIMITS.contextTokenLimit,
        completionReserveTokens: 0,
        candidateCapacity: REVIEW_LIMITS.candidateCapacity,
        patchBatchCharacters: REVIEW_LIMITS.patchBatchCharacters,
        pricingVersion: REVIEW_PRICING_VERSION,
        pricingValidUntil: REVIEW_PRICING_VALID_UNTIL,
        pricing: {
          input: pricing.input,
          read: pricing.read,
          write: pricing.write,
          output: pricing.output,
        },
        cache: { ...REVIEW_CACHE_POLICY, namespacePolicy: "isolated-case-strategy-trial-v1" },
      }),
    });

    return {
      configuration,
      review: Effect.fn("PrReviewEval.review")(function* (
        request: ReviewRequest,
        trial?: EvalTrialContext,
      ) {
        const crypto = yield* Crypto.Crypto;

        const cacheKey =
          trial?.cacheNamespace ??
          (yield* crypto.randomUUIDv4.pipe(Effect.mapError((error) => reviewerFailure(error))));

        // Allocate the shipping ledger per invocation, including concurrent/repeated trials.
        const provider = yield* makeReviewOpenAi({
          client: yield* OpenAiClient.OpenAiClient,
          model: configuration.model,
          cacheKey,
          strategy,
        }).pipe(Effect.mapError((error) => reviewerFailure(error)));

        const reviewer = makeReviewer({
          strategy,
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
    } satisfies EvalVariant<
      OpenAiClient.OpenAiClient | ReviewRepository | ReviewDiagnosticsSink | Crypto.Crypto
    >;
  },
);

import { makeReviewer, type ReviewRequestPresentation } from "@effect-agent/pr-review";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Option, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import {
  CURRENT_REVIEWER_PROFILE,
  SEGMENTED_FILES_REVIEWER_PROFILE,
  type EvalReasoningEffort,
  EvalReviewerFailure,
  EvalVariantConfiguration,
  type EvalVariantId,
} from "./contracts.ts";
import { digestText } from "./corpus.ts";
import { type EvalVariant } from "./runner.ts";

const ReviewerErrorView = Schema.Struct({
  _tag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  message: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(4_096))),
});

const reviewerFailure = (error: unknown): EvalReviewerFailure =>
  Option.match(Schema.decodeUnknownOption(ReviewerErrorView)(error), {
    onNone: () =>
      EvalReviewerFailure.make({
        errorTag: "ReviewInvocationFailure",
        message: "Reviewer invocation failed without a bounded typed diagnostic",
      }),
    onSome: (view) =>
      EvalReviewerFailure.make({
        errorTag: view._tag,
        message: view.message?.trim() || view._tag,
      }),
  });

export const openAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted("OPENAI_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

export interface CurrentOpenAiVariantOptions {
  readonly id: EvalVariantId;
  readonly model: string;
  readonly reasoningEffort: EvalReasoningEffort;
  readonly guidance?: string | undefined;
  readonly requestPresentation?: ReviewRequestPresentation | undefined;
}

export const makeCurrentOpenAiVariant = Effect.fn("PrReviewEval.makeCurrentOpenAiVariant")(
  function* (options: CurrentOpenAiVariantOptions) {
    const trimmedGuidance = options.guidance?.trim();
    const effectiveGuidance = trimmedGuidance === "" ? undefined : trimmedGuidance;
    const guidanceDigest =
      effectiveGuidance === undefined ? undefined : yield* digestGuidance(effectiveGuidance);
    const configuration = EvalVariantConfiguration.make({
      id: options.id,
      reviewerProfile:
        options.requestPresentation === undefined
          ? CURRENT_REVIEWER_PROFILE
          : SEGMENTED_FILES_REVIEWER_PROFILE,
      provider: "openai",
      model: options.model,
      reasoningEffort: options.reasoningEffort,
      maxOutputTokens: 8_000,
      strictJsonSchema: true,
      store: false,
      ...(guidanceDigest === undefined ? {} : { guidanceDigest }),
    });
    const reviewer = makeReviewer({
      model: OpenAiLanguageModel.model(options.model, {
        max_output_tokens: configuration.maxOutputTokens,
        reasoning: { effort: options.reasoningEffort },
        store: configuration.store,
        strictJsonSchema: configuration.strictJsonSchema,
      }),
      ...(effectiveGuidance === undefined ? {} : { guidance: effectiveGuidance }),
      ...(options.requestPresentation === undefined
        ? {}
        : { requestPresentation: options.requestPresentation }),
    });
    return {
      configuration,
      review: (request) => reviewer.review(request).pipe(Effect.mapError(reviewerFailure)),
    } satisfies EvalVariant<OpenAiClient.OpenAiClient>;
  },
);

export const digestGuidance = Effect.fn("PrReviewEval.digestGuidance")(function* (
  guidance: string,
) {
  return yield* digestText(guidance);
});

import { AnthropicClient, AnthropicLanguageModel } from "@effect/ai-anthropic";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { resolveEffortRung, type EffortAliasName, type EffortPosition } from "./effort.ts";

// ---------------------------------------------------------------------------
// Built-in provider bindings for the two host entrypoints (CLI and Action).
// The library itself stays provider-agnostic — the configuration factory
// takes any Effect AI Model — and these helpers exist so the batteries-
// included paths need one flag and one credential, nothing more. Client
// Layers carry their redacted credentials from configuration; the
// application supplies them at the edge (D-027).
// ---------------------------------------------------------------------------

export const ReviewProvider = Schema.Literals(["openai", "anthropic"]);
export type ReviewProvider = typeof ReviewProvider.Type;

/** OpenAI Responses service tiers supported by the packaged reviewer. */
export const OpenAiServiceTier = Schema.Literal("fast");
export type OpenAiServiceTier = typeof OpenAiServiceTier.Type;

/** A provider-specific OpenAI tier was configured for another provider. */
export class UnsupportedServiceTierProvider extends Schema.TaggedError<UnsupportedServiceTierProvider>()(
  "UnsupportedServiceTierProvider",
  {
    provider: ReviewProvider,
    serviceTier: OpenAiServiceTier,
  },
) {
  override get message() {
    return `Service tier '${this.serviceTier}' requires provider 'openai'; received '${this.provider}'.`;
  }
}

/** Reject an OpenAI-only service tier before constructing another provider's model. */
export const validateReviewServiceTier = Effect.fn("validateReviewServiceTier")(function* (
  provider: ReviewProvider,
  serviceTier: OpenAiServiceTier | undefined,
): Effect.fn.Return<OpenAiServiceTier | undefined, UnsupportedServiceTierProvider> {
  if (serviceTier !== undefined && provider !== "openai") {
    return yield* UnsupportedServiceTierProvider.make({ provider, serviceTier });
  }
  return serviceTier;
});

export const DEFAULT_PROVIDER: ReviewProvider = "openai";

export const DEFAULT_MODEL: Record<ReviewProvider, string> = {
  openai: "gpt-5.6-sol",
  anthropic: "claude-sonnet-5",
};

export const PROVIDER_CREDENTIAL_ENV: Record<ReviewProvider, string> = {
  openai: "OPENAI_API_KEY",
  anthropic: "ANTHROPIC_API_KEY",
};

/**
 * Each provider's offered reasoning-effort ladder, cheapest first. The rungs
 * that turn reasoning off (`none`, `minimal`) are deliberately not offered —
 * no review run wants them. An `EffortPosition` resolves into the running
 * provider's own ladder, so the same stored position survives a provider or
 * model change.
 */
export const PROVIDER_EFFORT_RUNGS = {
  openai: ["low", "medium", "high", "xhigh"],
  anthropic: ["low", "medium", "high"],
} as const satisfies Record<
  ReviewProvider,
  readonly [EffortAliasName, ...ReadonlyArray<EffortAliasName>]
>;

/** One OpenAI review model binding with the package's structured-output settings. */
export const makeOpenAiReviewModel = (
  model?: string,
  effort?: EffortPosition,
  serviceTier?: OpenAiServiceTier,
) =>
  OpenAiLanguageModel.model(model ?? DEFAULT_MODEL.openai, {
    // OpenAI counts hidden reasoning tokens and visible answer tokens against
    // this same ceiling. High-effort reviews can exhaust an 8k allowance
    // after reading every file but before emitting their structured report.
    max_output_tokens: 32_000,
    store: false,
    strictJsonSchema: true,
    ...(serviceTier === undefined ? {} : { service_tier: serviceTier }),
    ...(effort === undefined
      ? {}
      : { reasoning: { effort: resolveEffortRung(effort, PROVIDER_EFFORT_RUNGS.openai) } }),
  });

/** One Anthropic review model binding with the package's output settings. */
export const makeAnthropicReviewModel = (model?: string, effort?: EffortPosition) =>
  AnthropicLanguageModel.model(model ?? DEFAULT_MODEL.anthropic, {
    max_tokens: 8_000,
    ...(effort === undefined
      ? {}
      : { output_config: { effort: resolveEffortRung(effort, PROVIDER_EFFORT_RUNGS.anthropic) } }),
  });

/**
 * The human-readable descriptor of one provider binding, e.g.
 * `openai/gpt-5.6-sol (effort high, service tier fast)`. Rendered into the review footer and
 * included in the changeset-fingerprint signature, so a provider, model, or
 * request-profile change re-reviews instead of skipping.
 */
export const describeReviewModel = (
  provider: ReviewProvider,
  model?: string,
  effort?: EffortPosition,
  serviceTier?: OpenAiServiceTier,
): string => {
  const base = `${provider}/${model ?? DEFAULT_MODEL[provider]}`;
  const details = [
    ...(effort === undefined
      ? []
      : [`effort ${resolveEffortRung(effort, PROVIDER_EFFORT_RUNGS[provider])}`]),
    ...(serviceTier === undefined ? [] : [`service tier ${serviceTier}`]),
  ];
  return details.length === 0 ? base : `${base} (${details.join(", ")})`;
};

/** The OpenAI client Layer, credential from `OPENAI_API_KEY`. */
export const openAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted(PROVIDER_CREDENTIAL_ENV.openai),
}).pipe(Layer.provide(FetchHttpClient.layer));

/** The Anthropic client Layer, credential from `ANTHROPIC_API_KEY`. */
export const anthropicClientLayer = AnthropicClient.layerConfig({
  apiKey: Config.redacted(PROVIDER_CREDENTIAL_ENV.anthropic),
}).pipe(Layer.provide(FetchHttpClient.layer));

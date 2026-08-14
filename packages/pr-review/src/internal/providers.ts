import { AnthropicClient, AnthropicLanguageModel } from "@effect/ai-anthropic";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

// ---------------------------------------------------------------------------
// Built-in provider bindings for the two host entrypoints (CLI and Action).
// The library itself stays provider-agnostic — the configuration factory
// takes any Effect AI Model — and these helpers exist so the batteries-
// included paths need one flag and one credential, nothing more. Client
// Layers carry their redacted credentials from configuration; the
// application supplies them at the edge (D-027).
// ---------------------------------------------------------------------------

export type ReviewProvider = "openai" | "anthropic";

export const DEFAULT_PROVIDER: ReviewProvider = "openai";

export const DEFAULT_MODEL: Record<ReviewProvider, string> = {
  openai: "gpt-5.6-sol",
  anthropic: "claude-sonnet-5",
};

export const PROVIDER_CREDENTIAL_ENV: Record<ReviewProvider, string> = {
  openai: "OPENAI_API_KEY",
  anthropic: "ANTHROPIC_API_KEY",
};

/** One OpenAI review model binding with the package's structured-output settings. */
export const makeOpenAiReviewModel = (model?: string) =>
  OpenAiLanguageModel.model(model ?? DEFAULT_MODEL.openai, {
    max_output_tokens: 8_000,
    store: false,
    strictJsonSchema: true,
  });

/** One Anthropic review model binding with the package's output settings. */
export const makeAnthropicReviewModel = (model?: string) =>
  AnthropicLanguageModel.model(model ?? DEFAULT_MODEL.anthropic, {
    max_tokens: 8_000,
  });

/** The OpenAI client Layer, credential from `OPENAI_API_KEY`. */
export const openAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted(PROVIDER_CREDENTIAL_ENV.openai),
}).pipe(Layer.provide(FetchHttpClient.layer));

/** The Anthropic client Layer, credential from `ANTHROPIC_API_KEY`. */
export const anthropicClientLayer = AnthropicClient.layerConfig({
  apiKey: Config.redacted(PROVIDER_CREDENTIAL_ENV.anthropic),
}).pipe(Layer.provide(FetchHttpClient.layer));

import * as WebSearch from "@effect-agent/capabilities/WebSearch";
import * as CloudflareAiGateway from "@effect-agent/platform-cloudflare/CloudflareAiGateway";
import { AnthropicClient, AnthropicLanguageModel, AnthropicTool } from "@effect/ai-anthropic";
import { OpenAiClient, OpenAiLanguageModel, OpenAiTool } from "@effect/ai-openai";
import { Layer, type Redacted } from "effect";

/** Host configuration; supply HttpClient (FetchHttpClient works in both Node and Workers). */
export interface GatewayOptions {
  readonly accountId: string;
  readonly gatewayId: string;
  readonly apiToken: Redacted.Redacted<string>;
}

/** A WebSearch backend using Cloudflare Unified Billing and a provider-qualified model. */
export const openAiGatewaySearch = (options: GatewayOptions) =>
  WebSearch.layer({
    tool: OpenAiTool.WebSearch({ search_context_size: "medium" }),
  }).pipe(
    Layer.provide(
      OpenAiLanguageModel.model("openai/gpt-4.1-mini", { max_output_tokens: 2_048, store: false }),
    ),
    CloudflareAiGateway.provide(OpenAiClient.layer, { ...options, protocol: "responses" }),
  );

/** The same model-visible tool, backed by Anthropic through a provider-native Gateway route. */
export const anthropicGatewaySearch = (options: GatewayOptions) =>
  WebSearch.layer({
    tool: AnthropicTool.WebSearch_20250305({ maxUses: 3 }),
  }).pipe(
    Layer.provide(AnthropicLanguageModel.model("claude-haiku-4-5", { max_tokens: 2_048 })),
    CloudflareAiGateway.provide(AnthropicClient.layer, { ...options, provider: "anthropic" }),
  );

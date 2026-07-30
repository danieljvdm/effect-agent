import { OpenAiLanguageModel, OpenAiTool } from "@effect/ai-openai";
import { Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { Agent, AgentPolicy } from "@effect-agent/core";
import { CalculatorToolkit, ChatInput, ChatOutput, GeneralChatInstructions } from "./general-chat";

export const OPENAI_DEMO_MODEL = "gpt-5.6-luna" as const;

const UpstreamOpenAiWebSearch = OpenAiTool.WebSearch({
  search_context_size: "medium",
});

/**
 * Compatibility projection for Effect beta.102, whose OpenAI adapter emits an
 * empty hosted-search declaration before returning the final action as result.
 */
export const OpenAiWebSearch = Tool.providerDefined({
  id: UpstreamOpenAiWebSearch.id,
  customName: UpstreamOpenAiWebSearch.name,
  providerName: UpstreamOpenAiWebSearch.providerName,
  args: UpstreamOpenAiWebSearch.argsSchema,
  parameters: Schema.Struct({}),
  success: UpstreamOpenAiWebSearch.successSchema,
  failure: UpstreamOpenAiWebSearch.failureSchema,
})(UpstreamOpenAiWebSearch.args);

/** Live toolkit combining real local arithmetic with hosted web research. */
export const OpenAiChatToolkit = Toolkit.merge(CalculatorToolkit, Toolkit.make(OpenAiWebSearch));

/** General chat definition with optional, description-driven live tools. */
export const OpenAiChatDefinition = Agent.define("general-chat-openai", {
  input: ChatInput,
  output: ChatOutput,
  instructions: GeneralChatInstructions,
  toolkit: OpenAiChatToolkit,
  policy: AgentPolicy.make({
    maxTurns: 4,
    maxToolCalls: 6,
    maxDuration: "45 seconds",
    toolConcurrency: 1,
  }),
  description: "General chat with real arithmetic and OpenAI-hosted web research.",
  metadata: {
    deploymentClass: "E",
    phase: "P1-preview",
    providerCapability: "openai.web_search",
  },
});

/** Server-only model binding for live general chat. */
export const OpenAiChatAgent = Agent.withModel(
  OpenAiChatDefinition,
  OpenAiLanguageModel.model(OPENAI_DEMO_MODEL, {
    max_output_tokens: 1_600,
    store: false,
    strictJsonSchema: true,
    text: { verbosity: "low" },
  }),
);

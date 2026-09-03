import * as Agent from "@effect-agent/core/Agent";
import { TravelPlanner } from "@effect-agent/testing/TravelPlanner";
import { AnthropicLanguageModel } from "@effect/ai-anthropic";
import { OpenAiLanguageModel } from "@effect/ai-openai";

/** Explicit ephemeral binding for an application-supplied OpenAI client Layer. */
export const OpenAiTravelPlanner = Agent.withModel(
  TravelPlanner,
  OpenAiLanguageModel.model("gpt-4.1-mini"),
);

/** Explicit ephemeral binding for an application-supplied Anthropic client Layer. */
export const AnthropicTravelPlanner = Agent.withModel(
  TravelPlanner,
  AnthropicLanguageModel.model("claude-haiku-4-5"),
);

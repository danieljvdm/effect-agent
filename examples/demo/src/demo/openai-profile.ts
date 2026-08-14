import { OpenAiLanguageModel, OpenAiTool } from "@effect/ai-openai";
import { Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { Agent, AgentPolicy } from "@effect-agent/core";
import {
  HoldItinerary,
  TravelGuidance,
  TravelPlan,
  TravelPlannerPhase2Toolkit,
  TripRequest,
} from "@effect-agent/testing";
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

/**
 * The reusable Phase 2 fixture keeps a deliberately tiny 2,048-token policy
 * for deterministic tests. A real provider needs room for Tool schemas,
 * results, and one queued update while retaining the same P2 safety bounds.
 */
export const OpenAiTravelPlannerDefinition = Agent.define("travel-planner-phase-2-live-demo", {
  input: TripRequest,
  output: TravelPlan,
  instructions: (input) =>
    Effect.flatMap(TravelGuidance, (guidance) => guidance.instructions(input)),
  toolkit: TravelPlannerPhase2Toolkit,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 4,
    maxDuration: "30 seconds",
    toolConcurrency: 3,
    tokenBudget: 12_000,
  }),
  description:
    "Coordinate repeatable travel suppliers and require approval before a demo itinerary hold.",
  metadata: { deploymentClass: "E", phase: "P2-live-demo" },
});

/** Live coordinator for fixture travel suppliers and approval-gated holds. */
export const OpenAiTravelPlannerAgent = Agent.withModel(
  OpenAiTravelPlannerDefinition,
  OpenAiLanguageModel.model(OPENAI_DEMO_MODEL, {
    max_output_tokens: 1_600,
    store: false,
    strictJsonSchema: true,
    text: { verbosity: "low" },
  }),
);

/** Demo-hold sub-toolkit: the only real-travel tool that needs an application handler. */
export const RealTravelHoldToolkit = Toolkit.make(HoldItinerary);

/**
 * Real research toolkit: OpenAI-hosted web search (provider-executed, no
 * application handler) plus the approval-gated demo hold.
 */
export const RealTravelToolkit = Toolkit.merge(
  Toolkit.make(OpenAiWebSearch),
  RealTravelHoldToolkit,
);

const realTravelInstructions = (input: TripRequest): string =>
  [
    "You are a real travel agent. Research actual, current options with the web_search tool before answering.",
    `The traveler's message is authoritative: ${input.request}`,
    `Structured defaults, used only where the message is silent: origin ${input.origin}, destination ${input.destination}, departing ${input.departOn}, ${input.nights} nights, ${input.travelers} traveler(s), budget ${Math.round(input.budgetCents / 100)} ${input.currency}.`,
    "Use web_search to find real flight options, real places to stay, and real activities for the trip the traveler actually described. Prefer several focused searches over one broad one.",
    "Every itinerary must cite its sources: put each source URL you relied on in the assumptions array, alongside any assumptions you made.",
    "Prices are estimates compiled from public sources, never guaranteed quotes; state that in assumptions. Set quoteId to a short stable slug of the form research-<route>-<date> that you invent.",
    "Only call hold_itinerary when the traveler explicitly asks for a temporary hold. The runtime independently requires human approval before its handler starts, and demo holds create no real reservation — say so when you place one.",
    "Apply steering or follow-up messages that arrive at safe Turn boundaries.",
    'After researching, return only this JSON shape, with no omitted or additional keys and no Markdown fences: {"itineraries":[{"title":"string","route":"string","dates":"string","flight":"string","lodging":"string","activities":["string"],"estimatedTotalCents":1,"currency":"USD","quoteId":"string","assumptions":["string"],"unresolvedConstraints":["string"],"nextAction":"review"}]}.',
    'The dates field is one human-readable string, for example "22–26 September 2026"; it is never an object. estimatedTotalCents is a positive integer count of US cents. documents in assumptions are plain strings.',
  ].join("\n");

/**
 * The REAL travel agent: hosted web search over the live web instead of
 * fixture supplier catalogs. Wider bounds than the fixture profile because
 * genuine research takes several searches and search results are token-heavy;
 * every bound remains finite and run-level budgets still apply.
 */
export const OpenAiRealTravelPlannerDefinition = Agent.define("travel-planner-live-research", {
  input: TripRequest,
  output: TravelPlan,
  instructions: realTravelInstructions,
  toolkit: RealTravelToolkit,
  policy: AgentPolicy.make({
    maxTurns: 6,
    maxToolCalls: 8,
    maxDuration: "3 minutes",
    toolConcurrency: 2,
    tokenBudget: 160_000,
  }),
  description:
    "Research real travel options with OpenAI-hosted web search, cite sources, and require approval before a demo itinerary hold.",
  metadata: {
    deploymentClass: "E",
    phase: "P7-live",
    providerCapability: "openai.web_search",
  },
});

/** Live binding for the real research travel agent. */
export const OpenAiRealTravelPlannerAgent = Agent.withModel(
  OpenAiRealTravelPlannerDefinition,
  OpenAiLanguageModel.model(OPENAI_DEMO_MODEL, {
    max_output_tokens: 4_000,
    store: false,
    strictJsonSchema: true,
    text: { verbosity: "low" },
  }),
);

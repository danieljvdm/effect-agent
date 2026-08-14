import { describe, expect, it } from "vite-plus/test";

import { OpenAiTravelPlannerAgent, OpenAiTravelPlannerDefinition } from "./openai-profile";

describe("live travel profile", () => {
  it("binds the Phase 2 travel agent to an OpenAI model", () => {
    expect(OpenAiTravelPlannerAgent.definition).toBe(OpenAiTravelPlannerDefinition);
    expect(OpenAiTravelPlannerAgent.model.provider).toBe("openai");
    expect(OpenAiTravelPlannerAgent.definition.policy.toolConcurrency).toBe(3);
    expect(OpenAiTravelPlannerAgent.definition.policy.maxToolCalls).toBe(4);
  });
});

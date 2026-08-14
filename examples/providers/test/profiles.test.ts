import { TravelPlanner } from "@effect-agent/testing";
import { describe, expect, it } from "vite-plus/test";

import { AnthropicTravelPlanner, OpenAiTravelPlanner } from "../src/index.ts";

describe("provider Model bindings", () => {
  it("keeps the shared Travel Planner Definition explicit and model-agnostic", () => {
    expect(OpenAiTravelPlanner.definition).toBe(TravelPlanner);
    expect(AnthropicTravelPlanner.definition).toBe(TravelPlanner);
    expect(OpenAiTravelPlanner.model).not.toBe(AnthropicTravelPlanner.model);
  });
});

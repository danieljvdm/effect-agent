import { expectedTravelPlan } from "@effect-agent/testing/fixtures/travel-planner";
import { describe, expect, it } from "@effect/vitest";

import {
  formatTravelPlanForChat,
  primaryCapabilityRecipes,
  secondaryCapabilityRecipes,
} from "./chat-capabilities";

describe("chat-native Phase 2 recipes", () => {
  it("keeps every operational scenario explicitly reachable from the Chat launcher", () => {
    expect(
      Array.from(
        new Set(
          [...primaryCapabilityRecipes, ...secondaryCapabilityRecipes].map(
            (recipe) => recipe.scenario,
          ),
        ),
      ),
    ).toEqual([
      "guided",
      "hold",
      "budget-cost",
      "budget-tokens",
      "budget-tools",
      "budget-duration",
    ]);
  });

  it("renders the structured TravelPlan as a natural-language answer", () => {
    const answer = formatTravelPlanForChat(expectedTravelPlan);

    expect(answer).toContain("###");
    expect(answer).toContain("**Flight:**");
    expect(answer).toContain("**Stay:**");
    expect(answer).toContain("Estimated total:");
  });
});

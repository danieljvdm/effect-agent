import type { TravelPlan } from "@effect-agent/testing";

import type { DemoOperationalEvent, DemoScenario } from "./operational-contracts";

export interface CapabilityRecipe {
  readonly scenario: DemoScenario;
  readonly label: string;
  readonly message: string;
  readonly detail: string;
}

/** Natural-language recipes that exercise the real Phase 2 runtime from Chat. */
export const primaryCapabilityRecipes: ReadonlyArray<CapabilityRecipe> = [
  {
    scenario: "guided",
    label: "Plan with live tools",
    message:
      "Plan the fixed London demo trip. Compare flights, stays, and activities before recommending an itinerary.",
    detail: "OpenAI coordinates 3 fixture tools",
  },
  {
    scenario: "hold",
    label: "Ask before a demo hold",
    message:
      "Plan the fixed London demo trip, then place a temporary hold. Ask me before the hold tool starts.",
    detail: "Live model + explicit approval",
  },
  {
    scenario: "guided",
    label: "Change plans while it works",
    message: "Start planning the fixed London demo trip. I may change the dates while you work.",
    detail: "Queue an update during the run",
  },
];

export const secondaryCapabilityRecipes: ReadonlyArray<CapabilityRecipe> = [
  {
    scenario: "budget-cost",
    label: "Spending limit",
    message: "Try the fixed London demo trip with the deliberately tiny spending limit.",
    detail: "Blocked before execution",
  },
  {
    scenario: "budget-tokens",
    label: "Token limit",
    message: "Show me a run that safely stops at its input-token limit.",
    detail: "1-token allowance",
  },
  {
    scenario: "budget-tools",
    label: "Tool-call limit",
    message: "Show me a run that safely stops at its tool-call limit.",
    detail: "1 of 3 calls allowed",
  },
  {
    scenario: "budget-duration",
    label: "Time limit",
    message: "Show me a run that safely stops at its time limit.",
    detail: "10 ms deadline",
  },
];

export const formatTravelPlanForChat = (plan: TravelPlan): string => {
  const itinerary = plan.itineraries[0];
  if (itinerary === undefined) {
    return "The run completed, but the deterministic fixture returned no itinerary.";
  }

  return [
    `### ${itinerary.title}`,
    `${itinerary.route} · ${itinerary.dates}`,
    "",
    `**Flight:** ${itinerary.flight}`,
    `**Stay:** ${itinerary.lodging}`,
    `**Activities:** ${itinerary.activities.join(", ")}`,
    "",
    `Estimated total: **${new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: itinerary.currency,
    }).format(itinerary.estimatedTotalCents / 100)}**`,
    "",
    "Supplier results came from repeatable demo inventory. No real reservation was made.",
  ].join("\n");
};

export const capabilityFailureMessage = (
  events: ReadonlyArray<DemoOperationalEvent>,
  fallback: string,
): string => {
  const rejection = events.findLast((event) => event._tag === "DemoBudgetRejected");
  if (rejection?._tag === "DemoBudgetRejected") {
    const limitNames = {
      "input-tokens": "input-token",
      "output-tokens": "output-token",
      "tool-calls": "tool-call",
      cost: "spending",
      duration: "time",
    } as const;
    return `The run stopped safely at its **${limitNames[rejection.limit]} limit** (${rejection.observedValue} requested; ${rejection.limitValue} allowed). The rejected action did not start.`;
  }

  const denied = events.some(
    (event) => event._tag === "DemoApprovalSettled" && event.choice === "deny",
  );
  if (denied) {
    return "You denied the itinerary hold, so the risky tool never started. Nothing was placed on hold.";
  }

  return fallback;
};

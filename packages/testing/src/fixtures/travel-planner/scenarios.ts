import { Schema } from "effect";

import type { ScriptedTurnInput } from "../../ScriptedModel.ts";
import { TravelPlan, TripRequest, type TravelPlan as TravelPlanValue } from "./definition.ts";

const usage = { inputTokens: { total: 128 }, outputTokens: { total: 96 } };

export const phase1Trip = Schema.decodeSync(TripRequest)({
  request:
    "Plan a review-only London trip using the deterministic flight, lodging, and activity searches.",
  origin: "SFO",
  destination: "LHR",
  departOn: "2026-09-14",
  nights: 4,
  travelers: 2,
  budgetCents: 350_000,
  currency: "USD",
});

export const expectedTravelPlan: TravelPlanValue = Schema.decodeSync(TravelPlan)({
  itineraries: [
    {
      title: "Westward light, eastbound overnight",
      route: "San Francisco → London",
      dates: "14–19 September 2026",
      flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
      lodging: "Bloomsbury House · refundable studio · 4 nights",
      activities: ["British Museum timed entry", "Thames evening walk"],
      estimatedTotalCents: 284_000,
      currency: "USD",
      quoteId: "quote-sfo-lhr-001",
      assumptions: [
        "Two travelers sharing one studio",
        "Quote is read-only availability, not a reservation",
      ],
      unresolvedConstraints: [
        "Traveler names and accessibility requests are intentionally omitted",
      ],
      nextAction: "review",
    },
  ],
});

export const phase1HappyPathTurns = [
  {
    _tag: "Stream",
    parts: [
      {
        type: "tool-call",
        id: "flight-call-1",
        name: "search_flights",
        params: { origin: "SFO", destination: "LHR", departOn: "2026-09-14", travelers: 2 },
      },
      {
        type: "tool-call",
        id: "lodging-call-1",
        name: "search_lodging",
        params: { destination: "LHR", departOn: "2026-09-14", nights: 4, travelers: 2 },
      },
      {
        type: "tool-call",
        id: "activity-call-1",
        name: "search_activities",
        params: { destination: "LHR", departOn: "2026-09-14", nights: 4, travelers: 2 },
      },
      { type: "finish", reason: "tool-calls", usage },
    ],
    termination: { _tag: "Complete" },
  },
  {
    _tag: "Stream",
    parts: [
      { type: "text-start", id: "itinerary-json" },
      {
        type: "text-delta",
        id: "itinerary-json",
        delta: JSON.stringify(Schema.encodeSync(TravelPlan)(expectedTravelPlan)),
      },
      { type: "text-end", id: "itinerary-json" },
      { type: "finish", reason: "stop", usage },
    ],
    termination: { _tag: "Complete" },
  },
] satisfies readonly [ScriptedTurnInput, ScriptedTurnInput];

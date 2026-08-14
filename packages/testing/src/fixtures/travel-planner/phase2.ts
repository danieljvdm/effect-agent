import { Agent, AgentPolicy } from "@effect-agent/core";
import { Context, Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import {
  ActivityCatalog,
  FlightCatalog,
  LodgingCatalog,
  QuoteId,
  SearchActivities,
  SearchFlights,
  SearchLodging,
  TravelGuidance,
  TravelPlan,
  TripRequest,
} from "./definition.ts";

export class ItineraryHoldRequest extends Schema.Class<ItineraryHoldRequest>(
  "@effect-agent/testing/travel-planner/ItineraryHoldRequest",
)({
  quoteId: QuoteId,
  expiresInMinutes: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(60)),
}) {}

export class ItineraryHold extends Schema.Class<ItineraryHold>(
  "@effect-agent/testing/travel-planner/ItineraryHold",
)({
  holdId: Schema.NonEmptyString,
  quoteId: QuoteId,
  status: Schema.Literal("held"),
}) {}

export class ItineraryHoldUnavailable extends Schema.TaggedErrorClass<ItineraryHoldUnavailable>()(
  "ItineraryHoldUnavailable",
  {
    quoteId: QuoteId,
    message: Schema.String,
  },
) {}

export class ItineraryHoldGateway extends Context.Service<
  ItineraryHoldGateway,
  {
    readonly hold: (
      request: ItineraryHoldRequest,
    ) => Effect.Effect<ItineraryHold, ItineraryHoldUnavailable>;
  }
>()("@effect-agent/testing/travel-planner/ItineraryHoldGateway") {}

/**
 * The first mutating Travel Planner Tool. Effect AI marks it as approval-gated
 * so the engine must settle approval before acquiring a handler permit.
 */
export const HoldItinerary = Tool.make("hold_itinerary", {
  parameters: ItineraryHoldRequest,
  success: ItineraryHold,
  failure: ItineraryHoldUnavailable,
  failureMode: "error",
  dependencies: [ItineraryHoldGateway],
  needsApproval: true,
});

export const TravelPlannerPhase2Toolkit = Toolkit.make(
  SearchFlights,
  SearchLodging,
  SearchActivities,
  HoldItinerary,
);

export const TravelPlannerPhase2ToolkitLayer = TravelPlannerPhase2Toolkit.toLayer({
  search_flights: (query) => Effect.flatMap(FlightCatalog, (catalog) => catalog.search(query)),
  search_lodging: (query) => Effect.flatMap(LodgingCatalog, (catalog) => catalog.search(query)),
  search_activities: (query) => Effect.flatMap(ActivityCatalog, (catalog) => catalog.search(query)),
  hold_itinerary: (request) =>
    Effect.flatMap(ItineraryHoldGateway, (gateway) => gateway.hold(request)),
});

export const TravelPlannerPhase2 = Agent.define("travel-planner-phase-2", {
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
    tokenBudget: 2_048,
  }),
  description:
    "Build a review-only itinerary and require approval before creating a temporary hold.",
  metadata: { deploymentClass: "E", phase: "P2" },
});

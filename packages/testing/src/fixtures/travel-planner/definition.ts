import { Context, Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { Agent, AgentPolicy } from "@effect-agent/core";

export const AirportCode = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/testing/travel-planner/AirportCode"),
);
export type AirportCode = typeof AirportCode.Type;

export const QuoteId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/testing/travel-planner/QuoteId"),
);
export type QuoteId = typeof QuoteId.Type;

export const TripRequest = Schema.Struct({
  request: Schema.NonEmptyString,
  origin: AirportCode,
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
  budgetCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
});
export type TripRequest = typeof TripRequest.Type;

export const AvailabilityQuery = Schema.Struct({
  origin: AirportCode,
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
});
export type AvailabilityQuery = typeof AvailabilityQuery.Type;

export const AvailabilityOption = Schema.Struct({
  quoteId: QuoteId,
  flight: Schema.String,
  lodging: Schema.String,
  estimatedTotalCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
});
export type AvailabilityOption = typeof AvailabilityOption.Type;

export const Itinerary = Schema.Struct({
  title: Schema.String,
  route: Schema.String,
  dates: Schema.String,
  flight: Schema.String,
  lodging: Schema.String,
  estimatedTotalCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
  quoteId: QuoteId,
  assumptions: Schema.Array(Schema.String),
  unresolvedConstraints: Schema.Array(Schema.String),
  nextAction: Schema.Literal("review"),
});
export type Itinerary = typeof Itinerary.Type;

export const TravelPlan = Schema.Struct({
  itineraries: Schema.Array(Itinerary),
});
export type TravelPlan = typeof TravelPlan.Type;

export class AvailabilityUnavailable extends Schema.TaggedErrorClass<AvailabilityUnavailable>()(
  "AvailabilityUnavailable",
  {
    route: Schema.String,
    message: Schema.String,
  },
) {}

export class GuidanceFailure extends Schema.TaggedErrorClass<GuidanceFailure>()("GuidanceFailure", {
  message: Schema.String,
}) {}

export class AvailabilityCatalog extends Context.Service<
  AvailabilityCatalog,
  {
    readonly search: (
      query: AvailabilityQuery,
    ) => Effect.Effect<AvailabilityOption, AvailabilityUnavailable>;
  }
>()("@effect-agent/testing/travel-planner/AvailabilityCatalog") {}

export class TravelGuidance extends Context.Service<
  TravelGuidance,
  {
    readonly instructions: (input: TripRequest) => Effect.Effect<string, GuidanceFailure>;
  }
>()("@effect-agent/testing/travel-planner/TravelGuidance") {}

export const SearchAvailability = Tool.make("search_availability", {
  description: "Search the deterministic catalog for one read-only flight and lodging option.",
  parameters: AvailabilityQuery,
  success: AvailabilityOption,
  failure: AvailabilityUnavailable,
  failureMode: "error",
  dependencies: [AvailabilityCatalog],
});

export const TravelPlannerToolkit = Toolkit.make(SearchAvailability);

export const TravelPlannerToolkitLayer = TravelPlannerToolkit.toLayer({
  search_availability: (query) =>
    Effect.flatMap(AvailabilityCatalog, (catalog) => catalog.search(query)),
});

export const TravelPlanner = Agent.define("travel-planner", {
  input: TripRequest,
  output: TravelPlan,
  instructions: (input) =>
    Effect.flatMap(TravelGuidance, (guidance) => guidance.instructions(input)),
  toolkit: TravelPlannerToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  description: "Build one review-only itinerary from deterministic availability.",
  metadata: {
    deploymentClass: "E",
    phase: "P0",
  },
});

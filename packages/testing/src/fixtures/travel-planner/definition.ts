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

export const FlightQuery = Schema.Struct({
  origin: AirportCode,
  destination: AirportCode,
  departOn: Schema.String,
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
});
export type FlightQuery = typeof FlightQuery.Type;

export const LodgingQuery = Schema.Struct({
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
});
export type LodgingQuery = typeof LodgingQuery.Type;

export const ActivityQuery = Schema.Struct({
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
});
export type ActivityQuery = typeof ActivityQuery.Type;

export const FlightOption = Schema.Struct({
  quoteId: QuoteId,
  flight: Schema.String,
  estimatedCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
});
export type FlightOption = typeof FlightOption.Type;

export const LodgingOption = Schema.Struct({
  lodging: Schema.String,
  estimatedCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
});
export type LodgingOption = typeof LodgingOption.Type;

/** A successful empty activity search is distinct from supplier unavailability. */
export const ActivitySearchResult = Schema.Struct({
  activities: Schema.Array(Schema.String),
});
export type ActivitySearchResult = typeof ActivitySearchResult.Type;

export const Itinerary = Schema.Struct({
  title: Schema.String,
  route: Schema.String,
  dates: Schema.String,
  flight: Schema.String,
  lodging: Schema.String,
  activities: Schema.Array(Schema.String),
  estimatedTotalCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
  quoteId: QuoteId,
  assumptions: Schema.Array(Schema.String),
  unresolvedConstraints: Schema.Array(Schema.String),
  nextAction: Schema.Literal("review"),
});
export type Itinerary = typeof Itinerary.Type;

export const TravelPlan = Schema.Struct({ itineraries: Schema.Array(Itinerary) });
export type TravelPlan = typeof TravelPlan.Type;

const unavailableFields = { query: Schema.String, message: Schema.String };
export class FlightUnavailable extends Schema.TaggedErrorClass<FlightUnavailable>()(
  "FlightUnavailable",
  unavailableFields,
) {}
export class LodgingUnavailable extends Schema.TaggedErrorClass<LodgingUnavailable>()(
  "LodgingUnavailable",
  unavailableFields,
) {}
export class ActivityUnavailable extends Schema.TaggedErrorClass<ActivityUnavailable>()(
  "ActivityUnavailable",
  unavailableFields,
) {}
export class GuidanceFailure extends Schema.TaggedErrorClass<GuidanceFailure>()("GuidanceFailure", {
  message: Schema.String,
}) {}

export class FlightCatalog extends Context.Service<
  FlightCatalog,
  { readonly search: (query: FlightQuery) => Effect.Effect<FlightOption, FlightUnavailable> }
>()("@effect-agent/testing/travel-planner/FlightCatalog") {}
export class LodgingCatalog extends Context.Service<
  LodgingCatalog,
  { readonly search: (query: LodgingQuery) => Effect.Effect<LodgingOption, LodgingUnavailable> }
>()("@effect-agent/testing/travel-planner/LodgingCatalog") {}
export class ActivityCatalog extends Context.Service<
  ActivityCatalog,
  {
    readonly search: (
      query: ActivityQuery,
    ) => Effect.Effect<ActivitySearchResult, ActivityUnavailable>;
  }
>()("@effect-agent/testing/travel-planner/ActivityCatalog") {}
export class TravelGuidance extends Context.Service<
  TravelGuidance,
  { readonly instructions: (input: TripRequest) => Effect.Effect<string, GuidanceFailure> }
>()("@effect-agent/testing/travel-planner/TravelGuidance") {}

export const SearchFlights = Tool.make("search_flights", {
  parameters: FlightQuery,
  success: FlightOption,
  failure: FlightUnavailable,
  failureMode: "error",
  dependencies: [FlightCatalog],
});
export const SearchLodging = Tool.make("search_lodging", {
  parameters: LodgingQuery,
  success: LodgingOption,
  failure: LodgingUnavailable,
  failureMode: "error",
  dependencies: [LodgingCatalog],
});
export const SearchActivities = Tool.make("search_activities", {
  parameters: ActivityQuery,
  success: ActivitySearchResult,
  failure: ActivityUnavailable,
  failureMode: "error",
  dependencies: [ActivityCatalog],
});

export const TravelPlannerToolkit = Toolkit.make(SearchFlights, SearchLodging, SearchActivities);
export const TravelPlannerToolkitLayer = TravelPlannerToolkit.toLayer({
  search_flights: (query) => Effect.flatMap(FlightCatalog, (catalog) => catalog.search(query)),
  search_lodging: (query) => Effect.flatMap(LodgingCatalog, (catalog) => catalog.search(query)),
  search_activities: (query) => Effect.flatMap(ActivityCatalog, (catalog) => catalog.search(query)),
});

export const TravelPlanner = Agent.define("travel-planner", {
  input: TripRequest,
  output: TravelPlan,
  instructions: (input) =>
    Effect.flatMap(TravelGuidance, (guidance) => guidance.instructions(input)),
  toolkit: TravelPlannerToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 3,
  }),
  description: "Build one review-only itinerary from bounded parallel deterministic searches.",
  metadata: { deploymentClass: "E", phase: "P1" },
});

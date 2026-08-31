import { Agent, AgentPolicy } from "@effect-agent/core";
import { Context, Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

export const AirportCode = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/testing/travel-planner/AirportCode"),
);
export type AirportCode = typeof AirportCode.Type;

export const QuoteId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/testing/travel-planner/QuoteId"),
);
export type QuoteId = typeof QuoteId.Type;

export class TripRequest extends Schema.Class<TripRequest>("TripRequest")({
  request: Schema.NonEmptyString,
  origin: AirportCode,
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
  budgetCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
}) {}

export class FlightQuery extends Schema.Class<FlightQuery>("FlightQuery")({
  origin: AirportCode,
  destination: AirportCode,
  departOn: Schema.String,
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

export class LodgingQuery extends Schema.Class<LodgingQuery>("LodgingQuery")({
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

export class ActivityQuery extends Schema.Class<ActivityQuery>("ActivityQuery")({
  destination: AirportCode,
  departOn: Schema.String,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelers: Schema.Int.check(Schema.isGreaterThan(0)),
}) {}

export class FlightOption extends Schema.Class<FlightOption>("FlightOption")({
  quoteId: QuoteId,
  flight: Schema.String,
  estimatedCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
}) {}

export class LodgingOption extends Schema.Class<LodgingOption>("LodgingOption")({
  lodging: Schema.String,
  estimatedCents: Schema.Int.check(Schema.isGreaterThan(0)),
  currency: Schema.Literal("USD"),
}) {}

/** A successful empty activity search is distinct from supplier unavailability. */
export class ActivitySearchResult extends Schema.Class<ActivitySearchResult>(
  "ActivitySearchResult",
)({
  activities: Schema.Array(Schema.String),
}) {}

export class Itinerary extends Schema.Class<Itinerary>("Itinerary")({
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
}) {}

export class TravelPlan extends Schema.Class<TravelPlan>("TravelPlan")({
  itineraries: Schema.Array(Itinerary),
}) {}

const unavailableFields = { query: Schema.String, message: Schema.String };
export class FlightUnavailable extends Schema.TaggedError<FlightUnavailable>()(
  "FlightUnavailable",
  unavailableFields,
) {}
export class LodgingUnavailable extends Schema.TaggedError<LodgingUnavailable>()(
  "LodgingUnavailable",
  unavailableFields,
) {}
export class ActivityUnavailable extends Schema.TaggedError<ActivityUnavailable>()(
  "ActivityUnavailable",
  unavailableFields,
) {}
export class GuidanceFailure extends Schema.TaggedError<GuidanceFailure>()("GuidanceFailure", {
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

export const TravelPlanner = Agent.make("travel-planner", {
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

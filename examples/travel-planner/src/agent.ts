import { ToolExecutionClass } from "@yielded/agent/durable-step";
import { Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/ai";

import {
  PlannerError,
  PublishTripRequest,
  PublishedSite,
  SaveTripRequest,
  Trip,
  TripId,
  TripSiteStore,
} from "./domain.ts";
import { PlannerResponse } from "./response.ts";
import { trackTool } from "./server/progress.ts";
import { publishTrip, TripRepository } from "./server/trips.ts";
import { TravelContent } from "./travel-content.ts";

/** Display is a native read-only tool; the canonical settled result owns the card data. */
export const ShowTravelOptions = Tool.make("show_travel_options", {
  description:
    "Show researched stays, flights, places, or a proposed itinerary as travel cards in this conversation. Use real source URLs and only photo URLs returned by inspected pages. Leave unverified price/date fields null and explain uncertainty in notes. Never invent availability, amenities, listings, images, or bookings. This displays options without saving or booking them.",
  parameters: TravelContent,
  success: TravelContent,
})
  .annotate(Tool.Readonly, true)
  .annotate(ToolExecutionClass, "readonly");

const DisplayTools = Toolkit.make(ShowTravelOptions);

export const DeliverResponse = Tool.make("deliver_response", {
  description:
    "Deliver your final reply. Put brief conversational context in message. When recommending or comparing stays, flights, places, or an itinerary, include the options as structured content, not a Markdown list. Use content null only for a reply without travel options or when the same cards were already shown this turn. Source photo URLs from inspected pages; missing photos are an empty array, not a reason to omit cards. Unknown prices and availability stay unverified. This only displays a reply; it does not save or book anything.",
  parameters: PlannerResponse,
  success: PlannerResponse,
})
  .annotate(Tool.Readonly, true)
  .annotate(ToolExecutionClass, "readonly");

const ResponseTools = Toolkit.make(DeliverResponse);

export const ResponseToolsLive = ResponseTools.toLayer({
  deliver_response: (response) => Effect.succeed(response),
});

export const DisplayToolsLive = DisplayTools.toLayer({
  show_travel_options: (content, context) =>
    trackTool(
      context.toolCallId ?? "show_travel_options",
      "Showing travel options",
      Effect.succeed(content),
    ),
});

export const TripTools = Toolkit.make(
  Tool.make("list_trips", {
    description:
      "Read the saved trip in this conversation, including its current ID and revision. Returns an empty list for a new conversation. Use this before creating a draft or saving an update, including when research reports arrive without a selectedTripId.",
    success: Schema.Array(Trip),
    failure: PlannerError,
    dependencies: [TripRepository],
  }),
  Tool.make("get_trip", {
    description: "Read a saved trip and its current revision before changing it.",
    parameters: Schema.Struct({ tripId: TripId }),
    success: Trip,
    failure: PlannerError,
    failureMode: "return",
    dependencies: [TripRepository],
  }),
  Tool.make("save_trip", {
    description:
      "Save this conversation's trip. First use list_trips to get its ID and current revision; only use null tripId/expectedRevision when that list is empty. Other conversations' trips cannot be changed here. Preserve details the user has not changed. Dates may be null. Keep titles and activities under 240 characters; notes can include source links and descriptions up to 4000 characters each. Never invent confirmed prices or bookings. A rejection is not a successful save: refresh the current trip before correcting the request. After a storage error, the write may have committed; do not repeat it without reading the saved state.",
    parameters: SaveTripRequest,
    success: Trip,
    failure: PlannerError,
    failureMode: "return",
    dependencies: [TripRepository],
  }),
  Tool.make("publish_trip_site", {
    description:
      "Publish the user's saved trip as a standalone site for anyone with planner access and the link, only when the user explicitly asks to publish or share it. The exact saved revision and all notes are shared. Returns an immutable link.",
    parameters: PublishTripRequest,
    success: PublishedSite,
    failure: PlannerError,
    dependencies: [TripRepository, TripSiteStore],
  }),
);

export const TripToolsLive = (conversationId: string) =>
  TripTools.toLayer({
    list_trips: (_, context) =>
      trackTool(
        context.toolCallId ?? "list_trips",
        "Reading saved trips",
        Effect.gen(function* () {
          const trips = yield* TripRepository;

          return yield* Effect.filter(yield* trips.list, (trip) =>
            trips.conversationId(trip.id).pipe(Effect.map((id) => id === conversationId)),
          );
        }),
      ),
    get_trip: ({ tripId }, context) =>
      trackTool(
        context.toolCallId ?? "get_trip",
        "Reading trip details",
        Effect.flatMap(TripRepository, (trips) => trips.get(tripId)),
      ),
    save_trip: (request, context) =>
      trackTool(
        context.toolCallId ?? "save_trip",
        "Saving your trip",
        Effect.flatMap(TripRepository, (trips) => trips.save(request, conversationId)),
      ),
    publish_trip_site: (request, context) =>
      trackTool(
        context.toolCallId ?? "publish_trip_site",
        "Creating your trip website",
        publishTrip(request),
      ),
  }).pipe(Layer.merge(DisplayToolsLive), Layer.merge(ResponseToolsLive));

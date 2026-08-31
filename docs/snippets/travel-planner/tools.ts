import { Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

const SearchActivities = Tool.make("search_activities", {
  description: "Find activities in a city.",
  parameters: Schema.Struct({ city: Schema.String }),
  success: Schema.Array(Schema.String),
});

export const TravelTools = Toolkit.make(SearchActivities);

// Sample data. A real handler can query your database or a travel API.
const activities = [
  { city: "Lisbon", name: "Riverside walk" },
  { city: "Lisbon", name: "Food market" },
  { city: "Lisbon", name: "City museum" },
];

export const TravelToolsLive = TravelTools.toLayer({
  search_activities: ({ city }) =>
    Effect.succeed(activities.filter((a) => a.city === city).map((a) => a.name)),
});

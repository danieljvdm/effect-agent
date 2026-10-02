import { AgentRuntime, AgentUpdates } from "@yielded/agent";
import { Effect, Stream } from "effect";

import { HotelResearcher } from "./background-updates.ts";

const updates = AgentRuntime.stream(HotelResearcher, {
  city: "Johannesburg",
  area: "Rosebank",
  sources: [],
}).pipe(
  Stream.filter((event) => event._tag === "AgentUpdateEmitted"),
  Stream.map((event) => event.update),
);

// Provide the model and runtime services from the getting-started guide.
export const observe = AgentUpdates.observe(HotelResearcher, updates).pipe(
  Stream.runForEach((finding) => Effect.log(finding.area, finding.finding)),
);

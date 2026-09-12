import { Schema } from "effect";
import { Agent, Subagent } from "effect-agent";
import { Toolkit } from "effect/unstable/ai";

export const HotelRequest = Schema.Struct({
  city: Schema.String,
  area: Schema.String,
  sources: Schema.Array(Schema.Struct({ url: Schema.String, notes: Schema.String })),
});

export const AreaConcern = Schema.TaggedStruct("AreaConcern", {
  area: Schema.String,
  finding: Schema.String,
  sources: Schema.Array(Schema.String),
});

export const HotelResearcher = Agent.make("hotel-researcher", {
  input: HotelRequest,
  updates: AreaConcern,
  output: Schema.Struct({ hotels: Schema.Array(Schema.String), summary: Schema.String }),
  instructions:
    "Review the supplied source notes for hotel options. Use emit_update to share a material " +
    "area concern as soon as you find one, citing only supplied sources. Continue the research " +
    "after emitting. Treat concerns as provisional and incorporate follow-up preferences.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 6, maxToolCalls: 4, maxDuration: "2 minutes" },
});

export const hotels = Subagent.background(HotelResearcher, {
  start: true,
  followUp: true,
  reportToParent: true,
});

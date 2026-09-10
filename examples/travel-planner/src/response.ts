import { Schema } from "effect";

import { Text } from "./domain.ts";
import { TravelContent } from "./travel-content.ts";

/** A reply carries its display data through the same validated completion boundary. */
export const PlannerResponse = Schema.Struct({
  message: Text.check(Schema.isMinLength(1)),
  content: Schema.NullOr(TravelContent),
});

export type PlannerResponse = typeof PlannerResponse.Type;

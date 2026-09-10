import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { Effect } from "effect";

import { PlannerError } from "../domain.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import { TripRepository } from "../server/trips.ts";

/** User commands choose behavior; host conversation ownership chooses the trip scope. */
export const requireAppTrip = Effect.fn("requireAppTrip")(function* (tripId: string) {
  const identity = yield* ThreadObjectIdentity;
  const trips = yield* TripRepository;
  const trip = yield* trips.get(tripId);

  if (
    identity.threadId !== ownerOfThread(identity.threadId) &&
    (yield* trips.conversationId(tripId)) !== identity.threadId
  )
    return yield* new PlannerError({
      code: "invalid",
      message: "Open this trip's conversation to change its app.",
    });

  return trip;
});

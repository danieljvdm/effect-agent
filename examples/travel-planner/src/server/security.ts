import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { Effect, Layer, Schema } from "effect";

import { PlannerInput, PublishTripRequest } from "../domain.ts";

const PublicationCommand = Schema.String.check(
  Schema.isPattern(
    /^(?:please\s+)?(?:publish|share)(?:\s+(?:this|my|the))?\s+trip(?:\s+(?:site|website))?[.!]?$/i,
  ),
);

/** Initial chat approval grammar is intentionally complete, e.g. "Publish this trip". */
export const requestsPublication = (message: string): boolean =>
  Schema.is(PublicationCommand)(message.trim());

/** The grant comes from the admitted user command, never model prose or page contents. */
export const publicationAuthorization = RunToolAuthorization.of({
  authorize: ({ input, call }) => {
    if (call.toolName !== "publish_trip_site") return Effect.succeed({ _tag: "allowed" });
    const source = Schema.decodeUnknownOption(PlannerInput)(input);
    const target = Schema.decodeUnknownOption(PublishTripRequest)(call.parameters);

    return Effect.succeed(
      source._tag === "Some" &&
        target._tag === "Some" &&
        source.value.publication !== null &&
        source.value.selectedTripId === target.value.tripId &&
        source.value.publication.tripId === target.value.tripId &&
        source.value.publication.expectedRevision === target.value.expectedRevision
        ? { _tag: "allowed" }
        : {
            _tag: "denied",
            reason: "Ask the user to select this trip and explicitly publish its current revision.",
          },
    );
  },
});

export const PublicationAuthorizationLive = Layer.succeed(
  RunToolAuthorization,
  publicationAuthorization,
);

import { Effect, Schema } from "effect";

import { adminEmail } from "../access-domain.ts";
import { PlannerError, type PlannerSnapshot } from "../domain.ts";

export const storageOwner = "travel-planner-owner-v1";

/** Keep Daniel's existing Object and history; other verified emails own disjoint namespaces. */
export const plannerOwner = Effect.fn("plannerOwner")(function* (email: string) {
  if (email === adminEmail) return storageOwner;

  const digest = yield* Effect.promise(() =>
    crypto.subtle.digest("SHA-256", new TextEncoder().encode(email)),
  );

  return `member-${Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
});

export const ownerOfThread = (threadId: string): string =>
  /^member-[a-f0-9]{64}(?=--|$)/.exec(threadId)?.[0] ?? storageOwner;

export const privateConversation = (owner: string, conversationId: string) =>
  owner === storageOwner
    ? Schema.decodeUnknownEffect(
        Schema.String.check(Schema.makeFilter((id) => !id.startsWith("member-"))),
      )(conversationId).pipe(
        Effect.mapError(
          () => new PlannerError({ code: "invalid", message: "Invalid conversation." }),
        ),
      )
    : Effect.succeed(`${owner}--${conversationId}`);

/** Namespace addresses stay server-owned; the browser sees only its original conversation IDs. */
export const publicSnapshot = (owner: string, snapshot: PlannerSnapshot): PlannerSnapshot => {
  const unqualify = (id: string) => (owner === storageOwner ? id : id.slice(owner.length + 2));

  return {
    ...snapshot,
    conversationId: snapshot.conversationId === null ? null : unqualify(snapshot.conversationId),
    conversations: snapshot.conversations?.map((conversation) => ({
      ...conversation,
      conversationId: unqualify(conversation.conversationId),
    })),
    trips: snapshot.trips.map((trip) => ({
      ...trip,
      conversationId: unqualify(trip.conversationId),
    })),
  };
};

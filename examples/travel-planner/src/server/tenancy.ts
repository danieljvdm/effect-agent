import { Effect, Schema } from "effect";

import { AccountId } from "../auth/account.ts";
import { PlannerError, type PlannerSnapshot } from "../domain.ts";

export const StorageOwner = Schema.String.check(
  Schema.isPattern(/^account-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/),
);

export const plannerOwner = (subjectId: string) =>
  Schema.decodeUnknownEffect(AccountId)(subjectId).pipe(
    Effect.map((id) => `account-${id}`),
    Effect.mapError(() => new PlannerError({ code: "invalid", message: "Invalid account." })),
  );

/** Internal object addresses must have a server-assigned owner; corrupt addresses fail closed. */
export const ownerOfThread = (threadId: string): string =>
  Schema.decodeUnknownSync(StorageOwner)(threadId.split("--", 1)[0]);

export const privateConversation = (owner: string, conversationId: string) =>
  Schema.decodeUnknownEffect(StorageOwner)(owner).pipe(
    Effect.flatMap((owner) =>
      Schema.decodeUnknownEffect(
        Schema.String.check(Schema.makeFilter((id) => !id.includes("--"))),
      )(conversationId).pipe(Effect.map((id) => `${owner}--${id}`)),
    ),
    Effect.mapError(() => new PlannerError({ code: "invalid", message: "Invalid conversation." })),
  );

/** Namespace addresses stay server-owned; the browser sees only its original conversation IDs. */
export const publicSnapshot = (owner: string, snapshot: PlannerSnapshot): PlannerSnapshot => {
  const unqualify = (id: string) => id.slice(owner.length + 2);

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

import type { ThreadId } from "@effect-agent/core/Identifiers";
import { type MessageAddress, MessagingError } from "@effect-agent/core/Messaging";
import type { Principal } from "@effect-agent/core/Receipt";
import { Context, type Effect } from "effect";

/** Separate context, history, send and management grants; incoming messages grant none of them. */
export interface PeerAuthorizationRequest {
  readonly source: MessageAddress;
  readonly principal: Principal;
  readonly operation: MessagingError["operation"];
  readonly access: "context" | "read" | "send" | "control";
  readonly peerName?: string;
  readonly destination?: MessageAddress;
}

export const PeerAuthorizer = Context.Reference<{
  /** Return the stable delivery principal for sends. It is retained with the frozen envelope. */
  readonly authorize: (
    request: PeerAuthorizationRequest,
  ) => Effect.Effect<Principal, MessagingError>;
}>("@effect-agent/thread/PeerAuthorizer", {
  defaultValue: () => ({
    authorize: (request) => MessagingError.make({ operation: request.operation, reason: "denied" }),
  }),
});

/** Application-owned routing; the target Agent is resolved from the existing exact registrations. */
export const PeerRoutes = Context.Reference<{
  readonly resolve: (request: {
    readonly source: MessageAddress;
    readonly principal: Principal;
    readonly peerName: string;
    readonly targetAgentId: MessageAddress["agentId"];
  }) => Effect.Effect<ThreadId, MessagingError>;
}>("@effect-agent/thread/PeerRoutes", {
  defaultValue: () => ({
    resolve: () => MessagingError.make({ operation: "send", reason: "route-unavailable" }),
  }),
});

/** Retry windows are finite; exhaustion parks the retained envelope for explicit authorized retry. */
export const PeerDeliveryLifetime = Context.Reference<number>(
  "@effect-agent/thread/PeerDeliveryLifetime",
  { defaultValue: () => 86_400_000 },
);

/** Bound canonical send intents across all peers and principals in one source Thread. */
export const PeerMessageCapacity = Context.Reference<number>(
  "@effect-agent/thread/PeerMessageCapacity",
  { defaultValue: () => 256 },
);

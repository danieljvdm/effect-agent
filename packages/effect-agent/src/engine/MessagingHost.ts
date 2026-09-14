import { Context, type Effect } from "effect";

import type { AnyDefinition } from "../core/Agent.ts";
import type { ThreadId } from "../core/Identifiers.ts";
import {
  type InboxPage,
  type MessageAddress,
  type MessageRef,
  type MessageStatus,
  MessagingError,
} from "../core/Messaging.ts";
import type { IdempotencyKey, Principal } from "../core/Receipt.ts";
import type { WorkerSource } from "../core/Worker.ts";

export interface PeerTarget {
  readonly name: string;
  readonly target: AnyDefinition;
}

/** A fixed declaration chooses the route. Caller input never selects a destination Thread. */
export interface SendPeerMessage extends PeerTarget {
  readonly encodedInput: unknown;
  readonly idempotencyKey: IdempotencyKey;
  /** Correlation on sends; replies additionally resolve and authorize the recorded return address. */
  readonly inReplyTo?: MessageRef;
}

/** Trusted per-invocation facet. It carries caller authority without exposing it in Tool schemas. */
export class MessagingHost extends Context.Service<
  MessagingHost,
  {
    readonly context: Effect.Effect<WorkerSource, MessagingError>;
    readonly send: (request: SendPeerMessage) => Effect.Effect<MessageStatus, MessagingError>;
    readonly reply: (
      request: SendPeerMessage & { readonly inReplyTo: MessageRef },
    ) => Effect.Effect<MessageStatus, MessagingError>;
    readonly inbox: (
      request: PeerTarget & { readonly after?: number; readonly limit: number },
    ) => Effect.Effect<InboxPage, MessagingError>;
    readonly inspect: (
      request: PeerTarget & { readonly message: MessageRef },
    ) => Effect.Effect<MessageStatus, MessagingError>;
    readonly retry: (
      request: PeerTarget & { readonly message: MessageRef },
    ) => Effect.Effect<MessageStatus, MessagingError>;
  }
>()("@effect-agent/engine/MessagingHost") {
  static readonly unavailable: MessagingHost["Service"] = {
    context: MessagingError.make({ operation: "context", reason: "unavailable" }),
    send: () => MessagingError.make({ operation: "send", reason: "unavailable" }),
    reply: () => MessagingError.make({ operation: "reply", reason: "unavailable" }),
    inbox: () => MessagingError.make({ operation: "inbox", reason: "unavailable" }),
    inspect: () => MessagingError.make({ operation: "inspect", reason: "unavailable" }),
    retry: () => MessagingError.make({ operation: "retry", reason: "unavailable" }),
  };

  /** Runtime-owned per-call binding. Unconfigured Runs deny peer operations. */
  static readonly forTool = Context.Reference<
    (source: Extract<WorkerSource, { readonly _tag: "tool" }>) => MessagingHost["Service"]
  >("@effect-agent/engine/MessagingHost/forTool", {
    defaultValue: () => () => MessagingHost.unavailable,
  });
}

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

import type { AnyDefinition } from "@effect-agent/core/Agent";
import {
  type InboxPage,
  type MessageRef,
  type MessageStatus,
  MessagingError,
} from "@effect-agent/core/Messaging";
import type { IdempotencyKey } from "@effect-agent/core/Receipt";
import type { WorkerSource } from "@effect-agent/core/Worker";
import { Context, type Effect } from "effect";

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
}

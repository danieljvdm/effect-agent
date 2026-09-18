import { Schema } from "effect";

import { AgentId, ThreadId } from "./Identifiers.ts";
import { MessageRef } from "./internal/message-status.ts";
import { FrameworkMessage } from "./Worker.ts";

export { MessageRef, MessageStatus } from "./internal/message-status.ts";

/** Application-chosen fixed route name; it never authorizes a destination by itself. */
export const PeerName = Schema.NonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[a-zA-Z0-9_-]+$/u),
);

export type PeerName = typeof PeerName.Type;

export const MessageAddress = Schema.Struct({ threadId: ThreadId, agentId: AgentId });
export type MessageAddress = typeof MessageAddress.Type;

/** Host-authenticated provenance stored separately from destination-owned application input. */
export const MessageAdmission = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  message: MessageRef,
  peerName: PeerName,
  sender: MessageAddress,
  returnAddress: MessageAddress,
  inReplyTo: Schema.optionalKey(MessageRef),
}).check(
  Schema.makeFilter(
    (value) =>
      value.message.ownerThreadId === value.sender.threadId &&
      value.sender.threadId === value.returnAddress.threadId &&
      value.sender.agentId === value.returnAddress.agentId,
  ),
);

export type MessageAdmission = typeof MessageAdmission.Type;

export const InboxEntry = Schema.Struct({
  sequence: Schema.Natural.check(Schema.isGreaterThan(0)),
  admission: MessageAdmission,
});

export type InboxEntry = typeof InboxEntry.Type;

export const InboxPage = Schema.Struct({
  items: Schema.Array(InboxEntry).check(Schema.isMaxLength(100)),
  next: Schema.NullOr(Schema.Natural),
});

export type InboxPage = typeof InboxPage.Type;

export class MessagingError extends Schema.TaggedError<MessagingError>()("MessagingError", {
  operation: Schema.Literals(["context", "send", "reply", "inbox", "inspect", "retry"]),
  reason: Schema.Literals([
    "denied",
    "unavailable",
    "route-unavailable",
    "binding-mismatch",
    "invalid-input",
    "invalid-reference",
    "conflict",
    "capacity",
    "not-found",
    "storage",
    "corrupt",
  ]),
}) {}

/** Canonical input provenance: peer input or a framework-owned completion message. */
export const InputMessage = Schema.Union([MessageAdmission, FrameworkMessage]);
export type InputMessage = typeof InputMessage.Type;

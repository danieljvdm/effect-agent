import { Schema } from "effect";

import { SettlementId, ThreadId } from "../Identifiers.ts";
import { IdempotencyKey, Receipt } from "../Receipt.ts";

/** A retained outbound delivery identity, not a destination Receipt or access grant. */
export const MessageRef = Schema.Struct({ ownerThreadId: ThreadId, messageId: IdempotencyKey });
export type MessageRef = typeof MessageRef.Type;

const Reason = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const identity = { message: MessageRef };

/**
 * Source-owned delivery evidence. Pending confirms retention only; accepted supplies the
 * destination Receipt, and processed supplies its canonical settlement. Neither retention nor
 * acceptance proves execution. Refused is a definite rejection; parked stops automatic retry
 * while retaining the same identity and any acceptance evidence. Inspect instead of resending.
 */
export const MessageStatus = Schema.Union([
  Schema.Struct({
    ...identity,
    status: Schema.Literal("pending"),
    receipt: Schema.Null,
    settlement: Schema.Null,
    reason: Schema.NullOr(Reason),
  }),
  Schema.Struct({
    ...identity,
    status: Schema.Literal("accepted"),
    receipt: Receipt,
    settlement: Schema.Null,
    reason: Schema.NullOr(Reason),
  }),
  Schema.Struct({
    ...identity,
    status: Schema.Literal("processed"),
    receipt: Receipt,
    settlement: Schema.Struct({
      settlementId: SettlementId,
      outcome: Schema.Literals(["completed", "failed", "aborted"]),
    }),
    reason: Schema.Null,
  }),
  Schema.Struct({
    ...identity,
    status: Schema.Literal("refused"),
    receipt: Schema.Null,
    settlement: Schema.Null,
    reason: Reason,
  }),
  Schema.Struct({
    ...identity,
    status: Schema.Literal("parked"),
    receipt: Schema.NullOr(Receipt),
    settlement: Schema.Null,
    reason: Reason,
  }),
]);

export type MessageStatus = typeof MessageStatus.Type;

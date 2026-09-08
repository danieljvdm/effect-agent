import { Schema } from "effect";

import { ReceiptId, SubmissionId, ThreadId } from "./Identifiers.ts";

// Keep the established brand and Schema identities when sharing these values inward.
/** Caller-supplied deduplication key, scoped to one Thread and authenticated principal. */
export const IdempotencyKey = Schema.NonEmptyString.check(Schema.isMaxLength(256)).pipe(
  Schema.brand("@effect-agent/thread/IdempotencyKey"),
);

export type IdempotencyKey = typeof IdempotencyKey.Type;

/** Stable host-authenticated admission principal; the established durable brand is preserved. */
export const Principal = Schema.NonEmptyString.check(Schema.isMaxLength(256)).pipe(
  Schema.brand("@effect-agent/thread/Principal"),
);

export type Principal = typeof Principal.Type;

/** Thread-local FIFO position allocated once at admission. */
export const QueueSequence = Schema.Natural.pipe(
  Schema.brand("@effect-agent/thread/QueueSequence"),
);

export type QueueSequence = typeof QueueSequence.Type;

/**
 * One accepted input whose admission, Thread materialization and readiness are committed.
 * A Receipt identifies an observation target; possession grants no authorization.
 */
export class Receipt extends Schema.Class<Receipt>("@effect-agent/thread/Receipt")({
  receiptId: ReceiptId,
  submissionId: SubmissionId,
  threadId: ThreadId,
  queueSequence: QueueSequence,
}) {}

/** The input joined this host Submission; cancelling it must never silently cancel the host. */
export class JoinedToHost extends Schema.TaggedError<JoinedToHost>()("JoinedToHost", {
  submissionId: SubmissionId,
  hostSubmissionId: SubmissionId,
}) {}

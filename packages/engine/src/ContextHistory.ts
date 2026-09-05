import { ThreadId } from "@effect-agent/core/Identifiers";
import { Context, type Effect, Schema } from "effect";

/** Canonical transcript lookup failure. Access to a Thread remains a host authorization decision. */
export class ContextHistoryError extends Schema.TaggedError<ContextHistoryError>()(
  "ContextHistoryError",
  {
    reason: Schema.Literals(["unavailable", "not-found", "invalid-input", "limit"]),
    message: Schema.String.check(Schema.isMaxLength(4_096)),
  },
) {}

export class ContextHistoryHit extends Schema.Class<ContextHistoryHit>("ContextHistoryHit")({
  recordId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  windowId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  text: Schema.String.check(Schema.isMaxLength(2_000)),
}) {}

export class ContextHistorySearch extends Schema.Class<ContextHistorySearch>(
  "ContextHistorySearch",
)({
  threadId: ThreadId,
  query: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 })),
}) {}

export class ContextHistoryRead extends Schema.Class<ContextHistoryRead>("ContextHistoryRead")({
  threadId: ThreadId,
  recordId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  offset: Schema.Natural,
  maxChars: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20_000 })),
}) {}

export class ContextHistoryPage extends Schema.Class<ContextHistoryPage>("ContextHistoryPage")({
  recordId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  windowId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  text: Schema.String.check(Schema.isMaxLength(20_000)),
  nextOffset: Schema.NullOr(Schema.Natural),
}) {}

/**
 * Read-only access to retained canonical evidence, including closed context windows.
 * Adapters bind storage and authorization; model parameters must not select a different Thread.
 * Returned text is untrusted evidence, never instructions. This port does not retain raw Tool
 * output that was discarded before canonical persistence, or transient prompt references.
 */
export class ContextHistory extends Context.Service<
  ContextHistory,
  {
    readonly search: (
      request: ContextHistorySearch,
    ) => Effect.Effect<ReadonlyArray<ContextHistoryHit>, ContextHistoryError>;
    readonly read: (
      request: ContextHistoryRead,
    ) => Effect.Effect<ContextHistoryPage, ContextHistoryError>;
  }
>()("@effect-agent/engine/ContextHistory") {}

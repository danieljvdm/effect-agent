import * as Schema from "effect/Schema";

/** Stable identity of an agent definition, distinct from a display name. */
export const AgentId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/AgentId"));
export type AgentId = typeof AgentId.Type;

/** Identity shared by runs that participate in one thread history. */
export const ThreadId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/ThreadId"));
export type ThreadId = typeof ThreadId.Type;

/** Identity of one accepted input submission. */
export const SubmissionId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/core/SubmissionId"),
);
export type SubmissionId = typeof SubmissionId.Type;

/** Durable identity returned once ledger admission and Thread readiness are committed. */
export const ReceiptId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/ReceiptId"));
export type ReceiptId = typeof ReceiptId.Type;

/** Identity of the single durable terminal outcome owed to one accepted Submission. */
export const SettlementId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/core/SettlementId"),
);
export type SettlementId = typeof SettlementId.Type;

/** Identity of one execution ownership period for accepted work. */
export const AttemptId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/AttemptId"));
export type AttemptId = typeof AttemptId.Type;

/** Identity of one logical agent run. */
export const RunId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/RunId"));
export type RunId = typeof RunId.Type;

/** Identity of one model turn within a run. */
export const TurnId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/TurnId"));
export type TurnId = typeof TurnId.Type;

/** Identity used to correlate a Tool Call with progress and its terminal outcome. */
export const ToolCallId = Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/core/ToolCallId"));
export type ToolCallId = typeof ToolCallId.Type;

/** Stable identity of one configured Subagent capability. */
export const DelegationId = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/core/DelegationId"),
);
export type DelegationId = typeof DelegationId.Type;

/** Opaque position in a semantic event sequence. */
export const EventOffset = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/core/EventOffset"),
);
export type EventOffset = typeof EventOffset.Type;

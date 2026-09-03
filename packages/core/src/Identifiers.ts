import * as Schema from "effect/Schema";

const identifier = <const Name extends string>(name: Name) =>
  Schema.NonEmptyString.pipe(Schema.brand(`@effect-agent/core/${name}`));

/** Stable identity of an agent definition, distinct from a display name. */
export const AgentId = identifier("AgentId");
export type AgentId = typeof AgentId.Type;

/** Identity shared by runs that participate in one thread history. */
export const ThreadId = identifier("ThreadId");
export type ThreadId = typeof ThreadId.Type;

/** Identity of one accepted input submission. */
export const SubmissionId = identifier("SubmissionId");
export type SubmissionId = typeof SubmissionId.Type;

/** Durable identity returned once ledger admission and Thread readiness are committed. */
export const ReceiptId = identifier("ReceiptId");
export type ReceiptId = typeof ReceiptId.Type;

/** Identity of the single durable terminal outcome owed to one accepted Submission. */
export const SettlementId = identifier("SettlementId");
export type SettlementId = typeof SettlementId.Type;

/** Identity of one execution ownership period for accepted work. */
export const AttemptId = identifier("AttemptId");
export type AttemptId = typeof AttemptId.Type;

/** Identity of one logical agent run. */
export const RunId = identifier("RunId");
export type RunId = typeof RunId.Type;

/** Identity of one model turn within a run. */
export const TurnId = identifier("TurnId");
export type TurnId = typeof TurnId.Type;

/** Identity used to correlate a Tool Call with progress and its terminal outcome. */
export const ToolCallId = identifier("ToolCallId");
export type ToolCallId = typeof ToolCallId.Type;

/** Stable identity of one immutable Delegation Definition. */
export const DelegationId = identifier("DelegationId");
export type DelegationId = typeof DelegationId.Type;

/** Opaque position in a semantic event sequence. */
export const EventOffset = identifier("EventOffset");
export type EventOffset = typeof EventOffset.Type;

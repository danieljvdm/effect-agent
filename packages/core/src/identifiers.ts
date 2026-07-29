import { Schema } from "effect";

const identifier = <const Name extends string>(name: Name) =>
  Schema.NonEmptyString.pipe(Schema.brand(`@effect-agent/core/${name}`));

/** Stable identity of an agent definition, distinct from a display name. */
export const AgentId = identifier("AgentId");
export type AgentId = typeof AgentId.Type;

/** Identity shared by runs that participate in one conversation history. */
export const ConversationId = identifier("ConversationId");
export type ConversationId = typeof ConversationId.Type;

/** Identity of one accepted input submission. */
export const SubmissionId = identifier("SubmissionId");
export type SubmissionId = typeof SubmissionId.Type;

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

/** Opaque position in a semantic event sequence. */
export const EventOffset = identifier("EventOffset");
export type EventOffset = typeof EventOffset.Type;

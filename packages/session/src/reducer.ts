import { ConversationId, RunId } from "@effect-agent/core";
import { Schema } from "effect";

import { EMPTY_TAIL_DIGEST } from "./digest.ts";
import { CanonicalRecordEnvelope, Digest } from "./records.ts";

/**
 * Rebuildable Phase 3 projection. It contains only canonical values and can be discarded and
 * reconstructed from the record stream at any time.
 */
export class ConversationProjection extends Schema.Class<ConversationProjection>(
  "@effect-agent/session/ConversationProjection",
)({
  conversationId: ConversationId,
  throughSequence: Schema.Natural,
  tailDigest: Digest,
  inputs: Schema.Array(Schema.Json),
  modelOutputs: Schema.Array(Schema.Json),
  completedRuns: Schema.Array(RunId),
  failedRuns: Schema.Array(RunId),
}) {}

export const initialConversationProjection = (
  conversationId: ConversationId,
): ConversationProjection =>
  ConversationProjection.make({
    conversationId,
    throughSequence: 0,
    tailDigest: EMPTY_TAIL_DIGEST,
    inputs: [],
    modelOutputs: [],
    completedRuns: [],
    failedRuns: [],
  });

/** Pure one-record Conversation transition. */
export const reduceConversationRecord = (
  projection: ConversationProjection,
  envelope: CanonicalRecordEnvelope,
  tailDigest: Digest = projection.tailDigest,
): ConversationProjection => {
  const payload = envelope.record.payload;
  const inputs =
    payload._tag === "UserInputRecorded"
      ? [...projection.inputs, payload.input]
      : projection.inputs;
  const modelOutputs =
    payload._tag === "ModelCompleted"
      ? [...projection.modelOutputs, payload.output]
      : projection.modelOutputs;
  const completedRuns =
    payload._tag === "RunCompleted"
      ? [...projection.completedRuns, payload.runId]
      : projection.completedRuns;
  const failedRuns =
    payload._tag === "RunFailed"
      ? [...projection.failedRuns, payload.runId]
      : projection.failedRuns;

  return ConversationProjection.make({
    conversationId: projection.conversationId,
    throughSequence: envelope.sequence,
    tailDigest,
    inputs,
    modelOutputs,
    completedRuns,
    failedRuns,
  });
};

/** Pure full replay from the canonical beginning. */
export const replayConversation = (
  conversationId: ConversationId,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tailDigest: Digest = EMPTY_TAIL_DIGEST,
): ConversationProjection =>
  replayConversationFromCheckpoint(
    initialConversationProjection(conversationId),
    records,
    tailDigest,
  );

/**
 * Pure checkpoint replay. A validated checkpoint projection and its canonical tail produce the
 * same reducer path as full replay for every later record.
 */
export const replayConversationFromCheckpoint = (
  checkpoint: ConversationProjection,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tailDigest: Digest = checkpoint.tailDigest,
): ConversationProjection => {
  let projection = checkpoint;
  for (const record of records) {
    projection = reduceConversationRecord(projection, record, tailDigest);
  }
  return projection;
};

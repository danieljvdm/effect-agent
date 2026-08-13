import { ConversationId, RunId } from "@effect-agent/core";
import { Schema } from "effect";

import { EMPTY_TAIL_DIGEST } from "./digest.ts";
import {
  AbortRequested,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  Digest,
  PersistedJson,
  SubmissionSettled,
} from "./records.ts";

/**
 * Rebuildable canonical projection. It contains only canonical values and can be discarded and
 * reconstructed from the record stream at any time.
 *
 * Phase 4 adds `settlements` and `abortRequests`. A Phase 3 checkpoint whose persisted state
 * lacks these fields fails to decode against this schema; the checkpoint is rejected and the
 * projection is rebuilt from canonical records (documented disposable-checkpoint behavior,
 * STORE-007/STORE-008). `ModelResponseRecorded` advances `throughSequence` without dedicated
 * projection state: Prompt reconstruction reads canonical records directly.
 */
export class ConversationProjection extends Schema.Class<ConversationProjection>(
  "@effect-agent/session/ConversationProjection",
)({
  conversationId: ConversationId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
  inputs: Schema.Array(PersistedJson),
  modelOutputs: Schema.Array(PersistedJson),
  completedRuns: Schema.Array(RunId),
  failedRuns: Schema.Array(RunId),
  settlements: Schema.Array(SubmissionSettled),
  abortRequests: Schema.Array(AbortRequested),
}) {}

export const initialConversationProjection = (
  conversationId: ConversationId,
): ConversationProjection =>
  ConversationProjection.make({
    conversationId,
    throughSequence: Schema.decodeSync(CanonicalSequence)(0),
    tailDigest: EMPTY_TAIL_DIGEST,
    inputs: [],
    modelOutputs: [],
    completedRuns: [],
    failedRuns: [],
    settlements: [],
    abortRequests: [],
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
  const settlements =
    payload._tag === "SubmissionSettled"
      ? [...projection.settlements, payload]
      : projection.settlements;
  const abortRequests =
    payload._tag === "AbortRequested"
      ? [...projection.abortRequests, payload]
      : projection.abortRequests;

  return ConversationProjection.make({
    conversationId: projection.conversationId,
    throughSequence: envelope.sequence,
    tailDigest,
    inputs,
    modelOutputs,
    completedRuns,
    failedRuns,
    settlements,
    abortRequests,
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

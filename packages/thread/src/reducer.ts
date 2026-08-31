import { ThreadId, RunId, ToolCallId } from "@effect-agent/core";
import { Schema } from "effect";

import { EMPTY_TAIL_DIGEST } from "./digest.ts";
import type { CanonicalRecordEnvelope } from "./records.ts";
import {
  AbortRequested,
  CanonicalSequence,
  Digest,
  PersistedJson,
  SubagentJoined,
  SubagentLineageRecorded,
  SubagentRequested,
  SubagentStarted,
  SubmissionSettledRecord,
  ToolApprovalDecided,
  ToolApprovalRequested,
  ToolCallPrepared,
  ToolCallUnknown,
} from "./records.ts";

/** One prepared ordinary Tool Call still awaiting a canonical settled/resolved outcome. */
export class OpenToolCallState extends Schema.Class<OpenToolCallState>(
  "@effect-agent/thread/OpenToolCallState",
)({
  toolCallId: ToolCallId,
  toolName: ToolCallPrepared.fields.toolName,
  turn: ToolCallPrepared.fields.turn,
  runId: RunId,
}) {}

/** The canonical approval trail: requests and decisions in canonical order. */
export const ApprovalRecord = Schema.Union([ToolApprovalRequested, ToolApprovalDecided]);
export type ApprovalRecord = typeof ApprovalRecord.Type;

/**
 * Parent-side view of one Subagent Invocation, keyed by the parent Tool Call: the canonical
 * `SubagentRequested`/`SubagentStarted`/`SubagentJoined` payloads as they become canonical
 * in history. A disposable derived view — the canonical records and the child's
 * own Settlement remain the recovery truth (DUR-015).
 */
export class SubagentInvocationState extends Schema.Class<SubagentInvocationState>(
  "@effect-agent/thread/SubagentInvocationState",
)({
  toolCallId: ToolCallId,
  requested: Schema.optionalKey(SubagentRequested),
  started: Schema.optionalKey(SubagentStarted),
  joined: Schema.optionalKey(SubagentJoined),
}) {}

/**
 * Rebuildable canonical projection. It contains only canonical values and can be discarded and
 * reconstructed from the record stream at any time.
 *
 * Phase 4 added `settlements` and `abortRequests`; Phase 5 added `openToolCalls` (the
 * prepared-minus-settled/resolved fold), `unknownToolCalls`, and `approvals`; S2 adds
 * `subagentInvocations` (the parent-side requested/started/joined fold) and `parentLink` (the
 * child-side immutable lineage). A checkpoint whose persisted state lacks these fields fails to
 * decode against this schema; the checkpoint is rejected and the projection is rebuilt from
 * canonical records (documented disposable-checkpoint behavior, STORE-007/STORE-008).
 * `ModelResponseRecorded` advances `throughSequence` without dedicated projection state: Prompt
 * reconstruction reads canonical records directly.
 */
export class ThreadProjection extends Schema.Class<ThreadProjection>(
  "@effect-agent/thread/ThreadProjection",
)({
  threadId: ThreadId,
  throughSequence: CanonicalSequence,
  tailDigest: Digest,
  inputs: Schema.Array(PersistedJson),
  modelOutputs: Schema.Array(PersistedJson),
  completedRuns: Schema.Array(RunId),
  failedRuns: Schema.Array(RunId),
  settlements: Schema.Array(SubmissionSettledRecord),
  abortRequests: Schema.Array(AbortRequested),
  openToolCalls: Schema.Array(OpenToolCallState),
  unknownToolCalls: Schema.Array(ToolCallUnknown),
  approvals: Schema.Array(ApprovalRecord),
  subagentInvocations: Schema.Array(SubagentInvocationState),
  parentLink: Schema.optionalKey(SubagentLineageRecorded),
}) {}

export const initialThreadProjection = (threadId: ThreadId): ThreadProjection =>
  ThreadProjection.make({
    threadId,
    throughSequence: Schema.decodeSync(CanonicalSequence)(0),
    tailDigest: EMPTY_TAIL_DIGEST,
    inputs: [],
    modelOutputs: [],
    completedRuns: [],
    failedRuns: [],
    settlements: [],
    abortRequests: [],
    openToolCalls: [],
    unknownToolCalls: [],
    approvals: [],
    subagentInvocations: [],
  });

/**
 * Idempotent per-Tool-Call upsert of the parent-side Subagent fold. Deterministic record
 * identities make duplicates impossible in one canonical stream; the first canonical payload of
 * each stage wins so a replayed reduce is a no-op.
 */
const upsertSubagentInvocation = (
  invocations: ReadonlyArray<SubagentInvocationState>,
  payload: SubagentRequested | SubagentStarted | SubagentJoined,
): ReadonlyArray<SubagentInvocationState> => {
  const existing = invocations.find((invocation) => invocation.toolCallId === payload.toolCallId);
  const requested =
    existing?.requested ?? (payload._tag === "SubagentRequested" ? payload : undefined);
  const started = existing?.started ?? (payload._tag === "SubagentStarted" ? payload : undefined);
  const joined = existing?.joined ?? (payload._tag === "SubagentJoined" ? payload : undefined);
  const next = SubagentInvocationState.make({
    toolCallId: payload.toolCallId,
    ...(requested === undefined ? {} : { requested }),
    ...(started === undefined ? {} : { started }),
    ...(joined === undefined ? {} : { joined }),
  });
  return existing === undefined
    ? [...invocations, next]
    : invocations.map((invocation) =>
        invocation.toolCallId === payload.toolCallId ? next : invocation,
      );
};

/** Pure one-record Thread transition. */
export const reduceThreadRecord = (
  projection: ThreadProjection,
  envelope: CanonicalRecordEnvelope,
  tailDigest: Digest = projection.tailDigest,
): ThreadProjection => {
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
  // `ToolCallPrepared` opens a call; `ToolCallSettled` (results batch or late settle) and
  // `ToolCallResolved` (DUR-017) close it. `ToolCallUnknown` does NOT close it: the call stays
  // open until an authorized resolution or a recovered result arrives.
  const openToolCalls =
    payload._tag === "ToolCallPrepared"
      ? projection.openToolCalls.some((call) => call.toolCallId === payload.toolCallId)
        ? projection.openToolCalls
        : [
            ...projection.openToolCalls,
            OpenToolCallState.make({
              toolCallId: payload.toolCallId,
              toolName: payload.toolName,
              turn: payload.turn,
              runId: payload.runId,
            }),
          ]
      : payload._tag === "ToolCallSettled" || payload._tag === "ToolCallResolved"
        ? projection.openToolCalls.filter((call) => call.toolCallId !== payload.toolCallId)
        : projection.openToolCalls;
  const unknownToolCalls =
    payload._tag === "ToolCallUnknown"
      ? [...projection.unknownToolCalls, payload]
      : projection.unknownToolCalls;
  const approvals =
    payload._tag === "ToolApprovalRequested" || payload._tag === "ToolApprovalDecided"
      ? [...projection.approvals, payload]
      : projection.approvals;
  const subagentInvocations =
    payload._tag === "SubagentRequested" ||
    payload._tag === "SubagentStarted" ||
    payload._tag === "SubagentJoined"
      ? upsertSubagentInvocation(projection.subagentInvocations, payload)
      : projection.subagentInvocations;
  // The child-side lineage is immutable (SUB-004): the first canonical record wins forever.
  const parentLink =
    projection.parentLink ?? (payload._tag === "SubagentLineageRecorded" ? payload : undefined);

  return ThreadProjection.make({
    threadId: projection.threadId,
    throughSequence: envelope.sequence,
    tailDigest,
    inputs,
    modelOutputs,
    completedRuns,
    failedRuns,
    settlements,
    abortRequests,
    openToolCalls,
    unknownToolCalls,
    approvals,
    subagentInvocations,
    ...(parentLink === undefined ? {} : { parentLink }),
  });
};

/** Pure full replay from the canonical beginning. */
export const replayThread = (
  threadId: ThreadId,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tailDigest: Digest = EMPTY_TAIL_DIGEST,
): ThreadProjection =>
  replayThreadFromCheckpoint(initialThreadProjection(threadId), records, tailDigest);

/**
 * Pure checkpoint replay. A validated checkpoint projection and its canonical tail produce the
 * same reducer path as full replay for every later record.
 */
export const replayThreadFromCheckpoint = (
  checkpoint: ThreadProjection,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tailDigest: Digest = checkpoint.tailDigest,
): ThreadProjection => {
  let projection = checkpoint;
  for (const record of records) {
    projection = reduceThreadRecord(projection, record, tailDigest);
  }
  return projection;
};

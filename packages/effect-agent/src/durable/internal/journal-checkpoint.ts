import { Option, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";

import { RunId, SubmissionId } from "../../core/Identifiers.ts";
import { RunPolicyUsage } from "../../core/RunPolicyUsage.ts";
import { Selection } from "../../core/ToolExposure.ts";
import { RunUsageSummary } from "../../core/Usage.ts";
import { CanonicalRecordEnvelope, CanonicalSequence, Digest, PersistedJson } from "../Records.ts";

/** Retired accounting is additive; the latest Tool batch always remains replayable verbatim. */
export class JournalCheckpointSeed extends Schema.Class<JournalCheckpointSeed>(
  "@effect-agent/thread/internal/JournalCheckpointSeed",
)({
  runId: RunId,
  throughSequence: CanonicalSequence,
  firstSequence: Schema.optionalKey(CanonicalSequence),
  toolSelection: Schema.optionalKey(Selection),
  committedTurns: Schema.Natural,
  policyUsage: RunPolicyUsage,
  modelCalls: Schema.Natural,
  unobservedModelCalls: Schema.Natural,
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  lastInputTokens: Schema.Natural,
  lastOutputTokens: Schema.Natural,
  costMicrousd: Schema.Natural,
  summarizedModelUsage: RunUsageSummary,
  protectedContext: Schema.optionalKey(PersistedJson),
  contextWindowId: Schema.optionalKey(Schema.String),
  frontier: Schema.optionalKey(
    Schema.Struct({
      sequence: CanonicalSequence,
      tag: Schema.Literals(["ModelResponseRecorded", "ToolCallSettled"]),
    }),
  ),
  /** Canonical replacement whose covered batches were validated before retirement. */
  compaction: CanonicalRecordEnvelope,
}) {}

/** Canonical historical context for a later Run, without the previous Run's authority or usage. */
export class ThreadContextCheckpoint extends Schema.Class<ThreadContextCheckpoint>(
  "@effect-agent/thread/internal/ThreadContextCheckpoint",
)({
  throughSequence: CanonicalSequence,
  prompt: PersistedJson,
  contextWindowId: Schema.optionalKey(Schema.String),
}) {}

/** A bounded sparse projection, never a canonical log or a submission-ownership record. */
export class RecoveryCheckpointState extends Schema.Class<RecoveryCheckpointState>(
  "@effect-agent/thread/internal/RecoveryCheckpointState",
)({
  schemaVersion: Schema.Literal(2),
  policyAccountingVersion: Schema.Literal(1),
  submissionId: SubmissionId,
  submissionIds: Schema.Array(SubmissionId).check(Schema.isMaxLength(4_096)),
  seed: Schema.optionalKey(JournalCheckpointSeed),
  context: Schema.optionalKey(ThreadContextCheckpoint),
  records: Schema.Array(CanonicalRecordEnvelope).check(Schema.isMaxLength(4_096)),
}) {}

export class RecoveryCheckpointContents extends Schema.Class<RecoveryCheckpointContents>(
  "@effect-agent/thread/internal/RecoveryCheckpointContents",
)({
  state: RecoveryCheckpointState,
  digest: Digest,
}) {}

export const RECOVERY_ENGINE_VERSION = "effect-agent/recovery@4";

/**
 * Prove that indexed original-input absence also means absent prefix Run/control evidence.
 * A certificate covers only Run IDs with the given length. Every possible control marker for
 * those IDs is exactly :<run-id>:, including markers nested inside opaque IDs. Cache consumers
 * must have the checkpoint owner's ID length; shorter/longer aliases require full replay.
 * Parsing here can only decline caching; it never supplies an execution identity.
 * The temporary anchor set is discarded after certification, not persisted with the context.
 */
export const makeThreadContextCertificate = (runIdLength: number) => {
  const anchored = new Set<string>();
  let valid = true;

  return {
    add: ({ record }: CanonicalRecordEnvelope): void => {
      if (!valid) return;
      const payload = record.payload;

      if ("runId" in payload && payload.runId !== undefined) {
        const originalInput = payload._tag === "UserInputRecorded" && payload.kind === "user";

        if (originalInput ? anchored.has(payload.runId) : !anchored.has(payload.runId)) {
          valid = false;

          return;
        }
        if (originalInput) anchored.add(payload.runId);
      }
      for (
        let start = record.recordId.indexOf(":run:");
        start !== -1;
        start = record.recordId.indexOf(":run:", start + 1)
      ) {
        const end = start + 1 + runIdLength;

        if (record.recordId[end] === ":" && !anchored.has(record.recordId.slice(start + 1, end))) {
          valid = false;

          return;
        }
      }
    },
    isValid: (): boolean => valid,
  };
};

/**
 * Late evidence can invalidate an old compaction. Such histories use full canonical replay;
 * an omitted run/turn must never be treated as proven absent by a sparse projection.
 */
export const checkpointSuffixCompatible = (
  seed: JournalCheckpointSeed,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  retained: ReadonlyArray<CanonicalRecordEnvelope>,
): boolean =>
  records.every(({ sequence, record: { payload } }) => {
    if ("runId" in payload && payload.runId !== undefined && payload.runId !== seed.runId)
      return false;
    if ("turn" in payload && payload.turn <= seed.committedTurns) return false;
    if (payload._tag === "ToolCallSettled" || payload._tag === "ToolCallResolved") {
      // These records have no turn. The retained declaration/preparation proves their scope.
      return [...retained, ...records].some(
        ({ sequence: declarationSequence, record: { payload: candidate } }) => {
          if (
            candidate._tag !== "ModelResponseRecorded" ||
            candidate.runId !== payload.runId ||
            candidate.turn <= seed.committedTurns ||
            seed.compaction.record.payload._tag !== "CompactionCreated" ||
            declarationSequence <= seed.compaction.record.payload.coversThrough ||
            declarationSequence >= sequence
          )
            return false;
          const decoded = Schema.decodeUnknownOption(Prompt.Prompt)(candidate.messages);

          return (
            Option.isSome(decoded) &&
            decoded.value.content.some(
              (message) =>
                message.role === "assistant" &&
                message.content.some(
                  (part) => part.type === "tool-call" && part.id === payload.toolCallId,
                ),
            )
          );
        },
      );
    }

    return payload._tag !== "CompactionCreated" || payload.coversThrough >= seed.throughSequence;
  });

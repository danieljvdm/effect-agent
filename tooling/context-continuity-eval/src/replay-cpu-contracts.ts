import { Schema } from "effect";

export const REPLAY_CPU_PROTOCOL = "replay-cpu-v1";

export const REPLAY_CPU_PHASES = [
  "compactFirst",
  "freshFirst",
  "freshSecond",
  "compactAgain",
  "freshThird",
  "warmCompactFirst",
  "warmFreshFirst",
  "warmFreshSecond",
  "warmCompactAgain",
  "warmFreshThird",
] as const;

export const ReplayCpuRole = Schema.Literals(["baseline", "candidate", "control"]);

export const ReplayCpuStage = Schema.String.check(
  Schema.isPattern(/^[a-f0-9]{16}-b[0-2]-(baseline|candidate|control)$/),
);

export const ReplayCpuObjectName = Schema.String.check(Schema.isPattern(/^(small|large)-[0-3]$/));

export const ReplayCpuIncarnation = Schema.Struct({
  module: Schema.String,
  runtime: Schema.String,
});

export const ReplayCpuOperation = Schema.Struct({
  phase: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10 })),
  method: Schema.Literals(REPLAY_CPU_PHASES),
  incarnation: ReplayCpuIncarnation,
  submissionId: Schema.String,
  outcome: Schema.String,
  modelCalls: Schema.Natural,
  modelFinalizers: Schema.Natural,
  toolCalls: Schema.Natural,
  journalReadPages: Schema.Natural,
  journalReadRecords: Schema.Natural,
  priorJournalRecordsRead: Schema.Natural,
  throughSequence: Schema.Natural,
});

export const ReplayCpuIdentity = Schema.Struct({
  protocol: Schema.Literal(REPLAY_CPU_PROTOCOL),
  revision: Schema.String,
  fixtureSha256: Schema.String,
  packageVersions: Schema.Record(Schema.String, Schema.String),
  deploymentVersion: Schema.String,
  maxObjects: Schema.Literal(8),
  phasesPerObject: Schema.Literal(10),
  modelCallsPerPhase: Schema.Literal(2),
  toolCallsPerPhase: Schema.Literal(2),
  modelKind: Schema.Literal("scripted-no-network"),
});

export const ReplayCpuBuild = Schema.Struct({
  revision: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  fixtureSha256: Schema.String,
  lockfileSha256: Schema.String,
  bundleSha256: Schema.String,
  versions: Schema.Record(Schema.String, Schema.String),
  inputFiles: Schema.Natural,
});

export const ReplayCpuSeed = Schema.Struct({
  seeded: Schema.Literals([10, 1000]),
  objectId: Schema.String,
  threadId: Schema.String,
  incarnation: ReplayCpuIncarnation,
});

export const ReplayCpuEvidence = Schema.Struct({
  protocol: Schema.Literal(REPLAY_CPU_PROTOCOL),
  incarnation: ReplayCpuIncarnation,
  revision: Schema.String,
  fixtureSha256: Schema.String,
  deploymentVersion: Schema.String,
  objectId: Schema.String,
  threadId: Schema.String,
  phase: Schema.Literal(10),
  seedRecords: Schema.Literals([10, 1000]),
  canonicalRecords: Schema.Natural,
  databaseBytes: Schema.Natural,
  checks: Schema.Record(Schema.String, Schema.Boolean),
  valid: Schema.Boolean,
  operations: Schema.Array(ReplayCpuOperation),
  audits: Schema.Array(
    Schema.Struct({
      phase: Schema.Natural,
      call: Schema.Natural,
      nativeEstimatedTokens: Schema.Number,
      encodedBytes: Schema.Natural,
      prompt: Schema.optionalKey(Schema.Json),
    }),
  ),
  checkpoint: Schema.Struct({ present: Schema.Boolean }),
});

export class ReplayCpuError extends Schema.TaggedError<ReplayCpuError>()("ReplayCpuError", {
  message: Schema.String,
}) {}

export const isCompactionPhase = (phase: number) => [1, 4, 6, 9].includes(phase);

export const currentLargePhase = (phase: number) =>
  phase >= 9 ? 9 : phase >= 6 ? 6 : phase >= 4 ? 4 : 1;

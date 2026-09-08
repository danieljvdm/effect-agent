import { Schema } from "effect";

import { ProfileId } from "./profiles.ts";

export class EvaluationError extends Schema.TaggedError<EvaluationError>()("EvaluationError", {
  stage: Schema.String,
  message: Schema.String,
}) {}

/** The application's status contract. It contains no expected answers or grading hints. */
export const ProjectStatus = Schema.Struct({
  project: Schema.String.annotate({
    description: "The exact project identifier given by the user.",
  }),
  objective: Schema.Literals(["prepare-export-beta", "publish-export-beta", "cancel-export-beta"]),
  region: Schema.String,
  owner: Schema.String,
  launchDate: Schema.String,
  budgetUsd: Schema.Natural,
  customerData: Schema.Literals(["synthetic-only", "production-allowed"]),
  externalPublicationAllowed: Schema.Boolean,
  completed: Schema.Array(Schema.String),
  nextAction: Schema.Literals([
    "verify-backup",
    "rehearse-rollback",
    "fix-rollback",
    "get-security-approval",
    "prepare-handoff",
    "request-final-approval",
  ]),
  receipts: Schema.Array(
    Schema.Struct({ label: Schema.String, code: Schema.String, recordId: Schema.String }),
  ),
});

export type ProjectStatus = typeof ProjectStatus.Type;

export const Check = Schema.Struct({
  name: Schema.String,
  passed: Schema.Boolean,
  expected: Schema.String,
  actual: Schema.String,
});

export type Check = typeof Check.Type;

export const PhaseResult = Schema.Struct({
  index: Schema.Natural,
  runId: Schema.String,
  output: ProjectStatus,
  checks: Schema.Array(Check),
  modelCalls: Schema.Natural,
});

export type PhaseResult = typeof PhaseResult.Type;

export const WindowEvidence = Schema.Struct({
  id: Schema.String,
  recordId: Schema.String,
  sequence: Schema.Natural,
  coversThrough: Schema.Natural,
});

export const CompactionEvidence = Schema.Struct({
  runId: Schema.String,
  turn: Schema.Natural,
  trigger: Schema.Literals(["pressure", "overflow", "requested"]),
  estimatedTokens: Schema.Natural,
  targetTokens: Schema.NullOr(Schema.Natural),
  kind: Schema.Literals(["rollover", "summarize", "clear-tool-results"]),
});

export type CompactionEvidence = typeof CompactionEvidence.Type;

export const RestartEvidence = Schema.Struct({
  phase: Schema.Natural,
  location: Schema.Literals([
    "compaction:before-canonical-append",
    "compaction:after-canonical-append",
  ]),
  runId: Schema.String,
  notesRevisionBefore: Schema.NullOr(Schema.String),
  notesRevisionAfter: Schema.NullOr(Schema.String),
  notesTextUnchanged: Schema.Boolean,
  mechanism: Schema.Literals(["service-reacquisition", "SIGKILL", "durable-object-eviction"]),
  processBefore: Schema.NullOr(Schema.Natural),
  processAfter: Schema.NullOr(Schema.Natural),
  killConfirmed: Schema.Boolean,
});

export type RestartEvidence = typeof RestartEvidence.Type;

/** Observation of the optional native cache, not proof that recovery selected its fast path. */
export const RecoveryCheckpointEvidence = Schema.Union([
  Schema.Struct({ status: Schema.Literals(["unsupported", "missing"]) }),
  Schema.Struct({ status: Schema.Literal("rejected"), reason: Schema.String }),
  Schema.Struct({
    status: Schema.Literal("present"),
    throughSequence: Schema.Natural,
    tailDigest: Schema.String,
  }),
]);

export type RecoveryCheckpointEvidence = typeof RecoveryCheckpointEvidence.Type;

export const ModelUsage = Schema.Struct({
  calls: Schema.Natural,
  completedCalls: Schema.Natural,
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  maxInputTokens: Schema.Natural,
  estimatedCostMicrousd: Schema.Natural,
  reservedCostMicrousd: Schema.Natural,
  returnedModels: Schema.Array(Schema.String),
});

export type ModelUsage = typeof ModelUsage.Type;

export const EvaluationReport = Schema.Struct({
  version: Schema.Literal(3),
  status: Schema.Literals(["running", "passed", "failed"]),
  sourceCommit: Schema.String,
  dirtyWorkingTree: Schema.Boolean,
  scenarioDigest: Schema.String,
  seed: Schema.Natural,
  provider: Schema.Literal("openai"),
  model: Schema.String,
  reasoningEffort: Schema.String,
  serviceTier: Schema.Literal("default"),
  pricingVersion: Schema.Literal("openai-2026-09-08-conservative"),
  contextTokenLimit: Schema.Natural,
  maxOutputTokens: Schema.Natural,
  maxCostMicrousd: Schema.Natural,
  maxModelCalls: Schema.Natural,
  profile: ProfileId,
  compactions: Schema.Array(CompactionEvidence),
  startedAt: Schema.String,
  elapsedMillis: Schema.Natural,
  phases: Schema.Array(PhaseResult),
  windows: Schema.Array(WindowEvidence),
  restarts: Schema.Array(RestartEvidence),
  checks: Schema.Array(Check),
  usage: ModelUsage,
  failure: Schema.NullOr(Schema.String),
});

export type EvaluationReport = typeof EvaluationReport.Type;

export const ResumeCheckpoint = Schema.Struct({
  version: Schema.Literal(1),
  report: EvaluationReport,
  phase: Schema.Natural,
  runId: Schema.String,
  processId: Schema.Natural,
  notes: Schema.Struct({ revision: Schema.NullOr(Schema.String), text: Schema.String }),
});

export type ResumeCheckpoint = typeof ResumeCheckpoint.Type;

export const KillWitness = Schema.Struct({
  phase: Schema.Natural,
  processId: Schema.Natural,
  signal: Schema.Literal("SIGKILL"),
  exited: Schema.Literal(true),
});

export const check = (name: string, actual: unknown, expected: unknown): Check => ({
  name,
  passed: JSON.stringify(actual) === JSON.stringify(expected),
  actual: JSON.stringify(actual) ?? "undefined",
  expected: JSON.stringify(expected) ?? "undefined",
});

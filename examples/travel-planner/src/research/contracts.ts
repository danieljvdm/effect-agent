import { RunId, SettlementId, ThreadId } from "@effect-agent/core/Identifiers";
import { Receipt } from "@effect-agent/core/Receipt";
import { WorkerRef } from "@effect-agent/core/Worker";
import { Schema } from "effect";

import { PlannerInput, TextPlannerInput, PlannerSettings, ShortText, Text } from "../domain.ts";
import { TravelPhoto, TravelUrl } from "../travel-content.ts";

export const previousResearchCoordinatorId = "travel-planner-v9";
export const previousBudgetCoordinatorId = "travel-planner-v10";
export const previousTextCoordinatorId = "travel-planner-v11";
export const previousVoiceCoordinatorId = "travel-planner-v12";
export const researchCoordinatorId = "travel-planner-v13";

export const researchCoordinatorIds = [
  previousResearchCoordinatorId,
  previousBudgetCoordinatorId,
  previousTextCoordinatorId,
  previousVoiceCoordinatorId,
  researchCoordinatorId,
];

export const ScoutRequest = Schema.Struct({ title: ShortText, message: Text });

export const ScoutInput = Schema.Struct({
  ...ScoutRequest.fields,
  sourceThreadId: ThreadId,
  settings: PlannerSettings,
});

/** Bounded source evidence, never authority or private child history. */
export const ScoutFindings = Schema.Struct({
  summary: Text,
  sources: Schema.Array(
    Schema.Struct({
      title: ShortText,
      url: TravelUrl,
      notes: Schema.String.check(Schema.isMaxLength(1_200)),
      photos: Schema.Array(TravelPhoto).check(Schema.isMaxLength(4)),
    }),
  ).check(Schema.isMaxLength(6)),
}).check(
  Schema.makeFilter(
    (value) => new TextEncoder().encode(JSON.stringify(value)).byteLength <= 8 * 1_024,
    { title: "research findings of at most 8 KiB" },
  ),
);

export const ScoutReportInput = Schema.Struct({
  _tag: Schema.Literal("ResearchScoutReport"),
  worker: WorkerRef,
  receipt: Receipt,
  runId: RunId,
  settlementId: SettlementId,
  title: ShortText,
  settings: PlannerSettings,
  outcome: Schema.Literals(["completed", "failed", "aborted"]),
  findings: Schema.NullOr(ScoutFindings),
});

// Keep PlannerInput user-only so existing transcript projections exclude internal reports.
export const CoordinatorInput = Schema.Union([TextPlannerInput, ScoutReportInput]);

/** New admissions share this exact schema with the research reporting registration. */
export const ConversationInput = Schema.Union([PlannerInput, ScoutReportInput]);

/** A deliberately authored, sourced milestone, never a partial model response. */
export const ScoutProgress = Schema.Struct({
  summary: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(900)),
  sources: Schema.Array(TravelUrl).check(Schema.isMinLength(1), Schema.isMaxLength(3)),
});

export const ScoutProgressInput = Schema.Struct({
  _tag: Schema.Literal("ResearchScoutProgress"),
  worker: WorkerRef,
  title: ShortText,
  settings: PlannerSettings,
  finding: ScoutProgress,
});

export const EditorReportInput = Schema.Struct({
  _tag: Schema.Literal("AppEditorReport"),
  worker: WorkerRef,
  settings: PlannerSettings,
  outcome: Schema.Literals(["completed", "failed", "aborted"]),
  summary: Schema.NullOr(Text),
});

export const LiveConversationInput = Schema.Union([
  PlannerInput,
  ScoutReportInput,
  ScoutProgressInput,
  EditorReportInput,
]);

export const expandedCoordinatorIds = [
  previousTextCoordinatorId,
  previousVoiceCoordinatorId,
  researchCoordinatorId,
];

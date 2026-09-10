import { RunId, SettlementId, ThreadId } from "@effect-agent/core/Identifiers";
import { Receipt } from "@effect-agent/core/Receipt";
import { WorkerRef } from "@effect-agent/core/Worker";
import { Schema } from "effect";

import { PlannerInput, PlannerSettings, ShortText, Text } from "../domain.ts";
import { TravelPhoto, TravelUrl } from "../travel-content.ts";

export const researchCoordinatorId = "travel-planner-v9";

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
export const CoordinatorInput = Schema.Union([PlannerInput, ScoutReportInput]);

import type { ThreadExport } from "@effect-agent/thread/ThreadStore";
import { Schema } from "effect";

import { PlannerAnswer, PlannerInput, Text, Trip, type PlannerSnapshot } from "../domain.ts";

export type Messages = Array<PlannerSnapshot["messages"][number]>;

export const completedAnswer = Schema.Union([Text, PlannerAnswer]);

/** Project the original shared log by recorded trip outcomes; never rewrite historical records. */
export const legacyTripMessages = (source: ThreadExport | undefined, tripId: string): Messages => {
  const runs = new Map<string, string>();
  const submissions = new Map<string, string>();

  for (const { record } of source?.records ?? []) {
    const payload = record.payload;

    if (payload._tag === "SubmissionSettled" && payload.runId !== undefined)
      submissions.set(payload.submissionId, payload.runId);
    if (
      payload._tag === "ToolCallSettled" &&
      payload.toolName === "save_trip" &&
      !payload.isFailure
    ) {
      const trip = Schema.decodeUnknownOption(Trip)(payload.result);

      if (trip._tag === "Some") runs.set(payload.runId, trip.value.id);
    }
  }
  const messages: Messages = [];

  for (const { record, sequence } of source?.records ?? []) {
    const payload = record.payload;

    if (payload._tag === "UserInputRecorded") {
      const input = Schema.decodeUnknownOption(PlannerInput)(payload.input);

      const runId =
        payload.runId ??
        (payload.submissionId === undefined ? undefined : submissions.get(payload.submissionId));

      if (input._tag === "Some") {
        if (runId !== undefined && input.value.selectedTripId !== null && !runs.has(runId))
          runs.set(runId, input.value.selectedTripId);
        if ((runId === undefined ? input.value.selectedTripId : runs.get(runId)) === tripId)
          messages.push({
            id: `legacy-${sequence}`,
            role: "user",
            text: input.value.message,
            tripId,
          });
      }
    }
    if (payload._tag === "RunCompleted" && runs.get(payload.runId) === tripId) {
      const answer = Schema.decodeUnknownOption(completedAnswer)(payload.output);

      if (answer._tag === "Some")
        messages.push({
          id: `legacy-${sequence}`,
          role: "assistant",
          text: typeof answer.value === "string" ? answer.value : answer.value.message,
          tripId,
        });
    }
  }

  return messages.slice(-100);
};

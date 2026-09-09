import { type RunId } from "@effect-agent/core/Identifiers";

import {
  type CanonicalRecordEnvelope,
  type CompactionCreated,
  type ToolCallSettled,
} from "../Records.ts";
import { type JournalCheckpointSeed } from "./journal-checkpoint.ts";

/** Metadata for one exact canonical prefix, including the same optional checkpoint seed. */
export interface JournalMetadata {
  readonly ownerRunId: RunId | undefined;
  readonly seed: JournalCheckpointSeed | undefined;
  readonly firstSequenceByRun: ReadonlyMap<string, number>;
  readonly lastResponseSequenceByRun: ReadonlyMap<string, number>;
  readonly terminalSequenceByRun: ReadonlyMap<string, number>;
  readonly settledSpans: ReadonlyArray<{ readonly from: number; readonly to: number }>;
  readonly settledToolCallRecordIds: ReadonlySet<string>;
  readonly settledById: ReadonlyMap<
    string,
    Pick<ToolCallSettled, "isFailure" | "budgetRejected" | "toolSelection">
  >;
  readonly compactions: ReadonlyArray<{
    readonly payload: CompactionCreated;
    readonly sequence: number;
  }>;
}

/**
 * Attempt-local metadata collector. Feed every validated record exactly once, in canonical order,
 * including records omitted from the control view. A snapshot is independent of later suffix
 * additions and may only accompany a replay stream bounded at that same captured tail.
 */
export const makeJournalMetadata = (
  ownerRunId: RunId | undefined,
  seed?: JournalCheckpointSeed,
) => {
  const firstSequenceByRun = new Map<string, number>();

  if (seed?.firstSequence !== undefined) firstSequenceByRun.set(seed.runId, seed.firstSequence);
  const lastResponseSequenceByRun = new Map<string, number>();
  const terminalSequenceByRun = new Map<string, number>();
  const settledSpans: Array<{ readonly from: number; readonly to: number }> = [];
  const settledToolCallRecordIds = new Set<string>();

  const settledById = new Map<
    string,
    Pick<ToolCallSettled, "isFailure" | "budgetRejected" | "toolSelection">
  >();

  const compactions: Array<{ readonly payload: CompactionCreated; readonly sequence: number }> = [];

  return {
    add: (envelope: CanonicalRecordEnvelope): void => {
      const payload = envelope.record.payload;

      if (
        (payload._tag === "RunCompleted" ||
          payload._tag === "RunFailed" ||
          payload._tag === "SubmissionSettled") &&
        payload.runId !== undefined &&
        !terminalSequenceByRun.has(payload.runId)
      )
        terminalSequenceByRun.set(payload.runId, envelope.sequence);
      if (payload._tag === "CompactionCreated") {
        compactions.push({ payload, sequence: envelope.sequence });

        return;
      }
      if (
        "runId" in payload &&
        typeof payload.runId === "string" &&
        !firstSequenceByRun.has(payload.runId)
      )
        firstSequenceByRun.set(payload.runId, envelope.sequence);
      if (payload._tag === "ModelResponseRecorded") {
        lastResponseSequenceByRun.set(payload.runId, envelope.sequence);
      } else if (payload._tag === "ToolCallSettled") {
        settledToolCallRecordIds.add(envelope.record.recordId);
        if (payload.runId === ownerRunId)
          settledById.set(envelope.record.recordId, {
            isFailure: payload.isFailure,
            ...(payload.toolSelection === undefined
              ? {}
              : { toolSelection: payload.toolSelection }),
            ...(payload.budgetRejected === undefined
              ? {}
              : { budgetRejected: payload.budgetRejected }),
          });
        const from = lastResponseSequenceByRun.get(payload.runId);

        if (from !== undefined && from < envelope.sequence)
          settledSpans.push({ from, to: envelope.sequence });
      }
    },
    snapshot: (): JournalMetadata => ({
      ownerRunId,
      seed,
      firstSequenceByRun: new Map(firstSequenceByRun),
      lastResponseSequenceByRun: new Map(lastResponseSequenceByRun),
      terminalSequenceByRun: new Map(terminalSequenceByRun),
      settledSpans: [...settledSpans],
      settledToolCallRecordIds: new Set(settledToolCallRecordIds),
      settledById: new Map(settledById),
      compactions: [...compactions],
    }),
  };
};

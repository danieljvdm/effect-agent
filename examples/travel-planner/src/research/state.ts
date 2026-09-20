import { Schema } from "effect";
import type { ThreadExport } from "effect-agent/thread-store";

import type { ResearchScoutActivity } from "../domain.ts";
import { emptyProgress } from "../server/progress.ts";
import { ScoutRequest } from "./contracts.ts";
import { UpdatingResearchScout } from "./scout.ts";

/** Discover the newest task per source-owned worker without reading any child objects. */
export const researchOverview = (records: ThreadExport["records"]): ResearchScoutActivity[] => {
  const seen = new Set<string>();
  const scouts: ResearchScoutActivity[] = [];

  for (let index = records.length - 1; index >= 0 && scouts.length < 8; index--) {
    const entry = records[index];

    if (entry === undefined) continue;
    const payload = entry.record.payload;

    if (
      payload._tag !== "WorkerInputRequested" ||
      payload.admission.origin.worker.delegationId !== UpdatingResearchScout.delegationId
    )
      continue;
    const worker = payload.admission.origin.worker;

    if (seen.has(worker.threadId)) continue;
    seen.add(worker.threadId);
    const task = Schema.decodeUnknownOption(ScoutRequest)(payload.admission.parameters);

    if (task._tag === "None") continue;
    scouts.push({
      id: worker.threadId,
      sourceSequence: entry.sequence,
      title: task.value.title,
      task: task.value.message,
      state: "loading",
      progress: emptyProgress,
      activity: [],
    });
  }

  return scouts;
};

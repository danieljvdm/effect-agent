import { Schema } from "effect";
import type { ThreadExport } from "effect-agent/thread-store";

import type { EditorActivity } from "../domain.ts";
import { emptyProgress } from "../server/progress.ts";
import { AppEditor, EditorRequest } from "./editor.ts";

/** The overview uses the already loaded source journal, including its exact request locator. */
export const editorOverview = (
  tripId: string,
  records: ThreadExport["records"],
): EditorActivity | null => {
  for (let index = records.length - 1; index >= 0; index--) {
    const entry = records[index];

    if (entry === undefined) continue;
    const payload = entry.record.payload;

    if (
      payload._tag !== "WorkerInputRequested" ||
      payload.admission.origin.worker.delegationId !== AppEditor.delegationId
    )
      continue;
    const task = Schema.decodeUnknownOption(EditorRequest)(payload.admission.parameters);

    if (task._tag === "None" || task.value.tripId !== tripId) continue;

    return {
      id: payload.admission.origin.worker.threadId,
      sourceSequence: entry.sequence,
      task: task.value.message,
      state: "loading",
      progress: emptyProgress,
      activity: [],
    };
  }

  return null;
};

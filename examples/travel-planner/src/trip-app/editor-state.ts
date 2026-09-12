import * as Subagent from "@effect-agent/capabilities/Subagent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { Principal } from "@effect-agent/core/Receipt";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { RecordEnvelope } from "@effect-agent/thread/Records";
import type { ThreadExport } from "@effect-agent/thread/ThreadStore";
import { Effect, Schema, Stream } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { type EditorActivity, PlannerProgress } from "../domain.ts";
import { plannerActivity } from "../server/activity.ts";
import { RecordedDiagnostics } from "../server/diagnostics.ts";
import { emptyProgress } from "../server/progress.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import { AppEditor, EditorRequest } from "./editor.ts";

/** The source journal supplies the worker identity; callers cannot select another account's worker. */
export const editorSnapshot = Effect.fn("editorSnapshot")(function* (
  conversationId: string,
  tripId: string,
  records: ThreadExport["records"],
) {
  const request = records.toReversed().find(({ record }) => {
    const payload = record.payload;

    return (
      payload._tag === "WorkerInputRequested" &&
      payload.admission.origin.worker.delegationId === AppEditor.delegationId &&
      Schema.decodeUnknownOption(EditorRequest)(payload.admission.parameters).pipe(
        (value) => value._tag === "Some" && value.value.tripId === tripId,
      )
    );
  })?.record.payload;

  if (request?._tag !== "WorkerInputRequested") return null;
  const origin = request.admission.origin;
  const task = Schema.decodeUnknownOption(EditorRequest)(request.admission.parameters);

  if (task._tag === "None") return null;

  const base = {
    id: origin.worker.threadId,
    task: task.value.message,
    progress: emptyProgress,
    activity: [],
  };

  return yield* Effect.gen(function* () {
    const worker = yield* Schema.decodeEffect(Subagent.Worker(AppEditor))(origin.worker);
    const runtime = yield* DurableAgentRuntime;
    const sourceThreadId = yield* Schema.decodeEffect(ThreadId)(conversationId);
    const owner = ownerOfThread(conversationId);

    const principal = yield* Schema.decodeEffect(Principal)(owner);

    const host = yield* runtime.workerHost({ sourceThreadId, principal });

    const summary = yield* Subagent.inspect(AppEditor, worker).pipe(
      Effect.provideService(SubagentHost, host),
    );

    const history = yield* Subagent.observe(AppEditor, worker).pipe(
      Stream.takeRight(100),
      Stream.mapEffect((entry) =>
        Schema.decodeUnknownEffect(RecordEnvelope)(entry.record).pipe(
          Effect.map((record) => ({ record, sequence: entry.sequence })),
        ),
      ),
      Stream.runCollect,
      Effect.provideService(SubagentHost, host),
    );

    const env = yield* WorkerEnvironment;

    const progress = yield* Effect.tryPromise({
      try: () => env.ACCOUNT_THREADS.getByName(worker.threadId).plannerProgress(),
      catch: () => "unavailable" as const,
    }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(PlannerProgress))),
      Effect.orElseSucceed(() => emptyProgress),
    );

    const diagnostics = yield* Effect.tryPromise({
      try: () => env.ACCOUNT_THREADS.getByName(worker.threadId).plannerDiagnostics(),
      catch: () => "unavailable" as const,
    }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(RecordedDiagnostics))),
      Effect.orElseSucceed(() => []),
    );

    const settled = history
      .toReversed()
      .find(({ record }) => record.payload._tag === "SubmissionSettled")?.record.payload;

    return {
      ...base,
      state:
        summary.state === "idle" &&
        settled?._tag === "SubmissionSettled" &&
        settled.outcome !== "completed"
          ? "failed"
          : summary.state,
      progress,
      activity: plannerActivity(history, diagnostics).slice(-40),
    } satisfies EditorActivity;
  }).pipe(
    Effect.timeout("3 seconds"),
    Effect.orElseSucceed(() => ({ ...base, state: "unavailable" as const })),
  );
});

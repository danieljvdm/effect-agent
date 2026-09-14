import { Effect, Schema, Stream } from "effect";
import { Subagent, AgentUpdates } from "effect-agent";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ThreadId } from "effect-agent/identifiers";
import { Principal } from "effect-agent/receipt";
import { RecordEnvelope } from "effect-agent/records";
import { SubagentHost } from "effect-agent/subagent-host";
import type { ThreadExport } from "effect-agent/thread-store";
import { WorkerEnvironment } from "effect-cf";

import { PlannerProgress, type ResearchScoutActivity } from "../domain.ts";
import { plannerActivity } from "../server/activity.ts";
import { RecordedDiagnostics } from "../server/diagnostics.ts";
import { emptyProgress } from "../server/progress.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import { ScoutFindings, ScoutRequest } from "./contracts.ts";
import {
  ResearchScout,
  ProgressResearchScout,
  RecoverableResearchScout,
  UpdatingResearchScout,
  updatingResearchScout,
} from "./scout.ts";

/** Discover only source-owned native workers; opaque worker IDs never grant cross-account access. */
export const researchSnapshot = Effect.fn("researchSnapshot")(function* (
  conversationId: string,
  records: ThreadExport["records"],
) {
  const seen = new Set<string>();

  const requests = records
    .toReversed()
    .flatMap(({ record }) => {
      const payload = record.payload;

      if (
        payload._tag !== "WorkerInputRequested" ||
        payload.admission.origin.worker.delegationId !== ResearchScout.delegationId
      )
        return [];
      const worker = payload.admission.origin.worker;

      if (seen.has(worker.threadId)) return [];
      seen.add(worker.threadId);
      const task = Schema.decodeUnknownOption(ScoutRequest)(payload.admission.parameters);

      return task._tag === "Some" ? [{ worker, task: task.value }] : [];
    })
    .slice(0, 8);

  return yield* Effect.forEach(
    requests,
    ({ worker: reference, task }) => {
      const base = {
        id: reference.threadId,
        title: task.title,
        task: task.message,
        progress: emptyProgress,
        activity: [],
      };

      return Effect.gen(function* () {
        const declaration =
          reference.targetAgentId === UpdatingResearchScout.target.id
            ? UpdatingResearchScout
            : reference.targetAgentId === RecoverableResearchScout.target.id
              ? RecoverableResearchScout
              : reference.targetAgentId === ProgressResearchScout.target.id
                ? ProgressResearchScout
                : ResearchScout;

        const worker = yield* Schema.decodeEffect(Subagent.Worker(declaration))(reference);
        const runtime = yield* DurableAgentRuntime;
        const sourceThreadId = yield* Schema.decodeEffect(ThreadId)(conversationId);
        const owner = ownerOfThread(conversationId);

        const principal = yield* Schema.decodeEffect(Principal)(owner);

        const host = yield* runtime.workerHost({ sourceThreadId, principal });

        const summary = yield* Subagent.inspect(declaration, worker).pipe(
          Effect.provideService(SubagentHost, host),
        );

        const history = yield* Subagent.observe(declaration, worker).pipe(
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

        const completed = history
          .toReversed()
          .find(
            ({ record }) =>
              record.payload._tag === "RunCompleted" &&
              settled?._tag === "SubmissionSettled" &&
              record.payload.runId === settled.runId,
          )?.record.payload;

        const findings =
          completed?._tag === "RunCompleted"
            ? Schema.decodeUnknownOption(ScoutFindings)(completed.output)
            : undefined;

        const latestUpdate = history
          .toReversed()
          .find(({ record }) => record.payload._tag === "AgentUpdateEmitted")?.record.payload;

        const latestRun = history
          .toReversed()
          .find(({ record }) => record.payload._tag === "RunStarted")?.record.payload;

        const milestone =
          latestUpdate?._tag === "AgentUpdateEmitted" &&
          latestRun?._tag === "RunStarted" &&
          latestUpdate.update.runId === latestRun.runId
            ? yield* AgentUpdates.decode(updatingResearchScout, latestUpdate.update).pipe(
                Effect.option,
              )
            : undefined;

        return {
          ...base,
          ...(summary.state === "idle" &&
          settled?._tag === "SubmissionSettled" &&
          settled.outcome === "completed" &&
          findings?._tag === "Some"
            ? { finding: { id: settled.settlementId, text: findings.value.summary } }
            : {}),
          state:
            summary.state === "idle" &&
            settled?._tag === "SubmissionSettled" &&
            settled.outcome !== "completed"
              ? "failed"
              : summary.state,
          progress:
            summary.state === "active" && milestone?._tag === "Some"
              ? { ...progress, text: milestone.value.summary }
              : summary.state === "idle" &&
                  settled?._tag === "SubmissionSettled" &&
                  settled.outcome === "completed" &&
                  progress.text === "" &&
                  findings?._tag === "Some"
                ? { ...progress, text: findings.value.summary }
                : progress,
          activity: plannerActivity(history, diagnostics).slice(-40),
        } satisfies ResearchScoutActivity;
      }).pipe(
        Effect.timeout("3 seconds"),
        Effect.orElseSucceed(() => ({ ...base, state: "unavailable" as const })),
      );
    },
    { concurrency: 3 },
  );
});

import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import { Effect, Option, Schema, Stream } from "effect";
import { AgentUpdates } from "effect-agent";
import { ThreadId } from "effect-agent/identifiers";
import { MessageDeliveryStore } from "effect-agent/message-delivery";
import { IdempotencyKey, Principal } from "effect-agent/receipt";
import { CanonicalSequence } from "effect-agent/records";
import { SubmissionLedger, SubmissionLookupByKey } from "effect-agent/submission-ledger";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { WorkerRef } from "effect-agent/worker";
import { WorkerHostAuthorizer } from "effect-agent/worker-host";
import { WorkerEnvironment } from "effect-cf";

import { PlannerError, PlannerWorkerDetail, PlannerWorkerRequest } from "../domain.ts";
import { ScoutFindings } from "../research/contracts.ts";
import { ResearchScout, updatingResearchScout } from "../research/scout.ts";
import { plannerActivity } from "./activity.ts";
import { readDiagnostics } from "./diagnostics.ts";
import { ProgressStore } from "./progress.ts";
import { ownerOfThread } from "./tenancy.ts";
import { workerHistory } from "./worker-history.ts";

export const WorkerLocator = Schema.Struct({
  workerId: PlannerWorkerRequest.fields.workerId,
  sourceSequence: PlannerWorkerRequest.fields.sourceSequence,
});

/** Private RPC: only source-verified locators are translated into these ledger keys. */
export const WorkerStatusRequest = Schema.Struct({
  sourceThreadId: ThreadId,
  worker: WorkerRef,
  messageId: IdempotencyKey,
  principal: Schema.NullOr(Principal),
  refused: Schema.Boolean,
});

const unavailable = () =>
  new PlannerError({
    code: "unavailable",
    message: "Worker updates are temporarily unavailable.",
  });

const notFound = () => new PlannerError({ code: "not-found", message: "Worker not found." });

/**
 * Authenticate at the conversation object, then read exactly the source record named by the
 * overview. Canonical sequences are immutable; a guessed locator or worker ID grants no access.
 * The same host read authorizer used by Subagent.inspect/observe remains in force.
 */
export const plannerWorker = Effect.fn("plannerWorker")(
  function* (input: typeof WorkerLocator.Type) {
    const identity = yield* ThreadObjectIdentity;
    const sourceThreadId = yield* Schema.decodeEffect(ThreadId)(identity.threadId);
    const principal = yield* Schema.decodeEffect(Principal)(ownerOfThread(sourceThreadId));
    const authorizer = yield* WorkerHostAuthorizer;

    yield* authorizer.authorize({
      sourceThreadId,
      principal,
      operation: "inspect",
      access: "read",
    });
    const store = yield* ThreadStore;
    const afterSequence = yield* Schema.decodeEffect(CanonicalSequence)(input.sourceSequence - 1);

    const records = yield* store
      .read(ThreadRead.make({ threadId: sourceThreadId, afterSequence, limit: 1 }))
      .pipe(Stream.runCollect);

    const entry = records[0];
    const payload = entry?.record.payload;

    if (
      records.length !== 1 ||
      entry?.threadId !== sourceThreadId ||
      entry.sequence !== input.sourceSequence ||
      payload?._tag !== "WorkerInputRequested" ||
      payload.admission.origin.source.threadId !== sourceThreadId ||
      payload.admission.origin.worker.threadId !== input.workerId
    )
      return yield* notFound();
    const admission = payload.admission;
    const worker = admission.origin.worker;

    for (const operation of ["inspect", "observe"] as const)
      yield* authorizer.authorize({ sourceThreadId, principal, worker, operation, access: "read" });

    const deliveries = yield* MessageDeliveryStore;

    const delivery = yield* deliveries.get({
      ownerThreadId: sourceThreadId,
      messageId: admission.messageId,
    });

    const request = yield* Schema.encodeEffect(Schema.fromJsonString(WorkerStatusRequest))({
      sourceThreadId,
      worker,
      messageId: admission.messageId,
      principal: admission.deliveryPrincipal ?? delivery?.envelope.deliveryPrincipal ?? null,
      refused: delivery?.status === "refused",
    });

    const env = yield* WorkerEnvironment;

    const reply = yield* Effect.tryPromise({
      try: () => env.ACCOUNT_THREADS.getByName(worker.threadId).plannerWorkerStatus(request),
      catch: unavailable,
    });

    return yield* Schema.decodeEffect(Schema.fromJsonString(PlannerWorkerDetail))(reply);
  },
  Effect.timeout("3 seconds"),
  Effect.mapError(unavailable),
);

/**
 * Runs on the worker's owning object: one local key lookup, a nonterminal scan, and at most
 * 100 canonical records. No foreign ledger fan-out, full exports, history wire round trips,
 * or recovery/admission. The status describes the selected request plus any active work.
 */
export const workerStatus = Effect.fn("workerStatus")(
  function* (request: typeof WorkerStatusRequest.Type) {
    const identity = yield* ThreadObjectIdentity;

    if (identity.threadId !== request.worker.threadId) return yield* notFound();
    const threadId = request.worker.threadId;
    const ledger = yield* SubmissionLedger;

    const latest =
      request.principal === null
        ? Option.none()
        : yield* ledger.lookup(
            SubmissionLookupByKey.make({
              threadId,
              principal: request.principal,
              idempotencyKey: request.messageId,
            }),
          );

    if (
      Option.isSome(latest) &&
      (latest.value.threadId !== threadId ||
        latest.value.workerAdmission?.origin.source.threadId !== request.sourceThreadId ||
        latest.value.workerAdmission.origin.worker.threadId !== threadId ||
        latest.value.workerAdmission.origin.worker.delegationId !== request.worker.delegationId ||
        latest.value.workerAdmission.origin.worker.targetAgentId !== request.worker.targetAgentId)
    )
      return yield* notFound();

    const pending = yield* ledger.scanNonterminal.pipe(
      Stream.filter((row) => row.threadId === threadId),
      Stream.take(1),
      Stream.runCollect,
    );

    const history = yield* workerHistory(threadId);
    const progress = yield* Effect.flatMap(ProgressStore, (store) => store.read);
    const diagnostics = yield* readDiagnostics;

    const settled = history.findLast(({ record }) => record.payload._tag === "SubmissionSettled")
      ?.record.payload;

    const completed = history.findLast(
      ({ record }) =>
        record.payload._tag === "RunCompleted" &&
        settled?._tag === "SubmissionSettled" &&
        record.payload.runId === settled.runId,
    )?.record.payload;

    const findings =
      completed?._tag === "RunCompleted" &&
      request.worker.delegationId === ResearchScout.delegationId
        ? Schema.decodeUnknownOption(ScoutFindings)(completed.output)
        : Option.none();

    const latestUpdate = history.findLast(
      ({ record }) => record.payload._tag === "AgentUpdateEmitted",
    )?.record.payload;

    const latestRun = history.findLast(({ record }) => record.payload._tag === "RunStarted")?.record
      .payload;

    const milestone =
      latestUpdate?._tag === "AgentUpdateEmitted" &&
      latestRun?._tag === "RunStarted" &&
      latestUpdate.update.runId === latestRun.runId
        ? yield* AgentUpdates.decode(updatingResearchScout, latestUpdate.update).pipe(Effect.option)
        : Option.none();

    const state =
      pending.length > 0 || (Option.isSome(latest) && latest.value.state !== "settled")
        ? "active"
        : Option.isNone(latest)
          ? request.refused
            ? "failed"
            : "starting"
          : (latest.value.settledOutcome !== undefined &&
                latest.value.settledOutcome !== "completed") ||
              (settled?._tag === "SubmissionSettled" && settled.outcome !== "completed")
            ? "failed"
            : "idle";

    return {
      state,
      progress:
        state === "active" && Option.isSome(milestone)
          ? { ...progress, text: milestone.value.summary }
          : state === "idle" && Option.isSome(findings) && progress.text === ""
            ? { ...progress, text: findings.value.summary }
            : progress,
      activity: plannerActivity(history, diagnostics).slice(-40),
      ...(state === "idle" &&
      settled?._tag === "SubmissionSettled" &&
      settled.outcome === "completed" &&
      Option.isSome(findings)
        ? { finding: { id: settled.settlementId, text: findings.value.summary } }
        : {}),
    } satisfies PlannerWorkerDetail;
  },
  Effect.timeout("3 seconds"),
  Effect.mapError(unavailable),
);

import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import { Effect, Option, Schema, Stream } from "effect";
import { AgentUpdates } from "effect-agent";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ThreadId } from "effect-agent/identifiers";
import { MessageDeliveryStore } from "effect-agent/message-delivery";
import { IdempotencyKey, Principal } from "effect-agent/receipt";
import { CanonicalSequence, type ToolApprovalRequested } from "effect-agent/records";
import {
  modelResponseRecordId,
  runIdForSubmission,
  toolApprovalRequestRecordId,
} from "effect-agent/run-journal";
import {
  ApprovalDecisionCommand,
  RecoverySnapshotRequest,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  type SubmissionSnapshot,
} from "effect-agent/submission-ledger";
import { getRecord, getRunInput, ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { WorkerRef } from "effect-agent/worker";
import { WorkerHostAuthorizer } from "effect-agent/worker-host";
import { WorkerEnvironment } from "effect-cf";
import { Prompt } from "effect/unstable/ai";

import {
  PlannerError,
  PlannerWorkerDetail,
  PlannerWorkerRequest,
  ResearchPlan,
  WorkerApprovalDecision,
} from "../domain.ts";
import { ScoutFindings } from "../research/contracts.ts";
import {
  ReviewResearchPlan,
  UpdatingResearchScout,
  updatingResearchScout,
} from "../research/scout.ts";
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
const workerRequest = Effect.fn("workerRequest")(function* (
  input: typeof WorkerLocator.Type,
  access: "read" | "control" = "read",
) {
  const identity = yield* ThreadObjectIdentity;
  const sourceThreadId = yield* Schema.decodeEffect(ThreadId)(identity.threadId);
  const principal = yield* Schema.decodeEffect(Principal)(ownerOfThread(sourceThreadId));
  const authorizer = yield* WorkerHostAuthorizer;

  yield* authorizer.authorize({
    sourceThreadId,
    principal,
    operation: "inspect",
    access,
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
    yield* authorizer.authorize({ sourceThreadId, principal, worker, operation, access });

  const deliveries = yield* MessageDeliveryStore;

  const delivery = yield* deliveries.get({
    ownerThreadId: sourceThreadId,
    messageId: admission.messageId,
  });

  return WorkerStatusRequest.make({
    sourceThreadId,
    worker,
    messageId: admission.messageId,
    principal: admission.deliveryPrincipal ?? delivery?.envelope.deliveryPrincipal ?? null,
    refused: delivery?.status === "refused",
  });
}, Effect.mapError(unavailable));

export const plannerWorker = Effect.fn("plannerWorker")(
  function* (input: typeof WorkerLocator.Type) {
    const request = yield* Schema.encodeEffect(Schema.fromJsonString(WorkerStatusRequest))(
      yield* workerRequest(input),
    );

    const env = yield* WorkerEnvironment;

    const reply = yield* Effect.tryPromise({
      try: () => env.ACCOUNT_THREADS.getByName(input.workerId).plannerWorkerStatus(request),
      catch: unavailable,
    });

    return yield* Schema.decodeEffect(Schema.fromJsonString(PlannerWorkerDetail))(reply);
  },
  Effect.timeout("3 seconds"),
  Effect.mapError(unavailable),
);

export const WorkerApprovalRequest = Schema.Struct({
  ...WorkerStatusRequest.fields,
  approval: WorkerApprovalDecision,
});

/** The authenticated account addresses its conversation, which verifies the immutable worker link. */
export const decideWorkerApproval = Effect.fn("decideWorkerApproval")(
  function* (input: typeof WorkerLocator.Type & typeof WorkerApprovalDecision.Type) {
    const request = yield* workerRequest(input, "control");
    const env = yield* WorkerEnvironment;

    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(WorkerApprovalRequest))({
      ...request,
      approval: input,
    });

    yield* Effect.tryPromise({
      try: () =>
        env.ACCOUNT_THREADS.getByName(request.worker.threadId).plannerWorkerApproval(encoded),
      catch: unavailable,
    });
  },
  Effect.timeout("5 seconds"),
  Effect.mapError(unavailable),
);

/** Approval authority uses exact canonical records, independently of the activity window. */
const researchApproval = Effect.fn("researchApproval")(function* (
  submission: SubmissionSnapshot,
  toolCallId: (typeof WorkerApprovalDecision.Type)["toolCallId"],
) {
  const origin = submission.workerAdmission?.origin;

  if (origin === undefined) return undefined;
  const threadId = submission.threadId;
  const runId = runIdForSubmission(submission.submissionId);
  const input = yield* getRunInput({ threadId, runId });

  if (
    Option.isNone(input) ||
    input.value.record.payload._tag !== "UserInputRecorded" ||
    input.value.record.payload.submissionId !== submission.submissionId
  )
    return undefined;
  let requested: ToolApprovalRequested | undefined;

  // The retained policy bounds canonical turn numbers, including after steering or recovery.
  for (let turn = 1; turn <= origin.policy.maxTurns; turn++) {
    const record = yield* getRecord({
      threadId,
      recordId: toolApprovalRequestRecordId(runId, turn, toolCallId),
    });

    if (Option.isNone(record)) continue;
    const payload = record.value.record.payload;

    if (
      payload._tag !== "ToolApprovalRequested" ||
      payload.runId !== runId ||
      payload.turn !== turn ||
      payload.toolCallId !== toolCallId ||
      payload.toolName !== ReviewResearchPlan.name ||
      requested !== undefined
    )
      return undefined;
    requested = payload;
  }
  if (requested === undefined) return undefined;

  const response = yield* getRecord({
    threadId,
    recordId: modelResponseRecordId(runId, requested.turn),
  });

  if (Option.isNone(response)) return undefined;
  const payload = response.value.record.payload;

  if (
    payload._tag !== "ModelResponseRecorded" ||
    payload.runId !== runId ||
    payload.turn !== requested.turn ||
    payload.turnId !== requested.turnId
  )
    return undefined;
  const prompt = Schema.decodeUnknownOption(Prompt.Prompt)(payload.messages);

  if (Option.isNone(prompt)) return undefined;
  for (const message of prompt.value.content) {
    if (message.role !== "assistant") continue;
    for (const part of message.content) {
      if (
        part.type !== "tool-call" ||
        part.id !== toolCallId ||
        part.name !== ReviewResearchPlan.name
      )
        continue;
      const plan = Schema.decodeUnknownOption(ResearchPlan)(part.params);

      if (Option.isSome(plan)) return plan.value;
    }
  }

  return undefined;
});

/** Private child RPC: validate lineage and the declared checkpoint before the native decision. */
export const workerApproval = Effect.fn("workerApproval")(function* (
  request: typeof WorkerApprovalRequest.Type,
) {
  const identity = yield* ThreadObjectIdentity;
  const ledger = yield* SubmissionLedger;

  const found = yield* ledger.lookup(
    SubmissionLookupById.make({ submissionId: request.approval.submissionId }),
  );

  if (Option.isNone(found)) return yield* notFound();
  const submission = found.value;
  const origin = submission.workerAdmission?.origin;

  if (
    identity.threadId !== request.worker.threadId ||
    submission.threadId !== identity.threadId ||
    origin?.source.threadId !== request.sourceThreadId ||
    origin.worker.threadId !== identity.threadId ||
    origin.worker.delegationId !== UpdatingResearchScout.delegationId ||
    origin.worker.targetAgentId !== updatingResearchScout.id ||
    request.worker.delegationId !== origin.worker.delegationId ||
    request.worker.targetAgentId !== origin.worker.targetAgentId ||
    !(yield* researchApproval(submission, request.approval.toolCallId))
  )
    return yield* notFound();
  const runtime = yield* DurableAgentRuntime;

  yield* runtime.resolveApproval(
    ApprovalDecisionCommand.make({
      ...request.approval,
      resolver: ownerOfThread(request.sourceThreadId),
      reason: "Traveler decided the research plan in its approval card.",
    }),
  );
}, Effect.mapError(unavailable));

/**
 * Runs on the worker's owning object: a local key lookup, a nonterminal scan, and at most
 * 100 activity records. Approval adds indexed reads bounded by the retained turn policy.
 * No foreign ledger fan-out, full exports, history wire round trips, or recovery/admission.
 * The status describes the selected request plus any active work.
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
    const suspended = pending.find((row) => row.state === "suspended");

    const snapshot =
      suspended === undefined
        ? undefined
        : yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: suspended.submissionId }),
          );

    let approval: PlannerWorkerDetail["approval"];

    if (snapshot?.suspension?.reason._tag === "ApprovalPending") {
      const submissionId = snapshot.submission.submissionId;
      const toolCallId = snapshot.suspension.reason.toolCallIds[0];
      const plan = yield* researchApproval(snapshot.submission, toolCallId);
      const origin = snapshot.submission.workerAdmission?.origin;

      if (plan === undefined || origin === undefined) return yield* unavailable();
      approval = { submissionId, toolCallId, ...plan, expiresAt: origin.expiresAtMillis };
    }

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
      request.worker.delegationId === UpdatingResearchScout.delegationId
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
      ...(approval === undefined ? {} : { approval }),
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

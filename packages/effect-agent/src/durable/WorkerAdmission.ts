import { Context, Effect, Schema } from "effect";

import { DurableRuntimeFailpoint, DurableRuntimeFailpointError } from "./DurableFailpoint.ts";
import { ensureThreadCreated, ensureWorkerOrigin } from "./internal/thread-initialization.ts";
import { ProducerEpoch, ProducerId, WorkerAdmission } from "./Records.ts";
import {
  AdmissionConflict,
  AdmissionPolicyError,
  AdmissionRequest,
  LedgerError,
  MarkReadyRequest,
  SubmissionLedger,
} from "./SubmissionLedger.ts";
import {
  AppendConflict,
  FenceRejected,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadStore,
  ThreadStoreError,
} from "./ThreadStore.ts";
import { WakeScheduler } from "./WakeScheduler.ts";

/**
 * Storage admission after source authorization, input validation and capacity reservation.
 * This is an internal host port, not an authorization entrypoint. Hosts route the whole
 * operation to the destination owner and enroll recovery before executing it.
 */
export class WorkerAdmissionRequest extends Schema.Class<WorkerAdmissionRequest>(
  "@effect-agent/thread/WorkerAdmissionRequest",
)({
  ...AdmissionRequest.fields,
  workerAdmission: WorkerAdmission,
  producerId: ProducerId,
}) {}

export const WorkerAdmissionFailure = Schema.Union([
  AdmissionConflict,
  AdmissionPolicyError,
  LedgerError,
  ThreadStoreError,
  ThreadNotMaterialized,
  AppendConflict,
  FenceRejected,
  DurableRuntimeFailpointError,
]);

/** Local implementation shared by every host; retries finish the same admitted row. */
export const admitWorker = Effect.fn("WorkerAdmission.admitWorker")(function* (
  request: WorkerAdmissionRequest,
) {
  const origin = request.workerAdmission.origin;

  if (
    origin.worker.threadId !== request.threadId ||
    origin.worker.targetAgentId !== request.agentId ||
    request.parentLinkage !== undefined
  )
    return yield* LedgerError.make({
      operation: "admit worker",
      message: "Worker admission does not belong to the destination Thread",
    });

  const ledger = yield* SubmissionLedger;
  const store = yield* ThreadStore;
  const wake = yield* WakeScheduler;
  const failpoint = yield* DurableRuntimeFailpoint;
  const admitted = yield* ledger.admit(AdmissionRequest.make(request));

  yield* failpoint.hit("submit:after-admit");
  if (admitted.replayed && admitted.state !== "admitted") return admitted;

  yield* store
    .materialize(
      ThreadMaterialization.make({
        threadId: request.threadId,
        producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
      }),
    )
    .pipe(Effect.catchTag("FenceRejected", () => Effect.void));

  const config = {
    producerId: request.producerId,
    deploymentId: request.deploymentId,
  };

  yield* ensureThreadCreated(config, request.threadId, request.agentId, request.agentDigests);
  yield* ensureWorkerOrigin(config, origin).pipe(
    Effect.mapError((cause) =>
      LedgerError.make({ operation: "worker-origin", message: cause.reason, cause }),
    ),
  );
  yield* failpoint.hit("submit:after-materialize");
  yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
  yield* wake.notify(request.threadId);

  return admitted;
});

/** Placement adapter for the composite operation; local hosts use the same implementation. */
export const WorkerAdmissionPort = Context.Reference<{ readonly admit: typeof admitWorker }>(
  "@effect-agent/thread/WorkerAdmissionPort",
  { defaultValue: () => ({ admit: admitWorker }) },
);

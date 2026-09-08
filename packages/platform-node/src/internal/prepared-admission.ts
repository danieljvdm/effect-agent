import { type AgentId } from "@effect-agent/core/Identifiers";
import { type DurableSubmitAgent } from "@effect-agent/thread/DurableAgentRuntime";
import { PreparedInputAdmission } from "@effect-agent/thread/PreparedInputAdmission";
import { PersistedJson } from "@effect-agent/thread/Records";
import {
  ScheduledInputRetryable,
  ScheduledInputRefused,
  ScheduleStorageError,
} from "@effect-agent/thread/Schedule";
import { Context, Effect } from "effect";

import { type NodeDurableHost } from "../NodeDurableHost.ts";

const passthroughSubmitAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: { id: agentId, input: PersistedJson },
});

const ambiguous = (): ScheduledInputRetryable =>
  ScheduledInputRetryable.make({ reason: "ambiguous" });

const corrupt = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "corrupt" });

/** Host-owned admission gate, available while the host's worker pool is being assembled. */
export class NodeAdmission extends Context.Service<
  NodeAdmission,
  Pick<NodeDurableHost["Service"], "submit" | "submissionStatus">
>()("@effect-agent/platform-node/internal/NodeAdmission") {}

/** Acquire the gated source once; worker callers cannot replace its admission authority. */
export const makeNodePreparedInputAdmission = Effect.gen(function* () {
  const host = yield* NodeAdmission;

  return PreparedInputAdmission.of({
    submissionStatus: (receipt) =>
      host
        .submissionStatus(receipt)
        .pipe(Effect.mapError(() => ScheduledInputRetryable.make({ reason: "storage" }))),
    submit: (envelope) =>
      host
        .submit(passthroughSubmitAgent(envelope.agentId), envelope.input, {
          threadId: envelope.threadId,
          principal: envelope.deliveryPrincipal,
          idempotencyKey: envelope.admissionKey,
          ...(envelope.admissionGroup === undefined
            ? {}
            : { admissionGroup: envelope.admissionGroup }),
          ...(envelope.admissionFence === undefined
            ? {}
            : { admissionFence: envelope.admissionFence }),
          ...(envelope.workerAdmission === undefined
            ? {}
            : { workerAdmission: envelope.workerAdmission }),
          ...(envelope.messageAdmission === undefined
            ? {}
            : { messageAdmission: envelope.messageAdmission }),
          definitions: envelope.definitions,
        })
        .pipe(
          Effect.catchTags({
            AdmissionClosed: () =>
              Effect.fail(ScheduledInputRetryable.make({ reason: "host-closed" })),
            AgentInputError: () => Effect.fail(corrupt("prepared admission input")),
            AdmissionConflict: () => Effect.fail(corrupt("prepared admission conflict")),
            DigestError: () => Effect.fail(ambiguous()),
            AdmissionPolicyError: (error) =>
              error.reason === "refused"
                ? ScheduledInputRefused.make({ code: error.code })
                : ScheduledInputRetryable.make({
                    reason: error.reason === "occupied" ? "capacity" : "storage",
                  }),
            LedgerError: () => ScheduledInputRetryable.make({ reason: "storage" }),
            ThreadStoreError: () => Effect.fail(ambiguous()),
            ThreadNotMaterialized: () => Effect.fail(ambiguous()),
            AppendConflict: () => Effect.fail(ambiguous()),
            FenceRejected: () => Effect.fail(ambiguous()),
            DurableRuntimeFailpointError: () => Effect.fail(ambiguous()),
          }),
        ),
  });
});

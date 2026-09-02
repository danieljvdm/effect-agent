import type { AgentId } from "@effect-agent/core";
import {
  type DurableSubmitAgent,
  PersistedJson,
  type PreparedInput,
  PreparedInputAdmission,
  type ScheduledEnvelope,
  ScheduledInputAdmission,
  ScheduledInputRetryable,
  ScheduleStorageError,
} from "@effect-agent/thread";
import { Effect, Layer } from "effect";

import { CloudflareThreadClient, type ThreadClientError } from "./client.ts";

const passthroughAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: { id: agentId, input: PersistedJson },
});

/** Ordinary prepared admission through one freshly addressed Thread Object call. */
export const cloudflarePreparedInputAdmissionLayer: Layer.Layer<
  PreparedInputAdmission,
  never,
  CloudflareThreadClient
> = Layer.effect(
  PreparedInputAdmission,
  Effect.gen(function* () {
    const client = yield* CloudflareThreadClient;

    return PreparedInputAdmission.of({
      submit: (envelope) =>
        client
          .submit(passthroughAgent(envelope.agentId), envelope.input, {
            threadId: envelope.threadId,
            principal: envelope.deliveryPrincipal,
            idempotencyKey: envelope.admissionKey,
            definitions: envelope.definitions,
          })
          .pipe(
            Effect.catchTags({
              AdmissionConflict: () =>
                ScheduleStorageError.make({ operation: "prepared admission", reason: "corrupt" }),
              AdmissionLimitExceeded: () => ScheduledInputRetryable.make({ reason: "capacity" }),
              ThreadClientError: (error: ThreadClientError) =>
                ScheduledInputRetryable.make({
                  reason: error.overloaded === true ? "capacity" : "transport",
                }),
              HostProtocolError: () => ScheduledInputRetryable.make({ reason: "ambiguous" }),
              LedgerError: () => ScheduledInputRetryable.make({ reason: "storage" }),
              ThreadStoreError: () => ScheduledInputRetryable.make({ reason: "storage" }),
              DurableAlarmError: () => ScheduledInputRetryable.make({ reason: "storage" }),
              AgentInputError: () =>
                ScheduleStorageError.make({ operation: "prepared admission", reason: "corrupt" }),
              DigestError: () =>
                ScheduleStorageError.make({ operation: "prepared admission", reason: "corrupt" }),
              ThreadNotMaterialized: () => ScheduledInputRetryable.make({ reason: "storage" }),
              AppendConflict: () => ScheduledInputRetryable.make({ reason: "ambiguous" }),
              FenceRejected: () => ScheduledInputRetryable.make({ reason: "ambiguous" }),
              DurableRuntimeFailpointError: () =>
                ScheduledInputRetryable.make({ reason: "ambiguous" }),
            }),
          ),
    });
  }),
);

const preparedFromSchedule = (envelope: ScheduledEnvelope): PreparedInput => ({
  schemaVersion: 1,
  threadId: envelope.threadId,
  deliveryPrincipal: envelope.deliveryPrincipal,
  agentId: envelope.agentId,
  definitions: envelope.definitions,
  input: envelope.input,
  inputDigest: envelope.inputDigest,
  admissionKey: envelope.admissionKey,
  authorization: envelope.authorization,
});

/** Compatibility adapter retaining Scheduling's public admission port. */
export const cloudflareScheduledInputAdmissionLayer: Layer.Layer<
  ScheduledInputAdmission,
  never,
  PreparedInputAdmission
> = Layer.effect(
  ScheduledInputAdmission,
  Effect.map(PreparedInputAdmission, (admission) =>
    ScheduledInputAdmission.of({
      submit: (envelope) => admission.submit(preparedFromSchedule(envelope)),
    }),
  ),
);

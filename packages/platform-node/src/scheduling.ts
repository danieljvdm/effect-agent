import type { AgentId } from "@effect-agent/core";
import {
  type DurableSubmitAgent,
  type ScheduleProcessFailure,
  type ScheduleAuthorizer,
  type SchedulingLimits,
  ScheduleStorageError,
  ScheduleStore,
  type ScheduleValidationError,
  ScheduledInputAdmission,
  ScheduledInputRetryable,
  ScheduleWake,
  Scheduling,
  ScheduleDriver,
  defaultSchedulingLimits,
  PersistedJson,
} from "@effect-agent/session";
import { NodeCrypto } from "@effect/platform-node";
import { Cause, Duration, Effect, Layer, Option, PubSub, Result } from "effect";

import { NodeDurableHost } from "./host.ts";

const passthroughSubmitAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: { id: agentId, input: PersistedJson },
});

const ambiguous = (): ScheduledInputRetryable =>
  ScheduledInputRetryable.make({ reason: "ambiguous" });

const corrupt = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "corrupt" });

/**
 * Scheduled admission through the existing host gate. Once the gate admits the call, every
 * runtime failure stays ambiguous because the Submission may already have committed.
 */
export const nodeScheduledInputAdmissionLayer: Layer.Layer<
  ScheduledInputAdmission,
  never,
  NodeDurableHost
> = Layer.effect(
  ScheduledInputAdmission,
  Effect.gen(function* () {
    const host = yield* NodeDurableHost;
    return ScheduledInputAdmission.of({
      submit: (envelope) =>
        host
          .submit(passthroughSubmitAgent(envelope.agentId), envelope.input, {
            conversationId: envelope.conversationId,
            principal: envelope.deliveryPrincipal,
            idempotencyKey: envelope.admissionKey,
            definitions: envelope.definitions,
          })
          .pipe(
            Effect.catchTags({
              AdmissionClosed: () =>
                Effect.fail(ScheduledInputRetryable.make({ reason: "host-closed" })),
              AgentInputError: () => Effect.fail(corrupt("scheduled admission input")),
              AdmissionConflict: () => Effect.fail(corrupt("scheduled admission conflict")),
              DigestError: () => Effect.fail(ambiguous()),
              LedgerError: () => Effect.fail(ambiguous()),
              ConversationStoreError: () => Effect.fail(ambiguous()),
              ConversationNotMaterialized: () => Effect.fail(ambiguous()),
              AppendConflict: () => Effect.fail(ambiguous()),
              FenceRejected: () => Effect.fail(ambiguous()),
              DurableRuntimeFailpointError: () => Effect.fail(ambiguous()),
            }),
          ),
    });
  }),
);

/** One bounded hint slot for the single supported Node scheduler. Indexed polling repairs loss. */
export const nodeScheduleWakeLayer: Layer.Layer<ScheduleWake> = Layer.effect(
  ScheduleWake,
  Effect.gen(function* () {
    const hints = yield* PubSub.sliding<void>(1);
    const subscription = yield* PubSub.subscribe(hints);
    yield* Effect.addFinalizer(() => PubSub.shutdown(hints));
    return ScheduleWake.of({
      notify: PubSub.publish(hints, undefined).pipe(Effect.asVoid),
      await: PubSub.take(subscription),
    });
  }),
);

const reportPassFailure = (cause: Cause.Cause<ScheduleProcessFailure>): Effect.Effect<boolean> =>
  Cause.hasInterruptsOnly(cause)
    ? Effect.interrupt
    : Effect.logWarning("Node scheduling pass failed").pipe(
        Effect.annotateLogs({
          failureTag: Option.match(Cause.findErrorOption(cause), {
            onNone: () => "Defect",
            onSome: (error) => error._tag,
          }),
        }),
        Effect.as(false),
      );

const nodeSchedulingDriverLayer = (
  limits: SchedulingLimits,
): Layer.Layer<never, never, ScheduleDriver | ScheduleStore | ScheduleWake> =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const scheduling = yield* ScheduleDriver;
      const store = yield* ScheduleStore;
      const wake = yield* ScheduleWake;

      const run = Effect.gen(function* () {
        while (true) {
          const passSucceeded = yield* scheduling.runDue().pipe(
            Effect.map((pass) => pass.failed === 0),
            Effect.catchCause(reportPassFailure),
          );
          const deadlineResult = passSucceeded
            ? yield* store.nextDeadline().pipe(Effect.result)
            : Result.fail(
                ScheduleStorageError.make({ operation: "driver pass", reason: "unavailable" }),
              );
          if (Result.isFailure(deadlineResult) && passSucceeded) {
            yield* Effect.logWarning("Node scheduling deadline query failed");
          }
          const nowMillis = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
          const deadlineDelay =
            Result.isSuccess(deadlineResult) && deadlineResult.success !== null
              ? Math.max(0, deadlineResult.success - nowMillis)
              : limits.recoveryPollMillis;
          const delay = Math.min(deadlineDelay, limits.recoveryPollMillis);
          yield* Effect.raceFirst(wake.await, Effect.sleep(Duration.millis(delay)));
        }
      });

      yield* Effect.forkScoped(run);
    }),
  );

export interface NodeSchedulingOptions {
  readonly limits?: SchedulingLimits | undefined;
}

/**
 * Optional Scope-owned Node scheduling service and driver. The caller must provide the existing
 * host, its SQLite ScheduleStore, and an explicit ScheduleAuthorizer.
 */
export class NodeScheduling {
  static layer(
    options: NodeSchedulingOptions = {},
  ): Layer.Layer<
    Scheduling,
    ScheduleValidationError,
    NodeDurableHost | ScheduleStore | ScheduleAuthorizer
  > {
    const limits = options.limits ?? defaultSchedulingLimits;
    const schedulingWithDriver = nodeSchedulingDriverLayer(limits).pipe(
      Layer.provide(ScheduleDriver.layer(limits)),
      Layer.merge(Scheduling.layer(limits)),
    );
    return schedulingWithDriver.pipe(
      Layer.provide(
        Layer.mergeAll(nodeScheduledInputAdmissionLayer, nodeScheduleWakeLayer, NodeCrypto.layer),
      ),
    );
  }
}

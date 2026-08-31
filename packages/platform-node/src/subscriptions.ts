import type { AgentId } from "@effect-agent/core";
import {
  type DurableSubmitAgent,
  type EventSources,
  type SubscriptionInputBindings,
  PersistedJson,
  type PreparedInput,
  PreparedInputAdmission,
  type ScheduledEnvelope,
  ScheduledInputAdmission,
  ScheduledInputRetryable,
  type SubscriptionAuthorizer,
  SubscriptionDriver,
  type SubscriptionError,
  SubscriptionIntake,
  type SubscriptionLimits,
  type SubscriptionStoreFailure,
  SubscriptionStore,
  Subscriptions,
  defaultSubscriptionLimits,
  ScheduleStorageError,
} from "@effect-agent/thread";
import { NodeCrypto } from "@effect/platform-node";
import { Cause, Duration, Effect, Exit, Layer, Option } from "effect";

import { NodeDurableHost } from "./host.ts";

const passthroughSubmitAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: { id: agentId, input: PersistedJson },
});

const ambiguous = (): ScheduledInputRetryable =>
  ScheduledInputRetryable.make({ reason: "ambiguous" });

const corrupt = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "corrupt" });

/** Ordinary prepared admission through the Scope-owned Node host gate. */
export const nodePreparedInputAdmissionLayer: Layer.Layer<
  PreparedInputAdmission,
  never,
  NodeDurableHost
> = Layer.effect(
  PreparedInputAdmission,
  Effect.gen(function* () {
    const host = yield* NodeDurableHost;
    return PreparedInputAdmission.of({
      submit: (envelope) =>
        host
          .submit(passthroughSubmitAgent(envelope.agentId), envelope.input, {
            threadId: envelope.threadId,
            principal: envelope.deliveryPrincipal,
            idempotencyKey: envelope.admissionKey,
            definitions: envelope.definitions,
          })
          .pipe(
            Effect.catchTags({
              AdmissionClosed: () =>
                Effect.fail(ScheduledInputRetryable.make({ reason: "host-closed" })),
              AgentInputError: () => Effect.fail(corrupt("prepared admission input")),
              AdmissionConflict: () => Effect.fail(corrupt("prepared admission conflict")),
              DigestError: () => Effect.fail(ambiguous()),
              LedgerError: () => Effect.fail(ambiguous()),
              ThreadStoreError: () => Effect.fail(ambiguous()),
              ThreadNotMaterialized: () => Effect.fail(ambiguous()),
              AppendConflict: () => Effect.fail(ambiguous()),
              FenceRejected: () => Effect.fail(ambiguous()),
              DurableRuntimeFailpointError: () => Effect.fail(ambiguous()),
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

/** Compatibility adapter retaining the public scheduling admission port. */
const nodeScheduledInputAdmissionFromPreparedLayer: Layer.Layer<
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

export const nodeScheduledInputAdmissionLayer: Layer.Layer<
  ScheduledInputAdmission,
  never,
  NodeDurableHost
> = nodeScheduledInputAdmissionFromPreparedLayer.pipe(
  Layer.provide(nodePreparedInputAdmissionLayer),
);

const reportPassFailure = (cause: Cause.Cause<SubscriptionStoreFailure>): Effect.Effect<boolean> =>
  Cause.hasInterruptsOnly(cause)
    ? Effect.interrupt
    : Effect.logWarning("Node subscription pass failed").pipe(
        Effect.annotateLogs({
          failureTag: Option.match(Cause.findErrorOption(cause), {
            onNone: () => "Defect",
            onSome: (error) => error._tag,
          }),
        }),
        Effect.as(false),
      );

const nodeSubscriptionDriverLayer = (
  limits: SubscriptionLimits,
): Layer.Layer<never, never, SubscriptionDriver | SubscriptionStore> =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const driver = yield* SubscriptionDriver;
      const store = yield* SubscriptionStore;

      const run = Effect.gen(function* () {
        while (true) {
          const passSucceeded = yield* driver.runDue.pipe(
            Effect.map((pass) => pass.failed === 0),
            Effect.catchCause(reportPassFailure),
          );

          if (!passSucceeded) {
            yield* Effect.sleep(Duration.millis(limits.retryMillis));
            continue;
          }

          const deadline = yield* store.nextDeadline.pipe(Effect.exit);
          if (Exit.isFailure(deadline)) {
            yield* reportPassFailure(deadline.cause);
            yield* Effect.sleep(Duration.millis(limits.retryMillis));
            continue;
          }

          const nowMillis = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
          const delay =
            deadline.value === null
              ? limits.retryMillis
              : Math.max(1, Math.min(deadline.value - nowMillis, limits.retryMillis));
          yield* Effect.sleep(Duration.millis(delay));
        }
      });

      yield* Effect.forkScoped(run);
    }),
  );

export interface NodeSubscriptionsOptions {
  readonly limits?: SubscriptionLimits | undefined;
}

/**
 * One Scope-owned subscription partition in the sole process owning its SQLite database.
 * Indexed polling repairs restart and lost wake state; closing the Scope interrupts the driver.
 */
export class NodeSubscriptions {
  static layer(
    options: NodeSubscriptionsOptions = {},
  ): Layer.Layer<
    Subscriptions | SubscriptionIntake,
    SubscriptionError,
    | NodeDurableHost
    | SubscriptionStore
    | SubscriptionAuthorizer
    | EventSources
    | SubscriptionInputBindings
  > {
    const limits = options.limits ?? defaultSubscriptionLimits;
    const publicServices = Layer.merge(
      Subscriptions.layer(limits),
      SubscriptionIntake.layer(limits),
    );
    const driver = nodeSubscriptionDriverLayer(limits).pipe(
      Layer.provide(SubscriptionDriver.layer(limits)),
    );
    return Layer.merge(publicServices, driver).pipe(
      Layer.provide(nodePreparedInputAdmissionLayer),
      Layer.provide(NodeCrypto.layer),
    );
  }
}

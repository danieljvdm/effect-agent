import { type EventSources } from "@effect-agent/thread/EventSource";
import { PreparedInputAdmission } from "@effect-agent/thread/PreparedInputAdmission";
import { type ScheduledEnvelope, ScheduledInputAdmission } from "@effect-agent/thread/Schedule";
import {
  type PreparedInput,
  type SubscriptionAuthorizer,
  type SubscriptionError,
  type SubscriptionLimits,
  type SubscriptionStoreFailure,
  SubscriptionStore,
  defaultSubscriptionLimits,
} from "@effect-agent/thread/Subscription";
import { type SubscriptionInputBindings } from "@effect-agent/thread/SubscriptionInput";
import {
  SubscriptionDriver,
  SubscriptionIntake,
  Subscriptions,
} from "@effect-agent/thread/Subscriptions";
import { NodeCrypto } from "@effect/platform-node";
import { Cause, Duration, Effect, Exit, Layer, Option } from "effect";

import { makeNodePreparedInputAdmission, NodeAdmission } from "./internal/prepared-admission.ts";
import { NodeDurableHost } from "./NodeDurableHost.ts";

/** Ordinary prepared admission through the Scope-owned Node host gate. */
export const nodePreparedInputAdmissionLayer: Layer.Layer<
  PreparedInputAdmission,
  never,
  NodeDurableHost
> = Layer.effect(PreparedInputAdmission, makeNodePreparedInputAdmission).pipe(
  Layer.provide(Layer.effect(NodeAdmission, NodeDurableHost)),
);

const preparedFromSchedule = (envelope: ScheduledEnvelope): PreparedInput => ({
  schemaVersion: 1,
  threadId: envelope.threadId,
  deliveryPrincipal: envelope.deliveryPrincipal,
  ...(envelope.admissionGroup === undefined ? {} : { admissionGroup: envelope.admissionGroup }),
  ...(envelope.admissionFence === undefined ? {} : { admissionFence: envelope.admissionFence }),
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

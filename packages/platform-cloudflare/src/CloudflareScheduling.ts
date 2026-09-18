import { Effect, Layer } from "effect";
import {
  type ScheduleStorageError,
  type ScheduleValidationError,
  type ScheduleAuthorizer,
  type SchedulingLimits,
  defaultSchedulingLimits,
} from "effect-agent/schedule";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectState as EffectCfDurableObjectState,
  DurableObjectAlarm,
  WorkerEnvironment,
} from "effect-cf";

import { DurableObjectContext, type ThreadObjectNamespace } from "./CloudflareHostBindings.ts";
import { effectCfAlarmsLayer, restoreEffectCfAlarmError } from "./internal/effect-cf-alarms.ts";
import { effectCfRpcLayer } from "./internal/effect-cf-rpc.ts";
import * as Host from "./ScheduleOwnerHost.ts";
export * from "./ScheduleOwnerHost.ts";

export class CloudflareSchedulingClient extends Host.CloudflareSchedulingClient {
  static override readonly layer = Host.CloudflareSchedulingClient.layer.pipe(
    Layer.provide(effectCfRpcLayer),
  );
}

export const scheduleAlarmHandler = (limits: SchedulingLimits) =>
  Host.alarm(limits).pipe(
    Effect.provide(effectCfAlarmsLayer),
    Effect.catchTag("CloudflareAlarmError", restoreEffectCfAlarmError),
  );

export interface ScheduleOwnerObjectInstance extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<
    Record<never, never>,
    Host.Services | DurableObjectAlarm.DurableObjectAlarm
  >
> {
  schedule(encoded: unknown): Promise<unknown>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

export interface ScheduleOwnerObjectClass {
  new (ctx: DurableObjectState, env: Cloudflare.Env): ScheduleOwnerObjectInstance;
}

/**
 * The host Layer supplies authorization and routing and is cached for the object incarnation.
 * Cloudflare eviction does not guarantee its finalizers run. Do not acquire resources requiring
 * cleanup in this Layer; acquire them inside scoped `manage` / `prepare` operations instead.
 * Native services belong to effect-cf; the database and alarm runtime remain instance-owned.
 */
export const makeScheduleOwnerObjectClass = <E>(
  host: Layer.Layer<
    ScheduleAuthorizer | ThreadObjectNamespace,
    E,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment | Host.ScheduleOwnerIdentity
  >,
  limits: SchedulingLimits = defaultSchedulingLimits,
): ScheduleOwnerObjectClass => {
  const application = Host.makeRuntime(host, limits).pipe(
    Layer.provide(effectCfAlarmsLayer),
    Layer.provideMerge(DurableObjectAlarm.DurableObjectAlarm.layer),
    Layer.provideMerge(
      Layer.effect(DurableObjectContext)(
        Effect.gen(function* () {
          const state = yield* EffectCfDurableObjectState.DurableObjectState;

          return { ctx: state.raw, env: yield* WorkerEnvironment };
        }),
      ),
    ),
    Layer.provideMerge(effectCfRpcLayer),
  );

  const runtime: Layer.Layer<
    Host.Services | DurableObjectAlarm.DurableObjectAlarm,
    E | ScheduleStorageError | Host.ScheduleOwnerProtocolError | ScheduleValidationError,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
  > = Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const scope = yield* Effect.scope;

      return yield* state.blockConcurrencyWhile(Layer.buildWithScope(application, scope));
    }),
  );

  const rpc = {
    ...Host.rpc,
  } satisfies EffectCfDurableObject.DurableObjectRpc<
    Host.Services | DurableObjectAlarm.DurableObjectAlarm
  >;

  const Base = EffectCfDurableObject.make(runtime, {
    initialize: Effect.void,
    rpc,
    alarms: Host.alarm(limits).pipe(
      Effect.catchTag("CloudflareAlarmError", restoreEffectCfAlarmError),
    ),
  });

  class ScheduleOwnerObject extends Base {
    override alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void {
      return super.alarm?.(alarmInfo);
    }
  }

  return ScheduleOwnerObject;
};

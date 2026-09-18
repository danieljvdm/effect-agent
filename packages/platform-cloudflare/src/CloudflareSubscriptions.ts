import { Effect, Layer } from "effect";
import { type EventSources } from "effect-agent/event-source";
import {
  type SubscriptionError,
  type SubscriptionAuthorizer,
  type SubscriptionLimits,
  defaultSubscriptionLimits,
} from "effect-agent/subscription";
import { type SubscriptionInputBindings } from "effect-agent/subscription-input";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectState as EffectCfDurableObjectState,
  DurableObjectAlarm,
  WorkerEnvironment,
} from "effect-cf";

import { DurableObjectContext, type ThreadObjectNamespace } from "./CloudflareHostBindings.ts";
import { effectCfAlarmsLayer, restoreEffectCfAlarmError } from "./internal/effect-cf-alarms.ts";
import { effectCfRpcLayer } from "./internal/effect-cf-rpc.ts";
import * as Host from "./SubscriptionPartitionHost.ts";
export * from "./SubscriptionPartitionHost.ts";

export interface SubscriptionPartitionObjectInstance extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<
    Record<never, never>,
    Host.Services | DurableObjectAlarm.DurableObjectAlarm
  >
> {
  subscription(encoded: unknown): Promise<unknown>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

export interface SubscriptionPartitionObjectClass {
  new (ctx: DurableObjectState, env: Cloudflare.Env): SubscriptionPartitionObjectInstance;
}

/**
 * Build one source-addressed SQLite Durable Object. The host Layer binds the permitted source
 * catalog and authority. It must not retain cleanup-scoped resources across RPC events.
 */
export const makeSubscriptionPartitionObjectClass = <E>(
  host: Layer.Layer<
    SubscriptionAuthorizer | EventSources | SubscriptionInputBindings | ThreadObjectNamespace,
    E,
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
    | Host.SubscriptionPartitionIdentity
  >,
  limits: SubscriptionLimits = defaultSubscriptionLimits,
): SubscriptionPartitionObjectClass => {
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
    | E
    | SubscriptionError
    | Host.SubscriptionPartitionProtocolError
    | Host.CloudflareSubscriptionConfigError
    | Host.SubscriptionAlarmProtocolError,
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

  class SubscriptionPartitionObject extends Base {
    override alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void {
      return super.alarm?.(alarmInfo);
    }
  }

  return SubscriptionPartitionObject;
};

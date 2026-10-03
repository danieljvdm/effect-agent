import { type ThreadObjectNamespace } from "@yielded/agent-platform-cloudflare/cloudflare-host-bindings";
import * as Host from "@yielded/agent-platform-cloudflare/subscription-partition-host";
import { type EventSources } from "@yielded/agent/event-source";
import {
  type SubscriptionAuthorizer,
  defaultSubscriptionLimits,
  type SubscriptionLimits,
} from "@yielded/agent/subscription";
import { type SubscriptionInputBindings } from "@yielded/agent/subscription-input";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Effect, Layer, type Scope } from "effect";

import * as Alarms from "./Alarms.ts";
import {
  acquire,
  ownInstance,
  platformLayer,
  type HostServices,
  type Constructor,
  type NativeHandlers,
} from "./internal/runtime.ts";

export {
  sourcePartitionName,
  makeSubscriptionPartitionAlarmHandler,
  SubscriptionPartitionAlarmExtension,
  SubscriptionPartitionIdentity,
  SubscriptionPartitionNamespace,
  CloudflareSubscriptionsClient,
} from "@yielded/agent-platform-cloudflare/subscription-partition-host";

/** Host subscription routing and its retry obligations in an Alchemy Durable Object. */
export const make = <E>(
  host: Layer.Layer<
    SubscriptionAuthorizer | EventSources | SubscriptionInputBindings | ThreadObjectNamespace,
    E,
    HostServices | Host.SubscriptionPartitionIdentity
  >,
  limits: SubscriptionLimits = defaultSubscriptionLimits,
): Constructor<Rpc> =>
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;

    return Effect.gen(function* () {
      const runtime = Host.makeRuntime(host, limits).pipe(
        Layer.provideMerge(Alarms.layer),
        Layer.provideMerge(platformLayer),
      );

      const { invoke } = yield* acquire(runtime);

      return {
        subscription: (encoded: unknown) => invoke(Host.rpc.subscription(encoded)),
        alarm: () => invoke(Host.alarm(limits)).pipe(Effect.orDie),
      };
    }).pipe(ownInstance, Effect.provideService(WorkerEnvironment, env), Effect.orDie);
  });

export type Rpc = NativeHandlers<typeof Host.rpc> & {
  alarm: () => Effect.Effect<void, never, Scope.Scope>;
};

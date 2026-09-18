import { type ThreadObjectNamespace } from "@effect-agent/platform-cloudflare/cloudflare-host-bindings";
import * as Host from "@effect-agent/platform-cloudflare/subscription-partition-host";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Effect, Layer, type Scope } from "effect";
import { type EventSources } from "effect-agent/event-source";
import {
  type SubscriptionAuthorizer,
  defaultSubscriptionLimits,
  type SubscriptionLimits,
} from "effect-agent/subscription";
import { type SubscriptionInputBindings } from "effect-agent/subscription-input";

import * as Alarms from "./Alarms.ts";
import {
  acquire,
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
} from "@effect-agent/platform-cloudflare/subscription-partition-host";

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
    }).pipe(Effect.provideService(WorkerEnvironment, env), Effect.orDie);
  });

export type Rpc = NativeHandlers<typeof Host.rpc> & {
  alarm: () => Effect.Effect<void, never, Scope.Scope>;
};

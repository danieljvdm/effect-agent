import { type ThreadObjectNamespace } from "@effect-agent/platform-cloudflare/cloudflare-host-bindings";
import * as Host from "@effect-agent/platform-cloudflare/schedule-owner-host";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Effect, Layer, type Scope } from "effect";
import {
  type ScheduleAuthorizer,
  defaultSchedulingLimits,
  type SchedulingLimits,
} from "effect-agent/schedule";

import * as Alarms from "./Alarms.ts";
import {
  acquire,
  platformLayer,
  type HostServices,
  type Constructor,
  type NativeHandlers,
} from "./internal/runtime.ts";

export {
  ScheduleOwnerIdentity,
  ScheduleOwnerNamespace,
  CloudflareSchedulingClient,
} from "@effect-agent/platform-cloudflare/schedule-owner-host";

/** The same durable Schedule Owner, hosted by Alchemy's Effect runtime. */
export const make = <E>(
  host: Layer.Layer<
    ScheduleAuthorizer | ThreadObjectNamespace,
    E,
    HostServices | Host.ScheduleOwnerIdentity
  >,
  limits: SchedulingLimits = defaultSchedulingLimits,
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
        schedule: (encoded: unknown) => invoke(Host.rpc.schedule(encoded)),
        alarm: () => invoke(Host.alarm(limits)).pipe(Effect.orDie),
      };
    }).pipe(Effect.provideService(WorkerEnvironment, env), Effect.orDie);
  });

export type Rpc = NativeHandlers<typeof Host.rpc> & {
  alarm: () => Effect.Effect<void, never, Scope.Scope>;
};

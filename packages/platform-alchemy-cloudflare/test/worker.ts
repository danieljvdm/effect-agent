import { makeDurableObjectBridge, makeWorkerBridge } from "alchemy/Cloudflare/Bridge";
import { DurableObject as AlchemyDurableObject } from "alchemy/Cloudflare/Workers/DurableObject";
import { DurableObjectState as AlchemyState } from "alchemy/Cloudflare/Workers/DurableObjectState";
import { Worker } from "alchemy/Cloudflare/Workers/Worker";
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import type { Scope } from "effect";
import { Config, Context, Effect, Layer } from "effect";
import { HttpServerResponse } from "effect/unstable/http";

import * as ThreadObject from "../src/ThreadObject.ts";
import {
  Memories,
  MemoriesLive,
  Schedules,
  SchedulesLive,
  SubscriptionPartitions,
  SubscriptionPartitionsLive,
} from "./ancillary-fixtures.ts";
import {
  constructorCounts,
  definitions,
  eventFinalizers,
  initializationFinalizers,
  model,
  planner,
} from "./fixtures.ts";

class Threads extends AlchemyDurableObject<Threads, ThreadObject.Rpc>()("THREADS") {}

const ThreadsLive = Threads.make(
  ThreadObject.make(ThreadObject.layer([{ agent: planner, model, definitions }]), {
    namespaceBinding: "THREADS",
    deploymentId: "alchemy-test",
    producerPrefix: "alchemy-test",
    eventLayer: Layer.effectDiscard(
      Effect.gen(function* () {
        const { id } = yield* AlchemyState;

        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            eventFinalizers.push(`${id.name}:thread-event`);
          }),
        );
        if (id.name?.startsWith("fail-event"))
          return yield* Effect.fail("event acquisition failed");
      }),
    ),
  }),
);

class Instance extends Context.Service<
  Instance,
  { readonly name: string; readonly scope: Scope.Scope; readonly label: string }
>()("alchemy-test/Instance") {}

const InstanceLive = Layer.effect(Instance)(
  Effect.gen(function* () {
    const state = yield* AlchemyState;
    const name = state.id.name ?? "unnamed";
    const scope = yield* Effect.scope;
    const label = yield* Config.String("REGISTRATION_LABEL");

    yield* Effect.sync(() => constructorCounts.set(name, (constructorCounts.get(name) ?? 0) + 1));
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        initializationFinalizers.push(name);
      }),
    );
    if (name.startsWith("fail-init")) return yield* Effect.die("initialization failed");

    return { name, scope, label };
  }),
);

interface ProbeRpc {
  onlyFirst?: () => Effect.Effect<string>;
  alarm: () => Effect.Effect<void>;
  inspect: () => Effect.Effect<
    { name: string; label: string; acquisitions: number; differentScope: boolean },
    never,
    Scope.Scope
  >;
  outcome: (
    mode: "success" | "failure" | "defect" | "timeout" | "interrupt",
  ) => Effect.Effect<string, string, Scope.Scope>;
}
class Probes extends AlchemyDurableObject<Probes, ProbeRpc>()("PROBES") {}

const ProbesLive = Probes.make(
  Effect.succeed(
    Effect.gen(function* () {
      const built = yield* Layer.build(InstanceLive);
      const instance = Context.get(built, Instance);

      return {
        ...(instance.name.startsWith("first-")
          ? { onlyFirst: () => Effect.succeed(instance.name) }
          : {}),
        alarm: () => Effect.void,
        inspect: () =>
          Effect.map(Effect.scope, (scope) => ({
            name: instance.name,
            label: instance.label,
            acquisitions: constructorCounts.get(instance.name) ?? 0,
            differentScope: scope !== instance.scope,
          })),
        outcome: (mode: "success" | "failure" | "defect" | "timeout" | "interrupt") =>
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                eventFinalizers.push(`${instance.name}:${mode}`);
              }),
            );
            if (mode === "failure") return yield* Effect.fail("expected failure");
            if (mode === "defect") return yield* Effect.die("expected defect");
            if (mode === "interrupt") return yield* Effect.interrupt;
            if (mode === "timeout")
              return yield* Effect.never.pipe(Effect.timeout(1), Effect.orDie);

            return "ok";
          }),
      };
    }).pipe(Effect.orDie),
  ),
);

export const entrypoint = Worker(
  "AlchemyHostTest",
  { main: import.meta.url },
  Effect.gen(function* () {
    yield* Threads;
    yield* Probes;
    yield* Memories;
    yield* Schedules;
    yield* SubscriptionPartitions;

    return { fetch: Effect.succeed(HttpServerResponse.text("ready")) };
  }).pipe(
    Effect.provide([
      ThreadsLive,
      ProbesLive,
      MemoriesLive,
      SchedulesLive,
      SubscriptionPartitionsLive,
    ]),
  ),
);

// The same entrypoint bridge emitted by Alchemy's bundler; no substitute runtime.
const meta = { entrypoint, stack: { name: "effect-agent-alchemy-test", stage: "test" } };
const bridge = makeDurableObjectBridge(DurableObject, meta);

export class TestThread extends bridge("THREADS") {}
export class TestMemory extends bridge("MEMORIES") {}
export class TestScheduleOwner extends bridge("SCHEDULES") {}
export class TestSubscriptionPartition extends bridge("SUBSCRIPTIONS") {}
export class Probe extends bridge("PROBES") {}
export default makeWorkerBridge(WorkerEntrypoint, meta);

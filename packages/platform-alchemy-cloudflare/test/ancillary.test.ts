import { type MemoryObjectRpc } from "@effect-agent/platform-cloudflare/memory-object-host";
import { type ScheduleOwnerObjectRpc } from "@effect-agent/platform-cloudflare/schedule-owner-host";
import { type SubscriptionPartitionObjectRpc } from "@effect-agent/platform-cloudflare/subscription-partition-host";
import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect, Layer } from "effect";
import { ThreadId } from "effect-agent/identifiers";
import { ScheduleId } from "effect-agent/schedule";
import { scheduleOwnerKey } from "effect-agent/schedule-transition";
import { Scheduling } from "effect-agent/scheduling";
import { SubscriptionIntake, Subscriptions } from "effect-agent/subscriptions";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareMemoryClient } from "../src/MemoryObject.ts";
import { CloudflareSchedulingClient, ScheduleOwnerNamespace } from "../src/Scheduling.ts";
import {
  CloudflareSubscriptionsClient,
  sourcePartitionName,
  SubscriptionPartitionNamespace,
} from "../src/Subscriptions.ts";
import {
  definitionDigests,
  deniedPrincipal,
  memoryAccess,
  memoryPut,
  principal,
  sourceVersion,
} from "./ancillary-fixtures.ts";
import { planner } from "./fixtures.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      MEMORIES: DurableObjectNamespace<MemoryObjectRpc>;
      SCHEDULES: DurableObjectNamespace<ScheduleOwnerObjectRpc>;
      SUBSCRIPTIONS: DurableObjectNamespace<SubscriptionPartitionObjectRpc>;
    }
  }
}

// Routing and delivery are separate bounded passes; match the existing partition test horizon.
const advanceAlarmsUntil = async <A>(
  stub: DurableObjectStub,
  read: () => Promise<A>,
  complete: (value: A) => boolean,
): Promise<A> => {
  let observed = await read();

  for (let round = 0; round < 200 && !complete(observed); round += 1) {
    await runDurableObjectAlarm(stub);
    await new Promise((resolve) => setTimeout(resolve, 10));
    observed = await read();
  }

  return observed;
};

describe("Alchemy ancillary Durable Objects", () => {
  it("enforces memory authorization and durably deduplicates writes across RPCs", async () => {
    const name = `memory-${crypto.randomUUID()}`;
    const write = memoryPut(name);

    const client = (caller = principal) =>
      CloudflareMemoryClient.fromBinding(env.MEMORIES, {
        access: memoryAccess(name),
        principal: caller,
      });

    const denied = await Effect.runPromise(
      client(deniedPrincipal).pipe(
        Effect.flatMap((memory) => memory.change(write)),
        Effect.flip,
      ),
    );

    expect(denied).toMatchObject({ _tag: "MemoryRpcError", reason: "denied" });
    expect(
      await Effect.runPromise(client().pipe(Effect.flatMap((memory) => memory.get(write.key)))),
    ).toBeNull();

    const saved = await Effect.runPromise(
      client().pipe(Effect.flatMap((memory) => memory.change(write))),
    );

    const replay = await Effect.runPromise(
      client().pipe(Effect.flatMap((memory) => memory.change(write))),
    );

    const read = await Effect.runPromise(
      client().pipe(Effect.flatMap((memory) => memory.get(write.key))),
    );

    expect(replay).toEqual(saved);
    expect(read).toEqual(saved);
    expect(read).toMatchObject({ content: { text: "The memory survives its RPC invocation." } });
  });

  it("authorizes a schedule and admits its due occurrence through the native alarm", async () => {
    const name = crypto.randomUUID();
    const owner = { tenantId: `tenant-${name}`, ownerId: "owner" };
    const scope = { owner, principal };
    const scheduleId = ScheduleId.make("once");
    const threadId = ThreadId.make(`scheduled-${name}`);

    const client = CloudflareSchedulingClient.layer.pipe(
      Layer.provide(Layer.succeed(ScheduleOwnerNamespace)({ namespace: env.SCHEDULES })),
    );

    const run = <A, E>(effect: Effect.Effect<A, E, Scheduling>) =>
      Effect.runPromise(effect.pipe(Effect.provide(client)));

    const hashes = await Effect.runPromise(definitionDigests);

    const options = {
      scope,
      scheduleId,
      timing: { _tag: "At" as const, atMillis: 1 },
      destination: { _tag: "ExistingThread" as const, threadId },
      deliveryPrincipal: principal,
      definitions: hashes,
    };

    const denied = await run(
      Scheduling.use((scheduling) =>
        scheduling.create(
          { definition: planner },
          { question: "scheduled" },
          { ...options, scope: { ...scope, principal: deniedPrincipal } },
        ),
      ).pipe(Effect.flip),
    );

    expect(denied).toMatchObject({ _tag: "ScheduleAuthorizationError", code: "denied" });
    expect(await run(Scheduling.use((scheduling) => scheduling.list(scope, {})))).toMatchObject({
      items: [],
    });

    await run(
      Scheduling.use((scheduling) =>
        scheduling.create({ definition: planner }, { question: "scheduled" }, options),
      ),
    );
    const stub = env.SCHEDULES.getByName(scheduleOwnerKey(owner));

    const completed = await advanceAlarmsUntil(
      stub,
      () => run(Scheduling.use((scheduling) => scheduling.get(scope, scheduleId))),
      (snapshot) => snapshot.lastReceipt !== null || snapshot.lastRefusal !== null,
    );

    expect(completed.lastReceipt?.receipt.threadId).toBe(threadId);
    expect(completed.nextAtMillis).toBeNull();
    await runInDurableObject(stub, async (instance) => {
      if (instance.alarm === undefined)
        throw new Error("Schedule owner must export an alarm handler");
      await instance.alarm();
    });
    const repeated = await run(Scheduling.use((scheduling) => scheduling.get(scope, scheduleId)));

    expect(repeated.lastReceipt).toEqual(completed.lastReceipt);
    expect(repeated.nextAtMillis).toBeNull();
    expect(repeated.pending).toBeNull();
  });

  it("denies unauthorized intake and delivers an accepted event once through its partition alarm", async () => {
    const name = crypto.randomUUID();
    const partition = { tenantId: `tenant-${name}`, address: "application:events" };
    const scope = { partition, ownerId: "owner", principal };
    const threadId = ThreadId.make(`subscription-${name}`);
    const subscriptionId = "watch";

    const client = CloudflareSubscriptionsClient.layer(partition).pipe(
      Layer.provide(
        Layer.succeed(SubscriptionPartitionNamespace)({ namespace: env.SUBSCRIPTIONS }),
      ),
    );

    const run = <A, E>(effect: Effect.Effect<A, E, Subscriptions | SubscriptionIntake>) =>
      Effect.runPromise(effect.pipe(Effect.provide(client)));

    const hashes = await Effect.runPromise(definitionDigests);

    const configuration = {
      subscriptionId,
      source: sourceVersion,
      parameters: { topic: "news" },
      context: { instruction: "Summarize" },
      mode: "once" as const,
      expiresAtMillis: null,
      destination: { _tag: "ExistingThread" as const, threadId },
      deliveryPrincipal: principal,
      agentId: planner.id,
      definitions: hashes,
    };

    expect(
      await run(
        Subscriptions.use((subscriptions) =>
          subscriptions.subscribe({ ...scope, principal: deniedPrincipal }, configuration),
        ).pipe(Effect.flip),
      ),
    ).toMatchObject({ _tag: "SubscriptionError", reason: "unauthorized" });
    expect(
      await run(Subscriptions.use((subscriptions) => subscriptions.listSubscriptions(scope))),
    ).toMatchObject({ items: [] });
    await run(Subscriptions.use((subscriptions) => subscriptions.subscribe(scope, configuration)));
    const event = { eventId: "event", topic: "news", message: "A durable event" };

    expect(
      await run(
        SubscriptionIntake.use((intake) =>
          intake.accept(deniedPrincipal, sourceVersion, event),
        ).pipe(Effect.flip),
      ),
    ).toMatchObject({ _tag: "SubscriptionError", reason: "unauthorized" });

    const accepted = await run(
      SubscriptionIntake.use((intake) => intake.accept(principal, sourceVersion, event)),
    );

    const stub = env.SUBSCRIPTIONS.getByName(sourcePartitionName(partition));

    const deliveries = await advanceAlarmsUntil(
      stub,
      () =>
        run(
          Subscriptions.use((subscriptions) =>
            subscriptions.listDeliveries(scope, {
              partition,
              ownerId: scope.ownerId,
              subscriptionId,
            }),
          ),
        ),
      (page) =>
        page.items.some(
          (delivery) => delivery.state === "delivered" || delivery.state === "refused",
        ),
    );

    expect(deliveries.items).toHaveLength(1);
    expect(deliveries.items[0]).toMatchObject({ state: "delivered", receipt: { threadId } });
    expect(
      await run(
        SubscriptionIntake.use((intake) => intake.status(principal, sourceVersion, event.eventId)),
      ),
    ).toMatchObject({ routingComplete: true, routingFailure: null });
    expect(
      await run(SubscriptionIntake.use((intake) => intake.accept(principal, sourceVersion, event))),
    ).toEqual(accepted);
    expect(
      await run(
        Subscriptions.use((subscriptions) =>
          subscriptions.listDeliveries(scope, {
            partition,
            ownerId: scope.ownerId,
            subscriptionId,
          }),
        ),
      ),
    ).toEqual(deliveries);
  });
});

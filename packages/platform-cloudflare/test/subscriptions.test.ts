import {
  CloudflareSubscriptionsClient,
  sourcePartitionName,
  SubscriptionPartitionNamespace,
  validateCloudflareSubscriptionLimits,
} from "@effect-agent/platform-cloudflare/CloudflareSubscriptions";
import { defaultSubscriptionLimits } from "@effect-agent/thread/Subscription";
import { SubscriptionIntake, Subscriptions } from "@effect-agent/thread/Subscriptions";
import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { DateTime, Effect, Layer } from "effect";
import { DurableObject, DurableObjectAlarm } from "effect-cf";
import { expect, it } from "vite-plus/test";

import { laneRows } from "./harness.ts";
import {
  armSubscriptionEviction,
  subscriptionAgentId,
  subscriptionThreadId,
  subscriptionDefinitions,
  subscriptionEvictionsRemaining,
  subscriptionPartition,
  subscriptionPrincipal,
  SubscriptionTestSourceVersion,
} from "./subscription-fixtures.ts";
import type { TestSubscriptionPartitionObject } from "./worker.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      SUBSCRIPTIONS: DurableObjectNamespace<TestSubscriptionPartitionObject>;
    }
  }
}

const partitionName = sourcePartitionName(subscriptionPartition);
const stubFor = () => env.SUBSCRIPTIONS.get(env.SUBSCRIPTIONS.idFromName(partitionName));

const clientLayer = CloudflareSubscriptionsClient.layer(subscriptionPartition).pipe(
  Layer.provide(Layer.succeed(SubscriptionPartitionNamespace)({ namespace: env.SUBSCRIPTIONS })),
);

const runClient = <A, E>(effect: Effect.Effect<A, E, Subscriptions | SubscriptionIntake>) =>
  Effect.runPromise(effect.pipe(Effect.provide(clientLayer)));

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

it("rejects subscription limits that can outlive one safe alarm invocation", async () => {
  const failure = await Effect.runPromise(
    validateCloudflareSubscriptionLimits({
      ...defaultSubscriptionLimits,
      batchSize: 100,
      concurrency: 1,
      operationTimeoutMillis: 300_000,
    }).pipe(Effect.flip),
  );

  expect(failure._tag).toBe("CloudflareSubscriptionConfigError");
});

interface EvictionCase {
  readonly name: string;
  readonly point: string;
  readonly registrations: number;
}

const cases: ReadonlyArray<EvictionCase> = [
  { name: "accepted but unrouted event", point: "subscription:accept:after", registrations: 1 },
  { name: "partial fanout", point: "subscription:select:after", registrations: 2 },
  {
    name: "once selection before preparation",
    point: "subscription:select:after",
    registrations: 1,
  },
  {
    name: "prepared envelope before admission",
    point: "subscription:delivery-prepare:after",
    registrations: 1,
  },
  {
    name: "admission before Receipt recording",
    point: "subscription:admission:after",
    registrations: 1,
  },
];

for (const [caseIndex, row] of cases.entries()) {
  it(`recovers ${row.name} from the persisted partition alarm after eviction`, async () => {
    const topic = `topic-${caseIndex}`;
    const eventId = `event-${caseIndex}`;
    const ownerId = `owner-${caseIndex}`;
    const threads: Array<string> = [];

    for (let index = 0; index < row.registrations; index += 1) {
      const threadId = subscriptionThreadId(`${caseIndex}-${index}`);

      threads.push(threadId);
      await runClient(
        Effect.gen(function* () {
          const subscriptions = yield* Subscriptions;

          yield* subscriptions.subscribe(
            { partition: subscriptionPartition, ownerId, principal: subscriptionPrincipal },
            {
              subscriptionId: `subscription-${caseIndex}-${index}`,
              source: SubscriptionTestSourceVersion,
              parameters: { topic },
              context: { instruction: "continue after the event" },
              mode: "once",
              expiresAtMillis: Date.now() + 60_000,
              destination: { _tag: "ExistingThread", threadId },
              deliveryPrincipal: subscriptionPrincipal,
              agentId: subscriptionAgentId,
              definitions: subscriptionDefinitions,
            },
          );
        }),
      );
    }

    armSubscriptionEviction(partitionName, row.point);
    await runClient(
      Effect.gen(function* () {
        const intake = yield* SubscriptionIntake;

        return yield* intake.accept(subscriptionPrincipal, SubscriptionTestSourceVersion, {
          eventId,
          topic,
          message: "the durable event completed",
        });
      }).pipe(Effect.exit),
    );

    let delivered = false;

    for (let round = 0; round < 200; round += 1) {
      const pageResults = await Promise.allSettled(
        Array.from({ length: row.registrations }, (_, index) =>
          runClient(
            Effect.gen(function* () {
              const subscriptions = yield* Subscriptions;

              return yield* subscriptions.listDeliveries(
                { partition: subscriptionPartition, ownerId, principal: subscriptionPrincipal },
                {
                  partition: subscriptionPartition,
                  ownerId,
                  subscriptionId: `subscription-${caseIndex}-${index}`,
                },
              );
            }),
          ),
        ),
      );

      if (
        pageResults.every(
          (result) =>
            result.status === "fulfilled" &&
            result.value.items.length === 1 &&
            result.value.items[0]?.receipt !== null,
        )
      ) {
        delivered = true;
        break;
      }
      try {
        await runDurableObjectAlarm(stubFor());
      } catch {
        // The armed seam aborted this incarnation. A fresh stub reaches the replacement.
      }
      await sleep(10);
    }

    expect(delivered).toBe(true);
    expect(subscriptionEvictionsRemaining(partitionName)).toBe(0);
    for (const thread of threads) {
      expect(await laneRows(thread)).toHaveLength(1);
    }
  });
}

it("isolates failed and unknown ancillary alarms while advancing native work and preserving replacements", async () => {
  const partition = { tenantId: "alarm-fairness", address: "events" };
  const stub = env.SUBSCRIPTIONS.get(env.SUBSCRIPTIONS.idFromName(sourcePartitionName(partition)));

  // Keep the input gate closed through inspection: an automatic 10ms retry can otherwise
  // consume the newly armed wake between dispatch and getAlarm(), racing this assertion.
  const rows = await runInDurableObject(stub, (instance, state) =>
    state.blockConcurrencyWhile(async () => {
      await instance[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const alarms = yield* DurableObjectAlarm.DurableObjectAlarm;

          for (const [index, tag] of [
            "test/failing",
            "test/unknown",
            "effect-agent/unknown",
            "test/replacement",
            "effect-agent/SubscriptionPartitionWake",
          ].entries()) {
            yield* alarms.scheduleAlarm({
              tag,
              id: tag.startsWith("effect-agent/Subscription") ? "driver" : "one",
              runAt: DateTime.makeUnsafe(Date.now() - 100 + index),
              payload:
                tag === "test/replacement"
                  ? 1
                  : tag.startsWith("effect-agent/Subscription")
                    ? { schemaVersion: 1, generation: 1 }
                    : null,
            });
          }
        }),
      );
      await state.storage.deleteAlarm();
      await instance.alarm();

      try {
        const rows = state.storage.sql
          .exec<{ tag: string; payload: string; run_at: number }>(
            "SELECT tag, payload, run_at FROM effect_cf_scheduled_alarms ORDER BY tag",
          )
          .toArray();

        expect(await state.storage.getAlarm()).not.toBeNull();

        return rows;
      } finally {
        // Stop this fixture's intentionally failing retries after verifying rearm.
        await state.storage.deleteAlarm();
      }
    }),
  );

  expect(rows.map((row) => row.tag)).toEqual([
    "effect-agent/unknown",
    "test/failing",
    "test/replacement",
    "test/unknown",
  ]);
  const replacement = rows.find((row) => row.tag === "test/replacement");

  expect(replacement?.payload).toBe("2");
  expect(replacement?.run_at).toBeGreaterThan(Date.now() + 30_000);
});

it("reserves ancillary callback time within the total partition alarm budget", async () => {
  const limits = {
    ...defaultSubscriptionLimits,
    batchSize: 2,
    concurrency: 1,
    operationTimeoutMillis: 60_000,
  };

  await Effect.runPromise(validateCloudflareSubscriptionLimits(limits));

  const failure = await Effect.runPromise(
    validateCloudflareSubscriptionLimits(limits, { ancillaryAlarms: true }).pipe(Effect.flip),
  );

  expect(failure._tag).toBe("CloudflareSubscriptionConfigError");
});

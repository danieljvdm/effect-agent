import { defaultSubscriptionLimits, SubscriptionIntake, Subscriptions } from "@effect-agent/thread";
import { env, runDurableObjectAlarm } from "cloudflare:test";
import { Effect, Layer } from "effect";
import { expect, it } from "vite-plus/test";

import {
  CloudflareSubscriptionsClient,
  sourcePartitionName,
  SubscriptionPartitionNamespace,
  validateCloudflareSubscriptionLimits,
} from "../src/index.ts";
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

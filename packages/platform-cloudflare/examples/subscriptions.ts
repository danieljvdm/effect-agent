import {
  type EventSources,
  type SubscriptionInputBindings,
  type SourcePartition,
  type SubscriptionAuthorizer,
  SubscriptionIntake,
  Subscriptions,
} from "@effect-agent/thread";
import { Effect, Layer } from "effect";
import type { DurableObjectState, WorkerEnvironment } from "effect-cf";

import {
  CloudflareSubscriptionsClient,
  type ThreadObjectNamespace,
  makeSubscriptionPartitionObjectClass,
  type SubscriptionPartitionIdentity,
  SubscriptionPartitionNamespace,
  type SubscriptionPartitionObjectRpc,
} from "../src/index.ts";

/**
 * Export the returned class under a SQLite Durable Object binding. The cached host Layer binds
 * source behavior and authority; it must acquire cleanup-owned resources inside each operation.
 */
export const makeSubscriptionPartition = <E>(
  host: Layer.Layer<
    SubscriptionAuthorizer | EventSources | SubscriptionInputBindings | ThreadObjectNamespace,
    E,
    DurableObjectState.DurableObjectState | WorkerEnvironment | SubscriptionPartitionIdentity
  >,
) => makeSubscriptionPartitionObjectClass(host);

/** Bind one client to one permitted source partition; every operation creates a fresh RPC stub. */
export const subscriptionClientLayer = (
  partition: SourcePartition,
  namespace: DurableObjectNamespace<SubscriptionPartitionObjectRpc>,
) =>
  CloudflareSubscriptionsClient.layer(partition).pipe(
    Layer.provide(Layer.succeed(SubscriptionPartitionNamespace)({ namespace })),
  );

/** Registration and intake are separate authorities even when a host provides both services. */
export const registerThenAccept = Effect.fn("Example.registerThenAccept")(function* (
  scope: Parameters<Subscriptions["Service"]["subscribe"]>[0],
  options: Parameters<Subscriptions["Service"]["subscribe"]>[1],
  event: {
    readonly principal: Parameters<SubscriptionIntake["Service"]["accept"]>[0];
    readonly payload: unknown;
  },
) {
  const subscriptions = yield* Subscriptions;
  const intake = yield* SubscriptionIntake;
  const registered = yield* subscriptions.subscribe(scope, options);
  const acknowledgement = yield* intake.accept(event.principal, options.source, event.payload);

  return { registered, acknowledgement };
});

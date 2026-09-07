import { type ThreadObjectNamespace } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import {
  CloudflareSubscriptionsClient,
  makeSubscriptionPartitionAlarmHandler,
  makeSubscriptionPartitionObjectClass,
  SubscriptionAlarmExtensionError,
  SubscriptionPartitionAlarmExtension,
  type SubscriptionPartitionIdentity,
  SubscriptionPartitionNamespace,
  type SubscriptionPartitionObjectRpc,
} from "@effect-agent/platform-cloudflare/CloudflareSubscriptions";
import { type EventSources } from "@effect-agent/thread/EventSource";
import { Principal } from "@effect-agent/thread/SubmissionLedger";
import {
  type SourcePartition,
  type SubscriptionAuthorizer,
} from "@effect-agent/thread/Subscription";
import { type SubscriptionInputBindings } from "@effect-agent/thread/SubscriptionInput";
import { SubscriptionIntake, Subscriptions } from "@effect-agent/thread/Subscriptions";
import { Effect, Layer, Schema } from "effect";
import type { DurableObjectState, WorkerEnvironment } from "effect-cf";

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

/** Merge into the host Layer. Only trusted host code may enqueue these verified events. */
export const verifiedEventAlarms = Layer.effect(
  SubscriptionPartitionAlarmExtension,
  makeSubscriptionPartitionAlarmHandler({
    tag: "application/verified-event",
    payload: Schema.Struct({
      eventId: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
      topic: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
      message: Schema.String.check(Schema.isMaxLength(4_096)),
    }),
    timeoutMillis: 30_000,
    handle: ({ payload }) =>
      Effect.gen(function* () {
        const intake = yield* SubscriptionIntake;

        yield* intake.accept(
          Principal.make("application-verifier"),
          { name: "application-event", version: "1" },
          payload,
        );
      }).pipe(
        Effect.mapError(() => SubscriptionAlarmExtensionError.make({ code: "verified-intake" })),
      ),
  }).pipe(Effect.map((handler) => ({ handlers: [handler] }))),
);

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

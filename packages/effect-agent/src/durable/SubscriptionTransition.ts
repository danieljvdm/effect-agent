import { Result, Schema } from "effect";

import { Receipt } from "./DurableAgentRuntime.ts";
import { DefinitionDigests } from "./Records.ts";
import { AdmissionFence } from "./SubmissionLedger.ts";
import {
  type AcceptedEvent,
  type DeliveryChange,
  type SubscriptionChange,
  type SubscriptionLimits,
  type SourcePartition,
  SubscriptionDelivery,
  SubscriptionConfiguration,
  type SubscriptionDelivery as SubscriptionDeliveryType,
  type SubscriptionRecord,
  SubscriptionError,
} from "./Subscription.ts";

const conflict = (code: string) => SubscriptionError.make({ reason: "conflict", code });
const sameDefinitions = Schema.toEquivalence(DefinitionDigests);
const sameReceipt = Schema.toEquivalence(Receipt);

export const sameSourcePartition = (left: SourcePartition, right: SourcePartition): boolean =>
  left.tenantId === right.tenantId && left.address === right.address;

/** The retained event is authoritative. An absent historical occurrence stays unknown on replay. */
export const sameAcceptedEventIdentity = (left: AcceptedEvent, right: AcceptedEvent): boolean =>
  sameSourcePartition(left.partition, right.partition) &&
  left.eventId === right.eventId &&
  left.source.name === right.source.name &&
  left.source.version === right.source.version &&
  left.matchingKey === right.matchingKey &&
  left.payloadDigest === right.payloadDigest &&
  (left.occurredAtMillis === undefined || left.occurredAtMillis === right.occurredAtMillis);

export const subscriptionMatchesEvent = (
  subscription: SubscriptionRecord,
  event: AcceptedEvent,
): boolean =>
  subscription.configuration.source.name === event.source.name &&
  subscription.configuration.source.version === event.source.version &&
  subscription.configuration.matchingKey === event.matchingKey;

export const subscriptionCanSelect = (
  subscription: SubscriptionRecord,
  event: AcceptedEvent,
  nowMillis: number,
  bypassCutoff: boolean,
): boolean =>
  subscriptionMatchesEvent(subscription, event) &&
  subscription.state === "active" &&
  (subscription.configuration.expiresAtMillis === null ||
    subscription.configuration.expiresAtMillis > nowMillis) &&
  (bypassCutoff || subscription.ordinal <= event.cutoff);

export const subscriptionDeliveryCanSelect = (
  delivery: SubscriptionDeliveryType,
  subscription: SubscriptionRecord,
  event: AcceptedEvent,
): boolean => {
  const expectedThread =
    subscription.configuration.destination._tag === "ExistingThread"
      ? subscription.configuration.destination.threadId
      : `subscription:${delivery.deliveryId}`;

  return (
    event.tombstone !== true &&
    delivery.key.eventId === event.eventId &&
    delivery.key.subscription.partition.tenantId === subscription.key.partition.tenantId &&
    delivery.key.subscription.partition.address === subscription.key.partition.address &&
    delivery.key.subscription.ownerId === subscription.key.ownerId &&
    delivery.key.subscription.subscriptionId === subscription.key.subscriptionId &&
    delivery.source.name === event.source.name &&
    delivery.source.version === event.source.version &&
    delivery.subscriptionFingerprint === subscription.configurationFingerprint &&
    delivery.configurationRevision === subscription.configurationRevision &&
    Schema.toEquivalence(SubscriptionConfiguration)(
      delivery.configuration,
      subscription.configuration,
    ) &&
    delivery.eventDigest === event.payloadDigest &&
    delivery.threadId === expectedThread &&
    delivery.admissionKey === `subscription:${delivery.deliveryId}` &&
    delivery.state === "selected" &&
    delivery.envelope === null &&
    delivery.envelopeDigest === null &&
    delivery.receipt === null &&
    delivery.refusal === null
  );
};

export const sameSubscriptionDelivery = (
  left: SubscriptionDeliveryType,
  right: SubscriptionDeliveryType,
): boolean =>
  Schema.encodeSync(Schema.fromJsonString(SubscriptionDelivery))(left) ===
  Schema.encodeSync(Schema.fromJsonString(SubscriptionDelivery))(right);

/** Pure lifecycle transition used inside each adapter's local atomic mutation. */
export const applySubscriptionDeliveryChange = (
  existing: SubscriptionDeliveryType,
  subscription: SubscriptionRecord,
  deliveryId: string,
  change: DeliveryChange,
): Result.Result<SubscriptionDeliveryType, SubscriptionError> => {
  if (existing.deliveryId !== deliveryId) return Result.fail(conflict("stale-delivery"));

  const configuration = existing.configuration;

  switch (change._tag) {
    case "ObserveSettlement":
      if (
        existing.state !== "delivered" ||
        existing.receipt === null ||
        !sameReceipt(existing.receipt, change.receipt)
      )
        return Result.fail(conflict("settlement-receipt"));
      if (existing.settledAtMillis !== undefined) return Result.succeed(existing);

      return Result.succeed({
        ...existing,
        observeSettlement: !change.settled,
        ...(change.settled ? { settledAtMillis: change.nowMillis } : {}),
        retry: { ...existing.retry, nextAttemptAtMillis: change.nextAttemptAtMillis },
      });
    case "Recover":
      if (existing.retry.generation !== change.expectedGeneration) return Result.succeed(existing);
      if (existing.state === "delivered" || existing.state === "refused")
        return Result.succeed(existing);

      return Result.succeed({
        ...existing,
        retry: {
          ...existing.retry,
          generation: existing.retry.generation + 1,
          automaticAttempts: 0,
          parked: false,
          nextAttemptAtMillis: change.nowMillis,
        },
      });
    case "Prepare": {
      if (existing.state === "prepared") {
        return existing.envelope !== null &&
          existing.envelopeDigest === change.envelopeDigest &&
          Schema.encodeSync(Schema.fromJsonString(SubscriptionDelivery.fields.envelope))(
            existing.envelope,
          ) ===
            Schema.encodeSync(Schema.fromJsonString(SubscriptionDelivery.fields.envelope))(
              change.envelope,
            )
          ? Result.succeed(existing)
          : Result.fail(conflict("prepared-envelope"));
      }
      if (existing.state !== "selected") return Result.fail(conflict("delivery-state"));
      if (
        subscription.state === "cancelled" ||
        (configuration.expiresAtMillis !== null &&
          configuration.expiresAtMillis <= change.nowMillis)
      ) {
        return Result.succeed({
          ...existing,
          state: "refused",
          refusal: {
            phase: "preparation",
            code: subscription.state === "cancelled" ? "cancelled" : "expired",
          },
        });
      }
      if (
        change.envelope.threadId !== existing.threadId ||
        change.envelope.admissionKey !== existing.admissionKey ||
        change.envelope.deliveryPrincipal !== configuration.deliveryPrincipal ||
        change.envelope.agentId !== configuration.agentId ||
        change.envelope.admissionGroup !== configuration.admissionGroup ||
        !Schema.toEquivalence(Schema.optional(AdmissionFence))(
          change.envelope.admissionFence,
          configuration.admissionFence,
        ) ||
        !sameDefinitions(change.envelope.definitions, configuration.definitions)
      )
        return Result.fail(conflict("prepared-identity"));

      return Result.succeed({
        ...existing,
        state: "prepared",
        envelope: change.envelope,
        envelopeDigest: change.envelopeDigest,
      });
    }
    case "Complete":
      if (change.receipt.threadId !== existing.threadId)
        return Result.fail(conflict("receipt-thread"));
      if (existing.state === "delivered")
        return existing.receipt !== null && sameReceipt(existing.receipt, change.receipt)
          ? Result.succeed(existing)
          : Result.fail(conflict("receipt"));

      return existing.state === "prepared"
        ? Result.succeed({
            ...existing,
            state: "delivered",
            receipt: change.receipt,
            completedAtMillis: change.nowMillis,
          })
        : Result.fail(conflict("delivery-state"));
    case "Refuse":
      if (existing.state === "refused")
        return existing.refusal?.phase === change.refusal.phase &&
          existing.refusal.code === change.refusal.code
          ? Result.succeed(existing)
          : Result.fail(conflict("refusal"));
      if (
        (change.refusal.phase === "preparation" && existing.state !== "selected") ||
        (change.refusal.phase === "admission" && existing.state !== "prepared")
      )
        return Result.fail(conflict("refusal-phase"));

      return existing.state === "delivered"
        ? Result.fail(conflict("delivery-state"))
        : Result.succeed({
            ...existing,
            state: "refused",
            refusal: change.refusal,
            completedAtMillis: change.nowMillis,
          });
    case "Retry":
      if (existing.state === "delivered" || existing.state === "refused")
        return Result.fail(conflict("delivery-state"));
      if (
        change.retry.generation !== existing.retry.generation ||
        change.retry.attempts <= existing.retry.attempts ||
        change.retry.nextAttemptAtMillis < existing.retry.nextAttemptAtMillis
      )
        return Result.succeed(existing);

      return Result.succeed({ ...existing, retry: change.retry });
  }
};

/** CAS management never mutates a selected or prepared delivery. */
export const applySubscriptionChange = (
  existing: SubscriptionRecord,
  expectedRevision: number,
  change: SubscriptionChange,
): Result.Result<SubscriptionRecord, SubscriptionError> => {
  const revision = existing.configurationRevision;

  if (revision !== expectedRevision)
    return Result.fail(
      SubscriptionError.make({
        reason: "conflict",
        code: "configuration-revision",
        currentRevision: revision,
        currentState: existing.state,
      }),
    );
  if (existing.state === "cancelled" || existing.state === "consumed")
    return Result.fail(conflict("subscription-state"));
  if (change._tag === "Pause" && existing.state !== "active")
    return Result.fail(conflict("subscription-state"));
  if (change._tag === "Resume" && existing.state !== "paused")
    return Result.fail(conflict("subscription-state"));

  return Result.succeed({
    ...existing,
    creationConfiguration: existing.creationConfiguration,
    configurationRevision: revision + 1,
    ...(change._tag === "Update"
      ? {
          configuration: change.configuration,
          configurationFingerprint: change.configurationFingerprint,
          recovery: change.recovery,
        }
      : change._tag === "Recover"
        ? {
            recovery:
              existing.recovery === null
                ? null
                : { ...existing.recovery, nextAttemptAtMillis: change.nowMillis },
          }
        : { state: change._tag === "Pause" ? ("paused" as const) : ("active" as const) }),
  });
};

/** Called only after retained identity lookup: unfinished work remains replayable past the horizon. */
export const validateEventRetention = (
  event: AcceptedEvent,
  limits: SubscriptionLimits,
  nowMillis: number,
): Result.Result<void, SubscriptionError> => {
  if (limits.retention === undefined) return Result.void;
  if (
    event.occurredAtMillis === undefined ||
    event.occurredAtMillis > nowMillis ||
    event.occurredAtMillis <= nowMillis - limits.retention.replayHorizonMillis
  )
    return Result.fail(
      SubscriptionError.make({ reason: "validation", code: "event-replay-horizon" }),
    );

  return Result.void;
};

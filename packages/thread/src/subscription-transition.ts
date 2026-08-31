import { Result, Schema } from "effect";

import { Receipt } from "./durable-runtime.ts";
import { DefinitionDigests } from "./records.ts";
import {
  type AcceptedEvent,
  type DeliveryChange,
  type SourcePartition,
  SubscriptionDelivery,
  type SubscriptionDelivery as SubscriptionDeliveryType,
  type SubscriptionRecord,
  SubscriptionError,
} from "./subscription.ts";

const conflict = (code: string) => SubscriptionError.make({ reason: "conflict", code });
const sameDefinitions = Schema.toEquivalence(DefinitionDigests);
const sameReceipt = Schema.toEquivalence(Receipt);

export const sameSourcePartition = (left: SourcePartition, right: SourcePartition): boolean =>
  left.tenantId === right.tenantId && left.address === right.address;

export const sameAcceptedEventIdentity = (left: AcceptedEvent, right: AcceptedEvent): boolean =>
  sameSourcePartition(left.partition, right.partition) &&
  left.eventId === right.eventId &&
  left.source.name === right.source.name &&
  left.source.version === right.source.version &&
  left.matchingKey === right.matchingKey &&
  left.payloadDigest === right.payloadDigest;

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
  subscription.configuration.expiresAtMillis > nowMillis &&
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
    delivery.key.eventId === event.eventId &&
    delivery.key.subscription.partition.tenantId === subscription.key.partition.tenantId &&
    delivery.key.subscription.partition.address === subscription.key.partition.address &&
    delivery.key.subscription.ownerId === subscription.key.ownerId &&
    delivery.key.subscription.subscriptionId === subscription.key.subscriptionId &&
    delivery.source.name === event.source.name &&
    delivery.source.version === event.source.version &&
    delivery.subscriptionFingerprint === subscription.creationFingerprint &&
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
  switch (change._tag) {
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
        subscription.configuration.expiresAtMillis <= change.nowMillis
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
        change.envelope.deliveryPrincipal !== subscription.configuration.deliveryPrincipal ||
        change.envelope.agentId !== subscription.configuration.agentId ||
        !sameDefinitions(change.envelope.definitions, subscription.configuration.definitions)
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
        ? Result.succeed({ ...existing, state: "delivered", receipt: change.receipt })
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
        : Result.succeed({ ...existing, state: "refused", refusal: change.refusal });
    case "Retry":
      if (existing.state === "delivered" || existing.state === "refused")
        return Result.fail(conflict("delivery-state"));
      if (
        change.retry.attempts <= existing.retry.attempts ||
        change.retry.nextAttemptAtMillis < existing.retry.nextAttemptAtMillis
      )
        return Result.succeed(existing);
      return Result.succeed({ ...existing, retry: change.retry });
  }
};

import { AgentId, ThreadId } from "@effect-agent/core";
import { Context, Effect, Schema } from "effect";

import { Receipt } from "./durable-runtime.ts";
import { IdempotencyKey, Principal } from "./ledger.ts";
import { DefinitionDigests, Digest, PersistedJson } from "./records.ts";
import {
  ScheduleAuthorizationDecision,
  ScheduleDestination,
  ScheduleInstant,
  ScheduleRetry,
} from "./schedule.ts";

export const SubscriptionName = Schema.NonEmptyString.check(
  Schema.isMaxLength(256),
  Schema.isPattern(/^[^\p{Surrogate}]*$/u),
);

const Positive = Schema.Int.check(Schema.isGreaterThan(0));

/** Stable storage identity. Deployment and payload values must never change its routing. */
export const SourcePartition = Schema.Struct({
  tenantId: SubscriptionName,
  address: SubscriptionName,
});

export type SourcePartition = typeof SourcePartition.Type;

export const EventSourceVersion = Schema.Struct({
  name: SubscriptionName,
  version: SubscriptionName,
});

export type EventSourceVersion = typeof EventSourceVersion.Type;

export const SubscriptionKey = Schema.Struct({
  partition: SourcePartition,
  ownerId: SubscriptionName,
  subscriptionId: SubscriptionName,
});

export type SubscriptionKey = typeof SubscriptionKey.Type;

export const SubscriptionScope = Schema.Struct({
  partition: SourcePartition,
  ownerId: SubscriptionName,
  principal: Principal,
});

export type SubscriptionScope = typeof SubscriptionScope.Type;

export const SubscriptionConfiguration = Schema.Struct({
  source: EventSourceVersion,
  matchingKey: SubscriptionName,
  parameters: PersistedJson,
  context: PersistedJson,
  mode: Schema.Literals(["once", "continuous"]),
  expiresAtMillis: ScheduleInstant,
  destination: ScheduleDestination,
  deliveryPrincipal: Principal,
  agentId: AgentId,
  definitions: DefinitionDigests,
});

export type SubscriptionConfiguration = typeof SubscriptionConfiguration.Type;

export const SourceRecovery = Schema.Struct({
  attempts: Schema.Natural,
  /** Null retains a conclusive source failure without continuing provider polling. */
  nextAttemptAtMillis: Schema.NullOr(ScheduleInstant),
  lastFailure: Schema.NullOr(SubscriptionName),
}).check(
  Schema.makeFilter((value) => value.nextAttemptAtMillis !== null || value.lastFailure !== null),
);

export const SubscriptionRecord = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  key: SubscriptionKey,
  creationFingerprint: Digest,
  createdBy: Principal,
  createdAtMillis: ScheduleInstant,
  /** Assigned atomically in the same order as event eligibility cutoffs. */
  ordinal: Schema.Natural,
  configuration: SubscriptionConfiguration,
  state: Schema.Literals(["active", "consumed", "cancelled"]),
  recovery: Schema.NullOr(SourceRecovery),
}).check(
  Schema.makeFilter(
    (value) =>
      (value.state !== "consumed" || value.configuration.mode === "once") &&
      (value.recovery === null ||
        (value.configuration.mode === "once" && value.state === "active")),
  ),
);

export type SubscriptionRecord = typeof SubscriptionRecord.Type;

export const AcceptedEvent = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  partition: SourcePartition,
  eventId: SubscriptionName,
  source: EventSourceVersion,
  matchingKey: SubscriptionName,
  payload: PersistedJson,
  payloadDigest: Digest,
  acceptedAtMillis: ScheduleInstant,
  cutoff: Schema.Natural,
  cursor: Schema.Natural,
  routingComplete: Schema.Boolean,
  routingFailure: Schema.NullOr(SubscriptionName),
  nextAttemptAtMillis: ScheduleInstant,
}).check(Schema.makeFilter((value) => value.cursor <= value.cutoff));

export type AcceptedEvent = typeof AcceptedEvent.Type;

export const EventAcknowledgement = Schema.Struct({
  partition: SourcePartition,
  eventId: SubscriptionName,
  acceptedAtMillis: ScheduleInstant,
});

export type EventAcknowledgement = typeof EventAcknowledgement.Type;

/** Common admission data; neither a schedule cursor nor a subscription lifecycle lives here. */
export const PreparedInput = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  threadId: ThreadId,
  deliveryPrincipal: Principal,
  agentId: AgentId,
  definitions: DefinitionDigests,
  input: PersistedJson,
  inputDigest: Digest,
  admissionKey: IdempotencyKey,
  authorization: ScheduleAuthorizationDecision,
});

export type PreparedInput = typeof PreparedInput.Type;

export const SubscriptionDeliveryKey = Schema.Struct({
  subscription: SubscriptionKey,
  eventId: SubscriptionName,
});

export type SubscriptionDeliveryKey = typeof SubscriptionDeliveryKey.Type;

export const SubscriptionRefusal = Schema.Struct({
  phase: Schema.Literals(["preparation", "admission"]),
  code: SubscriptionName,
});

export const SubscriptionDelivery = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  key: SubscriptionDeliveryKey,
  deliveryId: Digest,
  subscriptionFingerprint: Digest,
  eventDigest: Digest,
  source: EventSourceVersion,
  threadId: ThreadId,
  admissionKey: IdempotencyKey,
  selectedAtMillis: ScheduleInstant,
  state: Schema.Literals(["selected", "prepared", "delivered", "refused"]),
  envelope: Schema.NullOr(PreparedInput),
  envelopeDigest: Schema.NullOr(Digest),
  retry: ScheduleRetry,
  receipt: Schema.NullOr(Receipt),
  refusal: Schema.NullOr(SubscriptionRefusal),
}).check(
  Schema.makeFilter((value) => {
    const hasEnvelope = value.envelope !== null && value.envelopeDigest !== null;
    const noEnvelope = value.envelope === null && value.envelopeDigest === null;

    switch (value.state) {
      case "selected":
        return noEnvelope && value.receipt === null && value.refusal === null;
      case "prepared":
        return hasEnvelope && value.receipt === null && value.refusal === null;
      case "delivered":
        return hasEnvelope && value.receipt !== null && value.refusal === null;
      case "refused":
        return (
          value.receipt === null &&
          value.refusal !== null &&
          (value.refusal.phase === "preparation" ? noEnvelope : hasEnvelope)
        );
    }
  }),
);

export type SubscriptionDelivery = typeof SubscriptionDelivery.Type;

/** Management projections intentionally exclude parameters, context, payload, and input. */
export const SubscriptionSnapshot = Schema.Struct({
  key: SubscriptionKey,
  source: EventSourceVersion,
  mode: Schema.Literals(["once", "continuous"]),
  state: Schema.Literals(["active", "consumed", "cancelled", "expired"]),
  createdAtMillis: ScheduleInstant,
  expiresAtMillis: ScheduleInstant,
  recovery: Schema.NullOr(SourceRecovery),
});

export type SubscriptionSnapshot = typeof SubscriptionSnapshot.Type;

export const SubscriptionDeliverySnapshot = Schema.Struct({
  key: SubscriptionDeliveryKey,
  state: SubscriptionDelivery.fields.state,
  retry: ScheduleRetry,
  receipt: Schema.NullOr(Receipt),
  refusal: Schema.NullOr(SubscriptionRefusal),
});

export class SubscriptionError extends Schema.TaggedError<SubscriptionError>()(
  "SubscriptionError",
  {
    reason: Schema.Literals([
      "validation",
      "conflict",
      "not-found",
      "capacity",
      "unauthorized",
      "unsupported-source",
      "unsupported-binding",
      "storage",
      "corrupt",
    ]),
    code: SubscriptionName,
  },
) {}

/** Host source adapters classify expected failures; raw provider diagnostics are never stored. */
export class SubscriptionSourceError extends Schema.TaggedError<SubscriptionSourceError>()(
  "SubscriptionSourceError",
  {
    code: SubscriptionName,
    retryable: Schema.Boolean,
  },
) {}

export class SubscriptionFailpointError extends Schema.TaggedError<SubscriptionFailpointError>()(
  "SubscriptionFailpointError",
  { point: Schema.String },
) {}

export type SubscriptionStoreFailure = SubscriptionError | SubscriptionFailpointError;

export const SubscriptionFailpoint = Context.Reference<{
  readonly hit: (point: string) => Effect.Effect<void, SubscriptionFailpointError>;
}>("@effect-agent/thread/SubscriptionFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

export const SubscriptionLimits = Schema.Struct({
  maxRegistrations: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
  maxRegistrationsPerOwner: Positive.check(Schema.isLessThanOrEqualTo(10_000)),
  maxEvents: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
  maxDeliveries: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
  maxDeliveriesPerOwner: Positive.check(Schema.isLessThanOrEqualTo(10_000)),
  maxPayloadBytes: Positive.check(Schema.isLessThanOrEqualTo(65_536)),
  maxContextBytes: Positive.check(Schema.isLessThanOrEqualTo(16_384)),
  maxLifetimeMillis: Positive.check(Schema.isLessThanOrEqualTo(8_640_000_000_000_000)),
  batchSize: Positive.check(Schema.isLessThanOrEqualTo(100)),
  concurrency: Positive.check(Schema.isLessThanOrEqualTo(16)),
  retryMillis: Positive.check(Schema.isLessThanOrEqualTo(86_400_000)),
  operationTimeoutMillis: Positive.check(Schema.isLessThanOrEqualTo(300_000)),
});

export type SubscriptionLimits = typeof SubscriptionLimits.Type;

export const defaultSubscriptionLimits: SubscriptionLimits = {
  maxRegistrations: 10_000,
  maxRegistrationsPerOwner: 1_000,
  maxEvents: 10_000,
  maxDeliveries: 10_000,
  maxDeliveriesPerOwner: 1_000,
  maxPayloadBytes: 65_536,
  maxContextBytes: 16_384,
  maxLifetimeMillis: 30 * 86_400_000,
  batchSize: 16,
  concurrency: 4,
  retryMillis: 30_000,
  operationTimeoutMillis: 30_000,
};

export const DeliveryChange = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Prepare"),
    envelope: PreparedInput,
    envelopeDigest: Digest,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({ _tag: Schema.Literal("Complete"), receipt: Receipt, nowMillis: ScheduleInstant }),
  Schema.Struct({
    _tag: Schema.Literal("Refuse"),
    refusal: SubscriptionRefusal,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Retry"),
    retry: ScheduleRetry,
    nowMillis: ScheduleInstant,
  }),
]);

export type DeliveryChange = typeof DeliveryChange.Type;

/** Progress of bounded recovery sweeps, independent of individual work success. */
export const SubscriptionScanCursors = Schema.Struct({
  events: Schema.String,
  deliveries: Schema.String,
  recovery: Schema.Natural,
});

export type SubscriptionScanCursors = typeof SubscriptionScanCursors.Type;

/**
 * One store instance owns exactly one tenant/source partition. Every mutation is local and
 * atomic, including required Cloudflare wakes. Node has one process owner per database.
 * Records are retained for the partition lifetime; hard quotas reject new retained work.
 * No pruning may discard creation replay, deduplication, or unfinished-work evidence.
 */
export class SubscriptionStore extends Context.Service<
  SubscriptionStore,
  {
    readonly partition: SourcePartition;
    readonly readScanCursors: Effect.Effect<SubscriptionScanCursors, SubscriptionError>;
    readonly advanceScanCursors: (
      cursors: SubscriptionScanCursors,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    readonly register: (
      record: SubscriptionRecord,
      limits: SubscriptionLimits,
    ) => Effect.Effect<SubscriptionRecord, SubscriptionStoreFailure>;
    readonly get: (
      key: SubscriptionKey,
    ) => Effect.Effect<SubscriptionRecord | null, SubscriptionError>;
    readonly list: (
      ownerId: string,
      after: number,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SubscriptionRecord>, SubscriptionError>;
    readonly cancel: (
      key: SubscriptionKey,
    ) => Effect.Effect<SubscriptionRecord, SubscriptionStoreFailure>;
    /** Duplicate identity returns the original cutoff and routing progress; payload/version conflicts fail. */
    readonly accept: (
      event: AcceptedEvent,
      limits: SubscriptionLimits,
    ) => Effect.Effect<AcceptedEvent, SubscriptionStoreFailure>;
    readonly event: (eventId: string) => Effect.Effect<AcceptedEvent | null, SubscriptionError>;
    readonly pendingEvents: (
      nowMillis: number,
      after: string,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<string>, SubscriptionError>;
    /** Indexed by partition, source/version, matchingKey, ordinal; never scan arbitrary parameters. */
    readonly candidates: (
      event: AcceptedEvent,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SubscriptionRecord>, SubscriptionError>;
    /** Recheck cursor, eligibility, lifecycle and expiry; consume once + insert obligations + cursor together. */
    readonly select: (
      event: AcceptedEvent,
      deliveries: ReadonlyArray<SubscriptionDelivery>,
      cursor: number,
      complete: boolean,
      nowMillis: number,
      limits: SubscriptionLimits,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    /** Only the explicitly reconciled watch may bypass cutoff, never reopen ordinary routing. */
    readonly catchUp: (
      event: AcceptedEvent,
      delivery: SubscriptionDelivery,
      nowMillis: number,
      limits: SubscriptionLimits,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    readonly deferEvent: (
      eventId: string,
      nextAttemptAtMillis: number,
      code?: string,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    readonly delivery: (
      key: SubscriptionDeliveryKey,
    ) => Effect.Effect<SubscriptionDelivery | null, SubscriptionError>;
    readonly pendingDeliveries: (
      nowMillis: number,
      after: string,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SubscriptionDeliveryKey>, SubscriptionError>;
    readonly listDeliveries: (
      key: SubscriptionKey,
      after: string,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SubscriptionDelivery>, SubscriptionError>;
    /**
     * A matching deliveryId fences stale outcomes. Prepare rechecks cancellation and expiry atomically.
     * Retry advances only with a higher attempt count and a nondecreasing next-attempt time.
     */
    readonly changeDelivery: (
      key: SubscriptionDeliveryKey,
      deliveryId: Digest,
      change: DeliveryChange,
    ) => Effect.Effect<SubscriptionDelivery, SubscriptionStoreFailure>;
    readonly recovering: (
      nowMillis: number,
      after: number,
      limit: number,
    ) => Effect.Effect<
      ReadonlyArray<{ readonly key: SubscriptionKey; readonly ordinal: number }>,
      SubscriptionError
    >;
    readonly deferRecovery: (
      key: SubscriptionKey,
      recovery: typeof SourceRecovery.Type | null,
    ) => Effect.Effect<void, SubscriptionStoreFailure>;
    readonly nextDeadline: Effect.Effect<number | null, SubscriptionError>;
  }
>()("@effect-agent/thread/SubscriptionStore") {}

/** Explicit host policy. Management scope is not inferred from handle possession. */
export class SubscriptionAuthorizer extends Context.Service<
  SubscriptionAuthorizer,
  {
    readonly manage: (
      operation: "subscribe" | "list" | "cancel" | "deliveries",
      scope: SubscriptionScope,
      configuration?: SubscriptionConfiguration,
    ) => Effect.Effect<void, SubscriptionError>;
    readonly intake: (
      partition: SourcePartition,
      source: EventSourceVersion,
      principal: Principal,
    ) => Effect.Effect<void, SubscriptionError>;
    /** Host recovery authority, independent of the principal who registered the watch. */
    readonly reconcile: (
      subscription: SubscriptionRecord,
    ) => Effect.Effect<void, SubscriptionError>;
    readonly prepare: (
      subscription: SubscriptionRecord,
      event: AcceptedEvent,
    ) => Effect.Effect<typeof ScheduleAuthorizationDecision.Type, SubscriptionError>;
  }
>()("@effect-agent/thread/SubscriptionAuthorizer") {}

export const subscriptionKeyString = (key: SubscriptionKey): string =>
  JSON.stringify([key.partition.tenantId, key.partition.address, key.ownerId, key.subscriptionId]);

export const subscriptionDeliveryKeyString = (key: SubscriptionDeliveryKey): string =>
  JSON.stringify([key.subscription.ownerId, key.subscription.subscriptionId, key.eventId]);

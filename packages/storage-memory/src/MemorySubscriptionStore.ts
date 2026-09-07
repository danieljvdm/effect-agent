import { Digest } from "@effect-agent/thread/Records";
import { compareScheduleNames } from "@effect-agent/thread/ScheduleTransition";
import {
  AcceptedEvent,
  DeliveryChange,
  SubscriptionChange,
  SourcePartition,
  SubscriptionDelivery,
  SubscriptionDeliveryKey,
  SubscriptionError,
  SubscriptionFailpoint,
  SubscriptionKey,
  SubscriptionLimits,
  SubscriptionRetentionPolicy,
  SubscriptionName,
  SubscriptionRecord,
  SubscriptionScanCursors,
  SubscriptionStore,
  subscriptionDeliveryKeyString,
  subscriptionKeyString,
} from "@effect-agent/thread/Subscription";
import {
  sameAcceptedEventIdentity,
  applySubscriptionDeliveryChange,
  applySubscriptionChange,
  validateEventRetention,
  subscriptionCanSelect,
  subscriptionDeliveryCanSelect,
} from "@effect-agent/thread/SubscriptionTransition";
import { Clock, Effect, Layer, Ref, Result, Schema } from "effect";

interface MemorySubscriptionState {
  readonly sequence: number;
  readonly retentionDeadline: number | null;
  readonly tombstoneCount: number;
  readonly eventKeys: ReadonlyArray<string>;
  readonly deliveryKeys: ReadonlyArray<string>;
  readonly maintenanceEvents: string;
  readonly maintenanceDeliveries: string;
  readonly recoveryCounts: ReadonlyMap<string, number>;
  readonly eventDeliveryCounts: ReadonlyMap<string, number>;
  readonly retentionHorizon: number | null;
  readonly registrations: ReadonlyMap<string, string>;
  readonly events: ReadonlyMap<string, string>;
  readonly deliveries: ReadonlyMap<string, string>;
  readonly registrationIndex: ReadonlyMap<string, RegistrationIndex>;
  readonly candidateIndex: ReadonlyMap<string, ReadonlyArray<string>>;
  readonly ownerRegistrationCounts: ReadonlyMap<string, number>;
  readonly eventIndex: ReadonlyMap<string, EventIndex>;
  readonly deliveryIndex: ReadonlyMap<string, DeliveryIndex>;
  readonly ownerDeliveryCounts: ReadonlyMap<string, number>;
  readonly scanCursors: SubscriptionScanCursors;
}

interface RegistrationIndex {
  readonly key: SubscriptionKey;
  readonly ordinal: number;
  readonly state: SubscriptionRecord["state"];
  readonly recoveryKey: string | null;
  readonly recoveryAt: number | null;
}
interface EventIndex {
  readonly routingComplete: boolean;
  readonly nextAttemptAtMillis: number;
}
interface DeliveryIndex {
  readonly key: SubscriptionDeliveryKey;
  readonly state: SubscriptionDelivery["state"];
  readonly parked?: boolean;
  readonly observeSettlement?: boolean;
  readonly nextAttemptAtMillis: number;
}

const error = (reason: SubscriptionError["reason"], code: string) =>
  SubscriptionError.make({ reason, code });

const samePartition = (left: SourcePartition, right: SourcePartition): boolean =>
  left.tenantId === right.tenantId && left.address === right.address;

const sameSource = (left: AcceptedEvent["source"], right: AcceptedEvent["source"]): boolean =>
  left.name === right.name && left.version === right.version;

const encode = <A, I>(
  schema: Schema.Codec<A, I>,
  value: A,
  code: string,
): Result.Result<string, SubscriptionError> =>
  Result.try({
    try: () => Schema.encodeSync(Schema.fromJsonString(schema))(value),
    catch: () => error("corrupt", code),
  });

const decode = <A, I>(
  schema: Schema.Codec<A, I>,
  value: string,
  code: string,
): Result.Result<A, SubscriptionError> =>
  Result.try({
    try: () => Schema.decodeSync(Schema.fromJsonString(schema))(value),
    catch: () => error("corrupt", code),
  });

const decodeEffect = <A, I>(schema: Schema.Codec<A, I>, value: string, code: string) =>
  Effect.fromResult(decode(schema, value, code));

const validate = <A, I>(schema: Schema.Codec<A, I>, value: unknown, code: string) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => error("validation", code)));

const jsonBytes = (value: unknown): number =>
  new TextEncoder().encode(JSON.stringify(value)).byteLength;

const sameDeliveryIdentity = (left: SubscriptionDelivery, right: SubscriptionDelivery): boolean =>
  subscriptionDeliveryKeyString(left.key) === subscriptionDeliveryKeyString(right.key) &&
  left.deliveryId === right.deliveryId &&
  left.source.name === right.source.name &&
  left.source.version === right.source.version &&
  left.threadId === right.threadId &&
  left.admissionKey === right.admissionKey &&
  left.subscriptionFingerprint === right.subscriptionFingerprint &&
  left.eventDigest === right.eventDigest;

const candidateIndexKey = (record: SubscriptionRecord): string =>
  JSON.stringify([
    record.configuration.source.name,
    record.configuration.source.version,
    record.configuration.matchingKey,
  ]);

const eventCandidateIndexKey = (event: AcceptedEvent): string =>
  JSON.stringify([event.source.name, event.source.version, event.matchingKey]);

const sameEventIdentity = sameAcceptedEventIdentity;

const deliveryBelongsTo = (
  delivery: SubscriptionDelivery,
  record: SubscriptionRecord,
  event: AcceptedEvent,
): boolean =>
  subscriptionKeyString(delivery.key.subscription) === subscriptionKeyString(record.key) &&
  delivery.key.eventId === event.eventId &&
  sameSource(delivery.source, event.source);

// Ordered in-memory indexes allow bounded maintenance pages without scanning retained values.
const removeKeys = (
  keys: ReadonlyArray<string>,
  removed: ReadonlySet<string>,
): ReadonlyArray<string> => {
  if (removed.size === 0) return keys;
  const result = [...keys];

  for (const key of removed) {
    const index = upperBound(result, key) - 1;

    if (result[index] === key) result.splice(index, 1);
  }

  return result;
};

const upperBound = (keys: ReadonlyArray<string>, after: string): number => {
  let low = 0;
  let high = keys.length;

  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    const key = keys[middle];

    if (key !== undefined && compareScheduleNames(key, after) <= 0) low = middle + 1;
    else high = middle;
  }

  return low;
};

const insertKey = (keys: ReadonlyArray<string>, key: string): ReadonlyArray<string> => {
  const at = upperBound(keys, key);

  if (at > 0 && keys[at - 1] === key) return keys;

  return [...keys.slice(0, at), key, ...keys.slice(at)];
};

const recoveryCountsAfter = (
  current: MemorySubscriptionState,
  next: ReadonlyMap<string, RegistrationIndex>,
  keys: ReadonlyArray<string>,
): ReadonlyMap<string, number> => {
  const counts = new Map(current.recoveryCounts);

  for (const key of keys) {
    const before = current.registrationIndex.get(key)?.recoveryKey;
    const after = next.get(key)?.recoveryKey;

    if (before === after) continue;
    if (before !== null && before !== undefined) counts.set(before, (counts.get(before) ?? 0) - 1);
    if (after !== null && after !== undefined) counts.set(after, (counts.get(after) ?? 0) + 1);
  }

  return counts;
};

const makeMemorySubscriptionStore = Effect.fn("makeMemorySubscriptionStore")(function* (
  ownedPartition: SourcePartition,
) {
  const partition = yield* validate(SourcePartition, ownedPartition, "partition");

  const state = yield* Ref.make<MemorySubscriptionState>({
    sequence: 0,
    retentionDeadline: null,
    tombstoneCount: 0,
    eventKeys: [],
    deliveryKeys: [],
    maintenanceEvents: "",
    maintenanceDeliveries: "",
    recoveryCounts: new Map(),
    eventDeliveryCounts: new Map(),
    retentionHorizon: null,
    registrations: new Map(),
    events: new Map(),
    deliveries: new Map(),
    registrationIndex: new Map(),
    candidateIndex: new Map(),
    ownerRegistrationCounts: new Map(),
    eventIndex: new Map(),
    deliveryIndex: new Map(),
    ownerDeliveryCounts: new Map(),
    scanCursors: { events: "", deliveries: "", recovery: 0 },
  });

  const failpoint = yield* SubscriptionFailpoint;

  const requirePartition = <A extends { readonly partition: SourcePartition }>(
    value: A,
    code: string,
  ) =>
    samePartition(value.partition, partition)
      ? Effect.succeed(value)
      : Effect.fail(error("validation", code));

  const requireKey = (key: SubscriptionKey, code: string) =>
    validate(SubscriptionKey, key, code).pipe(
      Effect.flatMap((decoded) => requirePartition(decoded, code)),
    );

  const register: SubscriptionStore["Service"]["register"] = Effect.fn(
    "MemorySubscriptionStore.register",
  )(function* (input, inputLimits) {
    const record = yield* validate(SubscriptionRecord, input, "register-record");
    const limits = yield* validate(SubscriptionLimits, inputLimits, "register-limits");

    yield* requirePartition(record.key, "register-partition");
    yield* failpoint.hit("subscription:register:before");

    const result = yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (
          current,
        ): readonly [
          Result.Result<SubscriptionRecord, SubscriptionError>,
          MemorySubscriptionState,
        ] => {
          const key = subscriptionKeyString(record.key);
          const existingText = current.registrations.get(key);

          if (existingText !== undefined) {
            const existing = decode(SubscriptionRecord, existingText, "register-existing");

            if (Result.isFailure(existing)) return [Result.fail(existing.failure), current];

            return existing.success.creationFingerprint === record.creationFingerprint
              ? [Result.succeed(existing.success), current]
              : [Result.fail(error("conflict", "registration-identity")), current];
          }
          if (jsonBytes(record.configuration.context) > limits.maxContextBytes)
            return [Result.fail(error("capacity", "context-bytes")), current];
          if (jsonBytes(record.configuration.parameters) > limits.maxPayloadBytes)
            return [Result.fail(error("capacity", "parameters-bytes")), current];
          if (
            record.configuration.expiresAtMillis !== null &&
            record.configuration.expiresAtMillis - record.createdAtMillis > limits.maxLifetimeMillis
          )
            return [Result.fail(error("capacity", "lifetime")), current];
          if (current.registrations.size >= limits.maxRegistrations)
            return [Result.fail(error("capacity", "registrations")), current];
          const ownerCount = current.ownerRegistrationCounts.get(record.key.ownerId) ?? 0;

          if (ownerCount >= limits.maxRegistrationsPerOwner)
            return [Result.fail(error("capacity", "owner-registrations")), current];
          const assigned = { ...record, ordinal: current.sequence + 1 };
          const encoded = encode(SubscriptionRecord, assigned, "register-encode");

          if (Result.isFailure(encoded)) return [Result.fail(encoded.failure), current];
          const registrations = new Map(current.registrations);

          registrations.set(key, encoded.success);
          const registrationIndex = new Map(current.registrationIndex);

          registrationIndex.set(key, {
            key: assigned.key,
            ordinal: assigned.ordinal,
            state: assigned.state,
            recoveryKey: assigned.recovery === null ? null : candidateIndexKey(assigned),
            recoveryAt: assigned.recovery?.nextAttemptAtMillis ?? null,
          });
          const candidateIndex = new Map(current.candidateIndex);
          const candidateKey = candidateIndexKey(assigned);

          candidateIndex.set(candidateKey, [...(candidateIndex.get(candidateKey) ?? []), key]);
          const ownerRegistrationCounts = new Map(current.ownerRegistrationCounts);

          ownerRegistrationCounts.set(assigned.key.ownerId, ownerCount + 1);

          return [
            Result.succeed(assigned),
            {
              ...current,
              sequence: assigned.ordinal,
              registrations,
              registrationIndex,
              recoveryCounts: recoveryCountsAfter(current, registrationIndex, [key]),
              candidateIndex,
              ownerRegistrationCounts,
            },
          ];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));

    yield* failpoint.hit("subscription:register:after");

    return result;
  });

  const get: SubscriptionStore["Service"]["get"] = Effect.fn("MemorySubscriptionStore.get")(
    function* (input) {
      const key = yield* requireKey(input, "get-key");
      const text = (yield* Ref.get(state)).registrations.get(subscriptionKeyString(key));

      return text === undefined
        ? null
        : yield* decodeEffect(SubscriptionRecord, text, "get-record");
    },
  );

  const list: SubscriptionStore["Service"]["list"] = Effect.fn("MemorySubscriptionStore.list")(
    function* (ownerId, after, limit) {
      if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit <= 0)
        return yield* error("validation", "list-page");
      const current = yield* Ref.get(state);
      const records: Array<SubscriptionRecord> = [];

      for (const [storageKey, indexed] of current.registrationIndex) {
        if (indexed.key.ownerId !== ownerId || indexed.ordinal <= after) continue;
        const text = current.registrations.get(storageKey);

        if (text === undefined) return yield* error("corrupt", "list-index");
        records.push(yield* decodeEffect(SubscriptionRecord, text, "list-record"));
      }
      records.sort((a, b) => a.ordinal - b.ordinal);

      return records.slice(0, limit);
    },
  );

  const change: SubscriptionStore["Service"]["change"] = Effect.fn(
    "MemorySubscriptionStore.change",
  )(function* (input, expectedRevision, inputChange) {
    const key = yield* requireKey(input, "change-key");
    const change = yield* validate(SubscriptionChange, inputChange, "change");

    yield* validate(Schema.Int.check(Schema.isGreaterThan(0)), expectedRevision, "revision");
    yield* failpoint.hit("subscription:change:before");

    const updated = yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (
          current,
        ): readonly [
          Result.Result<SubscriptionRecord, SubscriptionError>,
          MemorySubscriptionState,
        ] => {
          const storageKey = subscriptionKeyString(key);
          const text = current.registrations.get(storageKey);

          if (text === undefined) return [Result.fail(error("not-found", "subscription")), current];
          const existing = decode(SubscriptionRecord, text, "change-record");

          if (Result.isFailure(existing)) return [existing, current];
          const updated = applySubscriptionChange(existing.success, expectedRevision, change);

          if (Result.isFailure(updated)) return [updated, current];
          const revised = { ...updated.success, ordinal: current.sequence + 1 };
          const encoded = encode(SubscriptionRecord, revised, "change-encode");

          if (Result.isFailure(encoded)) return [Result.fail(encoded.failure), current];
          const registrations = new Map(current.registrations);

          registrations.set(storageKey, encoded.success);
          const registrationIndex = new Map(current.registrationIndex);

          registrationIndex.set(storageKey, {
            key,
            ordinal: revised.ordinal,
            state: revised.state,
            recoveryKey: revised.recovery === null ? null : candidateIndexKey(revised),
            recoveryAt: revised.recovery?.nextAttemptAtMillis ?? null,
          });
          const candidateIndex = new Map(current.candidateIndex);
          const oldKey = candidateIndexKey(existing.success);
          const nextKey = candidateIndexKey(revised);

          {
            candidateIndex.set(
              oldKey,
              (candidateIndex.get(oldKey) ?? []).filter((key) => key !== storageKey),
            );
            candidateIndex.set(
              nextKey,
              [...(candidateIndex.get(nextKey) ?? []), storageKey].sort(
                (a, b) =>
                  (registrationIndex.get(a)?.ordinal ?? 0) -
                  (registrationIndex.get(b)?.ordinal ?? 0),
              ),
            );
          }

          return [
            Result.succeed(revised),
            {
              ...current,
              sequence: revised.ordinal,
              registrations,
              registrationIndex,
              candidateIndex,
              recoveryCounts: recoveryCountsAfter(current, registrationIndex, [storageKey]),
            },
          ];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));

    yield* failpoint.hit("subscription:change:after");

    return updated;
  });

  const cancel: SubscriptionStore["Service"]["cancel"] = Effect.fn(
    "MemorySubscriptionStore.cancel",
  )(function* (input, expectedRevision) {
    const key = yield* requireKey(input, "cancel-key");

    yield* failpoint.hit("subscription:cancel:before");

    const result = yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (
          current,
        ): readonly [
          Result.Result<SubscriptionRecord, SubscriptionError>,
          MemorySubscriptionState,
        ] => {
          const storageKey = subscriptionKeyString(key);
          const text = current.registrations.get(storageKey);

          if (text === undefined) return [Result.fail(error("not-found", "subscription")), current];
          const decoded = decode(SubscriptionRecord, text, "cancel-record");

          if (Result.isFailure(decoded)) return [decoded, current];
          if (
            expectedRevision !== undefined &&
            expectedRevision !== decoded.success.configurationRevision &&
            !(
              decoded.success.state === "cancelled" &&
              expectedRevision + 1 === decoded.success.configurationRevision
            )
          )
            return [
              Result.fail(
                SubscriptionError.make({
                  reason: "conflict",
                  code: "configuration-revision",
                  currentRevision: decoded.success.configurationRevision,
                  currentState: decoded.success.state,
                }),
              ),
              current,
            ];
          if (decoded.success.state === "cancelled")
            return [Result.succeed(decoded.success), current];

          const cancelled = {
            ...decoded.success,
            configurationRevision: decoded.success.configurationRevision + 1,
            state: "cancelled" as const,
            recovery: null,
          };

          const encoded = encode(SubscriptionRecord, cancelled, "cancel-encode");

          if (Result.isFailure(encoded)) return [Result.fail(encoded.failure), current];
          const registrations = new Map(current.registrations);

          registrations.set(storageKey, encoded.success);
          const registrationIndex = new Map(current.registrationIndex);
          const indexed = registrationIndex.get(storageKey);

          if (indexed === undefined)
            return [Result.fail(error("corrupt", "cancel-index")), current];
          registrationIndex.set(storageKey, {
            ...indexed,
            state: "cancelled",
            recoveryAt: null,
            recoveryKey: null,
          });

          return [
            Result.succeed(cancelled),
            {
              ...current,
              registrations,
              registrationIndex,
              recoveryCounts: recoveryCountsAfter(current, registrationIndex, [storageKey]),
            },
          ];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));

    yield* failpoint.hit("subscription:cancel:after");

    return result;
  });

  const accept: SubscriptionStore["Service"]["accept"] = Effect.fn(
    "MemorySubscriptionStore.accept",
  )(function* (input, inputLimits) {
    const event = yield* validate(AcceptedEvent, input, "accept-event");
    const currentTimeMillis = yield* Clock.currentTimeMillis;
    const limits = yield* validate(SubscriptionLimits, inputLimits, "accept-limits");

    yield* requirePartition(event, "accept-partition");
    yield* failpoint.hit("subscription:accept:before");

    const result = yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (
          current,
        ): readonly [Result.Result<AcceptedEvent, SubscriptionError>, MemorySubscriptionState] => {
          const existingText = current.events.get(event.eventId);

          if (existingText !== undefined) {
            const existing = decode(AcceptedEvent, existingText, "accept-existing");

            if (Result.isFailure(existing)) return [Result.fail(existing.failure), current];

            return sameEventIdentity(existing.success, event)
              ? [Result.succeed(existing.success), current]
              : [Result.fail(error("conflict", "event-identity")), current];
          }
          if (
            current.retentionHorizon !== null &&
            current.retentionHorizon !== limits.retention?.replayHorizonMillis
          )
            return [Result.fail(error("conflict", "retention-horizon")), current];
          const horizon = validateEventRetention(event, limits, currentTimeMillis);

          if (Result.isFailure(horizon)) return [Result.fail(horizon.failure), current];
          if (jsonBytes(event.payload) > limits.maxPayloadBytes)
            return [Result.fail(error("capacity", "payload-bytes")), current];
          if (current.events.size - current.tombstoneCount >= limits.maxEvents)
            return [Result.fail(error("capacity", "events")), current];

          const accepted: AcceptedEvent = {
            ...event,
            cutoff: current.sequence + 1,
            cursor: 0,
            routingComplete: false,
            routingFailure: null,
          };

          const encoded = encode(AcceptedEvent, accepted, "accept-encode");

          if (Result.isFailure(encoded)) return [Result.fail(encoded.failure), current];
          const events = new Map(current.events);

          events.set(accepted.eventId, encoded.success);
          const eventIndex = new Map(current.eventIndex);

          eventIndex.set(accepted.eventId, {
            routingComplete: false,
            nextAttemptAtMillis: accepted.nextAttemptAtMillis,
          });

          return [
            Result.succeed(accepted),
            {
              ...current,
              sequence: accepted.cutoff,
              events,
              eventIndex,
              eventKeys: insertKey(current.eventKeys, accepted.eventId),
              retentionHorizon: limits.retention?.replayHorizonMillis ?? current.retentionHorizon,
              retentionDeadline:
                limits.retention === undefined
                  ? current.retentionDeadline
                  : accepted.acceptedAtMillis,
            },
          ];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));

    yield* failpoint.hit("subscription:accept:after");

    return result;
  });

  const event: SubscriptionStore["Service"]["event"] = Effect.fn("MemorySubscriptionStore.event")(
    function* (eventId) {
      const text = (yield* Ref.get(state)).events.get(eventId);

      return text === undefined ? null : yield* decodeEffect(AcceptedEvent, text, "event-record");
    },
  );

  const pendingEvents: SubscriptionStore["Service"]["pendingEvents"] = Effect.fn(
    "MemorySubscriptionStore.pendingEvents",
  )(function* (nowMillis, after, limit) {
    const events: Array<string> = [];

    for (const [eventId, indexed] of (yield* Ref.get(state)).eventIndex) {
      if (
        !indexed.routingComplete &&
        indexed.nextAttemptAtMillis <= nowMillis &&
        compareScheduleNames(eventId, after) > 0
      )
        events.push(eventId);
    }
    events.sort(compareScheduleNames);

    return events.slice(0, limit);
  });

  const candidates: SubscriptionStore["Service"]["candidates"] = Effect.fn(
    "MemorySubscriptionStore.candidates",
  )(function* (input, limit) {
    const accepted = yield* validate(AcceptedEvent, input, "candidates-event");

    yield* requirePartition(accepted, "candidates-partition");
    const stored = yield* event(accepted.eventId);

    if (stored === null || !sameEventIdentity(stored, accepted))
      return yield* error(stored === null ? "not-found" : "conflict", "event");
    const current = yield* Ref.get(state);
    const records: Array<SubscriptionRecord> = [];

    for (const storageKey of current.candidateIndex.get(eventCandidateIndexKey(stored)) ?? []) {
      const indexed = current.registrationIndex.get(storageKey);

      if (indexed === undefined) return yield* error("corrupt", "candidate-index");
      if (indexed.ordinal <= stored.cursor || indexed.ordinal > stored.cutoff) continue;
      const text = current.registrations.get(storageKey);

      if (text === undefined) return yield* error("corrupt", "candidate-record");
      records.push(yield* decodeEffect(SubscriptionRecord, text, "candidate-record"));
    }
    records.sort((a, b) => a.ordinal - b.ordinal);

    return records.slice(0, limit);
  });

  const select: SubscriptionStore["Service"]["select"] = Effect.fn(
    "MemorySubscriptionStore.select",
  )(function* (inputEvent, inputDeliveries, cursor, complete, nowMillis, inputLimits) {
    const suppliedEvent = yield* validate(AcceptedEvent, inputEvent, "select-event");

    const deliveries = yield* validate(
      Schema.Array(SubscriptionDelivery),
      inputDeliveries,
      "select-deliveries",
    );

    const limits = yield* validate(SubscriptionLimits, inputLimits, "select-limits");

    yield* requirePartition(suppliedEvent, "select-partition");
    yield* failpoint.hit("subscription:select:before");
    const effectiveNowMillis = Math.max(nowMillis, yield* Clock.currentTimeMillis);

    yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (current): readonly [Result.Result<void, SubscriptionError>, MemorySubscriptionState] => {
          const eventText = current.events.get(suppliedEvent.eventId);

          if (eventText === undefined) return [Result.fail(error("not-found", "event")), current];
          const decodedEvent = decode(AcceptedEvent, eventText, "select-event-record");

          if (Result.isFailure(decodedEvent)) return [Result.fail(decodedEvent.failure), current];
          const accepted = decodedEvent.success;

          if (
            !sameEventIdentity(accepted, suppliedEvent) ||
            suppliedEvent.cursor !== accepted.cursor
          )
            return [Result.fail(error("conflict", "event-cursor")), current];
          if (accepted.routingComplete)
            return [
              complete && cursor === accepted.cursor
                ? Result.void
                : Result.fail(error("conflict", "routing-complete")),
              current,
            ];
          if (!Number.isSafeInteger(cursor) || cursor < accepted.cursor || cursor > accepted.cutoff)
            return [Result.fail(error("validation", "cursor")), current];

          const additions: Array<readonly [string, string]> = [];
          const updates: Array<readonly [string, string]> = [];
          const additionIndex: Array<readonly [string, DeliveryIndex, string]> = [];
          const registrationUpdates: Array<readonly [string, RegistrationIndex]> = [];
          const owners = new Map(current.ownerDeliveryCounts);

          for (const delivery of deliveries) {
            const recordText = current.registrations.get(
              subscriptionKeyString(delivery.key.subscription),
            );

            if (recordText === undefined)
              return [Result.fail(error("not-found", "subscription")), current];
            const record = decode(SubscriptionRecord, recordText, "select-registration");

            if (Result.isFailure(record)) return [Result.fail(record.failure), current];
            if (
              !deliveryBelongsTo(delivery, record.success, accepted) ||
              !subscriptionDeliveryCanSelect(delivery, record.success, accepted) ||
              record.success.ordinal <= accepted.cursor ||
              record.success.ordinal > cursor
            )
              return [Result.fail(error("conflict", "selection")), current];
            if (!subscriptionCanSelect(record.success, accepted, effectiveNowMillis, false))
              continue;
            const deliveryKey = subscriptionDeliveryKeyString(delivery.key);
            const existingText = current.deliveries.get(deliveryKey);

            if (existingText !== undefined) {
              const existing = decode(SubscriptionDelivery, existingText, "select-existing");

              if (Result.isFailure(existing)) return [Result.fail(existing.failure), current];
              if (!sameDeliveryIdentity(existing.success, delivery))
                return [Result.fail(error("conflict", "delivery-identity")), current];
              continue;
            }

            const encodedDelivery = encode(
              SubscriptionDelivery,
              delivery,
              "select-delivery-encode",
            );

            if (Result.isFailure(encodedDelivery))
              return [Result.fail(encodedDelivery.failure), current];
            additions.push([deliveryKey, encodedDelivery.success]);
            additionIndex.push([
              deliveryKey,
              {
                key: delivery.key,
                state: delivery.state,
                nextAttemptAtMillis: delivery.retry.nextAttemptAtMillis,
              },
              record.success.key.ownerId,
            ]);
            const ownerId = record.success.key.ownerId;

            owners.set(ownerId, (owners.get(ownerId) ?? 0) + 1);
            if ((owners.get(ownerId) ?? 0) > limits.maxDeliveriesPerOwner)
              return [Result.fail(error("capacity", "owner-deliveries")), current];
            if (record.success.configuration.mode === "once") {
              const consumed = { ...record.success, state: "consumed" as const, recovery: null };

              const encodedRecord = encode(
                SubscriptionRecord,
                consumed,
                "select-registration-encode",
              );

              if (Result.isFailure(encodedRecord))
                return [Result.fail(encodedRecord.failure), current];
              const consumedKey = subscriptionKeyString(consumed.key);

              updates.push([consumedKey, encodedRecord.success]);
              const indexed = current.registrationIndex.get(consumedKey);

              if (indexed === undefined)
                return [Result.fail(error("corrupt", "selection-index")), current];
              registrationUpdates.push([
                consumedKey,
                { ...indexed, state: "consumed", recoveryAt: null, recoveryKey: null },
              ]);
            }
          }
          if (current.deliveries.size + additions.length > limits.maxDeliveries)
            return [Result.fail(error("capacity", "deliveries")), current];
          const registrations = new Map(current.registrations);

          for (const [key, value] of updates) registrations.set(key, value);
          const nextDeliveries = new Map(current.deliveries);

          for (const [key, value] of additions) nextDeliveries.set(key, value);
          const deliveryIndex = new Map(current.deliveryIndex);

          for (const [key, value] of additionIndex) deliveryIndex.set(key, value);
          const registrationIndex = new Map(current.registrationIndex);

          for (const [key, value] of registrationUpdates) registrationIndex.set(key, value);

          const eventDeliveryCounts = new Map(current.eventDeliveryCounts);

          eventDeliveryCounts.set(
            accepted.eventId,
            (eventDeliveryCounts.get(accepted.eventId) ?? 0) + additions.length,
          );

          const nextEvent: AcceptedEvent = {
            ...accepted,
            cursor,
            routingComplete: complete,
            routingFailure: null,
          };

          const encodedEvent = encode(AcceptedEvent, nextEvent, "select-event-encode");

          if (Result.isFailure(encodedEvent)) return [Result.fail(encodedEvent.failure), current];
          const events = new Map(current.events);

          events.set(nextEvent.eventId, encodedEvent.success);
          const eventIndex = new Map(current.eventIndex);

          eventIndex.set(nextEvent.eventId, {
            routingComplete: nextEvent.routingComplete,
            nextAttemptAtMillis: nextEvent.nextAttemptAtMillis,
          });

          return [
            Result.void,
            {
              ...current,
              registrations,
              registrationIndex,
              recoveryCounts: recoveryCountsAfter(
                current,
                registrationIndex,
                registrationUpdates.map(([key]) => key),
              ),
              eventDeliveryCounts,
              events,
              eventIndex,
              deliveries: nextDeliveries,
              deliveryKeys: additions.reduce(
                (keys, [key]) => insertKey(keys, key),
                current.deliveryKeys,
              ),
              deliveryIndex,
              ownerDeliveryCounts: owners,
            },
          ];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));
    yield* failpoint.hit("subscription:select:after");
  });

  const catchUp: SubscriptionStore["Service"]["catchUp"] = Effect.fn(
    "MemorySubscriptionStore.catchUp",
  )(function* (inputEvent, inputDelivery, nowMillis, inputLimits) {
    const suppliedEvent = yield* validate(AcceptedEvent, inputEvent, "catch-up-event");
    const delivery = yield* validate(SubscriptionDelivery, inputDelivery, "catch-up-delivery");
    const limits = yield* validate(SubscriptionLimits, inputLimits, "catch-up-limits");

    yield* requirePartition(suppliedEvent, "catch-up-partition");
    yield* failpoint.hit("subscription:catch-up:before");
    const effectiveNowMillis = Math.max(nowMillis, yield* Clock.currentTimeMillis);

    yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (current): readonly [Result.Result<void, SubscriptionError>, MemorySubscriptionState] => {
          const eventText = current.events.get(suppliedEvent.eventId);

          const recordText = current.registrations.get(
            subscriptionKeyString(delivery.key.subscription),
          );

          if (eventText === undefined || recordText === undefined)
            return [
              Result.fail(error("not-found", eventText === undefined ? "event" : "subscription")),
              current,
            ];
          const accepted = decode(AcceptedEvent, eventText, "catch-up-event-record");

          if (Result.isFailure(accepted)) return [Result.fail(accepted.failure), current];
          const record = decode(SubscriptionRecord, recordText, "catch-up-registration");

          if (Result.isFailure(record)) return [Result.fail(record.failure), current];
          if (
            !sameEventIdentity(accepted.success, suppliedEvent) ||
            !deliveryBelongsTo(delivery, record.success, accepted.success) ||
            !subscriptionDeliveryCanSelect(delivery, record.success, accepted.success) ||
            record.success.configuration.mode !== "once"
          )
            return [Result.fail(error("conflict", "catch-up-identity")), current];
          const key = subscriptionDeliveryKeyString(delivery.key);
          const existingText = current.deliveries.get(key);

          if (existingText !== undefined) {
            const existing = decode(SubscriptionDelivery, existingText, "catch-up-existing");

            if (Result.isFailure(existing)) return [Result.fail(existing.failure), current];

            return sameDeliveryIdentity(existing.success, delivery)
              ? [Result.void, current]
              : [Result.fail(error("conflict", "delivery-identity")), current];
          }
          if (!subscriptionCanSelect(record.success, accepted.success, effectiveNowMillis, true))
            return [Result.fail(error("conflict", "catch-up-eligibility")), current];
          if (current.deliveries.size >= limits.maxDeliveries)
            return [Result.fail(error("capacity", "deliveries")), current];
          const ownerCount = current.ownerDeliveryCounts.get(record.success.key.ownerId) ?? 0;

          if (ownerCount >= limits.maxDeliveriesPerOwner)
            return [Result.fail(error("capacity", "owner-deliveries")), current];

          const encodedDelivery = encode(
            SubscriptionDelivery,
            delivery,
            "catch-up-delivery-encode",
          );

          if (Result.isFailure(encodedDelivery))
            return [Result.fail(encodedDelivery.failure), current];
          const consumed = { ...record.success, state: "consumed" as const, recovery: null };

          const encodedRecord = encode(
            SubscriptionRecord,
            consumed,
            "catch-up-registration-encode",
          );

          if (Result.isFailure(encodedRecord)) return [Result.fail(encodedRecord.failure), current];
          const deliveries = new Map(current.deliveries);

          deliveries.set(key, encodedDelivery.success);
          const registrations = new Map(current.registrations);
          const consumedKey = subscriptionKeyString(consumed.key);

          registrations.set(consumedKey, encodedRecord.success);
          const registrationIndex = new Map(current.registrationIndex);
          const indexed = registrationIndex.get(consumedKey);

          if (indexed === undefined)
            return [Result.fail(error("corrupt", "catch-up-index")), current];
          registrationIndex.set(consumedKey, {
            ...indexed,
            state: "consumed",
            recoveryAt: null,
            recoveryKey: null,
          });
          const deliveryIndex = new Map(current.deliveryIndex);

          deliveryIndex.set(key, {
            key: delivery.key,
            state: delivery.state,
            nextAttemptAtMillis: delivery.retry.nextAttemptAtMillis,
          });
          const ownerDeliveryCounts = new Map(current.ownerDeliveryCounts);
          const eventDeliveryCounts = new Map(current.eventDeliveryCounts);

          eventDeliveryCounts.set(
            accepted.success.eventId,
            (eventDeliveryCounts.get(accepted.success.eventId) ?? 0) + 1,
          );

          ownerDeliveryCounts.set(consumed.key.ownerId, ownerCount + 1);

          return [
            Result.void,
            {
              ...current,
              deliveries,
              deliveryIndex,
              deliveryKeys: insertKey(current.deliveryKeys, key),
              ownerDeliveryCounts,
              registrations,
              registrationIndex,
              recoveryCounts: recoveryCountsAfter(current, registrationIndex, [consumedKey]),
              eventDeliveryCounts,
            },
          ];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));
    yield* failpoint.hit("subscription:catch-up:after");
  });

  const deferEvent: SubscriptionStore["Service"]["deferEvent"] = Effect.fn(
    "MemorySubscriptionStore.deferEvent",
  )(function* (eventId, nextAttemptAtMillis, code) {
    const routingFailure =
      code === undefined
        ? "routing-failed"
        : yield* validate(SubscriptionName, code, "routing-failure");

    yield* failpoint.hit("subscription:defer-event:before");
    yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (current): readonly [Result.Result<void, SubscriptionError>, MemorySubscriptionState] => {
          const text = current.events.get(eventId);

          if (text === undefined) return [Result.fail(error("not-found", "event")), current];
          const accepted = decode(AcceptedEvent, text, "defer-event-record");

          if (Result.isFailure(accepted)) return [Result.fail(accepted.failure), current];
          const updated = { ...accepted.success, nextAttemptAtMillis, routingFailure };
          const encoded = encode(AcceptedEvent, updated, "defer-event-encode");

          if (Result.isFailure(encoded)) return [Result.fail(encoded.failure), current];
          const events = new Map(current.events);

          events.set(eventId, encoded.success);
          const eventIndex = new Map(current.eventIndex);
          const indexed = eventIndex.get(eventId);

          if (indexed === undefined)
            return [Result.fail(error("corrupt", "defer-event-index")), current];
          eventIndex.set(eventId, { ...indexed, nextAttemptAtMillis });

          return [Result.void, { ...current, events, eventIndex }];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));
    yield* failpoint.hit("subscription:defer-event:after");
  });

  const delivery: SubscriptionStore["Service"]["delivery"] = Effect.fn(
    "MemorySubscriptionStore.delivery",
  )(function* (input) {
    const key = yield* validate(SubscriptionDeliveryKey, input, "delivery-key");

    yield* requirePartition(key.subscription, "delivery-partition");
    const text = (yield* Ref.get(state)).deliveries.get(subscriptionDeliveryKeyString(key));

    return text === undefined
      ? null
      : yield* decodeEffect(SubscriptionDelivery, text, "delivery-record");
  });

  const pendingDeliveries: SubscriptionStore["Service"]["pendingDeliveries"] = Effect.fn(
    "MemorySubscriptionStore.pendingDeliveries",
  )(function* (nowMillis, after, limit) {
    const items: Array<SubscriptionDeliveryKey> = [];

    for (const [storageKey, item] of (yield* Ref.get(state)).deliveryIndex) {
      if (
        ((item.state !== "delivered" && item.state !== "refused" && item.parked !== true) ||
          (item.state === "delivered" && item.observeSettlement === true)) &&
        item.nextAttemptAtMillis <= nowMillis &&
        compareScheduleNames(storageKey, after) > 0
      )
        items.push(item.key);
    }
    items.sort((a, b) =>
      compareScheduleNames(subscriptionDeliveryKeyString(a), subscriptionDeliveryKeyString(b)),
    );

    return items.slice(0, limit);
  });

  const listDeliveries: SubscriptionStore["Service"]["listDeliveries"] = Effect.fn(
    "MemorySubscriptionStore.listDeliveries",
  )(function* (input, after, limit) {
    const key = yield* requireKey(input, "list-deliveries-key");
    const items: Array<SubscriptionDelivery> = [];

    for (const text of (yield* Ref.get(state)).deliveries.values()) {
      const item = yield* decodeEffect(SubscriptionDelivery, text, "list-delivery");
      const itemKey = subscriptionDeliveryKeyString(item.key);

      if (
        subscriptionKeyString(item.key.subscription) === subscriptionKeyString(key) &&
        compareScheduleNames(itemKey, after) > 0
      )
        items.push(item);
    }
    items.sort((a, b) =>
      compareScheduleNames(
        subscriptionDeliveryKeyString(a.key),
        subscriptionDeliveryKeyString(b.key),
      ),
    );

    return items.slice(0, limit);
  });

  const changeDelivery: SubscriptionStore["Service"]["changeDelivery"] = Effect.fn(
    "MemorySubscriptionStore.changeDelivery",
  )(function* (inputKey, inputDeliveryId, inputChange) {
    const key = yield* validate(SubscriptionDeliveryKey, inputKey, "change-delivery-key");
    const deliveryId = yield* validate(Digest, inputDeliveryId, "change-delivery-id");
    const change = yield* validate(DeliveryChange, inputChange, "change-delivery-change");

    yield* requirePartition(key.subscription, "change-delivery-partition");
    yield* failpoint.hit(`subscription:delivery-${change._tag.toLowerCase()}:before`);

    const effectiveChange =
      change._tag === "Prepare"
        ? { ...change, nowMillis: Math.max(change.nowMillis, yield* Clock.currentTimeMillis) }
        : change;

    const result = yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (
          current,
        ): readonly [
          Result.Result<SubscriptionDelivery, SubscriptionError>,
          MemorySubscriptionState,
        ] => {
          const storageKey = subscriptionDeliveryKeyString(key);
          const text = current.deliveries.get(storageKey);

          if (text === undefined) return [Result.fail(error("not-found", "delivery")), current];
          const decoded = decode(SubscriptionDelivery, text, "change-delivery-record");

          if (Result.isFailure(decoded)) return [decoded, current];

          const registrationText = current.registrations.get(
            subscriptionKeyString(key.subscription),
          );

          if (registrationText === undefined)
            return [Result.fail(error("corrupt", "delivery-registration")), current];

          const registration = decode(
            SubscriptionRecord,
            registrationText,
            "delivery-registration",
          );

          if (Result.isFailure(registration)) return [Result.fail(registration.failure), current];

          const transition = applySubscriptionDeliveryChange(
            decoded.success,
            registration.success,
            deliveryId,
            effectiveChange,
          );

          if (Result.isFailure(transition)) return [Result.fail(transition.failure), current];
          if (transition.success === decoded.success)
            return [Result.succeed(decoded.success), current];
          const updated = transition.success;
          const encoded = encode(SubscriptionDelivery, updated, "change-delivery-encode");

          if (Result.isFailure(encoded)) return [Result.fail(encoded.failure), current];
          const deliveries = new Map(current.deliveries);

          deliveries.set(storageKey, encoded.success);
          const deliveryIndex = new Map(current.deliveryIndex);
          const indexed = deliveryIndex.get(storageKey);

          if (indexed === undefined)
            return [Result.fail(error("corrupt", "delivery-index")), current];
          deliveryIndex.set(storageKey, {
            ...indexed,
            state: updated.state,
            parked: updated.retry.parked ?? false,
            observeSettlement: updated.observeSettlement ?? false,
            nextAttemptAtMillis: updated.retry.nextAttemptAtMillis,
          });

          return [Result.succeed(updated), { ...current, deliveries, deliveryIndex }];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));

    yield* failpoint.hit(`subscription:delivery-${change._tag.toLowerCase()}:after`);

    return result;
  });

  const recovering: SubscriptionStore["Service"]["recovering"] = Effect.fn(
    "MemorySubscriptionStore.recovering",
  )(function* (nowMillis, after, limit) {
    const records: Array<{ readonly key: SubscriptionKey; readonly ordinal: number }> = [];

    for (const item of (yield* Ref.get(state)).registrationIndex.values()) {
      if (
        item.ordinal > after &&
        item.state === "active" &&
        item.recoveryAt !== null &&
        item.recoveryAt <= nowMillis
      )
        records.push({ key: item.key, ordinal: item.ordinal });
    }
    records.sort((a, b) => a.ordinal - b.ordinal);

    return records.slice(0, limit);
  });

  const deferRecovery: SubscriptionStore["Service"]["deferRecovery"] = Effect.fn(
    "MemorySubscriptionStore.deferRecovery",
  )(function* (input, expectedRevision, recovery) {
    const key = yield* requireKey(input, "defer-recovery-key");

    yield* failpoint.hit("subscription:defer-recovery:before");
    yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (current): readonly [Result.Result<void, SubscriptionError>, MemorySubscriptionState] => {
          const storageKey = subscriptionKeyString(key);
          const text = current.registrations.get(storageKey);

          if (text === undefined) return [Result.fail(error("not-found", "subscription")), current];
          const record = decode(SubscriptionRecord, text, "defer-recovery-record");

          if (Result.isFailure(record)) return [Result.fail(record.failure), current];

          if (record.success.configurationRevision !== expectedRevision)
            return [Result.void, current];

          const updated = {
            ...record.success,
            recovery:
              record.success.state === "active" || record.success.state === "paused"
                ? recovery
                : null,
          };

          const encoded = encode(SubscriptionRecord, updated, "defer-recovery-encode");

          if (Result.isFailure(encoded)) return [Result.fail(encoded.failure), current];
          const registrations = new Map(current.registrations);

          registrations.set(storageKey, encoded.success);
          const registrationIndex = new Map(current.registrationIndex);
          const indexed = registrationIndex.get(storageKey);

          if (indexed === undefined)
            return [Result.fail(error("corrupt", "recovery-index")), current];
          registrationIndex.set(storageKey, {
            ...indexed,
            recoveryKey: updated.recovery === null ? null : candidateIndexKey(updated),
            recoveryAt: updated.recovery?.nextAttemptAtMillis ?? null,
          });

          return [
            Result.void,
            {
              ...current,
              registrations,
              registrationIndex,
              recoveryCounts: recoveryCountsAfter(current, registrationIndex, [storageKey]),
            },
          ];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));
    yield* failpoint.hit("subscription:defer-recovery:after");
  });

  const readScanCursors: SubscriptionStore["Service"]["readScanCursors"] = Ref.get(state).pipe(
    Effect.map((current) => current.scanCursors),
  );

  const advanceScanCursors: SubscriptionStore["Service"]["advanceScanCursors"] = Effect.fn(
    "MemorySubscriptionStore.advanceScanCursors",
  )(function* (input) {
    const cursors = yield* validate(SubscriptionScanCursors, input, "scan-cursors");

    yield* failpoint.hit("subscription:advance-scan-cursors:before");
    yield* Effect.uninterruptible(
      Ref.update(state, (current) => ({ ...current, scanCursors: cursors })),
    );
    yield* failpoint.hit("subscription:advance-scan-cursors:after");
  });

  const nextDeadline = Effect.gen(function* () {
    let deadline: number | null = null;
    const current = yield* Ref.get(state);

    if (
      current.scanCursors.events !== "" ||
      current.scanCursors.deliveries !== "" ||
      current.scanCursors.recovery !== 0
    )
      return 0;

    const consider = (value: number) => {
      if (deadline === null || value < deadline) deadline = value;
    };

    if (current.retentionDeadline !== null) consider(current.retentionDeadline);
    for (const accepted of current.eventIndex.values())
      if (!accepted.routingComplete) consider(accepted.nextAttemptAtMillis);
    for (const item of current.deliveryIndex.values())
      if (
        (item.state !== "delivered" && item.state !== "refused" && item.parked !== true) ||
        (item.state === "delivered" && item.observeSettlement === true)
      )
        consider(item.nextAttemptAtMillis);
    for (const record of current.registrationIndex.values())
      if (record.state === "active" && record.recoveryAt !== null) consider(record.recoveryAt);

    return deadline;
  }).pipe(Effect.withSpan("MemorySubscriptionStore.nextDeadline"));

  const compact: SubscriptionStore["Service"]["compact"] = Effect.fn(
    "MemorySubscriptionStore.compact",
  )(function* (nowMillis, inputPolicy, requestedLimit) {
    nowMillis = Math.min(nowMillis, yield* Clock.currentTimeMillis);
    const policy = yield* validate(SubscriptionRetentionPolicy, inputPolicy, "retention-policy");

    const limit = yield* validate(
      Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
      requestedLimit,
      "maintenance-limit",
    );

    yield* failpoint.hit("subscription:compact:before");
    let corruptCandidates = 0;

    const result = yield* Effect.uninterruptible(
      Ref.modify(
        state,
        (current): readonly [Result.Result<number, SubscriptionError>, MemorySubscriptionState] => {
          if (
            current.retentionHorizon !== null &&
            current.retentionHorizon !== policy.replayHorizonMillis
          )
            return [Result.fail(error("conflict", "retention-horizon")), current];
          const events = new Map(current.events);
          const eventIndex = new Map(current.eventIndex);
          const deliveries = new Map(current.deliveries);
          const deliveryIndex = new Map(current.deliveryIndex);
          const eventDeliveryCounts = new Map(current.eventDeliveryCounts);
          const ownerDeliveryCounts = new Map(current.ownerDeliveryCounts);

          // Page indexes before decoding. Each pass reads at most 2 * limit event values and
          // limit delivery values; relationship checks use maintained reference counts.
          const deliveryPage = current.deliveryKeys.slice(
            upperBound(current.deliveryKeys, current.maintenanceDeliveries),
            upperBound(current.deliveryKeys, current.maintenanceDeliveries) + limit,
          );

          const eventPage = current.eventKeys.slice(
            upperBound(current.eventKeys, current.maintenanceEvents),
            upperBound(current.eventKeys, current.maintenanceEvents) + limit,
          );

          const cutoff = nowMillis - policy.completedRetentionMillis;
          let removed = 0;
          const removedEvents = new Set<string>();
          const removedDeliveries = new Set<string>();

          for (const key of deliveryPage) {
            const text = deliveries.get(key);

            if (text === undefined) continue;
            const decoded = decode(SubscriptionDelivery, text, "compact-delivery");

            if (Result.isFailure(decoded)) {
              corruptCandidates++;
              continue;
            }
            const delivery = decoded.success;

            if (
              subscriptionDeliveryKeyString(delivery.key) !== key ||
              !samePartition(delivery.key.subscription.partition, partition)
            ) {
              corruptCandidates++;
              continue;
            }

            if (
              delivery.state !== "refused" &&
              (delivery.state !== "delivered" || delivery.settledAtMillis === undefined)
            )
              continue;
            if (
              (delivery.settledAtMillis ??
                delivery.completedAtMillis ??
                delivery.selectedAtMillis) > cutoff
            )
              continue;
            const eventText = events.get(delivery.key.eventId);

            if (eventText === undefined) continue;
            const accepted = decode(AcceptedEvent, eventText, "compact-delivery-event");

            if (Result.isFailure(accepted)) {
              corruptCandidates++;
              continue;
            }
            const event = accepted.success;

            if (
              event.eventId !== delivery.key.eventId ||
              !samePartition(event.partition, partition)
            ) {
              corruptCandidates++;
              continue;
            }

            if (
              !event.routingComplete ||
              event.occurredAtMillis === undefined ||
              event.acceptedAtMillis > cutoff ||
              (current.recoveryCounts.get(eventCandidateIndexKey(event)) ?? 0) > 0
            )
              continue;
            deliveries.delete(key);
            removedDeliveries.add(key);
            deliveryIndex.delete(key);
            eventDeliveryCounts.set(
              event.eventId,
              (eventDeliveryCounts.get(event.eventId) ?? 1) - 1,
            );
            const owner = delivery.key.subscription.ownerId;

            ownerDeliveryCounts.set(owner, (ownerDeliveryCounts.get(owner) ?? 1) - 1);
          }
          let tombstones = current.tombstoneCount;

          for (const key of eventPage) {
            const text = events.get(key);

            if (text === undefined) continue;
            const decoded = decode(AcceptedEvent, text, "compact-event");

            if (Result.isFailure(decoded)) {
              corruptCandidates++;
              continue;
            }
            const event = decoded.success;

            if (event.eventId !== key || !samePartition(event.partition, partition)) {
              corruptCandidates++;
              continue;
            }

            if (!event.routingComplete || event.occurredAtMillis === undefined) continue;
            const expired = event.occurredAtMillis <= nowMillis - policy.replayHorizonMillis;

            if (event.tombstone === true && !expired) continue;
            if (
              event.acceptedAtMillis > cutoff ||
              (eventDeliveryCounts.get(key) ?? 0) > 0 ||
              (current.recoveryCounts.get(eventCandidateIndexKey(event)) ?? 0) > 0
            )
              continue;
            if (expired) {
              events.delete(key);
              removedEvents.add(key);
              eventIndex.delete(key);
              eventDeliveryCounts.delete(key);
              if (event.tombstone === true) tombstones--;
            } else {
              if (tombstones >= policy.maxTombstones) continue;

              const encoded = encode(
                AcceptedEvent,
                { ...event, payload: null, tombstone: true },
                "compact-tombstone",
              );

              if (Result.isFailure(encoded)) continue;
              events.set(key, encoded.success);
              tombstones++;
            }
            removed++;
          }

          return [
            Result.succeed(removed),
            {
              ...current,
              events,
              eventIndex,
              deliveries,
              deliveryIndex,
              eventDeliveryCounts,
              ownerDeliveryCounts,
              eventKeys: removeKeys(current.eventKeys, removedEvents),
              deliveryKeys: removeKeys(current.deliveryKeys, removedDeliveries),
              tombstoneCount: tombstones,
              maintenanceEvents: eventPage.length < limit ? "" : (eventPage.at(-1) ?? ""),
              maintenanceDeliveries: deliveryPage.length < limit ? "" : (deliveryPage.at(-1) ?? ""),
              retentionHorizon: policy.replayHorizonMillis,
              retentionDeadline: events.size === 0 ? null : nowMillis + 60_000,
            },
          ];
        },
      ),
    ).pipe(Effect.flatMap(Effect.fromResult));

    if (corruptCandidates > 0)
      yield* Effect.logWarning("Subscription retention preserved corrupt candidates", {
        count: corruptCandidates,
      });
    yield* failpoint.hit("subscription:compact:after");

    return result;
  });

  return SubscriptionStore.of({
    partition,
    compact,
    register,
    get,
    list,
    cancel,
    change,
    accept,
    event,
    pendingEvents,
    candidates,
    select,
    catchUp,
    deferEvent,
    delivery,
    pendingDeliveries,
    listDeliveries,
    changeDelivery,
    recovering,
    deferRecovery,
    readScanCursors,
    advanceScanCursors,
    nextDeadline,
  });
});

export const memorySubscriptionStoreLayer = (
  partition: SourcePartition,
): Layer.Layer<SubscriptionStore, SubscriptionError> =>
  Layer.effect(SubscriptionStore, makeMemorySubscriptionStore(partition));

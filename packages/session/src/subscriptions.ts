import { ConversationId } from "@effect-agent/core";
import {
  Cause,
  Context,
  Crypto,
  DateTime,
  Effect,
  Encoding,
  Layer,
  Option,
  Schema,
  Semaphore,
} from "effect";

import { digestJson } from "./digest.ts";
import { EventSources, type NormalizedEvent } from "./event-source.ts";
import { IdempotencyKey, type Principal } from "./ledger.ts";
import { admitPreparedInput, PreparedInputAdmission } from "./prepared-admission.ts";
import { DefinitionDigests, type PersistedJson } from "./records.ts";
import { type ScheduleRetryReason } from "./schedule.ts";
import { resolveSubscriptionInput, SubscriptionInputBindings } from "./subscription-input.ts";
import {
  AcceptedEvent,
  type EventAcknowledgement,
  type EventSourceVersion,
  PreparedInput,
  SubscriptionAuthorizer,
  SubscriptionConfiguration,
  SubscriptionDelivery,
  type SubscriptionDeliveryKey,
  type SubscriptionDeliverySnapshot,
  SubscriptionError,
  SubscriptionFailpoint,
  SubscriptionKey,
  SubscriptionLimits,
  type SubscriptionRecord,
  SubscriptionScope,
  type SubscriptionSnapshot,
  SubscriptionSourceError,
  SubscriptionStore,
  type SubscriptionStoreFailure,
  defaultSubscriptionLimits,
  subscriptionDeliveryKeyString,
} from "./subscription.ts";

const now = DateTime.now.pipe(Effect.map(DateTime.toEpochMillis));
const failure = (reason: SubscriptionError["reason"], code: string) =>
  SubscriptionError.make({ reason, code });
const bytes = (value: PersistedJson) => Encoding.encodeHex(JSON.stringify(value)).length / 2;
const sameSource = (a: EventSourceVersion, b: EventSourceVersion) =>
  a.name === b.name && a.version === b.version;
const samePartition = Schema.toEquivalence(SubscriptionScope.fields.partition);
const validate = <S extends Schema.Top>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => failure("validation", "schema")),
  );
const snapshot = (record: SubscriptionRecord, time: number): SubscriptionSnapshot => ({
  key: record.key,
  source: record.configuration.source,
  mode: record.configuration.mode,
  state:
    record.state === "active" && record.configuration.expiresAtMillis <= time
      ? "expired"
      : record.state,
  createdAtMillis: record.createdAtMillis,
  expiresAtMillis: record.configuration.expiresAtMillis,
  recovery: record.recovery,
});

export interface SubscribeOptions {
  readonly subscriptionId: string;
  readonly source: EventSourceVersion;
  readonly parameters: PersistedJson;
  readonly context: PersistedJson;
  readonly mode: "once" | "continuous";
  readonly expiresAtMillis: number;
  readonly destination: SubscriptionConfiguration["destination"];
  readonly deliveryPrincipal: Principal;
  readonly agentId: SubscriptionConfiguration["agentId"];
  readonly definitions: SubscriptionConfiguration["definitions"];
}
export type SubscriptionFailure = SubscriptionStoreFailure | SubscriptionSourceError;

/** Only management and redacted status. Do not provide intake or drivers to Agent Tools. */
export class Subscriptions extends Context.Service<
  Subscriptions,
  {
    readonly subscribe: (
      scope: SubscriptionScope,
      options: SubscribeOptions,
    ) => Effect.Effect<SubscriptionSnapshot, SubscriptionFailure>;
    readonly listSubscriptions: (
      scope: SubscriptionScope,
      after?: number,
      limit?: number,
    ) => Effect.Effect<
      { readonly items: ReadonlyArray<SubscriptionSnapshot>; readonly next: number | null },
      SubscriptionError
    >;
    readonly cancelSubscription: (
      scope: SubscriptionScope,
      key: SubscriptionKey,
    ) => Effect.Effect<SubscriptionSnapshot, SubscriptionStoreFailure>;
    readonly listDeliveries: (
      scope: SubscriptionScope,
      key: SubscriptionKey,
      after?: string,
      limit?: number,
    ) => Effect.Effect<
      {
        readonly items: ReadonlyArray<typeof SubscriptionDeliverySnapshot.Type>;
        readonly next: string | null;
      },
      SubscriptionError
    >;
  }
>()("@effect-agent/session/Subscriptions") {
  static layer(limits: SubscriptionLimits = defaultSubscriptionLimits) {
    return Layer.effect(Subscriptions, makeManagement(limits));
  }
}

/** Trusted intake acknowledges retained routing work, never a Conversation Submission Receipt. */
export class SubscriptionIntake extends Context.Service<
  SubscriptionIntake,
  {
    readonly accept: (
      principal: Principal,
      source: EventSourceVersion,
      payload: unknown,
    ) => Effect.Effect<EventAcknowledgement, SubscriptionFailure>;
    readonly status: (
      principal: Principal,
      source: EventSourceVersion,
      eventId: string,
    ) => Effect.Effect<
      EventAcknowledgement & {
        readonly routingComplete: boolean;
        readonly routingFailure: string | null;
        readonly nextAttemptAtMillis: number;
      },
      SubscriptionError
    >;
  }
>()("@effect-agent/session/SubscriptionIntake") {
  static layer(limits: SubscriptionLimits = defaultSubscriptionLimits) {
    return Layer.effect(SubscriptionIntake, makeIntake(limits));
  }
}

/** One bounded sweep. The platform owns polling/alarm recovery; no subscription retains a Run. */
export class SubscriptionDriver extends Context.Service<
  SubscriptionDriver,
  {
    readonly runDue: Effect.Effect<
      { readonly processed: number; readonly failed: number },
      SubscriptionStoreFailure
    >;
    readonly processDelivery: (
      key: SubscriptionDeliveryKey,
    ) => Effect.Effect<void, SubscriptionFailure>;
  }
>()("@effect-agent/session/SubscriptionDriver") {
  static layer(limits: SubscriptionLimits = defaultSubscriptionLimits) {
    return Layer.effect(SubscriptionDriver, makeDriver(limits));
  }
}

const dependencies = Effect.gen(function* () {
  const store = yield* SubscriptionStore;
  const authorizer = yield* SubscriptionAuthorizer;
  const sources = (yield* EventSources).sources;
  const crypto = yield* Crypto.Crypto;
  const digest = (value: PersistedJson) =>
    digestJson(value).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError(() => failure("storage", "digest")),
    );
  const source = (version: EventSourceVersion) => {
    const found = sources.filter((s) => sameSource(s.source, version));
    const item = found[0];
    return found.length === 1 && item !== undefined
      ? Effect.succeed(item)
      : Effect.fail(failure("unsupported-source", "source-version"));
  };
  const scope = Effect.fn("Subscriptions.scope")(function* (value: SubscriptionScope) {
    const decoded = yield* validate(SubscriptionScope, value);
    if (!samePartition(decoded.partition, store.partition))
      return yield* failure("unauthorized", "partition");
    return decoded;
  });
  return { store, authorizer, source, digest, scope };
});

const pageLimit = (value: number) =>
  validate(Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)), value);

const makeManagement = Effect.fn("Subscriptions.make")(function* (requested: SubscriptionLimits) {
  const limits = yield* validate(SubscriptionLimits, requested);
  const { bindings } = yield* SubscriptionInputBindings;
  const { store, authorizer, source, digest, scope } = yield* dependencies;
  const subscribe: Subscriptions["Service"]["subscribe"] = Effect.fn("Subscriptions.subscribe")(
    function* (scopeValue, options) {
      const owner = yield* scope(scopeValue);
      yield* authorizer.manage("subscribe", owner);
      const behavior = yield* source(options.source);
      const params = yield* behavior.parameters(options.parameters);
      const binding = yield* resolveSubscriptionInput(bindings, options);
      const context = yield* binding.context(options.context);
      const configuration = yield* validate(SubscriptionConfiguration, {
        ...options,
        parameters: params.parameters,
        context,
        matchingKey: params.matchingKey,
      });
      yield* authorizer.manage("subscribe", owner, configuration);
      const key = yield* validate(SubscriptionKey, {
        partition: owner.partition,
        ownerId: owner.ownerId,
        subscriptionId: options.subscriptionId,
      });
      const encodedConfiguration = yield* Schema.encodeEffect(SubscriptionConfiguration)(
        configuration,
      ).pipe(Effect.mapError(() => failure("validation", "configuration")));
      const creationFingerprint = yield* digest({
        key,
        createdBy: owner.principal,
        configuration: encodedConfiguration,
      });
      const existing = yield* store.get(key);
      if (existing !== null) {
        const retainedConfiguration = yield* Schema.encodeEffect(SubscriptionConfiguration)(
          existing.configuration,
        ).pipe(Effect.mapError(() => failure("corrupt", "creation-configuration")));
        const retainedFingerprint = yield* digest({
          key: existing.key,
          createdBy: existing.createdBy,
          configuration: retainedConfiguration,
        });
        if (retainedFingerprint !== existing.creationFingerprint)
          return yield* failure("corrupt", "creation-fingerprint");
        if (existing.creationFingerprint !== creationFingerprint)
          return yield* failure("conflict", "creation");
        return snapshot(existing, yield* now);
      }
      if (
        bytes(params.parameters) > limits.maxPayloadBytes ||
        bytes(context) > limits.maxContextBytes
      )
        return yield* failure("validation", "registration-bounds");
      const time = yield* now;
      if (
        configuration.expiresAtMillis <= time ||
        configuration.expiresAtMillis - time > limits.maxLifetimeMillis
      )
        return yield* failure("validation", "lifetime");
      if (behavior.reconcile !== undefined && configuration.mode !== "once")
        return yield* failure("validation", "reconciliation-requires-once");
      const record = yield* store.register(
        {
          schemaVersion: 1,
          key,
          creationFingerprint,
          createdBy: owner.principal,
          createdAtMillis: time,
          ordinal: 0,
          configuration,
          state: "active",
          recovery:
            behavior.reconcile === undefined
              ? null
              : { attempts: 0, nextAttemptAtMillis: time, lastFailure: null },
        },
        limits,
      );
      return snapshot(record, time);
    },
  );
  const listSubscriptions: Subscriptions["Service"]["listSubscriptions"] = Effect.fn(
    "Subscriptions.listSubscriptions",
  )(function* (scopeValue, after = 0, requestedLimit = 50) {
    const owner = yield* scope(scopeValue);
    yield* authorizer.manage("list", owner);
    const limit = yield* pageLimit(requestedLimit);
    yield* validate(Schema.Natural, after);
    const records = yield* store.list(owner.ownerId, after, limit);
    const time = yield* now;
    return {
      items: records.map((record) => snapshot(record, time)),
      next: records.length === limit ? (records.at(-1)?.ordinal ?? null) : null,
    };
  });
  const ownedKey = Effect.fn("Subscriptions.ownedKey")(function* (
    owner: SubscriptionScope,
    key: SubscriptionKey,
  ) {
    yield* validate(SubscriptionKey, key);
    if (!samePartition(owner.partition, key.partition) || owner.ownerId !== key.ownerId)
      return yield* failure("unauthorized", "owner");
    return key;
  });
  const cancelSubscription: Subscriptions["Service"]["cancelSubscription"] = Effect.fn(
    "Subscriptions.cancelSubscription",
  )(function* (scopeValue, key) {
    const owner = yield* scope(scopeValue);
    yield* authorizer.manage("cancel", owner);
    const record = yield* store.cancel(yield* ownedKey(owner, key));
    return snapshot(record, yield* now);
  });
  const listDeliveries: Subscriptions["Service"]["listDeliveries"] = Effect.fn(
    "Subscriptions.listDeliveries",
  )(function* (scopeValue, key, after = "", requestedLimit = 50) {
    const owner = yield* scope(scopeValue);
    yield* authorizer.manage("deliveries", owner);
    const limit = yield* pageLimit(requestedLimit);
    const records = yield* store.listDeliveries(yield* ownedKey(owner, key), after, limit);
    const last = records.at(-1);
    return {
      items: records.map(({ key, state, retry, receipt, refusal }) => ({
        key,
        state,
        retry,
        receipt,
        refusal,
      })),
      next:
        records.length === limit && last !== undefined
          ? subscriptionDeliveryKeyString(last.key)
          : null,
    };
  });
  return Subscriptions.of({ subscribe, listSubscriptions, cancelSubscription, listDeliveries });
});

const acceptNormalized = Effect.fn("Subscriptions.acceptNormalized")(function* (
  store: SubscriptionStore["Service"],
  digest: (
    value: PersistedJson,
  ) => Effect.Effect<AcceptedEvent["payloadDigest"], SubscriptionError>,
  source: EventSourceVersion,
  event: NormalizedEvent,
  limits: SubscriptionLimits,
) {
  const time = yield* now;
  const record = yield* validate(AcceptedEvent, {
    schemaVersion: 1,
    partition: store.partition,
    eventId: event.eventId,
    source,
    matchingKey: event.matchingKey,
    payload: event.payload,
    payloadDigest: yield* digest(event.payload),
    acceptedAtMillis: time,
    cutoff: 0,
    cursor: 0,
    routingComplete: false,
    routingFailure: null,
    nextAttemptAtMillis: time,
  });
  const retained = yield* store.accept(record, limits);
  if ((yield* digest(retained.payload)) !== retained.payloadDigest)
    return yield* failure("corrupt", "event-digest");
  return retained;
});

const makeIntake = Effect.fn("SubscriptionIntake.make")(function* (requested: SubscriptionLimits) {
  const limits = yield* validate(SubscriptionLimits, requested);
  const { store, authorizer, source, digest } = yield* dependencies;
  const accept: SubscriptionIntake["Service"]["accept"] = Effect.fn("SubscriptionIntake.accept")(
    function* (principal, version, payload) {
      yield* authorizer.intake(store.partition, version, principal);
      const behavior = yield* source(version);
      const event = yield* behavior.normalize(payload);
      const accepted = yield* acceptNormalized(store, digest, version, event, limits);
      return {
        partition: accepted.partition,
        eventId: accepted.eventId,
        acceptedAtMillis: accepted.acceptedAtMillis,
      };
    },
  );
  const status: SubscriptionIntake["Service"]["status"] = Effect.fn("SubscriptionIntake.status")(
    function* (principal, version, eventId) {
      yield* authorizer.intake(store.partition, version, principal);
      const event = yield* store.event(eventId);
      if (event === null) return yield* failure("not-found", "event");
      if (!sameSource(event.source, version)) return yield* failure("conflict", "event-source");
      return {
        partition: event.partition,
        eventId: event.eventId,
        acceptedAtMillis: event.acceptedAtMillis,
        routingComplete: event.routingComplete,
        routingFailure: event.routingFailure,
        nextAttemptAtMillis: event.nextAttemptAtMillis,
      };
    },
  );
  return SubscriptionIntake.of({ accept, status });
});

const makeDriver = Effect.fn("SubscriptionDriver.make")(function* (requested: SubscriptionLimits) {
  const limits = yield* validate(SubscriptionLimits, requested);
  const { bindings } = yield* SubscriptionInputBindings;
  const { store, authorizer, source, digest } = yield* dependencies;
  const admission = yield* PreparedInputAdmission;
  const failpoint = yield* SubscriptionFailpoint;
  const semaphore = yield* Semaphore.make(limits.concurrency);
  const sweepSemaphore = yield* Semaphore.make(1);
  const definitionEquals = Schema.toEquivalence(DefinitionDigests);
  const nextAttempt = (time: number) => Math.min(time + limits.retryMillis, 8_640_000_000_000_000);
  const selected = Effect.fn("Subscriptions.selected")(function* (
    event: AcceptedEvent,
    subscription: SubscriptionRecord,
  ) {
    const deliveryId = yield* digest({
      schemaVersion: 1,
      subscription: subscription.key,
      eventId: event.eventId,
    });
    const time = yield* now;
    return yield* validate(SubscriptionDelivery, {
      schemaVersion: 1,
      key: { subscription: subscription.key, eventId: event.eventId },
      deliveryId,
      subscriptionFingerprint: subscription.creationFingerprint,
      eventDigest: event.payloadDigest,
      source: event.source,
      conversationId:
        subscription.configuration.destination._tag === "ExistingConversation"
          ? subscription.configuration.destination.conversationId
          : Schema.decodeSync(ConversationId)(`subscription:${deliveryId}`),
      admissionKey: Schema.decodeSync(IdempotencyKey)(`subscription:${deliveryId}`),
      selectedAtMillis: time,
      state: "selected",
      envelope: null,
      envelopeDigest: null,
      retry: {
        attempts: 0,
        nextAttemptAtMillis: time,
        lastAttemptAtMillis: null,
        lastFailure: null,
      },
      receipt: null,
      refusal: null,
    });
  });
  const route = Effect.fn("Subscriptions.route")(function* (event: AcceptedEvent) {
    const behavior = yield* source(event.source);
    const normalized = yield* behavior.normalize(event.payload);
    if (
      normalized.eventId !== event.eventId ||
      normalized.matchingKey !== event.matchingKey ||
      (yield* digest(normalized.payload)) !== event.payloadDigest ||
      (yield* digest(event.payload)) !== event.payloadDigest
    )
      return yield* failure("corrupt", "event-source-bindings");
    const candidates = yield* store.candidates(event, limits.batchSize);
    const deliveries: Array<SubscriptionDelivery> = [];
    const time = yield* now;
    for (const candidate of candidates) {
      if (candidate.state !== "active" || candidate.configuration.expiresAtMillis <= time) continue;
      const config = yield* Schema.encodeEffect(SubscriptionConfiguration)(
        candidate.configuration,
      ).pipe(Effect.mapError(() => failure("corrupt", "configuration")));
      const fingerprint = yield* digest({
        key: candidate.key,
        createdBy: candidate.createdBy,
        configuration: config,
      });
      const parameters = yield* behavior.parameters(candidate.configuration.parameters);
      if (
        fingerprint !== candidate.creationFingerprint ||
        parameters.matchingKey !== candidate.configuration.matchingKey
      )
        return yield* failure("corrupt", "registration-source-bindings");
      if (yield* behavior.matches(event, candidate))
        deliveries.push(yield* selected(event, candidate));
    }
    const cursor = candidates.at(-1)?.ordinal ?? event.cursor;
    yield* store.select(
      event,
      deliveries,
      cursor,
      candidates.length < limits.batchSize,
      yield* now,
      limits,
    );
  });
  const retry = (delivery: SubscriptionDelivery, reason: ScheduleRetryReason) =>
    now.pipe(
      Effect.flatMap((time) =>
        store.changeDelivery(delivery.key, delivery.deliveryId, {
          _tag: "Retry",
          nowMillis: time,
          retry: {
            attempts: delivery.retry.attempts + 1,
            lastAttemptAtMillis: time,
            nextAttemptAtMillis: nextAttempt(time),
            lastFailure: reason,
          },
        }),
      ),
      Effect.asVoid,
    );
  const refuse = (
    delivery: SubscriptionDelivery,
    phase: "preparation" | "admission",
    code: string,
  ) =>
    now.pipe(
      Effect.flatMap((time) =>
        store.changeDelivery(delivery.key, delivery.deliveryId, {
          _tag: "Refuse",
          refusal: { phase, code },
          nowMillis: time,
        }),
      ),
      Effect.asVoid,
    );
  const verifySelected = Effect.fn("Subscriptions.verifySelected")(function* (
    delivery: SubscriptionDelivery,
  ) {
    const subscription = yield* store.get(delivery.key.subscription);
    const event = yield* store.event(delivery.key.eventId);
    if (
      subscription === null ||
      event === null ||
      !sameSource(delivery.source, event.source) ||
      !sameSource(delivery.source, subscription.configuration.source)
    )
      return yield* failure("corrupt", "selected-reference");
    const expectedId = yield* digest({
      schemaVersion: 1,
      subscription: subscription.key,
      eventId: event.eventId,
    });
    const configuration = yield* Schema.encodeEffect(SubscriptionConfiguration)(
      subscription.configuration,
    ).pipe(Effect.mapError(() => failure("corrupt", "configuration")));
    const fingerprint = yield* digest({
      key: subscription.key,
      createdBy: subscription.createdBy,
      configuration,
    });
    const destination = subscription.configuration.destination;
    if (
      delivery.subscriptionFingerprint !== fingerprint ||
      subscription.creationFingerprint !== fingerprint ||
      delivery.eventDigest !== event.payloadDigest ||
      (yield* digest(event.payload)) !== event.payloadDigest ||
      delivery.deliveryId !== expectedId ||
      delivery.admissionKey !== `subscription:${expectedId}` ||
      delivery.conversationId !==
        (destination._tag === "ExistingConversation"
          ? destination.conversationId
          : `subscription:${expectedId}`)
    )
      return yield* failure("corrupt", "selected-bindings");
    return { subscription, event };
  });
  const envelopeDigest = Effect.fn("Subscriptions.envelopeDigest")(function* (
    delivery: SubscriptionDelivery,
    envelope: PreparedInput,
  ) {
    const encoded = yield* Schema.encodeEffect(PreparedInput)(envelope).pipe(
      Effect.mapError(() => failure("corrupt", "prepared-envelope")),
    );
    return yield* digest({
      deliveryId: delivery.deliveryId,
      subscriptionFingerprint: delivery.subscriptionFingerprint,
      eventDigest: delivery.eventDigest,
      envelope: encoded,
    });
  });
  const prepare = Effect.fn("Subscriptions.prepare")(function* (delivery: SubscriptionDelivery) {
    const { subscription, event } = yield* verifySelected(delivery);
    const time = yield* now;
    if (subscription.state === "cancelled" || subscription.configuration.expiresAtMillis <= time) {
      yield* refuse(
        delivery,
        "preparation",
        subscription.state === "cancelled" ? "cancelled" : "expired",
      );
      return null;
    }
    const binding = yield* resolveSubscriptionInput(bindings, subscription.configuration);
    if ((yield* digest(event.payload)) !== event.payloadDigest)
      return yield* failure("corrupt", "event-digest");
    const authorization = yield* authorizer.prepare(subscription, event);
    const input = yield* binding.prepare(event, subscription);
    if (bytes(input) > limits.maxPayloadBytes)
      return yield* SubscriptionSourceError.make({ code: "input-bounds", retryable: false });
    const envelope = yield* validate(PreparedInput, {
      schemaVersion: 1,
      conversationId: delivery.conversationId,
      admissionKey: delivery.admissionKey,
      deliveryPrincipal: subscription.configuration.deliveryPrincipal,
      agentId: subscription.configuration.agentId,
      definitions: subscription.configuration.definitions,
      input,
      inputDigest: yield* digest(input),
      authorization,
    });
    return yield* store.changeDelivery(delivery.key, delivery.deliveryId, {
      _tag: "Prepare",
      envelope,
      envelopeDigest: yield* envelopeDigest(delivery, envelope),
      nowMillis: yield* now,
    });
  });
  const process = Effect.fn("Subscriptions.processDelivery")(function* (
    key: SubscriptionDeliveryKey,
  ) {
    let delivery = yield* store.delivery(key);
    if (delivery === null) return yield* failure("not-found", "delivery");
    if (
      delivery.retry.nextAttemptAtMillis > (yield* now) ||
      delivery.state === "delivered" ||
      delivery.state === "refused"
    )
      return;
    if (delivery.state === "selected") {
      const original = delivery;
      delivery = yield* prepare(original).pipe(
        Effect.timeout(limits.operationTimeoutMillis),
        Effect.catchTag("SubscriptionSourceError", (error) =>
          (error.retryable
            ? retry(original, "transport")
            : refuse(original, "preparation", error.code)
          ).pipe(Effect.as(null)),
        ),
        Effect.catchTag("SubscriptionError", (error) =>
          error.reason === "unauthorized"
            ? refuse(original, "preparation", error.code).pipe(Effect.as(null))
            : Effect.fail(error),
        ),
        Effect.catchTag("TimeoutError", () => retry(original, "timeout").pipe(Effect.as(null))),
      );
    }
    if (delivery === null || delivery.state === "refused" || delivery.state === "delivered") return;
    if (
      delivery.state !== "prepared" ||
      delivery.envelope === null ||
      delivery.envelopeDigest === null
    )
      return yield* failure("corrupt", "prepared-state");
    const { subscription } = yield* verifySelected(delivery);
    const envelope = delivery.envelope;
    if (
      (yield* digest(envelope.input)) !== envelope.inputDigest ||
      envelope.conversationId !== delivery.conversationId ||
      envelope.admissionKey !== delivery.admissionKey ||
      envelope.deliveryPrincipal !== subscription.configuration.deliveryPrincipal ||
      envelope.agentId !== subscription.configuration.agentId ||
      !definitionEquals(envelope.definitions, subscription.configuration.definitions) ||
      delivery.envelopeDigest !== (yield* envelopeDigest(delivery, envelope))
    )
      return yield* failure("corrupt", "prepared-envelope");
    const outcome = yield* admitPreparedInput(
      admission.submit(envelope),
      limits.operationTimeoutMillis,
    ).pipe(Effect.mapError(() => failure("corrupt", "admission")));
    if (outcome._tag === "Receipt") {
      yield* failpoint.hit("subscription:admission:after");
      if (outcome.receipt.conversationId !== envelope.conversationId)
        return yield* failure("corrupt", "receipt-destination");
      yield* store.changeDelivery(delivery.key, delivery.deliveryId, {
        _tag: "Complete",
        receipt: outcome.receipt,
        nowMillis: yield* now,
      });
    } else if (outcome._tag === "Refused") yield* refuse(delivery, "admission", outcome.error.code);
    else yield* retry(delivery, outcome.reason);
  });
  const processDelivery: SubscriptionDriver["Service"]["processDelivery"] = (key) =>
    semaphore.withPermit(process(key));
  const reconcile = Effect.fn("Subscriptions.reconcile")(function* (
    subscription: SubscriptionRecord,
  ) {
    const time = yield* now;
    if (subscription.state !== "active" || subscription.configuration.expiresAtMillis <= time)
      return yield* store.deferRecovery(subscription.key, null);
    const behavior = yield* source(subscription.configuration.source);
    if (behavior.reconcile === undefined)
      return yield* failure("unsupported-source", "source-reconciliation");
    yield* authorizer.reconcile(subscription);
    const observation = yield* behavior.reconcile(subscription).pipe(
      Effect.timeout(limits.operationTimeoutMillis),
      Effect.map((event) => ({ _tag: "Observed" as const, event })),
      Effect.catchTag("SubscriptionSourceError", (error) =>
        Effect.succeed({ _tag: "Failed" as const, error }),
      ),
    );
    if (observation._tag === "Failed")
      return yield* store.deferRecovery(subscription.key, {
        attempts: (subscription.recovery?.attempts ?? 0) + 1,
        nextAttemptAtMillis: observation.error.retryable ? nextAttempt(time) : null,
        lastFailure: observation.error.code,
      });
    const observed = observation.event;
    if (observed !== null) {
      const event = yield* acceptNormalized(store, digest, behavior.source, observed, limits);
      if (
        event.matchingKey !== subscription.configuration.matchingKey ||
        !(yield* behavior.matches(event, subscription))
      )
        return yield* failure("conflict", "reconciliation-match");
      yield* store.catchUp(event, yield* selected(event, subscription), yield* now, limits);
      return;
    }
    yield* store.deferRecovery(subscription.key, {
      attempts: (subscription.recovery?.attempts ?? 0) + 1,
      nextAttemptAtMillis: nextAttempt(time),
      lastFailure: null,
    });
  });
  const runDue = Effect.gen(function* () {
    const time = yield* now;
    let processed = 0;
    let failed = 0;
    const failureCode = <E>(cause: Cause.Cause<E>) =>
      Option.match(Cause.findErrorOption(cause), {
        onNone: () => "defect",
        onSome: (error) =>
          Schema.is(SubscriptionError)(error)
            ? error.reason
            : Schema.is(SubscriptionSourceError)(error)
              ? error.code
              : Schema.is(Schema.Struct({ _tag: Schema.Literal("TimeoutError") }))(error)
                ? "timeout"
                : "interrupted-work",
      });
    const attempt = <E>(
      work: Effect.Effect<void, E>,
      recover: (code: string) => Effect.Effect<void, SubscriptionStoreFailure>,
    ) =>
      work.pipe(
        Effect.matchCauseEffect({
          onSuccess: () =>
            Effect.sync(() => {
              processed += 1;
            }),
          onFailure: (cause) =>
            Cause.hasInterrupts(cause)
              ? Effect.interrupt
              : Effect.gen(function* () {
                  failed += 1;
                  const code = failureCode(cause);
                  yield* recover(code).pipe(
                    Effect.catchCause((c) =>
                      Cause.hasInterrupts(c) ? Effect.interrupt : Effect.void,
                    ),
                  );
                  yield* Effect.logWarning("Subscription work remains pending").pipe(
                    Effect.annotateLogs({ failureCode: code }),
                  );
                }),
        }),
      );
    // Persist progress even when one record cannot be decoded or deferred. An eviction cannot
    // send every sweep back to a corrupt first page. Each category receives one bounded page.
    const cursors = yield* store.readScanCursors;
    const records = yield* store.recovering(time, cursors.recovery, limits.batchSize);
    const events = yield* store.pendingEvents(time, cursors.events, limits.batchSize);
    const deliveries = yield* store.pendingDeliveries(time, cursors.deliveries, limits.batchSize);
    const lastDelivery = deliveries.at(-1);
    yield* store.advanceScanCursors({
      recovery: records.length < limits.batchSize ? 0 : (records.at(-1)?.ordinal ?? 0),
      events: events.length < limits.batchSize ? "" : (events.at(-1) ?? ""),
      deliveries:
        deliveries.length < limits.batchSize || lastDelivery === undefined
          ? ""
          : subscriptionDeliveryKeyString(lastDelivery),
    });
    yield* Effect.forEach(
      records,
      (cursor) =>
        attempt(
          store.get(cursor.key).pipe(
            Effect.flatMap((record) =>
              record === null ? failure("not-found", "recovery") : reconcile(record),
            ),
            Effect.timeout(limits.operationTimeoutMillis),
          ),
          (code) =>
            store.get(cursor.key).pipe(
              Effect.flatMap((record) =>
                record === null
                  ? Effect.void
                  : store.deferRecovery(cursor.key, {
                      attempts: (record.recovery?.attempts ?? 0) + 1,
                      nextAttemptAtMillis: code === "unauthorized" ? null : nextAttempt(time),
                      lastFailure: code,
                    }),
              ),
            ),
        ),
      { concurrency: limits.concurrency },
    );
    yield* Effect.forEach(
      events,
      (eventId) =>
        attempt(
          store.event(eventId).pipe(
            Effect.flatMap((event) =>
              event === null ? failure("not-found", "event") : route(event),
            ),
            Effect.timeout(limits.operationTimeoutMillis),
          ),
          (code) => store.deferEvent(eventId, nextAttempt(time), code),
        ),
      { concurrency: limits.concurrency },
    );
    yield* Effect.forEach(
      deliveries,
      (key) =>
        attempt(processDelivery(key), () =>
          store
            .delivery(key)
            .pipe(
              Effect.flatMap((delivery) =>
                delivery === null ? Effect.void : retry(delivery, "ambiguous"),
              ),
            ),
        ),
      { concurrency: limits.concurrency },
    );
    return { processed, failed };
  });
  return SubscriptionDriver.of({ runDue: sweepSemaphore.withPermit(runDue), processDelivery });
});

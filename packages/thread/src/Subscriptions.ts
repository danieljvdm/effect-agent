import { ThreadId } from "@effect-agent/core/Identifiers";
import { Cause, Context, Crypto, DateTime, Effect, Layer, Option, Schema, Semaphore } from "effect";

import { digestJson } from "./Digest.ts";
import { EventSources, type NormalizedEvent } from "./EventSource.ts";
import { utf8ByteLength } from "./internal/utf8.ts";
import { admitPreparedInput, PreparedInputAdmission } from "./PreparedInputAdmission.ts";
import { DefinitionDigests, type PersistedJson } from "./Records.ts";
import { type ScheduleRetryReason } from "./Schedule.ts";
import { type AdmissionFence, IdempotencyKey, type Principal } from "./SubmissionLedger.ts";
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
  type SubscriptionChange,
  SubscriptionScope,
  type SubscriptionSnapshot,
  SubscriptionSourceError,
  SubscriptionStore,
  type SubscriptionStoreFailure,
  defaultSubscriptionLimits,
  subscriptionDeliveryKeyString,
} from "./Subscription.ts";
import { resolveSubscriptionInput, SubscriptionInputBindings } from "./SubscriptionInput.ts";

const now = DateTime.now.pipe(Effect.map(DateTime.toEpochMillis));

const failure = (reason: SubscriptionError["reason"], code: string) =>
  SubscriptionError.make({ reason, code });

const bytes = (value: PersistedJson) => utf8ByteLength(JSON.stringify(value));

const sameSource = (a: EventSourceVersion, b: EventSourceVersion) =>
  a.name === b.name && a.version === b.version;

const samePartition = Schema.toEquivalence(SubscriptionScope.fields.partition);

const validate = <S extends Schema.Top>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => failure("validation", "schema")),
  );

const snapshot = (record: SubscriptionRecord, time: number): SubscriptionSnapshot => ({
  key: record.key,
  configurationRevision: record.configurationRevision,
  configurationFingerprint: record.configurationFingerprint,
  ...(record.configuration.admissionGroup === undefined
    ? {}
    : { admissionGroup: record.configuration.admissionGroup }),
  ...(record.configuration.admissionFence === undefined
    ? {}
    : { admissionFence: record.configuration.admissionFence }),
  source: record.configuration.source,
  mode: record.configuration.mode,
  state:
    record.state === "active" &&
    record.configuration.expiresAtMillis !== null &&
    record.configuration.expiresAtMillis <= time
      ? "expired"
      : record.state,
  createdAtMillis: record.createdAtMillis,
  expiresAtMillis: record.configuration.expiresAtMillis,
  recovery: record.recovery,
});

const deliverySnapshot = (
  record: SubscriptionDelivery,
): typeof SubscriptionDeliverySnapshot.Type => ({
  key: record.key,
  state: record.state,
  configurationRevision: record.configurationRevision,
  ...(record.observeSettlement === undefined
    ? {}
    : { observeSettlement: record.observeSettlement }),
  ...(record.settledAtMillis === undefined ? {} : { settledAtMillis: record.settledAtMillis }),
  retry: record.retry,
  receipt: record.receipt,
  refusal: record.refusal,
});

export interface SubscribeOptions {
  readonly subscriptionId: string;
  readonly source: EventSourceVersion;
  readonly parameters: PersistedJson;
  readonly context: PersistedJson;
  readonly mode: "once" | "continuous";
  readonly expiresAtMillis: number | null;
  readonly destination: SubscriptionConfiguration["destination"];
  readonly deliveryPrincipal: Principal;
  readonly admissionGroup?: string;
  readonly admissionFence?: AdmissionFence;
  readonly agentId: SubscriptionConfiguration["agentId"];
  readonly definitions: SubscriptionConfiguration["definitions"];
}

export type SubscriptionFailure = SubscriptionStoreFailure | SubscriptionSourceError;

/** Only management and redacted status. Do not provide intake or drivers to Agent Tools. */
export class Subscriptions extends Context.Service<
  Subscriptions,
  {
    readonly recoverSubscription: (
      scope: SubscriptionScope,
      key: SubscriptionKey,
      expectedRevision: number,
    ) => Effect.Effect<SubscriptionSnapshot, SubscriptionStoreFailure>;
    readonly getSubscription: (
      scope: SubscriptionScope,
      key: SubscriptionKey,
    ) => Effect.Effect<SubscriptionSnapshot, SubscriptionError>;
    readonly subscribe: (
      scope: SubscriptionScope,
      options: SubscribeOptions,
    ) => Effect.Effect<SubscriptionSnapshot, SubscriptionFailure>;
    readonly updateSubscription: (
      scope: SubscriptionScope,
      key: SubscriptionKey,
      expectedRevision: number,
      options: Omit<SubscribeOptions, "subscriptionId">,
    ) => Effect.Effect<SubscriptionSnapshot, SubscriptionFailure>;
    readonly pauseSubscription: (
      scope: SubscriptionScope,
      key: SubscriptionKey,
      expectedRevision: number,
    ) => Effect.Effect<SubscriptionSnapshot, SubscriptionStoreFailure>;
    readonly resumeSubscription: (
      scope: SubscriptionScope,
      key: SubscriptionKey,
      expectedRevision: number,
    ) => Effect.Effect<SubscriptionSnapshot, SubscriptionStoreFailure>;
    readonly listSubscriptions: (
      scope: SubscriptionScope,
      after?: number,
      limit?: number,
    ) => Effect.Effect<
      { readonly items: ReadonlyArray<SubscriptionSnapshot>; readonly next: number | null },
      SubscriptionError
    >;
    readonly recoverDelivery: (
      scope: SubscriptionScope,
      key: SubscriptionDeliveryKey,
      expectedGeneration: number,
    ) => Effect.Effect<typeof SubscriptionDeliverySnapshot.Type, SubscriptionStoreFailure>;
    readonly cancelSubscription: (
      scope: SubscriptionScope,
      key: SubscriptionKey,
      expectedRevision?: number,
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
>()("@effect-agent/thread/Subscriptions") {
  static layer(limits: SubscriptionLimits = defaultSubscriptionLimits) {
    return Layer.effect(Subscriptions, makeManagement(limits));
  }
}

/** Trusted intake acknowledges retained routing work, never a Thread Submission Receipt. */
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
>()("@effect-agent/thread/SubscriptionIntake") {
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
>()("@effect-agent/thread/SubscriptionDriver") {
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

  const acceptNormalized = Effect.fn("Subscriptions.acceptNormalized")(function* (
    version: EventSourceVersion,
    event: NormalizedEvent,
    limits: SubscriptionLimits,
  ) {
    const time = yield* now;

    const record = yield* validate(AcceptedEvent, {
      schemaVersion: 1,
      partition: store.partition,
      eventId: event.eventId,
      source: version,
      matchingKey: event.matchingKey,
      payload: event.payload,
      payloadDigest: yield* digest(event.payload),
      acceptedAtMillis: time,
      ...(event.occurredAtMillis === undefined ? {} : { occurredAtMillis: event.occurredAtMillis }),
      cutoff: 0,
      cursor: 0,
      routingComplete: false,
      routingFailure: null,
      nextAttemptAtMillis: time,
    });

    const retained = yield* store.accept(record, limits);

    if (retained.tombstone !== true && (yield* digest(retained.payload)) !== retained.payloadDigest)
      return yield* failure("corrupt", "event-digest");

    return retained;
  });

  return { store, authorizer, source, digest, scope, acceptNormalized };
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
          existing.creationConfiguration,
        ).pipe(Effect.mapError(() => failure("corrupt", "creation-configuration")));

        const retainedFingerprint = yield* digest({
          key: existing.key,
          createdBy: existing.createdBy,
          configuration: retainedConfiguration,
        });

        if (retainedFingerprint !== existing.creationFingerprint)
          return yield* failure("corrupt", "creation-fingerprint");

        const currentConfiguration = yield* Schema.encodeEffect(SubscriptionConfiguration)(
          existing.configuration,
        ).pipe(Effect.mapError(() => failure("corrupt", "configuration")));

        if (
          (yield* digest({
            key: existing.key,
            createdBy: existing.createdBy,
            configuration: currentConfiguration,
          })) !== existing.configurationFingerprint
        )
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
        configuration.expiresAtMillis !== null &&
        (configuration.expiresAtMillis <= time ||
          configuration.expiresAtMillis - time > limits.maxLifetimeMillis)
      )
        return yield* failure("validation", "lifetime");
      if (behavior.reconcile !== undefined && configuration.mode !== "once")
        return yield* failure("validation", "reconciliation-requires-once");

      const record = yield* store.register(
        {
          schemaVersion: 1,
          key,
          creationFingerprint,
          configurationRevision: 1,
          configurationFingerprint: creationFingerprint,
          creationConfiguration: configuration,
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

  const updateSubscription: Subscriptions["Service"]["updateSubscription"] = Effect.fn(
    "Subscriptions.updateSubscription",
  )(function* (scopeValue, key, expectedRevision, options) {
    const owner = yield* scope(scopeValue);

    yield* authorizer.manage("update", owner);
    yield* ownedKey(owner, key);
    const behavior = yield* source(options.source);
    const parameters = yield* behavior.parameters(options.parameters);
    const binding = yield* resolveSubscriptionInput(bindings, options);
    const context = yield* binding.context(options.context);

    const configuration = yield* validate(SubscriptionConfiguration, {
      ...options,
      ...parameters,
      context,
    });

    yield* authorizer.manage("update", owner, configuration);
    const time = yield* now;

    if (
      bytes(parameters.parameters) > limits.maxPayloadBytes ||
      bytes(context) > limits.maxContextBytes
    )
      return yield* failure("validation", "registration-bounds");
    if (
      configuration.expiresAtMillis !== null &&
      (configuration.expiresAtMillis <= time ||
        configuration.expiresAtMillis - time > limits.maxLifetimeMillis)
    )
      return yield* failure("validation", "lifetime");
    if (behavior.reconcile !== undefined && configuration.mode !== "once")
      return yield* failure("validation", "reconciliation-requires-once");
    const existing = yield* store.get(key);

    if (existing === null) return yield* failure("not-found", "subscription");

    const encoded = yield* Schema.encodeEffect(SubscriptionConfiguration)(configuration).pipe(
      Effect.mapError(() => failure("validation", "configuration")),
    );

    const configurationFingerprint = yield* digest({
      key,
      createdBy: existing.createdBy,
      configuration: encoded,
    });

    const updated = yield* store.change(key, expectedRevision, {
      _tag: "Update",
      configuration,
      configurationFingerprint,
      recovery:
        behavior.reconcile === undefined
          ? null
          : { attempts: 0, nextAttemptAtMillis: time, lastFailure: null },
    });

    return snapshot(updated, time);
  });

  const changeState = Effect.fn("Subscriptions.changeState")(function* (
    scopeValue: SubscriptionScope,
    key: SubscriptionKey,
    expectedRevision: number,
    change: Extract<SubscriptionChange, { readonly _tag: "Pause" | "Resume" }>,
  ) {
    const owner = yield* scope(scopeValue);

    yield* authorizer.manage(change._tag === "Pause" ? "pause" : "resume", owner);
    const updated = yield* store.change(yield* ownedKey(owner, key), expectedRevision, change);

    return snapshot(updated, yield* now);
  });

  const cancelSubscription: Subscriptions["Service"]["cancelSubscription"] = Effect.fn(
    "Subscriptions.cancelSubscription",
  )(function* (scopeValue, key, expectedRevision) {
    const owner = yield* scope(scopeValue);

    yield* authorizer.manage("cancel", owner);
    const record = yield* store.cancel(yield* ownedKey(owner, key), expectedRevision);

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
      items: records.map(deliverySnapshot),
      next:
        records.length === limit && last !== undefined
          ? subscriptionDeliveryKeyString(last.key)
          : null,
    };
  });

  const recoverDelivery: Subscriptions["Service"]["recoverDelivery"] = Effect.fn(
    "Subscriptions.recoverDelivery",
  )(function* (scopeValue, key, expectedGeneration) {
    const owner = yield* scope(scopeValue);

    yield* authorizer.manage("recover", owner);
    yield* ownedKey(owner, key.subscription);
    const existing = yield* store.delivery(key);

    if (existing === null) return yield* failure("not-found", "delivery");

    const recovered = yield* store.changeDelivery(key, existing.deliveryId, {
      _tag: "Recover",
      expectedGeneration,
      nowMillis: yield* now,
    });

    return deliverySnapshot(recovered);
  });

  const getSubscription: Subscriptions["Service"]["getSubscription"] = Effect.fn(
    "Subscriptions.getSubscription",
  )(function* (scopeValue, key) {
    const owner = yield* scope(scopeValue);

    yield* authorizer.manage("get", owner);
    const record = yield* store.get(yield* ownedKey(owner, key));

    if (record === null) return yield* failure("not-found", "subscription");

    return snapshot(record, yield* now);
  });

  return Subscriptions.of({
    getSubscription,
    recoverSubscription: (scopeValue, key, expectedRevision) =>
      Effect.gen(function* () {
        const owner = yield* scope(scopeValue);

        yield* authorizer.manage("recover", owner);

        return snapshot(
          yield* store.change(yield* ownedKey(owner, key), expectedRevision, {
            _tag: "Recover",
            nowMillis: yield* now,
          }),
          yield* now,
        );
      }),
    recoverDelivery,
    subscribe,
    updateSubscription,
    pauseSubscription: (scope, key, revision) =>
      changeState(scope, key, revision, { _tag: "Pause" }),
    resumeSubscription: (scope, key, revision) =>
      changeState(scope, key, revision, { _tag: "Resume" }),
    listSubscriptions,
    cancelSubscription,
    listDeliveries,
  });
});

const makeIntake = Effect.fn("SubscriptionIntake.make")(function* (requested: SubscriptionLimits) {
  const limits = yield* validate(SubscriptionLimits, requested);
  const { store, authorizer, source, acceptNormalized } = yield* dependencies;

  const accept: SubscriptionIntake["Service"]["accept"] = Effect.fn("SubscriptionIntake.accept")(
    function* (principal, version, payload) {
      yield* authorizer.intake(store.partition, version, principal);
      const behavior = yield* source(version);
      const event = yield* behavior.normalize(payload);
      const accepted = yield* acceptNormalized(version, event, limits);

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
  const { store, authorizer, source, digest, acceptNormalized } = yield* dependencies;
  const admission = yield* PreparedInputAdmission;

  if (limits.retention !== undefined && admission.submissionStatus === undefined)
    return yield* failure("validation", "retention-requires-submission-status");
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
      subscriptionFingerprint: subscription.configurationFingerprint,
      configurationRevision: subscription.configurationRevision,
      configuration: subscription.configuration,
      eventDigest: event.payloadDigest,
      source: event.source,
      threadId:
        subscription.configuration.destination._tag === "ExistingThread"
          ? subscription.configuration.destination.threadId
          : Schema.decodeSync(ThreadId)(`subscription:${deliveryId}`),
      admissionKey: Schema.decodeSync(IdempotencyKey)(`subscription:${deliveryId}`),
      selectedAtMillis: time,
      ...(limits.retention === undefined ? {} : { observeSettlement: true }),
      state: "selected",
      envelope: null,
      envelopeDigest: null,
      retry: {
        generation: 0,
        attempts: 0,
        automaticAttempts: 0,
        parked: false,
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
      normalized.occurredAtMillis !== event.occurredAtMillis ||
      normalized.matchingKey !== event.matchingKey ||
      (yield* digest(normalized.payload)) !== event.payloadDigest ||
      (yield* digest(event.payload)) !== event.payloadDigest
    )
      return yield* failure("corrupt", "event-source-bindings");
    const candidates = yield* store.candidates(event, limits.batchSize);
    const deliveries: Array<SubscriptionDelivery> = [];
    const time = yield* now;

    for (const candidate of candidates) {
      if (
        candidate.state !== "active" ||
        (candidate.configuration.expiresAtMillis !== null &&
          candidate.configuration.expiresAtMillis <= time)
      )
        continue;

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
        fingerprint !== candidate.configurationFingerprint ||
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
            generation: delivery.retry.generation,
            attempts: delivery.retry.attempts + 1,
            automaticAttempts: delivery.retry.automaticAttempts + 1,
            parked: delivery.retry.automaticAttempts + 1 >= (limits.maxAutomaticAttempts ?? 8),
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
    const current = yield* store.get(delivery.key.subscription);

    const subscription =
      current === null
        ? null
        : {
            ...current,
            configuration: delivery.configuration,
            configurationRevision: delivery.configurationRevision,
            configurationFingerprint: delivery.subscriptionFingerprint,
          };

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
      delivery.eventDigest !== event.payloadDigest ||
      (yield* digest(event.payload)) !== event.payloadDigest ||
      delivery.deliveryId !== expectedId ||
      delivery.admissionKey !== `subscription:${expectedId}` ||
      delivery.threadId !==
        (destination._tag === "ExistingThread"
          ? destination.threadId
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

    if (
      subscription.state === "cancelled" ||
      (subscription.configuration.expiresAtMillis !== null &&
        subscription.configuration.expiresAtMillis <= time)
    ) {
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
      threadId: delivery.threadId,
      admissionKey: delivery.admissionKey,
      deliveryPrincipal: subscription.configuration.deliveryPrincipal,
      ...(subscription.configuration.admissionGroup === undefined
        ? {}
        : { admissionGroup: subscription.configuration.admissionGroup }),
      ...(subscription.configuration.admissionFence === undefined
        ? {}
        : { admissionFence: subscription.configuration.admissionFence }),
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
      delivery.state === "delivered" &&
      delivery.observeSettlement === true &&
      delivery.receipt !== null
    ) {
      const settled =
        admission.submissionStatus === undefined
          ? false
          : yield* admission.submissionStatus(delivery.receipt).pipe(
              Effect.timeout(limits.operationTimeoutMillis),
              Effect.map((status) => status._tag === "settled"),
              Effect.catch((error) =>
                Effect.logWarning("Subscription settlement observation unavailable", {
                  tag: error._tag,
                }).pipe(Effect.as(false)),
              ),
            );

      const time = yield* now;

      yield* store.changeDelivery(delivery.key, delivery.deliveryId, {
        _tag: "ObserveSettlement",
        receipt: delivery.receipt,
        settled,
        nowMillis: time,
        nextAttemptAtMillis: nextAttempt(time),
      });

      return;
    }
    if (
      delivery.retry.parked === true ||
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
      envelope.threadId !== delivery.threadId ||
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
      if (outcome.receipt.threadId !== envelope.threadId)
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

    if (
      subscription.state !== "active" ||
      (subscription.configuration.expiresAtMillis !== null &&
        subscription.configuration.expiresAtMillis <= time)
    )
      return yield* store.deferRecovery(subscription.key, subscription.configurationRevision, null);
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
      return yield* store.deferRecovery(subscription.key, subscription.configurationRevision, {
        attempts: (subscription.recovery?.attempts ?? 0) + 1,
        nextAttemptAtMillis: observation.error.retryable ? nextAttempt(time) : null,
        lastFailure: observation.error.code,
      });
    const observed = observation.event;

    if (observed !== null) {
      const event = yield* acceptNormalized(behavior.source, observed, limits);

      if (event.tombstone === true)
        return yield* store.deferRecovery(subscription.key, subscription.configurationRevision, {
          attempts: (subscription.recovery?.attempts ?? 0) + 1,
          nextAttemptAtMillis: null,
          lastFailure: "event-reclaimed",
        });

      if (
        event.matchingKey !== subscription.configuration.matchingKey ||
        !(yield* behavior.matches(event, subscription))
      )
        return yield* failure("conflict", "reconciliation-match");
      yield* store.catchUp(event, yield* selected(event, subscription), yield* now, limits);

      return;
    }
    yield* store.deferRecovery(subscription.key, subscription.configurationRevision, {
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
            Cause.hasInterruptsOnly(cause)
              ? Effect.interrupt
              : Effect.gen(function* () {
                  failed += 1;
                  const code = failureCode(cause);

                  yield* recover(code).pipe(
                    Effect.catchCause((c) =>
                      Cause.hasInterruptsOnly(c) ? Effect.interrupt : Effect.void,
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
        Effect.gen(function* () {
          const record = yield* store.get(cursor.key);

          if (record === null) return;
          yield* attempt(
            reconcile(record).pipe(Effect.timeout(limits.operationTimeoutMillis)),
            (code) =>
              store.deferRecovery(record.key, record.configurationRevision, {
                attempts: (record.recovery?.attempts ?? 0) + 1,
                nextAttemptAtMillis: code === "unauthorized" ? null : nextAttempt(time),
                lastFailure: code,
              }),
          );
        }),
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

    if (limits.retention !== undefined)
      yield* store.compact(yield* now, limits.retention, limits.batchSize);

    return { processed, failed };
  });

  return SubscriptionDriver.of({ runDue: sweepSemaphore.withPermit(runDue), processDelivery });
});

import {
  DoSubscriptionAlarmControl,
  DoSubscriptionTransaction,
  doSubscriptionStoreLayer,
} from "@effect-agent/storage-cloudflare/DoSubscriptionStore";
import { type EventSources } from "@effect-agent/thread/EventSource";
import { PersistedJson } from "@effect-agent/thread/Records";
import { Principal } from "@effect-agent/thread/SubmissionLedger";
import {
  EventAcknowledgement,
  EventSourceVersion,
  SourcePartition,
  type SubscriptionAuthorizer,
  SubscriptionConfiguration,
  SubscriptionDeliverySnapshot,
  SubscriptionDeliveryKey,
  SubscriptionError,
  SubscriptionFailpointError,
  SubscriptionKey,
  SubscriptionScope,
  type SubscriptionLimits,
  SubscriptionSnapshot,
  SubscriptionSourceError,
  defaultSubscriptionLimits,
} from "@effect-agent/thread/Subscription";
import { type SubscriptionInputBindings } from "@effect-agent/thread/SubscriptionInput";
import {
  SubscriptionDriver,
  SubscriptionIntake,
  Subscriptions,
} from "@effect-agent/thread/Subscriptions";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Cause, Clock, Context, DateTime, Effect, Layer, Schema, type Scope } from "effect";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectAlarm,
  DurableObjectState as EffectCfDurableObjectState,
  type WorkerEnvironment,
} from "effect-cf";

import { type ThreadObjectNamespace } from "./CloudflareBindings.ts";
import { CloudflareThreadClient } from "./CloudflareThreadClient.ts";
import { cloudflarePreparedInputAdmissionLayer } from "./internal/prepared-admission.ts";

const SUBSCRIPTION_ALARM_TAG = "effect-agent/SubscriptionPartitionWake";
const SUBSCRIPTION_ALARM_ID = "driver";
const MAX_ALARM_WALL_MILLIS = 12 * 60_000;
const MAX_ALARMS_PER_INVOCATION = 16;
const MAX_ANCILLARY_ALARM_MILLIS = 30_000;

const SubscriptionAlarmPayload = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  generation: Schema.Int.check(Schema.isGreaterThan(0)),
});

export class SubscriptionAlarmProtocolError extends Schema.TaggedError<SubscriptionAlarmProtocolError>()(
  "SubscriptionAlarmProtocolError",
  { message: Schema.String },
) {}

/** Bounded host diagnostic; credentials and provider responses do not belong in alarm failures. */
export class SubscriptionAlarmExtensionError extends Schema.TaggedError<SubscriptionAlarmExtensionError>()(
  "SubscriptionAlarmExtensionError",
  { code: Schema.NonEmptyString.check(Schema.isMaxLength(128)) },
) {}

/** Native partition services supplied at alarm invocation, after the host Layer is built. */
export type SubscriptionPartitionAlarmServices =
  | Subscriptions
  | SubscriptionIntake
  | SubscriptionDriver;

export interface SubscriptionPartitionAlarmHandler<R = SubscriptionPartitionAlarmServices> {
  readonly tag: string;
  readonly handle: (
    event: DurableObjectAlarm.DurableObjectAlarmEvent,
  ) => Effect.Effect<void, SubscriptionAlarmProtocolError | SubscriptionAlarmExtensionError, R>;
}

/** Host-only handlers; the framework reserves its namespace and rejects every unknown tag. */
export const SubscriptionPartitionAlarmExtension = Context.Reference<{
  readonly handlers: ReadonlyArray<SubscriptionPartitionAlarmHandler>;
}>("@effect-agent/platform-cloudflare/SubscriptionPartitionAlarmExtension", {
  defaultValue: () => ({ handlers: [] }),
});

/** Capture host services once, deferring native partition services to invocation.
 * Each invocation owns its codec/handler Scope and timeout.
 * Callback failures stay typed. Defects and interruption reach the native alarm multiplexer.
 * The host owns durable idempotency, prearming and external-effect uncertainty.
 */
export const makeSubscriptionPartitionAlarmHandler = Effect.fn(
  "makeSubscriptionPartitionAlarmHandler",
)(function* <Payload extends Schema.Top, R>(options: {
  readonly tag: string;
  readonly payload: Payload;
  readonly timeoutMillis: number;
  readonly handle: (
    event: Omit<DurableObjectAlarm.DurableObjectAlarmEvent, "payload"> & {
      readonly payload: Payload["Type"];
    },
  ) => Effect.Effect<void, SubscriptionAlarmExtensionError, R>;
}): Effect.fn.Return<
  SubscriptionPartitionAlarmHandler<
    Exclude<
      Exclude<R | Payload["DecodingServices"], Scope.Scope>,
      Exclude<
        Exclude<R | Payload["DecodingServices"], Scope.Scope | SubscriptionPartitionAlarmServices>,
        SubscriptionPartitionAlarmServices
      >
    >
  >,
  SubscriptionAlarmProtocolError,
  Exclude<R | Payload["DecodingServices"], Scope.Scope | SubscriptionPartitionAlarmServices>
> {
  if (
    options.tag.length === 0 ||
    options.tag.length > 128 ||
    options.tag.startsWith("effect-agent/") ||
    !Number.isSafeInteger(options.timeoutMillis) ||
    options.timeoutMillis < 1 ||
    options.timeoutMillis > MAX_ANCILLARY_ALARM_MILLIS
  )
    return yield* SubscriptionAlarmProtocolError.make({
      message: "Invalid ancillary alarm tag or timeout",
    });

  // Context capture includes unrequested services too; never retain a host override of native work.
  const services = (yield* Effect.context<
    Exclude<R | Payload["DecodingServices"], Scope.Scope | SubscriptionPartitionAlarmServices>
  >()).pipe(Context.omit(Subscriptions, SubscriptionIntake, SubscriptionDriver));

  return {
    tag: options.tag,
    handle: (event) =>
      Effect.gen(function* () {
        if (event.tag !== options.tag)
          return yield* SubscriptionAlarmProtocolError.make({
            message: "Ancillary alarm tag mismatch",
          });

        const payload = yield* Schema.decodeEffect(options.payload)(event.payload).pipe(
          Effect.mapError(() =>
            SubscriptionAlarmProtocolError.make({ message: "Invalid ancillary alarm payload" }),
          ),
        );

        yield* options.handle({ ...event, payload });
      }).pipe(
        Effect.scoped,
        Effect.timeoutOrElse({
          duration: options.timeoutMillis,
          orElse: () => SubscriptionAlarmExtensionError.make({ code: "timeout" }),
        }),
        Effect.provideContext(services),
      ),
  };
});

export class SubscriptionPartitionProtocolError extends Schema.TaggedError<SubscriptionPartitionProtocolError>()(
  "SubscriptionPartitionProtocolError",
  { message: Schema.String.check(Schema.isMaxLength(4_096)) },
) {}

export class CloudflareSubscriptionConfigError extends Schema.TaggedError<CloudflareSubscriptionConfigError>()(
  "CloudflareSubscriptionConfigError",
  { message: Schema.String },
) {}

/** Reject limits whose four bounded phases could exceed the safe Durable Object alarm budget. */
export const validateCloudflareSubscriptionLimits = (
  limits: SubscriptionLimits,
  options: { readonly ancillaryAlarms?: boolean } = {},
): Effect.Effect<void, CloudflareSubscriptionConfigError> => {
  const worstCaseMillis =
    4 * Math.ceil(limits.batchSize / limits.concurrency) * limits.operationTimeoutMillis +
    (options.ancillaryAlarms === true ? MAX_ALARMS_PER_INVOCATION * MAX_ANCILLARY_ALARM_MILLIS : 0);

  return worstCaseMillis <= MAX_ALARM_WALL_MILLIS
    ? Effect.void
    : Effect.fail(
        CloudflareSubscriptionConfigError.make({
          message: "Subscription limits can exceed the bounded Durable Object alarm wall budget",
        }),
      );
};

const protocolMessage = (message: string): string =>
  message.length <= 4_096 ? message : `${message.slice(0, 4_093)}...`;

const SubscribeRequest = Schema.TaggedStruct("Subscribe", {
  schemaVersion: Schema.Literal(1),
  scope: Schema.Struct({
    ...SubscriptionScope.fields,
  }),
  options: Schema.Struct({
    subscriptionId: SubscriptionKey.fields.subscriptionId,
    source: EventSourceVersion,
    parameters: PersistedJson,
    context: PersistedJson,
    mode: Schema.Literals(["once", "continuous"]),
    expiresAtMillis: Schema.NullOr(Schema.Number),
    destination: SubscriptionConfiguration.fields.destination,
    deliveryPrincipal: SubscriptionConfiguration.fields.deliveryPrincipal,
    admissionGroup: SubscriptionConfiguration.fields.admissionGroup,
    admissionFence: SubscriptionConfiguration.fields.admissionFence,
    agentId: SubscriptionConfiguration.fields.agentId,
    definitions: SubscriptionConfiguration.fields.definitions,
  }),
});

const UpdateSubscriptionRequest = Schema.TaggedStruct("UpdateSubscription", {
  schemaVersion: Schema.Literal(1),
  scope: SubscribeRequest.fields.scope,
  key: SubscriptionKey,
  expectedRevision: Schema.Int.check(Schema.isGreaterThan(0)),
  options: SubscribeRequest.fields.options.mapFields(({ subscriptionId: _, ...fields }) => fields),
});

const SubscriptionStateRequest = Schema.Struct({
  _tag: Schema.Literals(["PauseSubscription", "ResumeSubscription", "RecoverSubscription"]),
  schemaVersion: Schema.Literal(1),
  scope: SubscribeRequest.fields.scope,
  key: SubscriptionKey,
  expectedRevision: Schema.Int.check(Schema.isGreaterThan(0)),
});

const ListSubscriptionsRequest = Schema.TaggedStruct("ListSubscriptions", {
  schemaVersion: Schema.Literal(1),
  scope: SubscribeRequest.fields.scope,
  after: Schema.optionalKey(Schema.Natural),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
});

const CancelSubscriptionRequest = Schema.TaggedStruct("CancelSubscription", {
  expectedRevision: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  schemaVersion: Schema.Literal(1),
  scope: SubscribeRequest.fields.scope,
  key: SubscriptionKey,
});

const GetSubscriptionRequest = Schema.TaggedStruct("GetSubscription", {
  schemaVersion: Schema.Literal(1),
  scope: SubscriptionScope,
  key: SubscriptionKey,
});

const RecoverDeliveryRequest = Schema.TaggedStruct("RecoverDelivery", {
  expectedGeneration: Schema.Natural,
  schemaVersion: Schema.Literal(1),
  scope: SubscribeRequest.fields.scope,
  key: SubscriptionDeliveryKey,
});

const ListDeliveriesRequest = Schema.TaggedStruct("ListDeliveries", {
  schemaVersion: Schema.Literal(1),
  scope: SubscribeRequest.fields.scope,
  key: SubscriptionKey,
  after: Schema.optionalKey(Schema.String),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
});

const AcceptRequest = Schema.TaggedStruct("Accept", {
  schemaVersion: Schema.Literal(1),
  partition: SourcePartition,
  principal: Principal,
  source: EventSourceVersion,
  payload: PersistedJson,
});

const StatusRequest = Schema.TaggedStruct("Status", {
  schemaVersion: Schema.Literal(1),
  partition: SourcePartition,
  principal: Principal,
  source: EventSourceVersion,
  eventId: Schema.String,
});

const SubscriptionPartitionRequest = Schema.Union([
  SubscribeRequest,
  GetSubscriptionRequest,
  UpdateSubscriptionRequest,
  SubscriptionStateRequest,
  ListSubscriptionsRequest,
  CancelSubscriptionRequest,
  ListDeliveriesRequest,
  RecoverDeliveryRequest,
  AcceptRequest,
  StatusRequest,
]);

type SubscriptionPartitionRequest = typeof SubscriptionPartitionRequest.Type;

const SubscriptionPage = Schema.Struct({
  items: Schema.Array(SubscriptionSnapshot),
  next: Schema.NullOr(Schema.Natural),
});

const DeliveryPage = Schema.Struct({
  items: Schema.Array(SubscriptionDeliverySnapshot),
  next: Schema.NullOr(Schema.String),
});

const IntakeStatus = Schema.Struct({
  ...EventAcknowledgement.fields,
  routingComplete: Schema.Boolean,
  routingFailure: Schema.NullOr(Schema.String),
  nextAttemptAtMillis: Schema.Number,
});

const SubscriptionPartitionFailure = Schema.Union([
  SubscriptionError,
  SubscriptionSourceError,
  SubscriptionFailpointError,
  SubscriptionPartitionProtocolError,
]);

type SubscriptionPartitionFailure = typeof SubscriptionPartitionFailure.Type;

const SubscriptionPartitionResponse = Schema.Union([
  Schema.TaggedStruct("Snapshot", { value: SubscriptionSnapshot }),
  Schema.TaggedStruct("SubscriptionPage", { value: SubscriptionPage }),
  Schema.TaggedStruct("DeliveryPage", { value: DeliveryPage }),
  Schema.TaggedStruct("DeliverySnapshot", { value: SubscriptionDeliverySnapshot }),
  Schema.TaggedStruct("Acknowledgement", { value: EventAcknowledgement }),
  Schema.TaggedStruct("Status", { value: IntakeStatus }),
  Schema.TaggedStruct("Failed", { failure: SubscriptionPartitionFailure }),
]);

type SubscriptionPartitionResponse = typeof SubscriptionPartitionResponse.Type;

const decodeRequest = Schema.decodeUnknownEffect(SubscriptionPartitionRequest);
const encodeRequest = Schema.encodeEffect(SubscriptionPartitionRequest);
const decodeResponse = Schema.decodeUnknownEffect(SubscriptionPartitionResponse);
const encodeResponse = Schema.encodeEffect(SubscriptionPartitionResponse);

const protocolFailure = (message: string): SubscriptionPartitionResponse => ({
  _tag: "Failed",
  failure: SubscriptionPartitionProtocolError.make({ message: protocolMessage(message) }),
});

export const sourcePartitionName = (partition: SourcePartition): string =>
  JSON.stringify([partition.tenantId, partition.address]);

export interface SubscriptionPartitionObjectRpc extends Rpc.DurableObjectBranded {
  subscription(encoded: unknown): Promise<unknown>;
}

export class SubscriptionPartitionNamespace extends Context.Service<
  SubscriptionPartitionNamespace,
  { readonly namespace: DurableObjectNamespace<SubscriptionPartitionObjectRpc> }
>()("@effect-agent/platform-cloudflare/SubscriptionPartitionNamespace") {}

const storageFailure = (code: string): SubscriptionError =>
  SubscriptionError.make({ reason: "storage", code });

const corruptFailure = (code: string): SubscriptionError =>
  SubscriptionError.make({ reason: "corrupt", code });

/** Worker-side management and trusted intake, routed to one fresh partition stub per call. */
export class CloudflareSubscriptionsClient {
  static layer(
    partition: SourcePartition,
  ): Layer.Layer<Subscriptions | SubscriptionIntake, never, SubscriptionPartitionNamespace> {
    return Layer.effectContext(
      Effect.gen(function* () {
        const { namespace } = yield* SubscriptionPartitionNamespace;

        const call = Effect.fn("CloudflareSubscriptionsClient.call")(function* (
          addressedPartition: SourcePartition,
          request: SubscriptionPartitionRequest,
        ) {
          if (!samePartition(partition, addressedPartition)) {
            return yield* SubscriptionError.make({ reason: "unauthorized", code: "partition" });
          }

          const encoded = yield* encodeRequest(request).pipe(
            Effect.mapError(() => corruptFailure("subscription-partition-protocol")),
          );

          const raw = yield* Effect.tryPromise({
            try: () =>
              namespace
                .get(namespace.idFromName(sourcePartitionName(partition)))
                .subscription(encoded),
            catch: () => storageFailure("call-subscription-partition"),
          });

          return yield* decodeResponse(raw).pipe(
            Effect.mapError(() => corruptFailure("subscription-partition-protocol")),
          );
        });

        const failed = (response: SubscriptionPartitionResponse): SubscriptionPartitionFailure =>
          response._tag === "Failed"
            ? response.failure
            : SubscriptionPartitionProtocolError.make({
                message: "Unexpected subscription response",
              });

        const asProtocolError = (): SubscriptionError =>
          corruptFailure("subscription-partition-protocol");

        const changeState = Effect.fn("CloudflareSubscriptions.changeState")(function* (
          scope: typeof SubscriptionScope.Type,
          key: typeof SubscriptionKey.Type,
          expectedRevision: number,
          _tag: "PauseSubscription" | "ResumeSubscription" | "RecoverSubscription",
        ) {
          const response = yield* call(scope.partition, {
            _tag,
            schemaVersion: 1,
            scope,
            key,
            expectedRevision,
          });

          if (response._tag === "Snapshot") return response.value;
          const failure = failed(response);

          return yield* failure._tag === "SubscriptionError" ||
          failure._tag === "SubscriptionFailpointError"
            ? failure
            : asProtocolError();
        });

        const subscriptions = Subscriptions.of({
          recoverSubscription: (scope, key, revision) =>
            changeState(scope, key, revision, "RecoverSubscription"),
          getSubscription: (scope, key) =>
            Effect.gen(function* () {
              const response = yield* call(scope.partition, {
                _tag: "GetSubscription",
                schemaVersion: 1,
                scope,
                key,
              });

              if (response._tag === "Snapshot") return response.value;
              const failure = failed(response);

              return yield* failure._tag === "SubscriptionError" ? failure : asProtocolError();
            }),
          recoverDelivery: (scope, key, expectedGeneration) =>
            Effect.gen(function* () {
              const response = yield* call(scope.partition, {
                _tag: "RecoverDelivery",
                expectedGeneration,
                schemaVersion: 1,
                scope,
                key,
              });

              if (response._tag === "DeliverySnapshot") return response.value;
              const failure = failed(response);

              return yield* failure._tag === "SubscriptionError" ||
              failure._tag === "SubscriptionFailpointError"
                ? failure
                : asProtocolError();
            }),
          updateSubscription: (scope, key, expectedRevision, options) =>
            Effect.gen(function* () {
              const response = yield* call(scope.partition, {
                _tag: "UpdateSubscription",
                schemaVersion: 1,
                scope,
                key,
                expectedRevision,
                options,
              });

              if (response._tag === "Snapshot") return response.value;
              const failure = failed(response);

              return yield* failure._tag === "SubscriptionError" ||
              failure._tag === "SubscriptionFailpointError" ||
              failure._tag === "SubscriptionSourceError"
                ? failure
                : asProtocolError();
            }),
          pauseSubscription: (scope, key, revision) =>
            changeState(scope, key, revision, "PauseSubscription"),
          resumeSubscription: (scope, key, revision) =>
            changeState(scope, key, revision, "ResumeSubscription"),
          subscribe: (scope, options) =>
            Effect.gen(function* () {
              const response = yield* call(scope.partition, {
                _tag: "Subscribe",
                schemaVersion: 1,
                scope,
                options,
              });

              if (response._tag === "Snapshot") return response.value;
              const failure = failed(response);

              if (
                failure._tag === "SubscriptionError" ||
                failure._tag === "SubscriptionSourceError" ||
                failure._tag === "SubscriptionFailpointError"
              )
                return yield* failure;

              return yield* asProtocolError();
            }),
          listSubscriptions: (scope, after, limit) =>
            Effect.gen(function* () {
              const response = yield* call(scope.partition, {
                _tag: "ListSubscriptions",
                schemaVersion: 1,
                scope,
                ...(after === undefined ? {} : { after }),
                ...(limit === undefined ? {} : { limit }),
              });

              if (response._tag === "SubscriptionPage") return response.value;
              const failure = failed(response);

              return yield* failure._tag === "SubscriptionError" ? failure : asProtocolError();
            }),
          cancelSubscription: (scope, key, expectedRevision) =>
            Effect.gen(function* () {
              const response = yield* call(scope.partition, {
                _tag: "CancelSubscription",
                ...(expectedRevision === undefined ? {} : { expectedRevision }),
                schemaVersion: 1,
                scope,
                key,
              });

              if (response._tag === "Snapshot") return response.value;
              const failure = failed(response);

              if (
                failure._tag === "SubscriptionError" ||
                failure._tag === "SubscriptionFailpointError"
              )
                return yield* failure;

              return yield* asProtocolError();
            }),
          listDeliveries: (scope, key, after, limit) =>
            Effect.gen(function* () {
              const response = yield* call(scope.partition, {
                _tag: "ListDeliveries",
                schemaVersion: 1,
                scope,
                key,
                ...(after === undefined ? {} : { after }),
                ...(limit === undefined ? {} : { limit }),
              });

              if (response._tag === "DeliveryPage") return response.value;
              const failure = failed(response);

              return yield* failure._tag === "SubscriptionError" ? failure : asProtocolError();
            }),
        });

        const intake = SubscriptionIntake.of({
          accept: (principal, source, payload) =>
            Effect.gen(function* () {
              const request = yield* Schema.decodeUnknownEffect(AcceptRequest)({
                _tag: "Accept",
                schemaVersion: 1,
                partition,
                principal,
                source,
                payload,
              }).pipe(
                Effect.mapError(() =>
                  SubscriptionError.make({ reason: "validation", code: "event-payload" }),
                ),
              );

              const response = yield* call(request.partition, request);

              if (response._tag === "Acknowledgement") return response.value;
              const failure = failed(response);

              if (
                failure._tag === "SubscriptionError" ||
                failure._tag === "SubscriptionSourceError" ||
                failure._tag === "SubscriptionFailpointError"
              )
                return yield* failure;

              return yield* asProtocolError();
            }),
          status: (principal, source, eventId) =>
            Effect.gen(function* () {
              const response = yield* call(partition, {
                _tag: "Status",
                schemaVersion: 1,
                partition,
                principal,
                source,
                eventId,
              });

              if (response._tag === "Status") return response.value;
              const failure = failed(response);

              return yield* failure._tag === "SubscriptionError" ? failure : asProtocolError();
            }),
        });

        return Context.make(Subscriptions, subscriptions).pipe(
          Context.add(SubscriptionIntake, intake),
        );
      }),
    );
  }
}

export class SubscriptionPartitionIdentity extends Context.Service<
  SubscriptionPartitionIdentity,
  { readonly partition: SourcePartition }
>()("@effect-agent/platform-cloudflare/SubscriptionPartitionIdentity") {}

const decodePartitionName = Effect.fn("decodeSubscriptionPartitionName")(function* (
  name: string | null | undefined,
) {
  if (name === null || name === undefined) {
    return yield* SubscriptionPartitionProtocolError.make({
      message: "Subscription Partition objects require an idFromName identity",
    });
  }

  const tuple = yield* Schema.decodeEffect(
    Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String])),
  )(name).pipe(
    Effect.mapError(() =>
      SubscriptionPartitionProtocolError.make({
        message: "Subscription Partition name is malformed",
      }),
    ),
  );

  return yield* Schema.decodeEffect(SourcePartition)({
    tenantId: tuple[0],
    address: tuple[1],
  }).pipe(
    Effect.mapError(() =>
      SubscriptionPartitionProtocolError.make({
        message: "Subscription Partition identity is invalid",
      }),
    ),
  );
});

const samePartition = Schema.toEquivalence(SourcePartition);

const requestPartition = (request: SubscriptionPartitionRequest): SourcePartition =>
  request._tag === "Accept" || request._tag === "Status"
    ? request.partition
    : request.scope.partition;

const handleRequest = Effect.fn("SubscriptionPartition.handleRequest")(function* (
  encoded: unknown,
) {
  const decoded = yield* decodeRequest(encoded).pipe(Effect.result);

  if (decoded._tag === "Failure") {
    return yield* encodeResponse(
      protocolFailure("The subscription request could not be decoded"),
    ).pipe(Effect.orDie);
  }
  const request = decoded.success;
  const { partition } = yield* SubscriptionPartitionIdentity;

  if (!samePartition(partition, requestPartition(request))) {
    return yield* encodeResponse(
      protocolFailure("The request partition does not match the addressed object"),
    ).pipe(Effect.orDie);
  }
  const subscriptions = yield* Subscriptions;
  const intake = yield* SubscriptionIntake;

  const response = yield* Effect.gen(function* (): Effect.fn.Return<
    SubscriptionPartitionResponse,
    SubscriptionPartitionFailure
  > {
    switch (request._tag) {
      case "GetSubscription":
        return {
          _tag: "Snapshot",
          value: yield* subscriptions.getSubscription(request.scope, request.key),
        };
      case "Subscribe":
        return {
          _tag: "Snapshot",
          value: yield* subscriptions.subscribe(request.scope, request.options),
        };
      case "UpdateSubscription":
        return {
          _tag: "Snapshot",
          value: yield* subscriptions.updateSubscription(
            request.scope,
            request.key,
            request.expectedRevision,
            request.options,
          ),
        };
      case "RecoverSubscription":
        return {
          _tag: "Snapshot",
          value: yield* subscriptions.recoverSubscription(
            request.scope,
            request.key,
            request.expectedRevision,
          ),
        };
      case "PauseSubscription":
        return {
          _tag: "Snapshot",
          value: yield* subscriptions.pauseSubscription(
            request.scope,
            request.key,
            request.expectedRevision,
          ),
        };
      case "ResumeSubscription":
        return {
          _tag: "Snapshot",
          value: yield* subscriptions.resumeSubscription(
            request.scope,
            request.key,
            request.expectedRevision,
          ),
        };
      case "ListSubscriptions":
        return {
          _tag: "SubscriptionPage",
          value: yield* subscriptions.listSubscriptions(
            request.scope,
            request.after,
            request.limit,
          ),
        };
      case "CancelSubscription":
        return {
          _tag: "Snapshot",
          value: yield* subscriptions.cancelSubscription(
            request.scope,
            request.key,
            request.expectedRevision,
          ),
        };
      case "RecoverDelivery":
        return {
          _tag: "DeliverySnapshot",
          value: yield* subscriptions.recoverDelivery(
            request.scope,
            request.key,
            request.expectedGeneration,
          ),
        };
      case "ListDeliveries":
        return {
          _tag: "DeliveryPage",
          value: yield* subscriptions.listDeliveries(
            request.scope,
            request.key,
            request.after,
            request.limit,
          ),
        };
      case "Accept":
        return {
          _tag: "Acknowledgement",
          value: yield* intake.accept(request.principal, request.source, request.payload),
        };
      case "Status":
        return {
          _tag: "Status",
          value: yield* intake.status(request.principal, request.source, request.eventId),
        };
    }
  }).pipe(
    Effect.catch((failure) =>
      Schema.is(SubscriptionPartitionFailure)(failure)
        ? Effect.succeed({ _tag: "Failed" as const, failure })
        : Effect.succeed(protocolFailure("The subscription operation failed outside its contract")),
    ),
  );

  return yield* encodeResponse(response).pipe(Effect.orDie);
});

const alarmStorageError = (code: string) => () => storageFailure(code);

const transactionLayer: Layer.Layer<
  DoSubscriptionTransaction,
  never,
  DurableObjectAlarm.DurableObjectAlarm
> = Layer.effect(
  DoSubscriptionTransaction,
  Effect.gen(function* () {
    const alarms = yield* DurableObjectAlarm.DurableObjectAlarm;

    return DoSubscriptionTransaction.of({
      run: (body) =>
        Effect.gen(function* () {
          const nowMillis = yield* Clock.currentTimeMillis;

          return yield* alarms
            .transaction((transaction) =>
              body((replacement) =>
                replacement.deadlineAtMillis === null
                  ? transaction
                      .cancelAlarm({ id: SUBSCRIPTION_ALARM_ID, tag: SUBSCRIPTION_ALARM_TAG })
                      .pipe(Effect.mapError(alarmStorageError("cancel-subscription-alarm")))
                  : Effect.fromOption(
                      DateTime.make(Math.max(replacement.deadlineAtMillis, nowMillis + 1)),
                    ).pipe(
                      Effect.mapError(() => corruptFailure("subscription-alarm-deadline")),
                      Effect.flatMap((runAt) =>
                        transaction
                          .scheduleAlarm({
                            id: SUBSCRIPTION_ALARM_ID,
                            tag: SUBSCRIPTION_ALARM_TAG,
                            runAt,
                            payload: { schemaVersion: 1, generation: replacement.generation },
                          })
                          .pipe(Effect.mapError(alarmStorageError("schedule-subscription-alarm"))),
                      ),
                    ),
              ),
            )
            .pipe(
              Effect.catchTag("StorageOperationError", () =>
                storageFailure("commit-subscription-transaction"),
              ),
            );
        }),
    });
  }),
);

const alarmHandler = (limits: SubscriptionLimits) =>
  DurableObjectAlarm.processDue(
    (event) =>
      Effect.gen(function* () {
        if (event.tag !== SUBSCRIPTION_ALARM_TAG) {
          const { handlers } = yield* SubscriptionPartitionAlarmExtension;
          const matches = handlers.filter((handler) => handler.tag === event.tag);
          const handler = matches[0];

          if (
            event.tag.startsWith("effect-agent/") ||
            matches.length !== 1 ||
            handler === undefined
          )
            return yield* SubscriptionAlarmProtocolError.make({
              message: "Unknown or ambiguous ancillary alarm tag",
            });

          return yield* handler.handle(event).pipe(
            Effect.scoped,
            Effect.timeoutOrElse({
              duration: MAX_ANCILLARY_ALARM_MILLIS,
              orElse: () => SubscriptionAlarmExtensionError.make({ code: "timeout" }),
            }),
          );
        }
        if (event.id !== SUBSCRIPTION_ALARM_ID) {
          return yield* SubscriptionAlarmProtocolError.make({
            message: `Unsupported Subscription Partition alarm ${event.tag}/${event.id}`,
          });
        }
        yield* Schema.decodeUnknownEffect(SubscriptionAlarmPayload)(event.payload).pipe(
          Effect.mapError(() =>
            SubscriptionAlarmProtocolError.make({
              message: "Unsupported subscription alarm payload",
            }),
          ),
        );
        const driver = yield* SubscriptionDriver;
        const alarmControl = yield* DoSubscriptionAlarmControl;

        yield* alarmControl.prearm((yield* Clock.currentTimeMillis) + limits.retryMillis);

        const pass = yield* driver.runDue.pipe(
          Effect.catchCauseIf(
            (cause) => !Cause.hasInterrupts(cause),
            (cause) =>
              Clock.currentTimeMillis.pipe(
                Effect.flatMap((time) => alarmControl.prearm(time + limits.retryMillis)),
                Effect.andThen(Effect.failCause(cause)),
              ),
          ),
        );

        if (pass.failed > 0) {
          yield* alarmControl.prearm((yield* Clock.currentTimeMillis) + limits.retryMillis);
        } else {
          yield* alarmControl.reconcile;
        }
      }),
    {
      mode: "isolated",
      limit: MAX_ALARMS_PER_INVOCATION,
      retryFailedAfter: limits.retryMillis,
      onFailure: () => Effect.logWarning("Subscription partition alarm retained for retry"),
    },
  ).pipe(Effect.asVoid);

type SubscriptionRuntimeServices =
  | Subscriptions
  | SubscriptionIntake
  | SubscriptionDriver
  | DoSubscriptionAlarmControl
  | SubscriptionPartitionIdentity
  | DurableObjectAlarm.DurableObjectAlarm;

export interface SubscriptionPartitionObjectInstance extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<Record<never, never>, SubscriptionRuntimeServices>
> {
  subscription(encoded: unknown): Promise<unknown>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

export interface SubscriptionPartitionObjectClass {
  new (ctx: DurableObjectState, env: Cloudflare.Env): SubscriptionPartitionObjectInstance;
}

/**
 * Build one source-addressed SQLite Durable Object. The host Layer binds the permitted source
 * catalog and authority. It must not retain cleanup-scoped resources across RPC events.
 */
export const makeSubscriptionPartitionObjectClass = <E>(
  host: Layer.Layer<
    SubscriptionAuthorizer | EventSources | SubscriptionInputBindings | ThreadObjectNamespace,
    E,
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
    | SubscriptionPartitionIdentity
  >,
  limits: SubscriptionLimits = defaultSubscriptionLimits,
): SubscriptionPartitionObjectClass => {
  const cloudflareLimitsLayer = Layer.effectDiscard(validateCloudflareSubscriptionLimits(limits));

  const identityLayer = Layer.effect(
    SubscriptionPartitionIdentity,
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;

      return SubscriptionPartitionIdentity.of({
        partition: yield* decodePartitionName(state.raw.id.name),
      });
    }),
  );

  const sqlLayer = Layer.unwrap(
    Effect.map(EffectCfDurableObjectState.DurableObjectState, (state) =>
      SqliteClient.layer({ storage: state.raw.storage }),
    ),
  );

  const partitionStore = Layer.unwrap(
    Effect.map(SubscriptionPartitionIdentity, ({ partition }) =>
      doSubscriptionStoreLayer(partition).pipe(
        Layer.provide(transactionLayer),
        Layer.provide(sqlLayer),
      ),
    ),
  );

  const application = Layer.mergeAll(
    Subscriptions.layer(limits),
    SubscriptionIntake.layer(limits),
    SubscriptionDriver.layer(limits),
    cloudflareLimitsLayer,
    // Capture the host reference before its scoped input Layer is hidden from the runtime.
    Layer.effect(
      SubscriptionPartitionAlarmExtension,
      Effect.gen(function* () {
        const extension = yield* SubscriptionPartitionAlarmExtension;
        const tags = extension.handlers.map((handler) => handler.tag);

        if (
          tags.length > MAX_ALARMS_PER_INVOCATION ||
          new Set(tags).size !== tags.length ||
          tags.some(
            (tag) => tag.length === 0 || tag.length > 128 || tag.startsWith("effect-agent/"),
          )
        )
          return yield* SubscriptionAlarmProtocolError.make({
            message: "Invalid ancillary alarm handler registry",
          });

        yield* validateCloudflareSubscriptionLimits(limits, { ancillaryAlarms: tags.length > 0 });

        return extension;
      }),
    ),
  ).pipe(
    Layer.provideMerge(partitionStore),
    Layer.provide(
      cloudflarePreparedInputAdmissionLayer.pipe(Layer.provide(CloudflareThreadClient.layer)),
    ),
    Layer.provide(BrowserCrypto.layer),
    Layer.provideMerge(DurableObjectAlarm.DurableObjectAlarm.layer),
    Layer.provide(host),
    Layer.provideMerge(identityLayer),
  );

  const runtime: Layer.Layer<
    SubscriptionRuntimeServices,
    | E
    | SubscriptionError
    | SubscriptionPartitionProtocolError
    | CloudflareSubscriptionConfigError
    | SubscriptionAlarmProtocolError,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
  > = Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const scope = yield* Effect.scope;

      return yield* state.blockConcurrencyWhile(Layer.buildWithScope(application, scope));
    }),
  );

  const rpc = {
    subscription: (encoded: unknown) => handleRequest(encoded),
  } satisfies EffectCfDurableObject.DurableObjectRpc<SubscriptionRuntimeServices>;

  const Base = EffectCfDurableObject.make(runtime, {
    initialize: Effect.void,
    rpc,
    alarms: alarmHandler(limits),
  });

  class SubscriptionPartitionObject extends Base {
    override alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void {
      return super.alarm?.(alarmInfo);
    }
  }

  return SubscriptionPartitionObject;
};

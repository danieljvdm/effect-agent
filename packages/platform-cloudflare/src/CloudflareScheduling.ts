import { AgentId } from "@effect-agent/core/Identifiers";
import {
  DoScheduleAlarmControl,
  DoScheduleTransaction,
  scheduleStoreLayer,
} from "@effect-agent/storage-cloudflare/DoScheduleStore";
import { type DurableSubmitAgent } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigests, PersistedJson } from "@effect-agent/thread/Records";
import {
  ScheduleAuthorizationError,
  type ScheduleAuthorizer,
  ScheduleCapacityError,
  ScheduleConflict,
  ScheduleDestination,
  ScheduleFailpointError,
  ScheduleId,
  type SchedulingLimits,
  ScheduleNotFound,
  ScheduleOwner,
  type ScheduleScope,
  ScheduleScope as ScheduleScopeSchema,
  ScheduleSnapshot,
  ScheduleSnapshotPage as ScheduleSnapshotPageSchema,
  ScheduleStorageError,
  ScheduleTimingRequest,
  ScheduleValidationError,
  defaultSchedulingLimits,
} from "@effect-agent/thread/Schedule";
import { scheduleOwnerKey } from "@effect-agent/thread/ScheduleTransition";
import {
  Scheduling,
  ScheduleDriver,
  type ScheduleManagementFailure,
  ScheduleWakeNoop,
} from "@effect-agent/thread/Scheduling";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Clock, Context, DateTime, Effect, Layer, Schema } from "effect";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectAlarm,
  DurableObjectState as EffectCfDurableObjectState,
  type WorkerEnvironment,
} from "effect-cf";

import type { ThreadObjectNamespace } from "./CloudflareBindings.ts";
import { CloudflareThreadClient } from "./CloudflareThreadClient.ts";
import {
  cloudflarePreparedInputAdmissionLayer,
  cloudflareScheduledInputAdmissionLayer,
} from "./internal/prepared-admission.ts";

const SCHEDULE_ALARM_TAG = "effect-agent/ScheduleOwnerWake";
const SCHEDULE_ALARM_ID = "driver";

const ScheduleAlarmPayload = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  generation: Schema.Int.check(Schema.isGreaterThan(0)),
});

export class ScheduleAlarmProtocolError extends Schema.TaggedError<ScheduleAlarmProtocolError>()(
  "ScheduleAlarmProtocolError",
  { message: Schema.String },
) {}

const boundedProtocolMessage = (message: string): string =>
  message.length <= 4_096 ? message : `${message.slice(0, 4_093)}...`;

export class ScheduleOwnerProtocolError extends Schema.TaggedError<ScheduleOwnerProtocolError>()(
  "ScheduleOwnerProtocolError",
  { message: Schema.String.check(Schema.isMaxLength(4_096)) },
) {}

const ScheduleMutationRequestFields = {
  schemaVersion: Schema.Literal(1),
  agentId: AgentId,
  input: PersistedJson,
  scope: ScheduleScopeSchema,
  scheduleId: ScheduleId,
  timing: ScheduleTimingRequest,
  destination: ScheduleDestination,
  deliveryPrincipal: ScheduleScopeSchema.fields.principal,
  definitions: DefinitionDigests,
};

const ScheduleCreateRequest = Schema.TaggedStruct("Create", ScheduleMutationRequestFields);

const ScheduleUpdateRequest = Schema.TaggedStruct("Update", {
  ...ScheduleMutationRequestFields,
  expectedRevision: Schema.Int.check(Schema.isGreaterThan(0)),
});

const ScheduleGetRequest = Schema.TaggedStruct("Get", {
  schemaVersion: Schema.Literal(1),
  scope: ScheduleScopeSchema,
  scheduleId: ScheduleId,
});

const ScheduleListRequest = Schema.TaggedStruct("List", {
  schemaVersion: Schema.Literal(1),
  scope: ScheduleScopeSchema,
  after: Schema.optionalKey(ScheduleId),
  limit: Schema.optionalKey(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
  ),
});

const ScheduleControlRequest = Schema.TaggedStruct("Control", {
  schemaVersion: Schema.Literal(1),
  operation: Schema.Literals(["pause", "resume", "cancel"]),
  scope: ScheduleScopeSchema,
  scheduleId: ScheduleId,
  expectedRevision: Schema.Int.check(Schema.isGreaterThan(0)),
});

const ScheduleOwnerRequest = Schema.Union([
  ScheduleCreateRequest,
  ScheduleUpdateRequest,
  ScheduleGetRequest,
  ScheduleListRequest,
  ScheduleControlRequest,
]);

type ScheduleOwnerRequest = typeof ScheduleOwnerRequest.Type;

const ScheduleOwnerFailure = Schema.Union([
  ScheduleValidationError,
  ScheduleAuthorizationError,
  ScheduleConflict,
  ScheduleNotFound,
  ScheduleCapacityError,
  ScheduleStorageError,
  ScheduleFailpointError,
  ScheduleOwnerProtocolError,
]);

type ScheduleOwnerFailure = typeof ScheduleOwnerFailure.Type;

const ScheduleOwnerResponse = Schema.Union([
  Schema.TaggedStruct("Snapshot", { value: ScheduleSnapshot }),
  Schema.TaggedStruct("Page", { value: ScheduleSnapshotPageSchema }),
  Schema.TaggedStruct("Failed", { failure: ScheduleOwnerFailure }),
]);

type ScheduleOwnerResponse = typeof ScheduleOwnerResponse.Type;

const decodeScheduleOwnerRequest = Schema.decodeUnknownEffect(ScheduleOwnerRequest);
const encodeScheduleOwnerRequest = Schema.encodeEffect(ScheduleOwnerRequest);
const decodeScheduleOwnerResponse = Schema.decodeUnknownEffect(ScheduleOwnerResponse);
const encodeScheduleOwnerResponse = Schema.encodeEffect(ScheduleOwnerResponse);

const scheduleProtocolFailure = (message: string): ScheduleOwnerResponse => ({
  _tag: "Failed",
  failure: ScheduleOwnerProtocolError.make({ message: boundedProtocolMessage(message) }),
});

export interface ScheduleOwnerObjectRpc extends Rpc.DurableObjectBranded {
  schedule(encoded: unknown): Promise<unknown>;
}

export class ScheduleOwnerNamespace extends Context.Service<
  ScheduleOwnerNamespace,
  { readonly namespace: DurableObjectNamespace<ScheduleOwnerObjectRpc> }
>()("@effect-agent/platform-cloudflare/ScheduleOwnerNamespace") {}

const passthroughAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: { id: agentId, input: PersistedJson },
});

const requestOwner = (request: ScheduleOwnerRequest): ScheduleOwner => request.scope.owner;

/** Provides the same authorized management service as NodeScheduling.layer. */
export class CloudflareSchedulingClient {
  static readonly layer: Layer.Layer<Scheduling, never, ScheduleOwnerNamespace> = Layer.effect(
    Scheduling,
    Effect.gen(function* () {
      const { namespace } = yield* ScheduleOwnerNamespace;

      const call = Effect.fn("CloudflareSchedulingClient.call")(function* (
        owner: ScheduleOwner,
        request: ScheduleOwnerRequest,
      ): Effect.fn.Return<ScheduleOwnerResponse, ScheduleManagementFailure> {
        const encoded = yield* encodeScheduleOwnerRequest(request).pipe(
          Effect.mapError(() =>
            ScheduleStorageError.make({ operation: "Schedule Owner protocol", reason: "corrupt" }),
          ),
        );

        const raw = yield* Effect.tryPromise({
          try: () => namespace.get(namespace.idFromName(scheduleOwnerKey(owner))).schedule(encoded),
          catch: () =>
            ScheduleStorageError.make({
              operation: "call Schedule Owner",
              reason: "unavailable",
            }),
        });

        const response = yield* decodeScheduleOwnerResponse(raw).pipe(
          Effect.mapError(() =>
            ScheduleStorageError.make({ operation: "Schedule Owner protocol", reason: "corrupt" }),
          ),
        );

        if (response._tag !== "Failed") return response;

        return yield* response.failure._tag === "ScheduleOwnerProtocolError"
          ? ScheduleStorageError.make({ operation: "Schedule Owner protocol", reason: "corrupt" })
          : response.failure;
      });

      const encodeInput = Effect.fn("CloudflareSchedulingClient.encodeInput")(function* <
        InputSchema extends Schema.Top,
      >(
        agent: DurableSubmitAgent<InputSchema>,
        input: InputSchema["Type"],
      ): Effect.fn.Return<PersistedJson, ScheduleValidationError, InputSchema["EncodingServices"]> {
        const encoded = yield* Schema.encodeEffect(agent.definition.input)(input).pipe(
          Effect.mapError(() =>
            ScheduleValidationError.make({
              message: "Unable to encode Agent input",
            }),
          ),
        );

        return yield* Schema.decodeUnknownEffect(PersistedJson)(encoded).pipe(
          Effect.mapError(() =>
            ScheduleValidationError.make({
              message: "Agent input does not satisfy the canonical persistence bounds",
            }),
          ),
        );
      });

      const create: Scheduling["Service"]["create"] = (agent, input, options) =>
        Effect.gen(function* () {
          const payload = yield* encodeInput(agent, input);

          const response = yield* call(options.scope.owner, {
            _tag: "Create",
            schemaVersion: 1,
            agentId: agent.definition.id,
            input: payload,
            ...options,
          });

          return response._tag === "Snapshot"
            ? response.value
            : yield* ScheduleStorageError.make({
                operation: "Schedule Owner protocol",
                reason: "corrupt",
              });
        });

      const update: Scheduling["Service"]["update"] = (agent, input, options) =>
        Effect.gen(function* () {
          const payload = yield* encodeInput(agent, input);

          const response = yield* call(options.scope.owner, {
            _tag: "Update",
            schemaVersion: 1,
            agentId: agent.definition.id,
            input: payload,
            ...options,
          });

          return response._tag === "Snapshot"
            ? response.value
            : yield* ScheduleStorageError.make({
                operation: "Schedule Owner protocol",
                reason: "corrupt",
              });
        });

      const get: Scheduling["Service"]["get"] = (scope, scheduleId) =>
        Effect.gen(function* () {
          const response = yield* call(scope.owner, {
            _tag: "Get",
            schemaVersion: 1,
            scope,
            scheduleId,
          });

          return response._tag === "Snapshot"
            ? response.value
            : yield* ScheduleStorageError.make({
                operation: "Schedule Owner protocol",
                reason: "corrupt",
              });
        });

      const list: Scheduling["Service"]["list"] = (scope, options = {}) =>
        Effect.gen(function* () {
          const response = yield* call(scope.owner, {
            _tag: "List",
            schemaVersion: 1,
            scope,
            ...(options.after === undefined ? {} : { after: options.after }),
            ...(options.limit === undefined ? {} : { limit: options.limit }),
          });

          return response._tag === "Page"
            ? response.value
            : yield* ScheduleStorageError.make({
                operation: "Schedule Owner protocol",
                reason: "corrupt",
              });
        });

      const control = (
        operation: "pause" | "resume" | "cancel",
        scope: ScheduleScope,
        scheduleId: ScheduleId,
        expectedRevision: number,
      ) =>
        Effect.gen(function* () {
          const response = yield* call(scope.owner, {
            _tag: "Control",
            schemaVersion: 1,
            operation,
            scope,
            scheduleId,
            expectedRevision,
          });

          return response._tag === "Snapshot"
            ? response.value
            : yield* ScheduleStorageError.make({
                operation: "Schedule Owner protocol",
                reason: "corrupt",
              });
        });

      return Scheduling.of({
        create,
        update,
        get,
        list,
        pause: (scope, id, revision) => control("pause", scope, id, revision),
        resume: (scope, id, revision) => control("resume", scope, id, revision),
        cancel: (scope, id, revision) => control("cancel", scope, id, revision),
      });
    }),
  );
}

export class ScheduleOwnerIdentity extends Context.Service<
  ScheduleOwnerIdentity,
  { readonly owner: ScheduleOwner }
>()("@effect-agent/platform-cloudflare/ScheduleOwnerIdentity") {}

const decodeOwnerName = Effect.fn("decodeScheduleOwnerName")(function* (
  name: string | null | undefined,
): Effect.fn.Return<ScheduleOwner, ScheduleOwnerProtocolError> {
  if (name === null || name === undefined) {
    return yield* ScheduleOwnerProtocolError.make({
      message: "Schedule Owner objects require an idFromName identity",
    });
  }

  const tuple = yield* Schema.decodeUnknownEffect(
    Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String])),
  )(name).pipe(
    Effect.mapError(() =>
      ScheduleOwnerProtocolError.make({ message: "Schedule Owner object name is malformed" }),
    ),
  );

  return yield* Schema.decodeUnknownEffect(ScheduleOwner)({
    tenantId: tuple[0],
    ownerId: tuple[1],
  }).pipe(
    Effect.mapError(() =>
      ScheduleOwnerProtocolError.make({ message: "Schedule Owner object identity is invalid" }),
    ),
  );
});

const alarmStorageError = (operation: string) => (error: { readonly _tag: string }) =>
  ScheduleStorageError.make({
    operation,
    reason: error._tag === "StorageOperationError" ? "unavailable" : "corrupt",
  });

const transactionLayer: Layer.Layer<
  DoScheduleTransaction,
  never,
  DurableObjectAlarm.DurableObjectAlarm
> = Layer.effect(
  DoScheduleTransaction,
  Effect.gen(function* () {
    const alarms = yield* DurableObjectAlarm.DurableObjectAlarm;

    return DoScheduleTransaction.of({
      run: (body) =>
        Effect.gen(function* () {
          const nowMillis = yield* Clock.currentTimeMillis;

          return yield* alarms
            .transaction((transaction) =>
              body((replacement) =>
                replacement.deadlineAtMillis === null
                  ? transaction
                      .cancelAlarm({ id: SCHEDULE_ALARM_ID, tag: SCHEDULE_ALARM_TAG })
                      .pipe(Effect.mapError(alarmStorageError("cancel Schedule Owner alarm")))
                  : Effect.fromOption(
                      DateTime.make(Math.max(replacement.deadlineAtMillis, nowMillis + 1)),
                    ).pipe(
                      Effect.mapError(() =>
                        ScheduleStorageError.make({
                          operation: "validate Schedule Owner alarm deadline",
                          reason: "corrupt",
                        }),
                      ),
                      Effect.flatMap((runAt) =>
                        transaction
                          .scheduleAlarm({
                            id: SCHEDULE_ALARM_ID,
                            tag: SCHEDULE_ALARM_TAG,
                            runAt,
                            payload: {
                              schemaVersion: 1,
                              generation: replacement.generation,
                            },
                          })
                          .pipe(
                            Effect.mapError(alarmStorageError("schedule Schedule Owner alarm")),
                          ),
                      ),
                    ),
              ),
            )
            .pipe(
              Effect.catchTag("StorageOperationError", () =>
                ScheduleStorageError.make({
                  operation: "commit Schedule Owner transaction",
                  reason: "unavailable",
                }),
              ),
            );
        }),
    });
  }),
);

type ScheduleRuntimeServices =
  | Scheduling
  | ScheduleDriver
  | DoScheduleAlarmControl
  | ScheduleOwnerIdentity
  | DurableObjectAlarm.DurableObjectAlarm;

const ensureOwner = (
  expected: ScheduleOwner,
  request: ScheduleOwnerRequest,
): Effect.Effect<void, ScheduleOwnerProtocolError> => {
  const observed = requestOwner(request);

  return observed.tenantId === expected.tenantId && observed.ownerId === expected.ownerId
    ? Effect.void
    : Effect.fail(
        ScheduleOwnerProtocolError.make({
          message: "The request owner does not match the addressed Schedule Owner object",
        }),
      );
};

const handleScheduleRequest = Effect.fn("ScheduleOwner.handleRequest")(function* (
  encoded: unknown,
): Effect.fn.Return<unknown, never, Scheduling | ScheduleOwnerIdentity> {
  const decoded = yield* decodeScheduleOwnerRequest(encoded).pipe(Effect.result);

  if (decoded._tag === "Failure") {
    return yield* encodeScheduleOwnerResponse(
      scheduleProtocolFailure("The Schedule request could not be decoded"),
    ).pipe(Effect.orDie);
  }
  const request = decoded.success;
  const { owner } = yield* ScheduleOwnerIdentity;
  const scheduling = yield* Scheduling;

  const response = yield* Effect.gen(function* () {
    yield* ensureOwner(owner, request);
    switch (request._tag) {
      case "Create": {
        const value = yield* scheduling.create(passthroughAgent(request.agentId), request.input, {
          scope: request.scope,
          scheduleId: request.scheduleId,
          timing: request.timing,
          destination: request.destination,
          deliveryPrincipal: request.deliveryPrincipal,
          definitions: request.definitions,
        });

        return { _tag: "Snapshot" as const, value };
      }
      case "Update": {
        const value = yield* scheduling.update(passthroughAgent(request.agentId), request.input, {
          scope: request.scope,
          scheduleId: request.scheduleId,
          timing: request.timing,
          destination: request.destination,
          deliveryPrincipal: request.deliveryPrincipal,
          definitions: request.definitions,
          expectedRevision: request.expectedRevision,
        });

        return { _tag: "Snapshot" as const, value };
      }
      case "Get":
        return {
          _tag: "Snapshot" as const,
          value: yield* scheduling.get(request.scope, request.scheduleId),
        };
      case "List":
        return {
          _tag: "Page" as const,
          value: yield* scheduling.list(request.scope, {
            ...(request.after === undefined ? {} : { after: request.after }),
            ...(request.limit === undefined ? {} : { limit: request.limit }),
          }),
        };
      case "Control": {
        const value =
          request.operation === "pause"
            ? yield* scheduling.pause(request.scope, request.scheduleId, request.expectedRevision)
            : request.operation === "resume"
              ? yield* scheduling.resume(
                  request.scope,
                  request.scheduleId,
                  request.expectedRevision,
                )
              : yield* scheduling.cancel(
                  request.scope,
                  request.scheduleId,
                  request.expectedRevision,
                );

        return { _tag: "Snapshot" as const, value };
      }
    }
  }).pipe(
    Effect.map((value): ScheduleOwnerResponse => value),
    Effect.catch((failure) =>
      Schema.is(ScheduleOwnerFailure)(failure)
        ? Effect.succeed({ _tag: "Failed" as const, failure })
        : Effect.succeed(
            scheduleProtocolFailure("The Schedule operation failed outside its public contract"),
          ),
    ),
  );

  return yield* encodeScheduleOwnerResponse(response).pipe(Effect.orDie);
});

/** @internal The complete native alarm operation, including its event deadline. */
export const scheduleAlarmHandler = (limits: SchedulingLimits) =>
  DurableObjectAlarm.processDue(
    (event) =>
      Effect.gen(function* () {
        if (event.tag !== SCHEDULE_ALARM_TAG || event.id !== SCHEDULE_ALARM_ID) {
          return yield* ScheduleAlarmProtocolError.make({
            message: `Unsupported Schedule Owner alarm ${event.tag}/${event.id}`,
          });
        }
        yield* Schema.decodeUnknownEffect(ScheduleAlarmPayload)(event.payload).pipe(
          Effect.mapError(() =>
            ScheduleAlarmProtocolError.make({
              message: "Unsupported Schedule Owner alarm payload version",
            }),
          ),
        );
        const scheduling = yield* ScheduleDriver;
        const alarmControl = yield* DoScheduleAlarmControl;
        const { owner } = yield* ScheduleOwnerIdentity;
        const nowMillis = yield* Clock.currentTimeMillis;

        yield* alarmControl.prearm(nowMillis + limits.recoveryPollMillis);
        const pass = yield* scheduling.runDue(owner);

        if (pass.failed > 0) {
          yield* alarmControl.prearm((yield* Clock.currentTimeMillis) + limits.recoveryPollMillis);
        } else {
          yield* alarmControl.reconcile;
        }
      }),
    { mode: "ordered" },
  ).pipe(
    // Bound due acquisition and acknowledgement as well as admission. Prepared occurrences
    // and their replacement alarm survive interruption and retain their idempotency keys.
    Effect.timeout("14 minutes"),
    Effect.asVoid,
  );

export interface ScheduleOwnerObjectInstance extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<Record<never, never>, ScheduleRuntimeServices>
> {
  schedule(encoded: unknown): Promise<unknown>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

export interface ScheduleOwnerObjectClass {
  new (ctx: DurableObjectState, env: Cloudflare.Env): ScheduleOwnerObjectInstance;
}

/**
 * The host Layer supplies authorization and routing and is cached for the object incarnation.
 * Cloudflare eviction does not guarantee its finalizers run. Do not acquire resources requiring
 * cleanup in this Layer; acquire them inside scoped `manage` / `prepare` operations instead.
 * Native services belong to effect-cf; the database and alarm runtime remain instance-owned.
 */
export const makeScheduleOwnerObjectClass = <E>(
  host: Layer.Layer<
    ScheduleAuthorizer | ThreadObjectNamespace,
    E,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment | ScheduleOwnerIdentity
  >,
  limits: SchedulingLimits = defaultSchedulingLimits,
): ScheduleOwnerObjectClass => {
  const ownerLayer = Layer.effect(
    ScheduleOwnerIdentity,
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;

      return ScheduleOwnerIdentity.of({ owner: yield* decodeOwnerName(state.raw.id.name) });
    }),
  );

  const sqlLayer = Layer.unwrap(
    Effect.map(EffectCfDurableObjectState.DurableObjectState, (state) =>
      SqliteClient.layer({ storage: state.raw.storage }),
    ),
  );

  const application = Layer.merge(Scheduling.layer(limits), ScheduleDriver.layer(limits)).pipe(
    Layer.provideMerge(
      scheduleStoreLayer.pipe(Layer.provide(transactionLayer), Layer.provide(sqlLayer)),
    ),
    Layer.provide(
      cloudflareScheduledInputAdmissionLayer.pipe(
        Layer.provide(cloudflarePreparedInputAdmissionLayer),
        Layer.provide(CloudflareThreadClient.layer),
      ),
    ),
    Layer.provide(ScheduleWakeNoop),
    Layer.provide(BrowserCrypto.layer),
    Layer.provideMerge(DurableObjectAlarm.DurableObjectAlarm.layer),
    Layer.provide(host),
    Layer.provideMerge(ownerLayer),
  );

  const runtime: Layer.Layer<
    ScheduleRuntimeServices,
    E | ScheduleStorageError | ScheduleOwnerProtocolError | ScheduleValidationError,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
  > = Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const scope = yield* Effect.scope;

      return yield* state.blockConcurrencyWhile(Layer.buildWithScope(application, scope));
    }),
  );

  const rpc = {
    schedule: (encoded: unknown) => handleScheduleRequest(encoded),
  } satisfies EffectCfDurableObject.DurableObjectRpc<ScheduleRuntimeServices>;

  const Base = EffectCfDurableObject.make(runtime, {
    initialize: Effect.void,
    rpc,
    alarms: scheduleAlarmHandler(limits),
  });

  class ScheduleOwnerObject extends Base {
    override alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void {
      return super.alarm?.(alarmInfo);
    }
  }

  return ScheduleOwnerObject;
};

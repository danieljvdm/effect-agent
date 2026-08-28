import { AgentId } from "@effect-agent/core";
import {
  DefinitionDigests,
  PersistedJson,
  type DurableSubmitAgent,
  ScheduleAuthorizationError,
  ScheduleAuthorizer,
  ScheduleCapacityError,
  type ScheduleCreateOptions,
  ScheduleConflict,
  ScheduleDestination,
  ScheduleFailpoint,
  ScheduleFailpointError,
  ScheduleId,
  type SchedulingLimits,
  ScheduleNotFound,
  ScheduleOwner,
  type ScheduleScope,
  ScheduleScope as ScheduleScopeSchema,
  ScheduleSnapshot,
  type ScheduleSnapshotPage,
  ScheduleSnapshotPage as ScheduleSnapshotPageSchema,
  ScheduleStorageError,
  ScheduleTimingRequest,
  type ScheduleUpdateOptions,
  ScheduleValidationError,
  ScheduledInputAdmission,
  ScheduledInputRetryable,
  type ScheduledEnvelope,
  Scheduling,
  defaultSchedulingLimits,
  scheduleOwnerKey,
  ScheduleWake,
} from "@effect-agent/session";
import {
  DoScheduleAlarmControl,
  DoScheduleTransaction,
  scheduleStoreLayer,
} from "@effect-agent/storage-cloudflare";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Clock, Context, DateTime, Effect, Layer, Predicate, Schema, type Scope } from "effect";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectAlarm,
  DurableObjectState as EffectCfDurableObjectState,
  WorkerEnvironment,
} from "effect-cf";

import {
  CloudflareBindingError,
  ConversationObjectNamespace,
  conversationNamespaceFromEnv,
} from "./bindings.ts";
import { CloudflareConversationClient, type ConversationClientError } from "./client.ts";

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
>()("@effect-agent/platform-cloudflare/ScheduleOwnerNamespace") {
  static layer(
    namespace: DurableObjectNamespace<ScheduleOwnerObjectRpc>,
  ): Layer.Layer<ScheduleOwnerNamespace> {
    return Layer.succeed(ScheduleOwnerNamespace)({ namespace });
  }
}

export const scheduleOwnerNamespaceFromEnv = Effect.fn("scheduleOwnerNamespaceFromEnv")(function* (
  env: unknown,
  binding: string,
): Effect.fn.Return<DurableObjectNamespace<ScheduleOwnerObjectRpc>, CloudflareBindingError> {
  if (!Predicate.isObjectKeyword(env)) {
    return yield* CloudflareBindingError.make({
      binding,
      message: "The Worker environment is not an object; no bindings are available.",
    });
  }
  const candidate = yield* Effect.try({
    try: () => {
      const value: unknown = Reflect.get(env, binding);
      if (!Predicate.isObjectKeyword(value)) return undefined;
      return typeof Reflect.get(value, "idFromName") === "function" &&
        typeof Reflect.get(value, "get") === "function"
        ? value
        : undefined;
    },
    catch: () =>
      CloudflareBindingError.make({
        binding,
        message: `env.${binding} could not be inspected as a DurableObjectNamespace binding.`,
      }),
  });
  if (candidate === undefined) {
    return yield* CloudflareBindingError.make({
      binding,
      message: `env.${binding} is not a Schedule Owner DurableObjectNamespace binding.`,
    });
  }
  return candidate as unknown as DurableObjectNamespace<ScheduleOwnerObjectRpc>;
});

export const scheduleOwnerNamespaceLayer = (
  env: unknown,
  binding: string,
): Layer.Layer<ScheduleOwnerNamespace, CloudflareBindingError> =>
  Layer.effect(
    ScheduleOwnerNamespace,
    Effect.map(scheduleOwnerNamespaceFromEnv(env, binding), (namespace) => ({ namespace })),
  );

const passthroughAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: { id: agentId, input: PersistedJson },
});

const requestOwner = (request: ScheduleOwnerRequest): ScheduleOwner => request.scope.owner;

export interface CloudflareSchedulingClientService {
  readonly create: <InputSchema extends Schema.Top>(
    agent: DurableSubmitAgent<InputSchema>,
    input: InputSchema["Type"],
    options: ScheduleCreateOptions,
  ) => Effect.Effect<
    typeof ScheduleSnapshot.Type,
    ScheduleOwnerFailure | ScheduleOwnerProtocolError,
    InputSchema["EncodingServices"]
  >;
  readonly update: <InputSchema extends Schema.Top>(
    agent: DurableSubmitAgent<InputSchema>,
    input: InputSchema["Type"],
    options: ScheduleUpdateOptions,
  ) => Effect.Effect<
    typeof ScheduleSnapshot.Type,
    ScheduleOwnerFailure | ScheduleOwnerProtocolError,
    InputSchema["EncodingServices"]
  >;
  readonly get: (
    scope: ScheduleScope,
    scheduleId: ScheduleId,
  ) => Effect.Effect<
    typeof ScheduleSnapshot.Type,
    ScheduleOwnerFailure | ScheduleOwnerProtocolError
  >;
  readonly list: (
    scope: ScheduleScope,
    options?: { readonly after?: ScheduleId; readonly limit?: number },
  ) => Effect.Effect<ScheduleSnapshotPage, ScheduleOwnerFailure | ScheduleOwnerProtocolError>;
  readonly pause: CloudflareScheduleControl;
  readonly resume: CloudflareScheduleControl;
  readonly cancel: CloudflareScheduleControl;
}

type CloudflareScheduleControl = (
  scope: ScheduleScope,
  scheduleId: ScheduleId,
  expectedRevision: number,
) => Effect.Effect<typeof ScheduleSnapshot.Type, ScheduleOwnerFailure | ScheduleOwnerProtocolError>;

export class CloudflareSchedulingClient extends Context.Service<
  CloudflareSchedulingClient,
  CloudflareSchedulingClientService
>()("@effect-agent/platform-cloudflare/CloudflareSchedulingClient") {
  static readonly layer: Layer.Layer<CloudflareSchedulingClient, never, ScheduleOwnerNamespace> =
    Layer.effect(
      CloudflareSchedulingClient,
      Effect.gen(function* () {
        const { namespace } = yield* ScheduleOwnerNamespace;

        const call = Effect.fn("CloudflareSchedulingClient.call")(function* (
          owner: ScheduleOwner,
          request: ScheduleOwnerRequest,
        ): Effect.fn.Return<
          ScheduleOwnerResponse,
          ScheduleOwnerFailure | ScheduleOwnerProtocolError
        > {
          const encoded = yield* encodeScheduleOwnerRequest(request).pipe(
            Effect.mapError(() =>
              ScheduleOwnerProtocolError.make({
                message: "The Schedule request could not be encoded",
              }),
            ),
          );
          const raw = yield* Effect.tryPromise({
            try: () =>
              namespace.get(namespace.idFromName(scheduleOwnerKey(owner))).schedule(encoded),
            catch: () =>
              ScheduleStorageError.make({
                operation: "call Schedule Owner",
                reason: "unavailable",
              }),
          });
          const response = yield* decodeScheduleOwnerResponse(raw).pipe(
            Effect.mapError(() =>
              ScheduleOwnerProtocolError.make({
                message: "The Schedule response could not be decoded",
              }),
            ),
          );
          return response._tag === "Failed" ? yield* response.failure : response;
        });

        const encodeInput = Effect.fn("CloudflareSchedulingClient.encodeInput")(function* <
          InputSchema extends Schema.Top,
        >(
          agent: DurableSubmitAgent<InputSchema>,
          input: InputSchema["Type"],
        ): Effect.fn.Return<
          PersistedJson,
          ScheduleValidationError,
          InputSchema["EncodingServices"]
        > {
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

        const create: CloudflareSchedulingClientService["create"] = (agent, input, options) =>
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
              : yield* ScheduleOwnerProtocolError.make({
                  message: "Schedule create returned a page response",
                });
          });

        const update: CloudflareSchedulingClientService["update"] = (agent, input, options) =>
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
              : yield* ScheduleOwnerProtocolError.make({
                  message: "Schedule update returned a page response",
                });
          });

        const get: CloudflareSchedulingClientService["get"] = (scope, scheduleId) =>
          Effect.gen(function* () {
            const response = yield* call(scope.owner, {
              _tag: "Get",
              schemaVersion: 1,
              scope,
              scheduleId,
            });
            return response._tag === "Snapshot"
              ? response.value
              : yield* ScheduleOwnerProtocolError.make({
                  message: "Schedule get returned a page response",
                });
          });

        const list: CloudflareSchedulingClientService["list"] = (scope, options = {}) =>
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
              : yield* ScheduleOwnerProtocolError.make({
                  message: "Schedule list returned a snapshot response",
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
              : yield* ScheduleOwnerProtocolError.make({
                  message: `Schedule ${operation} returned a page response`,
                });
          });

        return CloudflareSchedulingClient.of({
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

export interface ScheduleOwnerObjectSourceContext {
  readonly ctx: DurableObjectState;
  readonly env: unknown;
  readonly owner: ScheduleOwner;
}

export interface ScheduleOwnerObjectOptions {
  readonly conversationNamespaceBinding: string;
  readonly authorizer:
    | ScheduleAuthorizer["Service"]
    | ((context: ScheduleOwnerObjectSourceContext) => ScheduleAuthorizer["Service"]);
  readonly limits?: SchedulingLimits | undefined;
  readonly failpoint?:
    | ((context: ScheduleOwnerObjectSourceContext) => {
        readonly hit: (point: string) => Effect.Effect<void, ScheduleFailpointError>;
      })
    | undefined;
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
                  : transaction
                      .scheduleAlarm({
                        id: SCHEDULE_ALARM_ID,
                        tag: SCHEDULE_ALARM_TAG,
                        runAt: DateTime.makeUnsafe(
                          Math.max(replacement.deadlineAtMillis, nowMillis + 1),
                        ),
                        payload: {
                          schemaVersion: 1,
                          generation: replacement.generation,
                        },
                      })
                      .pipe(Effect.mapError(alarmStorageError("schedule Schedule Owner alarm"))),
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

const admissionLayer: Layer.Layer<ScheduledInputAdmission, never, CloudflareConversationClient> =
  Layer.effect(
    ScheduledInputAdmission,
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      const submit = (envelope: ScheduledEnvelope) =>
        client
          .submit(passthroughAgent(envelope.agentId), envelope.input, {
            conversationId: envelope.conversationId,
            principal: envelope.deliveryPrincipal,
            idempotencyKey: envelope.admissionKey,
            definitions: envelope.definitions,
          })
          .pipe(
            Effect.catchTags({
              AdmissionConflict: () =>
                ScheduleStorageError.make({ operation: "scheduled admission", reason: "corrupt" }),
              AdmissionLimitExceeded: () => ScheduledInputRetryable.make({ reason: "capacity" }),
              ConversationClientError: (error: ConversationClientError) =>
                ScheduledInputRetryable.make({
                  reason: error.overloaded === true ? "capacity" : "transport",
                }),
              HostProtocolError: () => ScheduledInputRetryable.make({ reason: "ambiguous" }),
              LedgerError: () => ScheduledInputRetryable.make({ reason: "storage" }),
              ConversationStoreError: () => ScheduledInputRetryable.make({ reason: "storage" }),
              DurableAlarmError: () => ScheduledInputRetryable.make({ reason: "storage" }),
              AgentInputError: () =>
                ScheduleStorageError.make({ operation: "scheduled admission", reason: "corrupt" }),
              DigestError: () =>
                ScheduleStorageError.make({ operation: "scheduled admission", reason: "corrupt" }),
              ConversationNotMaterialized: () =>
                ScheduledInputRetryable.make({ reason: "storage" }),
              AppendConflict: () => ScheduledInputRetryable.make({ reason: "ambiguous" }),
              FenceRejected: () => ScheduledInputRetryable.make({ reason: "ambiguous" }),
              DurableRuntimeFailpointError: () =>
                ScheduledInputRetryable.make({ reason: "ambiguous" }),
            }),
          );
      return ScheduledInputAdmission.of({ submit });
    }),
  );

type ScheduleRuntimeServices =
  | Scheduling
  | DoScheduleAlarmControl
  | ScheduleOwnerIdentity
  | DurableObjectAlarm.DurableObjectAlarm;

const makeScheduleRuntimeLayer = (
  options: ScheduleOwnerObjectOptions,
): Layer.Layer<
  ScheduleRuntimeServices,
  | ScheduleStorageError
  | ScheduleOwnerProtocolError
  | CloudflareBindingError
  | ScheduleValidationError,
  EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
> => {
  const limits = options.limits ?? defaultSchedulingLimits;
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
  const namespaceLayer = Layer.effect(
    ConversationObjectNamespace,
    Effect.gen(function* () {
      const env = yield* WorkerEnvironment;
      const namespace = yield* conversationNamespaceFromEnv(
        env,
        options.conversationNamespaceBinding,
      );
      return ConversationObjectNamespace.of({ namespace });
    }),
  );
  const authorizerLayer = Layer.effect(
    ScheduleAuthorizer,
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const env = yield* WorkerEnvironment;
      const { owner } = yield* ScheduleOwnerIdentity;
      const source = { ctx: state.raw, env, owner };
      return typeof options.authorizer === "function"
        ? options.authorizer(source)
        : options.authorizer;
    }),
  );
  const failpointLayer = Layer.effect(
    ScheduleFailpoint,
    Effect.gen(function* () {
      if (options.failpoint === undefined) return { hit: () => Effect.void };
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const env = yield* WorkerEnvironment;
      const { owner } = yield* ScheduleOwnerIdentity;
      return options.failpoint({ ctx: state.raw, env, owner });
    }),
  );
  const wakeLayer = Layer.succeed(ScheduleWake)({ notify: Effect.void, await: Effect.never });
  const base = Layer.mergeAll(
    ownerLayer,
    sqlLayer,
    namespaceLayer,
    DurableObjectAlarm.DurableObjectAlarm.layer,
    BrowserCrypto.layer,
  );
  const withTransaction = transactionLayer.pipe(Layer.provideMerge(base));
  const withFailpoint = failpointLayer.pipe(Layer.provideMerge(withTransaction));
  const withStore = scheduleStoreLayer.pipe(Layer.provideMerge(withFailpoint));
  const withAuthorizer = authorizerLayer.pipe(Layer.provideMerge(withStore));
  const withWake = wakeLayer.pipe(Layer.provideMerge(withAuthorizer));
  const withClient = CloudflareConversationClient.layer.pipe(Layer.provideMerge(withWake));
  const withAdmission = admissionLayer.pipe(Layer.provideMerge(withClient));
  return Scheduling.layer(limits).pipe(Layer.provideMerge(withAdmission));
};

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

const scheduleAlarmHandler = (limits: SchedulingLimits) =>
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
        const scheduling = yield* Scheduling;
        const alarmControl = yield* DoScheduleAlarmControl;
        const { owner } = yield* ScheduleOwnerIdentity;
        const nowMillis = yield* Clock.currentTimeMillis;
        yield* alarmControl.prearm(nowMillis + limits.recoveryPollMillis);
        yield* scheduling.runDue(owner);
        yield* alarmControl.reconcile(owner);
      }),
    { mode: "ordered" },
  ).pipe(Effect.asVoid);

export interface ScheduleOwnerObjectInstance extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<Record<never, never>, ScheduleRuntimeServices>
> {
  schedule(encoded: unknown): Promise<unknown>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

export interface ScheduleOwnerObjectClass {
  new (ctx: DurableObjectState, env: Cloudflare.Env): ScheduleOwnerObjectInstance;
}

/** Build the one SQLite-backed Schedule Owner Durable Object class per management scope. */
export const makeScheduleOwnerObjectClass = (
  options: ScheduleOwnerObjectOptions,
): ScheduleOwnerObjectClass => {
  const limits = options.limits ?? defaultSchedulingLimits;
  const application = makeScheduleRuntimeLayer(options);
  const runtime: Layer.Layer<
    ScheduleRuntimeServices,
    | ScheduleStorageError
    | ScheduleOwnerProtocolError
    | CloudflareBindingError
    | ScheduleValidationError,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
  > = Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const scope: Scope.Scope = yield* Effect.scope;
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

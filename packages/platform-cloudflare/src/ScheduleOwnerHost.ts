import {
  DoScheduleAlarmControl,
  DoScheduleTransaction,
  scheduleStoreLayer,
} from "@effect-agent/storage-cloudflare/do-schedule-store";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Clock, Context, DateTime, Effect, Layer, Schema } from "effect";
import { type DurableSubmitAgent } from "effect-agent/durable-agent-runtime";
import { AgentId } from "effect-agent/identifiers";
import { DefinitionDigests, PersistedJson } from "effect-agent/records";
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
} from "effect-agent/schedule";
import { scheduleOwnerKey } from "effect-agent/schedule-transition";
import {
  Scheduling,
  ScheduleDriver,
  type ScheduleManagementFailure,
  ScheduleWakeNoop,
} from "effect-agent/scheduling";
import { AdmissionFence } from "effect-agent/submission-ledger";

import { CloudflareAlarms, processDue } from "./CloudflareAlarms.ts";
import { DurableObjectContext } from "./CloudflareHostBindings.ts";
import type { ThreadObjectNamespace } from "./CloudflareHostBindings.ts";
import { RpcStrategy } from "./CloudflareRpc.ts";
import { CloudflareThreadClient } from "./CloudflareThreadClientHost.ts";
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
  admissionGroup: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(256))),
  admissionFence: Schema.optionalKey(AdmissionFence),
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

const ScheduleRecoverRequest = Schema.TaggedStruct("Recover", {
  schemaVersion: Schema.Literal(1),
  scope: ScheduleScopeSchema,
  scheduleId: ScheduleId,
  expectedRevision: Schema.Int.check(Schema.isGreaterThan(0)),
  expectedGeneration: Schema.Natural,
});

const ScheduleOwnerRequest = Schema.Union([
  ScheduleRecoverRequest,
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
      const rpcStrategy = yield* RpcStrategy;

      const call = Effect.fn("CloudflareSchedulingClient.call")(function* (
        owner: ScheduleOwner,
        request: ScheduleOwnerRequest,
      ): Effect.fn.Return<ScheduleOwnerResponse, ScheduleManagementFailure> {
        const encoded = yield* encodeScheduleOwnerRequest(request).pipe(
          Effect.mapError(() =>
            ScheduleStorageError.make({ operation: "Schedule Owner protocol", reason: "corrupt" }),
          ),
        );

        const unavailable = (cause: unknown) => {
          const error = ScheduleStorageError.make({
            operation: "call Schedule Owner",
            reason: "unavailable",
          });

          error.cause = cause;

          return error;
        };

        const address = scheduleOwnerKey(owner);

        const target = yield* rpcStrategy
          .get(namespace, address, () => namespace.get(namespace.idFromName(address)))
          .pipe(Effect.mapError(unavailable));

        const raw = yield* Effect.tryPromise({
          try: () => target.schedule(encoded),
          catch: unavailable,
        }).pipe(Effect.tapCause(() => rpcStrategy.invalidate(target)));

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
        recover: (scope, scheduleId, expectedRevision, expectedGeneration) =>
          Effect.gen(function* () {
            const response = yield* call(scope.owner, {
              _tag: "Recover",
              schemaVersion: 1,
              scope,
              scheduleId,
              expectedRevision,
              expectedGeneration,
            });

            return response._tag === "Snapshot"
              ? response.value
              : yield* ScheduleStorageError.make({
                  operation: "Schedule Owner protocol",
                  reason: "corrupt",
                });
          }),
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

  const tuple = yield* Schema.decodeEffect(
    Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String])),
  )(name).pipe(
    Effect.mapError(() =>
      ScheduleOwnerProtocolError.make({ message: "Schedule Owner object name is malformed" }),
    ),
  );

  return yield* Schema.decodeEffect(ScheduleOwner)({
    tenantId: tuple[0],
    ownerId: tuple[1],
  }).pipe(
    Effect.mapError(() =>
      ScheduleOwnerProtocolError.make({ message: "Schedule Owner object identity is invalid" }),
    ),
  );
});

const alarmStorageError =
  (operation: string) => (error: { readonly reason: "storage" | "invalid" }) =>
    ScheduleStorageError.make({
      operation,
      reason: error.reason === "storage" ? "unavailable" : "corrupt",
    });

const transactionLayer: Layer.Layer<DoScheduleTransaction, never, CloudflareAlarms> = Layer.effect(
  DoScheduleTransaction,
  Effect.gen(function* () {
    const alarms = yield* CloudflareAlarms;

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
              Effect.catchTag("CloudflareAlarmError", () =>
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

export type Services =
  | Scheduling
  | ScheduleDriver
  | DoScheduleAlarmControl
  | ScheduleOwnerIdentity
  | CloudflareAlarms;

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
          ...(request.admissionGroup === undefined
            ? {}
            : { admissionGroup: request.admissionGroup }),
          ...(request.admissionFence === undefined
            ? {}
            : { admissionFence: request.admissionFence }),
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
          ...(request.admissionGroup === undefined
            ? {}
            : { admissionGroup: request.admissionGroup }),
          ...(request.admissionFence === undefined
            ? {}
            : { admissionFence: request.admissionFence }),
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
      case "Recover":
        return {
          _tag: "Snapshot" as const,
          value: yield* scheduling.recover(
            request.scope,
            request.scheduleId,
            request.expectedRevision,
            request.expectedGeneration,
          ),
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
  processDue(
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

/** Compose owner services over the host's storage and logical alarm transaction adapter. */
export const makeRuntime = <E, R>(
  host: Layer.Layer<ScheduleAuthorizer | ThreadObjectNamespace, E, R>,
  limits: SchedulingLimits = defaultSchedulingLimits,
) => {
  const ownerLayer = Layer.effect(
    ScheduleOwnerIdentity,
    Effect.gen(function* () {
      const state = yield* DurableObjectContext;

      return ScheduleOwnerIdentity.of({ owner: yield* decodeOwnerName(state.ctx.id.name) });
    }),
  );

  const sqlLayer = Layer.unwrap(
    Effect.map(DurableObjectContext, (state) => SqliteClient.layer({ storage: state.ctx.storage })),
  );

  const application = Layer.mergeAll(
    Scheduling.layer(limits),
    ScheduleDriver.layer(limits),
    Layer.effect(CloudflareAlarms)(CloudflareAlarms),
  ).pipe(
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
    Layer.provide(host),
    Layer.provideMerge(ownerLayer),
  );

  return application;
};

export const rpc = { schedule: handleScheduleRequest };
export const alarm = scheduleAlarmHandler;

import { type AgentId, ConversationId } from "@effect-agent/core";
import {
  Cause,
  Context,
  Crypto,
  DateTime,
  Effect,
  Encoding,
  Layer,
  Option,
  Result,
  Schema,
  Semaphore,
} from "effect";

import { digestJson } from "./digest.ts";
import type { DurableSubmitAgent } from "./durable-runtime.ts";
import { IdempotencyKey, type Principal } from "./ledger.ts";
import { DefinitionDigests, type Digest, PersistedJson } from "./records.ts";
import {
  normalizeScheduleTiming,
  scheduleDueOccurrence,
  scheduleInitialCursor,
  scheduleNextAfter,
  scheduleResumeCursor,
} from "./schedule-time.ts";
import {
  defaultSchedulingLimits,
  type ScheduleAuthorizationError,
  ScheduleAuthorizer,
  ScheduleConflict,
  type ScheduleConfiguration,
  ScheduleDestination as ScheduleDestinationSchema,
  type ScheduleDestination,
  ScheduleFailpoint,
  type ScheduleId,
  type ScheduleDueCursor,
  ScheduleInstant,
  type ScheduleKey,
  SchedulingLimits,
  ScheduleNotFound,
  type ScheduleOwner,
  type ScheduleRecord,
  type ScheduleRefusal,
  type ScheduleRetryReason,
  type ScheduleScope,
  type ScheduleSnapshot,
  type ScheduleSnapshotPage,
  ScheduleStorageError,
  ScheduleStore,
  type ScheduleStoreFailure,
  ScheduleTimingRequest as ScheduleTimingRequestSchema,
  type ScheduleTimingRequest,
  ScheduleValidationError,
  ScheduledInputAdmission,
  ScheduleWake,
} from "./schedule.ts";

export interface ScheduleCreateOptions {
  readonly scope: ScheduleScope;
  readonly scheduleId: ScheduleId;
  readonly timing: ScheduleTimingRequest;
  readonly destination: ScheduleDestination;
  readonly deliveryPrincipal: Principal;
  readonly definitions: DefinitionDigests;
}

export interface ScheduleUpdateOptions extends ScheduleCreateOptions {
  readonly expectedRevision: number;
}

export interface ScheduleListOptions {
  readonly after?: ScheduleId | undefined;
  readonly limit?: number | undefined;
}

export type ScheduleManagementFailure =
  | ScheduleValidationError
  | ScheduleAuthorizationError
  | ScheduleStoreFailure;

export type ScheduleProcessFailure =
  | ScheduleValidationError
  | ScheduleStoreFailure
  | ScheduleStorageError;

const equivalentDefinitions = Schema.toEquivalence(DefinitionDigests);
const equivalentInput = Schema.toEquivalence(PersistedJson);

const keyOf = (scope: ScheduleScope, scheduleId: ScheduleId): ScheduleKey => ({
  owner: scope.owner,
  scheduleId,
});

const notFound = (key: ScheduleKey) => ScheduleNotFound.make({ key });

const asSnapshot = (record: ScheduleRecord, observedAtMillis: number): ScheduleSnapshot => {
  const intendedAtMillis = record.pending?.envelope.intendedAtMillis ?? record.nextAtMillis;
  return {
    owner: record.owner,
    scheduleId: record.scheduleId,
    createdAtMillis: record.createdAtMillis,
    updatedAtMillis: record.updatedAtMillis,
    configurationRevision: record.configurationRevision,
    configuration: {
      timing: record.configuration.timing,
      destination: record.configuration.destination,
      deliveryPrincipal: record.configuration.deliveryPrincipal,
      agentId: record.configuration.agentId,
    },
    state: record.state,
    nextAtMillis: record.nextAtMillis,
    pending:
      record.pending === null
        ? null
        : {
            intendedAtMillis: record.pending.envelope.intendedAtMillis,
            preparedAtMillis: record.pending.envelope.preparedAtMillis,
            occurrenceId: record.pending.envelope.occurrenceId,
            retry: record.pending.retry,
          },
    lastReceipt: record.lastReceipt,
    lastRefusal: record.lastRefusal,
    lastSkippedRange: record.lastSkippedRange,
    observedAtMillis,
    pendingAgeMillis:
      record.pending === null
        ? null
        : Math.max(0, observedAtMillis - record.pending.envelope.preparedAtMillis),
    latenessMillis:
      intendedAtMillis === null ? 0 : Math.max(0, observedAtMillis - intendedAtMillis),
  };
};

const inputByteLength = (input: PersistedJson): number =>
  Encoding.encodeHex(JSON.stringify(input)).length / 2;

const currentMillis = DateTime.now.pipe(Effect.map(DateTime.toEpochMillis));

const validateRequestedFields = Effect.fn("Scheduling.validateRequestedFields")(function* (
  options: ScheduleCreateOptions,
) {
  yield* Schema.encodeEffect(ScheduleTimingRequestSchema)(options.timing).pipe(
    Effect.mapError(() => ScheduleValidationError.make({ message: "Invalid timing request" })),
  );
  yield* Schema.encodeEffect(ScheduleDestinationSchema)(options.destination).pipe(
    Effect.mapError(() => ScheduleValidationError.make({ message: "Invalid destination" })),
  );
  yield* Schema.encodeEffect(DefinitionDigests)(options.definitions).pipe(
    Effect.mapError(() => ScheduleValidationError.make({ message: "Invalid definition digests" })),
  );
});

const resolveInput = Effect.fn("Scheduling.resolveInput")(function* <
  InputSchema extends Schema.Top,
>(
  agent: DurableSubmitAgent<InputSchema>,
  input: InputSchema["Type"],
  maximumBytes: number,
): Effect.fn.Return<
  { readonly payload: PersistedJson; readonly digest: Digest },
  ScheduleValidationError,
  InputSchema["EncodingServices"] | Crypto.Crypto
> {
  const encoded = yield* Schema.encodeEffect(agent.definition.input)(input).pipe(
    Effect.mapError(() =>
      ScheduleValidationError.make({ message: "Unable to encode Agent input" }),
    ),
  );
  const payload = yield* Schema.decodeUnknownEffect(PersistedJson)(encoded).pipe(
    Effect.mapError(() =>
      ScheduleValidationError.make({
        message: "Agent input does not satisfy the canonical persistence bounds",
      }),
    ),
  );
  if (inputByteLength(payload) > maximumBytes) {
    return yield* ScheduleValidationError.make({
      message: `Encoded Agent input exceeds the configured ${maximumBytes} byte limit`,
    });
  }
  const digest = yield* digestJson(payload).pipe(
    Effect.mapError(() =>
      ScheduleValidationError.make({ message: "Unable to digest encoded Agent input" }),
    ),
  );
  return { payload, digest };
});

const makeManagement = (limits: SchedulingLimits) =>
  Effect.gen(function* () {
    const nowMillis = yield* currentMillis;
    yield* Schema.decodeUnknownEffect(ScheduleInstant)(nowMillis + limits.recoveryPollMillis).pipe(
      Effect.mapError(() =>
        ScheduleValidationError.make({
          message: "Scheduling recovery deadline exceeds the supported instant range",
        }),
      ),
    );
    const store = yield* ScheduleStore;
    const authorizer = yield* ScheduleAuthorizer;
    const wake = yield* ScheduleWake;
    const crypto = yield* Crypto.Crypto;

    const withCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>) =>
      Effect.provideService(effect, Crypto.Crypto, crypto);

    const fingerprint = Effect.fn("Scheduling.fingerprint")(function* (
      agentId: AgentId,
      inputDigest: Digest,
      options: ScheduleCreateOptions,
    ) {
      const timing = yield* Schema.encodeEffect(ScheduleTimingRequestSchema)(options.timing).pipe(
        Effect.mapError(() => ScheduleValidationError.make({ message: "Invalid timing request" })),
      );
      const destination = yield* Schema.encodeEffect(ScheduleDestinationSchema)(
        options.destination,
      ).pipe(
        Effect.mapError(() => ScheduleValidationError.make({ message: "Invalid destination" })),
      );
      const definitions = yield* Schema.encodeEffect(DefinitionDigests)(options.definitions).pipe(
        Effect.mapError(() =>
          ScheduleValidationError.make({ message: "Invalid definition digests" }),
        ),
      );
      return yield* withCrypto(
        digestJson({
          schemaVersion: 1,
          owner: options.scope.owner,
          scheduleId: options.scheduleId,
          registeringPrincipal: options.scope.principal,
          deliveryPrincipal: options.deliveryPrincipal,
          agentId,
          definitions,
          destination,
          timing,
          inputDigest,
        }),
      ).pipe(
        Effect.mapError(() =>
          ScheduleValidationError.make({ message: "Unable to digest Schedule registration" }),
        ),
      );
    });

    const configuration = (
      agentId: AgentId,
      payload: PersistedJson,
      inputDigest: Digest,
      options: ScheduleCreateOptions,
      timing: ScheduleConfiguration["timing"],
    ): ScheduleConfiguration => ({
      timing,
      destination: options.destination,
      deliveryPrincipal: options.deliveryPrincipal,
      agentId,
      definitions: options.definitions,
      input: payload,
      inputDigest,
    });

    const create = Effect.fn("Scheduling.create")(function* <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: ScheduleCreateOptions,
    ): Effect.fn.Return<
      ScheduleSnapshot,
      ScheduleManagementFailure,
      InputSchema["EncodingServices"]
    > {
      const nowMillis = yield* currentMillis;
      const resolvedInput = yield* resolveInput(agent, input, limits.maxInputBytes).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
      );
      const creationFingerprint = yield* fingerprint(
        agent.definition.id,
        resolvedInput.digest,
        options,
      );
      const key = keyOf(options.scope, options.scheduleId);

      yield* authorizer.manage({
        operation: "create",
        scope: options.scope,
        scheduleId: options.scheduleId,
      });
      const existing = yield* store.get(key);
      if (existing !== null) {
        if (existing.creationFingerprint !== creationFingerprint) {
          return yield* ScheduleConflict.make({ reason: "creation", key });
        }
        yield* authorizer.manage({
          operation: "create",
          scope: options.scope,
          scheduleId: options.scheduleId,
          configuration: existing.configuration,
        });
        return asSnapshot(existing, nowMillis);
      }

      const timing = yield* Effect.fromResult(
        normalizeScheduleTiming(options.timing, nowMillis, limits.minIntervalMillis),
      );
      const nextAtMillis = yield* Effect.fromResult(scheduleInitialCursor(timing, nowMillis));
      if (nextAtMillis === null) {
        return yield* ScheduleValidationError.make({ message: "Timing has no next occurrence" });
      }
      const nextConfiguration = configuration(
        agent.definition.id,
        resolvedInput.payload,
        resolvedInput.digest,
        options,
        timing,
      );
      yield* authorizer.manage({
        operation: "create",
        scope: options.scope,
        scheduleId: options.scheduleId,
        configuration: nextConfiguration,
      });
      const record: ScheduleRecord = {
        schemaVersion: 1,
        owner: options.scope.owner,
        scheduleId: options.scheduleId,
        creationFingerprint,
        createdBy: options.scope.principal,
        createdAtMillis: nowMillis,
        updatedAtMillis: nowMillis,
        configurationRevision: 1,
        version: 1,
        configuration: nextConfiguration,
        state: "active",
        nextAtMillis,
        pending: null,
        lastReceipt: null,
        lastRefusal: null,
        lastSkippedRange: null,
      };
      const inserted = yield* store.insert(record, limits.maxSchedulesPerOwner);
      yield* wake.notify;
      return asSnapshot(inserted, nowMillis);
    });

    const update = Effect.fn("Scheduling.update")(function* <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: ScheduleUpdateOptions,
    ): Effect.fn.Return<
      ScheduleSnapshot,
      ScheduleManagementFailure,
      InputSchema["EncodingServices"]
    > {
      const nowMillis = yield* currentMillis;
      const key = keyOf(options.scope, options.scheduleId);
      yield* authorizer.manage({
        operation: "update",
        scope: options.scope,
        scheduleId: options.scheduleId,
      });
      yield* validateRequestedFields(options);
      const existing = yield* store.get(key);
      if (existing === null) return yield* notFound(key);
      if (existing.configurationRevision !== options.expectedRevision) {
        return yield* ScheduleConflict.make({ reason: "revision", key });
      }
      const resolvedInput = yield* resolveInput(agent, input, limits.maxInputBytes).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
      );
      const timingRequest =
        options.timing._tag === "Interval" &&
        options.timing.anchorMillis === undefined &&
        existing.configuration.timing._tag === "Interval" &&
        existing.configuration.timing.everyMillis === options.timing.everyMillis
          ? { ...options.timing, anchorMillis: existing.configuration.timing.anchorMillis }
          : options.timing;
      const timing = yield* Effect.fromResult(
        normalizeScheduleTiming(timingRequest, nowMillis, limits.minIntervalMillis),
      );
      const nextAtMillis =
        timing._tag === "At"
          ? timing.atMillis
          : yield* Effect.fromResult(scheduleNextAfter(timing, nowMillis));
      if (nextAtMillis === null) {
        return yield* ScheduleValidationError.make({ message: "Timing has no next occurrence" });
      }
      const nextConfiguration = configuration(
        agent.definition.id,
        resolvedInput.payload,
        resolvedInput.digest,
        options,
        timing,
      );
      yield* authorizer.manage({
        operation: "update",
        scope: options.scope,
        scheduleId: options.scheduleId,
        configuration: nextConfiguration,
      });
      const changed = yield* store.change(
        key,
        {
          _tag: "Update",
          expectedRevision: options.expectedRevision,
          configuration: nextConfiguration,
          nextAtMillis,
          nowMillis,
        },
        limits.maxSchedulesPerOwner,
      );
      yield* wake.notify;
      return asSnapshot(changed, nowMillis);
    });

    const get = Effect.fn("Scheduling.get")(function* (
      scope: ScheduleScope,
      scheduleId: ScheduleId,
    ) {
      yield* authorizer.manage({ operation: "get", scope, scheduleId });
      const key = keyOf(scope, scheduleId);
      const record = yield* store.get(key);
      if (record === null) return yield* notFound(key);
      return asSnapshot(record, yield* currentMillis);
    });

    const list = Effect.fn("Scheduling.list")(function* (
      scope: ScheduleScope,
      options: ScheduleListOptions = {},
    ) {
      yield* authorizer.manage({ operation: "list", scope });
      const limit = options.limit ?? 50;
      if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
        return yield* ScheduleValidationError.make({
          message: "Schedule page limit must be an integer from 1 through 100",
        });
      }
      const page = yield* store.list({
        owner: scope.owner,
        ...(options.after === undefined ? {} : { after: options.after }),
        limit,
      });
      const nowMillis = yield* currentMillis;
      return { items: page.items.map((record) => asSnapshot(record, nowMillis)), next: page.next };
    });

    const control = Effect.fn("Scheduling.control")(function* (
      operation: "pause" | "resume" | "cancel",
      scope: ScheduleScope,
      scheduleId: ScheduleId,
      expectedRevision: number,
    ): Effect.fn.Return<ScheduleSnapshot, ScheduleManagementFailure> {
      yield* authorizer.manage({ operation, scope, scheduleId });
      const key = keyOf(scope, scheduleId);
      const attempt = (): Effect.Effect<ScheduleRecord, ScheduleManagementFailure> =>
        Effect.suspend(() =>
          Effect.gen(function* () {
            const record = yield* store.get(key);
            if (record === null) return yield* notFound(key);
            if (record.configurationRevision !== expectedRevision) {
              return yield* ScheduleConflict.make({ reason: "revision", key });
            }
            if (operation === "resume" && record.state === "active") return record;
            const nowMillis = yield* currentMillis;
            const nextAtMillis =
              operation === "resume"
                ? yield* Effect.fromResult(
                    scheduleResumeCursor(
                      record.configuration.timing,
                      record.nextAtMillis,
                      nowMillis,
                    ),
                  )
                : operation === "cancel"
                  ? null
                  : record.nextAtMillis;
            const skippedRange =
              operation === "resume" &&
              record.configuration.timing._tag !== "At" &&
              record.nextAtMillis !== null &&
              nextAtMillis !== null &&
              nextAtMillis > record.nextAtMillis
                ? { fromMillis: record.nextAtMillis, toMillis: nextAtMillis }
                : null;
            return yield* store
              .change(
                key,
                {
                  _tag: "Control",
                  expectedRevision,
                  expectedVersion: record.version,
                  action: operation,
                  nextAtMillis,
                  nowMillis,
                  skippedRange,
                },
                limits.maxSchedulesPerOwner,
              )
              .pipe(
                Effect.catchTag("ScheduleConflict", (error) =>
                  error.reason === "revision" ? attempt() : Effect.fail(error),
                ),
              );
          }),
        );
      const changed = yield* attempt();
      yield* wake.notify;
      return asSnapshot(changed, yield* currentMillis);
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
  });

const makeDriver = (limits: SchedulingLimits) =>
  Effect.gen(function* () {
    const store = yield* ScheduleStore;
    const authorizer = yield* ScheduleAuthorizer;
    const admission = yield* ScheduledInputAdmission;
    const failpoint = yield* ScheduleFailpoint;
    const crypto = yield* Crypto.Crypto;
    const admissionSemaphore = yield* Semaphore.make(limits.admissionConcurrency);
    const withCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>) =>
      Effect.provideService(effect, Crypto.Crypto, crypto);

    const retry = Effect.fn("Scheduling.retry")(function* (
      key: ScheduleKey,
      record: ScheduleRecord,
      reason: ScheduleRetryReason,
      nowMillis: number,
    ) {
      if (record.pending === null) return record;
      const current = record.pending.retry;
      const delay = Math.min(
        limits.retryBaseMillis * 2 ** Math.min(current.attempts, 52),
        limits.retryMaxMillis,
      );
      return yield* store.change(key, {
        _tag: "Retry",
        occurrenceId: record.pending.envelope.occurrenceId,
        retry: {
          attempts: current.attempts + 1,
          nextAttemptAtMillis: Math.min(nowMillis + delay, 8_640_000_000_000_000),
          lastAttemptAtMillis: nowMillis,
          lastFailure: reason,
        },
        nowMillis,
      });
    });

    const verifyPending = Effect.fn("Scheduling.verifyPending")(function* (
      key: ScheduleKey,
      record: ScheduleRecord,
    ) {
      const pending = record.pending;
      if (pending === null) return;
      const envelope = pending.envelope;
      const occurrenceId = yield* withCrypto(
        digestJson({
          schemaVersion: 1,
          owner: envelope.owner,
          scheduleId: envelope.scheduleId,
          configurationRevision: envelope.configurationRevision,
          intendedAtMillis: envelope.intendedAtMillis,
        }),
      ).pipe(Effect.mapError(() => corrupt("process")));
      const usesCurrentConfiguration =
        envelope.configurationRevision === record.configurationRevision;
      const expectedConversationId = usesCurrentConfiguration
        ? record.configuration.destination._tag === "ExistingConversation"
          ? record.configuration.destination.conversationId
          : Schema.decodeSync(ConversationId)(`schedule-conversation:${envelope.occurrenceId}`)
        : null;
      const expectedAdmissionKey = Schema.decodeSync(IdempotencyKey)(
        `schedule-occurrence:${envelope.occurrenceId}`,
      );
      const digest = yield* withCrypto(digestJson(envelope.input)).pipe(
        Effect.mapError(() => corrupt("process")),
      );
      if (
        envelope.schemaVersion !== 1 ||
        envelope.owner.tenantId !== key.owner.tenantId ||
        envelope.owner.ownerId !== key.owner.ownerId ||
        envelope.scheduleId !== key.scheduleId ||
        envelope.configurationRevision > record.configurationRevision ||
        envelope.occurrenceId !== occurrenceId ||
        (usesCurrentConfiguration &&
          (envelope.deliveryPrincipal !== record.configuration.deliveryPrincipal ||
            envelope.agentId !== record.configuration.agentId ||
            !equivalentDefinitions(envelope.definitions, record.configuration.definitions) ||
            !equivalentInput(envelope.input, record.configuration.input) ||
            envelope.inputDigest !== record.configuration.inputDigest)) ||
        envelope.inputDigest !== digest ||
        (expectedConversationId !== null && envelope.conversationId !== expectedConversationId) ||
        envelope.admissionKey !== expectedAdmissionKey
      ) {
        return yield* corrupt("process");
      }
    });

    const deliverPending = Effect.fn("Scheduling.deliverPending")(function* (
      key: ScheduleKey,
      record: ScheduleRecord,
      nowMillis: number,
    ) {
      if (record.pending === null || record.pending.retry.nextAttemptAtMillis > nowMillis) {
        return record;
      }
      yield* verifyPending(key, record);
      const envelope = record.pending.envelope;
      const outcome = yield* admissionSemaphore.withPermit(
        admission.submit(envelope).pipe(
          Effect.timeout(limits.admissionTimeoutMillis),
          Effect.tap(() => failpoint.hit("schedule:admission:after")),
          Effect.map((receipt) => ({ _tag: "Receipt" as const, receipt })),
          Effect.catchTag("ScheduledInputRefused", (error) =>
            Effect.succeed({ _tag: "Refused" as const, error }),
          ),
          Effect.catchTag("ScheduledInputRetryable", (error) =>
            Effect.succeed({ _tag: "Retry" as const, reason: error.reason }),
          ),
          Effect.catchTag("TimeoutError", () =>
            Effect.succeed({ _tag: "Retry" as const, reason: "timeout" as const }),
          ),
          Effect.catchTag("ScheduleStorageError", (error) =>
            error.reason === "unavailable"
              ? Effect.succeed({ _tag: "Retry" as const, reason: "storage" as const })
              : Effect.fail(error),
          ),
        ),
      );
      const completedAtMillis = yield* currentMillis;
      if (outcome._tag === "Receipt") {
        if (outcome.receipt.conversationId !== envelope.conversationId) {
          return yield* corrupt("admission");
        }
        return yield* store
          .change(key, {
            _tag: "Complete",
            occurrenceId: envelope.occurrenceId,
            receipt: outcome.receipt,
            nowMillis: completedAtMillis,
          })
          .pipe(
            Effect.catchTag("ScheduleStorageError", (error) =>
              error.reason === "unavailable"
                ? retry(key, record, "storage", completedAtMillis)
                : Effect.fail(error),
            ),
          );
      }
      if (outcome._tag === "Refused") {
        const refusal: ScheduleRefusal = {
          atMillis: completedAtMillis,
          intendedAtMillis: envelope.intendedAtMillis,
          occurrenceId: envelope.occurrenceId,
          phase: "admission",
          code: outcome.error.code,
        };
        return yield* store
          .change(key, {
            _tag: "Refuse",
            occurrenceId: envelope.occurrenceId,
            refusal,
            nowMillis: completedAtMillis,
          })
          .pipe(
            Effect.catchTag("ScheduleStorageError", (error) =>
              error.reason === "unavailable"
                ? retry(key, record, "storage", completedAtMillis)
                : Effect.fail(error),
            ),
          );
      }
      return yield* retry(key, record, outcome.reason, completedAtMillis);
    });

    const processRecord = Effect.fn("Scheduling.processRecord")(function* (
      key: ScheduleKey,
      initial: ScheduleRecord,
      nowMillis: number,
    ): Effect.fn.Return<ScheduleRecord, ScheduleProcessFailure> {
      if (initial.pending !== null) return yield* deliverPending(key, initial, nowMillis);
      if (
        initial.state !== "active" ||
        initial.nextAtMillis === null ||
        initial.nextAtMillis > nowMillis
      ) {
        return initial;
      }

      const currentInputDigest = yield* withCrypto(digestJson(initial.configuration.input)).pipe(
        Effect.mapError(() =>
          ScheduleStorageError.make({ operation: "prepare", reason: "unavailable" }),
        ),
      );
      if (currentInputDigest !== initial.configuration.inputDigest) {
        return yield* corrupt("prepare");
      }

      const due = yield* Effect.fromResult(
        scheduleDueOccurrence(initial.configuration.timing, initial.nextAtMillis, nowMillis),
      );
      if (due === null) return initial;
      const occurrenceId = yield* withCrypto(
        digestJson({
          schemaVersion: 1,
          owner: initial.owner,
          scheduleId: initial.scheduleId,
          configurationRevision: initial.configurationRevision,
          intendedAtMillis: due.intendedAtMillis,
        }),
      ).pipe(Effect.mapError(() => corrupt("prepare")));
      const occurrence = {
        key,
        configurationRevision: initial.configurationRevision,
        configuration: initial.configuration,
        intendedAtMillis: due.intendedAtMillis,
        occurrenceId,
      };
      const authorization = yield* authorizer.prepare(occurrence).pipe(Effect.result);
      if (Result.isFailure(authorization)) {
        if (authorization.failure._tag === "ScheduleStorageError") {
          return yield* authorization.failure;
        }
        return yield* store.change(key, {
          _tag: "DenyPreparation",
          expectedRevision: initial.configurationRevision,
          expectedCursor: initial.nextAtMillis,
          refusal: {
            atMillis: nowMillis,
            intendedAtMillis: due.intendedAtMillis,
            occurrenceId,
            phase: "preparation",
            code: authorization.failure.code,
          },
          nowMillis,
        });
      }
      const conversationId =
        initial.configuration.destination._tag === "ExistingConversation"
          ? initial.configuration.destination.conversationId
          : Schema.decodeSync(ConversationId)(`schedule-conversation:${occurrenceId}`);
      const admissionKey = Schema.decodeSync(IdempotencyKey)(`schedule-occurrence:${occurrenceId}`);
      const prepared = yield* store.change(key, {
        _tag: "Prepare",
        expectedRevision: initial.configurationRevision,
        expectedCursor: initial.nextAtMillis,
        envelope: {
          schemaVersion: 1,
          owner: initial.owner,
          scheduleId: initial.scheduleId,
          configurationRevision: initial.configurationRevision,
          intendedAtMillis: due.intendedAtMillis,
          preparedAtMillis: nowMillis,
          occurrenceId,
          conversationId,
          deliveryPrincipal: initial.configuration.deliveryPrincipal,
          agentId: initial.configuration.agentId,
          definitions: initial.configuration.definitions,
          input: initial.configuration.input,
          inputDigest: initial.configuration.inputDigest,
          admissionKey,
          authorization: authorization.success,
        },
        nextAtMillis: due.nextAtMillis,
        skippedRange: due.skippedRange,
        nowMillis,
      });
      return yield* deliverPending(key, prepared, nowMillis);
    });

    const process = Effect.fn("Scheduling.process")(function* (key: ScheduleKey) {
      const nowMillis = yield* currentMillis;
      const record = yield* store.get(key);
      if (record === null) return yield* notFound(key);
      const processed = yield* processRecord(key, record, nowMillis).pipe(
        Effect.catchTag("ScheduleConflict", () =>
          store
            .get(key)
            .pipe(
              Effect.flatMap((current) =>
                current === null
                  ? Effect.fail(notFound(key))
                  : processRecord(key, current, nowMillis),
              ),
            ),
        ),
      );
      return processed;
    });

    const runDue = Effect.fn("ScheduleDriver.runDue")(function* (owner?: ScheduleOwner) {
      const nowMillis = yield* currentMillis;
      let after: ScheduleDueCursor | undefined;
      let processed = 0;
      let failed = 0;
      while (true) {
        const due = yield* store.due(nowMillis, limits.dueBatchSize, owner, after);
        if (due.length === 0) break;
        yield* Effect.forEach(
          due,
          (key) =>
            process(key).pipe(
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
                        yield* Effect.logWarning("Schedule processing failed").pipe(
                          Effect.annotateLogs({
                            failureTag: Option.match(Cause.findErrorOption(cause), {
                              onNone: () => "Defect",
                              onSome: (error) => error._tag,
                            }),
                          }),
                        );
                      }),
              }),
            ),
          { concurrency: limits.admissionConcurrency },
        );
        after = due.at(-1);
        if (due.length < limits.dueBatchSize) break;
      }
      return { processed, failed };
    });

    return ScheduleDriver.of({ process, runDue });
  });

const corrupt = (operation: string): ScheduleStorageError =>
  ScheduleStorageError.make({ operation, reason: "corrupt" });

const validateLimits = (limits: SchedulingLimits) =>
  Schema.decodeUnknownEffect(SchedulingLimits)(limits).pipe(
    Effect.mapError((error) =>
      ScheduleValidationError.make({ message: `Invalid Scheduling limits: ${error.message}` }),
    ),
    Effect.filterOrFail(
      (validated) => validated.retryBaseMillis <= validated.retryMaxMillis,
      () =>
        ScheduleValidationError.make({
          message: "Scheduling retryBaseMillis must not exceed retryMaxMillis",
        }),
    ),
  );

export class Scheduling extends Context.Service<
  Scheduling,
  {
    readonly create: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: ScheduleCreateOptions,
    ) => Effect.Effect<
      ScheduleSnapshot,
      ScheduleManagementFailure,
      InputSchema["EncodingServices"]
    >;
    readonly update: <InputSchema extends Schema.Top>(
      agent: DurableSubmitAgent<InputSchema>,
      input: InputSchema["Type"],
      options: ScheduleUpdateOptions,
    ) => Effect.Effect<
      ScheduleSnapshot,
      ScheduleManagementFailure,
      InputSchema["EncodingServices"]
    >;
    readonly get: (
      scope: ScheduleScope,
      scheduleId: ScheduleId,
    ) => Effect.Effect<ScheduleSnapshot, ScheduleManagementFailure>;
    readonly list: (
      scope: ScheduleScope,
      options?: ScheduleListOptions,
    ) => Effect.Effect<ScheduleSnapshotPage, ScheduleManagementFailure>;
    readonly pause: (
      scope: ScheduleScope,
      scheduleId: ScheduleId,
      expectedRevision: number,
    ) => Effect.Effect<ScheduleSnapshot, ScheduleManagementFailure>;
    readonly resume: (
      scope: ScheduleScope,
      scheduleId: ScheduleId,
      expectedRevision: number,
    ) => Effect.Effect<ScheduleSnapshot, ScheduleManagementFailure>;
    readonly cancel: (
      scope: ScheduleScope,
      scheduleId: ScheduleId,
      expectedRevision: number,
    ) => Effect.Effect<ScheduleSnapshot, ScheduleManagementFailure>;
  }
>()("@effect-agent/session/Scheduling") {
  static layer(
    limits: SchedulingLimits = defaultSchedulingLimits,
  ): Layer.Layer<
    Scheduling,
    ScheduleValidationError,
    ScheduleStore | ScheduleAuthorizer | ScheduleWake | Crypto.Crypto
  > {
    return Layer.effect(Scheduling, validateLimits(limits).pipe(Effect.flatMap(makeManagement)));
  }
}

export const ScheduleWakeNoop: Layer.Layer<ScheduleWake> = Layer.succeed(ScheduleWake, {
  notify: Effect.void,
  await: Effect.never,
});

/** Privileged host capability. Never provide this service to management callers or model tools. */
export class ScheduleDriver extends Context.Service<
  ScheduleDriver,
  {
    readonly process: (key: ScheduleKey) => Effect.Effect<ScheduleRecord, ScheduleProcessFailure>;
    readonly runDue: (
      owner?: ScheduleOwner,
    ) => Effect.Effect<
      { readonly processed: number; readonly failed: number },
      ScheduleStorageError
    >;
  }
>()("@effect-agent/session/ScheduleDriver") {
  static layer(
    limits: SchedulingLimits = defaultSchedulingLimits,
  ): Layer.Layer<
    ScheduleDriver,
    ScheduleValidationError,
    ScheduleStore | ScheduleAuthorizer | ScheduledInputAdmission | Crypto.Crypto
  > {
    return Layer.effect(ScheduleDriver, validateLimits(limits).pipe(Effect.flatMap(makeDriver)));
  }
}

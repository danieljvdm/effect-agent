import { AgentId, ConversationId } from "@effect-agent/core";
import { Context, Effect, Schema } from "effect";

import { Receipt } from "./durable-runtime.ts";
import { IdempotencyKey, Principal } from "./ledger.ts";
import { DefinitionDigests, Digest, PersistedJson } from "./records.ts";

const Name = Schema.NonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[^\p{Surrogate}]*$/u),
);
/** Milliseconds since the Unix epoch, within DateTime's supported range. */
export const ScheduleInstant = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(8_640_000_000_000_000),
);
const Positive = Schema.Int.check(Schema.isGreaterThan(0));

export const ScheduleId = Name.pipe(Schema.brand("@effect-agent/session/ScheduleId"));
export type ScheduleId = typeof ScheduleId.Type;
export const ScheduleOwner = Schema.Struct({ tenantId: Name, ownerId: Name });
export type ScheduleOwner = typeof ScheduleOwner.Type;
export const ScheduleKey = Schema.Struct({ owner: ScheduleOwner, scheduleId: ScheduleId });
export type ScheduleKey = typeof ScheduleKey.Type;
export const ScheduleScope = Schema.Struct({ owner: ScheduleOwner, principal: Principal });
export type ScheduleScope = typeof ScheduleScope.Type;

const At = Schema.Struct({ _tag: Schema.Literal("At"), atMillis: ScheduleInstant });
const After = Schema.Struct({ _tag: Schema.Literal("After"), delayMillis: Positive });
const Interval = Schema.Struct({
  _tag: Schema.Literal("Interval"),
  everyMillis: Positive,
  anchorMillis: ScheduleInstant,
});
const Cron = Schema.Struct({
  _tag: Schema.Literal("Cron"),
  expression: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  timeZone: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
});
/** A relative delay is a request value, never a stored unresolved deadline. */
export const ScheduleTimingRequest = Schema.Union([
  At,
  After,
  Schema.Struct({
    _tag: Schema.Literal("Interval"),
    everyMillis: Positive,
    anchorMillis: Schema.optionalKey(ScheduleInstant),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Cron"),
    expression: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
    timeZone: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(128))),
  }),
]);
export type ScheduleTimingRequest = typeof ScheduleTimingRequest.Type;
export const ScheduleTiming = Schema.Union([At, Interval, Cron]);
export type ScheduleTiming = typeof ScheduleTiming.Type;
export const ScheduleDestination = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("ExistingConversation"), conversationId: ConversationId }),
  Schema.Struct({ _tag: Schema.Literal("FreshConversation") }),
]);
export type ScheduleDestination = typeof ScheduleDestination.Type;

export const ScheduleConfiguration = Schema.Struct({
  timing: ScheduleTiming,
  destination: ScheduleDestination,
  deliveryPrincipal: Principal,
  agentId: AgentId,
  definitions: DefinitionDigests,
  input: PersistedJson,
  inputDigest: Digest,
});
export type ScheduleConfiguration = typeof ScheduleConfiguration.Type;

/** Persist only bounded policy identifiers, never credentials or arbitrary diagnostics. */
export const ScheduleAuthorizationDecision = Schema.Struct({
  policyId: Name,
  decisionId: Name,
});
export type ScheduleAuthorizationDecision = typeof ScheduleAuthorizationDecision.Type;
export const ScheduledEnvelope = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  owner: ScheduleOwner,
  scheduleId: ScheduleId,
  configurationRevision: Positive,
  intendedAtMillis: ScheduleInstant,
  preparedAtMillis: ScheduleInstant,
  occurrenceId: Digest,
  conversationId: ConversationId,
  deliveryPrincipal: Principal,
  agentId: AgentId,
  definitions: DefinitionDigests,
  input: PersistedJson,
  inputDigest: Digest,
  admissionKey: IdempotencyKey,
  authorization: ScheduleAuthorizationDecision,
});
export type ScheduledEnvelope = typeof ScheduledEnvelope.Type;

export const ScheduleRetryReason = Schema.Literals([
  "transport",
  "capacity",
  "storage",
  "host-closed",
  "timeout",
  "ambiguous",
]);
export type ScheduleRetryReason = typeof ScheduleRetryReason.Type;
export const ScheduleRetry = Schema.Struct({
  attempts: Schema.Natural,
  nextAttemptAtMillis: ScheduleInstant,
  lastAttemptAtMillis: Schema.NullOr(ScheduleInstant),
  lastFailure: Schema.NullOr(ScheduleRetryReason),
});
export type ScheduleRetry = typeof ScheduleRetry.Type;
export const SchedulePending = Schema.Struct({ envelope: ScheduledEnvelope, retry: ScheduleRetry });
export type SchedulePending = typeof SchedulePending.Type;
export const ScheduleRefusal = Schema.Struct({
  atMillis: ScheduleInstant,
  intendedAtMillis: ScheduleInstant,
  occurrenceId: Schema.NullOr(Digest),
  phase: Schema.Literals(["preparation", "admission"]),
  code: Name,
});
export type ScheduleRefusal = typeof ScheduleRefusal.Type;
/** Half-open interval [fromMillis, toMillis); the upper bound is not skipped. */
export const ScheduleSkippedRange = Schema.Struct({
  fromMillis: ScheduleInstant,
  toMillis: ScheduleInstant,
});
export type ScheduleSkippedRange = typeof ScheduleSkippedRange.Type;
export const ScheduleRecord = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  owner: ScheduleOwner,
  scheduleId: ScheduleId,
  creationFingerprint: Digest,
  createdBy: Principal,
  createdAtMillis: ScheduleInstant,
  updatedAtMillis: ScheduleInstant,
  configurationRevision: Positive,
  version: Positive,
  configuration: ScheduleConfiguration,
  state: Schema.Literals(["active", "paused", "cancelled"]),
  nextAtMillis: Schema.NullOr(ScheduleInstant),
  pending: Schema.NullOr(SchedulePending),
  lastReceipt: Schema.NullOr(
    Schema.Struct({
      atMillis: ScheduleInstant,
      intendedAtMillis: ScheduleInstant,
      occurrenceId: Digest,
      receipt: Receipt,
    }),
  ),
  lastRefusal: Schema.NullOr(ScheduleRefusal),
  lastSkippedRange: Schema.NullOr(ScheduleSkippedRange),
});
export type ScheduleRecord = typeof ScheduleRecord.Type;

export const ScheduleSnapshot = Schema.Struct({
  owner: ScheduleOwner,
  scheduleId: ScheduleId,
  createdAtMillis: ScheduleInstant,
  updatedAtMillis: ScheduleInstant,
  configurationRevision: Positive,
  configuration: Schema.Struct({
    timing: ScheduleTiming,
    destination: ScheduleDestination,
    deliveryPrincipal: Principal,
    agentId: AgentId,
  }),
  state: Schema.Literals(["active", "paused", "cancelled"]),
  nextAtMillis: Schema.NullOr(ScheduleInstant),
  pending: Schema.NullOr(
    Schema.Struct({
      intendedAtMillis: ScheduleInstant,
      preparedAtMillis: ScheduleInstant,
      occurrenceId: Digest,
      retry: ScheduleRetry,
    }),
  ),
  lastReceipt: Schema.NullOr(
    Schema.Struct({
      atMillis: ScheduleInstant,
      intendedAtMillis: ScheduleInstant,
      occurrenceId: Digest,
      receipt: Receipt,
    }),
  ),
  lastRefusal: Schema.NullOr(ScheduleRefusal),
  lastSkippedRange: Schema.NullOr(ScheduleSkippedRange),
  observedAtMillis: ScheduleInstant,
  pendingAgeMillis: Schema.NullOr(Schema.Natural),
  latenessMillis: Schema.Natural,
});
export type ScheduleSnapshot = typeof ScheduleSnapshot.Type;
export const ScheduleSnapshotPage = Schema.Struct({
  items: Schema.Array(ScheduleSnapshot).check(Schema.isMaxLength(100)),
  next: Schema.NullOr(ScheduleId),
});
export type ScheduleSnapshotPage = typeof ScheduleSnapshotPage.Type;

export class ScheduleValidationError extends Schema.TaggedError<ScheduleValidationError>()(
  "ScheduleValidationError",
  { message: Schema.String },
) {}
export class ScheduleConflict extends Schema.TaggedError<ScheduleConflict>()("ScheduleConflict", {
  reason: Schema.Literals(["creation", "revision", "cancelled"]),
  key: ScheduleKey,
}) {}
export class ScheduleNotFound extends Schema.TaggedError<ScheduleNotFound>()("ScheduleNotFound", {
  key: ScheduleKey,
}) {}
export class ScheduleAuthorizationError extends Schema.TaggedError<ScheduleAuthorizationError>()(
  "ScheduleAuthorizationError",
  { code: Name },
) {}
export class ScheduleCapacityError extends Schema.TaggedError<ScheduleCapacityError>()(
  "ScheduleCapacityError",
  { limit: Positive },
) {}
export class ScheduleStorageError extends Schema.TaggedError<ScheduleStorageError>()(
  "ScheduleStorageError",
  {
    operation: Schema.String,
    reason: Schema.Literals(["unavailable", "corrupt"]),
  },
) {}
/** Only when the unchanged envelope was not admitted and cannot succeed, including on replay. */
export class ScheduledInputRefused extends Schema.TaggedError<ScheduledInputRefused>()(
  "ScheduledInputRefused",
  { code: Name },
) {}
/** Includes lost replies: admission may already have committed. */
export class ScheduledInputRetryable extends Schema.TaggedError<ScheduledInputRetryable>()(
  "ScheduledInputRetryable",
  { reason: ScheduleRetryReason },
) {}
export type ScheduledInputFailure =
  | ScheduledInputRefused
  | ScheduledInputRetryable
  | ScheduleStorageError;
export class ScheduledInputAdmission extends Context.Service<
  ScheduledInputAdmission,
  {
    readonly submit: (envelope: ScheduledEnvelope) => Effect.Effect<Receipt, ScheduledInputFailure>;
  }
>()("@effect-agent/session/ScheduledInputAdmission") {}

export type ScheduleManagementOperation =
  | "create"
  | "get"
  | "list"
  | "update"
  | "pause"
  | "resume"
  | "cancel";
export interface ScheduleManagementAuthorization {
  readonly operation: ScheduleManagementOperation;
  readonly scope: ScheduleScope;
  readonly scheduleId?: ScheduleId;
  readonly configuration?: ScheduleConfiguration;
}
export interface ScheduleOccurrenceAuthorization {
  readonly key: ScheduleKey;
  readonly configurationRevision: number;
  readonly configuration: ScheduleConfiguration;
  readonly intendedAtMillis: number;
  readonly occurrenceId: Digest;
}
/** Hosts must supply an explicit authorizer. There is no possession/default allow Layer. */
export class ScheduleAuthorizer extends Context.Service<
  ScheduleAuthorizer,
  {
    readonly manage: (
      request: ScheduleManagementAuthorization,
    ) => Effect.Effect<void, ScheduleAuthorizationError | ScheduleStorageError>;
    readonly prepare: (
      request: ScheduleOccurrenceAuthorization,
    ) => Effect.Effect<
      ScheduleAuthorizationDecision,
      ScheduleAuthorizationError | ScheduleStorageError
    >;
  }
>()("@effect-agent/session/ScheduleAuthorizer") {}

export const SchedulingLimits = Schema.Struct({
  maxSchedulesPerOwner: Positive.check(Schema.isLessThanOrEqualTo(100_000)),
  minIntervalMillis: Positive,
  maxInputBytes: Positive.check(Schema.isLessThanOrEqualTo(65_536)),
  dueBatchSize: Positive.check(Schema.isLessThanOrEqualTo(1_024)),
  admissionConcurrency: Positive.check(Schema.isLessThanOrEqualTo(64)),
  retryBaseMillis: Positive,
  retryMaxMillis: Positive,
  admissionTimeoutMillis: Positive,
  recoveryPollMillis: Positive,
});
export type SchedulingLimits = typeof SchedulingLimits.Type;
export const defaultSchedulingLimits: SchedulingLimits = {
  maxSchedulesPerOwner: 1_000,
  minIntervalMillis: 60_000,
  maxInputBytes: 65_536,
  dueBatchSize: 64,
  admissionConcurrency: 8,
  retryBaseMillis: 1_000,
  retryMaxMillis: 300_000,
  admissionTimeoutMillis: 30_000,
  recoveryPollMillis: 30_000,
};

export const SchedulePageRequest = Schema.Struct({
  owner: ScheduleOwner,
  after: Schema.optionalKey(ScheduleId),
  limit: Positive.check(Schema.isLessThanOrEqualTo(100)),
});
export type SchedulePageRequest = typeof SchedulePageRequest.Type;
export const SchedulePage = Schema.Struct({
  items: Schema.Array(ScheduleRecord).check(Schema.isMaxLength(100)),
  next: Schema.NullOr(ScheduleId),
});
export type SchedulePage = typeof SchedulePage.Type;

/** Driver pagination uses the index only; a corrupt record cannot poison an entire page. */
export const ScheduleDueCursor = Schema.Struct({
  owner: ScheduleOwner,
  scheduleId: ScheduleId,
  deadlineAtMillis: ScheduleInstant,
});
export type ScheduleDueCursor = typeof ScheduleDueCursor.Type;

/** Local transaction commands. Admission is deliberately absent from this union. */
export const ScheduleChange = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Update"),
    expectedRevision: Positive,
    configuration: ScheduleConfiguration,
    nextAtMillis: Schema.NullOr(ScheduleInstant),
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Control"),
    expectedRevision: Positive,
    expectedVersion: Positive,
    action: Schema.Literals(["pause", "resume", "cancel"]),
    nextAtMillis: Schema.NullOr(ScheduleInstant),
    nowMillis: ScheduleInstant,
    skippedRange: Schema.NullOr(ScheduleSkippedRange),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Prepare"),
    expectedRevision: Positive,
    expectedCursor: ScheduleInstant,
    envelope: ScheduledEnvelope,
    nextAtMillis: Schema.NullOr(ScheduleInstant),
    skippedRange: Schema.NullOr(ScheduleSkippedRange),
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("DenyPreparation"),
    expectedRevision: Positive,
    expectedCursor: ScheduleInstant,
    refusal: ScheduleRefusal,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Complete"),
    occurrenceId: Digest,
    receipt: Receipt,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Retry"),
    occurrenceId: Digest,
    retry: ScheduleRetry,
    nowMillis: ScheduleInstant,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Refuse"),
    occurrenceId: Digest,
    refusal: ScheduleRefusal,
    nowMillis: ScheduleInstant,
  }),
]);
export type ScheduleChange = typeof ScheduleChange.Type;
export type ScheduleStoreFailure =
  | ScheduleStorageError
  | ScheduleConflict
  | ScheduleNotFound
  | ScheduleCapacityError
  | ScheduleFailpointError;

/** Each insert/change is one atomic local transaction, including platform wake changes. */
export class ScheduleStore extends Context.Service<
  ScheduleStore,
  {
    readonly insert: (
      record: ScheduleRecord,
      ownerLimit: number,
    ) => Effect.Effect<ScheduleRecord, ScheduleStoreFailure>;
    readonly get: (key: ScheduleKey) => Effect.Effect<ScheduleRecord | null, ScheduleStorageError>;
    readonly list: (
      request: SchedulePageRequest,
    ) => Effect.Effect<SchedulePage, ScheduleStorageError>;
    readonly change: (
      key: ScheduleKey,
      change: ScheduleChange,
      ownerLimit?: number,
    ) => Effect.Effect<ScheduleRecord, ScheduleStoreFailure>;
    /** Internal driver query, not a public management listing. Pending deadlines take precedence. */
    readonly due: (
      nowMillis: number,
      limit: number,
      owner?: ScheduleOwner,
      after?: ScheduleDueCursor,
    ) => Effect.Effect<ReadonlyArray<ScheduleDueCursor>, ScheduleStorageError>;
    readonly nextDeadline: (
      owner?: ScheduleOwner,
    ) => Effect.Effect<number | null, ScheduleStorageError>;
  }
>()("@effect-agent/session/ScheduleStore") {}

/** A dropped hint is repaired by the driver's indexed recovery query. */
export class ScheduleWake extends Context.Service<
  ScheduleWake,
  {
    readonly notify: Effect.Effect<void>;
    readonly await: Effect.Effect<void>;
  }
>()("@effect-agent/session/ScheduleWake") {}

export class ScheduleFailpointError extends Schema.TaggedError<ScheduleFailpointError>()(
  "ScheduleFailpointError",
  { point: Schema.String },
) {}
export const ScheduleFailpoint = Context.Reference<{
  readonly hit: (point: string) => Effect.Effect<void, ScheduleFailpointError>;
}>("@effect-agent/session/ScheduleFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

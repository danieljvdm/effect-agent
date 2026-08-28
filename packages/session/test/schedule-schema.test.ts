import { AgentId, ConversationId, ReceiptId, SubmissionId } from "@effect-agent/core";
import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";

import {
  defaultSchedulingLimits,
  DefinitionDigests,
  Digest,
  IdempotencyKey,
  Principal,
  QueueSequence,
  Receipt,
  ScheduleAuthorizationError,
  ScheduleCapacityError,
  ScheduleChange,
  ScheduleConflict,
  ScheduleDestination,
  ScheduleId,
  ScheduleKey,
  SchedulingLimits,
  ScheduleNotFound,
  SchedulePage,
  SchedulePageRequest,
  ScheduleRecord,
  ScheduleSnapshot,
  ScheduleSnapshotPage,
  ScheduleStorageError,
  ScheduleTiming,
  ScheduleTimingRequest,
  ScheduleValidationError,
  ScheduledEnvelope,
  ScheduledInputRefused,
  ScheduledInputRetryable,
} from "../src/index.ts";

const principal = Schema.decodeSync(Principal)("schedule-schema-principal");
const scheduleId = Schema.decodeSync(ScheduleId)("schedule-schema");
const agentId = Schema.decodeSync(AgentId)("schedule-schema-agent");
const conversationId = Schema.decodeSync(ConversationId)("schedule-schema-conversation");
const digest = Schema.decodeSync(Digest)("d".repeat(64));
const occurrenceId = Schema.decodeSync(Digest)("e".repeat(64));
const owner = { tenantId: "schedule-schema-tenant", ownerId: "schedule-schema-owner" };
const key = { owner, scheduleId };
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const receipt = Receipt.make({
  receiptId: Schema.decodeSync(ReceiptId)("schedule-schema-receipt"),
  submissionId: Schema.decodeSync(SubmissionId)("schedule-schema-submission"),
  conversationId,
  queueSequence: Schema.decodeSync(QueueSequence)(1),
});
const envelope = ScheduledEnvelope.make({
  schemaVersion: 1,
  ...key,
  configurationRevision: 1,
  intendedAtMillis: 100,
  preparedAtMillis: 110,
  occurrenceId,
  conversationId,
  deliveryPrincipal: principal,
  agentId,
  definitions,
  input: { work: "persisted" },
  inputDigest: digest,
  admissionKey: Schema.decodeSync(IdempotencyKey)("schedule-schema-admission"),
  authorization: { policyId: "policy", decisionId: "decision" },
});
const record = ScheduleRecord.make({
  schemaVersion: 1,
  ...key,
  creationFingerprint: digest,
  createdBy: principal,
  createdAtMillis: 1,
  updatedAtMillis: 110,
  configurationRevision: 1,
  version: 2,
  configuration: {
    timing: { _tag: "Interval", everyMillis: 60_000, anchorMillis: 100 },
    destination: { _tag: "ExistingConversation", conversationId },
    deliveryPrincipal: principal,
    agentId,
    definitions,
    input: { work: "persisted" },
    inputDigest: digest,
  },
  state: "active",
  nextAtMillis: 60_100,
  pending: {
    envelope,
    retry: {
      attempts: 1,
      nextAttemptAtMillis: 1_110,
      lastAttemptAtMillis: 110,
      lastFailure: "transport",
    },
  },
  lastReceipt: { atMillis: 90, intendedAtMillis: 80, occurrenceId, receipt },
  lastRefusal: {
    atMillis: 70,
    intendedAtMillis: 60,
    occurrenceId,
    phase: "admission",
    code: "capacity-policy",
  },
  lastSkippedRange: { fromMillis: 10, toMillis: 50 },
});

const roundTrip = <A, I>(schema: Schema.Codec<A, I, never, never>, value: A): void => {
  const encoded = Schema.encodeSync(schema)(value);
  expect(Schema.decodeSync(schema)(encoded)).toEqual(value);
};

describe("Schedule persisted schemas", () => {
  it("round trips requests, normalized values, records, pages, commands, and typed errors", () => {
    roundTrip(ScheduleTimingRequest, { _tag: "At", atMillis: 100 });
    roundTrip(ScheduleTimingRequest, { _tag: "After", delayMillis: 100 });
    roundTrip(ScheduleTimingRequest, { _tag: "Interval", everyMillis: 60_000 });
    roundTrip(ScheduleTimingRequest, { _tag: "Cron", expression: "0 * * * *" });
    roundTrip(ScheduleTiming, { _tag: "At", atMillis: 100 });
    roundTrip(ScheduleTiming, { _tag: "Interval", everyMillis: 60_000, anchorMillis: 100 });
    roundTrip(ScheduleTiming, { _tag: "Cron", expression: "0 * * * *", timeZone: "UTC" });
    roundTrip(ScheduleDestination, { _tag: "ExistingConversation", conversationId });
    roundTrip(ScheduleDestination, { _tag: "FreshConversation" });
    roundTrip(ScheduledEnvelope, envelope);
    roundTrip(ScheduleRecord, record);
    roundTrip(ScheduleSnapshot, {
      ...record,
      observedAtMillis: 1_200,
      pendingAgeMillis: 1_090,
      latenessMillis: 1_100,
    });
    roundTrip(SchedulePageRequest, { owner, after: scheduleId, limit: 25 });
    roundTrip(SchedulePage, { items: [record], next: scheduleId });
    roundTrip(ScheduleSnapshotPage, {
      items: [
        {
          ...record,
          observedAtMillis: 1_200,
          pendingAgeMillis: 1_090,
          latenessMillis: 1_100,
        },
      ],
      next: null,
    });
    roundTrip(SchedulingLimits, defaultSchedulingLimits);

    const changes: ReadonlyArray<typeof ScheduleChange.Type> = [
      {
        _tag: "Update",
        expectedRevision: 1,
        configuration: record.configuration,
        nextAtMillis: 60_100,
        nowMillis: 120,
      },
      {
        _tag: "Control",
        expectedRevision: 1,
        expectedVersion: 2,
        action: "resume",
        nextAtMillis: 60_100,
        nowMillis: 120,
        skippedRange: null,
      },
      {
        _tag: "Prepare",
        expectedRevision: 1,
        expectedCursor: 100,
        envelope,
        nextAtMillis: 60_100,
        skippedRange: { fromMillis: 100, toMillis: 100 },
        nowMillis: 110,
      },
      {
        _tag: "DenyPreparation",
        expectedRevision: 1,
        expectedCursor: 100,
        refusal: {
          atMillis: 110,
          intendedAtMillis: 100,
          occurrenceId,
          phase: "preparation",
          code: "denied",
        },
        nowMillis: 110,
      },
      { _tag: "Complete", occurrenceId, receipt, nowMillis: 120 },
      {
        _tag: "Retry",
        occurrenceId,
        retry: {
          attempts: 2,
          nextAttemptAtMillis: 1_200,
          lastAttemptAtMillis: 120,
          lastFailure: "storage",
        },
        nowMillis: 120,
      },
      {
        _tag: "Refuse",
        occurrenceId,
        refusal: {
          atMillis: 120,
          intendedAtMillis: 100,
          occurrenceId,
          phase: "admission",
          code: "refused",
        },
        nowMillis: 120,
      },
    ];
    for (const change of changes) roundTrip(ScheduleChange, change);

    roundTrip(ScheduleValidationError, ScheduleValidationError.make({ message: "invalid" }));
    roundTrip(ScheduleConflict, ScheduleConflict.make({ reason: "revision", key }));
    roundTrip(ScheduleNotFound, ScheduleNotFound.make({ key }));
    roundTrip(
      ScheduleAuthorizationError,
      ScheduleAuthorizationError.make({ code: "unauthorized" }),
    );
    roundTrip(ScheduleCapacityError, ScheduleCapacityError.make({ limit: 1 }));
    roundTrip(
      ScheduleStorageError,
      ScheduleStorageError.make({ operation: "get", reason: "corrupt" }),
    );
    roundTrip(ScheduledInputRefused, ScheduledInputRefused.make({ code: "refused" }));
    roundTrip(ScheduledInputRetryable, ScheduledInputRetryable.make({ reason: "ambiguous" }));
    expect(changes).toHaveLength(7);
  });

  it("rejects unknown versions and tags and enforces the 64 KiB scheduling ceiling", () => {
    expect(() => Schema.decodeUnknownSync(ScheduleRecord)({ ...record, schemaVersion: 2 })).toThrow(
      /./,
    );
    expect(() =>
      Schema.decodeUnknownSync(ScheduleChange)({ _tag: "Unknown", nowMillis: 1 }),
    ).toThrow(/./);
    expect(() =>
      Schema.decodeUnknownSync(SchedulingLimits)({
        ...defaultSchedulingLimits,
        maxInputBytes: 65_537,
      }),
    ).toThrow(/./);
  });

  it("rejects malformed Unicode names before they cross a UTF-8 storage boundary", () => {
    const isKey = Schema.is(ScheduleKey);
    for (const name of ["\ud800", "\udc00", "tenant\ud800name", "\udc00\ud800"]) {
      expect(isKey({ ...key, scheduleId: name })).toBe(false);
      expect(isKey({ ...key, owner: { ...owner, tenantId: name } })).toBe(false);
      expect(isKey({ ...key, owner: { ...owner, ownerId: name } })).toBe(false);
    }
    for (const name of ["team-\u{1f642}", "\u6f22\u5b57", "\ufffd"]) {
      expect(isKey({ owner: { tenantId: name, ownerId: name }, scheduleId: name })).toBe(true);
    }
  });
});

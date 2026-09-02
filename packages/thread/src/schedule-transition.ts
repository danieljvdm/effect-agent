import { Result, Schema } from "effect";

import { DefinitionDigests, PersistedJson } from "./records.ts";
import {
  ScheduleConflict,
  type ScheduleChange,
  type ScheduleKey,
  type ScheduleOwner,
  type ScheduleRecord,
} from "./schedule.ts";

const equivalentDefinitions = Schema.toEquivalence(DefinitionDigests);
const equivalentInput = Schema.toEquivalence(PersistedJson);

export const scheduleOwnerKey = (owner: ScheduleOwner): string =>
  JSON.stringify([owner.tenantId, owner.ownerId]);

export const scheduleKeyString = (key: ScheduleKey): string =>
  JSON.stringify([key.owner.tenantId, key.owner.ownerId, key.scheduleId]);

/** Locale-independent Unicode scalar ordering, matching UTF-8/BINARY order for valid text. */
export const compareScheduleNames = (left: string, right: string): number => {
  const leftPoints = Array.from(left, (value) => value.codePointAt(0) ?? 0);
  const rightPoints = Array.from(right, (value) => value.codePointAt(0) ?? 0);
  const length = Math.min(leftPoints.length, rightPoints.length);

  for (let index = 0; index < length; index += 1) {
    const compared = (leftPoints[index] ?? 0) - (rightPoints[index] ?? 0);

    if (compared !== 0) return compared;
  }

  return leftPoints.length - rightPoints.length;
};

export const compareScheduleKeys = (left: ScheduleKey, right: ScheduleKey): number =>
  compareScheduleNames(left.owner.tenantId, right.owner.tenantId) ||
  compareScheduleNames(left.owner.ownerId, right.owner.ownerId) ||
  compareScheduleNames(left.scheduleId, right.scheduleId);

export const scheduleKeyOf = (record: ScheduleRecord): ScheduleKey => ({
  owner: record.owner,
  scheduleId: record.scheduleId,
});

/** Pending recovery always outranks preparation, even after pause or cancellation. */
export const scheduleDeadline = (record: ScheduleRecord): number | null => {
  if (record.pending !== null) return record.pending.retry.nextAttemptAtMillis;

  return record.state === "active" ? record.nextAtMillis : null;
};

/** Paused work reserves capacity; terminal replay evidence does not. */
export const scheduleUsesCapacity = (record: ScheduleRecord): boolean =>
  record.pending !== null || (record.state !== "cancelled" && record.nextAtMillis !== null);

const conflict = (record: ScheduleRecord, reason: "revision" | "cancelled") =>
  Result.fail(ScheduleConflict.make({ reason, key: scheduleKeyOf(record) }));

const changed = (
  record: ScheduleRecord,
  fields: Partial<ScheduleRecord>,
): Result.Result<ScheduleRecord, ScheduleConflict> =>
  Result.succeed({ ...record, ...fields, version: record.version + 1 });

/**
 * The adapter-neutral local transaction reducer. Adapters call it only after decoding the row and
 * command. A stale delivery completion returns the original record without changing even bounded
 * status or the CAS version.
 */
export const applyScheduleChange = (
  record: ScheduleRecord,
  change: ScheduleChange,
): Result.Result<ScheduleRecord, ScheduleConflict> => {
  switch (change._tag) {
    case "Update": {
      if (record.state === "cancelled") return conflict(record, "cancelled");
      if (record.configurationRevision !== change.expectedRevision) {
        return conflict(record, "revision");
      }

      return changed(record, {
        configuration: change.configuration,
        configurationRevision: record.configurationRevision + 1,
        state: "active",
        nextAtMillis: change.nextAtMillis,
        updatedAtMillis: change.nowMillis,
        lastSkippedRange: record.lastSkippedRange,
      });
    }
    case "Control": {
      if (record.state === "cancelled") return conflict(record, "cancelled");
      if (record.configurationRevision !== change.expectedRevision) {
        return conflict(record, "revision");
      }
      if (record.version !== change.expectedVersion) return conflict(record, "revision");
      if (change.action === "resume" && record.state === "active") return Result.succeed(record);
      if (change.action === "pause") {
        return changed(record, {
          state: "paused",
          updatedAtMillis: change.nowMillis,
        });
      }
      if (change.action === "cancel") {
        return changed(record, {
          state: "cancelled",
          nextAtMillis: null,
          updatedAtMillis: change.nowMillis,
        });
      }

      return changed(record, {
        state: "active",
        nextAtMillis: change.nextAtMillis,
        updatedAtMillis: change.nowMillis,
        lastSkippedRange: change.skippedRange ?? record.lastSkippedRange,
      });
    }
    case "Prepare": {
      const configuration = record.configuration;
      const envelope = change.envelope;

      const cursorAdvances =
        configuration.timing._tag === "At"
          ? change.nextAtMillis === null
          : change.nextAtMillis !== null && change.nextAtMillis > envelope.intendedAtMillis;

      const destinationMatches =
        configuration.destination._tag === "FreshThread" ||
        configuration.destination.threadId === envelope.threadId;

      if (
        record.configurationRevision !== change.expectedRevision ||
        record.nextAtMillis !== change.expectedCursor ||
        record.state !== "active" ||
        record.pending !== null ||
        envelope.owner.tenantId !== record.owner.tenantId ||
        envelope.owner.ownerId !== record.owner.ownerId ||
        envelope.scheduleId !== record.scheduleId ||
        envelope.configurationRevision !== record.configurationRevision ||
        envelope.intendedAtMillis < change.expectedCursor ||
        envelope.intendedAtMillis > change.nowMillis ||
        envelope.deliveryPrincipal !== configuration.deliveryPrincipal ||
        envelope.agentId !== configuration.agentId ||
        envelope.inputDigest !== configuration.inputDigest ||
        !equivalentDefinitions(envelope.definitions, configuration.definitions) ||
        !equivalentInput(envelope.input, configuration.input) ||
        !destinationMatches ||
        !cursorAdvances
      ) {
        return conflict(record, "revision");
      }

      return changed(record, {
        nextAtMillis: change.nextAtMillis,
        pending: {
          envelope: change.envelope,
          retry: {
            attempts: 0,
            nextAttemptAtMillis: change.nowMillis,
            lastAttemptAtMillis: null,
            lastFailure: null,
          },
        },
        updatedAtMillis: change.nowMillis,
        lastSkippedRange: change.skippedRange ?? record.lastSkippedRange,
      });
    }
    case "DenyPreparation": {
      if (
        record.configurationRevision !== change.expectedRevision ||
        record.nextAtMillis !== change.expectedCursor ||
        record.state !== "active" ||
        record.pending !== null
      ) {
        return conflict(record, "revision");
      }

      return changed(record, {
        state: "paused",
        updatedAtMillis: change.nowMillis,
        lastRefusal: change.refusal,
      });
    }
    case "Complete": {
      if (record.pending?.envelope.occurrenceId !== change.occurrenceId) {
        return Result.succeed(record);
      }

      return changed(record, {
        pending: null,
        updatedAtMillis: change.nowMillis,
        lastReceipt: {
          atMillis: change.nowMillis,
          intendedAtMillis: record.pending.envelope.intendedAtMillis,
          occurrenceId: change.occurrenceId,
          receipt: change.receipt,
        },
      });
    }
    case "Retry": {
      if (record.pending?.envelope.occurrenceId !== change.occurrenceId) {
        return Result.succeed(record);
      }
      const current = record.pending.retry;

      if (
        change.retry.attempts <= current.attempts ||
        change.retry.nextAttemptAtMillis < current.nextAttemptAtMillis
      ) {
        return Result.succeed(record);
      }

      return changed(record, {
        pending: { envelope: record.pending.envelope, retry: change.retry },
        updatedAtMillis: change.nowMillis,
      });
    }
    case "Refuse": {
      if (record.pending?.envelope.occurrenceId !== change.occurrenceId) {
        return Result.succeed(record);
      }

      return changed(record, {
        state: record.state === "active" ? "paused" : record.state,
        pending: null,
        updatedAtMillis: change.nowMillis,
        lastRefusal: change.refusal,
      });
    }
  }
};

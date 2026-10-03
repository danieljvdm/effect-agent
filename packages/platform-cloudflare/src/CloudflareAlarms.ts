import type { DateTime, Duration } from "effect";
import { Context, Effect, Schema } from "effect";

/** The persisted identity and payload of a due logical alarm. */
export const AlarmEvent = Schema.TaggedStruct("AlarmDue", {
  id: Schema.NonEmptyString,
  payload: Schema.Json,
  scheduledAt: Schema.DateTimeUtc,
  tag: Schema.NonEmptyString,
});

export type AlarmEvent = typeof AlarmEvent.Type;

/** A logical alarm adapter failed before its host could complete the operation. */
export class CloudflareAlarmError extends Schema.TaggedError<CloudflareAlarmError>()(
  "CloudflareAlarmError",
  { reason: Schema.Literals(["storage", "invalid"]), cause: Schema.Defect() },
) {}

export interface AlarmRef {
  readonly id: string;
  readonly tag: string;
}

export interface AlarmInput extends AlarmRef {
  readonly payload: Schema.Json;
  readonly runAt: DateTime.Utc;
  readonly repeatEvery?: Duration.Input;
}

export interface AlarmTransaction {
  readonly scheduleAlarm: (input: AlarmInput) => Effect.Effect<void, CloudflareAlarmError>;
  readonly cancelAlarm: (input: AlarmRef) => Effect.Effect<void, CloudflareAlarmError>;
}

export interface ProcessOptions {
  /** Ordered stops this invocation on failure; isolated continues after typed handler failures. */
  readonly mode: "ordered" | "isolated";
  readonly limit?: number;
  /** Retry timing and backoff follow the host's policy. */
  readonly retryFailedAfter?: Duration.Input;
  /** Failure notification; its timing relative to retry persistence is host-owned. */
  readonly onFailure?: () => Effect.Effect<void>;
}

/**
 * Host-owned logical alarms. Transactions commit application SQL, alarm rows and the
 * single native alarm together on the same DO storage. Acknowledge only after handler
 * success and only if its persisted row was not replaced. Ordered failure propagates
 * and stops later handlers in that invocation; isolated typed handler failures retain
 * retries and allow later handlers. Retry deadlines are host-owned, so ordered mode
 * does not promise FIFO across retries. Interruption must not acknowledge work.
 */
export class CloudflareAlarms extends Context.Service<
  CloudflareAlarms,
  {
    readonly transaction: <A, E, R>(
      body: (transaction: AlarmTransaction) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | CloudflareAlarmError, R>;
    readonly processDue: <E, R>(
      handle: (event: AlarmEvent) => Effect.Effect<void, E, R>,
      options: ProcessOptions,
    ) => Effect.Effect<void, E | CloudflareAlarmError, R>;
  }
>()("@yielded/agent-platform-cloudflare/CloudflareAlarms") {}

export const processDue = <E, R>(
  handle: (event: AlarmEvent) => Effect.Effect<void, E, R>,
  options: ProcessOptions,
) => Effect.flatMap(CloudflareAlarms, (alarms) => alarms.processDue(handle, options));

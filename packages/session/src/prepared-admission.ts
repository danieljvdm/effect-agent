import { Context, Effect } from "effect";

import type { Receipt } from "./durable-runtime.ts";
import { type ScheduledInputFailure, ScheduleStorageError } from "./schedule.ts";
import type { PreparedInput } from "./subscription.ts";

/** Host admission shared by event deliveries and scheduling adapters. */
export class PreparedInputAdmission extends Context.Service<
  PreparedInputAdmission,
  {
    readonly submit: (envelope: PreparedInput) => Effect.Effect<Receipt, ScheduledInputFailure>;
  }
>()("@effect-agent/session/PreparedInputAdmission") {}

/**
 * Reduce one bounded admission attempt. The caller owns concurrency, durable retry state,
 * and completion fencing. Interruption and defects propagate without claiming a refusal.
 */
export const admitPreparedInput = Effect.fn("Session.admitPreparedInput")(
  <R>(submit: Effect.Effect<Receipt, ScheduledInputFailure, R>, timeoutMillis: number) =>
    submit.pipe(
      Effect.timeout(timeoutMillis),
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
          : Effect.fail(ScheduleStorageError.make(error)),
      ),
    ),
);

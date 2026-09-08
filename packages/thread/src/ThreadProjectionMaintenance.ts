import { Clock, Context, Effect, Layer, Option, Schema } from "effect";

import type { AppendResult, FencedAppendRequest } from "./ThreadStore.ts";

/** A derived projection failed; the canonical source remains authoritative. */
export class ThreadProjectionError extends Schema.TaggedError<ThreadProjectionError>()(
  "ThreadProjectionError",
  {
    operation: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * Host-owned, disposable indexes over a canonical Thread. Hooks receive raw local source
 * ports at construction and must not mutate them or acquire source producer ownership.
 * Rows and their contiguous watermark commit atomically; an empty projection still consumes
 * its record. Concurrent work must compare the starting watermark before committing.
 *
 * No method owns a scheduler. The platform owns wakeups, durable scheduling metadata and
 * source-producer fencing. Per-call resources belong to Scope; interruption remains interruption.
 */
export interface ThreadProjectionMaintenanceService {
  /**
   * Runs after a successful source commit and before dependent Tools can execute. An index
   * caught up through firstSequence - 1 must process the entire committed range, including
   * empty records, before returning. Canonical batches contain at most 256 records: chunk
   * within local byte limits and stop at result.lastSequence, never chase a changing tail.
   * An earlier gap defers to drain. Replayed or superseded ranges are idempotent no-ops.
   * Failure must leave the unprocessed prefix visible to pendingDeadline.
   */
  readonly applyCommitted: (
    request: FencedAppendRequest,
    result: AppendResult,
  ) => Effect.Effect<void, ThreadProjectionError>;
  /** One bounded, restartable backfill batch. Never drain until caught up in a loop. */
  readonly drain: Effect.Effect<void, ThreadProjectionError>;
  /**
   * Bounded local inspection. None means caught up; a pending epoch-millisecond deadline
   * survives reconstruction and repeated reads must not postpone it. Projection backlog
   * does not gate canonical execution or host approval publication.
   */
  readonly pendingDeadline: Effect.Effect<Option.Option<number>, ThreadProjectionError>;
}

export class ThreadProjectionMaintenance extends Context.Service<
  ThreadProjectionMaintenance,
  ThreadProjectionMaintenanceService
>()("@effect-agent/thread/ThreadProjectionMaintenance") {
  static readonly layer = Layer.succeed(this)({
    applyCommitted: () => Effect.void,
    drain: Effect.void,
    pendingDeadline: Effect.succeed(Option.none()),
  });
}

/** Run at most one due backfill batch, preserving typed failures, defects and interruption. */
export const drainDue = Effect.gen(function* () {
  const projection = yield* ThreadProjectionMaintenance;
  const deadline = yield* projection.pendingDeadline;

  if (Option.isNone(deadline) || deadline.value > (yield* Clock.currentTimeMillis)) return false;
  yield* projection.drain;

  return true;
});

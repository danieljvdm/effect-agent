import { Clock, Effect, Schema, Stream, type Crypto, type Scope } from "effect";

import {
  ActivityClaim,
  ActivityClaimRequest,
  ActivityProcessorKey,
  ActivityProcessorStore,
  PreparedActivity,
  type ActivityStoreFailure,
} from "./activity.ts";
import { digestJson, type DigestError } from "./digest.ts";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  PersistedJson,
  RecordEnvelope,
} from "./records.ts";
import {
  ThreadRead,
  ThreadStore,
  ThreadTail,
  ThreadTailRequest,
  type ThreadNotMaterialized,
  type ThreadStoreError,
} from "./store.ts";

export class ActivityPassLimits extends Schema.Class<ActivityPassLimits>(
  "@effect-agent/thread/ActivityPassLimits",
)({
  maxRecords: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4_096 })),
  pageSize: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  timeoutMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 300_000 })),
  leaseMillis: ActivityClaimRequest.fields.leaseMillis,
}) {}

export class ActivityProcessingError extends Schema.TaggedError<ActivityProcessingError>()(
  "ActivityProcessingError",
  {
    reason: Schema.Literals(["invalid-input", "noncontiguous", "timeout"]),
    message: Schema.String.check(Schema.isMaxLength(1_024)),
  },
) {}

export class ActivityPassResult extends Schema.Class<ActivityPassResult>(
  "@effect-agent/thread/ActivityPassResult",
)({
  key: ActivityProcessorKey,
  capturedTail: CanonicalSequence,
  throughSequence: CanonicalSequence,
  processed: Schema.Natural,
  pendingRecords: Schema.Natural,
  startedAt: Schema.Finite,
  finishedAt: Schema.Finite,
}) {}

export interface CommittedActivityProcessor<E = never, R = never, EApply = never, RApply = never> {
  readonly key: ActivityProcessorKey;
  /** Unique to this worker lifetime; it is not canonical Thread ownership. */
  readonly owner: string;
  readonly limits: ActivityPassLimits;
  /** Select eligibility and encode an application Schema; null can represent no output. */
  readonly extract: (record: CanonicalRecordEnvelope) => Effect.Effect<PersistedJson, E, R>;
  /**
   * Must durably reconcile workId before returning, using atomic idempotency and conditional
   * writes. This may run again after any lost acknowledgement. A stale invocation can finish
   * only the already pinned output; it must not overwrite a later correction or withdrawal.
   * Retain reconciliation evidence for the lifetime of pending work, including any supported
   * backup/restore window. Returning before durable application would permit skipped output.
   */
  readonly apply: (work: PreparedActivity) => Effect.Effect<void, EApply, RApply>;
}

const invalid = (message: string) =>
  ActivityProcessingError.make({ reason: "invalid-input", message });
const contiguous = () =>
  ActivityProcessingError.make({
    reason: "noncontiguous",
    message: "Committed activity does not match its captured Thread prefix or pinned record",
  });

/** Stable work identity independent of worker claims, retry count, extraction, and clocks. */
export const activityWorkId = Effect.fn("activityWorkId")(function* (
  key: ActivityProcessorKey,
  sequence: CanonicalSequence,
) {
  return yield* digestJson([
    "effect-agent/activity@1",
    key.processorId,
    key.processorVersion,
    key.threadId,
    sequence,
  ]);
});

/**
 * Process one bounded committed prefix of one application-selected Thread. Nothing runs until
 * invoked, and no daemon, global Thread directory, admission ledger, or engine checkpoint is
 * involved. PersistentHistory makes a successful Run's batch visible together; durable Runs
 * expose incremental committed records. The application decides what each record means.
 *
 * A durable pending output wins over a new extraction after restart. Apply finishes before
 * progress advances, so separate output stores require durable idempotency. This does not
 * promise exactly-once external side effects. Each callback owns a Scope; the finite pass has
 * a timeout and a longer lease. Claim release has a separate 500ms deadline; failure leaves
 * the bounded lease to expire.
 */
export const processCommittedActivity = Effect.fn("processCommittedActivity")(function* <
  E = never,
  R = never,
  EApply = never,
  RApply = never,
>(
  processor: CommittedActivityProcessor<E, R, EApply, RApply>,
): Effect.fn.Return<
  ActivityPassResult,
  | E
  | EApply
  | ActivityStoreFailure
  | ActivityProcessingError
  | DigestError
  | ThreadStoreError
  | ThreadNotMaterialized,
  ActivityProcessorStore | ThreadStore | Crypto.Crypto | Exclude<R | RApply, Scope.Scope>
> {
  const key = yield* Schema.decodeUnknownEffect(ActivityProcessorKey)(processor.key).pipe(
    Effect.mapError(() => invalid("Invalid activity processor key")),
  );
  const limits = yield* Schema.decodeUnknownEffect(ActivityPassLimits)(processor.limits).pipe(
    Effect.mapError(() => invalid("Invalid activity pass limits")),
  );
  if (limits.leaseMillis < limits.timeoutMillis + 1_000) {
    return yield* invalid("The activity lease must exceed the pass timeout by at least 1000ms");
  }
  const request = yield* ActivityClaimRequest.makeEffect({
    key,
    owner: processor.owner,
    leaseMillis: limits.leaseMillis,
  }).pipe(Effect.mapError(() => invalid("Invalid activity worker identity")));
  const progress = yield* ActivityProcessorStore;
  const threads = yield* ThreadStore;
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const acquired = yield* Effect.acquireRelease(
        progress.claim(request).pipe(Effect.interruptible),
        (claim) =>
          progress
            .release(claim)
            .pipe(Effect.interruptible, Effect.timeoutOption(500), Effect.ignore),
      );
      let claim = yield* Schema.decodeUnknownEffect(ActivityClaim)(acquired).pipe(
        Effect.mapError(() => invalid("Malformed activity claim")),
      );
      if (
        !Schema.toEquivalence(ActivityProcessorKey)(claim.key, key) ||
        claim.owner !== request.owner
      )
        return yield* invalid("Activity claim identity does not match its request");
      const startedAt = yield* Clock.currentTimeMillis;
      const tail = yield* threads
        .inspectTail(ThreadTailRequest.make({ threadId: key.threadId }))
        .pipe(
          Effect.flatMap((value) =>
            Schema.decodeUnknownEffect(ThreadTail)(value).pipe(Effect.mapError(contiguous)),
          ),
        );
      if (tail.threadId !== key.threadId || tail.tailSequence < claim.throughSequence) {
        return yield* contiguous();
      }
      const through = Math.min(tail.tailSequence, claim.throughSequence + limits.maxRecords);
      let processed = 0;
      while (claim.throughSequence < through) {
        const limit = Math.min(limits.pageSize, through - claim.throughSequence);
        const page = yield* threads
          .read(
            ThreadRead.make({
              threadId: key.threadId,
              afterSequence: claim.throughSequence,
              limit,
            }),
          )
          .pipe(Stream.take(limit + 1), Stream.runCollect);
        if (page.length !== limit) return yield* contiguous();
        for (const rawRecord of page) {
          const record = yield* Schema.decodeUnknownEffect(Schema.toType(CanonicalRecordEnvelope))(
            rawRecord,
          ).pipe(Effect.mapError(contiguous));
          if (record.threadId !== key.threadId || record.sequence !== claim.throughSequence + 1) {
            return yield* contiguous();
          }
          const workId = yield* activityWorkId(key, record.sequence);
          const encodedRecord = yield* Schema.encodeEffect(RecordEnvelope)(record.record).pipe(
            Effect.mapError(contiguous),
          );
          const recordDigest = yield* digestJson(encodedRecord);
          let work = yield* progress.loadPrepared(claim);
          if (work === null) {
            const output = yield* Effect.scoped(processor.extract(record)).pipe(
              Effect.flatMap((value) =>
                Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
                  Effect.mapError(() => invalid("Malformed activity output")),
                ),
              ),
            );
            work = yield* progress.prepare({
              claim,
              work: PreparedActivity.make({
                version: 1,
                key,
                sequence: record.sequence,
                workId,
                recordId: record.record.recordId,
                recordDigest,
                output,
              }),
            });
          }
          const prepared = yield* Schema.decodeUnknownEffect(PreparedActivity)(work).pipe(
            Effect.mapError(() => invalid("Malformed prepared activity")),
          );
          if (
            !Schema.toEquivalence(ActivityProcessorKey)(prepared.key, key) ||
            prepared.sequence !== record.sequence ||
            prepared.workId !== workId ||
            prepared.recordId !== record.record.recordId ||
            prepared.recordDigest !== recordDigest
          )
            return yield* contiguous();
          yield* Effect.scoped(processor.apply(prepared));
          const next = yield* progress
            .advance({ claim, workId })
            .pipe(
              Effect.flatMap((value) =>
                Schema.decodeUnknownEffect(ActivityClaim)(value).pipe(
                  Effect.mapError(() => invalid("Malformed activity progress")),
                ),
              ),
            );
          if (
            !Schema.toEquivalence(ActivityProcessorKey)(next.key, key) ||
            next.owner !== claim.owner ||
            next.epoch !== claim.epoch ||
            next.throughSequence !== record.sequence
          )
            return yield* invalid("Activity progress advanced outside the claimed record");
          claim = next;
          processed += 1;
        }
      }
      return ActivityPassResult.make({
        key,
        capturedTail: tail.tailSequence,
        throughSequence: claim.throughSequence,
        processed,
        pendingRecords: tail.tailSequence - claim.throughSequence,
        startedAt,
        finishedAt: yield* Clock.currentTimeMillis,
      });
    }),
  ).pipe(
    Effect.timeoutOrElse({
      duration: limits.timeoutMillis,
      orElse: () =>
        Effect.fail(
          ActivityProcessingError.make({ reason: "timeout", message: "Activity pass timed out" }),
        ),
    }),
  );
});

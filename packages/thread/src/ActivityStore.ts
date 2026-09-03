import { ThreadId } from "@effect-agent/core/Identifiers";
import { Context, Effect, Layer, Schema } from "effect";

import { CanonicalSequence, Digest, PersistedJson, RecordId } from "./Records.ts";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const Epoch = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const Timestamp = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));

/** Application-owned processor identity. Changing the version starts independent progress. */
export class ActivityProcessorKey extends Schema.Class<ActivityProcessorKey>(
  "@effect-agent/thread/ActivityProcessorKey",
)({
  processorId: Identity,
  processorVersion: Identity,
  threadId: ThreadId.check(Schema.isMaxLength(1_024)),
}) {}

/** Durable extraction output, pinned before any external application of that output. */
export class PreparedActivity extends Schema.Class<PreparedActivity>(
  "@effect-agent/thread/PreparedActivity",
)({
  version: Schema.Literal(1),
  key: ActivityProcessorKey,
  sequence: CanonicalSequence.check(Schema.isGreaterThan(0)),
  workId: Digest,
  recordId: RecordId,
  recordDigest: Digest,
  output: PersistedJson,
}) {}

/** Independent from Thread ownership, submission delivery, and canonical checkpoints. */
export class ActivityClaim extends Schema.Class<ActivityClaim>(
  "@effect-agent/thread/ActivityClaim",
)({
  key: ActivityProcessorKey,
  owner: Identity,
  epoch: Epoch,
  throughSequence: CanonicalSequence,
  leaseExpiresAt: Timestamp,
  /** Pending output captured atomically at acquisition; advance returns null here. */
  pending: Schema.NullOr(PreparedActivity),
}) {}

export class ActivityProgress extends Schema.Class<ActivityProgress>(
  "@effect-agent/thread/ActivityProgress",
)({
  version: Schema.Literal(1),
  key: ActivityProcessorKey,
  throughSequence: CanonicalSequence,
  epoch: Epoch,
  owner: Schema.NullOr(Identity),
  leaseExpiresAt: Timestamp,
  pending: Schema.NullOr(PreparedActivity),
  /** Time of the last cursor advance, not the activity time or a global freshness clock. */
  advancedAt: Schema.NullOr(Timestamp),
}) {}

export class ActivityStoreError extends Schema.TaggedError<ActivityStoreError>()(
  "ActivityStoreError",
  {
    operation: Schema.NonEmptyString,
    reason: Schema.Literals(["invalid-input", "unavailable", "corrupt", "incompatible"]),
  },
) {}

export class ActivityBusy extends Schema.TaggedError<ActivityBusy>()("ActivityBusy", {
  key: ActivityProcessorKey,
  leaseExpiresAt: Timestamp,
}) {}

export class ActivityOwnershipLost extends Schema.TaggedError<ActivityOwnershipLost>()(
  "ActivityOwnershipLost",
  { key: ActivityProcessorKey, owner: Identity, epoch: Epoch },
) {}

export class ActivityWorkConflict extends Schema.TaggedError<ActivityWorkConflict>()(
  "ActivityWorkConflict",
  { key: ActivityProcessorKey, workId: Digest },
) {}

export const ActivityMutationPoint = Schema.Literals([
  "activity:initialize:before",
  "activity:initialize:after",
  "activity:claim:before",
  "activity:claim:after-state",
  "activity:claim:after",
  "activity:prepare:before",
  "activity:prepare:after-state",
  "activity:prepare:after",
  "activity:advance:before",
  "activity:advance:after-state",
  "activity:advance:after",
  "activity:release:before",
  "activity:release:after-state",
  "activity:release:after",
]);

export type ActivityMutationPoint = typeof ActivityMutationPoint.Type;

export class ActivityMutationFailure extends Schema.TaggedError<ActivityMutationFailure>()(
  "ActivityMutationFailure",
  { point: ActivityMutationPoint },
) {}

export class ActivityMutationFailpoint extends Context.Service<
  ActivityMutationFailpoint,
  { readonly hit: (point: ActivityMutationPoint) => Effect.Effect<void, ActivityMutationFailure> }
>()("@effect-agent/thread/ActivityMutationFailpoint") {
  static readonly layer = Layer.succeed(this, { hit: () => Effect.void });
}

export type ActivityStoreFailure =
  | ActivityStoreError
  | ActivityBusy
  | ActivityOwnershipLost
  | ActivityWorkConflict
  | ActivityMutationFailure;

export class ActivityClaimRequest extends Schema.Class<ActivityClaimRequest>(
  "@effect-agent/thread/ActivityClaimRequest",
)({
  key: ActivityProcessorKey,
  owner: Identity,
  leaseMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 600_000 })),
}) {}

/**
 * Optional durable progress for finite committed-activity passes. Every mutation is atomic.
 * claim rejects an unexpired owner and allocates a strictly newer epoch on every successful
 * acquisition, including an initial grant and reacquisition after release with the same owner. It preserves
 * pending output and returns it with the claim. prepare and advance reject expired or superseded claims and
 * require the claim's throughSequence to equal current progress. prepare accepts only the next
 * sequence, pins one output, and rejects divergent reuse. advance requires its pending work ID,
 * increments progress by exactly one, and clears pending in the same transaction.
 *
 * release checks owner/epoch, independent of the cursor, and never discards pending output.
 * The adapter owns its lease clock. A failed release leaves only a bounded lease to expire.
 * This port never owns, mutates, or checkpoints the canonical Thread log.
 */
export class ActivityProcessorStore extends Context.Service<
  ActivityProcessorStore,
  {
    readonly inspect: (
      key: ActivityProcessorKey,
    ) => Effect.Effect<ActivityProgress | null, ActivityStoreError>;
    readonly claim: (
      request: ActivityClaimRequest,
    ) => Effect.Effect<ActivityClaim, ActivityStoreFailure>;
    readonly prepare: (request: {
      readonly claim: ActivityClaim;
      readonly work: PreparedActivity;
    }) => Effect.Effect<PreparedActivity, ActivityStoreFailure>;
    readonly advance: (request: {
      readonly claim: ActivityClaim;
      readonly workId: Digest;
    }) => Effect.Effect<ActivityClaim, ActivityStoreFailure>;
    readonly release: (claim: ActivityClaim) => Effect.Effect<void, ActivityStoreFailure>;
  }
>()("@effect-agent/thread/ActivityProcessorStore") {}

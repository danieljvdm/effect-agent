import { Context, Effect, Layer, Schema } from "effect";

import type * as MemoryNamespace from "./MemoryNamespace.ts";
import { MemorySourceReference } from "./MemoryReference.ts";
import { MemoryDocument, MemoryKey, MemoryWrite } from "./MemoryStore.ts";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));

const Counter = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
);

/** Host source-order lineage. Sequence is monotonic only within one authorityGeneration.
 * Opaque revision strings and different authority generations are never ordered.
 */
export const SourcePosition = Schema.Struct({
  authorityGeneration: Identity,
  sequence: Counter,
});

export type SourcePosition = typeof SourcePosition.Type;

export const comparePosition = (left: SourcePosition, right: SourcePosition): number | undefined =>
  left.authorityGeneration === right.authorityGeneration
    ? Math.sign(left.sequence - right.sequence)
    : undefined;

export const Source = Schema.Struct({
  key: MemoryKey.Wire,
  locator: MemorySourceReference.fields.locator,
  revision: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
  position: SourcePosition,
});

export type Source = typeof Source.Type;

class IntentWire extends Schema.Class<IntentWire>("@effect-agent/remembering/Intent")({
  version: Schema.Literal(1),
  /** Stable host-selected idempotency identity, including processor/schema version. It must be
   * unique across all source-owner Stores writing this target for the receipt retention window.
   */
  id: Identity,
  /** Trusted invocation identity, never taken from extracted/model values. */
  invocationId: Identity,
  source: Source,
  target: MemoryKey.Wire,
}) {}

export type Intent<
  S extends MemoryNamespace.Any = MemoryNamespace.Any,
  T extends MemoryNamespace.Any = MemoryNamespace.Any,
> = Omit<IntentWire, "source" | "target"> & {
  readonly source: Omit<Source, "key"> & { readonly key: MemoryKey<S> };
  readonly target: MemoryKey<T>;
};

export const Intent = {
  Wire: IntentWire,
  make: <S extends MemoryNamespace.Any, T extends MemoryNamespace.Any>(
    fields: Intent<S, T>,
  ): Intent<S, T> =>
    Object.assign(Schema.decodeUnknownSync(IntentWire)(fields), {
      source: { ...fields.source, key: MemoryKey.make(fields.source.key) },
      target: MemoryKey.make(fields.target),
    }),
};

/** Half-open UTF-8 byte range in the exact authoritative source revision. A quote proves
 * provenance only. Applications decide whether it supports a fact and may be shared.
 */
export const Evidence = Schema.Struct({
  source: Schema.Struct({ ...MemorySourceReference.fields, revision: Source.fields.revision }),
  startByte: Counter,
  endByte: Counter,
  quote: Schema.NonEmptyString.check(Schema.isMaxLength(65_536)),
});

export type Evidence = typeof Evidence.Type;

export const Proposal = Schema.Struct({
  version: Schema.Literal(1),
  value: Schema.Json,
  evidence: Schema.Array(Evidence).check(Schema.isMinLength(1), Schema.isMaxLength(128)),
});

export type Proposal = typeof Proposal.Type;

export interface Extracted<Value> {
  readonly value: Value;
  readonly evidence: ReadonlyArray<Evidence>;
}

export class Invalidation extends Schema.Class<Invalidation>(
  "@effect-agent/remembering/Invalidation",
)({
  version: Schema.Literal(1),
  id: Identity,
  source: MemoryKey.Wire,
  position: SourcePosition,
  reason: Schema.Literals(["source-edit", "source-deletion", "source-revocation", "forget"]),
}) {}

export type BoundInvalidation<S extends MemoryNamespace.Any> = Omit<Invalidation, "source"> & {
  readonly source: MemoryKey<S>;
};

const PreparedFields: {
  readonly proposal: typeof Proposal;
  readonly attempt: typeof Counter;
  readonly purpose: Schema.Literals<readonly ["remember", "cleanup"]>;
  readonly applied: Schema.NullOr<typeof MemoryDocument.Wire>;
} = {
  proposal: Proposal,
  attempt: Counter,
  purpose: Schema.Literals(["remember", "cleanup"]),
  /** Original applied revision, retained for source-aware conditional cleanup. */
  applied: Schema.NullOr(MemoryDocument.Wire),
};

const CompletedFields: {
  readonly proposal: Schema.NullOr<typeof Proposal>;
  readonly attempt: typeof Counter;
  readonly applied: Schema.NullOr<typeof MemoryDocument.Wire>;
  readonly outcome: Schema.Literals<readonly ["applied", "no-change", "rejected", "suppressed"]>;
  readonly cleaned: typeof Schema.Boolean;
} = {
  proposal: Schema.NullOr(Proposal),
  attempt: Counter,
  applied: Schema.NullOr(MemoryDocument.Wire),
  outcome: Schema.Literals(["applied", "no-change", "rejected", "suppressed"]),
  cleaned: Schema.Boolean,
};

interface ProgressSchema extends Schema.Union<
  readonly [
    Schema.TaggedStruct<"Pending", {}>,
    Schema.TaggedStruct<"Proposed", typeof PreparedFields>,
    Schema.TaggedStruct<
      "Prepared",
      typeof PreparedFields & { readonly command: typeof MemoryWrite.Wire }
    >,
    Schema.TaggedStruct<"Completed", typeof CompletedFields>,
  ]
> {}

export const Progress: ProgressSchema = Schema.Union([
  Schema.TaggedStruct("Pending", {}),
  Schema.TaggedStruct("Proposed", PreparedFields),
  Schema.TaggedStruct("Prepared", { ...PreparedFields, command: MemoryWrite.Wire }),
  Schema.TaggedStruct("Completed", CompletedFields),
]);

export type Progress<T extends MemoryNamespace.Any = MemoryNamespace.Any> =
  | (typeof Progress.members)[0]["Type"]
  | (Omit<(typeof Progress.members)[1]["Type"], "applied"> & {
      readonly applied: MemoryDocument<T> | null;
    })
  | (Omit<(typeof Progress.members)[2]["Type"], "applied" | "command"> & {
      readonly applied: MemoryDocument<T> | null;
      readonly command: MemoryWrite<T>;
    })
  | (Omit<(typeof Progress.members)[3]["Type"], "applied"> & {
      readonly applied: MemoryDocument<T> | null;
    });

/** Retain this source-target reference after removing a job from the active queue. It contains
 * the immutable proposal and original application receipt needed by later invalidation.
 * Prepared commands must never be pruned or replaced while their outcome is uncertain.
 */
export class Checkpoint extends Schema.Class<Checkpoint>("@effect-agent/remembering/Checkpoint")({
  version: Counter,
  intent: Intent.Wire,
  suppression: Schema.NullOr(Invalidation),
  progress: Progress,
}) {}

/** A checked checkpoint bound to the caller's original namespace definitions. */
export type BoundCheckpoint<S extends MemoryNamespace.Any, T extends MemoryNamespace.Any> = Omit<
  Checkpoint,
  "intent" | "progress" | "suppression"
> & {
  readonly intent: Intent<S, T>;
  readonly progress: Progress<T>;
  readonly suppression: BoundInvalidation<S> | null;
};

export class Admission extends Schema.Class<Admission>("@effect-agent/remembering/Admission")({
  id: Identity,
  status: Schema.Literals(["queued", "duplicate"]),
}) {}

export class InvalidationReceipt extends Schema.Class<InvalidationReceipt>(
  "@effect-agent/remembering/InvalidationReceipt",
)({
  id: Identity,
  status: Schema.Literals(["accepted", "duplicate", "stale"]),
  affected: Counter,
}) {}

export class AdmissionError extends Schema.TaggedError<AdmissionError>()(
  "RememberingAdmissionError",
  {
    reason: Schema.Literals(["invalid-input", "conflict", "capacity", "suppressed", "disabled"]),
  },
) {}

export class CheckpointError extends Schema.TaggedError<CheckpointError>()(
  "RememberingCheckpointError",
  {
    reason: Schema.Literals(["fenced", "missing", "corrupt"]),
  },
) {}

export class ProcessingError extends Schema.TaggedError<ProcessingError>()(
  "RememberingProcessingError",
  {
    reason: Schema.Literals([
      "invalid-input",
      "invalid-evidence",
      "source-changed",
      "budget",
      "timeout",
      "disabled",
      "cleanup-rejected",
    ]),
  },
) {}

export const MutationPoint = Schema.Literals([
  "admission:before",
  "admission:after",
  "proposal:before",
  "proposal:after",
  "command:before",
  "command:after",
  "rebase:before",
  "rebase:after",
  "completion:before",
  "completion:after",
  "suppression:before",
  "suppression:after",
  "cleanup:before",
  "cleanup:after",
]);

export type MutationPoint = typeof MutationPoint.Type;

export class MutationFailure extends Schema.TaggedError<MutationFailure>()(
  "RememberingMutationFailure",
  {
    point: MutationPoint,
  },
) {}

/** Explicit transition failpoints. No-op unless a test installs a replacement. */
export class MutationFailpoint extends Context.Service<
  MutationFailpoint,
  {
    readonly hit: (point: MutationPoint) => Effect.Effect<void, MutationFailure>;
  }
>()("@effect-agent/remembering/MutationFailpoint") {
  static readonly layer = Layer.succeed(this, { hit: () => Effect.void });
}

/** Host-owned durable admission/checkpoint port. It is not a scheduler or profile store.
 *
 * admit atomically binds the complete intent, receipt, source authority, and ready obligation.
 * Identical retries return duplicate; reusing an id with different intent fails conflict.
 * read/save validate the complete intent. save atomically checks expectedVersion and advances
 * its fence; invalidation advances the same fence. A failed CAS must never dispatch new work.
 *
 * invalidate durably records suppression before source acknowledgement and marks every retained
 * source-target reference ready for reconciliation/cleanup. It MUST preserve Prepared commands.
 * Older positions cannot undo newer authority; forget is terminal for that source identity.
 * Bind the active authorityGeneration before admission. Unknown or mismatched lineages fail
 * closed; an explicit host restore cutover installs a new lineage and fences all old workers.
 * Source edits suppress contributions from earlier positions; deletion/forget include their own
 * position. A duplicate invalidation cannot lose pending cleanup. New admissions against an old
 * authority, withdrawn source, or terminal forget fail suppressed.
 *
 * Bound active jobs, proposals, references and receipts. Capacity failure rejects new admission;
 * it never evicts an unresolved command, suppression, or cleanup obligation. Retain references,
 * receipts and suppression for the entire supported delivery/replay/restore window. Expired or
 * incompatible restores fail closed. The host owns alarms, outboxes, ordering, retries, leases,
 * source authorization and whether extraction is enabled. Cleanup remains runnable when it is off.
 */
export interface Store<E = never, R = never> {
  readonly admit: (intent: Intent) => Effect.Effect<Admission, E | AdmissionError, R>;
  readonly read: (intent: Intent) => Effect.Effect<Checkpoint, E | CheckpointError, R>;
  readonly save: (input: {
    readonly intent: Intent;
    readonly expectedVersion: number;
    readonly progress: Progress;
  }) => Effect.Effect<Checkpoint, E | CheckpointError, R>;
  readonly invalidate: (
    event: Invalidation,
  ) => Effect.Effect<InvalidationReceipt, E | AdmissionError, R>;
}

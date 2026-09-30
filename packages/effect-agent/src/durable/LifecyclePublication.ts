import { Cause, Clock, Context, Crypto, Effect, Layer, Option, Schema, type Scope } from "effect";

import { SubmissionId, ThreadId } from "../core/Identifiers.ts";
import { Receipt } from "../core/Receipt.ts";
import { AssignmentTerminal } from "../core/Worker.ts";
import { MessageDeliveryKey } from "./MessageDelivery.ts";
import {
  AbortRequested,
  AgentUpdateEmitted,
  CanonicalSequence,
  RunStartedRecord,
  SubagentJoined,
  SubagentRequested,
  SubagentStarted,
  SubmissionSettledRecord,
  ToolApprovalDecided,
  ToolApprovalRequested,
  UserInputRecorded,
  WorkerInputCompleted,
  WorkerInputRequested,
  WorkerStopRequested,
} from "./Records.ts";
import {
  AbortIntent,
  Settlement,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionSnapshot,
  SuspensionReason,
} from "./SubmissionLedger.ts";
import { PreparedInput } from "./Subscription.ts";
import { getRunInput } from "./ThreadStore.ts";

/** Immutable native admission evidence. It is private and grants no application authority. */
export const LifecyclePublicationSource = SubmissionSnapshot.mapFields((fields) => ({
  submissionId: fields.submissionId,
  threadId: fields.threadId,
  queueSequence: fields.queueSequence,
  principal: fields.principal,
  idempotencyKey: fields.idempotencyKey,
  agentId: fields.agentId,
  agentDigests: fields.agentDigests,
  inputPayload: fields.inputPayload,
  inputDigest: fields.inputDigest,
  receiptId: fields.receiptId,
  parentLinkage: fields.parentLinkage,
  workerAdmission: fields.workerAdmission,
  messageAdmission: fields.messageAdmission,
  createdAt: fields.createdAt,
}));

export type LifecyclePublicationSource = typeof LifecyclePublicationSource.Type;

/** Closed producer facts; model history, Tool payloads and diagnostics are not a publication API. */
export const LifecyclePublicationFact = Schema.Union([
  Schema.TaggedStruct("DeliveryRetained", {
    key: MessageDeliveryKey,
    envelope: PreparedInput,
    createdAtMillis: Schema.Natural,
  }),
  Schema.TaggedStruct("DeliveryChanged", {
    key: MessageDeliveryKey,
    envelope: PreparedInput,
    status: Schema.Literals(["pending", "accepted", "processed", "refused", "parked"]),
    receipt: Schema.NullOr(Receipt),
    settlement: Schema.NullOr(Settlement),
    version: Schema.Natural,
  }),
  Schema.TaggedStruct("WorkerInboxSealed", {
    threadId: ThreadId,
    activeSubmissionIds: Schema.Array(SubmissionId),
    /** First native seal winner; null is an explicit stop rather than assignment completion. */
    terminal: Schema.NullOr(AssignmentTerminal),
  }),
  Schema.TaggedStruct("SubmissionReady", { submissionId: SubmissionId }),
  Schema.TaggedStruct("SubmissionSuspended", {
    submissionId: SubmissionId,
    reason: SuspensionReason,
  }),
  Schema.TaggedStruct("SubmissionUnknown", { submissionId: SubmissionId }),
  Schema.TaggedStruct("SubmissionResumed", { submissionId: SubmissionId }),
  Schema.TaggedStruct("AbortIntentRecorded", { intent: AbortIntent }),
  UserInputRecorded,
  RunStartedRecord,
  AgentUpdateEmitted,
  ToolApprovalRequested,
  ToolApprovalDecided,
  AbortRequested,
  WorkerInputCompleted,
  WorkerInputRequested,
  WorkerStopRequested,
  SubagentRequested,
  SubagentStarted,
  SubagentJoined,
  SubmissionSettledRecord,
]);

export type LifecyclePublicationFact = typeof LifecyclePublicationFact.Type;

/**
 * An exact undelivered native fact. `id` is stable across retries and lost acknowledgements.
 * Ordinals order facts only within ownerThreadId. Accepted input order is source.queueSequence;
 * never compare ordinals from the launching Thread and its worker Thread.
 */
export class LifecyclePublication extends Schema.Class<LifecyclePublication>(
  "@effect-agent/thread/LifecyclePublication",
)({
  id: Schema.NonEmptyString.check(Schema.isMaxLength(4096)),
  ownerThreadId: ThreadId,
  ordinal: Schema.Int.check(Schema.isGreaterThan(0)),
  createdAt: Schema.DateTimeUtcFromString,
  canonicalSequence: Schema.optionalKey(CanonicalSequence),
  source: Schema.optionalKey(LifecyclePublicationSource),
  fact: LifecyclePublicationFact,
}) {}

export class LifecyclePublicationError extends Schema.TaggedError<LifecyclePublicationError>()(
  "LifecyclePublicationError",
  {
    reason: Schema.Literals(["unavailable", "conflict", "corrupt", "capacity"]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Bound both retained facts and the immutable admission evidence resolved before dispatch. */
export const lifecyclePublicationBatchMaxFacts = 8;

/** One owner's bounded pending prefix, strictly ordered by its durable ordinals. */
export const LifecyclePublicationBatch = Schema.NonEmptyArray(LifecyclePublication).check(
  Schema.isMaxLength(lifecyclePublicationBatchMaxFacts),
  Schema.makeFilter(
    (facts) =>
      facts.every((fact, index) => {
        const previous = facts[index - 1];

        return (
          fact.ownerThreadId === facts[0].ownerThreadId &&
          (previous === undefined || fact.ordinal > previous.ordinal)
        );
      }),
    { expected: "One owner with strictly increasing publication ordinals" },
  ),
);

export type LifecyclePublicationBatch = typeof LifecyclePublicationBatch.Type;

/** Native recovery state, independent of Attempts. Hosts serialize drains per storage owner. */
export interface LifecyclePublicationStorage {
  /** Select up to `limit` due owners, with a bounded pending prefix for each selected owner.
   * `retainedOnly` excludes unmaterialized source facts, for a precommitted start prefix.
   */
  readonly pending: (
    nowMillis: number,
    limit: number,
    options?: { readonly retainedOnly?: boolean },
  ) => Effect.Effect<ReadonlyArray<LifecyclePublicationBatch>, LifecyclePublicationError>;
  /** Atomically acknowledge the exact batch, retaining its identities and fingerprints. */
  readonly acknowledge: (
    publications: LifecyclePublicationBatch,
  ) => Effect.Effect<void, LifecyclePublicationError>;
  /**
   * Claim a due owner batch and persist its retry before dispatch. Eight automatic attempts;
   * 1s exponential backoff capped at 60s, after the dispatch timeout. The final attempt parks
   * the payload before dispatch so process loss cannot renew its budget. False means not due.
   */
  readonly claim: (
    publications: LifecyclePublicationBatch,
    nowMillis: number,
    timeoutMillis: number,
  ) => Effect.Effect<boolean, LifecyclePublicationError>;
  readonly pendingDeadline: Effect.Effect<Option.Option<number>, LifecyclePublicationError>;
  /**
   * Deadline for `pending(..., { retainedOnly: true })`, excluding unmaterialized source facts.
   * Source-backed stores provide this when `pendingDeadline` also includes source intent.
   * Otherwise `pendingDeadline` already describes retained work.
   */
  readonly retainedPendingDeadline?: Effect.Effect<
    Option.Option<number>,
    LifecyclePublicationError
  >;
  /** Explicit operator retry after repairing a parked owner's destination. */
  readonly retryParked: (
    ownerThreadId: ThreadId,
    nowMillis: number,
  ) => Effect.Effect<void, LifecyclePublicationError>;
}

/**
 * Publish one owner's ordered batch in one idempotent host transaction, including authorization,
 * records, receipts and delivery intents. Larger backlogs continue in later batches.
 * Return only after the entire batch commits. Retries
 * may include already committed identities plus later facts; deduplicate each fact's `id`.
 * Private input/update/result fields remain private; select declared public fields explicitly.
 * Revocation/deletion is an acknowledged domain decision, not an infrastructure retry.
 */
export class LifecyclePublicationHandler extends Context.Service<
  LifecyclePublicationHandler,
  {
    readonly publish: (
      publications: LifecyclePublicationBatch,
    ) => Effect.Effect<void, LifecyclePublicationError, Scope.Scope>;
  }
>()("@effect-agent/thread/LifecyclePublicationHandler") {}

/** Adapter configuration; the opt-in layer captures its explicit Crypto requirement once. */
export const LifecyclePublicationConfig = Context.Reference<Option.Option<Crypto.Crypto>>(
  "@effect-agent/thread/LifecyclePublicationConfig",
  { defaultValue: () => Option.none() },
);

/** Enable native SQL obligations without changing dependencies of disabled storage assemblies. */
export const lifecyclePublicationLayer = Layer.effect(LifecyclePublicationConfig)(
  Effect.map(Crypto.Crypto, Option.some),
);

/** Resolve immutable evidence by its exact native identity, never by scanning execution history. */
const withSource = Effect.fn("LifecyclePublication.withSource")(
  function* (publication: LifecyclePublication) {
    if (
      publication.source !== undefined ||
      (publication.fact._tag === "WorkerInputCompleted" &&
        publication.fact.workerThreadId !== publication.ownerThreadId)
    )
      return publication;
    const fact = publication.fact;

    let submissionId =
      fact._tag === "AbortIntentRecorded"
        ? fact.intent.submissionId
        : "submissionId" in fact
          ? fact.submissionId
          : undefined;

    const runId =
      fact._tag === "AgentUpdateEmitted"
        ? fact.update.runId
        : "runId" in fact
          ? fact.runId
          : undefined;

    if (submissionId === undefined && runId !== undefined) {
      const input = yield* getRunInput({ threadId: publication.ownerThreadId, runId });

      if (Option.isNone(input) || input.value.record.payload._tag !== "UserInputRecorded")
        return yield* LifecyclePublicationError.make({ reason: "unavailable" });
      submissionId = input.value.record.payload.submissionId;
    }
    if (submissionId === undefined) return publication;
    const ledger = yield* SubmissionLedger;
    const found = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

    if (Option.isNone(found))
      return yield* LifecyclePublicationError.make({ reason: "unavailable" });

    const source = yield* Schema.decodeEffect(Schema.toType(LifecyclePublicationSource))(
      found.value,
    );

    return LifecyclePublication.make({ ...publication, source });
  },
  Effect.mapError((cause) => LifecyclePublicationError.make({ reason: "unavailable", cause })),
);

/**
 * One finite wave for the host's existing maintenance coordinator. Persist the next deadline
 * before dispatch, so interruption and process loss retain the same facts without another timer.
 * Finish independent owner batches before surfacing failures; interruption stops the wave.
 * Acknowledgements are exact and idempotent. No producer or external Tool is re-executed here.
 */
export const drainLifecyclePublications = Effect.fn("LifecyclePublication.drain")(function* (
  storage: LifecyclePublicationStorage,
  timeoutMillis = 10_000,
  limit = 4,
  options?: { readonly retainedOnly?: boolean },
) {
  const handler = yield* LifecyclePublicationHandler;
  const pending = yield* storage.pending(yield* Clock.currentTimeMillis, limit, options);

  let failures: Cause.Cause<LifecyclePublicationError> = Cause.empty;

  for (const batch of pending) {
    yield* Effect.gen(function* () {
      if (!(yield* storage.claim(batch, yield* Clock.currentTimeMillis, timeoutMillis))) return;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const publications = yield* Effect.forEach(batch, withSource);

          yield* handler.publish(publications);
        }),
      ).pipe(
        Effect.timeoutOrElse({
          duration: timeoutMillis,
          orElse: () => LifecyclePublicationError.make({ reason: "unavailable" }),
        }),
      );
      yield* storage.acknowledge(batch);
    }).pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterrupts(cause),
        (cause) => {
          failures = Cause.combine(failures, cause);

          return Effect.void;
        },
      ),
    );
  }
  if (failures.reasons.length > 0) return yield* Effect.failCause(failures);

  return pending.length;
});

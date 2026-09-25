import { Clock, Context, Crypto, Effect, Layer, Option, Schema, type Scope } from "effect";

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

/** Native recovery state, not a second application record or launch queue. */
export interface LifecyclePublicationStorage {
  readonly pending: (
    nowMillis: number,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<LifecyclePublication>, LifecyclePublicationError>;
  readonly acknowledge: (
    publication: LifecyclePublication,
  ) => Effect.Effect<void, LifecyclePublicationError>;
  readonly defer: (
    publication: LifecyclePublication,
    untilMillis: number,
  ) => Effect.Effect<void, LifecyclePublicationError>;
  readonly pendingDeadline: Effect.Effect<Option.Option<number>, LifecyclePublicationError>;
  /** Earliest retained owner obligation, including deferred facts that still gate execution. */
  readonly pendingDeadlineFor: (
    ownerThreadId: ThreadId,
  ) => Effect.Effect<Option.Option<number>, LifecyclePublicationError>;
}

/**
 * Publish an idempotent domain command, including its authorization, exact receipt and delivery
 * intent. Return only after that command commits. Treat private input/update/result fields as
 * private; the application must select its declared public fields. Revocation/deletion is an
 * acknowledged domain decision, not an indefinitely retryable infrastructure failure.
 */
export class LifecyclePublicationHandler extends Context.Service<
  LifecyclePublicationHandler,
  {
    readonly publish: (
      publication: LifecyclePublication,
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
 * before dispatch, so interruption and process loss retain the same fact without another timer.
 * Acknowledgements are exact and idempotent. No producer or external Tool is re-executed here.
 */
export const drainLifecyclePublications = Effect.fn("LifecyclePublication.drain")(function* (
  storage: LifecyclePublicationStorage,
  timeoutMillis = 10_000,
  limit = 4,
) {
  const handler = yield* LifecyclePublicationHandler;
  const pending = yield* storage.pending(yield* Clock.currentTimeMillis, limit);

  for (const publication of pending) {
    yield* storage.defer(publication, (yield* Clock.currentTimeMillis) + timeoutMillis);
    yield* Effect.scoped(withSource(publication).pipe(Effect.flatMap(handler.publish))).pipe(
      Effect.timeoutOrElse({
        duration: timeoutMillis,
        orElse: () => LifecyclePublicationError.make({ reason: "unavailable" }),
      }),
    );
    yield* storage.acknowledge(publication);
  }

  return pending.length;
});

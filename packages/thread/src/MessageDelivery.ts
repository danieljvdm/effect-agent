import { ThreadId } from "@effect-agent/core/Identifiers";
import { Receipt } from "@effect-agent/core/Receipt";
import { Clock, Context, Crypto, Effect, Layer, Result, Schema, Semaphore } from "effect";

import { digestJson } from "./Digest.ts";
import { admitPreparedInput, PreparedInputAdmission } from "./PreparedInputAdmission.ts";
import { Digest } from "./Records.ts";
import { ScheduleInstant, ScheduleRetry, ScheduleRetryReason } from "./Schedule.ts";
import { IdempotencyKey, Settlement } from "./SubmissionLedger.ts";
import { PreparedInput } from "./Subscription.ts";

const Positive = Schema.Int.check(Schema.isGreaterThan(0));
const BoundedName = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));

/** An owner is always a source Thread. Destination admission has its own frozen key. */
export const MessageDeliveryKey = Schema.Struct({
  ownerThreadId: ThreadId,
  messageId: IdempotencyKey,
});

export type MessageDeliveryKey = typeof MessageDeliveryKey.Type;

export const MessageDeliveryPolicy = Schema.Struct({
  maxAutomaticAttempts: Positive.check(Schema.isLessThanOrEqualTo(100)),
  attemptTimeoutMillis: Positive.check(Schema.isLessThanOrEqualTo(300_000)),
  retryBaseMillis: Positive,
  retryMaxMillis: Positive,
  settlementPollMillis: Positive,
}).check(Schema.makeFilter((value) => value.retryBaseMillis <= value.retryMaxMillis));

export type MessageDeliveryPolicy = typeof MessageDeliveryPolicy.Type;

export const defaultMessageDeliveryPolicy: MessageDeliveryPolicy = {
  maxAutomaticAttempts: 8,
  attemptTimeoutMillis: 30_000,
  retryBaseMillis: 1_000,
  retryMaxMillis: 60_000,
  settlementPollMillis: 5_000,
};

export const MessageDeliveryStoreLimits = Schema.Struct({
  maxPendingPerOwner: Positive,
  maxRetainedPerOwner: Positive,
  maxEnvelopeBytes: Positive,
}).check(Schema.makeFilter((value) => value.maxPendingPerOwner <= value.maxRetainedPerOwner));

export type MessageDeliveryStoreLimits = typeof MessageDeliveryStoreLimits.Type;

export const defaultMessageDeliveryStoreLimits: MessageDeliveryStoreLimits = {
  maxPendingPerOwner: 100,
  maxRetainedPerOwner: 1_000,
  maxEnvelopeBytes: 262_144,
};

const ParkReason = Schema.Literals(["exhausted", "deadline", "status-unavailable"]);

export const MessageDeliveryRecord = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  key: MessageDeliveryKey,
  envelope: PreparedInput,
  envelopeDigest: Digest,
  createdAtMillis: ScheduleInstant,
  /** Immutable creation identity; explicit recovery only renews deadlineAtMillis. */
  initialDeadlineAtMillis: ScheduleInstant,
  deadlineAtMillis: ScheduleInstant,
  policy: MessageDeliveryPolicy,
  version: Positive,
  status: Schema.Literals(["pending", "accepted", "processed", "refused", "parked"]),
  receipt: Schema.NullOr(Receipt),
  settlement: Schema.NullOr(Settlement),
  refusal: Schema.NullOr(BoundedName),
  parkReason: Schema.NullOr(ParkReason),
  leaseUntilMillis: Schema.NullOr(ScheduleInstant),
  retry: ScheduleRetry,
}).check(
  Schema.makeFilter(
    (record) =>
      record.deadlineAtMillis > record.createdAtMillis &&
      record.initialDeadlineAtMillis > record.createdAtMillis &&
      (record.status === "processed") === (record.settlement !== null) &&
      (record.status === "refused") === (record.refusal !== null) &&
      (record.status === "parked") === (record.parkReason !== null) &&
      (record.status === "parked") === record.retry.parked &&
      (record.status === "accepted" || record.status === "processed"
        ? record.receipt !== null
        : true) &&
      (record.status === "pending" || record.status === "refused"
        ? record.receipt === null
        : true) &&
      (record.receipt === null || record.receipt.threadId === record.envelope.threadId) &&
      (record.settlement === null ||
        (record.settlement.receiptId === record.receipt?.receiptId &&
          record.settlement.submissionId === record.receipt.submissionId)) &&
      (record.status === "processed" || record.status === "refused" || record.status === "parked"
        ? record.leaseUntilMillis === null
        : true),
  ),
);

export type MessageDeliveryRecord = typeof MessageDeliveryRecord.Type;

export class MessageDeliveryError extends Schema.TaggedError<MessageDeliveryError>()(
  "MessageDeliveryError",
  {
    reason: Schema.Literals([
      "validation",
      "conflict",
      "capacity",
      "not-found",
      "storage",
      "corrupt",
    ]),
    operation: Schema.String,
  },
) {}

export class MessageDeliveryFailpointError extends Schema.TaggedError<MessageDeliveryFailpointError>()(
  "MessageDeliveryFailpointError",
  {
    point: Schema.String,
  },
) {}

export type MessageDeliveryFailure = MessageDeliveryError | MessageDeliveryFailpointError;

export const MessageDeliveryFailpoint = Context.Reference<{
  readonly hit: (point: string) => Effect.Effect<void, MessageDeliveryFailpointError>;
}>("@effect-agent/thread/MessageDeliveryFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

const fence = { expectedVersion: Positive, nowMillis: ScheduleInstant };

export const MessageDeliveryChange = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Claim"), ...fence }),
  Schema.Struct({ _tag: Schema.Literal("Accept"), ...fence, receipt: Receipt }),
  Schema.Struct({ _tag: Schema.Literal("Process"), ...fence, settlement: Settlement }),
  Schema.Struct({ _tag: Schema.Literal("ObservePending"), ...fence }),
  Schema.Struct({ _tag: Schema.Literal("Refuse"), ...fence, code: BoundedName }),
  Schema.Struct({ _tag: Schema.Literal("Retry"), ...fence, reason: ScheduleRetryReason }),
  Schema.Struct({ _tag: Schema.Literal("Park"), ...fence, reason: ParkReason }),
  Schema.Struct({ _tag: Schema.Literal("Recover"), ...fence, deadlineAtMillis: ScheduleInstant }),
]);

export type MessageDeliveryChange = typeof MessageDeliveryChange.Type;

export const MessageDeliveryPageRequest = Schema.Struct({
  ownerThreadId: ThreadId,
  limit: Positive.check(Schema.isLessThanOrEqualTo(100)),
  after: Schema.optionalKey(IdempotencyKey),
});

export type MessageDeliveryPageRequest = typeof MessageDeliveryPageRequest.Type;

export interface MessageDeliveryPage {
  readonly items: ReadonlyArray<MessageDeliveryRecord>;
  readonly next: IdempotencyKey | null;
}

/**
 * Trusted host port, independent of either Thread's active Submission. List/get require an owner.
 * Inserts and changes are atomic; identical inserts return current state even after completion.
 * No automatic deletion: completed rows retain deduplication evidence and count toward retention.
 * `due`/`nextDeadline` are bounded host recovery seams, never model-facing management operations.
 */
export class MessageDeliveryStore extends Context.Service<
  MessageDeliveryStore,
  {
    readonly insert: (
      record: MessageDeliveryRecord,
    ) => Effect.Effect<MessageDeliveryRecord, MessageDeliveryFailure>;
    readonly get: (
      key: MessageDeliveryKey,
    ) => Effect.Effect<MessageDeliveryRecord | null, MessageDeliveryError>;
    readonly list: (
      request: MessageDeliveryPageRequest,
    ) => Effect.Effect<MessageDeliveryPage, MessageDeliveryError>;
    readonly change: (
      key: MessageDeliveryKey,
      change: MessageDeliveryChange,
    ) => Effect.Effect<MessageDeliveryRecord, MessageDeliveryFailure>;
    readonly due: (
      nowMillis: number,
      limit: number,
      ownerThreadId?: ThreadId,
    ) => Effect.Effect<ReadonlyArray<MessageDeliveryKey>, MessageDeliveryError>;
    readonly nextDeadline: (
      ownerThreadId?: ThreadId,
    ) => Effect.Effect<number | null, MessageDeliveryError>;
  }
>()("@effect-agent/thread/MessageDeliveryStore") {}

export const messageDeliveryKeyString = (key: MessageDeliveryKey): string =>
  JSON.stringify([key.ownerThreadId, key.messageId]);

export const messageDeliveryUsesCapacity = (record: MessageDeliveryRecord): boolean =>
  record.status !== "processed" && record.status !== "refused";

export const messageDeliveryDeadline = (record: MessageDeliveryRecord): number | null =>
  record.status === "pending" || record.status === "accepted"
    ? Math.min(record.deadlineAtMillis, record.leaseUntilMillis ?? record.retry.nextAttemptAtMillis)
    : null;

/** Creation identity deliberately excludes operational counters and renewed recovery deadlines. */
export const sameMessageDeliveryIdentity = (
  left: MessageDeliveryRecord,
  right: MessageDeliveryRecord,
): boolean =>
  messageDeliveryKeyString(left.key) === messageDeliveryKeyString(right.key) &&
  left.envelopeDigest === right.envelopeDigest &&
  Schema.toEquivalence(PreparedInput)(left.envelope, right.envelope) &&
  Schema.toEquivalence(MessageDeliveryPolicy)(left.policy, right.policy) &&
  left.createdAtMillis === right.createdAtMillis &&
  left.initialDeadlineAtMillis === right.initialDeadlineAtMillis;

const error = (reason: MessageDeliveryError["reason"], operation: string) =>
  MessageDeliveryError.make({ reason, operation });

export const validateMessageDelivery = <A, I>(
  schema: Schema.Codec<A, I>,
  value: unknown,
  operation: string,
) =>
  Schema.decodeUnknownEffect(Schema.toType(schema))(value).pipe(
    Effect.mapError(() => error("validation", operation)),
  );

const MessageDeliveryPreparation = Schema.Struct({
  key: MessageDeliveryKey,
  envelope: PreparedInput,
  createdAtMillis: ScheduleInstant,
  deadlineAtMillis: ScheduleInstant,
  policy: MessageDeliveryPolicy,
});

/** Freeze and digest the complete admission envelope before publishing the obligation. */
export const prepareMessageDelivery = Effect.fn("MessageDelivery.prepare")(function* (options: {
  readonly key: MessageDeliveryKey;
  readonly envelope: PreparedInput;
  readonly createdAtMillis: number;
  readonly deadlineAtMillis: number;
  readonly policy?: MessageDeliveryPolicy;
}): Effect.fn.Return<MessageDeliveryRecord, MessageDeliveryError, Crypto.Crypto> {
  // JSON detaches every caller-owned value before Crypto can suspend preparation.
  const snapshot = yield* Schema.encodeEffect(Schema.fromJsonString(MessageDeliveryPreparation))({
    ...options,
    policy: options.policy ?? defaultMessageDeliveryPolicy,
  }).pipe(
    Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(MessageDeliveryPreparation))),
    Effect.mapError(() => error("validation", "prepare")),
  );

  const envelope = snapshot.envelope;

  const encoded = yield* Schema.encodeEffect(PreparedInput)(envelope).pipe(
    Effect.mapError(() => error("validation", "prepare")),
  );

  const inputDigest = yield* digestJson(encoded.input).pipe(
    Effect.mapError(() => error("storage", "digest")),
  );

  if (inputDigest !== envelope.inputDigest) return yield* error("corrupt", "input-digest");

  const envelopeDigest = yield* digestJson(encoded).pipe(
    Effect.mapError(() => error("storage", "digest")),
  );

  return yield* validateMessageDelivery(
    MessageDeliveryRecord,
    {
      schemaVersion: 1,
      key: snapshot.key,
      envelope,
      envelopeDigest,
      createdAtMillis: snapshot.createdAtMillis,
      initialDeadlineAtMillis: snapshot.deadlineAtMillis,
      deadlineAtMillis: snapshot.deadlineAtMillis,
      policy: snapshot.policy,
      version: 1,
      status: "pending",
      receipt: null,
      settlement: null,
      refusal: null,
      parkReason: null,
      leaseUntilMillis: null,
      retry: {
        attempts: 0,
        generation: 0,
        automaticAttempts: 0,
        parked: false,
        nextAttemptAtMillis: snapshot.createdAtMillis,
        lastAttemptAtMillis: null,
        lastFailure: null,
      },
    },
    "prepare",
  );
});

/** Pure reducer used under the adapter's atomic compare-and-swap boundary. */
export const applyMessageDeliveryChange = (
  record: MessageDeliveryRecord,
  change: MessageDeliveryChange,
): Result.Result<MessageDeliveryRecord, MessageDeliveryError> => {
  const conflict = () => Result.fail(error("conflict", change._tag));

  if (record.version !== change.expectedVersion) return conflict();
  if (record.status === "processed" || record.status === "refused") return conflict();
  const now = change.nowMillis;
  const base = { ...record, version: record.version + 1 };

  const parked = (reason: typeof ParkReason.Type): MessageDeliveryRecord => ({
    ...base,
    status: "parked",
    parkReason: reason,
    leaseUntilMillis: null,
    retry: { ...record.retry, parked: true },
  });

  if (change._tag === "Recover") {
    if (record.status !== "parked" || change.deadlineAtMillis <= now) return conflict();

    return Result.succeed({
      ...base,
      deadlineAtMillis: change.deadlineAtMillis,
      status: record.receipt === null ? "pending" : "accepted",
      parkReason: null,
      retry: {
        ...record.retry,
        generation: record.retry.generation + 1,
        automaticAttempts: 0,
        parked: false,
        nextAttemptAtMillis: now,
      },
    });
  }
  if (record.status === "parked") return conflict();
  if (change._tag === "Park") return Result.succeed(parked(change.reason));
  if (change._tag === "Claim") {
    const due = messageDeliveryDeadline(record);

    if (due === null || due > now) return conflict();
    if (now >= record.deadlineAtMillis) return Result.succeed(parked("deadline"));
    if (record.retry.automaticAttempts >= record.policy.maxAutomaticAttempts)
      return Result.succeed(parked("exhausted"));

    return Result.succeed({
      ...base,
      leaseUntilMillis: Math.min(now + record.policy.attemptTimeoutMillis, record.deadlineAtMillis),
      retry: {
        ...record.retry,
        attempts: record.retry.attempts + 1,
        automaticAttempts: record.retry.automaticAttempts + 1,
        lastAttemptAtMillis: now,
      },
    });
  }
  if (record.leaseUntilMillis === null || now > record.leaseUntilMillis) return conflict();
  if (change._tag === "Accept") {
    if (record.status !== "pending") return conflict();
    if (change.receipt.threadId !== record.envelope.threadId)
      return Result.fail(error("corrupt", "receipt-identity"));

    return Result.succeed({
      ...base,
      status: "accepted",
      receipt: change.receipt,
      leaseUntilMillis: null,
      retry: {
        ...record.retry,
        automaticAttempts: 0,
        nextAttemptAtMillis: Math.min(
          now + record.policy.settlementPollMillis,
          record.deadlineAtMillis,
        ),
        lastFailure: null,
      },
    });
  }
  if (change._tag === "Process") {
    if (record.status !== "accepted") return conflict();
    if (
      change.settlement.submissionId !== record.receipt?.submissionId ||
      change.settlement.receiptId !== record.receipt.receiptId
    )
      return Result.fail(error("corrupt", "settlement-identity"));

    return Result.succeed({
      ...base,
      status: "processed",
      settlement: change.settlement,
      leaseUntilMillis: null,
    });
  }
  if (change._tag === "ObservePending") {
    if (record.status !== "accepted") return conflict();
    if (now >= record.deadlineAtMillis) return Result.succeed(parked("deadline"));

    return Result.succeed({
      ...base,
      leaseUntilMillis: null,
      retry: {
        ...record.retry,
        automaticAttempts: 0,
        lastFailure: null,
        nextAttemptAtMillis: Math.min(
          now + record.policy.settlementPollMillis,
          record.deadlineAtMillis,
        ),
      },
    });
  }
  if (change._tag === "Refuse") {
    if (record.status !== "pending") return conflict();

    return Result.succeed({
      ...base,
      status: "refused",
      refusal: change.code,
      leaseUntilMillis: null,
    });
  }
  if (now >= record.deadlineAtMillis) return Result.succeed(parked("deadline"));
  if (record.retry.automaticAttempts >= record.policy.maxAutomaticAttempts)
    return Result.succeed(parked("exhausted"));

  const delay = Math.min(
    record.policy.retryBaseMillis * 2 ** Math.min(record.retry.automaticAttempts - 1, 52),
    record.policy.retryMaxMillis,
  );

  return Result.succeed({
    ...base,
    leaseUntilMillis: null,
    retry: {
      ...record.retry,
      nextAttemptAtMillis: Math.min(
        now + (record.status === "accepted" ? record.policy.settlementPollMillis : delay),
        record.deadlineAtMillis,
      ),
      lastFailure: change.reason,
    },
  });
};

export const MessageDeliveryDriverLimits = Schema.Struct({
  batchSize: Positive.check(Schema.isLessThanOrEqualTo(100)),
  concurrency: Positive.check(Schema.isLessThanOrEqualTo(32)),
});

export type MessageDeliveryDriverLimits = typeof MessageDeliveryDriverLimits.Type;

/**
 * Finite, structured host pump. The host owns its Scope and wakes it from nextDeadline even
 * after source/receiver settlement. Failed or interrupted calls retain their expiring claim;
 * no ordinary tool is replayed here, only idempotent PreparedInput admission/status lookup.
 * Spans expose operation names; envelopes, principals, and message contents are never logged.
 */
export class MessageDeliveryDriver extends Context.Service<
  MessageDeliveryDriver,
  {
    readonly process: (
      key: MessageDeliveryKey,
    ) => Effect.Effect<MessageDeliveryRecord, MessageDeliveryFailure>;
    readonly runDue: (
      ownerThreadId?: ThreadId,
    ) => Effect.Effect<ReadonlyArray<MessageDeliveryRecord>, MessageDeliveryFailure>;
    readonly retry: (
      key: MessageDeliveryKey,
      expectedVersion: number,
      deadlineAtMillis: number,
    ) => Effect.Effect<MessageDeliveryRecord, MessageDeliveryFailure>;
  }
>()("@effect-agent/thread/MessageDeliveryDriver") {
  static layer(
    limits: MessageDeliveryDriverLimits = { batchSize: 100, concurrency: 4 },
  ): Layer.Layer<
    MessageDeliveryDriver,
    MessageDeliveryError,
    MessageDeliveryStore | PreparedInputAdmission | Crypto.Crypto
  > {
    return Layer.effect(
      MessageDeliveryDriver,
      Effect.gen(function* () {
        const config = yield* validateMessageDelivery(
          MessageDeliveryDriverLimits,
          limits,
          "driver-limits",
        );

        const store = yield* MessageDeliveryStore;
        const admission = yield* PreparedInputAdmission;
        const crypto = yield* Crypto.Crypto;
        const failpoint = yield* MessageDeliveryFailpoint;
        const semaphore = yield* Semaphore.make(config.concurrency);

        const process = Effect.fn("MessageDelivery.process")((key: MessageDeliveryKey) =>
          semaphore
            .withPermit(
              Effect.gen(function* () {
                const current = yield* store.get(key);

                if (current === null) return yield* error("not-found", "process");
                const nowMillis = yield* Clock.currentTimeMillis;
                const due = messageDeliveryDeadline(current);

                if (due === null || due > nowMillis) return current;

                const claimed = yield* store.change(key, {
                  _tag: "Claim",
                  expectedVersion: current.version,
                  nowMillis,
                });

                if (claimed.status === "parked") return claimed;

                const envelope = yield* Schema.encodeEffect(PreparedInput)(claimed.envelope).pipe(
                  Effect.mapError(() => error("corrupt", "envelope")),
                );

                const digest = yield* digestJson(envelope).pipe(
                  Effect.provideService(Crypto.Crypto, crypto),
                  Effect.mapError(() => error("storage", "digest")),
                );

                if (digest !== claimed.envelopeDigest)
                  return yield* error("corrupt", "envelope-digest");

                const timeout = Math.min(
                  claimed.policy.attemptTimeoutMillis,
                  (claimed.leaseUntilMillis ?? claimed.deadlineAtMillis) -
                    (yield* Clock.currentTimeMillis),
                );

                // Digest computation cannot extend this claim's admission authority.
                if (timeout <= 0) return claimed;

                type WithoutFence<T> = T extends MessageDeliveryChange
                  ? Omit<T, "expectedVersion" | "nowMillis">
                  : never;

                const commit = (change: WithoutFence<MessageDeliveryChange>) =>
                  Clock.currentTimeMillis.pipe(
                    Effect.flatMap((time) =>
                      store.change(key, {
                        ...change,
                        expectedVersion: claimed.version,
                        nowMillis: time,
                      }),
                    ),
                  );

                if (claimed.status === "pending") {
                  const outcome = yield* admitPreparedInput(
                    admission.submit(claimed.envelope),
                    timeout,
                  ).pipe(Effect.mapError(() => error("corrupt", "admission")));

                  yield* failpoint.hit("message-delivery:admission:after");
                  if (outcome._tag === "Receipt")
                    return yield* commit({ _tag: "Accept", receipt: outcome.receipt });
                  if (outcome._tag === "Refused")
                    return yield* commit({ _tag: "Refuse", code: outcome.error.code });

                  return yield* commit({ _tag: "Retry", reason: outcome.reason });
                }
                if (claimed.receipt === null) return yield* error("corrupt", "receipt");
                if (admission.submissionStatus === undefined)
                  return yield* commit({ _tag: "Park", reason: "status-unavailable" });

                const status = yield* admission.submissionStatus(claimed.receipt).pipe(
                  Effect.timeout(timeout),
                  Effect.map((status) => ({ _tag: "Status" as const, status })),
                  Effect.catchTag("ScheduledInputRetryable", (failure) =>
                    Effect.succeed({ _tag: "Retry" as const, reason: failure.reason }),
                  ),
                  Effect.catchTag("ScheduledInputRefused", () =>
                    Effect.succeed({ _tag: "Unavailable" as const }),
                  ),
                  Effect.catchTag("TimeoutError", () =>
                    Effect.succeed({ _tag: "Retry" as const, reason: "timeout" as const }),
                  ),
                  Effect.catchTag("ScheduleStorageError", (failure) =>
                    failure.reason === "unavailable"
                      ? Effect.succeed({ _tag: "Retry" as const, reason: "storage" as const })
                      : Effect.fail(error("corrupt", "status")),
                  ),
                );

                if (status._tag === "Unavailable")
                  return yield* commit({ _tag: "Park", reason: "status-unavailable" });
                if (status._tag === "Status" && status.status._tag === "settled")
                  return yield* commit({ _tag: "Process", settlement: status.status.settlement });
                if (status._tag === "Status") return yield* commit({ _tag: "ObservePending" });

                return yield* commit({ _tag: "Retry", reason: status.reason });
              }),
            )
            .pipe(
              Effect.catchTag("MessageDeliveryError", (failure) =>
                failure.reason === "conflict"
                  ? store
                      .get(key)
                      .pipe(
                        Effect.flatMap((record) =>
                          record === null ? Effect.fail(failure) : Effect.succeed(record),
                        ),
                      )
                  : Effect.fail(failure),
              ),
            ),
        );

        return MessageDeliveryDriver.of({
          process,
          runDue: Effect.fn("MessageDelivery.runDue")(function* (ownerThreadId) {
            const keys = yield* store.due(
              yield* Clock.currentTimeMillis,
              config.batchSize,
              ownerThreadId,
            );

            return yield* Effect.forEach(keys, process, { concurrency: config.concurrency });
          }),
          retry: Effect.fn("MessageDelivery.retry")(
            function* (key, expectedVersion, deadlineAtMillis) {
              return yield* store.change(key, {
                _tag: "Recover",
                expectedVersion,
                deadlineAtMillis,
                nowMillis: yield* Clock.currentTimeMillis,
              });
            },
          ),
        });
      }),
    );
  }
}

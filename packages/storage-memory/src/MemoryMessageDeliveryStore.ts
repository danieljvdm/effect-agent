import { ThreadId } from "@effect-agent/core/Identifiers";
import {
  applyMessageDeliveryChange,
  defaultMessageDeliveryStoreLimits,
  MessageDeliveryChange,
  MessageDeliveryError,
  MessageDeliveryFailpoint,
  MessageDeliveryKey,
  MessageDeliveryPageRequest,
  MessageDeliveryRecord,
  MessageDeliveryStore,
  MessageDeliveryStoreLimits,
  messageDeliveryDeadline,
  messageDeliveryKeyString,
  messageDeliveryUsesCapacity,
  sameMessageDeliveryIdentity,
  validateMessageDelivery,
} from "@effect-agent/thread/MessageDelivery";
import { ScheduleInstant } from "@effect-agent/thread/Schedule";
import { Effect, Layer, Ref, Schema, Semaphore } from "effect";

const codec = Schema.fromJsonString(MessageDeliveryRecord);

const decode = (text: string) =>
  Schema.decodeEffect(codec)(text).pipe(
    Effect.mapError(() => MessageDeliveryError.make({ reason: "corrupt", operation: "decode" })),
  );

const Scan = Schema.Struct({
  nowMillis: ScheduleInstant,
  limit: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
  ownerThreadId: Schema.optionalKey(ThreadId),
});

export interface MemoryMessageDeliveryStoreOptions {
  /** UTF-8 bound on the complete stored record, including a processed Settlement. */
  readonly maxStoredValueBytes?: number;
}

/** Reference adapter. All retained rows are bounded; completed deduplication evidence is never evicted. */
export const memoryMessageDeliveryStoreLayer = (
  limits: MessageDeliveryStoreLimits = defaultMessageDeliveryStoreLimits,
  options: MemoryMessageDeliveryStoreOptions = {},
): Layer.Layer<MessageDeliveryStore, MessageDeliveryError> =>
  Layer.effect(
    MessageDeliveryStore,
    Effect.gen(function* () {
      const config = yield* validateMessageDelivery(MessageDeliveryStoreLimits, limits, "limits");

      const maxStoredValueBytes = yield* validateMessageDelivery(
        Schema.Int.check(Schema.isGreaterThan(0)),
        options.maxStoredValueBytes ?? 16 * 1_024 * 1_024,
        "stored-value-limit",
      );

      const state = yield* Ref.make<ReadonlyMap<string, string>>(new Map());
      const lock = yield* Semaphore.make(1);
      const failpoint = yield* MessageDeliveryFailpoint;

      const encode = Effect.fn("MemoryMessageDeliveryStore.encode")(function* (
        record: MessageDeliveryRecord,
      ) {
        const text = yield* Schema.encodeEffect(codec)(record).pipe(
          Effect.mapError(() =>
            MessageDeliveryError.make({ reason: "corrupt", operation: "encode" }),
          ),
        );

        if (new TextEncoder().encode(text).byteLength > maxStoredValueBytes) {
          return yield* MessageDeliveryError.make({
            reason: "capacity",
            operation: "stored-value-bytes",
          });
        }

        return text;
      });

      const all = Effect.fn("MemoryMessageDeliveryStore.all")(function* () {
        return yield* Effect.forEach((yield* Ref.get(state)).values(), decode);
      });

      const insert: MessageDeliveryStore["Service"]["insert"] = Effect.fn(
        "MemoryMessageDeliveryStore.insert",
      )(function* (record) {
        const encoded = yield* encode(record);
        const input = yield* decode(encoded);

        if (
          input.version !== 1 ||
          input.status !== "pending" ||
          input.leaseUntilMillis !== null ||
          input.retry.attempts !== 0 ||
          input.retry.generation !== 0 ||
          input.retry.automaticAttempts !== 0 ||
          input.retry.nextAttemptAtMillis !== input.createdAtMillis ||
          input.retry.lastAttemptAtMillis !== null ||
          input.retry.lastFailure !== null ||
          input.deadlineAtMillis !== input.initialDeadlineAtMillis
        ) {
          return yield* MessageDeliveryError.make({
            reason: "validation",
            operation: "insert-state",
          });
        }
        if (
          new TextEncoder().encode(JSON.stringify(input.envelope)).byteLength >
          config.maxEnvelopeBytes
        ) {
          return yield* MessageDeliveryError.make({
            reason: "capacity",
            operation: "envelope-bytes",
          });
        }
        yield* failpoint.hit("message-delivery:insert:before");

        const inserted = yield* lock.withPermit(
          Effect.uninterruptible(
            Effect.gen(function* () {
              const records = yield* Ref.get(state);
              const key = messageDeliveryKeyString(input.key);
              const existingText = records.get(key);

              if (existingText !== undefined) {
                const existing = yield* decode(existingText);

                if (!sameMessageDeliveryIdentity(existing, input))
                  return yield* MessageDeliveryError.make({
                    reason: "conflict",
                    operation: "insert",
                  });

                return existing;
              }

              const owned = (yield* all()).filter(
                (record) => record.key.ownerThreadId === input.key.ownerThreadId,
              );

              if (
                owned.length >= config.maxRetainedPerOwner ||
                owned.filter(messageDeliveryUsesCapacity).length >= config.maxPendingPerOwner
              ) {
                return yield* MessageDeliveryError.make({
                  reason: "capacity",
                  operation: "insert",
                });
              }
              yield* Ref.set(state, new Map(records).set(key, encoded));

              return input;
            }),
          ),
        );

        yield* failpoint.hit("message-delivery:insert:after");

        return yield* decode(yield* encode(inserted));
      });

      const get: MessageDeliveryStore["Service"]["get"] = Effect.fn(
        "MemoryMessageDeliveryStore.get",
      )(function* (key) {
        const input = yield* validateMessageDelivery(MessageDeliveryKey, key, "get");
        const text = (yield* Ref.get(state)).get(messageDeliveryKeyString(input));

        return text === undefined ? null : yield* decode(text);
      });

      const change: MessageDeliveryStore["Service"]["change"] = Effect.fn(
        "MemoryMessageDeliveryStore.change",
      )(function* (key, change) {
        const input = yield* validateMessageDelivery(MessageDeliveryChange, change, "change");
        const decodedKey = yield* validateMessageDelivery(MessageDeliveryKey, key, "change");
        const point = `message-delivery:${input._tag.toLowerCase()}`;

        yield* failpoint.hit(`${point}:before`);

        const changed = yield* lock.withPermit(
          Effect.uninterruptible(
            Effect.gen(function* () {
              const existing = yield* get(decodedKey);

              if (existing === null)
                return yield* MessageDeliveryError.make({
                  reason: "not-found",
                  operation: "change",
                });
              const next = yield* Effect.fromResult(applyMessageDeliveryChange(existing, input));
              const encoded = yield* encode(next);

              yield* Ref.update(state, (records) =>
                new Map(records).set(messageDeliveryKeyString(decodedKey), encoded),
              );

              return next;
            }),
          ),
        );

        yield* failpoint.hit(`${point}:after`);

        return yield* decode(yield* encode(changed));
      });

      return MessageDeliveryStore.of({
        insert,
        get,
        change,
        list: Effect.fn("MemoryMessageDeliveryStore.list")(function* (request) {
          const input = yield* validateMessageDelivery(MessageDeliveryPageRequest, request, "list");

          const records = (yield* all())
            .filter(
              (record) =>
                record.key.ownerThreadId === input.ownerThreadId &&
                (input.after === undefined || record.key.messageId > input.after),
            )
            .sort((left, right) =>
              left.key.messageId < right.key.messageId
                ? -1
                : left.key.messageId > right.key.messageId
                  ? 1
                  : 0,
            );

          const items = records.slice(0, input.limit);

          return {
            items,
            next: records.length > input.limit ? (items.at(-1)?.key.messageId ?? null) : null,
          };
        }),
        due: Effect.fn("MemoryMessageDeliveryStore.due")(
          function* (nowMillis, limit, ownerThreadId) {
            const input = yield* validateMessageDelivery(
              Scan,
              { nowMillis, limit, ...(ownerThreadId === undefined ? {} : { ownerThreadId }) },
              "due",
            );

            return (yield* all())
              .flatMap((record) => {
                const deadline = messageDeliveryDeadline(record);

                return deadline !== null &&
                  deadline <= input.nowMillis &&
                  (input.ownerThreadId === undefined ||
                    input.ownerThreadId === record.key.ownerThreadId)
                  ? [{ key: record.key, deadline }]
                  : [];
              })
              .sort(
                (left, right) =>
                  left.deadline - right.deadline ||
                  (messageDeliveryKeyString(left.key) < messageDeliveryKeyString(right.key)
                    ? -1
                    : 1),
              )
              .slice(0, input.limit)
              .map((record) => record.key);
          },
        ),
        nextDeadline: Effect.fn("MemoryMessageDeliveryStore.nextDeadline")(
          function* (ownerThreadId) {
            if (ownerThreadId !== undefined)
              yield* validateMessageDelivery(ThreadId, ownerThreadId, "nextDeadline");
            let next: number | null = null;

            for (const record of yield* all()) {
              if (ownerThreadId !== undefined && ownerThreadId !== record.key.ownerThreadId)
                continue;
              const deadline = messageDeliveryDeadline(record);

              if (deadline !== null && (next === null || deadline < next)) next = deadline;
            }

            return next;
          },
        ),
      });
    }),
  );

export const MemoryMessageDeliveryStoreLive = memoryMessageDeliveryStoreLayer();

import { Update, UpdateError } from "@effect-agent/core/AgentUpdates";
import type { RunId, ThreadId } from "@effect-agent/core/Identifiers";
import { IdempotencyKey } from "@effect-agent/core/Receipt";
import { Clock, Crypto, DateTime, Effect, Option, Schema } from "effect";

import { digestJson } from "../Digest.ts";
import { DurableRuntimeFailpoint } from "../DurableFailpoint.ts";
import {
  MessageDeliveryStore,
  MessageDeliveryRecord,
  messageDeliveryCapacity,
  messageDeliveryUsesCapacity,
  prepareMessageDelivery,
} from "../MessageDelivery.ts";
import {
  AgentUpdateEmitted,
  BatchId,
  CanonicalBatch,
  type CanonicalRecordEnvelope,
  DefinitionDigests,
  type DeploymentId,
  PersistedJson,
  type ProducerEpoch,
  type ProducerId,
  RecordEnvelope,
  RecordId,
} from "../Records.ts";
import { LedgerError, type SubmissionSnapshot } from "../SubmissionLedger.ts";
import { PreparedInput } from "../Subscription.ts";
import {
  FencedAppendRequest,
  ThreadExportRequest,
  ThreadStore,
  ThreadTailRequest,
} from "../ThreadStore.ts";
import { utf8ByteLength } from "./utf8.ts";
import { WorkerRuntime } from "./worker-runtime.ts";

export const lastWorkerReportMessageId = (records: ReadonlyArray<CanonicalRecordEnvelope>) => {
  for (let index = records.length - 1; index >= 0; index--) {
    const payload = records[index]?.record.payload;

    if (payload?._tag === "WorkerReportPrepared") return payload.messageId;
    if (payload?._tag === "AgentUpdateEmitted" && payload.delivery !== undefined)
      return payload.delivery.messageId;
  }

  return undefined;
};

type Delivery = NonNullable<AgentUpdateEmitted["delivery"]>;

const storage = () =>
  LedgerError.make({ operation: "agent-update", message: "Durable update persistence failed" });

const rejected = (reason: UpdateError["reason"]) => UpdateError.make({ reason });

/** Acceptance is fenced by the actual emitting Attempt; recovery only recreates frozen outbox rows. */
export const makeAgentUpdateRuntime = Effect.fn("AgentUpdates.makeDurable")(function* (options: {
  readonly deploymentId: DeploymentId;
  readonly producerId: ProducerId;
}) {
  const store = yield* ThreadStore;
  const workers = yield* WorkerRuntime;
  const deliveries = yield* Effect.serviceOption(MessageDeliveryStore);
  const crypto = yield* Crypto.Crypto;
  const failpoint = yield* DurableRuntimeFailpoint;

  const read = (threadId: ThreadId) =>
    store.export(ThreadExportRequest.make({ threadId })).pipe(Effect.mapError(storage));

  const prepareDelivery = Effect.fn("AgentUpdates.prepareDelivery")(function* (
    update: Update,
    delivery: Delivery,
  ) {
    const envelope = yield* Schema.decodeUnknownEffect(PreparedInput)(delivery.envelope).pipe(
      Effect.mapError(storage),
    );

    return yield* prepareMessageDelivery({
      key: { ownerThreadId: update.threadId, messageId: delivery.messageId },
      envelope,
      createdAtMillis: delivery.createdAtMillis,
      deadlineAtMillis: delivery.deadlineAtMillis,
      ...(delivery.predecessor === undefined ? {} : { predecessor: delivery.predecessor }),
    }).pipe(Effect.provideService(Crypto.Crypto, crypto), Effect.mapError(storage));
  });

  const insert = Effect.fn("AgentUpdates.restoreDelivery")(function* (record: AgentUpdateEmitted) {
    const delivery = record.delivery;

    if (delivery === undefined) return;
    if (Option.isNone(deliveries)) return yield* storage();
    const prepared = yield* prepareDelivery(record.update, delivery);

    yield* failpoint.hit("update:before-delivery-insert");
    yield* deliveries.value.insert(prepared).pipe(Effect.mapError(storage));
    yield* failpoint.hit("update:after-delivery-insert");
  });

  const repair = Effect.fn("AgentUpdates.repair")(function* (threadId: ThreadId) {
    const history = yield* read(threadId);

    for (const { record } of history.records) {
      if (record.payload._tag === "AgentUpdateEmitted" && record.payload.delivery !== undefined)
        yield* insert(record.payload);
    }
  });

  const emit = Effect.fn("AgentUpdates.accept")(function* (request: {
    readonly submission: SubmissionSnapshot;
    readonly runId: RunId;
    readonly producerEpoch: ProducerEpoch;
    readonly definitions: DefinitionDigests;
    readonly updateId: IdempotencyKey;
    readonly value: Schema.Json;
    readonly maxCount?: number;
    readonly maxBytes?: number;
  }) {
    const value = yield* Schema.encodeEffect(Schema.fromJsonString(PersistedJson))(
      request.value,
    ).pipe(
      Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(PersistedJson))),
      Effect.mapError(() => rejected("validation")),
    );

    const updateId = yield* Schema.decodeUnknownEffect(IdempotencyKey)(request.updateId).pipe(
      Effect.mapError(() => rejected("validation")),
    );

    const digest = yield* digestJson([request.submission.threadId, request.runId, updateId]).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError(storage),
    );

    const id = `agent-update:${digest}`;
    const messageId = Schema.decodeSync(IdempotencyKey)(`worker-update:${digest}`);
    const maxCount = request.maxCount ?? 32;
    const maxBytes = request.maxBytes ?? 16_384;

    if (
      !Number.isSafeInteger(maxCount) ||
      maxCount < 1 ||
      !Number.isSafeInteger(maxBytes) ||
      maxBytes < 1
    )
      return yield* rejected("validation");

    for (let attempt = 0; attempt < 32; attempt++) {
      const history = yield* read(request.submission.threadId);

      const updates = history.records.flatMap(({ record }) =>
        record.payload._tag === "AgentUpdateEmitted" ? [record.payload] : [],
      );

      const existing = updates.find(
        (entry) => entry.update.runId === request.runId && entry.update.updateId === updateId,
      );

      if (existing !== undefined) {
        if (
          existing.update.agentId !== request.submission.agentId ||
          !Schema.toEquivalence(PersistedJson)(existing.update.value, value) ||
          !Schema.toEquivalence(DefinitionDigests)(existing.definitions, request.definitions)
        )
          return yield* rejected("conflict");
        yield* insert(existing);

        return existing.update;
      }
      const own = updates.filter((entry) => entry.update.runId === request.runId);

      if (
        own.length >= maxCount ||
        own.reduce(
          (total, entry) => total + utf8ByteLength(JSON.stringify(entry.update.value)),
          0,
        ) +
          utf8ByteLength(JSON.stringify(value)) >
          maxBytes
      )
        return yield* rejected("capacity");
      if (
        history.records.some(
          ({ record }) =>
            record.payload._tag === "SubmissionSettled" && record.payload.runId === request.runId,
        ) ||
        !history.records.some(
          ({ record }) =>
            record.payload._tag === "RunStarted" && record.payload.runId === request.runId,
        )
      )
        return yield* rejected("identity");

      const update = Update.make({
        schemaVersion: 1,
        agentId: request.submission.agentId,
        threadId: request.submission.threadId,
        runId: request.runId,
        updateId,
        sequence: (updates.at(-1)?.update.sequence ?? 0) + 1,
        value,
      });

      const prepared = yield* workers.prepareUpdate(update, request.submission, messageId);
      let delivery: Delivery | undefined;

      if (prepared !== undefined) {
        if (Option.isNone(deliveries)) return yield* rejected("unavailable");
        const retained = updates.filter((entry) => entry.delivery !== undefined);
        const capacity = messageDeliveryCapacity(deliveries.value.limits, true);

        if (
          retained.length >= capacity.retained ||
          utf8ByteLength(JSON.stringify(prepared.envelope)) >
            deliveries.value.limits.maxEnvelopeBytes
        )
          return yield* rejected("capacity");
        let pending = 0;

        for (const accepted of retained) {
          if (accepted.delivery === undefined) continue;

          const row = yield* deliveries.value
            .get({
              ownerThreadId: request.submission.threadId,
              messageId: accepted.delivery.messageId,
            })
            .pipe(Effect.mapError(storage));

          if (row === null || messageDeliveryUsesCapacity(row)) pending++;
        }
        if (pending >= capacity.pending) return yield* rejected("capacity");
        const predecessor = lastWorkerReportMessageId(history.records);

        delivery = { ...prepared, ...(predecessor === undefined ? {} : { predecessor }) };
        const deliveryRecord = yield* prepareDelivery(update, delivery);

        const stored = yield* Schema.encodeEffect(Schema.fromJsonString(MessageDeliveryRecord))(
          deliveryRecord,
        ).pipe(Effect.mapError(storage));

        if (utf8ByteLength(stored) > deliveries.value.maxStoredValueBytes)
          return yield* rejected("capacity");
      }

      const payload = AgentUpdateEmitted.make({
        update,
        definitions: request.definitions,
        ...(delivery === undefined ? {} : { delivery }),
      });

      const tail = yield* store
        .inspectTail(ThreadTailRequest.make({ threadId: request.submission.threadId }))
        .pipe(Effect.mapError(storage));

      if (tail.producerEpoch !== request.producerEpoch) return yield* storage();
      if (tail.tailSequence !== history.tailSequence || tail.tailDigest !== history.tailDigest)
        continue;
      yield* failpoint.hit("update:before-canonical-append");

      const appended = yield* store
        .append(
          FencedAppendRequest.make({
            threadId: request.submission.threadId,
            producerEpoch: request.producerEpoch,
            expectedTailSequence: history.tailSequence,
            expectedTailDigest: history.tailDigest,
            batch: CanonicalBatch.make({
              batchId: Schema.decodeSync(BatchId)(id),
              producerId: options.producerId,
              records: [
                RecordEnvelope.make({
                  recordId: Schema.decodeSync(RecordId)(id),
                  family: "thread",
                  schemaVersion: 1,
                  createdAt: DateTime.makeUnsafe(yield* Clock.currentTimeMillis),
                  deploymentId: options.deploymentId,
                  payload,
                }),
              ],
            }),
          }),
        )
        .pipe(
          Effect.as(true),
          Effect.catchTag("AppendConflict", () => Effect.succeed(false)),
          Effect.mapError(storage),
        );

      if (!appended) continue;
      yield* failpoint.hit("update:after-canonical-append");
      yield* insert(payload);

      return update;
    }

    return yield* storage();
  });

  return { emit, repair };
});

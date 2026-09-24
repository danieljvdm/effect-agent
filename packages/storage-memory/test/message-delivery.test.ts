import { memoryMessageDeliveryStoreLayer } from "@effect-agent/storage-memory/memory-message-delivery-store";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Crypto, DateTime, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { digestJson } from "effect-agent/digest";
import { Receipt } from "effect-agent/durable-agent-runtime";
import { AgentId, ReceiptId, SettlementId, SubmissionId, ThreadId } from "effect-agent/identifiers";
import {
  MessageDeliveryDriver,
  MessageDeliveryStore,
  prepareMessageDelivery,
  type MessageDeliveryKey,
  type MessageDeliveryPolicy,
  type MessageDeliveryStoreLimits,
} from "effect-agent/message-delivery";
import { PreparedInputAdmission } from "effect-agent/prepared-input-admission";
import { DefinitionDigests, Digest } from "effect-agent/records";
import { ScheduledInputRetryable } from "effect-agent/schedule";
import {
  IdempotencyKey,
  Principal,
  QueueSequence,
  Settlement,
} from "effect-agent/submission-ledger";
import { SettledSubmission } from "effect-agent/submission-status";
import { PreparedInput } from "effect-agent/subscription";
import { TestClock } from "effect/testing";

const ownerThreadId = Schema.decodeSync(ThreadId)("sender");
const threadId = Schema.decodeSync(ThreadId)("receiver");
const digest = Schema.decodeSync(Digest)("a".repeat(64));

const policy: MessageDeliveryPolicy = {
  maxAutomaticAttempts: 2,
  attemptTimeoutMillis: 100,
  retryBaseMillis: 10,
  retryMaxMillis: 20,
  settlementPollMillis: 10,
};

const key = (name: string): MessageDeliveryKey => ({
  ownerThreadId,
  messageId: Schema.decodeSync(IdempotencyKey)(name),
});

const receipt = (envelope: PreparedInput) =>
  Receipt.make({
    threadId: envelope.threadId,
    receiptId: Schema.decodeSync(ReceiptId)(`receipt:${envelope.admissionKey}`),
    submissionId: Schema.decodeSync(SubmissionId)(`submission:${envelope.admissionKey}`),
    queueSequence: Schema.decodeSync(QueueSequence)(1),
  });

const settlement = (receipt: Receipt) =>
  Settlement.make({
    receiptId: receipt.receiptId,
    submissionId: receipt.submissionId,
    settlementId: Schema.decodeSync(SettlementId)(`settlement:${receipt.submissionId}`),
    outcome: "completed",
    settledAt: DateTime.makeUnsafe(10),
  });

const initial = Effect.fn("test.initial")(function* (name = "message", text = "hello") {
  const input = { text };

  return yield* prepareMessageDelivery({
    key: key(name),
    createdAtMillis: 0,
    deadlineAtMillis: 1_000,
    policy,
    envelope: {
      schemaVersion: 1,
      threadId,
      admissionKey: Schema.decodeSync(IdempotencyKey)(`admit:${name}`),
      deliveryPrincipal: Schema.decodeSync(Principal)("principal"),
      agentId: Schema.decodeSync(AgentId)("agent"),
      definitions: DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
      input,
      inputDigest: yield* digestJson(input),
      authorization: { policyId: "policy", decisionId: "decision" },
    },
  });
});

const dependencies = (
  admission: PreparedInputAdmission["Service"],
  limits?: MessageDeliveryStoreLimits,
) =>
  Layer.mergeAll(
    memoryMessageDeliveryStoreLayer(limits),
    NodeCrypto.layer,
    Layer.succeed(PreparedInputAdmission, admission),
  );

const layer = (
  admission: PreparedInputAdmission["Service"] = {
    submit: (envelope) => Effect.succeed(receipt(envelope)),
    submissionStatus: (receipt) =>
      Effect.succeed(new SettledSubmission({ settlement: settlement(receipt) })),
  },
  limits?: MessageDeliveryStoreLimits,
) => MessageDeliveryDriver.layer().pipe(Layer.provideMerge(dependencies(admission, limits)));

describe("direct message delivery", () => {
  it.effect("admits predecessor before successor after a lost acknowledgement", () => {
    const delivered: string[] = [];
    let firstAttempts = 0;

    return Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const driver = yield* MessageDeliveryDriver;
      const first = yield* initial("first");
      const second = { ...(yield* initial("second")), predecessor: first.key.messageId };

      yield* store.insert(first);
      yield* store.insert(second);
      expect((yield* driver.process(second.key)).retry.automaticAttempts).toBe(0);
      expect(delivered).toEqual([]);
      yield* driver.process(first.key);
      yield* TestClock.adjust(10);
      yield* driver.process(second.key);
      expect(delivered).toEqual(["admit:first"]);
      yield* driver.process(first.key);
      yield* TestClock.adjust(10);
      yield* driver.process(second.key);
      expect(delivered).toEqual(["admit:first", "admit:first", "admit:second"]);
      expect((yield* store.get(second.key))?.retry.attempts).toBe(1);
    }).pipe(
      Effect.provide(
        layer({
          submit: (envelope) =>
            Effect.suspend(() => {
              delivered.push(envelope.admissionKey);
              if (envelope.admissionKey === "admit:first" && firstAttempts++ === 0)
                return ScheduledInputRetryable.make({ reason: "ambiguous" });

              return Effect.succeed(receipt(envelope));
            }),
        }),
      ),
    );
  });

  it.effect(
    "snapshots every preparation choice before asynchronous hashing can observe caller mutation",
    () =>
      Effect.gen(function* () {
        const crypto = yield* Crypto.Crypto;
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const base = yield* initial();
        const input = { text: "hello", nested: { value: "original" } };

        const options = {
          key: { ...base.key },
          createdAtMillis: 0,
          deadlineAtMillis: 1_000,
          policy: { ...policy },
          envelope: {
            ...base.envelope,
            authorization: { ...base.envelope.authorization },
            input,
            inputDigest: yield* digestJson(input),
          },
        };

        const originalEnvelope = Schema.decodeSync(Schema.fromJsonString(PreparedInput))(
          Schema.encodeSync(Schema.fromJsonString(PreparedInput))(options.envelope),
        );

        const originalEnvelopeDigest = yield* digestJson(
          Schema.encodeSync(PreparedInput)(originalEnvelope),
        );

        let calls = 0;

        const blockedCrypto: Crypto.Crypto = {
          ...crypto,
          digest: (algorithm, bytes) =>
            Effect.gen(function* () {
              calls += 1;
              if (calls === 1) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }

              return yield* crypto.digest(algorithm, bytes);
            }),
        };

        const preparing = yield* Effect.forkChild(
          prepareMessageDelivery(options).pipe(Effect.provideService(Crypto.Crypto, blockedCrypto)),
        );

        yield* Deferred.await(entered);
        input.nested.value = "mutated";
        options.key.ownerThreadId = threadId;
        options.createdAtMillis = 900;
        options.deadlineAtMillis = 2_000;
        options.policy.maxAutomaticAttempts = 90;
        options.envelope.authorization.decisionId = "mutated";
        options.envelope.threadId = ownerThreadId;
        yield* Deferred.succeed(release, undefined);

        const prepared = yield* Fiber.join(preparing);

        expect(prepared).toMatchObject({
          key: base.key,
          createdAtMillis: 0,
          deadlineAtMillis: 1_000,
          initialDeadlineAtMillis: 1_000,
          policy,
        });
        expect(prepared.envelope).toEqual(originalEnvelope);
        expect(prepared.envelope.inputDigest).toBe(yield* digestJson(prepared.envelope.input));
        expect(prepared.envelopeDigest).toBe(originalEnvelopeDigest);
      }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.effect(
    "rejects an oversized processed record atomically while preserving the accepted Receipt",
    () =>
      Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const record = yield* initial();

        yield* store.insert(record);

        const claim = yield* store.change(record.key, {
          _tag: "Claim",
          expectedVersion: record.version,
          nowMillis: 0,
        });

        const accepted = yield* store.change(record.key, {
          _tag: "Accept",
          expectedVersion: claim.version,
          nowMillis: 0,
          receipt: receipt(record.envelope),
        });

        const observing = yield* store.change(record.key, {
          _tag: "Claim",
          expectedVersion: accepted.version,
          nowMillis: 10,
        });

        const failure = yield* store
          .change(record.key, {
            _tag: "Process",
            expectedVersion: observing.version,
            nowMillis: 10,
            settlement: Settlement.make({
              ...settlement(receipt(record.envelope)),
              runDisposition: { detail: "x".repeat(8_192) },
            }),
          })
          .pipe(Effect.flip);

        expect(failure).toMatchObject({ reason: "capacity", operation: "stored-value-bytes" });
        expect(yield* store.get(record.key)).toEqual(observing);
        expect(
          (yield* store.change(record.key, {
            _tag: "Process",
            expectedVersion: observing.version,
            nowMillis: 10,
            settlement: settlement(receipt(record.envelope)),
          })).status,
        ).toBe("processed");
        expect(yield* store.due(0.5, 1).pipe(Effect.flip)).toMatchObject({
          reason: "validation",
          operation: "due",
        });
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            NodeCrypto.layer,
            memoryMessageDeliveryStoreLayer(undefined, { maxStoredValueBytes: 4_096 }),
          ),
        ),
      ),
  );

  it.effect("bounds failed status lookups while retaining the accepted Receipt", () =>
    Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const driver = yield* MessageDeliveryDriver;
      const record = yield* initial();

      yield* store.insert(record);
      yield* driver.process(record.key);
      yield* TestClock.adjust(10);
      expect((yield* driver.process(record.key)).status).toBe("accepted");
      yield* TestClock.adjust(10);

      const parked = yield* driver.process(record.key);

      expect(parked).toMatchObject({
        status: "parked",
        parkReason: "exhausted",
        receipt: receipt(record.envelope),
        settlement: null,
        lastFailureDiagnostic: {
          _tag: "Error",
          errorTag: "ScheduledInputRetryable",
          cause: { _tag: "Error", message: "Status transport closed", code: "ECONNRESET" },
        },
      });
    }).pipe(
      Effect.provide(
        layer({
          submit: (envelope) => Effect.succeed(receipt(envelope)),
          submissionStatus: () =>
            Effect.fail(
              ScheduledInputRetryable.make({
                reason: "transport",
                cause: Object.assign(new Error("Status transport closed"), { code: "ECONNRESET" }),
              }),
            ),
        }),
      ),
    ),
  );

  for (const stop of ["timeout", "interrupt"]) {
    it.effect(
      `releases admission resources on ${stop} and recovers bounded persisted attempts`,
      () =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          let finalized = 0;

          const admission: PreparedInputAdmission["Service"] = {
            submit: () =>
              Effect.scoped(
                Effect.gen(function* () {
                  yield* Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
                    Effect.sync(() => {
                      finalized += 1;
                    }),
                  );

                  return yield* Effect.never;
                }),
              ),
          };

          yield* Effect.gen(function* () {
            const store = yield* MessageDeliveryStore;
            const driver = yield* MessageDeliveryDriver;
            const record = yield* initial();

            yield* store.insert(record);
            const running = yield* Effect.forkChild(driver.process(record.key));

            yield* Deferred.await(entered);
            if (stop === "interrupt") yield* Fiber.interrupt(running);
            if (stop === "timeout") yield* TestClock.adjust(100);
            const exit = yield* Fiber.await(running);

            expect(Exit.isSuccess(exit)).toBe(stop === "timeout");
            expect(finalized).toBe(1);
            const stored = yield* store.get(record.key);

            expect(stored?.retry.attempts).toBe(1);
            if (stop !== "timeout") {
              yield* TestClock.adjust(100);
              expect(yield* store.due(100, 1)).toEqual([record.key]);
            }
          }).pipe(Effect.provide(layer(admission)));
        }),
    );
  }

  it.effect("does not double-dispatch concurrent claims and bounds automatic deadlines", () =>
    Effect.gen(function* () {
      let calls = 0;

      yield* Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const driver = yield* MessageDeliveryDriver;
        const record = yield* initial();

        yield* store.insert(record);
        yield* Effect.all([driver.process(record.key), driver.process(record.key)], {
          concurrency: 2,
        });
        expect(calls).toBe(1);
        yield* TestClock.adjust(1_000);
        expect((yield* driver.process(record.key)).parkReason).toBe("deadline");
      }).pipe(
        Effect.provide(
          layer({
            submit: (envelope) =>
              Effect.sync(() => {
                calls += 1;

                return receipt(envelope);
              }),
          }),
        ),
      );
    }),
  );
});

import {
  AgentId,
  ReceiptId,
  SettlementId,
  SubmissionId,
  ThreadId,
} from "@effect-agent/core/Identifiers";
import { memoryMessageDeliveryStoreLayer } from "@effect-agent/storage-memory/MemoryMessageDeliveryStore";
import { digestJson } from "@effect-agent/thread/Digest";
import { Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import {
  defaultMessageDeliveryStoreLimits,
  MessageDeliveryDriver,
  MessageDeliveryFailpoint,
  MessageDeliveryFailpointError,
  MessageDeliveryStore,
  prepareMessageDelivery,
  type MessageDeliveryChange,
  type MessageDeliveryKey,
  type MessageDeliveryPolicy,
  type MessageDeliveryStoreLimits,
} from "@effect-agent/thread/MessageDelivery";
import { PreparedInputAdmission } from "@effect-agent/thread/PreparedInputAdmission";
import { DefinitionDigests, Digest } from "@effect-agent/thread/Records";
import { ScheduledInputRefused, ScheduledInputRetryable } from "@effect-agent/thread/Schedule";
import {
  IdempotencyKey,
  Principal,
  QueueSequence,
  Settlement,
} from "@effect-agent/thread/SubmissionLedger";
import { PendingSubmission, SettledSubmission } from "@effect-agent/thread/SubmissionStatus";
import { PreparedInput } from "@effect-agent/thread/Subscription";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Crypto, DateTime, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
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

        expect(calls).toBe(2);
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

  it.effect(
    "rediscover obligations without either ledger, retain acceptance separately, and observe canonical processing",
    () =>
      Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const driver = yield* MessageDeliveryDriver;
        const record = yield* initial();

        yield* store.insert(record);
        expect(yield* store.due(0, 10)).toEqual([key("message")]);
        expect((yield* driver.runDue())[0]?.status).toBe("accepted");
        const accepted = yield* store.get(record.key);

        expect(accepted?.settlement).toBeNull();
        yield* TestClock.adjust(10);
        expect((yield* driver.runDue())[0]?.status).toBe("processed");
        expect(yield* store.nextDeadline()).toBeNull();
        const replayed = yield* store.insert(record);

        expect(replayed.status).toBe("processed");
        expect(replayed.settlement?.outcome).toBe("completed");
      }).pipe(Effect.provide(layer())),
  );

  it.effect(
    "retries an acknowledgement lost after admission with the identical full envelope and key",
    () => {
      const delivered: PreparedInput[] = [];

      return Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const driver = yield* MessageDeliveryDriver;
        const record = yield* initial();

        yield* store.insert(record);
        expect((yield* driver.process(record.key)).status).toBe("pending");
        yield* TestClock.adjust(10);
        expect((yield* driver.process(record.key)).status).toBe("accepted");
        expect(delivered).toEqual([record.envelope, record.envelope]);
      }).pipe(
        Effect.provide(
          layer({
            submit: (envelope) =>
              Effect.suspend(() => {
                delivered.push(envelope);

                return delivered.length === 1
                  ? Effect.fail(ScheduledInputRetryable.make({ reason: "ambiguous" }))
                  : Effect.succeed(receipt(envelope));
              }),
          }),
        ),
      );
    },
  );

  it.effect("freezes nested input and rejects a conflicting duplicate without mutation", () =>
    Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const first = yield* initial();

      yield* store.insert(first);

      const snapshot = yield* store.get(first.key);

      Object.assign(first.envelope.input ?? {}, { text: "caller mutation" });

      expect(yield* store.get(first.key)).toEqual(snapshot);

      const different = yield* initial("message", "different");

      expect(yield* store.insert(different).pipe(Effect.flip)).toMatchObject({
        reason: "conflict",
      });
      expect(yield* store.get(first.key)).toEqual(snapshot);
      expect((yield* store.list({ ownerThreadId: threadId, limit: 1 })).items).toEqual([]);
      expect(yield* store.get({ ...first.key, ownerThreadId: threadId })).toBeNull();
    }).pipe(Effect.provide(layer())),
  );

  it.effect(
    "rejects a changed initial deadline or malformed input digest without storing work",
    () =>
      Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const record = yield* initial();

        yield* store.insert(record);
        expect(
          yield* store
            .insert({ ...record, initialDeadlineAtMillis: 2_000, deadlineAtMillis: 2_000 })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "conflict" });
        expect(
          yield* prepareMessageDelivery({
            key: key("bad"),
            envelope: { ...record.envelope, inputDigest: digest },
            createdAtMillis: 0,
            deadlineAtMillis: 1_000,
          }).pipe(Effect.flip),
        ).toMatchObject({ reason: "corrupt", operation: "input-digest" });
        expect(yield* store.get(key("bad"))).toBeNull();
      }).pipe(Effect.provide(layer())),
  );

  it.effect("applies envelope byte backpressure before insertion", () =>
    Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const record = yield* initial();

      expect(yield* store.insert(record).pipe(Effect.flip)).toMatchObject({
        reason: "capacity",
        operation: "envelope-bytes",
      });
      expect(yield* store.get(record.key)).toBeNull();
    }).pipe(
      Effect.provide(
        layer(undefined, { ...defaultMessageDeliveryStoreLimits, maxEnvelopeBytes: 10 }),
      ),
    ),
  );

  it.effect("recovers a lost local admission acknowledgement from the persisted claim", () => {
    let fail = true;
    const delivered: PreparedInput[] = [];

    return Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const driver = yield* MessageDeliveryDriver;
      const record = yield* initial();

      yield* store.insert(record);
      expect(yield* driver.process(record.key).pipe(Effect.flip)).toMatchObject({
        _tag: "MessageDeliveryFailpointError",
      });
      expect((yield* store.get(record.key))?.status).toBe("pending");
      fail = false;
      yield* TestClock.adjust(100);
      expect((yield* driver.runDue())[0]?.status).toBe("accepted");
      expect(delivered).toEqual([record.envelope, record.envelope]);
    }).pipe(
      Effect.provide(
        layer({
          submit: (envelope) =>
            Effect.sync(() => {
              delivered.push(envelope);

              return receipt(envelope);
            }),
        }).pipe(
          Layer.provide(
            Layer.succeed(MessageDeliveryFailpoint, {
              hit: (point) =>
                Effect.suspend(() =>
                  fail && point === "message-delivery:admission:after"
                    ? Effect.fail(MessageDeliveryFailpointError.make({ point }))
                    : Effect.void,
                ),
            }),
          ),
        ),
      ),
    );
  });

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
      });
    }).pipe(
      Effect.provide(
        layer({
          submit: (envelope) => Effect.succeed(receipt(envelope)),
          submissionStatus: () =>
            Effect.fail(ScheduledInputRetryable.make({ reason: "transport" })),
        }),
      ),
    ),
  );

  it.effect(
    "parks exhausted retries and explicitly recovers the exact envelope under a new generation",
    () =>
      Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const driver = yield* MessageDeliveryDriver;
        const record = yield* initial();

        yield* store.insert(record);
        yield* driver.process(record.key);
        yield* TestClock.adjust(10);
        const parked = yield* driver.process(record.key);

        expect(parked).toMatchObject({
          status: "parked",
          parkReason: "exhausted",
          retry: { attempts: 2, automaticAttempts: 2 },
        });
        expect(yield* store.nextDeadline()).toBeNull();
        const recovered = yield* driver.retry(record.key, parked.version, 2_000);

        expect(recovered).toMatchObject({
          status: "pending",
          envelope: record.envelope,
          envelopeDigest: record.envelopeDigest,
          retry: { generation: 1, automaticAttempts: 0 },
        });
        expect(
          yield* driver.retry(record.key, parked.version, 2_000).pipe(Effect.flip),
        ).toMatchObject({ reason: "conflict" });
      }).pipe(
        Effect.provide(
          layer({
            submit: () => Effect.fail(ScheduledInputRetryable.make({ reason: "capacity" })),
          }),
        ),
      ),
  );

  it.effect("records conclusive refusal and never rewinds a terminal outcome", () =>
    Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const driver = yield* MessageDeliveryDriver;
      const record = yield* initial();

      yield* store.insert(record);
      const refused = yield* driver.process(record.key);

      expect(refused).toMatchObject({ status: "refused", refusal: "denied", receipt: null });
      expect(
        yield* driver.retry(record.key, refused.version, 2_000).pipe(Effect.flip),
      ).toMatchObject({ reason: "conflict" });
      expect(yield* store.nextDeadline()).toBeNull();
    }).pipe(
      Effect.provide(
        layer({ submit: () => Effect.fail(ScheduledInputRefused.make({ code: "denied" })) }),
      ),
    ),
  );

  it.effect("keeps healthy pending observations accepted until the absolute deadline", () =>
    Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const driver = yield* MessageDeliveryDriver;
      const record = yield* initial();

      yield* store.insert(record);
      yield* driver.process(record.key);
      yield* TestClock.adjust(10);
      expect((yield* driver.process(record.key)).status).toBe("accepted");
      for (let poll = 0; poll < 4; poll += 1) {
        yield* TestClock.adjust(10);
        const observed = yield* driver.process(record.key);

        expect(observed.status).toBe("accepted");
        expect(observed.retry.automaticAttempts).toBe(0);
      }
      yield* TestClock.adjust(950);
      const parked = yield* driver.process(record.key);

      expect(parked).toMatchObject({ status: "parked", parkReason: "deadline" });
      expect(parked.receipt).toEqual(receipt(record.envelope));
      expect(parked.settlement).toBeNull();
      expect((yield* driver.retry(record.key, parked.version, 2_000)).status).toBe("accepted");
    }).pipe(
      Effect.provide(
        layer({
          submit: (envelope) => Effect.succeed(receipt(envelope)),
          submissionStatus: () => Effect.succeed(new PendingSubmission()),
        }),
      ),
    ),
  );

  it.effect(
    "bounds pending and retained rows separately, and keeps idempotent replay available at capacity",
    () =>
      Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const driver = yield* MessageDeliveryDriver;
        const a = yield* initial("a");
        const b = yield* initial("b");

        yield* store.insert(a);
        expect(yield* store.insert(b).pipe(Effect.flip)).toMatchObject({ reason: "capacity" });
        yield* driver.process(a.key);
        yield* store.insert(b);
        yield* driver.process(b.key);
        expect(yield* store.insert(yield* initial("c")).pipe(Effect.flip)).toMatchObject({
          reason: "capacity",
        });
        expect((yield* store.insert(a)).status).toBe("refused");
        const page = yield* store.list({ ownerThreadId, limit: 1 });

        expect(page.next).toBe("a");
        expect(
          (yield* store.list({ ownerThreadId, limit: 1, after: page.next ?? undefined })).items[0]
            ?.key.messageId,
        ).toBe("b");
      }).pipe(
        Effect.provide(
          layer(
            { submit: () => Effect.fail(ScheduledInputRefused.make({ code: "denied" })) },
            { ...defaultMessageDeliveryStoreLimits, maxPendingPerOwner: 1, maxRetainedPerOwner: 2 },
          ),
        ),
      ),
  );

  it.effect("fences stale completion after an expired claim is reclaimed", () =>
    Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const record = yield* initial();

      yield* store.insert(record);

      const first = yield* store.change(record.key, {
        _tag: "Claim",
        expectedVersion: 1,
        nowMillis: 0,
      });

      expect(yield* store.due(99, 1)).toEqual([]);

      const second = yield* store.change(record.key, {
        _tag: "Claim",
        expectedVersion: first.version,
        nowMillis: 100,
      });

      expect(
        yield* store
          .change(record.key, {
            _tag: "Accept",
            expectedVersion: first.version,
            nowMillis: 100,
            receipt: receipt(record.envelope),
          })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "conflict" });
      expect((yield* store.get(record.key))?.version).toBe(second.version);
    }).pipe(Effect.provide(layer())),
  );

  for (const stop of ["timeout", "interrupt", "defect"] as const) {
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

                  return yield* stop === "defect" ? Effect.die("failure") : Effect.never;
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

  for (const phase of ["before", "after"] as const) {
    for (const tag of [
      "insert",
      "Claim",
      "Accept",
      "Process",
      "ObservePending",
      "Refuse",
      "Retry",
      "Park",
      "Recover",
    ] as const) {
      it.effect(`${tag} ${phase} failpoint leaves an atomic, recoverable mutation`, () => {
        let selected = "";

        return Effect.gen(function* () {
          const store = yield* MessageDeliveryStore;
          const record = yield* initial();

          if (tag !== "insert") yield* store.insert(record);
          let before = record;

          if (
            tag === "Accept" ||
            tag === "Process" ||
            tag === "ObservePending" ||
            tag === "Refuse" ||
            tag === "Retry"
          )
            before = yield* store.change(record.key, {
              _tag: "Claim",
              expectedVersion: before.version,
              nowMillis: 0,
            });
          if (tag === "Process" || tag === "ObservePending") {
            before = yield* store.change(record.key, {
              _tag: "Accept",
              expectedVersion: before.version,
              nowMillis: 0,
              receipt: receipt(record.envelope),
            });
            before = yield* store.change(record.key, {
              _tag: "Claim",
              expectedVersion: before.version,
              nowMillis: 10,
            });
          }
          if (tag === "Recover")
            before = yield* store.change(record.key, {
              _tag: "Park",
              expectedVersion: before.version,
              nowMillis: 0,
              reason: "deadline",
            });
          selected = `message-delivery:${tag.toLowerCase()}:${phase}`;

          const fence = {
            expectedVersion: before.version,
            nowMillis: tag === "Process" || tag === "ObservePending" ? 10 : 0,
          };

          const change: MessageDeliveryChange =
            tag === "Accept"
              ? { _tag: tag, ...fence, receipt: receipt(record.envelope) }
              : tag === "Process"
                ? { _tag: tag, ...fence, settlement: settlement(receipt(record.envelope)) }
                : tag === "ObservePending"
                  ? { _tag: tag, ...fence }
                  : tag === "Refuse"
                    ? { _tag: tag, ...fence, code: "denied" }
                    : tag === "Retry"
                      ? { _tag: tag, ...fence, reason: "transport" }
                      : tag === "Park"
                        ? { _tag: tag, ...fence, reason: "deadline" }
                        : tag === "Recover"
                          ? { _tag: tag, ...fence, deadlineAtMillis: 2_000 }
                          : { _tag: "Claim", ...fence };

          expect(
            yield* (
              tag === "insert" ? store.insert(record) : store.change(record.key, change)
            ).pipe(Effect.flip),
          ).toMatchObject({ _tag: "MessageDeliveryFailpointError", point: selected });
          const stored = yield* store.get(record.key);

          if (tag === "insert") expect(stored === null).toBe(phase === "before");
          else expect(stored?.version).toBe(before.version + (phase === "after" ? 1 : 0));
        }).pipe(
          Effect.provide(
            layer().pipe(
              Layer.provide(
                Layer.succeed(MessageDeliveryFailpoint, {
                  hit: (point) =>
                    Effect.suspend(() =>
                      point === selected
                        ? Effect.fail(MessageDeliveryFailpointError.make({ point }))
                        : Effect.void,
                    ),
                }),
              ),
            ),
          ),
        );
      });
    }
  }
});

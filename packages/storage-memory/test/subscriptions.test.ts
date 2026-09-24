import { memorySubscriptionStoreLayer } from "@effect-agent/storage-memory/memory-subscription-store";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { DateTime, Cause, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { digestJson } from "effect-agent/digest";
import { Receipt } from "effect-agent/durable-agent-runtime";
import { EventSources, makeEventSource } from "effect-agent/event-source";
import { AgentId, ThreadId, ReceiptId, SubmissionId } from "effect-agent/identifiers";
import { PreparedInputAdmission } from "effect-agent/prepared-input-admission";
import { DefinitionDigests, Digest } from "effect-agent/records";
import { ScheduledInputRefused, ScheduledInputRetryable } from "effect-agent/schedule";
import {
  Settlement,
  submissionSettlementId,
  Principal,
  QueueSequence,
} from "effect-agent/submission-ledger";
import { SettledSubmission } from "effect-agent/submission-status";
import {
  SubscriptionAuthorizer,
  SubscriptionError,
  SubscriptionFailpoint,
  SubscriptionFailpointError,
  SubscriptionSourceError,
  SubscriptionStore,
  defaultSubscriptionLimits,
  type PreparedInput,
  type SubscriptionLimits,
} from "effect-agent/subscription";
import {
  makeSubscriptionInputBinding,
  SubscriptionInputBindings,
  type SubscriptionInputBinding,
} from "effect-agent/subscription-input";
import {
  SubscriptionDriver,
  SubscriptionIntake,
  Subscriptions,
  type SubscribeOptions,
} from "effect-agent/subscriptions";
import { TestClock } from "effect/testing";

const partition = { tenantId: "tenant", address: "repository:42" };
const principal = Schema.decodeSync(Principal)("manager");
const scope = { partition, ownerId: "owner", principal };
const source = { name: "trusted", version: "1" };
const agentId = Schema.decodeSync(AgentId)("subscription-agent");
const threadId = Schema.decodeSync(ThreadId)("subscription-thread");
const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const Event = Schema.Struct({
  id: Schema.String,
  key: Schema.String,
  text: Schema.String,
  occurredAtMillis: Schema.optionalKey(Schema.Number),
});

type Event = typeof Event.Type;
const Input = Schema.Struct({ text: Schema.String });

const limits: SubscriptionLimits = {
  ...defaultSubscriptionLimits,
  batchSize: 2,
  concurrency: 2,
  retryMillis: 10,
  operationTimeoutMillis: 1_000,
};

const options = (
  subscriptionId: string,
  mode: "once" | "continuous" = "once",
): SubscribeOptions => ({
  subscriptionId,
  source,
  parameters: { key: "matched" },
  context: { text: "private-continuation" },
  mode,
  expiresAtMillis: 100_000,
  destination: { _tag: "ExistingThread", threadId },
  deliveryPrincipal: principal,
  agentId,
  definitions,
});

const event = (id: string): Event => ({ id, key: "matched", text: id });

const key = (subscriptionId = "watch", eventId = "completion") => ({
  subscription: { partition, ownerId: scope.ownerId, subscriptionId },
  eventId,
});

const receipt = (input: PreparedInput) =>
  Receipt.make({
    threadId: input.threadId,
    receiptId: Schema.decodeSync(ReceiptId)(`receipt:${input.admissionKey}`),
    submissionId: Schema.decodeSync(SubmissionId)(`submission:${input.admissionKey}`),
    queueSequence: Schema.decodeSync(QueueSequence)(1),
  });

interface Scenario {
  readonly prepare?: (event: Event) => Effect.Effect<typeof Input.Type, SubscriptionSourceError>;
  readonly reconcile?: () => Effect.Effect<Event | null, SubscriptionSourceError>;
  readonly submit?: PreparedInputAdmission["Service"]["submit"];
  readonly submissionStatus?: NonNullable<PreparedInputAdmission["Service"]["submissionStatus"]>;
  readonly authorize?: SubscriptionAuthorizer["Service"]["prepare"];
  readonly authorizeIntake?: SubscriptionAuthorizer["Service"]["intake"];
  readonly authorizeReconcile?: SubscriptionAuthorizer["Service"]["reconcile"];
  readonly bindings?: ReadonlyArray<SubscriptionInputBinding>;
  readonly failpoint?: {
    readonly hit: (point: string) => Effect.Effect<void, SubscriptionFailpointError>;
  };
  readonly limits?: SubscriptionLimits;
}

const layer = (scenario: Scenario = {}) => {
  const configured = scenario.limits ?? limits;

  const catalog = Layer.effect(
    EventSources,
    makeEventSource({
      source,
      continuity: "Trusted caller registers before intake.",
      event: Event,
      parameters: Schema.Struct({ key: Schema.String }),
      identity: (e) => e.id,
      occurredAtMillis: (e) => e.occurredAtMillis,
      eventKey: (e) => e.key,
      parameterKey: (p) => p.key,
      matches: (e, p) => e.key === p.key,
      ...(scenario.reconcile === undefined ? {} : { reconcile: scenario.reconcile }),
    }).pipe(Effect.map((value) => ({ sources: [value] }))),
  );

  const dependencies = Layer.mergeAll(
    NodeCrypto.layer,
    catalog,
    Layer.effect(
      SubscriptionInputBindings,
      makeSubscriptionInputBinding({
        source,
        agentId,
        definitions,
        event: Event,
        parameters: Schema.Struct({ key: Schema.String }),
        context: Schema.Struct({ text: Schema.String }),
        input: Input,
        prepare: (e) => scenario.prepare?.(e) ?? Effect.succeed({ text: e.text }),
      }).pipe(Effect.map((binding) => ({ bindings: scenario.bindings ?? [binding] }))),
    ),
    Layer.succeed(SubscriptionAuthorizer, {
      manage: () => Effect.void,
      intake: scenario.authorizeIntake ?? (() => Effect.void),
      reconcile: scenario.authorizeReconcile ?? (() => Effect.void),
      prepare:
        scenario.authorize ??
        (() => Effect.succeed({ policyId: "policy", decisionId: "decision" })),
    }),
    Layer.succeed(PreparedInputAdmission, {
      ...(scenario.submissionStatus === undefined
        ? {}
        : { submissionStatus: scenario.submissionStatus }),
      submit: scenario.submit ?? ((input) => Effect.succeed(receipt(input))),
    }),
  );

  const services = Layer.mergeAll(
    Subscriptions.layer(configured),
    SubscriptionIntake.layer(configured),
    SubscriptionDriver.layer(configured),
  ).pipe(
    Layer.provideMerge(memorySubscriptionStoreLayer(partition)),
    Layer.provideMerge(dependencies),
  );

  return scenario.failpoint === undefined
    ? services
    : services.pipe(Layer.provide(Layer.succeed(SubscriptionFailpoint, scenario.failpoint)));
};

const drain = Effect.fn("test.drainSubscriptions")(function* (passes = 8) {
  const driver = yield* SubscriptionDriver;

  for (let index = 0; index < passes; index += 1) yield* driver.runDue;
});

const registerAndAccept = Effect.gen(function* () {
  yield* (yield* Subscriptions).subscribe(scope, options("watch"));
  yield* (yield* SubscriptionIntake).accept(principal, source, event("completion"));
  yield* drain(1);
});

describe("Durable subscription delivery", () => {
  it.effect(
    "parks ambiguous admission and explicitly recovers the same envelope after cancellation",
    () => {
      const attempts: Array<PreparedInput> = [];
      let recovered = false;

      return Effect.gen(function* () {
        yield* registerAndAccept;
        for (let attempt = 0; attempt < 3; attempt++) {
          yield* drain();
          yield* TestClock.adjust(10);
        }
        const store = yield* SubscriptionStore;

        expect((yield* store.delivery(key()))?.retry).toMatchObject({ attempts: 3, parked: true });
        yield* TestClock.adjust(1000);
        yield* drain();
        expect(attempts).toHaveLength(3);
        yield* (yield* Subscriptions).cancelSubscription(scope, key().subscription, 1);
        recovered = true;
        yield* (yield* Subscriptions).recoverDelivery(scope, key(), 0);
        yield* drain();
        expect((yield* store.delivery(key()))?.state).toBe("delivered");
        expect(attempts).toHaveLength(4);
        expect(
          attempts.every((envelope) => envelope.admissionKey === attempts[0]?.admissionKey),
        ).toBe(true);
      }).pipe(
        Effect.provide(
          layer({
            limits: { ...limits, maxAutomaticAttempts: 3 },
            submit: (input) =>
              Effect.suspend(() => {
                attempts.push(input);

                return recovered
                  ? Effect.succeed(receipt(input))
                  : ScheduledInputRetryable.make({ reason: "ambiguous" });
              }),
          }),
        ),
      );
    },
  );

  // Keep the historical >1,000-delivery regression through the complete driver here.
  // Shared store conformance uses short quota cycles for each adapter; this also exercises
  // intake, routing, settlement observation and maintenance together throughout the run.
  it.effect(
    "sustains over 1000 distinct events with bounded retention and rejects expired replay",
    () => {
      let admitted = 0;

      return Effect.gen(function* () {
        yield* (yield* Subscriptions).subscribe(scope, options("watch", "continuous"));
        const intake = yield* SubscriptionIntake;
        const store = yield* SubscriptionStore;

        for (let index = 0; index < 1005; index++) {
          const payload = { ...event(`event-${index}`), occurredAtMillis: index * 20 };

          yield* intake.accept(principal, source, payload);
          yield* drain(4);
          yield* intake.accept(principal, source, payload);
          expect(
            (yield* (yield* Subscriptions).listDeliveries(scope, key().subscription)).items.length,
          ).toBeLessThanOrEqual(4);
          yield* TestClock.adjust(20);
        }
        expect(admitted).toBe(1005);
        expect(yield* store.event("event-0")).toBeNull();
        expect(
          yield* intake
            .accept(principal, source, { ...event("event-0"), occurredAtMillis: 0 })
            .pipe(Effect.flip),
        ).toMatchObject({ code: "event-replay-horizon" });
        expect(yield* store.nextDeadline).not.toBeNull();
      }).pipe(
        Effect.provide(
          layer({
            limits: {
              ...limits,
              maxEvents: 4,
              maxDeliveries: 4,
              maxDeliveriesPerOwner: 4,
              retention: {
                replayHorizonMillis: 100,
                completedRetentionMillis: 0,
                maxTombstones: 8,
              },
            },
            submissionStatus: (receipt) =>
              Effect.succeed(
                SettledSubmission.make({
                  settlement: Settlement.make({
                    submissionId: receipt.submissionId,
                    receiptId: receipt.receiptId,
                    settlementId: submissionSettlementId(receipt.submissionId),
                    outcome: "completed",
                    settledAt: DateTime.makeUnsafe(0),
                  }),
                }),
              ),
            submit: (input) =>
              Effect.sync(() => {
                admitted++;

                return receipt(input);
              }),
          }),
        ),
      );
    },
    { timeout: 30_000 },
  );

  it.effect("defers failed settlement probes while retaining admitted evidence", () => {
    let probes = 0;

    return Effect.gen(function* () {
      yield* (yield* Subscriptions).subscribe(scope, options("watch"));
      yield* (yield* SubscriptionIntake).accept(principal, source, {
        ...event("completion"),
        occurredAtMillis: 0,
      });
      yield* drain();
      expect(probes).toBe(1);
      expect(yield* (yield* SubscriptionStore).delivery(key())).toMatchObject({
        state: "delivered",
        observeSettlement: true,
        retry: { generation: 0, automaticAttempts: 0, parked: false, nextAttemptAtMillis: 10 },
      });
      yield* drain();
      expect(probes).toBe(1);
      yield* TestClock.adjust(10);
      yield* drain();
      expect(probes).toBe(2);
    }).pipe(
      Effect.provide(
        layer({
          limits: {
            ...limits,
            retention: { replayHorizonMillis: 100, completedRetentionMillis: 0, maxTombstones: 8 },
          },
          submissionStatus: () =>
            Effect.suspend(() => {
              probes++;

              return ScheduledInputRetryable.make({ reason: "transport" });
            }),
        }),
      ),
    );
  });

  it.effect("keeps selected work pending when its exact preparation binding is unavailable", () =>
    Effect.gen(function* () {
      let hold = true;

      yield* Effect.gen(function* () {
        yield* registerAndAccept;
        expect((yield* (yield* SubscriptionStore).delivery(key()))?.state).toBe("selected");
        hold = false;
        const original = (yield* SubscriptionInputBindings).bindings[0];

        if (original === undefined) return yield* Effect.die("Expected preparation binding");

        const changed = {
          ...original,
          definitions: { ...definitions, agent: Schema.decodeSync(Digest)("b".repeat(64)) },
        };

        for (const bindings of [[], [changed], [original, original]]) {
          yield* Effect.gen(function* () {
            const driver = yield* SubscriptionDriver;
            const rejected = yield* driver.processDelivery(key()).pipe(Effect.flip);

            expect(rejected).toMatchObject({
              reason: "unsupported-binding",
              code: "input-binding",
            });
          }).pipe(
            Effect.provide(
              SubscriptionDriver.layer(limits).pipe(
                Layer.provide(Layer.succeed(SubscriptionInputBindings, { bindings })),
              ),
            ),
          );
        }
        yield* (yield* SubscriptionDriver).processDelivery(key());
        expect((yield* (yield* SubscriptionStore).delivery(key()))?.state).toBe("delivered");
      }).pipe(
        Effect.provide(
          layer({
            failpoint: {
              hit: (point) =>
                hold && point === "subscription:delivery-prepare:before"
                  ? SubscriptionFailpointError.make({ point })
                  : Effect.void,
            },
          }),
        ),
      );
    }),
  );

  it.effect("authorizes recovery independently of registration and webhook ingress", () => {
    const webhook = Schema.decodeSync(Principal)("webhook");
    let polls = 0;

    return Effect.gen(function* () {
      const subscriptions = yield* Subscriptions;

      yield* subscriptions.subscribe(scope, options("watch"));
      yield* subscriptions.subscribe(scope, {
        ...options("denied"),
        parameters: { key: "denied" },
      });

      const rejected = yield* (yield* SubscriptionIntake)
        .accept(principal, source, event("completion"))
        .pipe(Effect.flip);

      expect(rejected).toMatchObject({ reason: "unauthorized" });
      yield* drain();
      const store = yield* SubscriptionStore;

      expect((yield* store.delivery(key()))?.state).toBe("delivered");
      expect((yield* store.get(key("denied").subscription))?.recovery).toMatchObject({
        nextAttemptAtMillis: null,
        lastFailure: "unauthorized",
      });
      expect(polls).toBe(1);
      yield* (yield* SubscriptionIntake).accept(webhook, source, event("completion"));
    }).pipe(
      Effect.provide(
        layer({
          authorizeIntake: (_partition, _source, caller) =>
            caller === webhook
              ? Effect.void
              : SubscriptionError.make({ reason: "unauthorized", code: "webhook-only" }),
          authorizeReconcile: (subscription) =>
            subscription.key.subscriptionId === "denied"
              ? SubscriptionError.make({ reason: "unauthorized", code: "recovery-denied" })
              : Effect.void,
          reconcile: () =>
            Effect.sync(() => {
              polls++;

              return event("completion");
            }),
        }),
      ),
    );
  });

  it.effect("checks expiry after waiting at the atomic preparation boundary", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();

        yield* Effect.gen(function* () {
          yield* (yield* Subscriptions).subscribe(scope, {
            ...options("watch"),
            expiresAtMillis: 100,
          });
          yield* (yield* SubscriptionIntake).accept(principal, source, event("completion"));
          yield* drain(1);
          const store = yield* SubscriptionStore;
          const selected = yield* store.delivery(key());

          if (selected === null) return yield* Effect.die("Expected selected fixture");

          const committing = yield* store
            .changeDelivery(key(), selected.deliveryId, {
              _tag: "Prepare",
              nowMillis: 0,
              envelopeDigest: digest,
              envelope: {
                schemaVersion: 1,
                threadId,
                deliveryPrincipal: principal,
                agentId,
                definitions,
                input: { text: "completion" },
                inputDigest: digest,
                admissionKey: selected.admissionKey,
                authorization: { policyId: "policy", decisionId: "decision" },
              },
            })
            .pipe(Effect.forkScoped);

          yield* Deferred.await(started);
          yield* TestClock.adjust(101);
          yield* Deferred.succeed(release, undefined);
          expect((yield* Fiber.join(committing)).refusal?.code).toBe("expired");
        }).pipe(
          Effect.provide(
            layer({
              failpoint: {
                hit: (point) =>
                  point === "subscription:delivery-prepare:before"
                    ? Deferred.succeed(started, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      )
                    : Effect.void,
              },
            }),
          ),
        );
      }),
    ),
  );

  it.effect(
    "reconciles a later watch against one retained completion without reopening its cutoff",
    () => {
      let available = false;
      let reads = 0;

      return Effect.gen(function* () {
        const intake = yield* SubscriptionIntake;
        const store = yield* SubscriptionStore;

        yield* intake.accept(principal, source, event("completion"));
        yield* drain();
        const original = yield* store.event("completion");

        yield* (yield* Subscriptions).subscribe(scope, options("watch"));
        yield* drain();
        expect((yield* store.get(key().subscription))?.recovery?.lastFailure).toBe(
          "provider-unavailable",
        );
        available = true;
        yield* TestClock.adjust(10);
        yield* drain();
        expect((yield* store.delivery(key()))?.state).toBe("delivered");
        expect((yield* store.event("completion"))?.cutoff).toBe(original?.cutoff);
        expect((yield* store.event("completion"))?.routingComplete).toBe(true);
        yield* intake.accept(principal, source, event("completion"));
        yield* TestClock.adjust(100);
        yield* drain();
        expect(reads).toBe(2);
        expect(
          (yield* (yield* Subscriptions).listDeliveries(scope, key().subscription)).items,
        ).toHaveLength(1);
      }).pipe(
        Effect.provide(
          layer({
            reconcile: () =>
              Effect.suspend(() => {
                reads += 1;

                return available
                  ? Effect.succeed(event("completion"))
                  : SubscriptionSourceError.make({ code: "provider-unavailable", retryable: true });
              }),
          }),
        ),
      );
    },
  );

  for (const phase of ["admission"]) {
    it.effect(`keeps once consumption after conclusive ${phase} refusal`, () =>
      Effect.gen(function* () {
        yield* registerAndAccept;
        yield* drain();
        const store = yield* SubscriptionStore;

        expect((yield* store.delivery(key()))?.state).toBe("refused");
        expect((yield* store.get(key().subscription))?.state).toBe("consumed");
        yield* (yield* SubscriptionIntake).accept(principal, source, event("later"));
        yield* drain();
        expect(
          (yield* (yield* Subscriptions).listDeliveries(scope, key().subscription)).items,
        ).toHaveLength(1);
      }).pipe(
        Effect.provide(
          layer({
            submit: () => ScheduledInputRefused.make({ code: "proven-not-admitted" }),
          }),
        ),
      ),
    );
  }

  for (const point of [
    "subscription:select:after",
    "subscription:delivery-prepare:after",
    "subscription:admission:after",
  ]) {
    it.effect(`recovers one admission identity at ${point}`, () => {
      let armed = true;
      const admitted = new Map<string, Receipt>();

      return Effect.gen(function* () {
        yield* (yield* Subscriptions).subscribe(scope, options("watch"));
        const intake = yield* SubscriptionIntake;

        yield* intake.accept(principal, source, event("completion")).pipe(Effect.result);
        // A lost intake acknowledgement must converge on the original event/cutoff.
        yield* intake.accept(principal, source, event("completion"));
        yield* drain();
        yield* TestClock.adjust(10);
        yield* drain();
        expect(armed).toBe(false);
        expect(admitted.size).toBe(1);
        expect((yield* (yield* SubscriptionStore).delivery(key()))?.state).toBe("delivered");
      }).pipe(
        Effect.provide(
          layer({
            failpoint: {
              hit: (observed) =>
                Effect.suspend(() => {
                  if (armed && observed === point) {
                    armed = false;

                    return SubscriptionFailpointError.make({ point });
                  }

                  return Effect.void;
                }),
            },
            submit: (envelope) =>
              Effect.sync(() => {
                const existing = admitted.get(envelope.admissionKey);

                if (existing !== undefined) return existing;
                const accepted = receipt(envelope);

                admitted.set(envelope.admissionKey, accepted);

                return accepted;
              }),
          }),
        ),
      );
    });
  }

  it.effect(
    "retries the frozen envelope after admission loses its reply and policy is revoked",
    () => {
      const attempts: Array<PreparedInput> = [];
      let preparations = 0;
      let revoked = false;

      return Effect.gen(function* () {
        yield* registerAndAccept;
        yield* drain();
        const stored = yield* (yield* SubscriptionStore).delivery(key());

        expect(stored?.state).toBe("prepared");
        yield* (yield* Subscriptions).cancelSubscription(scope, key().subscription);
        revoked = true;
        yield* TestClock.adjust(10);
        yield* drain();
        expect((yield* (yield* SubscriptionStore).delivery(key()))?.state).toBe("delivered");
        expect(attempts).toHaveLength(2);
        expect(attempts[1]).toEqual(attempts[0]);
        expect(preparations).toBe(1);
      }).pipe(
        Effect.provide(
          layer({
            prepare: (e) =>
              Effect.sync(() => {
                preparations += 1;

                return { text: e.text };
              }),
            authorize: () =>
              revoked
                ? SubscriptionError.make({ reason: "unauthorized", code: "revoked" })
                : Effect.succeed({ policyId: "policy", decisionId: "decision" }),
            submit: (envelope) =>
              Effect.suspend(() => {
                attempts.push(envelope);

                return attempts.length === 1
                  ? ScheduledInputRetryable.make({ reason: "ambiguous" })
                  : Effect.succeed(receipt(envelope));
              }),
          }),
        ),
      );
    },
  );

  for (const stop of ["cancel", "expire"] as const) {
    it.effect(`rechecks ${stop} after fallible preparation before freezing input`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let admissions = 0;

          yield* Effect.gen(function* () {
            yield* (yield* Subscriptions).subscribe(scope, {
              ...options("watch"),
              expiresAtMillis: 100,
            });
            yield* (yield* SubscriptionIntake).accept(principal, source, event("completion"));
            yield* drain(1);

            const preparing = yield* (yield* SubscriptionDriver)
              .processDelivery(key())
              .pipe(Effect.forkScoped);

            yield* Deferred.await(started);
            if (stop === "cancel")
              yield* (yield* Subscriptions).cancelSubscription(scope, key().subscription);
            else yield* TestClock.adjust(100);
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(preparing);
            const stored = yield* (yield* SubscriptionStore).delivery(key());

            expect(stored?.state).toBe("refused");
            expect(stored?.refusal?.code).toBe(stop === "cancel" ? "cancelled" : "expired");
            expect(admissions).toBe(0);
          }).pipe(
            Effect.provide(
              layer({
                prepare: (e) =>
                  Deferred.succeed(started, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.as({ text: e.text }),
                  ),
                submit: (input) =>
                  Effect.sync(() => {
                    admissions += 1;

                    return receipt(input);
                  }),
              }),
            ),
          );
        }),
      ),
    );
  }

  for (const stop of ["interrupt", "timeout"]) {
    it.effect(`keeps selected work and closes preparation resources on ${stop}`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>();
          let finalized = 0;
          let ready = false;

          yield* Effect.gen(function* () {
            yield* registerAndAccept;
            const driver = yield* SubscriptionDriver;

            const running = yield* driver
              .processDelivery(key())
              .pipe(Effect.exit, Effect.forkScoped);

            yield* Deferred.await(started);
            if (stop === "interrupt") yield* Fiber.interrupt(running);
            else {
              yield* TestClock.adjust(limits.operationTimeoutMillis);
              const exit = yield* Fiber.join(running);

              expect(Exit.isFailure(exit)).toBe(false);
            }
            expect(finalized).toBe(1);
            expect((yield* (yield* SubscriptionStore).delivery(key()))?.state).toBe("selected");
            expect((yield* (yield* SubscriptionStore).get(key().subscription))?.state).toBe(
              "consumed",
            );
            ready = true;
            yield* TestClock.adjust(10);
            yield* driver.processDelivery(key());
            expect((yield* (yield* SubscriptionStore).delivery(key()))?.state).toBe("delivered");
          }).pipe(
            Effect.provide(
              layer({
                prepare: (e) =>
                  ready
                    ? Effect.succeed({ text: e.text })
                    : Effect.scoped(
                        Effect.gen(function* () {
                          yield* Effect.acquireRelease(Effect.void, () =>
                            Effect.sync(() => {
                              finalized += 1;
                            }),
                          );
                          yield* Deferred.succeed(started, undefined);

                          return yield* Effect.never;
                        }),
                      ),
              }),
            ),
          );
        }),
      ),
    );
  }

  it.effect("retries selected work after a mixed preparation defect and interruption", () => {
    let failPreparation = true;

    return Effect.gen(function* () {
      yield* registerAndAccept;
      const driver = yield* SubscriptionDriver;

      expect(yield* driver.runDue).toMatchObject({ failed: 1 });
      const store = yield* SubscriptionStore;

      expect((yield* store.delivery(key()))?.state).toBe("selected");
      failPreparation = false;
      yield* TestClock.adjust(limits.retryMillis);
      yield* drain();
      expect((yield* store.delivery(key()))?.state).toBe("delivered");
    }).pipe(
      Effect.provide(
        layer({
          prepare: (event) =>
            failPreparation
              ? Effect.failCause(Cause.combine(Cause.die("preparation defect"), Cause.interrupt(0)))
              : Effect.succeed({ text: event.text }),
        }),
      ),
    );
  });

  for (const failure of ["corrupt", "timeout"]) {
    it.effect(`isolates ${failure} recovery reads while routing, delivering and reclaiming`, () => {
      const configured: SubscriptionLimits = {
        ...limits,
        retention: {
          replayHorizonMillis: 10_000,
          completedRetentionMillis: 0,
          maxTombstones: 8,
        },
      };

      return Effect.gen(function* () {
        const subscriptions = yield* Subscriptions;

        yield* subscriptions.subscribe(scope, options("healthy"));
        const intake = yield* SubscriptionIntake;

        yield* intake.accept(principal, source, { ...event("delivery"), occurredAtMillis: 0 });
        yield* drain(1);
        yield* subscriptions.subscribe(scope, {
          ...options("broken"),
          parameters: { key: "other" },
        });
        yield* intake.accept(principal, source, {
          ...event("reclaim"),
          key: "unmatched",
          occurredAtMillis: 0,
        });
        const store = yield* SubscriptionStore;
        const original = yield* store.get(key("broken").subscription);

        const faulty = SubscriptionStore.of({
          ...store,
          get: (key) =>
            key.subscriptionId !== "broken"
              ? store.get(key)
              : failure === "corrupt"
                ? SubscriptionError.make({ reason: "corrupt", code: "registration-record" })
                : Effect.never,
        });

        yield* Effect.gen(function* () {
          const driver = yield* SubscriptionDriver;

          for (let pass = 0; pass < 2; pass++) {
            const sweep = yield* Effect.forkChild(driver.runDue);

            yield* TestClock.adjust(configured.operationTimeoutMillis);
            expect(yield* Fiber.join(sweep)).toMatchObject({ failed: 1 });
            // One recovery row is smaller than the page: the cursor wraps on every pass.
            expect((yield* store.readScanCursors).recovery).toBe(0);
          }
        }).pipe(
          Effect.provide(
            SubscriptionDriver.layer(configured).pipe(
              Layer.provide(Layer.succeed(SubscriptionStore, faulty)),
            ),
          ),
        );
        expect((yield* store.delivery(key("healthy", "delivery")))?.state).toBe("delivered");
        expect((yield* store.event("reclaim"))?.tombstone).toBe(true);
        expect((yield* store.get(key("broken").subscription))?.recovery).toEqual(
          original?.recovery,
        );
      }).pipe(
        Effect.provide(
          layer({
            limits: configured,
            reconcile: () => Effect.succeed(null),
            submissionStatus: () => ScheduledInputRetryable.make({ reason: "storage" }),
          }),
        ),
      );
    });
  }

  it.effect(
    "commits sweep progress before work and continues after a corrupt event across driver restart",
    () => {
      let crash = false;

      return Effect.gen(function* () {
        yield* (yield* Subscriptions).subscribe(scope, options("watch", "continuous"));
        const intake = yield* SubscriptionIntake;

        yield* intake.accept(principal, source, event("a"));
        yield* intake.accept(principal, source, event("b"));
        crash = true;
        expect((yield* (yield* SubscriptionDriver).runDue.pipe(Effect.flip))._tag).toBe(
          "SubscriptionFailpointError",
        );
        const store = yield* SubscriptionStore;

        expect((yield* store.readScanCursors).events).toBe("a");
        expect((yield* store.event("a"))?.cursor).toBe(0);

        const faulty = SubscriptionStore.of({
          ...store,
          event: (id) =>
            store
              .event(id)
              .pipe(
                Effect.map((stored) =>
                  stored === null || id !== "a"
                    ? stored
                    : { ...stored, payload: { ...event("a"), text: "corrupted" } },
                ),
              ),
        });

        yield* drain(10).pipe(
          Effect.provide(
            SubscriptionDriver.layer({ ...limits, batchSize: 1 }).pipe(
              Layer.provide(Layer.succeed(SubscriptionStore, faulty)),
            ),
          ),
        );
        const deliveries = yield* (yield* Subscriptions).listDeliveries(scope, key().subscription);

        expect(deliveries.items.map((d) => d.key.eventId)).toEqual(["b"]);
        expect((yield* intake.status(principal, source, "a")).routingFailure).toBe("corrupt");
      }).pipe(
        Effect.provide(
          layer({
            limits: { ...limits, batchSize: 1 },
            failpoint: {
              hit: (point) =>
                Effect.suspend(() => {
                  if (crash && point === "subscription:advance-scan-cursors:after") {
                    crash = false;

                    return SubscriptionFailpointError.make({ point });
                  }

                  return Effect.void;
                }),
            },
          }),
        ),
      );
    },
  );

  it.effect(
    "rejects forged prepared input before admission and rejects impossible persisted states",
    () =>
      Effect.gen(function* () {
        yield* registerAndAccept;
        yield* drain();
        const store = yield* SubscriptionStore;
        const prepared = yield* store.delivery(key());

        if (prepared?.envelope === null || prepared === null)
          return yield* Effect.die("Expected prepared fixture");
        const forgedInput = { text: "forged" };
        const forgedDigest = yield* digestJson(forgedInput);

        const forged = {
          ...prepared,
          envelope: { ...prepared.envelope, input: forgedInput, inputDigest: forgedDigest },
        };

        let admissions = 0;

        yield* TestClock.adjust(10);
        yield* Effect.gen(function* () {
          const error = yield* (yield* SubscriptionDriver).processDelivery(key()).pipe(Effect.flip);

          expect(error).toMatchObject({ reason: "corrupt", code: "prepared-envelope" });
        }).pipe(
          Effect.provide(
            SubscriptionDriver.layer(limits).pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(SubscriptionStore, {
                    ...store,
                    delivery: () => Effect.succeed(forged),
                  }),
                  Layer.succeed(PreparedInputAdmission, {
                    submit: (input) =>
                      Effect.sync(() => {
                        admissions += 1;

                        return receipt(input);
                      }),
                  }),
                ),
              ),
            ),
          ),
        );
        expect(admissions).toBe(0);
      }).pipe(
        Effect.provide(
          layer({ submit: () => ScheduledInputRetryable.make({ reason: "transport" }) }),
        ),
      ),
  );
});

import { AgentId, ThreadId, ReceiptId, SubmissionId } from "@effect-agent/core";
import {
  AcceptedEvent,
  DefinitionDigests,
  Digest,
  EventSources,
  PreparedInputAdmission,
  Principal,
  QueueSequence,
  Receipt,
  ScheduledInputRefused,
  ScheduledInputRetryable,
  SubscriptionAuthorizer,
  SubscriptionDelivery,
  SubscriptionDriver,
  SubscriptionError,
  SubscriptionFailpoint,
  SubscriptionFailpointError,
  SubscriptionIntake,
  SubscriptionRecord,
  SubscriptionSourceError,
  SubscriptionStore,
  Subscriptions,
  defaultSubscriptionLimits,
  digestJson,
  makeEventSource,
  makeSubscriptionInputBinding,
  SubscriptionInputBindings,
  type SubscriptionInputBinding,
  type PreparedInput,
  type SubscribeOptions,
  type SubscriptionLimits,
} from "@effect-agent/thread";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import { TestClock } from "effect/testing";

import { memorySubscriptionStoreLayer } from "../src/index.ts";

const partition = { tenantId: "tenant", address: "repository:42" };
const principal = Schema.decodeSync(Principal)("manager");
const scope = { partition, ownerId: "owner", principal };
const source = { name: "trusted", version: "1" };
const agentId = Schema.decodeSync(AgentId)("subscription-agent");
const threadId = Schema.decodeSync(ThreadId)("subscription-thread");
const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const Event = Schema.Struct({ id: Schema.String, key: Schema.String, text: Schema.String });
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
  it.effect("prepares one event for different Agents and retained definition versions", () =>
    Effect.gen(function* () {
      const otherAgent = Schema.decodeSync(AgentId)("other-agent");
      const nextDefinitions = { ...definitions, agent: Schema.decodeSync(Digest)("b".repeat(64)) };
      const common = {
        source,
        event: Event,
        parameters: Schema.Struct({ key: Schema.String }),
      };
      const original = yield* makeSubscriptionInputBinding({
        ...common,
        agentId,
        definitions,
        context: Schema.Struct({ text: Schema.String }),
        input: Input,
        prepare: (e, _p, c) => Effect.succeed({ text: `${c.text}:${e.text}` }),
      });
      const next = yield* makeSubscriptionInputBinding({
        ...common,
        agentId,
        definitions: nextDefinitions,
        context: Schema.Struct({ prefix: Schema.String }),
        input: Input,
        prepare: (e, _p, c) => Effect.succeed({ text: `${c.prefix}:${e.text}` }),
      });
      const other = yield* makeSubscriptionInputBinding({
        ...common,
        agentId: otherAgent,
        definitions,
        context: Schema.Struct({ count: Schema.Number }),
        input: Schema.Struct({ count: Schema.Number, eventId: Schema.String }),
        prepare: (e, _p, c) => Effect.succeed({ count: c.count, eventId: e.id }),
      });
      const admitted: Array<PreparedInput> = [];
      yield* Effect.gen(function* () {
        const subscriptions = yield* Subscriptions;
        yield* subscriptions.subscribe(scope, options("old"));
        yield* subscriptions.subscribe(scope, {
          ...options("new"),
          definitions: nextDefinitions,
          context: { prefix: "new" },
        });
        yield* subscriptions.subscribe(scope, {
          ...options("other"),
          agentId: otherAgent,
          context: { count: 7 },
        });
        yield* (yield* SubscriptionIntake).accept(principal, source, event("completion"));
        yield* drain();
        expect(admitted).toHaveLength(3);
        expect(admitted.map((input) => input.input)).toEqual(
          expect.arrayContaining([
            { text: "private-continuation:completion" },
            { text: "new:completion" },
            { count: 7, eventId: "completion" },
          ]),
        );
      }).pipe(
        Effect.provide(
          layer({
            bindings: [original, next, other],
            submit: (input) =>
              Effect.sync(() => {
                admitted.push(input);
                return receipt(input);
              }),
          }),
        ),
      );
    }),
  );

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

  it.effect("rejects duplicate intake when the retained payload no longer matches its digest", () =>
    Effect.gen(function* () {
      yield* (yield* SubscriptionIntake).accept(principal, source, event("completion"));
      const store = yield* SubscriptionStore;
      const corruptStore = SubscriptionStore.of({
        ...store,
        accept: (record, limits) =>
          store.accept(record, limits).pipe(
            Effect.map((retained) => ({
              ...retained,
              payload: { ...event("completion"), text: "corrupted" },
            })),
          ),
      });
      const rejected = yield* Effect.gen(function* () {
        return yield* (yield* SubscriptionIntake)
          .accept(principal, source, event("completion"))
          .pipe(Effect.flip);
      }).pipe(
        Effect.provide(
          SubscriptionIntake.layer(limits).pipe(
            Layer.provide(Layer.succeed(SubscriptionStore, corruptStore)),
          ),
        ),
      );
      expect(rejected).toMatchObject({ reason: "corrupt", code: "event-digest" });
      expect((yield* store.event("completion"))?.payload).toEqual(event("completion"));
    }).pipe(Effect.provide(layer())),
  );

  it.effect(
    "rejects creation replay when retained configuration no longer matches its fingerprint",
    () =>
      Effect.gen(function* () {
        yield* (yield* Subscriptions).subscribe(scope, options("watch"));
        const store = yield* SubscriptionStore;
        const corruptStore = SubscriptionStore.of({
          ...store,
          get: (key) =>
            store.get(key).pipe(
              Effect.map((record) =>
                record === null
                  ? null
                  : {
                      ...record,
                      configuration: { ...record.configuration, context: { text: "corrupted" } },
                    },
              ),
            ),
        });
        const rejected = yield* Effect.gen(function* () {
          return yield* (yield* Subscriptions).subscribe(scope, options("watch")).pipe(Effect.flip);
        }).pipe(
          Effect.provide(
            Subscriptions.layer(limits).pipe(
              Layer.provide(Layer.succeed(SubscriptionStore, corruptStore)),
            ),
          ),
        );
        expect(rejected).toMatchObject({ reason: "corrupt", code: "creation-fingerprint" });
        expect((yield* store.get(key().subscription))?.configuration.context).toEqual(
          options("watch").context,
        );
      }).pipe(Effect.provide(layer())),
  );

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

  for (const point of ["subscription:register:before", "subscription:register:after"]) {
    it.effect(`recovers registration identity at ${point}`, () => {
      let armed = true;
      return Effect.gen(function* () {
        const management = yield* Subscriptions;
        yield* management.subscribe(scope, options("watch")).pipe(Effect.result);
        const replay = yield* management.subscribe(scope, options("watch"));
        expect(replay.key.subscriptionId).toBe("watch");
        expect((yield* management.listSubscriptions(scope)).items).toHaveLength(1);
        expect(
          yield* management
            .subscribe(scope, { ...options("watch"), context: { text: "changed" } })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "conflict" });
      }).pipe(
        Effect.provide(
          layer({
            failpoint: {
              hit: (observed) =>
                Effect.suspend(() => {
                  if (armed && point === observed) {
                    armed = false;
                    return SubscriptionFailpointError.make({ point });
                  }
                  return Effect.void;
                }),
            },
          }),
        ),
      );
    });
  }
  for (const phase of ["authorization", "construction", "admission"] as const) {
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
            ...(phase === "authorization"
              ? {
                  authorize: () =>
                    SubscriptionError.make({ reason: "unauthorized", code: "revoked" }),
                }
              : {}),
            ...(phase === "construction"
              ? {
                  prepare: () =>
                    SubscriptionSourceError.make({ code: "invalid-input", retryable: false }),
                }
              : {}),
            ...(phase === "admission"
              ? { submit: () => ScheduledInputRefused.make({ code: "proven-not-admitted" }) }
              : {}),
          }),
        ),
      ),
    );
  }

  for (const point of [
    "subscription:accept:before",
    "subscription:accept:after",
    "subscription:select:before",
    "subscription:select:after",
    "subscription:delivery-prepare:before",
    "subscription:delivery-prepare:after",
    "subscription:admission:after",
    "subscription:delivery-complete:before",
    "subscription:delivery-complete:after",
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
    "preserves distinct continuous events, once races, intake cutoff and scoped redaction",
    () =>
      Effect.gen(function* () {
        const management = yield* Subscriptions;
        const intake = yield* SubscriptionIntake;
        yield* management.subscribe(scope, options("once"));
        yield* management.subscribe(scope, options("continuous", "continuous"));
        yield* Effect.all(
          [
            intake.accept(principal, source, event("a")),
            intake.accept(principal, source, event("b")),
          ],
          { concurrency: 2 },
        );
        yield* management.subscribe(scope, options("late"));
        yield* Effect.all(
          [(yield* SubscriptionDriver).runDue, (yield* SubscriptionDriver).runDue],
          { concurrency: 2 },
        );
        yield* drain();
        expect(
          (yield* management.listDeliveries(scope, key("once").subscription)).items,
        ).toHaveLength(1);
        expect(
          (yield* management.listDeliveries(scope, key("continuous").subscription)).items
            .map((d) => d.key.eventId)
            .sort(),
        ).toEqual(["a", "b"]);
        expect(
          (yield* management.listDeliveries(scope, key("late").subscription)).items,
        ).toHaveLength(0);
        const listing = yield* management.listSubscriptions(scope);
        expect(JSON.stringify(listing)).not.toContain("private-continuation");
        expect(
          yield* management
            .cancelSubscription({ ...scope, ownerId: "another-owner" }, key("once").subscription)
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "unauthorized" });
        expect(
          (yield* intake
            .accept(principal, source, { ...event("a"), text: "conflict" })
            .pipe(Effect.flip))._tag,
        ).toBe("SubscriptionError");
        expect((yield* intake.status(principal, source, "a")).routingComplete).toBe(true);
      }).pipe(Effect.provide(layer())),
  );

  it.effect(
    "replays creation and intake after limits tighten without admitting new oversized work",
    () =>
      Effect.gen(function* () {
        const management = yield* Subscriptions;
        const intake = yield* SubscriptionIntake;
        const original = yield* management.subscribe(scope, options("watch"));
        const accepted = yield* intake.accept(principal, source, event("completion"));
        const tight = { ...limits, maxPayloadBytes: 1, maxContextBytes: 1, maxLifetimeMillis: 1 };
        yield* Effect.gen(function* () {
          expect(yield* (yield* Subscriptions).subscribe(scope, options("watch"))).toEqual(
            original,
          );
          expect(
            yield* (yield* SubscriptionIntake).accept(principal, source, event("completion")),
          ).toEqual(accepted);
          expect(
            (yield* (yield* Subscriptions).subscribe(scope, options("new")).pipe(Effect.flip))._tag,
          ).toBe("SubscriptionError");
          expect(
            (yield* (yield* SubscriptionIntake)
              .accept(principal, source, event("new"))
              .pipe(Effect.flip))._tag,
          ).toBe("SubscriptionError");
        }).pipe(
          Effect.provide(Layer.merge(Subscriptions.layer(tight), SubscriptionIntake.layer(tight))),
        );
      }).pipe(Effect.provide(layer())),
  );

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

  for (const stop of ["interrupt", "timeout", "defect"] as const) {
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
              if (stop === "timeout") yield* TestClock.adjust(limits.operationTimeoutMillis);
              const exit = yield* Fiber.join(running);
              expect(Exit.isFailure(exit)).toBe(stop === "defect");
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
                          return yield* stop === "defect"
                            ? Effect.die("private defect diagnostic")
                            : Effect.never;
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

  it.effect("retains permanent source failure visibly and does not poll it again", () => {
    let polls = 0;
    return Effect.gen(function* () {
      yield* (yield* Subscriptions).subscribe(scope, options("watch"));
      yield* drain();
      yield* TestClock.adjust(1_000);
      yield* drain();
      const snapshot = (yield* (yield* Subscriptions).listSubscriptions(scope)).items[0];
      expect(snapshot?.recovery).toMatchObject({
        nextAttemptAtMillis: null,
        lastFailure: "provider-unauthorized",
      });
      expect(polls).toBe(1);
      expect(
        (yield* (yield* Subscriptions).listDeliveries(scope, key().subscription)).items,
      ).toHaveLength(0);
    }).pipe(
      Effect.provide(
        layer({
          reconcile: () =>
            Effect.suspend(() => {
              polls += 1;
              return SubscriptionSourceError.make({
                code: "provider-unauthorized",
                retryable: false,
              });
            }),
        }),
      ),
    );
  });

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
        expect(Schema.is(SubscriptionDelivery)({ ...prepared, envelope: null })).toBe(false);
        expect(Schema.is(SubscriptionDelivery)({ ...prepared, schemaVersion: 2 })).toBe(false);
        const registration = yield* store.get(key().subscription);
        if (registration === null) return yield* Effect.die("Expected registration fixture");
        expect(
          Schema.is(SubscriptionRecord)({
            ...registration,
            configuration: { ...registration.configuration, mode: "continuous" },
          }),
        ).toBe(false);
        expect(
          Schema.is(AcceptedEvent)({ ...(yield* store.event("completion")), schemaVersion: 2 }),
        ).toBe(false);
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

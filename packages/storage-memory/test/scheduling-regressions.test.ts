import { AgentId, ConversationId, ReceiptId, SubmissionId } from "@effect-agent/core";
import {
  defaultSchedulingLimits,
  DefinitionDigests,
  Digest,
  Principal,
  QueueSequence,
  Receipt,
  ScheduleAuthorizationError,
  ScheduleAuthorizer,
  ScheduleId,
  type ScheduleCreateOptions,
  type ScheduledEnvelope,
  ScheduledInputAdmission,
  ScheduledInputRefused,
  ScheduledInputRetryable,
  Scheduling,
  type SchedulingLimits,
  ScheduleStore,
  ScheduleStorageError,
  type ScheduleTimingRequest,
  ScheduleWakeNoop,
} from "@effect-agent/session";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Tracer } from "effect";
import * as TestClock from "effect/testing/TestClock";

import { MemoryScheduleStoreLive } from "../src/index.ts";

const Input = Schema.Struct({ text: Schema.String });
const agent = { definition: { id: Schema.decodeSync(AgentId)("schedule-test"), input: Input } };
const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const scope = {
  owner: { tenantId: "tenant", ownerId: "owner" },
  principal: Schema.decodeSync(Principal)("manager"),
};
const deliveryPrincipal = Schema.decodeSync(Principal)("delivery");
const options = (
  id: string,
  timing: ScheduleTimingRequest = { _tag: "At", atMillis: 0 },
): ScheduleCreateOptions => ({
  scope,
  scheduleId: Schema.decodeSync(ScheduleId)(id),
  timing,
  destination: { _tag: "FreshConversation" },
  deliveryPrincipal,
  definitions,
});
const keyOf = (request: ScheduleCreateOptions) => ({
  owner: request.scope.owner,
  scheduleId: request.scheduleId,
});
const receiptFor = (envelope: ScheduledEnvelope): Receipt =>
  Receipt.make({
    conversationId: envelope.conversationId,
    receiptId: Schema.decodeSync(ReceiptId)(`receipt:${envelope.occurrenceId}`),
    submissionId: Schema.decodeSync(SubmissionId)(`submission:${envelope.occurrenceId}`),
    queueSequence: Schema.decodeSync(QueueSequence)(1),
  });
const allow: ScheduleAuthorizer["Service"] = {
  manage: () => Effect.void,
  prepare: () => Effect.succeed({ policyId: "test-policy", decisionId: "allowed" }),
};
const layer = (
  submit: ScheduledInputAdmission["Service"]["submit"] = (envelope) =>
    Effect.succeed(receiptFor(envelope)),
  authorizer: ScheduleAuthorizer["Service"] = allow,
  limits: SchedulingLimits = defaultSchedulingLimits,
  store: Layer.Layer<ScheduleStore> = MemoryScheduleStoreLive,
) =>
  Scheduling.layer(limits).pipe(
    Layer.provideMerge(store),
    Layer.provide(
      Layer.mergeAll(
        NodeCrypto.layer,
        ScheduleWakeNoop,
        Layer.succeed(ScheduleAuthorizer, authorizer),
        Layer.succeed(ScheduledInputAdmission, { submit }),
      ),
    ),
  );

describe("Scheduling public recovery contract", () => {
  it.effect("an authorization outage keeps the firing active for a later preparation", () => {
    let unavailable = true;
    const authorizer: ScheduleAuthorizer["Service"] = {
      manage: allow.manage,
      prepare: () =>
        unavailable
          ? Effect.fail(
              ScheduleStorageError.make({ operation: "authorize", reason: "unavailable" }),
            )
          : Effect.succeed({ policyId: "policy", decisionId: "available" }),
    };
    return Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const request = options("authorization-outage");
      yield* scheduler.create(agent, { text: "work" }, request);
      const failure = yield* scheduler.process(keyOf(request)).pipe(Effect.flip);
      expect(failure).toMatchObject({ _tag: "ScheduleStorageError", reason: "unavailable" });
      const unchanged = yield* scheduler.get(scope, request.scheduleId);
      expect(unchanged.state).toBe("active");
      expect(unchanged.pending).toBeNull();
      expect(unchanged.nextAtMillis).toBe(0);
      expect(unchanged.lastRefusal).toBeNull();
      unavailable = false;
      const delivered = yield* scheduler.process(keyOf(request));
      expect(delivered.lastReceipt?.intendedAtMillis).toBe(0);
    }).pipe(Effect.provide(layer(undefined, authorizer)));
  });

  it.effect("coalesces cron downtime at the exact boundary and skips paused firings", () => {
    const intended: Array<number> = [];
    return Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const request = options("cron", { _tag: "Cron", expression: "* * * * *" });
      const created = yield* scheduler.create(agent, { text: "tick" }, request);
      expect(created.configuration.timing).toEqual({
        _tag: "Cron",
        expression: "* * * * *",
        timeZone: "UTC",
      });
      yield* TestClock.adjust(180_000);
      const first = yield* scheduler.process(keyOf(request));
      expect(first.lastReceipt?.intendedAtMillis).toBe(180_000);
      expect(first.nextAtMillis).toBe(240_000);
      expect(first.lastSkippedRange).toEqual({ fromMillis: 60_000, toMillis: 180_000 });
      yield* scheduler.pause(scope, request.scheduleId, 1);
      yield* TestClock.adjust(150_000);
      expect(yield* scheduler.runDue()).toEqual([]);
      const resumed = yield* scheduler.resume(scope, request.scheduleId, 1);
      expect(resumed.nextAtMillis).toBe(360_000);
      expect(resumed.lastSkippedRange).toEqual({ fromMillis: 240_000, toMillis: 360_000 });
      yield* TestClock.adjust(30_000);
      yield* scheduler.process(keyOf(request));
      yield* TestClock.setTime(300_000);
      expect(yield* scheduler.runDue()).toEqual([]);
      yield* TestClock.setTime(420_000);
      yield* scheduler.process(keyOf(request));
      expect(intended).toEqual([180_000, 360_000, 420_000]);
    }).pipe(
      Effect.provide(
        layer((envelope) =>
          Effect.sync(() => {
            intended.push(envelope.intendedAtMillis);
            return receiptFor(envelope);
          }),
        ),
      ),
    );
  });

  it.effect("enforces owner quotas and input bounds without exposing other owners", () =>
    Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const request = options("quota");
      const input = { text: "original" };
      yield* scheduler.create(agent, input, request);
      input.text = "mutated";
      const first = yield* scheduler.get(scope, request.scheduleId);
      expect(first.configuration.input).toEqual({ text: "original" });
      Reflect.set(first.configuration, "input", { text: "changed snapshot" });
      expect((yield* scheduler.get(scope, request.scheduleId)).configuration.input).toEqual({
        text: "original",
      });
      const capacity = yield* scheduler
        .create(agent, { text: "new" }, options("excess"))
        .pipe(Effect.flip);
      expect(capacity._tag).toBe("ScheduleCapacityError");
      const otherScope = { ...scope, owner: { ...scope.owner, ownerId: "other" } };
      yield* scheduler.create(agent, { text: "other" }, { ...request, scope: otherScope });
      const page = yield* scheduler.list(scope);
      expect(page.items.map((record) => record.configuration.input)).toEqual([
        { text: "original" },
      ]);
      expect(page.next).toBeNull();
      const oversized = yield* scheduler
        .create(
          agent,
          { text: "x".repeat(65_536) },
          {
            ...options("oversized"),
            scope: otherScope,
          },
        )
        .pipe(Effect.flip);
      expect(oversized._tag).toBe("ScheduleValidationError");
    }).pipe(
      Effect.provide(
        layer(undefined, allow, { ...defaultSchedulingLimits, maxSchedulesPerOwner: 1 }),
      ),
    ),
  );

  it.effect(
    "replays creation after edits without resolving the original relative delay again",
    () =>
      Effect.gen(function* () {
        const scheduler = yield* Scheduling;
        const request = options("replay", { _tag: "After", delayMillis: 10_000 });
        const created = yield* scheduler.create(agent, { text: "original" }, request);
        expect(created.nextAtMillis).toBe(10_000);
        yield* TestClock.adjust(2_000);
        const edited = yield* scheduler.update(
          agent,
          { text: "edited" },
          {
            ...request,
            expectedRevision: 1,
            timing: { _tag: "After", delayMillis: 30_000 },
          },
        );
        yield* TestClock.adjust(4_000);
        const replay = yield* scheduler.create(agent, { text: "original" }, request);
        expect(replay.configuration).toEqual(edited.configuration);
        expect(replay.nextAtMillis).toBe(32_000);
        expect(replay.createdAtMillis).toBe(0);
        expect(replay.configurationRevision).toBe(2);
        expect(replay.creationFingerprint).toBe(created.creationFingerprint);
        const conflict = yield* scheduler
          .create(agent, { text: "different" }, request)
          .pipe(Effect.flip);
        expect(conflict._tag).toBe("ScheduleConflict");
      }).pipe(Effect.provide(layer())),
  );

  it.effect(
    "recovers one Receipt after a lost reply despite updates, cancellation and revocation",
    () => {
      const admitted = new Map<string, Receipt>();
      const submitted: Array<ScheduledEnvelope> = [];
      let preparationCount = 0;
      let revoked = false;
      const submit: ScheduledInputAdmission["Service"]["submit"] = Effect.fn(function* (envelope) {
        submitted.push(envelope);
        const existing = admitted.get(envelope.admissionKey);
        if (existing !== undefined) return existing;
        admitted.set(envelope.admissionKey, receiptFor(envelope));
        return yield* ScheduledInputRetryable.make({ reason: "ambiguous" });
      });
      const authorizer: ScheduleAuthorizer["Service"] = {
        manage: allow.manage,
        prepare: () => {
          preparationCount += 1;
          return revoked
            ? Effect.fail(ScheduleAuthorizationError.make({ code: "revoked" }))
            : Effect.succeed({ policyId: "test-policy", decisionId: "allowed" });
        },
      };
      return Effect.gen(function* () {
        const scheduler = yield* Scheduling;
        const request = options("lost-reply");
        yield* scheduler.create(agent, { text: "frozen" }, request);
        const pending = yield* scheduler.process(keyOf(request));
        expect(pending.pending?.retry.lastFailure).toBe("ambiguous");
        expect(pending.pending?.retry.nextAttemptAtMillis).toBe(1_000);
        const envelope = pending.pending?.envelope;
        const updated = yield* scheduler.update(
          agent,
          { text: "replacement" },
          {
            ...request,
            expectedRevision: 1,
            destination: {
              _tag: "ExistingConversation",
              conversationId: Schema.decodeSync(ConversationId)("other-conversation"),
            },
            timing: { _tag: "At", atMillis: 50_000 },
          },
        );
        expect(updated.pending?.envelope).toEqual(envelope);
        yield* scheduler.pause(scope, request.scheduleId, 2);
        const cancelled = yield* scheduler.cancel(scope, request.scheduleId, 2);
        expect(cancelled.pending?.envelope).toEqual(envelope);
        revoked = true;
        yield* TestClock.adjust(1_000);
        const recovered = yield* scheduler.process(keyOf(request));
        expect(recovered.state).toBe("cancelled");
        expect(recovered.pending).toBeNull();
        expect(recovered.nextAtMillis).toBeNull();
        expect(recovered.lastReceipt?.receipt).toEqual(admitted.values().next().value);
        expect(submitted).toHaveLength(2);
        expect(submitted[0]).toEqual(submitted[1]);
        expect(admitted.size).toBe(1);
        expect(preparationCount).toBe(1);
      }).pipe(Effect.provide(layer(submit, authorizer)));
    },
  );

  it.effect("duplicate due passes converge on one occurrence per schedule", () => {
    const admitted = new Map<string, Receipt>();
    const submit: ScheduledInputAdmission["Service"]["submit"] = Effect.fn(function* (envelope) {
      yield* Effect.yieldNow;
      const receipt = admitted.get(envelope.admissionKey) ?? receiptFor(envelope);
      admitted.set(envelope.admissionKey, receipt);
      return receipt;
    });
    return Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const requests = [options("duplicate-a"), options("duplicate-b")];
      yield* Effect.forEach(requests, (request) =>
        scheduler.create(agent, { text: "work" }, request),
      );
      yield* Effect.all([scheduler.runDue(), scheduler.runDue()], { concurrency: "unbounded" });
      const snapshots = yield* scheduler.list(scope);
      expect(snapshots.items.map((record) => record.pending)).toEqual([null, null]);
      expect(snapshots.items.every((record) => record.lastReceipt !== null)).toBe(true);
      expect(admitted.size).toBe(2);
      expect(yield* scheduler.runDue()).toEqual([]);
    }).pipe(Effect.provide(layer(submit)));
  });

  it.effect("a Receipt for a different Conversation does not clear the pending obligation", () => {
    const submit: ScheduledInputAdmission["Service"]["submit"] = (envelope) =>
      Effect.succeed(
        Receipt.make({
          ...receiptFor(envelope),
          conversationId: Schema.decodeSync(ConversationId)("wrong-conversation"),
        }),
      );
    return Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const request = options("wrong-receipt");
      yield* scheduler.create(agent, { text: "work" }, request);
      const failure = yield* scheduler.process(keyOf(request)).pipe(Effect.flip);
      expect(failure).toMatchObject({ _tag: "ScheduleStorageError", reason: "corrupt" });
      const current = yield* scheduler.get(scope, request.scheduleId);
      expect(current.pending?.envelope.input).toEqual({ text: "work" });
      expect(current.lastReceipt).toBeNull();
    }).pipe(Effect.provide(layer(submit)));
  });

  it.effect("persists storage backoff when completing an admitted occurrence is unavailable", () =>
    Effect.gen(function* () {
      let failCompletion = true;
      const wrappedStore = Layer.effect(
        ScheduleStore,
        Effect.gen(function* () {
          const store = yield* ScheduleStore;
          return ScheduleStore.of({
            ...store,
            change: (key, change) => {
              if (change._tag === "Complete" && failCompletion) {
                failCompletion = false;
                return Effect.fail(
                  ScheduleStorageError.make({ operation: "complete", reason: "unavailable" }),
                );
              }
              return store.change(key, change);
            },
          });
        }),
      ).pipe(Layer.provide(MemoryScheduleStoreLive));
      yield* Effect.gen(function* () {
        const scheduler = yield* Scheduling;
        const request = options("completion-storage-backoff");
        yield* scheduler.create(agent, { text: "work" }, request);
        const pending = yield* scheduler.process(keyOf(request));
        expect(pending.pending?.retry).toMatchObject({
          attempts: 1,
          nextAttemptAtMillis: 1_000,
          lastFailure: "storage",
        });
        expect(pending.lastReceipt).toBeNull();
        yield* TestClock.adjust(1_000);
        const completed = yield* scheduler.process(keyOf(request));
        expect(completed.pending).toBeNull();
        expect(completed.lastReceipt).not.toBeNull();
      }).pipe(Effect.provide(layer(undefined, allow, defaultSchedulingLimits, wrappedStore)));
    }),
  );

  it.effect("rejects a corrupted current input digest before authorization or admission", () =>
    Effect.gen(function* () {
      let corruptReads = false;
      const preparations = yield* Ref.make(0);
      const submissions = yield* Ref.make(0);
      const wrappedStore = Layer.effect(
        ScheduleStore,
        Effect.gen(function* () {
          const store = yield* ScheduleStore;
          return ScheduleStore.of({
            ...store,
            get: (key) =>
              store.get(key).pipe(
                Effect.map((current) =>
                  corruptReads && current !== null
                    ? {
                        ...current,
                        configuration: {
                          ...current.configuration,
                          input: { text: "tampered-without-digest" },
                        },
                      }
                    : current,
                ),
              ),
          });
        }),
      ).pipe(Layer.provide(MemoryScheduleStoreLive));
      const authorizer: ScheduleAuthorizer["Service"] = {
        manage: allow.manage,
        prepare: (request) =>
          Ref.update(preparations, (count) => count + 1).pipe(
            Effect.as({ policyId: "policy", decisionId: `decision-${request.occurrenceId}` }),
          ),
      };
      const submit: ScheduledInputAdmission["Service"]["submit"] = (envelope) =>
        Ref.update(submissions, (count) => count + 1).pipe(Effect.as(receiptFor(envelope)));
      yield* Effect.gen(function* () {
        const scheduler = yield* Scheduling;
        const request = options("corrupt-current-input");
        yield* scheduler.create(agent, { text: "original" }, request);
        corruptReads = true;
        const failure = yield* scheduler.process(keyOf(request)).pipe(Effect.flip);
        expect(failure).toMatchObject({ _tag: "ScheduleStorageError", reason: "corrupt" });
        expect(yield* Ref.get(preparations)).toBe(0);
        expect(yield* Ref.get(submissions)).toBe(0);
      }).pipe(Effect.provide(layer(submit, authorizer, defaultSchedulingLimits, wrappedStore)));
    }),
  );

  it.effect("does not expose rejected Agent input through errors or spans", () => {
    const sentinel = "schedule-input-secret-sentinel";
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const SecretInput = Schema.String.check(Schema.isMinLength(128));
    const secretAgent = {
      definition: { id: Schema.decodeSync(AgentId)("schedule-secret-test"), input: SecretInput },
    };
    return Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const failure = yield* scheduler
        .create(secretAgent, sentinel, options("redacted-input"))
        .pipe(Effect.flip);
      expect(failure).toMatchObject({
        _tag: "ScheduleValidationError",
        message: "Unable to encode Agent input",
      });
      const spanFailures = spans.flatMap((span) =>
        span.status._tag === "Ended" && Exit.isFailure(span.status.exit)
          ? [Cause.pretty(span.status.exit.cause)]
          : [],
      );
      expect([failure.message, ...spanFailures].join("\n")).not.toContain(sentinel);
    }).pipe(Effect.provide(layer()), Effect.provideService(Tracer.Tracer, tracer));
  });

  it.effect("denies an update before reading whether the schedule exists", () =>
    Effect.gen(function* () {
      const reads = yield* Ref.make(0);
      const observedStore = Layer.effect(
        ScheduleStore,
        Effect.gen(function* () {
          const store = yield* ScheduleStore;
          return ScheduleStore.of({
            ...store,
            get: (key) =>
              Ref.update(reads, (count) => count + 1).pipe(Effect.andThen(store.get(key))),
          });
        }),
      ).pipe(Layer.provide(MemoryScheduleStoreLive));
      const authorizer: ScheduleAuthorizer["Service"] = {
        ...allow,
        manage: () => Effect.fail(ScheduleAuthorizationError.make({ code: "unauthorized" })),
      };
      yield* Effect.gen(function* () {
        const scheduler = yield* Scheduling;
        const failure = yield* scheduler
          .update(
            agent,
            { text: "private" },
            {
              ...options("not-visible"),
              expectedRevision: 1,
            },
          )
          .pipe(Effect.flip);
        expect(failure._tag).toBe("ScheduleAuthorizationError");
        expect(yield* Ref.get(reads)).toBe(0);
      }).pipe(Effect.provide(layer(undefined, authorizer, defaultSchedulingLimits, observedStore)));
    }),
  );

  it.effect("shares one admission bound across overlapping process calls", () =>
    Effect.gen(function* () {
      const firstEntered = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const active = yield* Ref.make(0);
      const maximum = yield* Ref.make(0);
      const submit: ScheduledInputAdmission["Service"]["submit"] = Effect.fn(function* (envelope) {
        const count = yield* Ref.updateAndGet(active, (value) => value + 1);
        yield* Ref.update(maximum, (value) => Math.max(value, count));
        yield* Deferred.succeed(firstEntered, undefined);
        yield* Deferred.await(releaseFirst).pipe(
          Effect.ensuring(Ref.update(active, (value) => value - 1)),
        );
        return receiptFor(envelope);
      });
      yield* Effect.gen(function* () {
        const scheduler = yield* Scheduling;
        const a = options("bounded-a");
        const b = options("bounded-b");
        yield* scheduler.create(agent, { text: "a" }, a);
        yield* scheduler.create(agent, { text: "b" }, b);
        const first = yield* scheduler.process(keyOf(a)).pipe(Effect.forkScoped);
        yield* Deferred.await(firstEntered);
        const second = yield* scheduler.process(keyOf(b)).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(yield* Ref.get(active)).toBe(1);
        yield* Deferred.succeed(releaseFirst, undefined);
        yield* Fiber.join(first);
        yield* Fiber.join(second);
        expect(yield* Ref.get(maximum)).toBe(1);
        expect(yield* Ref.get(active)).toBe(0);
      }).pipe(
        Effect.provide(
          layer(submit, allow, { ...defaultSchedulingLimits, admissionConcurrency: 1 }),
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("resumes a never-prepared overdue one-shot but cannot revive a refused one", () => {
    let authorized = false;
    const authorizer: ScheduleAuthorizer["Service"] = {
      manage: allow.manage,
      prepare: () =>
        authorized
          ? Effect.succeed({ policyId: "policy", decisionId: "allowed" })
          : Effect.fail(ScheduleAuthorizationError.make({ code: "revoked" })),
    };
    return Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const request = options("one-shot-refusal");
      yield* scheduler.create(agent, { text: "work" }, request);
      const denied = yield* scheduler.process(keyOf(request));
      expect(denied.state).toBe("paused");
      expect(denied.pending).toBeNull();
      expect(denied.nextAtMillis).toBe(0);
      authorized = true;
      yield* TestClock.adjust(5_000);
      const resumed = yield* scheduler.resume(scope, request.scheduleId, 1);
      expect(resumed.nextAtMillis).toBe(0);
      const refused = yield* scheduler.process(keyOf(request));
      expect(refused.lastRefusal?.phase).toBe("admission");
      expect(refused.pending).toBeNull();
      expect(refused.state).toBe("paused");
      const secondResume = yield* scheduler.resume(scope, request.scheduleId, 1);
      expect(secondResume.nextAtMillis).toBeNull();
      expect(yield* scheduler.runDue()).toEqual([]);
      const updated = yield* scheduler.update(
        agent,
        { text: "changed" },
        {
          ...request,
          expectedRevision: 1,
          timing: { _tag: "At", atMillis: 6_000 },
        },
      );
      expect(updated.state).toBe("active");
      expect(updated.nextAtMillis).toBe(6_000);
    }).pipe(
      Effect.provide(
        layer(() => Effect.fail(ScheduledInputRefused.make({ code: "permanent" })), authorizer),
      ),
    );
  });

  it.effect("timeout, interruption and defect preserve a frozen pending delivery", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      let behavior: "timeout" | "interruption" | "defect" | "success" = "timeout";
      const submit: ScheduledInputAdmission["Service"]["submit"] = Effect.fn(function* (envelope) {
        yield* Deferred.succeed(entered, undefined);
        switch (behavior) {
          case "timeout":
          case "interruption":
            return yield* Effect.never;
          case "defect":
            return yield* Effect.die("injected scheduler admission defect");
          case "success":
            return receiptFor(envelope);
        }
      });
      yield* Effect.gen(function* () {
        const scheduler = yield* Scheduling;
        const request = options("failure-paths");
        yield* scheduler.create(agent, { text: "work" }, request);
        const timed = yield* scheduler.process(keyOf(request)).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* TestClock.adjust(100);
        const pending = yield* Fiber.join(timed);
        expect(pending.pending?.retry.lastFailure).toBe("timeout");
        const envelope = pending.pending?.envelope;
        yield* TestClock.adjust(1_000);
        behavior = "interruption";
        const interrupted = yield* scheduler.process(keyOf(request)).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(interrupted);
        expect((yield* scheduler.get(scope, request.scheduleId)).pending?.envelope).toEqual(
          envelope,
        );
        behavior = "defect";
        const defect = yield* scheduler.process(keyOf(request)).pipe(Effect.exit);
        expect(defect._tag).toBe("Failure");
        expect((yield* scheduler.get(scope, request.scheduleId)).pending?.envelope).toEqual(
          envelope,
        );
        behavior = "success";
        const completed = yield* scheduler.process(keyOf(request));
        expect(completed.pending).toBeNull();
        expect(completed.lastReceipt?.occurrenceId).toBe(envelope?.occurrenceId);
      }).pipe(
        Effect.provide(
          layer(submit, allow, { ...defaultSchedulingLimits, admissionTimeoutMillis: 100 }),
        ),
      );
    }).pipe(Effect.scoped),
  );
});

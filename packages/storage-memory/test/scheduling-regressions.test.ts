import { AgentId, ThreadId, ReceiptId, SubmissionId } from "@effect-agent/core/Identifiers";
import { MemoryScheduleStoreLive } from "@effect-agent/storage-memory/MemoryScheduleStore";
import { Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigests, Digest } from "@effect-agent/thread/Records";
import {
  defaultSchedulingLimits,
  ScheduleAuthorizationError,
  ScheduleAuthorizer,
  ScheduleId,
  type ScheduledEnvelope,
  ScheduledInputAdmission,
  ScheduledInputRefused,
  ScheduledInputRetryable,
  SchedulingLimits,
  ScheduleStore,
  ScheduleStorageError,
  type ScheduleTimingRequest,
} from "@effect-agent/thread/Schedule";
import {
  type ScheduleCreateOptions,
  Scheduling,
  ScheduleDriver,
  ScheduleWakeNoop,
} from "@effect-agent/thread/Scheduling";
import { Principal, QueueSequence } from "@effect-agent/thread/SubmissionLedger";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Tracer } from "effect";
import * as TestClock from "effect/testing/TestClock";

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
  destination: { _tag: "FreshThread" },
  deliveryPrincipal,
  definitions,
});

const keyOf = (request: ScheduleCreateOptions) => ({
  owner: request.scope.owner,
  scheduleId: request.scheduleId,
});

const receiptFor = (envelope: ScheduledEnvelope): Receipt =>
  Receipt.make({
    threadId: envelope.threadId,
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
  Layer.merge(Scheduling.layer(limits), ScheduleDriver.layer(limits)).pipe(
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
  it.effect("management cannot acquire driver authority or reveal persistence fields", () => {
    let revoked = false;

    return Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const request = options("private-input");
      const created = yield* scheduler.create(agent, { text: "private-sentinel" }, request);

      yield* (yield* ScheduleDriver).process(keyOf(request));
      const snapshot = yield* scheduler.get(scope, request.scheduleId);
      const page = yield* scheduler.list(scope);

      for (const value of [created, snapshot, ...page.items]) {
        expect(value).not.toHaveProperty("creationFingerprint");
        expect(value).not.toHaveProperty("version");
        expect(value).not.toHaveProperty("schemaVersion");
        expect(value.configuration).not.toHaveProperty("input");
        expect(value.configuration).not.toHaveProperty("inputDigest");
        if (value.pending !== null) expect(value.pending).not.toHaveProperty("envelope");
        expect(JSON.stringify(value)).not.toContain("private-sentinel");
      }
      expect(scheduler).not.toHaveProperty("process");
      expect(scheduler).not.toHaveProperty("runDue");
      revoked = true;
      expect((yield* scheduler.get(scope, request.scheduleId).pipe(Effect.flip))._tag).toBe(
        "ScheduleAuthorizationError",
      );
      expect((yield* scheduler.list(scope).pipe(Effect.flip))._tag).toBe(
        "ScheduleAuthorizationError",
      );
    }).pipe(
      Effect.provide(
        layer(() => Effect.fail(ScheduledInputRetryable.make({ reason: "ambiguous" })), {
          ...allow,
          manage: () =>
            revoked
              ? Effect.fail(ScheduleAuthorizationError.make({ code: "revoked" }))
              : Effect.void,
        }),
      ),
    );
  });

  it.effect("repeated resume preserves an active due occurrence and its storage version", () =>
    Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const store = yield* ScheduleStore;
      const request = options("resume-replay", { _tag: "Interval", everyMillis: 60_000 });

      yield* scheduler.create(agent, { text: "tick" }, request);
      yield* scheduler.pause(scope, request.scheduleId, 1);
      yield* scheduler.resume(scope, request.scheduleId, 1);
      const before = yield* store.get(keyOf(request));

      yield* TestClock.adjust(60_000);
      const resumed = yield* scheduler.resume(scope, request.scheduleId, 1);

      expect(resumed.nextAtMillis).toBe(60_000);
      expect(yield* store.get(keyOf(request))).toEqual(before);
      const delivered = yield* (yield* ScheduleDriver).process(keyOf(request));

      expect(delivered.lastReceipt?.intendedAtMillis).toBe(60_000);
      expect((yield* scheduler.resume(scope, request.scheduleId, 2).pipe(Effect.flip))._tag).toBe(
        "ScheduleConflict",
      );
    }).pipe(Effect.provide(layer())),
  );

  it.effect("supports a lower positive host interval minimum without changing the default", () =>
    Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const request = options("fast-interval", { _tag: "Interval", everyMillis: 100 });

      yield* scheduler.create(agent, { text: "tick" }, request);
      yield* TestClock.adjust(100);
      expect(
        (yield* (yield* ScheduleDriver).process(keyOf(request))).lastReceipt?.intendedAtMillis,
      ).toBe(100);
      expect(defaultSchedulingLimits.minIntervalMillis).toBe(60_000);
      expect(
        Schema.is(SchedulingLimits)({ ...defaultSchedulingLimits, minIntervalMillis: 0 }),
      ).toBe(false);
    }).pipe(
      Effect.provide(
        layer(undefined, allow, { ...defaultSchedulingLimits, minIntervalMillis: 100 }),
      ),
    ),
  );

  for (const failure of ["storage", "defect", "decode"] as const) {
    it.effect(`a ${failure} failure cannot starve later schedules with a one-record page`, () => {
      let broken = true;

      const wrapped = Layer.effect(
        ScheduleStore,
        Effect.gen(function* () {
          const store = yield* ScheduleStore;

          return ScheduleStore.of({
            ...store,
            get: (key) =>
              failure === "decode" && broken && key.scheduleId === "a-failing"
                ? Effect.fail(ScheduleStorageError.make({ operation: "decode", reason: "corrupt" }))
                : store.get(key),
          });
        }),
      ).pipe(Layer.provide(MemoryScheduleStoreLive));

      return Effect.gen(function* () {
        const scheduler = yield* Scheduling;
        const driver = yield* ScheduleDriver;

        broken = false;
        yield* scheduler.create(agent, { text: "first" }, options("a-failing"));
        yield* scheduler.create(agent, { text: "second" }, options("b-healthy"));
        broken = true;
        expect(yield* driver.runDue()).toEqual({ processed: 1, failed: 1 });
        expect(
          (yield* scheduler.get(scope, options("b-healthy").scheduleId)).lastReceipt,
        ).not.toBeNull();
        expect(yield* driver.runDue()).toEqual({ processed: 0, failed: 1 });
        broken = false;
        expect(yield* driver.runDue()).toEqual({ processed: 1, failed: 0 });
      }).pipe(
        Effect.provide(
          layer(
            (envelope) =>
              broken && envelope.scheduleId === "a-failing" && failure !== "decode"
                ? failure === "defect"
                  ? Effect.die("injected")
                  : Effect.fail(
                      ScheduleStorageError.make({ operation: "admit", reason: "corrupt" }),
                    )
                : Effect.succeed(receiptFor(envelope)),
            allow,
            { ...defaultSchedulingLimits, dueBatchSize: 1 },
            wrapped,
          ),
        ),
      );
    });
  }

  it.effect("delivers the spring-gap occurrence through ordinary admission", () => {
    const submitted: Array<number> = [];

    return Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2026-03-07T12:00:00Z"));
      const scheduler = yield* Scheduling;

      const request = options("spring-forward", {
        _tag: "Cron",
        expression: "30 2 * * *",
        timeZone: "America/New_York",
      });

      const created = yield* scheduler.create(agent, { text: "spring" }, request);

      expect(created.nextAtMillis).toBe(Date.parse("2026-03-08T07:30:00Z"));
      yield* TestClock.setTime(Date.parse("2026-03-08T07:30:00Z"));
      yield* (yield* ScheduleDriver).runDue();
      yield* TestClock.setTime(Date.parse("2026-03-09T06:30:00Z"));
      yield* (yield* ScheduleDriver).runDue();
      expect(submitted).toEqual([
        Date.parse("2026-03-08T07:30:00Z"),
        Date.parse("2026-03-09T06:30:00Z"),
      ]);
    }).pipe(
      Effect.provide(
        layer((envelope) =>
          Effect.sync(() => {
            submitted.push(envelope.intendedAtMillis);

            return receiptFor(envelope);
          }),
        ),
      ),
    );
  });

  it.effect(
    "rejects an unrepresentable recovery deadline through the typed initialization channel",
    () =>
      Effect.void.pipe(
        Effect.provide(
          layer(undefined, allow, {
            ...defaultSchedulingLimits,
            recoveryPollMillis: Number.MAX_SAFE_INTEGER,
          }),
        ),
        Effect.flip,
        Effect.map((error) =>
          expect(error).toMatchObject({
            _tag: "ScheduleValidationError",
            message: "Scheduling recovery deadline exceeds the supported instant range",
          }),
        ),
      ),
  );

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
      const failure = yield* (yield* ScheduleDriver).process(keyOf(request)).pipe(Effect.flip);

      expect(failure).toMatchObject({ _tag: "ScheduleStorageError", reason: "unavailable" });
      const unchanged = yield* scheduler.get(scope, request.scheduleId);

      expect(unchanged.state).toBe("active");
      expect(unchanged.pending).toBeNull();
      expect(unchanged.nextAtMillis).toBe(0);
      expect(unchanged.lastRefusal).toBeNull();
      unavailable = false;
      const delivered = yield* (yield* ScheduleDriver).process(keyOf(request));

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
      const first = yield* (yield* ScheduleDriver).process(keyOf(request));

      expect(first.lastReceipt?.intendedAtMillis).toBe(180_000);
      expect(first.nextAtMillis).toBe(240_000);
      expect(first.lastSkippedRange).toEqual({ fromMillis: 60_000, toMillis: 180_000 });
      yield* scheduler.pause(scope, request.scheduleId, 1);
      yield* TestClock.adjust(150_000);
      expect(yield* (yield* ScheduleDriver).runDue()).toEqual({ processed: 0, failed: 0 });
      const resumed = yield* scheduler.resume(scope, request.scheduleId, 1);

      expect(resumed.nextAtMillis).toBe(360_000);
      expect(resumed.lastSkippedRange).toEqual({ fromMillis: 240_000, toMillis: 360_000 });
      yield* TestClock.adjust(30_000);
      yield* (yield* ScheduleDriver).process(keyOf(request));
      yield* TestClock.setTime(300_000);
      expect(yield* (yield* ScheduleDriver).runDue()).toEqual({ processed: 0, failed: 0 });
      yield* TestClock.setTime(420_000);
      yield* (yield* ScheduleDriver).process(keyOf(request));
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

      expect(first.configuration).not.toHaveProperty("input");
      const store = yield* ScheduleStore;

      expect((yield* store.get(keyOf(request)))?.configuration.input).toEqual({ text: "original" });

      const capacity = yield* scheduler
        .create(agent, { text: "new" }, options("excess"))
        .pipe(Effect.flip);

      expect(capacity._tag).toBe("ScheduleCapacityError");
      const otherScope = { ...scope, owner: { ...scope.owner, ownerId: "other" } };

      yield* scheduler.create(agent, { text: "other" }, { ...request, scope: otherScope });
      const page = yield* scheduler.list(scope);

      expect(page.items.map((record) => record.scheduleId)).toEqual([request.scheduleId]);
      expect(JSON.stringify(page)).not.toContain("original");
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
        const pending = yield* (yield* ScheduleDriver).process(keyOf(request));

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
              _tag: "ExistingThread",
              threadId: Schema.decodeSync(ThreadId)("other-thread"),
            },
            timing: { _tag: "At", atMillis: 50_000 },
          },
        );

        expect(updated.pending?.occurrenceId).toBe(envelope?.occurrenceId);
        expect((yield* (yield* ScheduleStore).get(keyOf(request)))?.pending?.envelope).toEqual(
          envelope,
        );
        yield* scheduler.pause(scope, request.scheduleId, 2);
        const cancelled = yield* scheduler.cancel(scope, request.scheduleId, 2);

        expect(cancelled.pending?.occurrenceId).toBe(envelope?.occurrenceId);
        expect((yield* (yield* ScheduleStore).get(keyOf(request)))?.pending?.envelope).toEqual(
          envelope,
        );
        revoked = true;
        yield* TestClock.adjust(1_000);
        const recovered = yield* (yield* ScheduleDriver).process(keyOf(request));

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
      yield* Effect.all([(yield* ScheduleDriver).runDue(), (yield* ScheduleDriver).runDue()], {
        concurrency: "unbounded",
      });
      const snapshots = yield* scheduler.list(scope);

      expect(snapshots.items.map((record) => record.pending)).toEqual([null, null]);
      expect(snapshots.items.every((record) => record.lastReceipt !== null)).toBe(true);
      expect(admitted.size).toBe(2);
      expect(yield* (yield* ScheduleDriver).runDue()).toEqual({ processed: 0, failed: 0 });
    }).pipe(Effect.provide(layer(submit)));
  });

  it.effect("a Receipt for a different Thread does not clear the pending obligation", () => {
    const submit: ScheduledInputAdmission["Service"]["submit"] = (envelope) =>
      Effect.succeed(
        Receipt.make({
          ...receiptFor(envelope),
          threadId: Schema.decodeSync(ThreadId)("wrong-thread"),
        }),
      );

    return Effect.gen(function* () {
      const scheduler = yield* Scheduling;
      const request = options("wrong-receipt");

      yield* scheduler.create(agent, { text: "work" }, request);
      const failure = yield* (yield* ScheduleDriver).process(keyOf(request)).pipe(Effect.flip);

      expect(failure).toMatchObject({ _tag: "ScheduleStorageError", reason: "corrupt" });
      const current = yield* scheduler.get(scope, request.scheduleId);

      expect(current.pending).not.toBeNull();
      expect((yield* (yield* ScheduleStore).get(keyOf(request)))?.pending?.envelope.input).toEqual({
        text: "work",
      });
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
        const pending = yield* (yield* ScheduleDriver).process(keyOf(request));

        expect(pending.pending?.retry).toMatchObject({
          attempts: 1,
          nextAttemptAtMillis: 1_000,
          lastFailure: "storage",
        });
        expect(pending.lastReceipt).toBeNull();
        yield* TestClock.adjust(1_000);
        const completed = yield* (yield* ScheduleDriver).process(keyOf(request));

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
        const failure = yield* (yield* ScheduleDriver).process(keyOf(request)).pipe(Effect.flip);

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
        const first = yield* (yield* ScheduleDriver).process(keyOf(a)).pipe(Effect.forkScoped);

        yield* Deferred.await(firstEntered);
        const second = yield* (yield* ScheduleDriver).process(keyOf(b)).pipe(Effect.forkScoped);

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
      const denied = yield* (yield* ScheduleDriver).process(keyOf(request));

      expect(denied.state).toBe("paused");
      expect(denied.pending).toBeNull();
      expect(denied.nextAtMillis).toBe(0);
      authorized = true;
      yield* TestClock.adjust(5_000);
      const resumed = yield* scheduler.resume(scope, request.scheduleId, 1);

      expect(resumed.nextAtMillis).toBe(0);
      const refused = yield* (yield* ScheduleDriver).process(keyOf(request));

      expect(refused.lastRefusal?.phase).toBe("admission");
      expect(refused.pending).toBeNull();
      expect(refused.state).toBe("paused");
      const secondResume = yield* scheduler.resume(scope, request.scheduleId, 1);

      expect(secondResume.nextAtMillis).toBeNull();
      expect(yield* (yield* ScheduleDriver).runDue()).toEqual({ processed: 0, failed: 0 });

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

        const timed = yield* (yield* ScheduleDriver)
          .process(keyOf(request))
          .pipe(Effect.forkScoped);

        yield* Deferred.await(entered);
        yield* TestClock.adjust(100);
        const pending = yield* Fiber.join(timed);

        expect(pending.pending?.retry.lastFailure).toBe("timeout");
        const envelope = pending.pending?.envelope;

        yield* TestClock.adjust(1_000);
        behavior = "interruption";

        const interrupted = yield* (yield* ScheduleDriver)
          .process(keyOf(request))
          .pipe(Effect.forkScoped);

        yield* Effect.yieldNow;
        yield* Fiber.interrupt(interrupted);
        expect((yield* (yield* ScheduleStore).get(keyOf(request)))?.pending?.envelope).toEqual(
          envelope,
        );
        behavior = "defect";
        const defect = yield* (yield* ScheduleDriver).process(keyOf(request)).pipe(Effect.exit);

        expect(defect._tag).toBe("Failure");
        expect((yield* (yield* ScheduleStore).get(keyOf(request)))?.pending?.envelope).toEqual(
          envelope,
        );
        behavior = "success";
        const completed = yield* (yield* ScheduleDriver).process(keyOf(request));

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

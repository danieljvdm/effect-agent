import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Logger,
  References,
  Scope,
  Tracer,
} from "effect";
import { TestClock } from "effect/testing";

import {
  flushCloudflareRuntimeTelemetry,
  fulfillCloudflareTelemetryBackground,
  logCloudflareWaitUntilRegistrationFailure,
  makeCloudflareTelemetryFlushCoordinator,
  MAX_CLOUDFLARE_TELEMETRY_BATCH_DELIVERIES,
  registerCloudflareTelemetryAfterNativeSettlement,
  withCloudflareNativeSpanFailure,
} from "../../src/telemetry-internal.ts";
import { CloudflareRuntimeTelemetry, CloudflareTelemetryExportError } from "../../src/telemetry.ts";
import { makeTelemetryProbeFixture, type TelemetryFlushBlock } from "../telemetry-fixtures.ts";

class TestCauseSource extends Context.Service<TestCauseSource, string>()(
  "@effect-agent/platform-cloudflare/test/TestCauseSource",
) {}

const promiseGate = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((resume) => {
    resolve = resume;
  });
  return { promise, resolve };
};

const reachableValues = (root: unknown): ReadonlyArray<unknown> => {
  const values: Array<unknown> = [];
  const visited = new WeakSet<object>();
  const visit = (value: unknown): void => {
    values.push(value);
    if ((typeof value !== "object" && typeof value !== "function") || value === null) return;
    if (visited.has(value)) return;
    visited.add(value);
    for (const key of Reflect.ownKeys(value)) {
      visit(key);
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor !== undefined && "value" in descriptor) visit(descriptor.value);
    }
  };
  visit(root);
  return values;
};

const exportedLogObservation = ({ cause, fiber, logLevel, message }: Logger.Options<unknown>) => ({
  message,
  level: logLevel,
  cause,
  annotations: { ...fiber.getRef(References.CurrentLogAnnotations) },
  logSpans: fiber
    .getRef(References.CurrentLogSpans)
    .map(([label, startTime]) => ({ label, startTime })),
  fiberId: fiber.id,
  currentSpan:
    fiber.currentSpan === undefined
      ? undefined
      : {
          traceId: fiber.currentSpan.traceId,
          spanId: fiber.currentSpan.spanId,
        },
});

describe("Cloudflare telemetry flush boundary", () => {
  it.effect("dispose releases an exporter gate after the flush has claimed it", () => {
    const probe = makeTelemetryProbeFixture();

    return Effect.gen(function* () {
      const telemetry = yield* CloudflareRuntimeTelemetry;
      yield* probe.withFlushBlock((block) =>
        Effect.gen(function* () {
          const flushFiber = yield* telemetry.flush.pipe(Effect.forkChild);
          yield* probe.awaitFlushBlock(block);
          expect(block.isActive()).toBe(true);
          expect(block.isCompleted()).toBe(false);

          probe.dispose();
          const exit = yield* Fiber.await(flushFiber);
          expect(Exit.isSuccess(exit)).toBe(true);
          yield* probe.awaitFlushBlockCompleted(block);
          expect(block.isCompleted()).toBe(true);
        }),
      );
    }).pipe(Effect.provide(probe.layer), Effect.ensuring(Effect.sync(() => probe.dispose())));
  });

  it.effect("releases a global Workerd registration when its Effect scope fails", () => {
    const first = makeTelemetryProbeFixture();
    const second = makeTelemetryProbeFixture();
    const conversationId = "scoped-registration-after-failure";
    const expected = new Error("registration owner failed");

    return Effect.gen(function* () {
      const failedExit = yield* first
        .registerConversation(conversationId)
        .pipe(Effect.andThen(Effect.fail(expected)), Effect.scoped, Effect.exit);
      if (Exit.isSuccess(failedExit)) throw new Error("Expected the registration owner to fail");
      expect(failedExit.cause.reasons).toHaveLength(1);
      expect(failedExit.cause.reasons[0]?._tag).toBe("Fail");
      if (failedExit.cause.reasons[0]?._tag !== "Fail") {
        throw new Error("Expected the exact registration-owner failure");
      }
      expect(failedExit.cause.reasons[0].error).toBe(expected);

      yield* Effect.scoped(second.registerConversation(conversationId));
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          first.dispose();
          second.dispose();
        }),
      ),
    );
  });

  it.effect(
    "rejects overlapping registrations without prematurely releasing the first owner",
    () => {
      const first = makeTelemetryProbeFixture();
      const second = makeTelemetryProbeFixture();
      const conversationId = "overlapping-registration-ownership";

      const expectDuplicateRegistration = (exit: Exit.Exit<void, never>) => {
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) throw new Error("Expected duplicate registration to fail");
        expect(exit.cause.reasons).toHaveLength(1);
        const reason = exit.cause.reasons[0];
        expect(reason?._tag).toBe("Die");
        if (reason?._tag !== "Die" || !(reason.defect instanceof Error)) {
          throw new Error("Expected one duplicate-registration defect");
        }
        expect(reason.defect.message).toBe(
          `Telemetry fixture already registered for ${conversationId}`,
        );
      };

      return Effect.acquireUseRelease(
        Scope.make(),
        (ownerScope) =>
          Effect.gen(function* () {
            yield* first
              .registerConversation(conversationId)
              .pipe(Effect.provideService(Scope.Scope, ownerScope));

            const sameFixtureDuplicate = yield* Effect.scoped(
              first.registerConversation(conversationId),
            ).pipe(Effect.exit);
            expectDuplicateRegistration(sameFixtureDuplicate);

            const otherFixtureWhileOwnerLives = yield* Effect.scoped(
              second.registerConversation(conversationId),
            ).pipe(Effect.exit);
            expectDuplicateRegistration(otherFixtureWhileOwnerLives);
          }),
        (ownerScope, exit) => Scope.close(ownerScope, exit),
      ).pipe(
        Effect.andThen(Effect.scoped(second.registerConversation(conversationId))),
        Effect.ensuring(
          Effect.sync(() => {
            first.dispose();
            second.dispose();
          }),
        ),
      );
    },
  );

  it("always fulfills the shared waitUntil bridge across background failures", async () => {
    const firstFailure = new Error("first background failure");
    const trailingFailure = new Error("trailing background failure");

    await expect(
      fulfillCloudflareTelemetryBackground(Promise.reject(firstFailure)),
    ).resolves.toBeUndefined();
    await expect(
      fulfillCloudflareTelemetryBackground(Promise.reject(trailingFailure)),
    ).resolves.toBeUndefined();
  });

  it("returns the exact native delivery when waitUntil registration throws synchronously", async () => {
    const deliveryFailure = new Error("native delivery failure");
    const registrationFailure = new Error("waitUntil registration failure");
    const diagnosticFailure = new Error("synchronous diagnostic failure");
    const delivery = Promise.reject(deliveryFailure);
    let registered: Promise<void> | undefined;
    let flushAttempts = 0;
    let diagnosticCalls = 0;
    let diagnosedCause: unknown;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(() => {
      flushAttempts += 1;
      return Promise.resolve();
    });

    const returned = registerCloudflareTelemetryAfterNativeSettlement(
      (background) => {
        registered = background;
        throw registrationFailure;
      },
      delivery,
      coordinator.reserve,
      (cause) => {
        diagnosticCalls += 1;
        diagnosedCause = cause;
        throw diagnosticFailure;
      },
    );

    expect(returned).toBe(delivery);
    await expect(returned).rejects.toBe(deliveryFailure);
    if (registered === undefined) throw new Error("Expected background registration attempt");
    await expect(registered).resolves.toBeUndefined();
    expect(flushAttempts).toBe(0);
    expect(diagnosticCalls).toBe(1);
    expect(diagnosedCause).toBe(registrationFailure);
  });

  it.effect("logs only a bounded waitUntil registration classification", () => {
    const registrationFailure = new Error("secret waitUntil platform rejection");
    const observations: Array<ReturnType<typeof exportedLogObservation>> = [];
    const logger = Logger.make<unknown, void>((options) => {
      observations.push(exportedLogObservation(options));
    });

    return Effect.gen(function* () {
      yield* logCloudflareWaitUntilRegistrationFailure(registrationFailure);

      expect(observations).toHaveLength(1);
      expect(observations[0]?.cause.reasons).toEqual([]);
      expect(observations[0]?.annotations).toMatchObject({
        "effect_agent.cloudflare.telemetry.failure_kind": "wait_until_registration",
      });
      const values = reachableValues(observations);
      expect(values).not.toContain(registrationFailure);
      const strings = values.filter((value) => typeof value === "string").join("\n");
      expect(strings).not.toContain(registrationFailure.message);
    }).pipe(Effect.provide(Logger.layer([logger])));
  });

  it("issues one flush attempt for a successfully registered settled delivery", async () => {
    const delivery = Promise.resolve("delivered");
    let registered: Promise<void> | undefined;
    let flushAttempts = 0;
    let diagnosticCalls = 0;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(() => {
      flushAttempts += 1;
      return Promise.resolve();
    });

    const returned = registerCloudflareTelemetryAfterNativeSettlement(
      (background) => {
        registered = background;
      },
      delivery,
      coordinator.reserve,
      () => {
        diagnosticCalls += 1;
      },
    );

    expect(returned).toBe(delivery);
    await expect(returned).resolves.toBe("delivered");
    if (registered === undefined) throw new Error("Expected successful background registration");
    await expect(registered).resolves.toBeUndefined();
    expect(flushAttempts).toBe(1);
    expect(diagnosticCalls).toBe(0);
  });

  it("flushes after a rejected delivery whose background registration succeeds", async () => {
    const deliveryFailure = new Error("native delivery rejected");
    const delivery = Promise.reject(deliveryFailure);
    let registered: Promise<void> | undefined;
    let flushAttempts = 0;
    let diagnosticCalls = 0;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(() => {
      flushAttempts += 1;
      return Promise.resolve();
    });

    const returned = registerCloudflareTelemetryAfterNativeSettlement(
      (background) => {
        registered = background;
      },
      delivery,
      coordinator.reserve,
      () => {
        diagnosticCalls += 1;
      },
    );

    expect(returned).toBe(delivery);
    await expect(returned).rejects.toBe(deliveryFailure);
    if (registered === undefined) throw new Error("Expected successful background registration");
    await expect(registered).resolves.toBeUndefined();
    expect(flushAttempts).toBe(1);
    expect(diagnosticCalls).toBe(0);
  });

  it("bounds shared waitUntil registrations across stalled first, trailing, and queued batches", async () => {
    const started = [promiseGate(), promiseGate(), promiseGate()];
    const release = [promiseGate(), promiseGate(), promiseGate()];
    const registrations: Array<Promise<void>> = [];
    let attempts = 0;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(async () => {
      const index = attempts++;
      started[index]?.resolve();
      await release[index]?.promise;
    });
    const register = (delivery: Promise<string>): Promise<string> =>
      registerCloudflareTelemetryAfterNativeSettlement(
        (background) => {
          registrations.push(background);
        },
        delivery,
        coordinator.reserve,
        () => undefined,
      );

    try {
      const firstDeliveries = Array.from({ length: 64 }, (_, index) =>
        Promise.resolve(`first-${index}`),
      );
      expect(firstDeliveries.map(register)).toEqual(firstDeliveries);
      await started[0]?.promise;
      expect(registrations).toHaveLength(1);

      const trailingDeliveries = Array.from({ length: 64 }, (_, index) =>
        Promise.resolve(`trailing-${index}`),
      );
      expect(trailingDeliveries.map(register)).toEqual(trailingDeliveries);
      expect(registrations).toHaveLength(2);
      release[0]?.resolve();
      await started[1]?.promise;

      const queuedDeliveries = Array.from({ length: 64 }, (_, index) =>
        Promise.resolve(`queued-${index}`),
      );
      expect(queuedDeliveries.map(register)).toEqual(queuedDeliveries);
      expect(registrations).toHaveLength(3);
      expect(new Set(registrations).size).toBe(3);

      release[1]?.resolve();
      await started[2]?.promise;
      release[2]?.resolve();
      await Promise.all(registrations);
      expect(attempts).toBe(3);
    } finally {
      for (const gate of release) gate.resolve();
    }
  });

  it("caps retained delivery settlements and diagnoses lossy coalescing once per batch", async () => {
    const started = promiseGate();
    const release = promiseGate();
    const droppedDelivery = promiseGate();
    let attempts = 0;
    let dropDiagnostics = 0;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(
      async () => {
        attempts += 1;
        started.resolve();
        await release.promise;
      },
      {
        onReservationDropped: () => {
          dropDiagnostics += 1;
        },
      },
    );

    const retained = Array.from({ length: MAX_CLOUDFLARE_TELEMETRY_BATCH_DELIVERIES }, () =>
      coordinator.reserve(Promise.resolve()),
    );
    const dropped = Array.from({ length: MAX_CLOUDFLARE_TELEMETRY_BATCH_DELIVERIES * 4 }, () =>
      coordinator.reserve(droppedDelivery.promise),
    );

    try {
      expect(retained.filter(({ owner }) => owner)).toHaveLength(1);
      expect(retained.every(({ dropped }) => !dropped)).toBe(true);
      expect(dropped.every((reservation) => reservation.dropped && !reservation.owner)).toBe(true);
      expect(new Set([...retained, ...dropped].map(({ background }) => background)).size).toBe(1);
      expect(dropDiagnostics).toBe(1);

      // The unresolved overflow Promise is deliberately not retained by the batch, so export can
      // start and settle without waiting for it.
      await started.promise;
      expect(attempts).toBe(1);
      release.resolve();
      await expect(retained[0]?.cycle).resolves.toBeUndefined();
      expect(dropDiagnostics).toBe(1);
    } finally {
      droppedDelivery.resolve();
      release.resolve();
    }
  });

  it("coalesces same-turn requests into the not-yet-started exporter attempt", async () => {
    const started = promiseGate();
    const release = promiseGate();
    let attempts = 0;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(async () => {
      attempts += 1;
      started.resolve();
      await release.promise;
    });

    const requests = Array.from({ length: 64 }, () => coordinator.reserve(Promise.resolve()));
    await started.promise;
    expect(attempts).toBe(1);
    expect(requests.filter(({ owner }) => owner)).toHaveLength(1);
    expect(new Set(requests.map(({ background }) => background)).size).toBe(1);
    expect(new Set(requests.map(({ cycle }) => cycle)).size).toBe(1);
    release.resolve();
    await Promise.all(requests.map(({ background }) => background));
    await requests[0]?.cycle;
    expect(attempts).toBe(1);
  });

  it("runs one active exporter attempt plus one trailing attempt for a concurrent burst", async () => {
    const started = [promiseGate(), promiseGate()];
    const release = [promiseGate(), promiseGate()];
    let attempts = 0;
    let activeAttempts = 0;
    let maximumActiveAttempts = 0;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(async () => {
      const index = attempts++;
      activeAttempts += 1;
      maximumActiveAttempts = Math.max(maximumActiveAttempts, activeAttempts);
      started[index]?.resolve();
      await release[index]?.promise;
      activeAttempts -= 1;
    });

    const first = coordinator.reserve(Promise.resolve());
    await started[0]?.promise;
    const concurrent = Array.from({ length: 64 }, () => coordinator.reserve(Promise.resolve()));
    expect(attempts).toBe(1);
    expect(concurrent.every(({ cycle }) => cycle === first.cycle)).toBe(true);
    expect(concurrent.filter(({ owner }) => owner)).toHaveLength(1);
    expect(new Set(concurrent.map(({ background }) => background)).size).toBe(1);
    expect(concurrent[0]?.background).not.toBe(first.background);

    release[0]?.resolve();
    await started[1]?.promise;
    expect(attempts).toBe(2);
    expect(maximumActiveAttempts).toBe(1);
    release[1]?.resolve();
    await Promise.all([first.background, ...concurrent.map(({ background }) => background)]);
    await first.cycle;
    expect(attempts).toBe(2);
    expect(activeAttempts).toBe(0);
  });

  it("caps each cycle at two attempts and routes trailing traffic into a fresh cycle", async () => {
    const started = [promiseGate(), promiseGate(), promiseGate()];
    const release = [promiseGate(), promiseGate(), promiseGate()];
    let attempts = 0;
    let activeAttempts = 0;
    let maximumActiveAttempts = 0;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(async () => {
      const index = attempts++;
      activeAttempts += 1;
      maximumActiveAttempts = Math.max(maximumActiveAttempts, activeAttempts);
      started[index]?.resolve();
      await release[index]?.promise;
      activeAttempts -= 1;
    });

    const firstCycle = coordinator.reserve(Promise.resolve());
    let nextCycle: ReturnType<typeof coordinator.reserve> | undefined;
    try {
      await started[0]?.promise;
      const trailing = coordinator.reserve(Promise.resolve());
      expect(trailing.cycle).toBe(firstCycle.cycle);

      release[0]?.resolve();
      await started[1]?.promise;
      nextCycle = coordinator.reserve(Promise.resolve());
      expect(nextCycle.cycle).not.toBe(firstCycle.cycle);
      let nextCycleSettled = false;
      void nextCycle.cycle.then(
        () => {
          nextCycleSettled = true;
        },
        () => {
          nextCycleSettled = true;
        },
      );
      expect(
        Array.from({ length: 64 }, () => coordinator.reserve(Promise.resolve())).every(
          ({ background, cycle }) =>
            background === nextCycle?.background && cycle === nextCycle.cycle,
        ),
      ).toBe(true);

      release[1]?.resolve();
      await started[2]?.promise;
      await expect(firstCycle.cycle).resolves.toBeUndefined();
      expect(activeAttempts).toBe(1);
      expect(nextCycleSettled).toBe(false);

      release[2]?.resolve();
      await expect(nextCycle.cycle).resolves.toBeUndefined();
      expect(attempts).toBe(3);
      expect(maximumActiveAttempts).toBe(1);
      expect(activeAttempts).toBe(0);
    } finally {
      for (const gate of release) gate.resolve();
    }
  });

  it("retains both exporter failures from a capped two-attempt cycle", async () => {
    const firstStarted = promiseGate();
    const firstRelease = promiseGate();
    const firstFailure = new Error("first exporter failure");
    const trailingFailure = new Error("trailing exporter failure");
    let attempts = 0;
    const coordinator = makeCloudflareTelemetryFlushCoordinator(async () => {
      attempts += 1;
      if (attempts === 1) {
        firstStarted.resolve();
        await firstRelease.promise;
        throw firstFailure;
      }
      throw trailingFailure;
    });

    const cycle = coordinator.reserve(Promise.resolve());
    await firstStarted.promise;
    expect(coordinator.reserve(Promise.resolve()).cycle).toBe(cycle.cycle);
    firstRelease.resolve();

    let rejection: unknown;
    try {
      await cycle.cycle;
    } catch (cause) {
      rejection = cause;
    }
    expect(rejection).toBeInstanceOf(AggregateError);
    if (!(rejection instanceof AggregateError)) throw new Error("Expected the bounded aggregate");
    expect(rejection.errors).toEqual([firstFailure, trailingFailure]);
    expect(attempts).toBe(2);
  });

  it("rejects directly with the sole exporter failure from a one-attempt cycle", async () => {
    const failure = new Error("sole exporter failure");
    const coordinator = makeCloudflareTelemetryFlushCoordinator(() => Promise.reject(failure));

    await expect(coordinator.reserve(Promise.resolve()).cycle).rejects.toBe(failure);
  });

  it("starts a fresh attempt for a request reentered at the final-settlement boundary", async () => {
    let attempts = 0;
    let requestAtSettlement:
      | ReturnType<ReturnType<typeof makeCloudflareTelemetryFlushCoordinator>["reserve"]>
      | undefined;
    let coordinator: ReturnType<typeof makeCloudflareTelemetryFlushCoordinator>;
    coordinator = makeCloudflareTelemetryFlushCoordinator(
      () => {
        attempts += 1;
        return Promise.resolve();
      },
      {
        onSettling: () => {
          if (requestAtSettlement === undefined) {
            requestAtSettlement = coordinator.reserve(Promise.resolve());
          }
        },
      },
    );

    const first = coordinator.reserve(Promise.resolve());
    await first.cycle;
    if (requestAtSettlement === undefined) {
      throw new Error("Expected a request from the synchronous settlement hook");
    }
    expect(requestAtSettlement.cycle).not.toBe(first.cycle);
    await requestAtSettlement.cycle;
    expect(attempts).toBe(2);
  });

  it.effect(
    "logs only bounded failure classes while preserving exact typed, defect, and interrupt exits",
    () => {
      const logs: Array<{
        readonly message: unknown;
        readonly cause: Cause.Cause<unknown>;
        readonly annotations: Readonly<Record<string, unknown>>;
      }> = [];
      const logger = Logger.make<unknown, void>(({ cause, fiber, message }) => {
        logs.push({
          message,
          cause,
          annotations: fiber.getRef(References.CurrentLogAnnotations),
        });
      });
      const foreignCause = new Error("typed exporter diagnostic");
      const expectedError = CloudflareTelemetryExportError.make({ cause: foreignCause });
      const defect = new Error("exporter defect diagnostic");

      return Effect.gen(function* () {
        const typedExit = yield* flushCloudflareRuntimeTelemetry(20).pipe(
          Effect.provideService(
            CloudflareRuntimeTelemetry,
            CloudflareRuntimeTelemetry.of({ flush: Effect.fail(expectedError) }),
          ),
          Effect.exit,
        );
        if (Exit.isSuccess(typedExit)) throw new Error("Expected the typed exporter failure");
        expect(typedExit.cause.reasons).toEqual([Cause.makeFailReason(expectedError)]);

        const defectExit = yield* flushCloudflareRuntimeTelemetry(20).pipe(
          Effect.provideService(
            CloudflareRuntimeTelemetry,
            CloudflareRuntimeTelemetry.of({ flush: Effect.die(defect) }),
          ),
          Effect.exit,
        );
        if (Exit.isSuccess(defectExit)) throw new Error("Expected the exporter defect");
        expect(defectExit.cause.reasons).toEqual([Cause.makeDieReason(defect)]);

        const flushStarted = yield* Deferred.make<void>();
        const interruptedFiber = yield* flushCloudflareRuntimeTelemetry(20).pipe(
          Effect.provideService(
            CloudflareRuntimeTelemetry,
            CloudflareRuntimeTelemetry.of({
              flush: Deferred.succeed(flushStarted, undefined).pipe(Effect.andThen(Effect.never)),
            }),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(flushStarted);
        yield* Fiber.interrupt(interruptedFiber);
        const interruptedExit = yield* Fiber.await(interruptedFiber);
        if (Exit.isSuccess(interruptedExit)) throw new Error("Expected external interruption");
        expect(interruptedExit.cause.reasons).toHaveLength(1);
        expect(Cause.isInterruptReason(interruptedExit.cause.reasons[0]!)).toBe(true);

        const loggedValues = reachableValues(logs);
        expect(loggedValues).not.toContain(expectedError);
        expect(loggedValues).not.toContain(foreignCause);
        expect(loggedValues).not.toContain(defect);
        const loggedStrings = loggedValues.filter((value) => typeof value === "string").join("\n");
        expect(loggedStrings).not.toContain(foreignCause.message);
        expect(loggedStrings).not.toContain(defect.message);
        expect(loggedStrings).toContain("Cloudflare telemetry flush interrupted");
        expect(logs.map(({ cause }) => cause.reasons)).toEqual([[], [], []]);
        expect(
          logs
            .map(({ annotations }) => annotations["effect_agent.cloudflare.telemetry.failure_kind"])
            .toSorted(),
        ).toEqual(["defect", "exporter", "interrupted"]);
      }).pipe(Effect.provide(Logger.layer([logger])));
    },
  );

  it.effect("interrupts a cooperative stalled exporter at the configured background budget", () =>
    Effect.gen(function* () {
      const flushStarted = yield* Deferred.make<void>();
      const flushInterrupted = yield* Deferred.make<void>();
      const flush = Deferred.succeed(flushStarted, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() => Deferred.succeed(flushInterrupted, undefined)),
      );
      const fiber = yield* flushCloudflareRuntimeTelemetry(20).pipe(
        Effect.provideService(CloudflareRuntimeTelemetry, CloudflareRuntimeTelemetry.of({ flush })),
        Effect.forkChild,
      );

      yield* Deferred.await(flushStarted);
      expect(fiber.pollUnsafe()).toBeUndefined();
      yield* TestClock.adjust(19);
      expect(fiber.pollUnsafe()).toBeUndefined();
      expect(yield* Deferred.isDone(flushInterrupted)).toBe(false);

      yield* TestClock.adjust(1);
      const exit = yield* Fiber.await(fiber);
      if (Exit.isSuccess(exit)) throw new Error("Expected the cooperative flush timeout");
      expect(exit.cause.reasons).toHaveLength(1);
      const timeoutReason = exit.cause.reasons[0]!;
      expect(Cause.isFailReason(timeoutReason)).toBe(true);
      if (!Cause.isFailReason(timeoutReason)) throw new Error("Expected only a timeout failure");
      expect(timeoutReason.error._tag).toBe("TimeoutError");
      expect(yield* Deferred.isDone(flushInterrupted)).toBe(true);
    }),
  );

  it.effect("does not claim the cooperative budget can interrupt an uninterruptible exporter", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const flushStarted = yield* Deferred.make<void>();
        const releaseFlush = yield* Effect.acquireRelease(Deferred.make<void>(), (release) =>
          Deferred.succeed(release, undefined).pipe(Effect.asVoid),
        );
        const flush = Deferred.succeed(flushStarted, undefined).pipe(
          Effect.andThen(Deferred.await(releaseFlush)),
          Effect.uninterruptible,
        );
        const fiber = yield* flushCloudflareRuntimeTelemetry(20).pipe(
          Effect.provideService(
            CloudflareRuntimeTelemetry,
            CloudflareRuntimeTelemetry.of({ flush }),
          ),
          Effect.forkChild,
        );

        yield* Deferred.await(flushStarted);
        yield* TestClock.adjust(20);
        expect(fiber.pollUnsafe()).toBeUndefined();

        yield* Deferred.succeed(releaseFlush, undefined);
        const exit = yield* Fiber.await(fiber);
        if (Exit.isSuccess(exit)) throw new Error("Expected the delayed cooperative timeout");
        expect(exit.cause.reasons).toHaveLength(1);
        const timeoutReason = exit.cause.reasons[0]!;
        expect(Cause.isFailReason(timeoutReason)).toBe(true);
        if (!Cause.isFailReason(timeoutReason)) throw new Error("Expected only a timeout failure");
        expect(timeoutReason.error._tag).toBe("TimeoutError");
      }),
    ),
  );

  it.effect("releases a blocked same-scope child before propagating bracket body failure", () => {
    const probe = makeTelemetryProbeFixture();
    return Effect.gen(function* () {
      const bodyFailure = new Error("flush block bracket body failed");
      let observedBlock: TelemetryFlushBlock | undefined;
      let observedFiber: Fiber.Fiber<void, CloudflareTelemetryExportError> | undefined;

      const bodyExit = yield* probe
        .withFlushBlock((block) =>
          Effect.gen(function* () {
            observedBlock = block;
            const telemetry = yield* CloudflareRuntimeTelemetry;
            observedFiber = yield* telemetry.flush.pipe(Effect.forkChild);
            yield* probe.awaitFlushBlock(block);
            return yield* Effect.fail(bodyFailure);
          }),
        )
        .pipe(Effect.provide(probe.layer), Effect.exit);

      if (Exit.isSuccess(bodyExit)) throw new Error("Expected the bracket body failure");
      expect(bodyExit.cause.reasons).toEqual([Cause.makeFailReason(bodyFailure)]);
      if (observedBlock === undefined || observedFiber === undefined) {
        throw new Error("Expected the bracket body to start its blocked flush child");
      }
      expect(Exit.isSuccess(yield* Fiber.await(observedFiber))).toBe(true);
      yield* probe.awaitFlushBlockCompleted(observedBlock);
      expect(observedBlock.isCompleted()).toBe(true);
    }).pipe(Effect.ensuring(Effect.sync(probe.dispose)));
  });

  it.effect("interrupts a bracket body that directly executes a blocked flush", () => {
    const probe = makeTelemetryProbeFixture();
    return Effect.gen(function* () {
      const blockReady = yield* Deferred.make<TelemetryFlushBlock>();
      const bracketFiber = yield* probe
        .withFlushBlock((block) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(blockReady, block);
            const telemetry = yield* CloudflareRuntimeTelemetry;
            return yield* telemetry.flush;
          }),
        )
        .pipe(Effect.provide(probe.layer), Effect.forkChild);

      const block = yield* Deferred.await(blockReady);
      yield* probe.awaitFlushBlock(block);
      yield* Fiber.interrupt(bracketFiber).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const bracketExit = yield* Fiber.await(bracketFiber);
      if (Exit.isSuccess(bracketExit)) throw new Error("Expected the bracket body interruption");
      expect(bracketExit.cause.reasons).toHaveLength(1);
      expect(Cause.isInterruptReason(bracketExit.cause.reasons[0]!)).toBe(true);
      yield* probe.awaitFlushBlockCompleted(block);
      expect(block.isCompleted()).toBe(true);
    }).pipe(Effect.ensuring(Effect.sync(probe.dispose)));
  });

  it.effect("dequeues a bracketed flush block released before any exporter claims it", () => {
    const probe = makeTelemetryProbeFixture();
    return Effect.gen(function* () {
      const unusedObservation = yield* probe.withFlushBlock((unused) =>
        Effect.sync(() => {
          unused.release();
          return unused;
        }),
      );

      yield* probe.withFlushBlock((claimed) =>
        Effect.gen(function* () {
          const telemetry = yield* CloudflareRuntimeTelemetry;
          const flushFiber = yield* telemetry.flush.pipe(Effect.forkChild);

          yield* probe.awaitFlushBlock(claimed);
          expect(unusedObservation.isActive()).toBe(false);
          expect(unusedObservation.isCompleted()).toBe(false);
          expect(claimed.isActive()).toBe(true);
          claimed.release();
          expect(Exit.isSuccess(yield* Fiber.await(flushFiber))).toBe(true);
          yield* probe.awaitFlushBlockCompleted(claimed);
          expect(claimed.isCompleted()).toBe(true);
        }),
      );
    }).pipe(Effect.provide(probe.layer), Effect.ensuring(Effect.sync(probe.dispose)));
  });

  it.effect("keeps delayed exporter controls isolated between per-test fixtures", () => {
    const staleProbe = makeTelemetryProbeFixture();
    const currentProbe = makeTelemetryProbeFixture();
    return Effect.gen(function* () {
      const staleAttempt = staleProbe.expectFlushAttempt();
      const currentAttempt = currentProbe.expectFlushAttempt();
      const staleTelemetry = yield* CloudflareRuntimeTelemetry.pipe(
        Effect.provide(staleProbe.layer),
      );
      const currentTelemetry = yield* CloudflareRuntimeTelemetry.pipe(
        Effect.provide(currentProbe.layer),
      );
      const startStale = yield* Deferred.make<void>();
      const staleFiber = yield* Deferred.await(startStale).pipe(
        Effect.andThen(staleTelemetry.flush),
        Effect.forkChild,
      );
      const currentFailure = new Error("current-fixture exporter failure");
      currentProbe.failNextFlush(currentFailure);

      yield* currentProbe.withFlushBlock((currentBlock) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(startStale, undefined);
          yield* Fiber.join(staleFiber);
          yield* staleProbe.awaitFlushAttempt(staleAttempt);
          expect(currentAttempt.isActive()).toBe(false);
          expect(currentBlock.isActive()).toBe(false);
          expect(currentProbe.exportErrors).toEqual([]);

          const currentFiber = yield* currentTelemetry.flush.pipe(Effect.forkChild);
          yield* currentProbe.awaitFlushBlock(currentBlock);
          currentBlock.release();
          const currentExit = yield* Fiber.await(currentFiber);
          if (Exit.isSuccess(currentExit)) throw new Error("Expected the tagged current failure");
          expect(currentExit.cause.reasons).toHaveLength(1);
          const failureReason = currentExit.cause.reasons[0]!;
          expect(Cause.isFailReason(failureReason)).toBe(true);
          if (!Cause.isFailReason(failureReason)) {
            throw new Error("Expected only the typed export failure");
          }
          expect(failureReason.error).toMatchObject({
            _tag: "CloudflareTelemetryExportError",
            cause: currentFailure,
          });
          yield* currentProbe.awaitFlushAttempt(currentAttempt);
          yield* currentProbe.awaitFlushBlockCompleted(currentBlock);
          expect(currentProbe.exportErrors.map(({ cause }) => cause)).toEqual([currentFailure]);
          expect(staleProbe.exportErrors).toEqual([]);
        }),
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          staleProbe.dispose();
          currentProbe.dispose();
        }),
      ),
    );
  });

  it.effect("restores the original Cause plus residual trace-finalizer reasons", () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const expected = new Error("typed endpoint secret");
    const finalizerDefect = new Error("endpoint finalizer secret");
    const finalizerDefectReason = Cause.makeDieReason(finalizerDefect).annotate(
      Context.make(TestCauseSource, "native-span-finalizer"),
    );
    const finalizerInterruptReason = Cause.makeInterruptReason(4242).annotate(
      Context.make(TestCauseSource, "native-span-interrupt"),
    );

    return Effect.gen(function* () {
      const exit = yield* withCloudflareNativeSpanFailure(Effect.fail(expected), (masked) =>
        masked.pipe(
          Effect.withSpan("cloudflare-native-marker-test"),
          Effect.onExit((spanExit) =>
            Exit.isFailure(spanExit)
              ? Effect.failCause(
                  Cause.combine(
                    spanExit.cause,
                    Cause.fromReasons([finalizerDefectReason, finalizerInterruptReason]),
                  ),
                )
              : Effect.void,
          ),
        ),
      ).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isSuccess(exit)) throw new Error("Expected the composed endpoint Cause");
      expect(exit.cause.reasons).toHaveLength(3);
      const restoredFailureReason = exit.cause.reasons[0]!;
      expect(Cause.isFailReason(restoredFailureReason)).toBe(true);
      if (!Cause.isFailReason(restoredFailureReason)) {
        throw new Error("Expected the restored typed failure first");
      }
      expect(restoredFailureReason.error).toBe(expected);
      expect(exit.cause.reasons[1]).toBe(finalizerDefectReason);
      expect(exit.cause.reasons[2]).toBe(finalizerInterruptReason);
      expect([...exit.cause.reasons[1]!.annotations.values()]).toContain("native-span-finalizer");
      expect([...exit.cause.reasons[2]!.annotations.values()]).toContain("native-span-interrupt");
      expect(Cause.interruptors(exit.cause)).toEqual(new Set([4242]));
      const publicFailureText = Cause.pretty(exit.cause);
      expect(publicFailureText).not.toContain("ConversationObjectDeliveryFailed");
      expect(publicFailureText).not.toContain("Cloudflare Conversation Object delivery failed");

      const span = spans.find(({ name }) => name === "cloudflare-native-marker-test");
      if (span?.status._tag !== "Ended" || !Exit.isFailure(span.status.exit)) {
        throw new Error("Expected the native marker span to end failed");
      }
      const spanFailureText = Cause.pretty(span.status.exit.cause);
      expect(spanFailureText).toContain("Cloudflare Conversation Object delivery failed");
      expect(spanFailureText).not.toContain(expected.message);
      expect(spanFailureText).not.toContain(finalizerDefect.message);
    }).pipe(Effect.provideService(Tracer.Tracer, tracer));
  });

  it.effect("preserves a marker-free native span Cause by referential identity", () => {
    const failureReason = Cause.makeFailReason(new Error("ordinary native failure")).annotate(
      Context.make(TestCauseSource, "trace"),
    );
    const defectReason = Cause.makeDieReason(new Error("ordinary native defect"));
    const markerFree = Cause.fromReasons([failureReason, defectReason]);

    return Effect.gen(function* () {
      const exit = yield* withCloudflareNativeSpanFailure(Effect.void, () =>
        Effect.failCause(markerFree),
      ).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isSuccess(exit)) throw new Error("Expected the marker-free native Cause");
      expect(exit.cause).toBe(markerFree);
      expect(exit.cause.reasons[0]).toBe(failureReason);
      expect([...exit.cause.reasons[0]!.annotations.values()]).toContain("trace");
      expect(exit.cause.reasons[1]).toBe(defectReason);
    });
  });
});

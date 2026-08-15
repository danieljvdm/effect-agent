import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Cause, Effect, Exit, Scope } from "effect";
import { afterEach, beforeEach, describe, expect, it } from "vite-plus/test";

import {
  makeTelemetryProbeFixture,
  TelemetryLayerAcquisitionError,
  type TelemetryFlushBlock,
  type TelemetryObservation,
  type TelemetryProbeFixture,
  type TelemetrySpanObservation,
} from "./telemetry-fixtures.ts";

let conversationCounter = 0;
let telemetryFixture: TelemetryProbeFixture;
let telemetryFixtureScope: Scope.Closeable;
let telemetryObservations: Array<TelemetryObservation>;
let telemetryExportErrors: TelemetryProbeFixture["exportErrors"];
let telemetryLayerAcquisitionErrors: TelemetryProbeFixture["acquisitionErrors"];

const conversation = (label: string): string => {
  const conversationId = `cf-telemetry-${label}-${conversationCounter++}`;
  Effect.runSync(
    telemetryFixture
      .registerConversation(conversationId)
      .pipe(Effect.provideService(Scope.Scope, telemetryFixtureScope)),
  );
  return conversationId;
};

const expectTelemetryFlushAttempt = () => telemetryFixture.expectFlushAttempt();
const awaitTelemetryFlushAttempt: TelemetryProbeFixture["awaitFlushAttempt"] = (...args) =>
  telemetryFixture.awaitFlushAttempt(...args);
const awaitTelemetryFlushBlock: TelemetryProbeFixture["awaitFlushBlock"] = (...args) =>
  telemetryFixture.awaitFlushBlock(...args);
const awaitTelemetryFlushBlockCompleted: TelemetryProbeFixture["awaitFlushBlockCompleted"] = (
  ...args
) => telemetryFixture.awaitFlushBlockCompleted(...args);
const awaitTelemetryObservationCount: TelemetryProbeFixture["awaitObservationCount"] = (...args) =>
  telemetryFixture.awaitObservationCount(...args);
const failNextTelemetryFlush = (cause: unknown): void => telemetryFixture.failNextFlush(cause);
const withTelemetryFlushBlock: TelemetryProbeFixture["withFlushBlock"] = (use) =>
  telemetryFixture.withFlushBlock(use);

const observationsFor = (
  conversationId: string,
  entrypoint: string,
): ReadonlyArray<TelemetrySpanObservation> =>
  telemetryObservations.filter(
    (observation): observation is TelemetrySpanObservation =>
      observation.phase === "span-ended" &&
      observation.conversationId === conversationId &&
      observation.entrypoint === entrypoint,
  );

const observeDelivery = async <A>(delivery: () => Promise<A>) => {
  const start = telemetryObservations.length;
  const attempt = expectTelemetryFlushAttempt();
  const result = await delivery();
  await Effect.runPromise(awaitTelemetryFlushAttempt(attempt));
  return {
    result,
    observations: telemetryObservations.slice(start),
  } satisfies { readonly result: A; readonly observations: ReadonlyArray<TelemetryObservation> };
};

const settle = <A>(delivery: Promise<A>): Promise<Exit.Exit<A, unknown>> =>
  delivery.then(Exit.succeed, Exit.fail);

const awaitSettlement = <A>(
  settlement: Promise<Exit.Exit<A, unknown>>,
): Promise<Exit.Exit<A, unknown>> =>
  Effect.runPromise(Effect.promise(() => settlement).pipe(Effect.timeout("5 seconds")));

/** Own an external Workerd exporter gate for the complete asynchronous test body. */
const withWorkerdTelemetryFlushBlock = <A>(
  use: (block: TelemetryFlushBlock) => Promise<A>,
): Effect.Effect<A, Cause.UnknownError> =>
  withTelemetryFlushBlock((block) => Effect.tryPromise(() => use(block)));

/** Own both coordinator gates in one scope so either test failure releases both. */
const withTelemetryFlushBlockPair = <A>(
  use: (active: TelemetryFlushBlock, trailing: TelemetryFlushBlock) => Promise<A>,
): Effect.Effect<A, Cause.UnknownError> =>
  withTelemetryFlushBlock((active) =>
    withTelemetryFlushBlock((trailing) => Effect.tryPromise(() => use(active, trailing))),
  );

/** Own a first/trailing/queued coordinator sequence under one cleanup-safe bracket tree. */
const withTelemetryFlushBlockTriple = <A>(
  use: (
    active: TelemetryFlushBlock,
    trailing: TelemetryFlushBlock,
    queued: TelemetryFlushBlock,
  ) => Promise<A>,
): Effect.Effect<A, Cause.UnknownError> =>
  withTelemetryFlushBlock((active) =>
    withTelemetryFlushBlock((trailing) =>
      withTelemetryFlushBlock((queued) => Effect.tryPromise(() => use(active, trailing, queued))),
    ),
  );

/** Traverse own data properties, including non-enumerable Error fields such as `message`. */
const reachableObservationValues = (root: unknown): ReadonlyArray<unknown> => {
  const values: Array<unknown> = [];
  const visited = new WeakSet<object>();
  const visit = (value: unknown): void => {
    values.push(value);
    if ((typeof value !== "object" && typeof value !== "function") || value === null) return;
    if (visited.has(value)) return;
    visited.add(value);
    if (value instanceof Map) {
      for (const [key, item] of value) {
        visit(key);
        visit(item);
      }
    } else if (value instanceof Set) {
      for (const item of value) visit(item);
    }
    for (const key of Reflect.ownKeys(value)) {
      visit(key);
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor !== undefined && "value" in descriptor) visit(descriptor.value);
    }
  };
  visit(root);
  return values;
};

/** Normalize only the generated lane identity when comparing otherwise exact encoded results. */
const remapEncodedIdentity = (value: unknown, from: string, to: string): unknown => {
  if (value === from) return to;
  if (Array.isArray(value)) {
    return value.map((item) => remapEncodedIdentity(item, from, to));
  }
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, remapEncodedIdentity(item, from, to)]),
    );
  }
  return value;
};

describe("Cloudflare native telemetry lifecycle", () => {
  beforeEach(() => {
    telemetryFixtureScope = Scope.makeUnsafe();
    telemetryFixture = makeTelemetryProbeFixture();
    const ownedFixture = telemetryFixture;
    Effect.runSync(
      Scope.addFinalizer(
        telemetryFixtureScope,
        Effect.sync(() => ownedFixture.dispose()),
      ),
    );
    telemetryObservations = telemetryFixture.observations;
    telemetryExportErrors = telemetryFixture.exportErrors;
    telemetryLayerAcquisitionErrors = telemetryFixture.acquisitionErrors;
  });

  afterEach(() => Effect.runPromise(Scope.close(telemetryFixtureScope, Exit.void)));

  it("provides the DO context to host telemetry and retains typed acquisition failure", async () => {
    const conversationId = conversation("telemetry-acquisition-failure");
    const stub = env.TELEMETRY_ACQUISITION.get(
      env.TELEMETRY_ACQUISITION.idFromName(conversationId),
    );

    const rejection = await stub.wake().then(
      () => undefined,
      (cause: unknown) => cause,
    );

    // Workerd transports a constructor-gate failure as a reset Error, while the Layer-side probe
    // retains the exact typed value that ManagedRuntime rejected with.
    if (!(rejection instanceof Error)) throw new Error("Expected the typed acquisition rejection");
    expect(rejection.message).toBe("TelemetryLayerAcquisitionError");
    expect(Object.getOwnPropertyDescriptor(rejection, "durableObjectReset")?.value).toBe(true);
    expect(telemetryLayerAcquisitionErrors).toHaveLength(1);
    expect(telemetryLayerAcquisitionErrors[0]).toBeInstanceOf(TelemetryLayerAcquisitionError);
    expect(telemetryLayerAcquisitionErrors[0]).toMatchObject({
      _tag: "TelemetryLayerAcquisitionError",
      envHasTelemetryBinding: true,
    });
  });

  it("preserves every native RPC and wake result while flushing content-free owner spans", async () => {
    const conversationId = conversation("native-success");
    const controlConversationId = conversation("native-control");
    const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));
    const controlStub = env.TELEMETRY.get(env.TELEMETRY.idFromName(controlConversationId));
    const marker = "encoded-request-must-not-be-exported";
    const protocolFailure = (
      responseTag: "HostFailed" | "AdminFailed" | "PortFailed",
      failureTag: "HostProtocolError" | "PortProtocolError",
    ): unknown => ({
      _tag: responseTag,
      failure: { _tag: failureTag, message: expect.any(String) },
    });
    const calls: ReadonlyArray<{
      readonly entrypoint: string;
      readonly expected: unknown;
      readonly containsConversationIdentity?: true;
      readonly call: (target: typeof stub) => Promise<unknown>;
    }> = [
      {
        entrypoint: "submit",
        expected: protocolFailure("HostFailed", "HostProtocolError"),
        call: (target) => target.submitEncoded(marker),
      },
      {
        entrypoint: "await_settlement",
        expected: protocolFailure("HostFailed", "HostProtocolError"),
        call: (target) => target.awaitSettlementEncoded(marker),
      },
      {
        entrypoint: "observe",
        expected: protocolFailure("HostFailed", "HostProtocolError"),
        call: (target) => target.observePage(marker),
      },
      {
        entrypoint: "abort",
        expected: protocolFailure("HostFailed", "HostProtocolError"),
        call: (target) => target.abortEncoded(marker),
      },
      {
        entrypoint: "resolve_approval",
        expected: protocolFailure("HostFailed", "HostProtocolError"),
        call: (target) => target.resolveApprovalEncoded(marker),
      },
      {
        entrypoint: "resolve_unknown",
        expected: protocolFailure("HostFailed", "HostProtocolError"),
        call: (target) => target.resolveUnknownEncoded(marker),
      },
      {
        entrypoint: "explain",
        expected: { _tag: "ExplainedRecovery", explanations: [] },
        call: (target) => target.explainEncoded({ marker }),
      },
      {
        entrypoint: "verify",
        expected: {
          _tag: "AdminFailed",
          failure: { _tag: "ConversationNotMaterialized", conversationId },
        },
        containsConversationIdentity: true,
        call: (target) => target.verifyEncoded({ marker }),
      },
      {
        entrypoint: "retry",
        expected: protocolFailure("AdminFailed", "HostProtocolError"),
        call: (target) => target.retryEncoded(marker),
      },
      {
        entrypoint: "obligations",
        expected: protocolFailure("AdminFailed", "HostProtocolError"),
        call: (target) => target.obligationsEncoded(marker),
      },
      {
        entrypoint: "port_call",
        expected: protocolFailure("PortFailed", "PortProtocolError"),
        call: (target) => target.portCall(marker),
      },
    ];

    for (const { entrypoint, expected, containsConversationIdentity, call } of calls) {
      // The bounded Schema diagnostics are renderer-owned; the successful-flush control proves
      // exact envelope preservation without coupling this lifecycle test to diagnostic wording.
      const { result: controlResult } = await observeDelivery(() => call(controlStub));
      const { result, observations } = await observeDelivery(() => call(stub));
      expect(result).toEqual(
        containsConversationIdentity === true
          ? remapEncodedIdentity(controlResult, controlConversationId, conversationId)
          : controlResult,
      );
      expect(result).toEqual(expected);
      expect(observations.map(({ phase }) => phase)).toEqual([
        "span-ended",
        "flush-started",
        "flush-completed",
      ]);
      const span = observations.find(
        (observation): observation is TelemetrySpanObservation =>
          observation.phase === "span-ended",
      );
      expect(span).toMatchObject({
        failed: false,
        spanName: `effect_agent.cloudflare.conversation_object.${entrypoint}`,
        kind: "server",
        attributes: {
          "effect_agent.cloudflare.entrypoint": entrypoint,
          "effect_agent.deployment.id": "cf-test-deployment",
          conversationId,
          producerId: `cf-test-producer:${conversationId}`,
        },
      });
      expect(Object.keys(span?.attributes ?? {}).toSorted()).toEqual([
        "conversationId",
        "effect_agent.cloudflare.entrypoint",
        "effect_agent.deployment.id",
        "producerId",
      ]);
      expect(span?.events).toEqual([]);
      expect(span?.links).toEqual([]);
    }

    const { result: wakeResult, observations: wakeObservations } = await observeDelivery(() =>
      stub.wake(),
    );
    expect(wakeResult).toBeUndefined();
    expect(wakeObservations.map(({ phase }) => phase)).toEqual([
      "span-ended",
      "flush-started",
      "flush-completed",
    ]);
    const wakeSpan = observationsFor(conversationId, "wake")[0];
    expect(wakeSpan?.events).toEqual([]);
    expect(wakeSpan?.links).toEqual([]);
    const exportedValues = reachableObservationValues(telemetryObservations);
    expect(exportedValues).not.toContain(marker);
    expect(exportedValues.filter((value) => typeof value === "string").join("\n")).not.toContain(
      marker,
    );
  });

  it("does not hold an RPC response on a blocked background flush", () =>
    Effect.runPromise(
      withWorkerdTelemetryFlushBlock(async (flushBlock) => {
        const conversationId = conversation("uninterruptible-rpc");
        const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));
        const observationStart = telemetryObservations.length;
        const deliverySettlement = settle(stub.submitEncoded("uninterruptible-rpc-marker"));

        await Effect.runPromise(awaitTelemetryObservationCount(observationStart + 2));
        await Effect.runPromise(awaitTelemetryFlushBlock(flushBlock));
        expect(flushBlock.isActive()).toBe(true);
        const deliveryExit = await awaitSettlement(deliverySettlement);
        if (Exit.isFailure(deliveryExit)) {
          throw new Error("Expected the RPC response while its exporter remained pending");
        }
        expect(deliveryExit.value).toMatchObject({
          _tag: "HostFailed",
          failure: { _tag: "HostProtocolError" },
        });
        expect(flushBlock.isCompleted()).toBe(false);
        expect(observationsFor(conversationId, "submit")).toHaveLength(1);

        flushBlock.release();
        await Effect.runPromise(awaitTelemetryFlushBlockCompleted(flushBlock));
        expect(flushBlock.isCompleted()).toBe(true);
      }),
    ));

  it("releases an external exporter gate when its owning test scope fails", async () => {
    const conversationId = conversation("scoped-gate-cleanup");
    const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));
    const expectedFailure = new Error("intentional scoped gate cleanup failure");
    let claimedBlock: TelemetryFlushBlock | undefined;

    const exit = await Effect.runPromiseExit(
      withWorkerdTelemetryFlushBlock(async (flushBlock) => {
        claimedBlock = flushBlock;
        const deliverySettlement = settle(stub.submitEncoded("scoped-gate-cleanup-marker"));
        await Effect.runPromise(awaitTelemetryFlushBlock(flushBlock));
        const deliveryExit = await awaitSettlement(deliverySettlement);
        if (Exit.isFailure(deliveryExit)) {
          throw new Error("Expected the cleanup-probe RPC response");
        }
        throw expectedFailure;
      }),
    );

    if (Exit.isSuccess(exit)) throw new Error("Expected the scoped test body to fail");
    const failures = exit.cause.reasons.filter(Cause.isFailReason).map(({ error }) => error);
    expect(failures).toHaveLength(1);
    const failure = failures[0];
    expect(Cause.isUnknownError(failure)).toBe(true);
    if (!Cause.isUnknownError(failure)) throw new Error("Expected the tryPromise boundary error");
    expect(failure.cause).toBe(expectedFailure);
    if (claimedBlock === undefined) throw new Error("Expected the fixture gate to be acquired");
    await Effect.runPromise(awaitTelemetryFlushBlockCompleted(claimedBlock));
    expect(claimedBlock.isCompleted()).toBe(true);
  });

  it("preserves a failed alarm retry signal while its background flush is pending", () =>
    Effect.runPromise(
      withWorkerdTelemetryFlushBlock(async (flushBlock) => {
        const conversationId = conversation("alarm-failure");
        const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));
        try {
          await runInDurableObject(stub, (_instance, state) => {
            state.storage.sql.exec(
              "ALTER TABLE effect_agent_submissions RENAME TO effect_agent_submissions_missing",
            );
            return state.storage.setAlarm(Date.UTC(2100, 0, 1));
          });

          const observationStart = telemetryObservations.length;
          const alarmSettlement = settle(runDurableObjectAlarm(stub));
          let alarmFailure: Error | undefined;

          await Effect.runPromise(awaitTelemetryObservationCount(observationStart + 2));
          await Effect.runPromise(awaitTelemetryFlushBlock(flushBlock));
          expect(flushBlock.isActive()).toBe(true);
          const alarmExit = await awaitSettlement(alarmSettlement);
          if (Exit.isSuccess(alarmExit)) {
            throw new Error("Expected the alarm delivery to preserve its rejection");
          }
          const failures = alarmExit.cause.reasons
            .filter(Cause.isFailReason)
            .map(({ error }) => error);
          expect(failures).toHaveLength(1);
          const failure = failures[0];
          if (!(failure instanceof Error)) throw new Error("Expected the alarm rejection Error");
          alarmFailure = failure;
          expect(failure.message).toContain("Failed to execute statement");
          expect(flushBlock.isCompleted()).toBe(false);
          expect(observationsFor(conversationId, "alarm")).toHaveLength(1);

          flushBlock.release();
          await Effect.runPromise(awaitTelemetryFlushBlockCompleted(flushBlock));
          const observations = telemetryObservations.slice(observationStart);
          expect(observations.map(({ phase }) => phase)).toEqual([
            "span-ended",
            "flush-started",
            "flush-completed",
          ]);
          const failedDelivery = observationsFor(conversationId, "alarm").find(
            ({ failed }) => failed,
          );
          if (failedDelivery === undefined || alarmFailure === undefined) {
            throw new Error("Expected the failed alarm observation and rejection");
          }
          expect(failedDelivery.events).toEqual([]);
          expect(failedDelivery.links).toEqual([]);
          const exportedValues = reachableObservationValues(failedDelivery);
          expect(exportedValues).not.toContain(alarmFailure);
          expect(
            exportedValues.filter((value) => typeof value === "string").join("\n"),
          ).not.toContain(alarmFailure.message);
        } finally {
          try {
            await runInDurableObject(stub, async (_instance, state) => {
              const renamed = state.storage.sql
                .exec<{ readonly name: string }>(
                  "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_agent_submissions_missing'",
                )
                .toArray();
              if (renamed.length > 0) {
                state.storage.sql.exec(
                  "ALTER TABLE effect_agent_submissions_missing RENAME TO effect_agent_submissions",
                );
              }
              await state.storage.deleteAlarm();
            });
          } finally {
            const claimed = flushBlock.isActive();
            flushBlock.release();
            if (claimed) {
              await Effect.runPromise(awaitTelemetryFlushBlockCompleted(flushBlock));
            }
          }
        }
      }),
    ));

  it("retains a typed exporter cause without changing the RPC result", async () => {
    const conversationId = conversation("flush-failure");
    const controlConversationId = conversation("flush-control");
    const foreignCause = new Error("foreign exporter diagnostic must stay out of spans");
    const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));
    const controlStub = env.TELEMETRY.get(env.TELEMETRY.idFromName(controlConversationId));
    const { result: controlResult } = await observeDelivery(() =>
      Promise.resolve(controlStub.submitEncoded({ malformed: true })),
    );
    failNextTelemetryFlush(foreignCause);
    const { result, observations } = await observeDelivery(() =>
      Promise.resolve(stub.submitEncoded({ malformed: true })),
    );

    expect(result).toEqual(controlResult);
    expect(observations.map(({ phase }) => phase)).toEqual([
      "span-ended",
      "flush-started",
      "flush-failed",
    ]);
    expect(telemetryExportErrors).toHaveLength(1);
    expect(telemetryExportErrors[0]).toMatchObject({ _tag: "CloudflareTelemetryExportError" });
    expect(telemetryExportErrors[0]?.cause).toBe(foreignCause);
    const endedObservation = observationsFor(conversationId, "submit")[0];
    expect(endedObservation?.events).toEqual([]);
    expect(endedObservation?.links).toEqual([]);
    const exportedValues = reachableObservationValues(telemetryObservations);
    expect(exportedValues.some((value) => value === foreignCause)).toBe(false);
    expect(exportedValues.filter((value) => typeof value === "string").join("\n")).not.toContain(
      foreignCause.message,
    );
  });

  it("flushes successful alarm and wake lifecycles through the same native boundary", async () => {
    const wakeConversationId = conversation("wake-success");
    const wakeStub = env.TELEMETRY.get(env.TELEMETRY.idFromName(wakeConversationId));
    const { result: wakeResult, observations: wakeObservations } = await observeDelivery(() =>
      wakeStub.wake(),
    );

    expect(wakeResult).toBeUndefined();
    expect(wakeObservations.map(({ phase }) => phase)).toEqual([
      "span-ended",
      "flush-started",
      "flush-completed",
    ]);
    expect(observationsFor(wakeConversationId, "wake")[0]?.events).toEqual([]);
    expect(observationsFor(wakeConversationId, "wake")[0]?.links).toEqual([]);

    const alarmConversationId = conversation("alarm-success");
    const alarmStub = env.TELEMETRY.get(env.TELEMETRY.idFromName(alarmConversationId));
    await runInDurableObject(alarmStub, (_instance, state) =>
      state.storage.setAlarm(Date.UTC(2100, 0, 1)),
    );
    const { alarmExit, alarmObservations, queuedWakeExit, trailingWakeExit } =
      await Effect.runPromise(
        withTelemetryFlushBlockTriple(async (activeBlock, trailingBlock, queuedBlock) => {
          const alarmObservationStart = telemetryObservations.length;
          const alarmSettlement = settle(runDurableObjectAlarm(alarmStub));
          await Effect.runPromise(awaitTelemetryFlushBlock(activeBlock));
          // Wait for every alarm delivery to settle and request export while A remains blocked. Then
          // one additional native delivery deterministically requests the single trailing export B.
          const settledAlarm = await awaitSettlement(alarmSettlement);
          const settledWake = await awaitSettlement(settle(alarmStub.wake()));
          activeBlock.release();
          await Effect.runPromise(awaitTelemetryFlushBlockCompleted(activeBlock));
          await Effect.runPromise(awaitTelemetryFlushBlock(trailingBlock));
          // A delivery while B is active deterministically promotes the coordinator's one queued
          // cycle C, covering the complete capped lifecycle without scheduler-idle inference.
          const settledQueuedWake = await awaitSettlement(settle(alarmStub.wake()));
          trailingBlock.release();
          await Effect.runPromise(awaitTelemetryFlushBlockCompleted(trailingBlock));
          await Effect.runPromise(awaitTelemetryFlushBlock(queuedBlock));
          queuedBlock.release();
          await Effect.runPromise(awaitTelemetryFlushBlockCompleted(queuedBlock));
          return {
            alarmExit: settledAlarm,
            queuedWakeExit: settledQueuedWake,
            trailingWakeExit: settledWake,
            alarmObservations: telemetryObservations.slice(alarmObservationStart),
          };
        }),
      );
    expect(alarmExit).toEqual(Exit.succeed(true));
    expect(trailingWakeExit).toEqual(Exit.succeed(undefined));
    expect(queuedWakeExit).toEqual(Exit.succeed(undefined));
    const alarmSpans = alarmObservations.filter(
      (observation): observation is TelemetrySpanObservation =>
        observation.phase === "span-ended" && observation.entrypoint === "alarm",
    );
    const alarmFlushStarted = alarmObservations.filter(
      ({ phase }) => phase === "flush-started",
    ).length;
    const alarmFlushCompleted = alarmObservations.filter(
      ({ phase }) => phase === "flush-completed",
    ).length;
    expect(alarmSpans.length).toBeGreaterThanOrEqual(1);
    expect(alarmSpans.every(({ failed }) => !failed)).toBe(true);
    expect(alarmSpans.every(({ events }) => events.length === 0)).toBe(true);
    expect(alarmSpans.every(({ links }) => links.length === 0)).toBe(true);
    expect(alarmFlushStarted).toBe(3);
    expect(alarmFlushCompleted).toBe(3);
    expect(alarmObservations.some(({ phase }) => phase === "flush-failed")).toBe(false);
  });

  it("keeps concurrent waitUntil flush observations correlation-free", () =>
    Effect.runPromise(
      withTelemetryFlushBlockPair(async (activeBlock, trailingBlock) => {
        const conversationId = conversation("concurrent");
        const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));
        const observationStart = telemetryObservations.length;
        const submitSettlement = settle(stub.submitEncoded("concurrent-submit-marker"));

        await Effect.runPromise(awaitTelemetryFlushBlock(activeBlock));
        // Start B only after A's exporter is definitely active. B must settle immediately while its
        // flush request coalesces into exactly one trailing attempt behind A.
        const observeSettlement = settle(stub.observePage("concurrent-observe-marker"));
        const [submitExit, observeExit] = await Promise.all([
          awaitSettlement(submitSettlement),
          awaitSettlement(observeSettlement),
        ]);
        if (Exit.isFailure(submitExit) || Exit.isFailure(observeExit)) {
          throw new Error("Expected both concurrent RPC responses");
        }
        expect(submitExit.value).toMatchObject({
          _tag: "HostFailed",
          failure: { _tag: "HostProtocolError" },
        });
        expect(observeExit.value).toMatchObject({
          _tag: "HostFailed",
          failure: { _tag: "HostProtocolError" },
        });
        expect(trailingBlock.isActive()).toBe(false);

        activeBlock.release();
        await Effect.runPromise(awaitTelemetryFlushBlockCompleted(activeBlock));
        await Effect.runPromise(awaitTelemetryFlushBlock(trailingBlock));
        expect(activeBlock.isCompleted()).toBe(true);
        expect(trailingBlock.isActive()).toBe(true);

        trailingBlock.release();
        await Effect.runPromise(awaitTelemetryFlushBlockCompleted(trailingBlock));
        await Effect.runPromise(awaitTelemetryObservationCount(observationStart + 6));
        const observations = telemetryObservations.slice(observationStart);
        const spans = observations.filter(
          (observation): observation is TelemetrySpanObservation =>
            observation.phase === "span-ended",
        );
        const flushes = observations.filter(({ phase }) => phase !== "span-ended");
        expect(spans.map(({ entrypoint }) => entrypoint).toSorted()).toEqual(["observe", "submit"]);
        expect(flushes.map(({ phase }) => phase).toSorted()).toEqual([
          "flush-completed",
          "flush-completed",
          "flush-started",
          "flush-started",
        ]);
        expect(flushes.map((observation) => Object.keys(observation))).toEqual([
          ["phase"],
          ["phase"],
          ["phase"],
          ["phase"],
        ]);
        const exportedStrings = reachableObservationValues(observations)
          .filter((value) => typeof value === "string")
          .join("\n");
        expect(exportedStrings).not.toContain("concurrent-submit-marker");
        expect(exportedStrings).not.toContain("concurrent-observe-marker");
      }),
    ));
});

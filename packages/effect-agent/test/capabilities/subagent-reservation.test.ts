import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Schema } from "effect";
import { RunId, ToolCallId } from "effect-agent/identifiers";
import { SubagentDelegationCaps, SubagentReservationAmounts } from "effect-agent/subagent-contract";
import type { SubagentParentBudgetView } from "effect-agent/subagent-reservations";
import {
  SubagentReservationRequest,
  SubagentReservations,
  SubagentReservationsMemoryLive,
} from "effect-agent/subagent-reservations";

const decodeRunId = Schema.decodeSync(RunId);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const runId = decodeRunId("parent-run-1");
const toolCall = (index: number) => decodeToolCallId(`tool-call-${index}`);

const amounts = (
  partial: Partial<Record<keyof typeof SubagentReservationAmounts.fields, number>> = {},
): SubagentReservationAmounts =>
  SubagentReservationAmounts.make({
    turns: 0,
    toolCalls: 0,
    durationMillis: 0,
    inputTokens: 0,
    outputTokens: 0,
    costMicrousd: 0,
    resultBytes: 0,
    ...partial,
  });

const request = (index: number, allocation: SubagentReservationAmounts) =>
  SubagentReservationRequest.make({
    parentRunId: runId,
    parentToolCallId: toolCall(index),
    allocation,
  });

const dimensionKeys = [
  "turns",
  "toolCalls",
  "durationMillis",
  "inputTokens",
  "outputTokens",
  "costMicrousd",
  "resultBytes",
] as const;

type DimensionKey = (typeof dimensionKeys)[number];

const capOf = (caps: SubagentDelegationCaps, key: DimensionKey): number | undefined => {
  switch (key) {
    case "turns":
      return caps.maxTurns;
    case "toolCalls":
      return caps.maxToolCalls;
    case "durationMillis":
      return caps.maxDurationMillis;
    case "inputTokens":
      return caps.maxInputTokens;
    case "outputTokens":
      return caps.maxOutputTokens;
    case "costMicrousd":
      return caps.maxCostMicrousd;
    case "resultBytes":
      return caps.maxResultBytes;
  }
};

/** Spec §7 conservation: recomputed from reservation views, not trusted aggregates. */
const assertConservation = (view: SubagentParentBudgetView): void => {
  for (const key of dimensionKeys) {
    let open = 0;
    let observed = 0;
    let overrun = 0;

    for (const reservation of view.reservations) {
      const observedValue = reservation.observedConsumed[key] ?? 0;

      observed += observedValue;
      overrun += reservation.overrun[key];
      if (reservation.status !== "released") {
        open += reservation.allocated[key] - reservation.coveredConsumed[key];
      }
      // observedConsumed = coveredConsumed + overrun
      expect(observedValue).toBe(reservation.coveredConsumed[key] + reservation.overrun[key]);
      if (reservation.status === "released") {
        // allocated = coveredConsumed + released
        expect(reservation.allocated[key]).toBe(
          reservation.coveredConsumed[key] + reservation.released[key],
        );
      }
    }
    // overrun is charged exactly once to the parent aggregate
    expect(view.cumulativeOverrun[key]).toBe(overrun);
    const cap = capOf(view.caps, key);

    if (cap !== undefined) {
      // cap + cumulativeOverrun = available + open reservations + cumulativeObservedConsumed
      expect(cap + view.cumulativeOverrun[key]).toBe((view.available[key] ?? 0) + open + observed);
    }
  }
};

describe("subagent budget reservations", () => {
  it.effect("reserves descendant slots atomically and holds their concurrency permits", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;

      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({
          maxTotalChildInvocations: 2,
          maxConcurrentChildren: 2,
          maxTurns: 4,
        }),
      );

      const tree = SubagentReservationRequest.make({
        ...request(0, amounts({ turns: 4 })),
        descendantInvocations: 1,
      });

      yield* reservations.reserve(tree);
      expect((yield* reservations.parentSnapshot(runId)).totalChildInvocations).toBe(2);
      const rejected = yield* reservations.reserve(request(1, amounts())).pipe(Effect.flip);

      expect(rejected).toMatchObject({
        _tag: "SubagentBudgetExhausted",
        dimension: "total-child-invocations",
        observedValue: 3,
      });

      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const holder = yield* Effect.gen(function* () {
        yield* reservations.acquireChildSlot(runId, 2);
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
      }).pipe(Effect.scoped, Effect.forkChild);

      yield* Deferred.await(entered);
      const successor = yield* Deferred.make<void>();

      const waiter = yield* Effect.gen(function* () {
        yield* reservations.acquireChildSlot(runId);
        yield* Deferred.succeed(successor, undefined);
      }).pipe(Effect.scoped, Effect.forkChild);

      yield* Effect.yieldNow;
      expect(yield* Deferred.isDone(successor)).toBe(false);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.join(waiter);
      expect(yield* Deferred.isDone(successor)).toBe(true);
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("parallel reserve calls never oversubscribe the parent budget", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;

      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({ maxInputTokens: 10 }),
      );

      const outcomes = yield* Effect.all(
        Array.from({ length: 4 }, (_, index) =>
          reservations.reserve(request(index, amounts({ inputTokens: 3 }))).pipe(
            Effect.map(() => "reserved" as const),
            Effect.catchTag("SubagentBudgetExhausted", () => Effect.succeed("exhausted" as const)),
          ),
        ),
        { concurrency: "unbounded" },
      );

      expect(outcomes.filter((outcome) => outcome === "reserved")).toHaveLength(3);
      const snapshot = yield* reservations.parentSnapshot(runId);

      expect(snapshot.available.inputTokens).toBe(1);
      assertConservation(snapshot);
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("bounds concurrent children and frees a queued slot on interruption", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;

      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({ maxConcurrentChildren: 1 }),
      );
      const firstHolding = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();

      const first = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* reservations.acquireChildSlot(runId);
          yield* Deferred.succeed(firstHolding, undefined);
          yield* Deferred.await(releaseFirst);
        }),
      ).pipe(Effect.forkChild);

      yield* Deferred.await(firstHolding);

      const queuedAcquired = yield* Deferred.make<void>();

      const queued = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* reservations.acquireChildSlot(runId);
          yield* Deferred.succeed(queuedAcquired, undefined);

          return yield* Effect.never;
        }),
      ).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      expect(yield* Deferred.isDone(queuedAcquired)).toBe(false);
      yield* Fiber.interrupt(queued);
      expect(yield* Deferred.isDone(queuedAcquired)).toBe(false);

      const successorAcquired = yield* Deferred.make<void>();

      const successor = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* reservations.acquireChildSlot(runId);
          yield* Deferred.succeed(successorAcquired, undefined);
        }),
      ).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      expect(yield* Deferred.isDone(successorAcquired)).toBe(false);

      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(successor);
      expect(yield* Deferred.isDone(successorAcquired)).toBe(true);
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );
});

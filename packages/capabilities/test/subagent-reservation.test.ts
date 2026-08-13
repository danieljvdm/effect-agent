import { describe, expect, it } from "@effect/vitest";

import { Deferred, Effect, Fiber, Schema } from "effect";
import { FastCheck } from "effect/testing";
import { RunId, ToolCallId } from "@effect-agent/core";

import {
  makeBudgetReservationId,
  SubagentBudgetExhausted,
  SubagentDelegationCaps,
  SubagentObservedUsage,
  SubagentParentBudgetConflict,
  SubagentParentBudgetView,
  SubagentParentBudgetUnknown,
  SubagentReservationAmounts,
  SubagentReservationConflict,
  SubagentReservationRequest,
  SubagentReservations,
  SubagentReservationsMemoryLive,
  SubagentReservationUnknown,
  SubagentReservationView,
} from "../src/index.ts";

const decodeRunId = Schema.decodeSync(RunId);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const runId = decodeRunId("parent-run-1");
const toolCall = (index: number) => decodeToolCallId(`tool-call-${index}`);
const reservationId = (index: number) => makeBudgetReservationId(runId, toolCall(index));

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

const boundedAmount = FastCheck.integer({ min: 0, max: 6 });
const slotIndex = FastCheck.integer({ min: 0, max: 4 });
const operationArb = FastCheck.oneof(
  FastCheck.record({
    kind: FastCheck.constant("reserve" as const),
    slot: slotIndex,
    allocation: FastCheck.record({
      toolCalls: boundedAmount,
      inputTokens: boundedAmount,
      costMicrousd: boundedAmount,
    }),
  }),
  FastCheck.record({
    kind: FastCheck.constant("observe" as const),
    slot: slotIndex,
    usage: FastCheck.record(
      {
        toolCalls: boundedAmount,
        inputTokens: boundedAmount,
        costMicrousd: boundedAmount,
      },
      { requiredKeys: [] },
    ),
  }),
  FastCheck.record({ kind: FastCheck.constant("beginRelease" as const), slot: slotIndex }),
  FastCheck.record({ kind: FastCheck.constant("release" as const), slot: slotIndex }),
);
const capArb = FastCheck.option(FastCheck.integer({ min: 0, max: 12 }), { nil: undefined });

describe("subagent budget reservations", () => {
  it("derives the stable reservation identity from parent Run and Tool Call", () => {
    expect(makeBudgetReservationId(runId, toolCall(7))).toBe("parent-run-1:tool-call-7");
  });

  it("keeps delimiter-bearing identities injective in the reservation identity", () => {
    const left = makeBudgetReservationId(decodeRunId("run:x"), decodeToolCallId("call"));
    const right = makeBudgetReservationId(decodeRunId("run"), decodeToolCallId("x:call"));
    expect(left).not.toBe(right);
  });

  it.effect("reserves idempotently by stable identity and rejects a changed allocation", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({ maxToolCalls: 10, maxTotalChildInvocations: 8 }),
      );
      const first = yield* reservations.reserve(request(1, amounts({ toolCalls: 4 })));
      const second = yield* reservations.reserve(request(1, amounts({ toolCalls: 4 })));

      expect(first.reservationId).toBe(reservationId(1));
      expect(second).toEqual(first);
      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.totalChildInvocations).toBe(1);
      expect(snapshot.available.toolCalls).toBe(6);

      const conflict = yield* reservations
        .reserve(request(1, amounts({ toolCalls: 5 })))
        .pipe(Effect.flip);
      expect(conflict).toBeInstanceOf(SubagentReservationConflict);
      assertConservation(yield* reservations.parentSnapshot(runId));
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("checks every dimension atomically and commits nothing on rejection", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({
          maxTurns: 10,
          maxCostMicrousd: 5,
          maxTotalChildInvocations: 8,
        }),
      );
      const error = yield* reservations
        .reserve(request(1, amounts({ turns: 4, costMicrousd: 6 })))
        .pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentBudgetExhausted);
      if (error instanceof SubagentBudgetExhausted) {
        expect(error.dimension).toBe("cost");
        expect(error.limitValue).toBe(5);
        expect(error.observedValue).toBe(6);
      }
      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.totalChildInvocations).toBe(0);
      expect(snapshot.available.turns).toBe(10);
      expect(snapshot.available.costMicrousd).toBe(5);
      expect(snapshot.reservations).toEqual([]);
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("records covered consumption then overrun without clipping and blocks new work", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(runId, SubagentDelegationCaps.make({ maxToolCalls: 10 }));
      yield* reservations.reserve(request(1, amounts({ toolCalls: 5 })));
      const partial = yield* reservations.observe(
        reservationId(1),
        SubagentObservedUsage.make({ toolCalls: 3 }),
      );
      expect(partial.coveredConsumed.toolCalls).toBe(3);
      expect(partial.overrun.toolCalls).toBe(0);

      const over = yield* reservations.observe(
        reservationId(1),
        SubagentObservedUsage.make({ toolCalls: 4 }),
      );
      expect(over.observedConsumed.toolCalls).toBe(7);
      expect(over.coveredConsumed.toolCalls).toBe(5);
      expect(over.overrun.toolCalls).toBe(2);

      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.cumulativeOverrun.toolCalls).toBe(2);
      // available is not mutated by overrun (§7 equation), but overrun reduces headroom
      expect(snapshot.available.toolCalls).toBe(5);
      const blocked = yield* reservations
        .reserve(request(2, amounts({ toolCalls: 4 })))
        .pipe(Effect.flip);
      expect(blocked).toBeInstanceOf(SubagentBudgetExhausted);
      const admitted = yield* reservations.reserve(request(3, amounts({ toolCalls: 3 })));
      expect(admitted.status).toBe("reserved");
      assertConservation(yield* reservations.parentSnapshot(runId));
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("settles through releasePending and returns unused allocation exactly once", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({ maxInputTokens: 10 }),
      );
      yield* reservations.reserve(request(1, amounts({ inputTokens: 10 })));
      yield* reservations.observe(reservationId(1), SubagentObservedUsage.make({ inputTokens: 4 }));

      const pending = yield* reservations.beginRelease(reservationId(1));
      expect(pending.status).toBe("releasePending");
      expect((yield* reservations.parentSnapshot(runId)).available.inputTokens).toBe(0);
      const pendingAgain = yield* reservations.beginRelease(reservationId(1));
      expect(pendingAgain).toEqual(pending);

      // late usage after the settlement decision is pure overrun and creates no budget
      const late = yield* reservations.observe(
        reservationId(1),
        SubagentObservedUsage.make({ inputTokens: 2 }),
      );
      expect(late.coveredConsumed.inputTokens).toBe(4);
      expect(late.overrun.inputTokens).toBe(2);

      const settled = yield* reservations.release(reservationId(1));
      expect(settled.status).toBe("released");
      expect(settled.released.inputTokens).toBe(6);
      expect((yield* reservations.parentSnapshot(runId)).available.inputTokens).toBe(6);

      const again = yield* reservations.release(reservationId(1));
      expect(again).toEqual(settled);
      expect((yield* reservations.parentSnapshot(runId)).available.inputTokens).toBe(6);

      const lateAfterRelease = yield* reservations.observe(
        reservationId(1),
        SubagentObservedUsage.make({ inputTokens: 1 }),
      );
      expect(lateAfterRelease.overrun.inputTokens).toBe(3);
      const final = yield* reservations.parentSnapshot(runId);
      expect(final.available.inputTokens).toBe(6);
      assertConservation(final);
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("conservatively consumes dimensions with no observed usage at settlement", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({ maxTurns: 8, maxToolCalls: 6 }),
      );
      yield* reservations.reserve(request(1, amounts({ turns: 2, toolCalls: 3 })));
      yield* reservations.observe(reservationId(1), SubagentObservedUsage.make({ toolCalls: 1 }));
      const settled = yield* reservations.release(reservationId(1));

      expect(settled.observedConsumed.turns).toBe(2);
      expect(settled.coveredConsumed.turns).toBe(2);
      expect(settled.released.turns).toBe(0);
      expect(settled.coveredConsumed.toolCalls).toBe(1);
      expect(settled.released.toolCalls).toBe(2);
      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.available.turns).toBe(6);
      expect(snapshot.available.toolCalls).toBe(5);
      assertConservation(snapshot);
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("bounds total invocations monotonically apart from the concurrency gate", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({ maxTotalChildInvocations: 2, maxConcurrentChildren: 1 }),
      );
      // Sequential slots recycle through the semaphore; the counter must not.
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* reservations.acquireChildSlot(runId);
          yield* reservations.reserve(request(0, amounts()));
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* reservations.acquireChildSlot(runId);
          yield* reservations.reserve(request(1, amounts()));
        }),
      );
      yield* reservations.release(reservationId(0));
      yield* reservations.release(reservationId(1));

      const error = yield* reservations.reserve(request(2, amounts())).pipe(Effect.flip);
      expect(error).toBeInstanceOf(SubagentBudgetExhausted);
      if (error instanceof SubagentBudgetExhausted) {
        expect(error.dimension).toBe("total-child-invocations");
        expect(error.limitValue).toBe(2);
        expect(error.observedValue).toBe(3);
      }
      // idempotent re-reserve of an existing key still resolves after exhaustion
      const again = yield* reservations.reserve(request(1, amounts()));
      expect(again.reservationId).toBe(reservationId(1));
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
        Array.from({ length: 8 }, (_, index) =>
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

  it.effect("fails closed when the configured concurrent-children cap is zero", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(
        runId,
        SubagentDelegationCaps.make({ maxConcurrentChildren: 0 }),
      );
      const error = yield* Effect.scoped(reservations.acquireChildSlot(runId)).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentBudgetExhausted);
      if (error instanceof SubagentBudgetExhausted) {
        expect(error.dimension).toBe("concurrent-children");
        expect(error.limitValue).toBe(0);
      }
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("leaves child execution ungated when no concurrency cap is configured", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(runId, SubagentDelegationCaps.make({}));
      const acquired: void = yield* Effect.scoped(reservations.acquireChildSlot(runId));
      expect(acquired).toBeUndefined();
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect(
    "fails typed for unknown parents, unknown reservations, and conflicting registrations",
    () =>
      Effect.gen(function* () {
        const reservations = yield* SubagentReservations;
        const unknownParent = yield* reservations.reserve(request(1, amounts())).pipe(Effect.flip);
        expect(unknownParent).toBeInstanceOf(SubagentParentBudgetUnknown);
        const slotUnknown = yield* Effect.scoped(reservations.acquireChildSlot(runId)).pipe(
          Effect.flip,
        );
        expect(slotUnknown).toBeInstanceOf(SubagentParentBudgetUnknown);
        const snapshotUnknown = yield* reservations.parentSnapshot(runId).pipe(Effect.flip);
        expect(snapshotUnknown).toBeInstanceOf(SubagentParentBudgetUnknown);

        yield* reservations.registerParent(runId, SubagentDelegationCaps.make({ maxToolCalls: 2 }));
        const reRegistered = yield* reservations.registerParent(
          runId,
          SubagentDelegationCaps.make({ maxToolCalls: 2 }),
        );
        expect(reRegistered.parentRunId).toBe(runId);
        const conflict = yield* reservations
          .registerParent(runId, SubagentDelegationCaps.make({ maxToolCalls: 3 }))
          .pipe(Effect.flip);
        expect(conflict).toBeInstanceOf(SubagentParentBudgetConflict);

        const unknownObserve = yield* reservations
          .observe(reservationId(9), SubagentObservedUsage.make({}))
          .pipe(Effect.flip);
        expect(unknownObserve).toBeInstanceOf(SubagentReservationUnknown);
        const unknownBegin = yield* reservations.beginRelease(reservationId(9)).pipe(Effect.flip);
        expect(unknownBegin).toBeInstanceOf(SubagentReservationUnknown);
        const unknownRelease = yield* reservations.release(reservationId(9)).pipe(Effect.flip);
        expect(unknownRelease).toBeInstanceOf(SubagentReservationUnknown);
      }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect("round-trips reservation accounting and typed errors through their Schemas", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      yield* reservations.registerParent(runId, SubagentDelegationCaps.make({ maxToolCalls: 4 }));
      yield* reservations.reserve(request(1, amounts({ toolCalls: 3 })));
      yield* reservations.observe(reservationId(1), SubagentObservedUsage.make({ toolCalls: 4 }));
      const settled = yield* reservations.release(reservationId(1));
      const decodedView = yield* Schema.decodeEffect(SubagentReservationView)(
        yield* Schema.encodeEffect(SubagentReservationView)(settled),
      );
      expect(decodedView).toEqual(settled);

      const error = yield* reservations
        .reserve(request(2, amounts({ toolCalls: 3 })))
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(SubagentBudgetExhausted);
      if (error instanceof SubagentBudgetExhausted) {
        const decoded = yield* Schema.decodeEffect(SubagentBudgetExhausted)(
          yield* Schema.encodeEffect(SubagentBudgetExhausted)(error),
        );
        expect(decoded).toBeInstanceOf(SubagentBudgetExhausted);
        expect(decoded.dimension).toBe("tool-calls");
      }
      const snapshot = yield* reservations.parentSnapshot(runId);
      const decodedSnapshot = yield* Schema.decodeEffect(SubagentParentBudgetView)(
        yield* Schema.encodeEffect(SubagentParentBudgetView)(snapshot),
      );
      expect(decodedSnapshot).toEqual(snapshot);
    }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );

  it.effect.prop(
    "generated parallel reserve/observe/release sequences preserve conservation",
    {
      toolCallsCap: capArb,
      inputTokensCap: capArb,
      costCap: capArb,
      operations: FastCheck.array(operationArb, { maxLength: 32 }),
    },
    ({ costCap, inputTokensCap, operations, toolCallsCap }) =>
      Effect.gen(function* () {
        const reservations = yield* SubagentReservations;
        yield* reservations.registerParent(
          runId,
          SubagentDelegationCaps.make({
            ...(toolCallsCap !== undefined ? { maxToolCalls: toolCallsCap } : {}),
            ...(inputTokensCap !== undefined ? { maxInputTokens: inputTokensCap } : {}),
            ...(costCap !== undefined ? { maxCostMicrousd: costCap } : {}),
          }),
        );
        const execute = (operation: (typeof operations)[number]): Effect.Effect<void> => {
          switch (operation.kind) {
            case "reserve":
              return reservations
                .reserve(request(operation.slot, amounts(operation.allocation)))
                .pipe(Effect.ignore);
            case "observe":
              return reservations
                .observe(reservationId(operation.slot), SubagentObservedUsage.make(operation.usage))
                .pipe(Effect.ignore);
            case "beginRelease":
              return reservations.beginRelease(reservationId(operation.slot)).pipe(Effect.ignore);
            case "release":
              return reservations.release(reservationId(operation.slot)).pipe(Effect.ignore);
          }
        };
        yield* Effect.all(operations.map(execute), { concurrency: "unbounded" });

        const openSnapshot = yield* reservations.parentSnapshot(runId);
        assertConservation(openSnapshot);

        // settle everything; double release must return unused budget exactly once
        for (const reservation of openSnapshot.reservations) {
          yield* reservations.release(reservation.reservationId);
          yield* reservations.release(reservation.reservationId);
        }
        const settled = yield* reservations.parentSnapshot(runId);
        assertConservation(settled);
        for (const reservation of settled.reservations) {
          expect(reservation.status).toBe("released");
        }
      }).pipe(Effect.provide(SubagentReservationsMemoryLive)),
  );
});

import { describe, expect, it } from "@effect/vitest";

import { Cause, Deferred, Effect, Exit, Fiber, Ref, Schema, Stream } from "effect";

import {
  DemoApprovalPending,
  type DemoOperationalEvent,
  DemoRunFailure,
  ResolveRunApprovalRequest,
  StartOperationalRunRequest,
} from "./operational-contracts";
import { DemoInteractiveRuntime, DemoInteractiveRuntimeLive } from "./operational-runtime.server";

const statusesFor = (
  events: ReadonlyArray<DemoOperationalEvent>,
  content: string,
): ReadonlyArray<string> =>
  events.flatMap((event) =>
    event._tag === "DemoCommandStateChanged" && event.content === content ? [event.status] : [],
  );

describe("Phase 2 operational runtime", () => {
  it.effect("extends one official conversation across consecutive Runs", () =>
    Effect.gen(function* () {
      const runtime = yield* DemoInteractiveRuntime;
      const first = yield* runtime
        .start(StartOperationalRunRequest.make({ scenario: "guided" }))
        .pipe(Stream.runCollect);
      const second = yield* runtime
        .start(StartOperationalRunRequest.make({ scenario: "guided" }))
        .pipe(Stream.runCollect);

      expect(first.at(-1)?._tag).toBe("RunCompleted");
      expect(second.at(-1)?._tag).toBe("RunCompleted");
      expect(
        Array.from(second).some(
          (event) => event._tag === "DemoContextPrepared" && event.officialMessageCount > 2,
        ),
      ).toBe(true);
    }).pipe(Effect.provide(DemoInteractiveRuntimeLive), Effect.scoped),
  );

  it.effect("shows deterministic parallel work, safe input seams, and bounded adapters", () =>
    Effect.gen(function* () {
      const runtime = yield* DemoInteractiveRuntime;
      const collected = yield* runtime
        .start(StartOperationalRunRequest.make({ scenario: "guided" }))
        .pipe(Stream.runCollect);
      const events = Array.from(collected);
      const batch = events.find((event) => event._tag === "DemoToolBatchCommitted");

      expect(batch?._tag).toBe("DemoToolBatchCommitted");
      if (batch?._tag === "DemoToolBatchCommitted") {
        expect(batch.declaredOrder).toEqual(["flight-call-1", "lodging-call-1", "activity-call-1"]);
        expect(batch.completionOrder).toEqual([
          "activity-call-1",
          "lodging-call-1",
          "flight-call-1",
        ]);
      }
      expect(statusesFor(events, "Change the departure date to 2026-09-21.")).toEqual([
        "queued",
        "claimed",
        "delivered",
      ]);
      expect(statusesFor(events, "Prefer a quiet room away from the lift.")).toEqual([
        "queued",
        "claimed",
        "delivered",
      ]);
      expect(events.some((event) => event._tag === "DemoContextPrepared" && event.compacted)).toBe(
        true,
      );
      expect(events.some((event) => event._tag === "DemoMcpConnected")).toBe(true);
      expect(
        events.some(
          (event) =>
            event._tag === "DemoSandboxObserved" &&
            event.event._tag === "SandboxStarted" &&
            event.event.implementation.isolation === "unisolated",
        ),
      ).toBe(true);
      expect(events.at(-1)?._tag).toBe("RunCompleted");
    }).pipe(Effect.provide(DemoInteractiveRuntimeLive), Effect.scoped),
  );

  it.effect("does not start a denied hold handler", () =>
    Effect.gen(function* () {
      const runtime = yield* DemoInteractiveRuntime;
      const pending = yield* Deferred.make<DemoApprovalPending>();
      const observed = yield* Ref.make<ReadonlyArray<DemoOperationalEvent>>([]);
      const fiber = yield* runtime
        .start(StartOperationalRunRequest.make({ scenario: "hold" }))
        .pipe(
          Stream.tap((event) =>
            Ref.update(observed, (events) => [...events, event]).pipe(
              Effect.andThen(
                event._tag === "DemoApprovalPending"
                  ? Deferred.succeed(pending, event)
                  : Effect.void,
              ),
            ),
          ),
          Stream.runDrain,
          Effect.forkChild,
        );
      const request = yield* Deferred.await(pending);
      yield* runtime.resolveApproval(
        ResolveRunApprovalRequest.make({
          handle: request.handle,
          requestId: request.request.requestId,
          choice: "deny",
        }),
      );
      const exit = yield* Fiber.await(fiber);
      const events = yield* Ref.get(observed);
      const starts = events
        .filter((event) => event._tag === "DemoHoldHandlerState")
        .map((event) => event.starts);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(starts).toEqual([0]);
      expect(events.some((event) => event._tag === "RunFailed")).toBe(true);
    }).pipe(Effect.provide(DemoInteractiveRuntimeLive), Effect.scoped),
  );

  it.effect("terminalizes the client stream and frees the registry when a Tool handler dies", () =>
    Effect.gen(function* () {
      const runtime = yield* DemoInteractiveRuntime;
      const observed = yield* Ref.make<ReadonlyArray<DemoOperationalEvent>>([]);
      const exit = yield* runtime
        .start(StartOperationalRunRequest.make({ scenario: "tool-defect" }))
        .pipe(
          Stream.tap((event) => Ref.update(observed, (events) => [...events, event])),
          Stream.runDrain,
          Effect.exit,
        );
      const events = yield* Ref.get(observed);

      expect(Exit.isFailure(exit)).toBe(true);
      const failure = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
      expect(Schema.is(DemoRunFailure)(failure)).toBe(true);
      if (Schema.is(DemoRunFailure)(failure)) {
        expect(failure.errorTag).toBe("DemoRunError");
        expect(failure.message).toContain("crashed");
      }
      expect(events.some((event) => event._tag === "DemoRunOpened")).toBe(true);

      const reopened = yield* Ref.make<ReadonlyArray<DemoOperationalEvent>>([]);
      const secondExit = yield* runtime
        .start(StartOperationalRunRequest.make({ scenario: "tool-defect" }))
        .pipe(
          Stream.tap((event) => Ref.update(reopened, (events) => [...events, event])),
          Stream.runDrain,
          Effect.exit,
        );
      const reopenedEvents = yield* Ref.get(reopened);

      expect(Exit.isFailure(secondExit)).toBe(true);
      const secondFailure = Exit.isFailure(secondExit) ? Cause.squash(secondExit.cause) : undefined;
      if (Schema.is(DemoRunFailure)(secondFailure)) {
        expect(secondFailure.errorTag).not.toBe("DemoRunAlreadyActive");
      }
      expect(reopenedEvents.some((event) => event._tag === "DemoRunOpened")).toBe(true);
    }).pipe(Effect.provide(DemoInteractiveRuntimeLive), Effect.scoped),
  );

  it.effect("reports the exact budget rejection before any Tool handler starts", () =>
    Effect.gen(function* () {
      const runtime = yield* DemoInteractiveRuntime;
      const observed = yield* Ref.make<ReadonlyArray<DemoOperationalEvent>>([]);
      const exit = yield* runtime
        .start(StartOperationalRunRequest.make({ scenario: "budget-cost" }))
        .pipe(
          Stream.tap((event) => Ref.update(observed, (events) => [...events, event])),
          Stream.runDrain,
          Effect.exit,
        );
      const events = yield* Ref.get(observed);
      const rejected = events.find((event) => event._tag === "DemoBudgetRejected");

      expect(Exit.isFailure(exit)).toBe(true);
      expect(rejected?._tag).toBe("DemoBudgetRejected");
      if (rejected?._tag === "DemoBudgetRejected") {
        expect(rejected.limit).toBe("cost");
        expect(rejected.limitValue).toBe(100);
        expect(rejected.observedValue).toBe(500);
      }
      expect(events.some((event) => event._tag === "ToolCallStarted")).toBe(false);
    }).pipe(Effect.provide(DemoInteractiveRuntimeLive), Effect.scoped),
  );
});

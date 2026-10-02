import type { DurableBindingFailure } from "@yielded/agent/agent-registration";
import {
  DurableAgentRuntime,
  type DurableWorkerFailure,
} from "@yielded/agent/durable-agent-runtime";
import type { ThreadId } from "@yielded/agent/identifiers";
import { SubmissionLedger } from "@yielded/agent/submission-ledger";
import { WakeScheduler } from "@yielded/agent/wake-scheduler";
import { Effect, Queue, Stream } from "effect";

const PENDING_CAPACITY = 1_024;

type DispatchState = "pending" | "active" | "dirty";

/**
 * One discovery path feeds bounded concurrent Thread drains. Hints carry no authority:
 * the runtime still claims, fences, parks and releases every Attempt. Concurrency has
 * already been validated by the host configuration.
 */
export const runNodeWorkerDispatch = (
  concurrency: number,
): Effect.Effect<
  void,
  DurableWorkerFailure | DurableBindingFailure,
  DurableAgentRuntime | SubmissionLedger | WakeScheduler
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const wake = yield* WakeScheduler;
      const pending = yield* Queue.bounded<ThreadId>(PENDING_CAPACITY);
      const states = new Map<ThreadId, DispatchState>();

      yield* Effect.addFinalizer(() => Queue.shutdown(pending));

      const enqueue = (threadId: ThreadId) =>
        Effect.gen(function* () {
          const offer = yield* Effect.sync(() => {
            const state = states.get(threadId);

            if (state === "pending" || state === "dirty") return false;
            if (state === "active") {
              states.set(threadId, "dirty");

              return false;
            }

            states.set(threadId, "pending");

            return true;
          });

          // The single ingress can reserve at most one entry beyond queue capacity.
          // Backpressure retains scan tails instead of repeatedly dropping later lanes.
          if (offer) yield* Queue.offer(pending, threadId);
        });

      const initial = ledger.scanNonterminal.pipe(Stream.map((item) => item.threadId));

      // Merge acquires both streams concurrently; it does not guarantee that the wake
      // subscription precedes the initial read. The scheduler's fallback repairs lost hints.
      // Initial completion is normal; wake completion must stop the entire pool.
      const ingress = Stream.merge(initial, wake.wakes, { haltStrategy: "right" }).pipe(
        Stream.runForEach(enqueue),
      );

      const consume = Effect.gen(function* () {
        const threadId = yield* Queue.take(pending);

        yield* Effect.sync(() => states.set(threadId, "active"));

        let again = true;

        while (again) {
          yield* runtime.processThreadResolved(threadId);
          again = yield* Effect.sync(() => {
            if (states.get(threadId) === "dirty") {
              states.set(threadId, "active");

              return true;
            }

            states.delete(threadId);

            return false;
          });
          // Re-drain inline: workers re-enqueuing into a full queue can all deadlock.
          // This retains the runtime's drain-to-empty fairness, including parked returns.
        }
      }).pipe(Effect.forever);

      // Any exit stops and joins every other participant; no detached producer or worker.
      yield* Effect.raceAllFirst([ingress, ...Array.from({ length: concurrency }, () => consume)]);
    }),
  );

import { Cause, Effect, ErrorReporter, Exit, Stream } from "effect";

import type { RunToolFailureObserver, ToolFailureObservation } from "./run-options.ts";

/** @internal Only the explicitly installed trusted observer sees this live value. */
export const deliverToolFailure = (
  observer: RunToolFailureObserver,
  observation: ToolFailureObservation,
): Effect.Effect<void> =>
  isolateToolDerivative(Effect.suspend(() => observer.observe(observation)));

/**
 * Report failures of derivative work without consuming external interruption. Reporter defects
 * are isolated too. Neither observer delivery nor telemetry owns the authoritative Tool outcome.
 */
const reportDerivativeCause = <E>(cause: Cause.Cause<E>): Effect.Effect<void> => {
  const reportableReasons: Array<Cause.Reason<E>> = [];
  const interruptionReasons: Array<Cause.Interrupt> = [];
  for (const reason of cause.reasons) {
    if (Cause.isInterruptReason(reason)) interruptionReasons.push(reason);
    else reportableReasons.push(reason);
  }

  const reportExit =
    reportableReasons.length === 0
      ? Effect.succeed(Exit.succeed(undefined))
      : Effect.exit(ErrorReporter.report(Cause.fromReasons(reportableReasons)));
  return Effect.flatMap(reportExit, (exit) => {
    if (Exit.isFailure(exit)) {
      for (const reason of exit.cause.reasons) {
        if (Cause.isInterruptReason(reason)) interruptionReasons.push(reason);
      }
    }
    return interruptionReasons.length === 0
      ? Effect.void
      : Effect.failCause(Cause.fromReasons<never>(interruptionReasons));
  });
};

/** @internal Isolate reporting defects independently of Logger/Tracer configuration. */
export const isolateToolDerivative = <R>(
  effect: Effect.Effect<void, never, R>,
): Effect.Effect<void, never, R> => effect.pipe(Effect.catchCause(reportDerivativeCause));

/**
 * @internal Emit one authoritative value, then run derivative work from either the next pull or
 * structured finalization on early close. Telemetry and application observation share this one
 * owner. A started action is never retried after interruption.
 */
export const emitThenAfter = <A, E, R, E2, R2>(
  event: Effect.Effect<A, E, R>,
  after: Effect.Effect<void, E2, R2>,
): Stream.Stream<A, E | E2, R | R2> =>
  Stream.unwrap(
    Effect.sync(() => {
      let firstPull = true;
      let afterPhase: "unarmed" | "pending" | "running" | "completed" = "unarmed";
      const runAfter = Effect.suspend((): Effect.Effect<void, E2, R2> => {
        if (afterPhase !== "pending") return Effect.void;
        afterPhase = "running";
        return after.pipe(
          Effect.onExit(() =>
            Effect.sync(() => {
              afterPhase = "completed";
            }),
          ),
        );
      });
      const pull = Effect.suspend(
        (): Effect.Effect<readonly [A], E | E2 | Cause.Done<void>, R | R2> => {
          if (firstPull) {
            return Effect.map(event, (value) => {
              firstPull = false;
              afterPhase = "pending";
              return [value] as const;
            });
          }
          return Effect.andThen(runAfter, Cause.done());
        },
      );
      return Stream.fromPull(Effect.succeed(pull)).pipe(
        Stream.ensuring(
          // A typed `after` failure remains visible to an ordinary second pull. Cleanup owns
          // only early close; capture its Exit after terminal annotation. The isolated Tool span
          // derives its export status from that annotation even if cleanup consumes the private
          // span marker. External interruption still escapes.
          Effect.exit(Effect.interruptible(runAfter)).pipe(
            Effect.flatMap((exit) => {
              if (Exit.isSuccess(exit)) return Effect.void;
              return reportDerivativeCause(exit.cause);
            }),
          ),
        ),
      );
    }),
  );

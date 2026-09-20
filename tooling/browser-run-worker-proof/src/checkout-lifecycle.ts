import {
  Clock,
  Context,
  Effect,
  FileSystem,
  Layer,
  Schema,
  Semaphore,
  SynchronizedRef,
} from "effect";

import { Report } from "./checkout-contract.ts";

/** An absent owner proves nothing about its browsers without retained closure acknowledgements. */
export const retirementPlan = (
  ownerStatus: number,
  cleanup: typeof Report.Type.cleanup,
  recordedClosures: ReadonlyArray<boolean>,
) =>
  ownerStatus === 200
    ? "close"
    : ownerStatus === 404 &&
        (cleanup === "browsers-closed" ||
          cleanup === "confirmed" ||
          recordedClosures.every((closed) => closed))
      ? "destroy"
      : "blocked";

/** Publish a whole recovery snapshot; interruption keeps either the preceding or next report. */
export const writeReportSnapshot = Effect.fnUntraced(function* (
  path: string,
  report: typeof Report.Type,
) {
  const fs = yield* FileSystem.FileSystem;
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Report))(report);

  yield* fs.writeFileString(`${path}.tmp`, encoded);
  yield* fs.rename(`${path}.tmp`, path);
});

const makeReportStore = Effect.fnUntraced(function* (path: string) {
  const state = yield* SynchronizedRef.make<typeof Report.Type | undefined>(undefined);

  return {
    get: SynchronizedRef.get(state),
    update: (f: (report: typeof Report.Type | undefined) => typeof Report.Type | undefined) =>
      SynchronizedRef.updateEffect(state, (previous) =>
        Effect.gen(function* () {
          const next = f(previous);

          if (next !== undefined) yield* writeReportSnapshot(path, next);

          return next;
        }),
      ).pipe(Effect.uninterruptible),
  };
});

/** One owner serializes read/modify/publish, including the shared temporary filename. */
export class CheckoutReport extends Context.Service<
  CheckoutReport,
  Effect.Success<ReturnType<typeof makeReportStore>>
>()("checkout/Report") {
  static layer(path: string) {
    return Layer.effect(CheckoutReport, makeReportStore(path));
  }
}

/** Space admissions without holding the permit for an entire checkout. */
export const makeCaseAdmission = Effect.fnUntraced(function* (startIntervalMillis: number) {
  const starts = yield* Semaphore.make(1);
  let nextStart = 0;

  return starts.withPermit(
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;

      yield* Effect.sleep(Math.max(0, nextStart - now));
      nextStart = (yield* Clock.currentTimeMillis) + startIntervalMillis;
    }),
  );
});

/** Register retirement before deployment, including partially acknowledged provisioning. */
export const withRetirement = <A, E, R, E2, R2>(
  run: Effect.Effect<A, E, R>,
  retire: Effect.Effect<void, E2, R2>,
) => run.pipe(Effect.ensuring(retire.pipe(Effect.orDie)));

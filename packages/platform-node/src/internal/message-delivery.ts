import {
  MessageDeliveryDriver,
  type MessageDeliveryFailure,
  MessageDeliveryStore,
} from "@effect-agent/thread/MessageDelivery";
import { Cause, Clock, Effect, Exit, Option } from "effect";

const reportFailure = (cause: Cause.Cause<MessageDeliveryFailure>): Effect.Effect<void> =>
  Cause.hasInterruptsOnly(cause)
    ? Effect.interrupt
    : Effect.logWarning("Node message delivery pass failed").pipe(
        Effect.annotateLogs({
          failureTag: Option.match(Cause.findErrorOption(cause), {
            onNone: () => "Defect",
            onSome: (failure) => failure._tag,
          }),
        }),
      );

/** Caller-owned loop. Indexed scans repair absent hints even when both Threads have settled. */
export const runNodeMessageDeliveries = Effect.fn("NodeMessageDelivery.run")(function* (
  scanInterval: number,
) {
  const driver = yield* MessageDeliveryDriver;
  const store = yield* MessageDeliveryStore;

  while (true) {
    const pass = yield* driver.runDue().pipe(Effect.exit);

    if (Exit.isFailure(pass)) {
      yield* reportFailure(pass.cause);
      yield* Effect.sleep(scanInterval);
      continue;
    }

    const deadline = yield* store.nextDeadline().pipe(Effect.exit);

    if (Exit.isFailure(deadline)) {
      yield* reportFailure(deadline.cause);
      yield* Effect.sleep(scanInterval);
      continue;
    }

    const nowMillis = yield* Clock.currentTimeMillis;

    const delay =
      deadline.value === null
        ? scanInterval
        : Math.max(1, Math.min(deadline.value - nowMillis, scanInterval));

    yield* Effect.sleep(delay);
  }
});

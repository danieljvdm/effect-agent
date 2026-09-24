import { makeSubscriptionPartitionAlarmHandler } from "@effect-agent/platform-cloudflare/cloudflare-subscriptions";
import { Context, DateTime, Deferred, Effect, Exit, Fiber, Schema, SchemaGetter } from "effect";
import { SubscriptionDriver } from "effect-agent/subscriptions";
import { DurableObjectAlarm } from "effect-cf";
import { TestClock } from "effect/testing";
import { expect, it } from "vite-plus/test";

class Host extends Context.Service<Host, string>()("test/AlarmHost") {}
class Decoder extends Context.Service<Decoder, string>()("test/AlarmDecoder") {}

const nativeDriver = SubscriptionDriver.of({
  runDue: Effect.succeed({ processed: 1, failed: 0 }),
  processDelivery: () => Effect.void,
});

const hostDriver = SubscriptionDriver.of({
  runDue: Effect.die("The host driver must not replace the invocation's native driver"),
  processDelivery: () => Effect.void,
});

const event = DurableObjectAlarm.DurableObjectAlarmEvent.make({
  _tag: "AlarmDue",
  tag: "host/task",
  id: "one",
  payload: "work",
  scheduledAt: DateTime.makeUnsafe(0),
});

for (const outcome of ["timeout"] as const) {
  it(`closes ancillary alarm codec and callback scopes on ${outcome}`, () =>
    Effect.runPromise(
      Effect.gen(function* () {
        let hostClosed = false;
        let finalized = 0;
        const started = yield* Deferred.make<void>();

        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                hostClosed = true;
              }),
            );

            const payload = Schema.String.pipe(
              Schema.decodeTo(Schema.String, {
                decode: SchemaGetter.transformEffect((value) =>
                  Effect.gen(function* () {
                    expect(yield* Decoder).toBe("decoder");
                    expect(yield* SubscriptionDriver).toBe(nativeDriver);
                    yield* Effect.addFinalizer(() =>
                      Effect.sync(() => {
                        finalized++;
                      }),
                    );

                    return value;
                  }),
                ),
                encode: SchemaGetter.transform((value) => value),
              }),
            );

            const made = makeSubscriptionPartitionAlarmHandler({
              tag: event.tag,
              payload,
              timeoutMillis: 100,
              handle: () =>
                Effect.gen(function* () {
                  expect(yield* Host).toBe("host");
                  expect(yield* SubscriptionDriver).toBe(nativeDriver);
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      finalized++;
                    }),
                  );
                  yield* Deferred.succeed(started, undefined);

                  return yield* Effect.never;
                }),
            });

            const handler = yield* made.pipe(Effect.provideService(SubscriptionDriver, hostDriver));

            const fiber = yield* Effect.forkChild(
              handler
                .handle(event)
                .pipe(
                  Effect.provideService(SubscriptionDriver, nativeDriver),
                  Effect.provideService(Host, "invocation host must not replace captured host"),
                  Effect.provideService(
                    Decoder,
                    "invocation decoder must not replace captured decoder",
                  ),
                ),
            );

            yield* Deferred.await(started);
            yield* TestClock.adjust(100);

            const exit = yield* Fiber.await(fiber);

            expect(Exit.isSuccess(exit)).toBe(false);
            expect(finalized).toBe(2);
            expect(hostClosed).toBe(false);
          }),
        );
        expect(hostClosed).toBe(true);
      }).pipe(
        Effect.provideService(Host, "host"),
        Effect.provideService(Decoder, "decoder"),
        Effect.provide(TestClock.layer()),
      ),
    ));
}

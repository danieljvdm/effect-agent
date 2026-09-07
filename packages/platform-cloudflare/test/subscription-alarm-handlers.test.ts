import type { SubscriptionAlarmProtocolError } from "@effect-agent/platform-cloudflare/CloudflareSubscriptions";
import {
  makeSubscriptionPartitionAlarmHandler,
  SubscriptionAlarmExtensionError,
} from "@effect-agent/platform-cloudflare/CloudflareSubscriptions";
import { Context, DateTime, Deferred, Effect, Exit, Fiber, Schema, SchemaGetter } from "effect";
import { DurableObjectAlarm } from "effect-cf";
import { TestClock } from "effect/testing";
import { expect, expectTypeOf, it } from "vite-plus/test";

class Host extends Context.Service<Host, string>()("test/AlarmHost") {}
class Decoder extends Context.Service<Decoder, string>()("test/AlarmDecoder") {}

const event = DurableObjectAlarm.DurableObjectAlarmEvent.make({
  _tag: "AlarmDue",
  tag: "host/task",
  id: "one",
  payload: "work",
  scheduledAt: DateTime.makeUnsafe(0),
});

for (const outcome of ["success", "failure", "defect", "timeout", "interruption"] as const) {
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
                decode: SchemaGetter.transformOrFail((value) =>
                  Effect.gen(function* () {
                    expect(yield* Decoder).toBe("decoder");
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
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      finalized++;
                    }),
                  );
                  yield* Deferred.succeed(started, undefined);
                  switch (outcome) {
                    case "success":
                      return;
                    case "failure":
                      return yield* SubscriptionAlarmExtensionError.make({ code: "unavailable" });
                    case "defect":
                      return yield* Effect.die("test defect");
                    case "timeout":
                    case "interruption":
                      return yield* Effect.never;
                  }
                }),
            });

            expectTypeOf<Effect.Services<typeof made>>().toEqualTypeOf<Host | Decoder>();
            expectTypeOf<
              Effect.Error<typeof made>
            >().toEqualTypeOf<SubscriptionAlarmProtocolError>();
            const handler = yield* made;

            expectTypeOf<
              Effect.Services<ReturnType<typeof handler.handle>>
            >().toEqualTypeOf<never>();
            expectTypeOf<Effect.Error<ReturnType<typeof handler.handle>>>().toEqualTypeOf<
              SubscriptionAlarmExtensionError | SubscriptionAlarmProtocolError
            >();
            const fiber = yield* Effect.forkChild(handler.handle(event));

            yield* Deferred.await(started);
            if (outcome === "timeout") yield* TestClock.adjust(100);
            if (outcome === "interruption") yield* Fiber.interrupt(fiber);
            const exit = yield* Fiber.await(fiber);

            expect(Exit.isSuccess(exit)).toBe(outcome === "success");
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

it("rejects reserved ownership, wrong tags and malformed payloads", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const options = {
        tag: "effect-agent/foreign",
        payload: Schema.String,
        timeoutMillis: 100,
        handle: () => Effect.void,
      };

      expect((yield* makeSubscriptionPartitionAlarmHandler(options).pipe(Effect.flip))._tag).toBe(
        "SubscriptionAlarmProtocolError",
      );
      const handler = yield* makeSubscriptionPartitionAlarmHandler({ ...options, tag: event.tag });

      expect((yield* handler.handle({ ...event, tag: "other/task" }).pipe(Effect.flip))._tag).toBe(
        "SubscriptionAlarmProtocolError",
      );
      expect((yield* handler.handle({ ...event, payload: 42 }).pipe(Effect.flip))._tag).toBe(
        "SubscriptionAlarmProtocolError",
      );
    }),
  ));

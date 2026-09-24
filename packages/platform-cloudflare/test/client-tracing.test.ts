import {
  threadNamespaceLayer,
  type ThreadObjectRpc,
} from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/cloudflare-thread-client";
import { BrowserCrypto } from "@effect/platform-browser";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema, Tracer } from "effect";
import { CanonicalSequence } from "effect-agent/records";
import { TestClock } from "effect/testing";

import { decodeThreadId } from "./fixtures.ts";

const binding = "TASK_ORCHESTRATORS";
const threadId = decodeThreadId("private-thread-not-a-span-name");

const zeroSequence = Schema.decodeSync(CanonicalSequence)(0);

const clientMethods = [
  "awaitProgressEncoded",
  "cancelProgressEncoded",
] as const satisfies ReadonlyArray<keyof ThreadObjectRpc>;

type ClientMethod = (typeof clientMethods)[number];
interface NativeCall {
  readonly method: ClientMethod;
  readonly args: ReadonlyArray<unknown>;
}

// A local native transport substitute records the actual argument list, including undefined.
const clientFixture = (
  invoke: (method: ClientMethod, args: ReadonlyArray<unknown>) => Promise<unknown>,
  options: { readonly rpcTracing?: boolean; readonly binding?: string } = {},
) => {
  const calls: Array<NativeCall> = [];
  const spans: Array<Tracer.NativeSpan> = [];

  const stub = Object.fromEntries(
    clientMethods.map((method) => [
      method,
      (...args: Array<unknown>) => {
        calls.push({ method, args });

        return invoke(method, args);
      },
    ]),
  );

  const service = options.binding ?? binding;

  const layer = CloudflareThreadClient.layer.pipe(
    Layer.provide([
      threadNamespaceLayer(
        { [service]: { idFromName: (name: string) => name, get: () => stub } },
        service,
        options,
      ),
      BrowserCrypto.layer,
    ]),
  );

  const tracer = Tracer.make({
    span(spanOptions) {
      const span = new Tracer.NativeSpan(spanOptions);

      spans.push(span);

      return span;
    },
  });

  return { calls, spans, layer, tracer };
};

describe("DEPLOY-016 opt-in native Thread RPC tracing", () => {
  it.effect.each(["stalled"] as const)(
    "preserves caller interruption and finishes cleanup when remote cancellation is %s",
    () => {
      const started = Deferred.makeUnsafe<void>();
      const cancelling = Deferred.makeUnsafe<void>();
      const response = Deferred.makeUnsafe<unknown>();
      const cancelled = Deferred.makeUnsafe<unknown>();
      let finalized = 0;

      const fixture = clientFixture(
        (method) => {
          if (method === "awaitProgressEncoded") {
            Deferred.doneUnsafe(started, Effect.void);

            return Effect.runPromise(Deferred.await(response));
          }
          Deferred.doneUnsafe(cancelling, Effect.void);

          return Effect.runPromise(Deferred.await(cancelled));
        },
        { rpcTracing: true },
      );

      return Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        const waiting = yield* client.awaitProgress(threadId, zeroSequence).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              finalized++;
            }),
          ),
          Effect.forkChild,
        );

        yield* Deferred.await(started);
        const interrupting = yield* Fiber.interrupt(waiting).pipe(Effect.forkChild);

        yield* Deferred.await(cancelling);
        {
          expect(finalized).toBe(0);
          yield* TestClock.adjust("1 second");
        }
        yield* Fiber.join(interrupting);
        const exit = yield* Fiber.await(waiting);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
        expect(finalized).toBe(1);
        expect(fixture.calls.map((call) => call.method)).toEqual([
          "awaitProgressEncoded",
          "cancelProgressEncoded",
        ]);
      }).pipe(
        Effect.provide(fixture.layer),
        Effect.ensuring(Deferred.succeed(response, { _tag: "ProgressObserved" })),
        Effect.ensuring(Deferred.succeed(cancelled, { _tag: "ProgressCancelled" })),
      );
    },
  );
});

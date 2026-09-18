import type { DurableObjectState } from "alchemy/Cloudflare/Workers/DurableObjectState";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Context, Effect, Layer, type Scope } from "effect";
import { expect, expectTypeOf, it } from "vite-plus/test";

import type * as MemoryObject from "../src/MemoryObject.ts";
import type * as Scheduling from "../src/Scheduling.ts";
import type * as Subscriptions from "../src/Subscriptions.ts";
import * as ThreadObject from "../src/ThreadObject.ts";

type ThreadConstructor = Effect.Success<ReturnType<typeof ThreadObject.make>>;

it("keeps native constructor dependencies and RPC invocation scope explicit", () => {
  expectTypeOf<
    Effect.Services<ReturnType<typeof ThreadObject.make>>
  >().toEqualTypeOf<WorkerEnvironment>();
  expectTypeOf<Effect.Services<ThreadConstructor>>().toEqualTypeOf<
    DurableObjectState | Scope.Scope
  >();
  expectTypeOf<Effect.Error<ThreadConstructor>>().toEqualTypeOf<never>();
  expectTypeOf<
    Effect.Services<ReturnType<ThreadObject.Rpc["submitEncoded"]>>
  >().toEqualTypeOf<Scope.Scope>();
  expectTypeOf<Effect.Error<ReturnType<MemoryObject.Rpc["memory"]>>>().toEqualTypeOf<never>();
  expectTypeOf<Effect.Services<ReturnType<Scheduling.Rpc["alarm"]>>>().toEqualTypeOf<Scope.Scope>();
  expectTypeOf<
    Effect.Services<ReturnType<Subscriptions.Rpc["alarm"]>>
  >().toEqualTypeOf<Scope.Scope>();
});

class ApplicationValue extends Context.Service<ApplicationValue, { value: string }>()(
  "types/ApplicationValue",
) {}
class EventValue extends Context.Service<EventValue, { value: number }>()("types/EventValue") {}
class MissingValue extends Context.Service<MissingValue, { missing: true }>()(
  "types/MissingValue",
) {}

it("preserves custom RPC arguments/results and requires every application/event dependency", () => {
  const application = ThreadObject.layer([]).pipe(
    Layer.provideMerge(Layer.succeed(ApplicationValue, { value: "app" })),
  );

  const options = {
    namespaceBinding: "THREADS",
    deploymentId: "type-test",
    producerPrefix: "type-test",
    eventLayer: Layer.succeed(EventValue, { value: 2 }),
  };

  const handlers = {
    custom: (prefix: string, count?: number) =>
      Effect.gen(function* () {
        const app = yield* ApplicationValue;
        const event = yield* EventValue;

        yield* Effect.scope;
        if (count === 0) return yield* Effect.fail("empty" as const);

        return { label: `${prefix}:${app.value}`, count: count ?? event.value };
      }),
  };

  const constructor = ThreadObject.make(application, options, handlers);

  type Rpc = Effect.Success<Effect.Success<typeof constructor>>;

  expectTypeOf<Parameters<Rpc["custom"]>>().toEqualTypeOf<[prefix: string, count?: number]>();
  expectTypeOf<Effect.Success<ReturnType<Rpc["custom"]>>>().toEqualTypeOf<{
    label: string;
    count: number;
  }>();
  expectTypeOf<Effect.Error<ReturnType<Rpc["custom"]>>>().toEqualTypeOf<never>();
  expectTypeOf<Effect.Services<ReturnType<Rpc["custom"]>>>().toEqualTypeOf<Scope.Scope>();
  expectTypeOf<Rpc>().toEqualTypeOf<ThreadObject.Rpc<typeof handlers>>();

  // @ts-expect-error MissingValue is not supplied by the application or invocation layer.
  void ThreadObject.make(application, options, { custom: () => MissingValue.asEffect() });
  // An event dependency is unavailable when no event layer supplies it.
  expectTypeOf<typeof handlers>().not.toExtend<ThreadObject.Handlers<ApplicationValue>>();
  // @ts-expect-error Framework operations cannot be replaced.
  void ThreadObject.make(application, options, { submitEncoded: () => Effect.void });
  // @ts-expect-error Native lifecycle handlers are not custom RPCs.
  void ThreadObject.make(application, options, { fetch: () => Effect.void });
  // @ts-expect-error Object prototype names are reserved.
  void ThreadObject.make(application, options, { constructor: () => Effect.void });
});

it.each(["wake", "fetch", "constructor", "then"])(
  "rejects dynamic reserved RPC %s before constructing the application",
  async (name) => {
    const handlers = Object.fromEntries([[name, () => Effect.void]]);

    const constructor = ThreadObject.make(
      ThreadObject.layer([]),
      {
        namespaceBinding: "THREADS",
        deploymentId: "test",
        producerPrefix: "test",
      },
      handlers,
    );

    await expect(
      Effect.runPromise(constructor.pipe(Effect.provideService(WorkerEnvironment, {}))),
    ).rejects.toThrow(`Custom RPC name '${name}' is reserved`);
  },
);

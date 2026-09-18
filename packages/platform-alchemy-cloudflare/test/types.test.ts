import type { DurableObjectState } from "alchemy/Cloudflare/Workers/DurableObjectState";
import type { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { type Effect, type Scope } from "effect";
import { expectTypeOf, it } from "vite-plus/test";

import type * as MemoryObject from "../src/MemoryObject.ts";
import type * as Scheduling from "../src/Scheduling.ts";
import type * as Subscriptions from "../src/Subscriptions.ts";
import type * as ThreadObject from "../src/ThreadObject.ts";

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

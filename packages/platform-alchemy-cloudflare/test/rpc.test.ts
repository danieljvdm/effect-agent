import { RpcStrategy } from "@effect-agent/platform-cloudflare/cloudflare-rpc";
import { Effect } from "effect";
import { expect, it } from "vite-plus/test";

import * as Rpc from "../src/Rpc.ts";

it("shares targets only within one invocation and reacquires invalidated targets", async () => {
  const owner = {};
  let acquisitions = 0;
  const create = () => ({ acquisition: ++acquisitions });

  await Effect.runPromise(
    Effect.gen(function* () {
      const strategy = yield* RpcStrategy;

      const call = Effect.gen(function* () {
        const first = yield* strategy.get(owner, "thread", create);

        expect(yield* Rpc.withScope(strategy.get(owner, "thread", create))).toBe(first);
        yield* strategy.invalidate(first);
        const replacement = yield* strategy.get(owner, "thread", create);

        expect(replacement).not.toBe(first);
        expect(yield* strategy.get(owner, "thread", create)).toBe(replacement);
      }).pipe(Rpc.withScope, Effect.scoped);

      yield* call;
      yield* call;
      expect(acquisitions).toBe(4);
      expect(yield* strategy.traceArguments).toEqual([]);
    }).pipe(Effect.provide(Rpc.layer)),
  );
});

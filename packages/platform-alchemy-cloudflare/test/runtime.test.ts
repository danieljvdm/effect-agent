import { type ThreadObjectRpc } from "@effect-agent/platform-cloudflare/cloudflare-host-bindings";
import { BrowserCrypto } from "@effect/platform-browser";
import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { digestDefinitions } from "effect-agent/digest";
import { ThreadId } from "effect-agent/identifiers";
import { IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { type Crypto } from "effect/Crypto";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import { definitions, eventFinalizers, initializationFinalizers, planner } from "./fixtures.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      THREADS: DurableObjectNamespace<ThreadObjectRpc>;
      PROBES: DurableObjectNamespace;
    }
  }
}

const run = <A, E>(effect: Effect.Effect<A, E, CloudflareThreadClient | Crypto>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide([
        CloudflareThreadClient.layerFromBinding({ namespace: env.THREADS }),
        BrowserCrypto.layer,
      ]),
    ),
  );

describe("Alchemy durable host", () => {
  it("admits once, executes by alarm, and returns the schema-validated settlement", async () => {
    const name = `thread-${crypto.randomUUID()}`;

    const submit = CloudflareThreadClient.use((client) =>
      Effect.gen(function* () {
        const hashes = yield* digestDefinitions(definitions);

        return yield* client.submit(
          { definition: planner },
          { question: "hello" },
          {
            threadId: ThreadId.make(name),
            principal: Principal.make("test-user"),
            idempotencyKey: IdempotencyKey.make("first"),
            definitions: hashes,
          },
        );
      }),
    );

    const receipt = await run(submit);

    expect(await run(submit)).toEqual(receipt);
    await runDurableObjectAlarm(env.THREADS.getByName(name));

    const settlement = await run(
      CloudflareThreadClient.use((client) => client.awaitSettlement(receipt)),
    );

    expect(settlement.outcome).toBe("completed");
  });

  it("keeps each object application's scope and memoized services separate from its events", async () => {
    const names = [`first-${crypto.randomUUID()}`, `second-${crypto.randomUUID()}`];

    for (const name of names) {
      const stub = env.PROBES.getByName(name);
      const remote = await Reflect.apply(Reflect.get(stub, "inspect"), stub, []);

      expect(remote).toEqual({
        name,
        label: "alchemy-config",
        acquisitions: 1,
        differentScope: true,
      });
      await runInDurableObject(stub, async (instance) => {
        const result = await Reflect.apply(Reflect.get(instance, "inspect"), instance, []);

        expect(result).toEqual({
          name,
          label: "alchemy-config",
          acquisitions: 1,
          differentScope: true,
        });
        expect(await Reflect.apply(Reflect.get(instance, "inspect"), instance, [])).toEqual(result);
      });
      expect(initializationFinalizers).not.toContain(name);

      // Await the native RpcPromise once before handing it to Vitest's matcher.
      const optional = (async () =>
        await Reflect.apply(Reflect.get(stub, "onlyFirst"), stub, []))();

      if (name.startsWith("first-")) await expect(optional).resolves.toBe(name);
      else await expect(optional).rejects.toThrow("not found on Durable Object");
    }
    const first = env.PROBES.getByName(names[0]!);

    expect(await Reflect.apply(Reflect.get(first, "inspect"), first, [])).toMatchObject({
      name: names[0],
      acquisitions: 1,
    });
  });

  it.each(["success", "failure", "defect", "timeout", "interrupt"] as const)(
    "finalizes the event scope after %s",
    async (mode) => {
      const name = `event-${mode}-${crypto.randomUUID()}`;
      const stub = env.PROBES.getByName(name);

      await runInDurableObject(stub, async (instance, state) => {
        const pending: Array<Promise<unknown>> = [];
        const original = state.waitUntil.bind(state);

        state.waitUntil = (promise) => {
          pending.push(promise);
          original(promise);
        };
        try {
          const result = Reflect.apply(Reflect.get(instance, "outcome"), instance, [mode]);

          if (mode === "success") await expect(result).resolves.toBe("ok");
          else if (mode === "failure")
            await expect(result).resolves.toMatchObject({ error: "expected failure" });
          else await expect(result).rejects.toBeDefined();
          await Promise.all(pending);
          expect(eventFinalizers.filter((value) => value === `${name}:${mode}`)).toHaveLength(1);
        } finally {
          state.waitUntil = original;
        }
      });
    },
  );

  it("releases acquired constructor resources when initialization fails", async () => {
    const name = `fail-init-${crypto.randomUUID()}`;

    await expect(env.PROBES.getByName(name).fetch("https://test/")).rejects.toBeDefined();
    expect(initializationFinalizers.filter((value) => value === name)).toHaveLength(1);
  });

  it("acquires event resources only for invocation and rejects native RPC when acquisition fails", async () => {
    const name = `fail-event-${crypto.randomUUID()}`;
    const stub = env.THREADS.getByName(name);

    await runInDurableObject(stub, async (instance, state) => {
      expect(eventFinalizers).not.toContain(`${name}:thread-event`);
      const pending: Array<Promise<unknown>> = [];
      const original = state.waitUntil.bind(state);

      state.waitUntil = (promise) => {
        pending.push(promise);
        original(promise);
      };
      try {
        await expect(Reflect.apply(Reflect.get(instance, "wake"), instance, [])).rejects.toThrow(
          "event acquisition failed",
        );
        await Promise.all(pending);
        expect(eventFinalizers.filter((value) => value === `${name}:thread-event`)).toHaveLength(1);
      } finally {
        state.waitUntil = original;
      }
    });

    const nativeFailure = await Effect.runPromise(
      Effect.tryPromise({
        try: async () => {
          await stub.submitEncoded("invalid");
        },
        catch: (cause) => String(cause),
      }).pipe(Effect.flip),
    );

    expect(nativeFailure).toContain("event acquisition failed");
  });

  it("runs custom native RPCs in the existing application and cleans each invocation", async () => {
    const names = [`custom-first-${crypto.randomUUID()}`, `custom-second-${crypto.randomUUID()}`];

    for (const name of names) {
      const stub = env.THREADS.getByName(name);
      const pending: Array<Promise<unknown>> = [];
      let restore = () => {};

      await runInDurableObject(stub, async (_instance, state) => {
        const original = state.waitUntil.bind(state);

        state.waitUntil = (promise) => {
          pending.push(promise);
          original(promise);
        };
        restore = () => {
          state.waitUntil = original;
        };
      });
      try {
        for (const suffix of [1, 2]) {
          const result = await Reflect.apply(Reflect.get(stub, "customInspect"), stub, [
            "prefix",
            suffix,
          ]);

          expect(result).toEqual({
            label: `prefix:${name}:${suffix}`,
            event: `event:${name}`,
            acquisitions: 1,
            differentScope: true,
          });
        }
        await Promise.all(pending);
        expect(eventFinalizers.filter((value) => value === `${name}:thread-event`)).toHaveLength(2);
        expect(initializationFinalizers).not.toContain(name);
      } finally {
        restore();
      }
    }
  });

  it.each(["typed", "throw"] as const)(
    "rejects a custom RPC %s failure and finalizes its event",
    async (mode) => {
      const name = `custom-fail-${mode}-${crypto.randomUUID()}`;
      const stub = env.THREADS.getByName(name);
      const pending: Array<Promise<unknown>> = [];
      let restore = () => {};

      await runInDurableObject(stub, async (_instance, state) => {
        const original = state.waitUntil.bind(state);

        state.waitUntil = (promise) => {
          pending.push(promise);
          original(promise);
        };
        restore = () => {
          state.waitUntil = original;
        };
      });
      try {
        const result = (async () =>
          await Reflect.apply(Reflect.get(stub, "customFail"), stub, [mode]))();

        await expect(result).rejects.toThrow(
          mode === "typed" ? "custom expected failure" : "custom synchronous defect",
        );
        await Promise.all(pending);
        expect(eventFinalizers.filter((value) => value === `${name}:thread-event`)).toHaveLength(1);
      } finally {
        restore();
      }
    },
  );
});

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vite-plus/test";

import type { TelemetryThreadObject } from "./worker.ts";

describe("effect-cf Durable Object observability ownership", () => {
  const runEventAndDrainFlush = async <A>(
    threadId: string,
    run: (instance: TelemetryThreadObject) => Promise<A> | A,
    options?: { readonly failFlush?: boolean },
  ): Promise<A> => {
    const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(threadId));

    return runInDurableObject(stub, async (instance, state) => {
      const eventWaitUntilWork: Array<Promise<unknown>> = [];
      const nativeWaitUntil = state.waitUntil.bind(state);

      const waitUntil = vi.spyOn(state, "waitUntil").mockImplementation((promise) => {
        eventWaitUntilWork.push(promise);
        nativeWaitUntil(promise);
      });

      try {
        if (options?.failFlush === true) {
          instance.failNextFlush();
        }
        const result = await run(instance);

        expect(eventWaitUntilWork).toHaveLength(1);
        await expect(Promise.all(eventWaitUntilWork)).resolves.toEqual([undefined]);
        expect(await instance.flushCount()).toBe(1);

        return result;
      } finally {
        waitUntil.mockRestore();
      }
    });
  };

  it("routes a native RPC flush through effect-cf's event-scoped waitUntil", async () => {
    const result = await runEventAndDrainFlush("effect-cf-observability-rpc", (instance) =>
      instance.observePage({}),
    );

    expect(result).toMatchObject({ _tag: "HostFailed" });
  });

  it("routes an alarm flush through effect-cf's event-scoped waitUntil", async () => {
    const result = await runEventAndDrainFlush("effect-cf-observability-alarm", (instance) =>
      instance.alarm(),
    );

    expect(result).toBeUndefined();
  });

  it("isolates exporter defects from the exact native RPC result", async () => {
    const expected = await runEventAndDrainFlush(
      "effect-cf-observability-rpc-control",
      (instance) => instance.observePage({}),
    );

    const actual = await runEventAndDrainFlush(
      "effect-cf-observability-rpc-failing-exporter",
      (instance) => instance.observePage({}),
      { failFlush: true },
    );

    expect(actual).toEqual(expected);
  });

  it("isolates exporter defects from successful raw alarm delivery", async () => {
    const result = await runEventAndDrainFlush(
      "effect-cf-observability-alarm-failing-exporter",
      (instance) => instance.alarm(),
      { failFlush: true },
    );

    expect(result).toBeUndefined();
  });
});

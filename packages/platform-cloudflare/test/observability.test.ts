import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vite-plus/test";

import type { TelemetryConversationObject } from "./worker.ts";

describe("effect-cf Durable Object observability ownership", () => {
  const runEventAndDrainFlush = async (
    conversationId: string,
    run: (instance: TelemetryConversationObject) => Promise<unknown> | void,
  ): Promise<void> => {
    const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));
    await runInDurableObject(stub, async (instance, state) => {
      const eventWaitUntilWork: Array<Promise<unknown>> = [];
      const nativeWaitUntil = state.waitUntil.bind(state);
      const waitUntil = vi.spyOn(state, "waitUntil").mockImplementation((promise) => {
        eventWaitUntilWork.push(promise);
        nativeWaitUntil(promise);
      });

      try {
        await run(instance);
        expect(eventWaitUntilWork).toHaveLength(1);
        await Promise.all(eventWaitUntilWork);
        expect(await instance.flushCount()).toBe(1);
      } finally {
        waitUntil.mockRestore();
      }
    });
  };

  it("routes a native RPC flush through effect-cf's event-scoped waitUntil", async () => {
    await runEventAndDrainFlush("effect-cf-observability-rpc", (instance) =>
      instance.observePage({}),
    );
  });

  it("routes an alarm flush through effect-cf's event-scoped waitUntil", async () => {
    await runEventAndDrainFlush("effect-cf-observability-alarm", (instance) => instance.alarm());
  });
});

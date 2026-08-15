import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { resetFlushCount } from "./observability-fixture.ts";

describe("effect-cf Durable Object observability ownership", () => {
  const conversationId = "effect-cf-observability";

  beforeEach(() => resetFlushCount(conversationId));

  it("schedules exactly one event-scoped OTLP flush after one native RPC", async () => {
    const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));

    await stub.observePage({});

    await vi.waitFor(async () => {
      expect(await stub.flushCount()).toBe(1);
    });
  });

  it("schedules exactly one event-scoped OTLP flush after one alarm", async () => {
    const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));

    await runInDurableObject(stub, async (_instance, state) => {
      await state.storage.setAlarm(Date.now() + 60_000);
    });

    expect(await runDurableObjectAlarm(stub)).toBe(true);

    await vi.waitFor(async () => {
      expect(await stub.flushCount()).toBe(1);
    });
  });
});

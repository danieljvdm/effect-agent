import { env } from "cloudflare:test";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { resetFlushCount } from "./observability-fixture.ts";

describe("effect-cf native RPC observability ownership", () => {
  const conversationId = "effect-cf-observability";

  beforeEach(() => resetFlushCount(conversationId));

  it("schedules exactly one event-scoped OTLP flush after one native RPC", async () => {
    const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(conversationId));

    await stub.observePage({});

    await vi.waitFor(async () => {
      expect(await stub.flushCount()).toBe(1);
    });
  });
});

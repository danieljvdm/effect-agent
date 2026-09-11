import { Schema } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { adminEmail } from "../src/access-domain.ts";
import { changeDemoAccessAtom, demoAccessAtom, demoEmailAtom } from "../src/demo-access-state.ts";
import { openAiConnectionAtom, sessionAtom } from "../src/state.ts";

const Packet = Schema.Struct({ id: Schema.Unknown, tag: Schema.String, payload: Schema.Unknown });

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("refreshes sponsored connection metadata after a grant and hides administration after account changes", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("location", new URL("https://planner.test"));
  let email = adminEmail;
  const emails: string[] = [];
  const writes: string[] = [];

  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(
      input instanceof Request ? input : new URL(String(input), "https://planner.test"),
      init,
    );

    const packet = Schema.decodeUnknownSync(Schema.fromJsonString(Packet))(
      (await request.text()).trim(),
    );

    if (packet.tag === "GrantDemoAccess") {
      const granted = Schema.decodeUnknownSync(Schema.Struct({ email: Schema.String }))(
        packet.payload,
      );

      writes.push(granted.email);
      emails.push(granted.email);
    }

    const value =
      packet.tag === "GetSession"
        ? { email, isAdmin: email === adminEmail }
        : packet.tag === "GetOpenAiConnection"
          ? {
              connected: emails.includes(email),
              lastFour: null,
              updatedAt: null,
              ...(emails.includes(email) ? { source: "demo" } : {}),
            }
          : { emails, configured: true };

    return new Response(
      `${JSON.stringify({ _tag: "Exit", requestId: packet.id, exit: { _tag: "Success", value } })}\n`,
      { headers: { "content-type": "application/ndjson" } },
    );
  });
  const registry = AtomRegistry.make({ defaultIdleTTL: 0, timeoutResolution: 1 });

  const unmounts = [
    registry.mount(demoAccessAtom),
    registry.mount(openAiConnectionAtom),
    registry.mount(changeDemoAccessAtom),
  ];

  try {
    await vi.advanceTimersByTimeAsync(1);
    expect(AsyncResult.value(registry.get(demoAccessAtom))).toMatchObject({
      value: { emails: [] },
    });
    registry.set(demoEmailAtom, adminEmail.toUpperCase());
    registry.set(changeDemoAccessAtom, { action: "grant" });
    await vi.advanceTimersByTimeAsync(1);
    expect(writes).toEqual([adminEmail]);
    expect(registry.get(demoEmailAtom)).toBe("");
    expect(AsyncResult.value(registry.get(openAiConnectionAtom))).toMatchObject({
      value: { connected: true, source: "demo" },
    });
    email = "other@example.com";
    registry.refresh(sessionAtom);
    await vi.advanceTimersByTimeAsync(1);
    expect(AsyncResult.isInitial(registry.get(demoAccessAtom))).toBe(true);
    registry.set(demoEmailAtom, email);
    registry.set(changeDemoAccessAtom, { action: "grant" });
    await vi.advanceTimersByTimeAsync(1);
    expect(writes).toEqual([adminEmail]);
    expect(AsyncResult.isFailure(registry.get(changeDemoAccessAtom))).toBe(true);
  } finally {
    unmounts.forEach((unmount) => unmount());
    registry.dispose();
  }
});

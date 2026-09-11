import { Redacted, Schema } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";

import {
  changeOpenAiConnectionAtom,
  openAiConnectionAtom,
  refreshOpenAiConnectionAtom,
  sessionAtom,
} from "../src/state.ts";

const Packet = Schema.Struct({ id: Schema.Unknown, tag: Schema.String, payload: Schema.Unknown });
const connected = { connected: true, lastFour: "1111", updatedAt: "2026-09-10T00:00:00Z" };
const disconnected = { connected: false, lastFour: null, updatedAt: null };
const fetchMock = vi.fn<typeof fetch>();

const setup = () => {
  vi.useFakeTimers();
  vi.stubGlobal("location", new URL("https://planner.test"));
  let email = "first@example.com";

  const reads: {
    email: string;
    succeed: (value: typeof connected | typeof disconnected) => void;
  }[] = [];

  const writes: string[] = [];

  vi.stubGlobal(
    "fetch",
    fetchMock.mockImplementation(async (input, init) => {
      const request = new Request(
        input instanceof Request ? input : new URL(input, "https://planner.test"),
        init,
      );

      const packet = Schema.decodeUnknownSync(Schema.fromJsonString(Packet))(
        (await request.text()).trim(),
      );

      const response = (exit: unknown) =>
        new Response(`${JSON.stringify({ _tag: "Exit", requestId: packet.id, exit })}\n`, {
          headers: { "content-type": "application/ndjson" },
        });

      if (packet.tag === "GetOpenAiConnection")
        return new Promise<Response>((resolve) =>
          reads.push({ email, succeed: (value) => resolve(response({ _tag: "Success", value })) }),
        );
      if (packet.tag === "ConnectOpenAi" || packet.tag === "DisconnectOpenAi") {
        writes.push(email);

        return response({
          _tag: "Failure",
          cause: [
            {
              _tag: "Fail",
              error: { _tag: "PlannerError", code: "unavailable", message: "Try again" },
            },
          ],
        });
      }
      throw new Error(`Unexpected fixture request: ${packet.tag}`);
    }),
  );
  const registry = AtomRegistry.make({ defaultIdleTTL: 0, timeoutResolution: 1 });

  registry.set(sessionAtom, AsyncResult.success({ subjectId: email, displayName: email }));

  const unmounts = [
    registry.mount(openAiConnectionAtom),
    registry.mount(changeOpenAiConnectionAtom),
  ];

  return {
    registry,
    reads,
    writes,
    switchAccount() {
      email = "second@example.com";
      registry.set(sessionAtom, AsyncResult.success({ subjectId: email, displayName: email }));
    },
    close() {
      for (const unmount of unmounts) unmount();
      registry.dispose();
    },
  };
};

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});
const flush = () => vi.advanceTimersByTimeAsync(1);

it("ignores late connection metadata from a previous account", async () => {
  const fixture = setup();

  try {
    await flush();
    fixture.switchAccount();
    await flush();
    expect(fixture.reads.map((read) => read.email)).toEqual([
      "first@example.com",
      "second@example.com",
    ]);
    fixture.reads[0]?.succeed(connected);
    await flush();
    expect(AsyncResult.value(fixture.registry.get(openAiConnectionAtom))._tag).toBe("None");
    fixture.reads[1]?.succeed(disconnected);
    await flush();
    expect(AsyncResult.value(fixture.registry.get(openAiConnectionAtom))).toMatchObject({
      value: disconnected,
    });
  } finally {
    fixture.close();
  }
});

it("does not carry a key edit across an unresolved sign-in and refresh clears failed mutations", async () => {
  const fixture = setup();

  try {
    fixture.registry.set(sessionAtom, AsyncResult.waiting(fixture.registry.get(sessionAtom)));
    fixture.registry.set(changeOpenAiConnectionAtom, {
      action: "connect",
      apiKey: Redacted.make("sk-fixture-private-1111"),
    });
    await flush();
    expect(fixture.writes).toEqual([]);
    expect(AsyncResult.isFailure(fixture.registry.get(changeOpenAiConnectionAtom))).toBe(true);
    fixture.registry.set(
      sessionAtom,
      AsyncResult.success({ subjectId: "first@example.com", displayName: "first@example.com" }),
    );
    fixture.reads[0]?.succeed(disconnected);
    await flush();
    fixture.registry.set(changeOpenAiConnectionAtom, {
      action: "connect",
      apiKey: Redacted.make("sk-fixture-private-1111"),
    });
    await flush();
    expect(AsyncResult.error(fixture.registry.get(changeOpenAiConnectionAtom))).toMatchObject({
      value: { message: "Try again" },
    });
    expect(fixture.writes).toEqual(["first@example.com"]);
    expect(AsyncResult.isFailure(fixture.registry.get(changeOpenAiConnectionAtom))).toBe(true);
    fixture.registry.set(refreshOpenAiConnectionAtom, undefined);
    await flush();
    expect(AsyncResult.isInitial(fixture.registry.get(changeOpenAiConnectionAtom))).toBe(true);
    fixture.reads.at(-1)?.succeed(disconnected);
    await flush();
    expect(AsyncResult.value(fixture.registry.get(openAiConnectionAtom))).toMatchObject({
      value: disconnected,
    });
  } finally {
    fixture.close();
  }
});

import { Effect, Schema, Stream } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { selectionAtom, sessionAtom } from "../src/state.ts";
import * as browser from "../src/voice/browser.ts";
import {
  startVoiceAtom,
  stopVoiceAtom,
  voiceBoundaryAtom,
  voiceViewAtom,
} from "../src/voice/state.ts";

const Packet = Schema.Struct({ id: Schema.Unknown, tag: Schema.String });

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

it("starts a fresh conversation, retains stop controls, and clears captions across identities", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("location", new URL("https://planner.test"));
  vi.stubGlobal("sessionStorage", { getItem: () => null, setItem: () => {} });
  let email = "first@example.com";

  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(
      input instanceof Request ? input : new URL(String(input), "https://planner.test"),
      init,
    );

    const packet = Schema.decodeUnknownSync(Schema.fromJsonString(Packet))(
      (await request.text()).trim(),
    );

    const exit =
      packet.tag === "GetSession"
        ? { _tag: "Success", value: { email, isAdmin: false } }
        : {
            _tag: "Failure",
            cause: [
              {
                _tag: "Fail",
                error: { _tag: "PlannerError", code: "unavailable", message: "No snapshot" },
              },
            ],
          };

    return new Response(`${JSON.stringify({ _tag: "Exit", requestId: packet.id, exit })}\n`, {
      headers: { "content-type": "application/ndjson" },
    });
  });

  const events = Stream.make(
    { type: "session.started" as const, session: { id: "live-test" } },
    {
      type: "session.input_transcript.delta" as const,
      event_id: "caption",
      delta: "Private trip",
      start_ms: 0,
      end_ms: 100,
    },
  ).pipe(Stream.concat(Stream.never));

  const sends: string[] = [];
  let finalized = 0;

  vi.spyOn(browser, "connectBrowserVoice").mockReturnValue(
    Effect.addFinalizer(() =>
      Effect.sync(() => {
        finalized++;
      }),
    ).pipe(
      Effect.as({
        events,
        send: (event) =>
          Effect.sync(() => {
            sends.push(String(event.type));
          }),
        silence: Effect.void,
        resume: Effect.void,
      }),
    ),
  );
  const registry = AtomRegistry.make({ defaultIdleTTL: 0, timeoutResolution: 1 });

  const unmounts = [
    registry.mount(voiceBoundaryAtom),
    registry.mount(voiceViewAtom),
    registry.mount(startVoiceAtom),
  ];

  try {
    await vi.advanceTimersByTimeAsync(1);
    expect(AsyncResult.isSuccess(registry.get(sessionAtom))).toBe(true);
    registry.set(startVoiceAtom, { muted: false } as HTMLAudioElement);
    await vi.advanceTimersByTimeAsync(1000);
    expect(registry.get(selectionAtom).conversationId).toBeTruthy();
    expect(registry.get(voiceViewAtom).status).toBe("listening");
    expect(registry.get(voiceViewAtom).captions).toHaveLength(1);
    registry.set(stopVoiceAtom, undefined);
    await vi.advanceTimersByTimeAsync(6000);
    expect(sends).toEqual(["session.close"]);
    expect(finalized).toBe(1);
    expect(registry.get(voiceViewAtom).captions).toHaveLength(1);
    email = "second@example.com";
    registry.refresh(sessionAtom);
    await vi.advanceTimersByTimeAsync(1);
    expect(registry.get(voiceViewAtom).captions).toEqual([]);
  } finally {
    for (const unmount of unmounts) unmount();
    registry.dispose();
  }
});

import { Effect, Schema, Stream } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { selectionAtom, sessionAtom, messagesAtom } from "../src/state.ts";
import * as browser from "../src/voice/browser.ts";
import {
  startVoiceAtom,
  stopVoiceAtom,
  voiceBoundaryAtom,
  voiceViewAtom,
} from "../src/voice/state.ts";
import { fixtureSession } from "./fixtures/identity.ts";

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

  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(
      input instanceof Request ? input : new URL(String(input), "https://planner.test"),
      init,
    );

    const packet = Schema.decodeUnknownSync(Schema.fromJsonString(Packet))(
      (await request.text()).trim(),
    );

    const exit = {
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
    {
      type: "session.output_transcript.delta" as const,
      event_id: "acknowledgment",
      delta: "Okay.",
      start_ms: 100,
      end_ms: 500,
    },
    {
      type: "session.input_transcript.delta" as const,
      event_id: "continuation",
      delta: " with a hot tub",
      start_ms: 200,
      end_ms: 900,
    },
    {
      type: "session.output_transcript.delta" as const,
      event_id: "first-update",
      delta: "I’m checking.",
      start_ms: 1100,
      end_ms: 2100,
    },
    {
      type: "session.output_transcript.delta" as const,
      event_id: "second-update",
      delta: "Two places look promising.",
      start_ms: 5000,
      end_ms: 6000,
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

  registry.set(sessionAtom, AsyncResult.success(fixtureSession("first@example.com")));

  const unmounts = [
    registry.mount(voiceBoundaryAtom),
    registry.mount(voiceViewAtom),
    registry.mount(messagesAtom),
    registry.mount(startVoiceAtom),
  ];

  try {
    await vi.advanceTimersByTimeAsync(1);
    expect(AsyncResult.isSuccess(registry.get(sessionAtom))).toBe(true);
    registry.set(startVoiceAtom, { muted: false } as HTMLAudioElement);
    await vi.advanceTimersByTimeAsync(1000);
    expect(registry.get(selectionAtom).conversationId).toBeTruthy();
    expect(registry.get(voiceViewAtom).status).toBe("listening");
    expect(registry.get(voiceViewAtom).captions).toHaveLength(5);
    const displayed = registry.get(messagesAtom);

    expect(displayed).toMatchObject([
      { role: "user", text: "Private trip with a hot tub" },
      { role: "assistant", text: "Okay." },
      { role: "assistant", text: "I’m checking." },
      { role: "assistant", text: "Two places look promising." },
    ]);
    registry.set(stopVoiceAtom, undefined);
    await vi.advanceTimersByTimeAsync(6000);
    expect(sends).toEqual(["session.thinking.append", "session.close"]);
    expect(finalized).toBe(1);
    expect(registry.get(voiceViewAtom).captions).toHaveLength(5);
    expect(registry.get(messagesAtom)).toEqual(displayed);
    registry.set(sessionAtom, AsyncResult.success(fixtureSession("second@example.com")));
    await vi.advanceTimersByTimeAsync(1);
    expect(registry.get(voiceViewAtom).captions).toEqual([]);
    expect(registry.get(messagesAtom)).toEqual([]);
  } finally {
    for (const unmount of unmounts) unmount();
    registry.dispose();
  }
});

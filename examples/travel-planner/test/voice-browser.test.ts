import { Effect, Fiber } from "effect";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { connectBrowserVoice } from "../src/voice/browser.ts";

afterEach(() => vi.unstubAllGlobals());

const setup = () => {
  const stop = vi.fn();
  const media = { getTracks: () => [{ stop }], getAudioTracks: () => [{ stop }] };

  const channel = Object.assign(new EventTarget(), {
    close: vi.fn(),
    readyState: "open",
    bufferedAmount: 0,
    send: vi.fn(),
  });

  const close = vi.fn();

  const peer = Object.assign(new EventTarget(), {
    close,
    iceGatheringState: "complete",
    addTrack: vi.fn(),
    createDataChannel: () => channel,
    localDescription: { sdp: "offer" },
    createOffer: async () => ({ type: "offer", sdp: "offer" }),
    setLocalDescription: async () => {},
    setRemoteDescription: vi.fn(async () => {}),
  });

  vi.stubGlobal("RTCPeerConnection", function () {
    return peer;
  });
  vi.stubGlobal("navigator", { mediaDevices: { getUserMedia: async () => media } });
  vi.stubGlobal(
    "fetch",
    vi.fn(async () =>
      Response.json(
        { session: { id: "live-fixture" }, transport: { type: "webrtc", sdp: "answer" } },
        { status: 201 },
      ),
    ),
  );

  // A media boundary fixture, not a schema conversion.
  const audio = {
    srcObject: null,
    muted: false,
    play: vi.fn(async () => {}),
    pause: vi.fn(),
  } as unknown as HTMLAudioElement;

  return { stop, media, channel, close, peer, audio };
};

it("owns and releases media and the peer on normal scope exit and malformed session answers", async () => {
  const test = setup();

  await Effect.runPromise(
    connectBrowserVoice([], test.audio, "00000000-0000-0000-0000-000000000001").pipe(Effect.scoped),
  );
  expect(test.stop).toHaveBeenCalledTimes(1);
  expect(test.close).toHaveBeenCalledTimes(1);
  expect(test.channel.close).toHaveBeenCalledTimes(1);
  expect(test.peer.setRemoteDescription).toHaveBeenCalledWith({ type: "answer", sdp: "answer" });
  vi.stubGlobal("fetch", async () => Response.json({ credential: "never accepted" }));

  const failed = await Effect.runPromiseExit(
    connectBrowserVoice([], test.audio, "00000000-0000-0000-0000-000000000001").pipe(Effect.scoped),
  );

  expect(failed._tag).toBe("Failure");
  expect(test.stop).toHaveBeenCalledTimes(2);
  expect(test.close).toHaveBeenCalledTimes(2);
});

it("stops a microphone grant that arrives after call interruption", async () => {
  const test = setup();
  let grant: ((media: typeof test.media) => void) | undefined;
  let entered: (() => void) | undefined;

  const requested = new Promise<void>((resolve) => {
    entered = resolve;
  });

  vi.stubGlobal("navigator", {
    mediaDevices: {
      getUserMedia: () =>
        new Promise((resolve) => {
          grant = resolve;
          entered?.();
        }),
    },
  });

  const fiber = Effect.runFork(
    connectBrowserVoice([], test.audio, "00000000-0000-0000-0000-000000000001").pipe(Effect.scoped),
  );

  await requested;
  await Effect.runPromise(Fiber.interrupt(fiber));
  grant?.(test.media);
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  expect(test.stop).toHaveBeenCalledTimes(1);
  expect(test.close).toHaveBeenCalledTimes(1);
});

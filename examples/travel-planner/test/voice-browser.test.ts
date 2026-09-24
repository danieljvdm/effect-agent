import { Effect, Fiber, Scope, Stream } from "effect";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { connectBrowserVoice } from "../src/voice/browser.ts";
import { type LiveEvent } from "../src/voice/protocol.ts";

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

it("discards pending voice events when the channel closes", async () => {
  const test = setup();
  const observed: LiveEvent[] = [];

  const event: LiveEvent = {
    type: "session.delegation.created",
    event_id: "pending-delegation",
    offset_ms: 100,
    delegation: { id: "delegation", target: "client" },
  };

  const emit = (data: unknown) => test.channel.dispatchEvent(new MessageEvent("message", { data }));

  await Effect.runPromise(
    Effect.gen(function* () {
      const callScope = yield* Scope.make();

      yield* Effect.addFinalizer((exit) => Scope.close(callScope, exit));

      const connection = yield* connectBrowserVoice(
        [],
        test.audio,
        "00000000-0000-0000-0000-000000000001",
      ).pipe(Effect.provideService(Scope.Scope, callScope));

      emit(JSON.stringify(event));

      test.channel.dispatchEvent(new Event("close"));
      yield* connection.events.pipe(
        Stream.runForEach((received) => Effect.sync(() => observed.push(received))),
        Effect.flip,
      );
      expect(observed).toEqual([]);
    }).pipe(Effect.scoped),
  );
  expect(test.stop).toHaveBeenCalledTimes(1);
  expect(test.close).toHaveBeenCalledTimes(1);
  expect(test.channel.close).toHaveBeenCalledTimes(1);
});

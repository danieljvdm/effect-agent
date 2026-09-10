import { Cause, Effect, Queue, Schema, Stream } from "effect";

import { LiveEvent, VoiceAnswer, VoiceError, type VoiceOffer } from "./protocol.ts";

const failed = () =>
  new VoiceError({ message: "Voice disconnected. Reconnect to check existing work." });

export interface VoiceConnection {
  readonly events: Stream.Stream<LiveEvent, VoiceError>;
  readonly send: (event: Readonly<Record<string, unknown>>) => Effect.Effect<void, VoiceError>;
  readonly silence: Effect.Effect<void>;
  readonly resume: Effect.Effect<void, VoiceError>;
}

/** The call scope owns microphone tracks, media, data listeners, queues and the peer. */
export const connectBrowserVoice = Effect.fn("connectBrowserVoice")(function* (
  history: (typeof VoiceOffer.Type)["history"],
  audio: HTMLAudioElement,
) {
  const queue = yield* Queue.bounded<LiveEvent, VoiceError>(128);

  const peer = yield* Effect.acquireRelease(
    Effect.try({ try: () => new RTCPeerConnection(), catch: failed }),
    (connection) =>
      Effect.sync(() => {
        connection.close();
        audio.pause();
        audio.srcObject = null;
      }),
  );

  let captured: MediaStream | undefined;
  let released = false;

  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      released = true;
      captured?.getTracks().forEach((track) => track.stop());
    }),
  );

  const microphone = yield* Effect.tryPromise({
    // getUserMedia is not abortable. Acquisition stays interruptible and a late grant is released.
    try: async (signal) => {
      const media = await navigator.mediaDevices.getUserMedia({ audio: true });

      if (signal.aborted || released) {
        media.getTracks().forEach((track) => track.stop());
        throw failed();
      }
      captured = media;

      return media;
    },
    catch: () => new VoiceError({ message: "Allow microphone access to start voice." }),
  });

  const channel = yield* Effect.acquireRelease(
    Effect.sync(() => peer.createDataChannel("oai-events")),
    (events) => Effect.sync(() => events.close()),
  );

  const onMessage = (message: MessageEvent) => {
    if (typeof message.data !== "string" || message.data.length > 32 * 1024) {
      Queue.failCauseUnsafe(queue, Cause.fail(failed()));

      return;
    }
    const event = Schema.decodeUnknownOption(Schema.fromJsonString(LiveEvent))(message.data);

    // Forward-compatible events are ignored; never interpreted as task instructions.
    if (event._tag === "Some" && !Queue.offerUnsafe(queue, event.value))
      Queue.failCauseUnsafe(queue, Cause.fail(failed()));
  };

  const onClose = () => Queue.failCauseUnsafe(queue, Cause.fail(failed()));

  const onTrack = (event: RTCTrackEvent) => {
    audio.srcObject = new MediaStream([event.track]);
    // Autoplay failure is visible through native audio controls.
    void audio.play().catch(() => {});
  };

  yield* Effect.acquireRelease(
    Effect.sync(() => {
      channel.addEventListener("message", onMessage);
      channel.addEventListener("close", onClose);
      channel.addEventListener("error", onClose);
      peer.addEventListener("track", onTrack);
      for (const track of microphone.getAudioTracks()) peer.addTrack(track, microphone);
    }),
    () =>
      Effect.sync(() => {
        channel.removeEventListener("message", onMessage);
        channel.removeEventListener("close", onClose);
        channel.removeEventListener("error", onClose);
        peer.removeEventListener("track", onTrack);
        Queue.failCauseUnsafe(queue, Cause.fail(failed()));
      }),
  );
  yield* Effect.tryPromise({
    try: async () => {
      await peer.setLocalDescription(await peer.createOffer());
    },
    catch: failed,
  });
  yield* Effect.callback<void, VoiceError>((resume) => {
    const changed = () => {
      if (peer.iceGatheringState === "complete") resume(Effect.void);
    };

    peer.addEventListener("icegatheringstatechange", changed);
    changed();

    return Effect.sync(() => peer.removeEventListener("icegatheringstatechange", changed));
  }).pipe(Effect.timeout("10 seconds"), Effect.mapError(failed));
  const sdp = peer.localDescription?.sdp;

  if (!sdp) return yield* failed();

  const answer = yield* Effect.tryPromise({
    try: async (signal) => {
      const response = await fetch("/api/voice", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sdp, history }),
        signal,
      });

      if (!response.ok) throw failed();

      return await response.json();
    },
    catch: failed,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(VoiceAnswer)),
    Effect.timeout("30 seconds"),
    Effect.mapError(failed),
  );

  yield* Effect.tryPromise({
    try: () => peer.setRemoteDescription({ type: "answer", sdp: answer.transport.sdp }),
    catch: failed,
  });

  return {
    events: Stream.fromQueue(queue),
    send: (event) =>
      Effect.try({
        try: () => {
          if (channel.readyState !== "open" || channel.bufferedAmount > 32 * 1024) throw failed();
          channel.send(JSON.stringify(event));
        },
        catch: failed,
      }),
    silence: Effect.sync(() => {
      audio.muted = true;
    }),
    resume: Effect.tryPromise({
      try: () => {
        audio.muted = false;

        return audio.play();
      },
      catch: failed,
    }),
  } satisfies VoiceConnection;
});

import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/unstable/reactivity";
import { Mic, PhoneOff, Volume2, VolumeX } from "lucide-react";
import { useEffect, useRef } from "react";

import { captionRows } from "../voice/delegation.ts";
import {
  muteVoiceAtom,
  startVoiceAtom,
  stopVoiceAtom,
  voiceViewAtom,
  voiceBoundaryAtom,
} from "../voice/state.ts";

export function VoiceControls({ enabled }: { readonly enabled: boolean }) {
  useAtomValue(voiceBoundaryAtom);
  const audio = useRef<HTMLAudioElement>(null);
  const [result, start] = useAtom(startVoiceAtom);
  const stop = useAtomSet(stopVoiceAtom);
  const mute = useAtomSet(muteVoiceAtom);
  const view = useAtomValue(voiceViewAtom);

  const active =
    view.status === "connecting" || view.status === "listening" || view.status === "ending";

  useEffect(() => () => stop(), [stop]);

  return (
    <div className="voice-controls">
      <div className="voice-actions">
        <button
          type="button"
          disabled={(!active && !enabled) || view.status === "ending"}
          onClick={() => {
            if (active) stop();
            else if (audio.current) start(audio.current);
          }}
        >
          {active ? <PhoneOff size={16} /> : <Mic size={16} />}
          {active
            ? "End voice"
            : view.status === "disconnected"
              ? "Reconnect voice"
              : "Start voice"}
        </button>
        {active && (
          <button type="button" onClick={() => mute()} aria-pressed={view.muted}>
            {view.muted ? <Volume2 size={16} /> : <VolumeX size={16} />}
            {view.muted ? "Resume audio" : "Stop playback"}
          </button>
        )}
      </div>
      <audio ref={audio} autoPlay controls hidden={!active} aria-label="AI voice playback" />
      {view.note && <p role="status">{view.note}</p>}
      {!active && AsyncResult.isFailure(result) && (
        <p role="alert">
          Voice could not stay connected. Check microphone permission and GPT-Live access, then
          reconnect.
        </p>
      )}
      {view.captions.length > 0 && (
        <details>
          <summary>Voice transcript</summary>
          <div className="voice-captions">
            {captionRows(view.captions).map((row) => (
              <p key={row.id}>
                <strong>{row.speaker}: </strong>
                {row.text}
              </p>
            ))}
          </div>
        </details>
      )}
    </div>
  );
}

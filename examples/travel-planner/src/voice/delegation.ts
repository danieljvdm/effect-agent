import { Schema } from "effect";

import { SendMessageRequest, VoiceWork, type PlannerProgress } from "../domain.ts";
import { shortContext, type Caption, type LiveEvent } from "./protocol.ts";

/** A frozen envelope survives uncertain admission and a replacement media session. */
export const VoiceRequest = Schema.Struct({
  request: SendMessageRequest,
  delegationId: Schema.String,
  sessionId: Schema.String,
  offset: Schema.Natural,
  status: Schema.Literals(["prepared", "accepted", "uncertain", "settled"]),
  receipt: Schema.NullOr(VoiceWork),
});

export type VoiceRequest = typeof VoiceRequest.Type;

/** Local display/context state. Captions never admit work and never grant authority. */
export const appendCaption = (captions: ReadonlyArray<Caption>, event: Caption) => {
  if (captions.some((caption) => caption.event_id === event.event_id)) return captions;

  return [...captions, event].slice(-128);
};

/** Presentation groups grow by speaker; they never establish completed turns. */
export const captionRows = (captions: ReadonlyArray<Caption>) => {
  const rows: Array<{ id: string; speaker: "You" | "AI voice"; text: string }> = [];

  for (const caption of captions) {
    const speaker = caption.type === "session.input_transcript.delta" ? "You" : "AI voice";
    const previous = rows.at(-1);

    if (previous?.speaker === speaker) previous.text += caption.delta;
    else rows.push({ id: caption.event_id, speaker, text: caption.delta });
  }

  return rows;
};

export const delegationMessage = (
  captions: ReadonlyArray<Caption>,
  offset: number,
  after = -1,
): string | null => {
  const selected = captions
    .filter((caption) => caption.start_ms <= offset && caption.end_ms > after)
    .slice(-48);

  if (!selected.some((caption) => caption.type === "session.input_transcript.delta")) return null;
  // The work request contains user words; the attributed dialogue travels separately in voice.messages.
  const fragments: Caption[] = [];
  let size = 0;
  let hasUser = false;

  for (const caption of [...selected].reverse()) {
    // Reserve enough label/separator space even if every fragment changes speaker.
    const length = caption.delta.length + 16;

    if (size + length > 3400) break;
    fragments.unshift(caption);
    size += length;
    hasUser ||= caption.type === "session.input_transcript.delta";
  }
  if (!hasUser) return null;

  return captionRows(fragments)
    .filter((row) => row.speaker === "You")
    .map((row) => row.text)
    .join("\n");
};

/** Output selection is allowlisted; diagnostics, reasoning, cards and tool data never pass. */
export const voiceUpdate = (
  work: VoiceWork,
  progress: PlannerProgress | null,
): { readonly kind: "progress" | "result"; readonly text: string; readonly key: string } | null => {
  if (work.superseded) return null;
  if (work.state === "completed")
    return {
      kind: "result",
      key: `settled:${work.receiptId}`,
      text: shortContext(
        work.text ??
          "The planner completed this request without a separate answer. Check the saved conversation.",
      ),
    };
  if (work.state === "failed" || work.state === "aborted")
    return {
      kind: "result",
      key: `settled:${work.receiptId}`,
      text:
        work.state === "failed"
          ? "The planner could not finish this request. Saved work remains available."
          : "The planner request was stopped. Saved work remains available.",
    };
  if (work.state !== "pending" || progress?.submissionId !== work.submissionId) return null;

  // Full replacement previews are provisional; never present their partial JSON as final output.
  const text = progress.text
    ? `Provisional planner response (work is still running): ${progress.text}`
    : "";

  return text
    ? {
        kind: "progress",
        key: `${progress.attemptId}:${progress.revision}`,
        text: shortContext(text),
      }
    : null;
};

export const isCaption = (event: LiveEvent): event is Caption =>
  event.type === "session.input_transcript.delta" ||
  event.type === "session.output_transcript.delta";

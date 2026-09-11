import type { SpokenMessage } from "../domain.ts";
import type { Caption } from "./protocol.ts";

type Cursor = {
  readonly id: string;
  readonly start: number;
  readonly end: number;
};

export type CaptionGroups = Partial<Record<SpokenMessage["role"], Cursor>>;

/** Revisable display groups, not authoritative turns. Original captions still own their timing. */
export const groupCaption = (
  event: Caption,
  messages: ReadonlyArray<SpokenMessage>,
  groups: CaptionGroups,
  after: string | null,
  makeId: () => string,
): { readonly message: SpokenMessage; readonly groups: CaptionGroups } => {
  const role = event.type === "session.input_transcript.delta" ? "user" : "assistant";
  const cursor = groups[role];
  const prior = messages.find((message) => message.id === cursor?.id);
  const other = groups[role === "user" ? "assistant" : "user"];
  const otherText = messages.find((message) => message.id === other?.id)?.text ?? "";
  // A short backchannel can overlap one continuing thought. A question or a substantive
  // intervening reply separates answers, even when the traveler responds immediately.
  const intervening = cursor && other && other.end > cursor.end && other.end <= event.start_ms;

  const briefAcknowledgment =
    other &&
    other.end - other.start <= 1200 &&
    otherText.trim().length <= 32 &&
    !/[?？]/u.test(otherText);

  const continued =
    cursor &&
    prior &&
    event.start_ms - cursor.end <= (role === "user" ? 3000 : 1500) &&
    (!intervening || (role === "user" && briefAcknowledgment)) &&
    prior.text.length + event.delta.length <= 8000;

  const message: SpokenMessage = {
    id: continued ? prior.id : makeId(),
    role,
    text: continued ? prior.text + event.delta : event.delta,
    after: continued ? prior.after : after,
  };

  return {
    message,
    groups: {
      ...groups,
      [role]: {
        id: message.id,
        start: continued ? Math.min(cursor.start, event.start_ms) : event.start_ms,
        end: continued ? Math.max(cursor.end, event.end_ms) : event.end_ms,
      },
    },
  };
};

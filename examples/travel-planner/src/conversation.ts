import type { PlannerSnapshot, SpokenMessage, VoiceContext } from "./domain.ts";

type Message = PlannerSnapshot["messages"][number];

/** Speech stays at its original place when a late fragment or saved snapshot arrives. */
export const mergeSpeech = <M extends Message>(
  messages: ReadonlyArray<M>,
  speech: ReadonlyArray<SpokenMessage>,
): ReadonlyArray<M | Message> => {
  const result: Array<M | Message> = [...messages];

  for (const row of speech) {
    const existing = result.findIndex((message) => message.id === row.id);

    if (existing !== -1) {
      result[existing] = { ...result[existing]!, text: row.text };
      continue;
    }

    const after = result.findIndex(
      (message) => message.id === row.after || message.requestId === row.after,
    );

    const index = after === -1 ? (row.after === null ? 0 : result.length) : after + 1;

    result.splice(index, 0, { id: row.id, role: row.role, text: row.text, tripId: null });
  }

  return result;
};

/** Freeze whole recent rows for one submission; captions themselves never submit work. */
export const speechContext = (
  messages: ReadonlyArray<SpokenMessage>,
): (typeof VoiceContext.Type)["messages"] => {
  const selected: SpokenMessage[] = [];
  let size = 0;

  for (const message of messages.slice(-48).toReversed()) {
    size += JSON.stringify(message).length + 1;
    if (size > 23000) break;
    selected.unshift(message);
  }

  return selected;
};

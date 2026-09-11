import { expect, it } from "vite-plus/test";

import { mergeSpeech, speechContext } from "../src/conversation.ts";
import type { SpokenMessage } from "../src/domain.ts";
import type { Caption } from "../src/voice/protocol.ts";
import { groupCaption, type CaptionGroups } from "../src/voice/transcript.ts";

const transcript = () => {
  let messages: ReadonlyArray<SpokenMessage> = [];
  let groups: CaptionGroups = {};
  let sequence = 0;

  const add = (role: SpokenMessage["role"], text: string, start: number, end: number) => {
    const event: Caption = {
      event_id: `event-${sequence++}`,
      type: role === "user" ? "session.input_transcript.delta" : "session.output_transcript.delta",
      delta: text,
      start_ms: start,
      end_ms: end,
    };

    const result = groupCaption(
      event,
      messages,
      groups,
      messages.at(-1)?.id ?? null,
      () => `speech-${sequence}`,
    );

    const existing = messages.some((message) => message.id === result.message.id);

    groups = result.groups;
    messages = existing
      ? messages.map((message) => (message.id === result.message.id ? result.message : message))
      : [...messages, result.message].slice(-48);

    return result.message;
  };

  return {
    add,
    rows: () => messages,
    boundary: () => {
      groups = {};
    },
  };
};

it("keeps a user's sentence together across an overlapping acknowledgment and a delayed fragment", () => {
  const t = transcript();
  const original = t.add("user", "Hey, how’s it", 0, 800);

  t.add("assistant", "Hey.", 700, 1000);
  const frozen = speechContext(t.rows());
  const continued = t.add("user", " going?", 800, 1400);

  expect(continued.id).toBe(original.id);
  expect(frozen[0]?.text).toBe("Hey, how’s it");
  expect(t.rows().map((row) => [row.role, row.text])).toEqual([
    ["user", "Hey, how’s it going?"],
    ["assistant", "Hey."],
  ]);
  const saved = frozen.map((row) => ({ ...row, tripId: null }));

  expect(mergeSpeech(saved, t.rows()).map((row) => row.text)).toEqual([
    "Hey, how’s it going?",
    "Hey.",
  ]);
  t.add("user", "Between Christmas and the New", 5000, 7200);
  t.add("assistant", "Okay.", 7400, 7800);
  t.add("user", " Year’s, maybe a big house.", 8100, 9600);
  expect(
    t
      .rows()
      .filter((row) => row.role === "user")
      .map((row) => row.text),
  ).toEqual(["Hey, how’s it going?", "Between Christmas and the New Year’s, maybe a big house."]);
});

it("separates distinct assistant updates by their media timeline, not packet arrival or punctuation", () => {
  const t = transcript();

  t.add("assistant", "Okay. I’m on it.", 0, 1800);
  t.add("assistant", " I’ll check the beaches and golf.", 2200, 4200);
  t.add("assistant", "Still comparing the flight routes.", 16000, 18300);
  t.add("assistant", " I don’t have confirmed matches yet.", 18600, 20800);
  t.add("assistant", "I’ve saved the trip basics.", 33000, 35100);
  expect(t.rows().map((row) => row.text)).toEqual([
    "Okay. I’m on it. I’ll check the beaches and golf.",
    "Still comparing the flight routes. I don’t have confirmed matches yet.",
    "I’ve saved the trip basics.",
  ]);
});

it("keeps quick answers to questions and substantive replies in distinct user messages", () => {
  const t = transcript();

  t.add("user", "A weekend away.", 0, 900);
  t.add("assistant", "When?", 1000, 1500);
  t.add("user", "Friday.", 1600, 1900);
  t.add("assistant", "I can compare a few coastal towns for that weekend.", 2000, 3400);
  t.add("user", "Great.", 3500, 3900);
  expect(t.rows().map((row) => row.text)).toEqual([
    "A weekend away.",
    "When?",
    "Friday.",
    "I can compare a few coastal towns for that weekend.",
    "Great.",
  ]);
});

it("resets grouping at a typed-input or new-call boundary and preserves bounded chunks", () => {
  const t = transcript();

  t.add("user", "Friday", 0, 1000);
  t.boundary();
  t.add("user", "Saturday", 1100, 1800);
  t.add("assistant", "a".repeat(8000), 2000, 2500);
  t.add("assistant", "More", 2500, 2600);
  expect(t.rows().map((row) => row.text)).toEqual(["Friday", "Saturday", "a".repeat(8000), "More"]);
});

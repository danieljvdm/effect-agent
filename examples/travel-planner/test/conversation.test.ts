import { Schema } from "effect";
import { AtomRegistry } from "effect/unstable/reactivity";
import { expect, it } from "vite-plus/test";

import { mergeSpeech, speechContext } from "../src/conversation.ts";
import { SavedTrip, Trip, type PlannerSnapshot, type SpokenMessage } from "../src/domain.ts";
import { draftAtom, selectionAtom, selectTripAtom } from "../src/state.ts";

const trip = Schema.decodeSync(Trip)({
  id: "tahoe-trip",
  revision: 1,
  title: "Tahoe",
  destination: "Lake Tahoe",
  summary: "Fall break",
  startDate: null,
  endDate: null,
  travelers: 2,
  days: [],
  notes: [],
  published: null,
});

it("clears the draft when switching conversations and preserves it when reselecting the same one", () => {
  const registry = AtomRegistry.make();

  try {
    registry.set(draftAtom, "Draft from another trip");
    registry.set(selectTripAtom, { conversationId: "new-conversation", id: null });
    expect(registry.get(selectionAtom).conversationId).toBe("new-conversation");
    expect(registry.get(draftAtom)).toBe("");
    registry.set(draftAtom, "Still writing");
    registry.set(selectTripAtom, { conversationId: "new-conversation", id: null });
    expect(registry.get(draftAtom)).toBe("Still writing");

    const saved = Schema.decodeSync(SavedTrip)({
      ...trip,
      conversationId: "saved-conversation",
    });

    registry.set(selectTripAtom, saved);
    expect(registry.get(draftAtom)).toBe("");
    expect(registry.get(selectionAtom)).toEqual({
      conversationId: "saved-conversation",
      tripId: trip.id,
    });
  } finally {
    registry.dispose();
  }
});

it("keeps speech attributed and ordered as typed input is recorded and late captions grow", () => {
  const speech: SpokenMessage[] = [
    {
      id: "voice-question",
      role: "assistant",
      text: "Starting from where?",
      after: "typed-request",
    },
    { id: "voice-reply", role: "user", text: "The Bay Area", after: "voice-question" },
  ];

  const saved: PlannerSnapshot["messages"] = [
    {
      id: "record-1",
      requestId: "typed-request",
      role: "user",
      text: "A quiet seaside trip",
      tripId: null,
    },
    { id: "record-2", role: "assistant", text: "Two options are ready", tripId: null },
  ];

  const first = mergeSpeech(saved, speech);

  expect(first.map(({ text }) => text)).toEqual([
    "A quiet seaside trip",
    "Starting from where?",
    "The Bay Area",
    "Two options are ready",
  ]);

  const refreshed = mergeSpeech(
    first.map((message) =>
      message.id === "voice-question" ? { ...message, requestId: "some-receipt" } : message,
    ),
    [speech[0]!, { ...speech[1]!, text: "The Bay Area, for three nights." }],
  );

  expect(refreshed.map(({ id }) => id)).toEqual(first.map(({ id }) => id));
  expect(refreshed[1]?.role).toBe("assistant");
  expect(refreshed[2]?.text).toBe("The Bay Area, for three nights.");
  const frozen = speechContext(speech);

  expect(frozen[1]?.text).toBe("The Bay Area");
});

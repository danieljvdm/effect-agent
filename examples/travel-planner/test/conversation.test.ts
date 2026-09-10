import { ThreadExport } from "@effect-agent/thread/ThreadStore";
import { Schema } from "effect";
import { AtomRegistry } from "effect/unstable/reactivity";
import { expect, it } from "vite-plus/test";

import { mergeSpeech, speechContext } from "../src/conversation.ts";
import { SavedTrip, Trip, type PlannerSnapshot, type SpokenMessage } from "../src/domain.ts";
import { legacyTripMessages } from "../src/server/conversation.ts";
import { draftAtom, selectionAtom, selectTripAtom } from "../src/state.ts";

const trip = Schema.decodeUnknownSync(Trip)({
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

it("retains only the selected trip's historical conversation without changing its original log", () => {
  const payloads = [
    {
      _tag: "UserInputRecorded",
      kind: "user",
      submissionId: "first",
      input: { message: "Tahoe in fall", selectedTripId: null, publication: null },
    },
    {
      _tag: "ToolCallSettled",
      runId: "tahoe",
      toolCallId: "save",
      toolName: "save_trip",
      result: trip,
      isFailure: false,
    },
    { _tag: "RunCompleted", runId: "tahoe", output: { message: "Saved Tahoe" } },
    {
      _tag: "SubmissionSettled",
      submissionId: "first",
      settlementId: "first-done",
      receiptId: "first-receipt",
      outcome: "completed",
      runId: "tahoe",
      result: { message: "Saved Tahoe" },
    },
    {
      _tag: "UserInputRecorded",
      kind: "user",
      runId: "lisbon",
      input: { message: "Lisbon please", selectedTripId: "lisbon-trip", publication: null },
    },
    { _tag: "RunCompleted", runId: "lisbon", output: { message: "Only Lisbon" } },
    {
      _tag: "UserInputRecorded",
      kind: "user",
      runId: "tahoe-again",
      input: { message: "A private hot tub is a must", selectedTripId: trip.id, publication: null },
    },
    { _tag: "RunCompleted", runId: "tahoe-again", output: { message: "I will look for hot tubs" } },
  ];

  const source = Schema.decodeUnknownSync(ThreadExport)({
    format: "effect-agent/thread@1",
    threadId: "legacy",
    tailSequence: payloads.length,
    tailDigest: "0".repeat(64),
    records: payloads.map((payload, index) => ({
      threadId: "legacy",
      batchId: "history",
      sequence: index + 1,
      offset: `test-${index}`,
      record: {
        recordId: `record-${index}`,
        family: "thread",
        schemaVersion: 1,
        createdAt: "2026-09-09T00:00:00.000Z",
        deploymentId: "old-planner",
        payload,
      },
    })),
  });

  const before = JSON.stringify(source);

  expect(legacyTripMessages(source, trip.id).map((message) => message.text)).toEqual([
    "Tahoe in fall",
    "Saved Tahoe",
    "A private hot tub is a must",
    "I will look for hot tubs",
  ]);
  expect(legacyTripMessages(source, "lisbon-trip").map((message) => message.text)).toEqual([
    "Lisbon please",
    "Only Lisbon",
  ]);
  expect(JSON.stringify(source)).toBe(before);
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

    const saved = Schema.decodeUnknownSync(SavedTrip)({
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

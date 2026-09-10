import { ThreadExport } from "@effect-agent/thread/ThreadStore";
import { Schema } from "effect";
import { AtomRegistry } from "effect/unstable/reactivity";
import { expect, it } from "vite-plus/test";

import { SavedTrip, Trip } from "../src/domain.ts";
import { legacyTripMessages } from "../src/server/conversation.ts";
import { draftAtom, newTripAtom, selectionAtom, selectTripAtom } from "../src/state.ts";

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

it("starts distinct new conversations and returns to the saved trip's conversation", () => {
  const registry = AtomRegistry.make();

  try {
    registry.set(draftAtom, "Draft from another trip");
    registry.set(newTripAtom, undefined);
    const first = registry.get(selectionAtom);

    expect(first.conversationId).toBeTruthy();
    expect(registry.get(draftAtom)).toBe("");
    registry.set(newTripAtom, undefined);
    expect(registry.get(selectionAtom).conversationId).not.toBe(first.conversationId);

    const saved = Schema.decodeUnknownSync(SavedTrip)({
      ...trip,
      conversationId: "saved-conversation",
    });

    registry.set(selectTripAtom, saved);
    expect(registry.get(selectionAtom)).toEqual({
      conversationId: "saved-conversation",
      tripId: trip.id,
    });
  } finally {
    registry.dispose();
  }
});

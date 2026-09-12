import { Schema } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";

import {
  PlannerSettings,
  PlannerSnapshot,
  SendMessageRequest,
  defaultPlannerSettings,
} from "../src/domain.ts";
import {
  changeSettingsAtom,
  draftAtom,
  pendingMessagesAtom,
  messagesAtom,
  plannerAtom,
  selectionAtom,
  sendMessageAtom,
  sessionAtom,
  settingsAtom,
  latestTypedInputAtom,
  spokenConversationAtom,
} from "../src/state.ts";

const Packet = Schema.Struct({
  id: Schema.Union([Schema.String, Schema.Number]),
  tag: Schema.String,
  payload: Schema.Unknown,
});

const snapshot = (
  conversationId = "lisbon",
  messages: PlannerSnapshot["messages"] = [],
  queuedMessages: NonNullable<PlannerSnapshot["queuedMessages"]> = [],
) =>
  Schema.decodeSync(PlannerSnapshot)({
    conversationId,
    messages,
    queuedMessages,
    trips: [],
    activity: [],
    pending: queuedMessages.length,
    pendingSubmissionIds: [],
    usage: { model: "test", inputTokens: null, outputTokens: null, estimatedCostMicrousd: null },
  });

const canonical = (request: SendMessageRequest): PlannerSnapshot["messages"][number] => ({
  id: `recorded:${request.requestId}`,
  requestId: request.requestId,
  role: "user",
  text: request.message,
  tripId: null,
});

const fetchMock = vi.fn<typeof fetch>();

const setup = () => {
  vi.useFakeTimers();
  vi.stubGlobal("location", new URL("https://planner.test"));
  let email = "danieljmerwe@gmail.com";

  const reads: Array<{ conversationId: string | null; succeed: (value: PlannerSnapshot) => void }> =
    [];

  const sends: Array<{ payload: SendMessageRequest; succeed: () => void; fail: () => void }> = [];
  const preferences: Array<{ succeed: (value?: PlannerSettings) => void }> = [];

  vi.stubGlobal(
    "fetch",
    fetchMock.mockImplementation(async (input, init) => {
      const request = new Request(
        input instanceof Request ? input : new URL(String(input), "https://planner.test"),
        init,
      );

      const packet = Schema.decodeSync(Schema.fromJsonString(Packet))(
        (await request.text()).trim(),
      );

      const response = (exit: unknown) =>
        new Response(`${JSON.stringify({ _tag: "Exit", requestId: packet.id, exit })}\n`, {
          headers: { "content-type": "application/ndjson" },
        });

      const success = (value: unknown) => response({ _tag: "Success", value });

      if (packet.tag === "SavePlannerSettings")
        return success(Schema.decodeUnknownSync(PlannerSettings)(packet.payload));

      return new Promise<Response>((resolve) => {
        if (packet.tag === "GetPlannerSettings")
          preferences.push({
            succeed: (value = defaultPlannerSettings) => resolve(success(value)),
          });
        else if (packet.tag === "GetPlanner") {
          const payload = Schema.decodeUnknownSync(
            Schema.Struct({ conversationId: Schema.NullOr(Schema.String) }),
          )(packet.payload);

          reads.push({
            conversationId: payload.conversationId,
            succeed: (value) => resolve(success(value)),
          });
        } else if (packet.tag === "SendMessage")
          sends.push({
            payload: Schema.decodeUnknownSync(SendMessageRequest)(packet.payload),
            succeed: () => resolve(success({ accepted: true })),
            fail: () =>
              resolve(
                response({
                  _tag: "Failure",
                  cause: [
                    {
                      _tag: "Fail",
                      error: { _tag: "PlannerError", code: "unavailable", message: "Try again" },
                    },
                  ],
                }),
              ),
          });
        else throw new Error(`Unexpected RPC ${packet.tag}`);
      });
    }),
  );

  const createDevice = () => {
    const registry = AtomRegistry.make({
      scheduleTask: (task) => {
        const timer = setTimeout(task, 0);

        return () => clearTimeout(timer);
      },
    });

    registry.set(selectionAtom, { conversationId: "lisbon", tripId: null });

    registry.set(sessionAtom, AsyncResult.success({ subjectId: email, displayName: email }));

    const unmounts = [
      registry.mount(draftAtom),
      registry.mount(plannerAtom),
      registry.mount(pendingMessagesAtom),
      registry.mount(messagesAtom),
      registry.mount(sendMessageAtom),
      registry.mount(settingsAtom),
      registry.mount(latestTypedInputAtom),
    ];

    return {
      registry,
      setEmail: (value: string) => {
        email = value;
        registry.set(sessionAtom, AsyncResult.success({ subjectId: email, displayName: email }));
      },
      close: () => {
        for (const unmount of unmounts) unmount();
        registry.dispose();
      },
    };
  };

  return { reads, sends, preferences, createDevice };
};

const flush = () => vi.advanceTimersByTimeAsync(1);

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("renders idle sends in the transcript immediately and keeps them there through admission", async () => {
  const fixture = setup();
  const device = fixture.createDevice();
  const { registry } = device;

  try {
    await flush();
    fixture.reads[0]!.succeed(snapshot());
    await flush();
    registry.set(draftAtom, "Plan Lisbon");
    registry.set(sendMessageAtom, undefined);
    // Voice steering uses the submitted draft even before admission or a snapshot refresh.
    expect(registry.get(latestTypedInputAtom)).toEqual({ revision: 1, text: "Plan Lisbon" });
    expect(registry.get(draftAtom)).toBe("");
    expect(registry.get(messagesAtom)).toMatchObject([
      { role: "user", text: "Plan Lisbon", delivery: "sending" },
    ]);
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
    await flush();
    expect(fixture.sends).toHaveLength(0);
    fixture.preferences[0]!.succeed();
    await flush();
    expect(fixture.sends).toHaveLength(1);
    expect(registry.get(messagesAtom)).toMatchObject([{ delivery: "sending" }]);
    fixture.sends[0]!.succeed();
    await flush();
    expect(registry.get(messagesAtom)).toMatchObject([{ delivery: "accepted" }]);
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
    const request = fixture.sends[0]!.payload;

    fixture.reads
      .at(-1)!
      .succeed(snapshot("lisbon", [], [{ requestId: request.requestId, text: request.message }]));
    await flush();
    expect(registry.get(messagesAtom)).toMatchObject([{ text: "Plan Lisbon" }]);
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
    await vi.advanceTimersByTimeAsync(2_010);
    fixture.reads.at(-1)!.succeed(snapshot("lisbon", [canonical(fixture.sends[0]!.payload)]));
    await flush();
    expect(registry.get(messagesAtom)).toEqual([canonical(request)]);
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
    registry.set(draftAtom, "Find a hotel");
    registry.set(sendMessageAtom, undefined);
    expect(registry.get(latestTypedInputAtom)).toEqual({ revision: 2, text: "Find a hotel" });
    expect(registry.get(messagesAtom)).toMatchObject([
      { text: "Plan Lisbon" },
      { text: "Find a hotel", delivery: "sending" },
    ]);
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
  } finally {
    device.close();
  }
});

it("distinguishes identical messages and never restores a canonical message after a lost acknowledgement", async () => {
  const fixture = setup();
  const device = fixture.createDevice();
  const { registry } = device;

  try {
    await flush();
    fixture.reads[0]!.succeed({ ...snapshot(), pending: 1, pendingSubmissionIds: ["active-run"] });
    fixture.preferences[0]!.succeed();
    await flush();
    registry.set(draftAtom, "More beaches");
    registry.set(sendMessageAtom, undefined);
    expect(registry.get(messagesAtom)).toEqual([]);
    expect(registry.get(pendingMessagesAtom)).toMatchObject([
      { text: "More beaches", status: "sending" },
    ]);
    await flush();
    const first = fixture.sends[0]!;

    first.succeed();
    await flush();
    fixture.reads.at(-1)!.succeed(snapshot());
    await flush();
    registry.set(draftAtom, "More beaches");
    registry.set(sendMessageAtom, undefined);
    await flush();
    const second = fixture.sends[1]!;

    expect(first.payload.requestId).not.toBe(second.payload.requestId);
    expect(registry.get(pendingMessagesAtom)).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(2_010);
    fixture.reads.at(-1)!.succeed(snapshot("lisbon", [canonical(second.payload)]));
    await flush();
    expect(registry.get(pendingMessagesAtom).map(({ id }) => id)).toEqual([
      first.payload.requestId,
    ]);
    second.fail();
    await flush();
    expect(registry.get(pendingMessagesAtom).map(({ id }) => id)).toEqual([
      first.payload.requestId,
    ]);
    await vi.advanceTimersByTimeAsync(2_010);
    fixture.reads
      .at(-1)!
      .succeed(snapshot("lisbon", [canonical(first.payload), canonical(second.payload)]));
    await flush();
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
  } finally {
    device.close();
  }
});

it("retries an idle message in place with its captured request and settings, preserving the next draft", async () => {
  const fixture = setup();
  const device = fixture.createDevice();
  const { registry } = device;

  try {
    await flush();
    fixture.reads[0]!.succeed(snapshot());
    fixture.preferences[0]!.succeed();
    await flush();
    registry.set(draftAtom, "Find a hotel");
    registry.set(sendMessageAtom, undefined);
    await flush();
    const first = fixture.sends[0]!;

    first.fail();
    await flush();
    expect(registry.get(messagesAtom)).toMatchObject([
      { text: "Find a hotel", delivery: "failed" },
    ]);
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
    registry.set(changeSettingsAtom, { kind: "model", value: "gpt-6-astra" });
    await flush();
    expect(registry.get(settingsAtom).model).toBe("gpt-6-astra");
    registry.set(draftAtom, "A separate next message");
    registry.set(sendMessageAtom, first.payload.requestId);
    await flush();
    expect(fixture.sends[1]!.payload).toEqual(first.payload);
    expect(registry.get(draftAtom)).toBe("A separate next message");
    expect(registry.get(messagesAtom)).toMatchObject([{ delivery: "sending" }]);
    fixture.sends[1]!.succeed();
    await flush();
    expect(registry.get(messagesAtom)).toMatchObject([{ delivery: "accepted" }]);
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
  } finally {
    device.close();
  }
});

it("isolates optimistic messages by conversation and verified account", async () => {
  const fixture = setup();
  const device = fixture.createDevice();
  const { registry } = device;

  try {
    await flush();
    fixture.reads[0]!.succeed(snapshot());
    fixture.preferences[0]!.succeed();
    await flush();
    registry.set(draftAtom, "Owner's Lisbon request");
    registry.set(sendMessageAtom, undefined);
    await flush();
    registry.set(selectionAtom, { conversationId: "kyoto", tripId: null });
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
    expect(registry.get(messagesAtom)).toEqual([]);
    await flush();
    fixture.reads.at(-1)!.succeed(snapshot("kyoto"));
    await flush();
    registry.set(selectionAtom, { conversationId: "lisbon", tripId: null });
    expect(registry.get(messagesAtom)).toMatchObject([{ text: "Owner's Lisbon request" }]);
    device.setEmail("guest@example.com");
    await flush();
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
    expect(registry.get(messagesAtom)).toEqual([]);
    fixture.sends[0]!.fail();
    await flush();
    expect(registry.get(pendingMessagesAtom)).toEqual([]);
    expect(registry.get(messagesAtom)).toEqual([]);
  } finally {
    device.close();
  }
});

it("restores queued messages into a fresh registry without a local outbox", async () => {
  const fixture = setup();
  const firstDevice = fixture.createDevice();

  await flush();
  firstDevice.close();
  const device = fixture.createDevice();

  try {
    await flush();
    fixture.reads
      .at(-1)!
      .succeed(
        snapshot("lisbon", [], [{ requestId: "persisted-request", text: "Add a beach day" }]),
      );
    await flush();
    expect(fixture.sends).toEqual([]);
    expect(device.registry.get(pendingMessagesAtom)).toEqual([
      { id: "persisted-request", text: "Add a beach day", status: "queued" },
    ]);
  } finally {
    device.close();
  }
});

it("carries the spoken exchange into a typed follow-up without displaying a transcript submission", async () => {
  const fixture = setup();
  const device = fixture.createDevice();
  const { registry } = device;

  try {
    await flush();
    fixture.reads[0]!.succeed(snapshot());
    await flush();
    registry.set(spokenConversationAtom, {
      subjectId: "danieljmerwe@gmail.com",
      conversationId: "lisbon",
      active: true,
      baseline: [],
      responses: [],
      messages: [
        { id: "speech-question", role: "assistant", text: "Starting from where?", after: null },
        { id: "speech-reply", role: "user", text: "The Bay Area", after: "speech-question" },
      ],
    });
    registry.set(draftAtom, "Three nights, please");
    registry.set(sendMessageAtom, undefined);
    await flush();
    fixture.preferences[0]!.succeed();
    await flush();
    expect(fixture.sends[0]?.payload).toMatchObject({
      message: "Three nights, please",
      voice: {
        input: false,
        messages: [
          { role: "assistant", text: "Starting from where?" },
          { role: "user", text: "The Bay Area" },
        ],
      },
    });
    expect(registry.get(messagesAtom).map(({ text }) => text)).toEqual([
      "Starting from where?",
      "The Bay Area",
      "Three nights, please",
    ]);
    fixture.sends[0]!.succeed();
    await flush();
  } finally {
    device.close();
  }
});

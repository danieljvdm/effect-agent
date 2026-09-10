import { it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { expect, expectTypeOf } from "vite-plus/test";

import {
  defaultPlannerSettings,
  PlannerError,
  type SendMessageRequest,
  type VoiceWork,
} from "../src/domain.ts";
import { emptyProgress } from "../src/server/progress.ts";
import {
  appendCaption,
  captionRows,
  delegationMessage,
  voiceUpdate,
  type VoiceRequest,
} from "../src/voice/delegation.ts";
import { LiveEvent, VoiceError } from "../src/voice/protocol.ts";
import type { Caption } from "../src/voice/protocol.ts";
import { runVoiceSession, type VoiceBackend } from "../src/voice/session.ts";

const envelope = {
  conversationId: "lisbon",
  selectedTripId: null,
  settings: defaultPlannerSettings,
};

const unavailable = new PlannerError({ code: "unavailable", message: "Lost response" });
const request = { ...envelope, message: "Plan Lisbon", requestId: "request-original" };

const observation = (
  requestId = request.requestId,
  state: VoiceWork["state"] = "pending",
): VoiceWork => ({
  requestId,
  state,
  receiptId: `receipt-${requestId}`,
  submissionId: `submission-${requestId}`,
  runId: "run-one",
  superseded: false,
  text: state === "completed" ? "Saved your Lisbon trip." : null,
});

const caption = (id: string, text: string, start = 0) =>
  Schema.decodeUnknownSync(LiveEvent)({
    type: "session.input_transcript.delta",
    event_id: id,
    delta: text,
    start_ms: start,
    end_ms: start + 100,
  });

const delegation = (id: string, offset = 1000) =>
  Schema.decodeUnknownSync(LiveEvent)({
    type: "session.delegation.created",
    event_id: `event-${id}`,
    offset_ms: offset,
    delegation: { id, target: "client" },
  });

const setup = Effect.fn("voiceTest.setup")(function* (retained: ReadonlyArray<VoiceRequest> = []) {
  const queue = yield* Queue.bounded<LiveEvent, VoiceError>(128);
  const stop = yield* Deferred.make<void>();
  const sent: Readonly<Record<string, unknown>>[] = [];
  const admitted: SendMessageRequest[] = [];
  let stored = retained;
  let finalized = false;
  let typed = 0;

  const backend: { -readonly [K in keyof VoiceBackend]: VoiceBackend[K] } = {
    submit: (input) =>
      Effect.sync(() => {
        admitted.push(input);
      }),
    read: (input) => Effect.succeed(observation(input.requestId)),
    progress: () => null,
    typedRevision: () => typed,
    typedContext: () => "Thursday instead of Friday",
    persist: (value) =>
      Effect.sync(() => {
        stored = value;
      }),
    view: () => {},
  };

  const run = runVoiceSession(
    {
      events: Stream.fromQueue(queue).pipe(
        Stream.ensuring(
          Effect.sync(() => {
            finalized = true;
          }),
        ),
      ),
      send: (event) =>
        Effect.sync(() => {
          sent.push(event);
        }),
      silence: Effect.void,
      resume: Effect.void,
    },
    backend,
    envelope,
    retained,
    stop,
  );

  const offer = (event: LiveEvent) => Queue.offer(queue, event);

  return {
    backend,
    run,
    sent,
    admitted,
    offer,
    stop,
    stored: () => stored,
    finalized: () => finalized,
    type: () => {
      typed++;
    },
    start: offer({ type: "session.started", session: { id: "live-new" } }),
  };
});

it.effect(
  "only delegates on metadata, freezes a typed envelope, and deduplicates delegation IDs",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const fiber = yield* test.run.pipe(Effect.forkChild);

      yield* test.start;
      yield* test.offer(caption("speech", "Plan a trip to Lisbon."));
      yield* TestClock.adjust("1 second");
      expect(test.admitted).toEqual([]);
      yield* test.offer(delegation("item-one"));
      yield* test.offer(delegation("item-one"));
      yield* TestClock.adjust("1 second");
      expect(test.admitted).toHaveLength(1);
      expect(test.admitted[0]?.conversationId).toBe("lisbon");
      expect(test.admitted[0]?.message).toContain("Plan a trip to Lisbon.");
      expect(test.stored()[0]?.request).toEqual(test.admitted[0]);
      expect(test.stored()[0]?.receipt?.receiptId).toBeTruthy();
      yield* Fiber.interrupt(fiber);
      expect(test.finalized()).toBe(true);
    }),
);

it.effect("reconnect reconciles uncertain acceptance without submitting accepted work again", () =>
  Effect.gen(function* () {
    const retained: VoiceRequest = {
      request,
      delegationId: "old-item",
      sessionId: "old-session",
      offset: 1000,
      status: "uncertain",
      receipt: null,
    };

    const test = yield* setup([retained]);
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* TestClock.adjust("1 second");
    expect(test.admitted).toEqual([]);
    expect(test.stored()[0]?.receipt).toEqual(observation());
    yield* Fiber.interrupt(fiber);
    expect(test.stored()[0]?.request).toEqual(request);
  }),
);

it.effect(
  "retries only an authoritative missing admission, keeping original request and settings",
  () =>
    Effect.gen(function* () {
      const test = yield* setup([
        {
          request,
          delegationId: "old-item",
          sessionId: "old-session",
          offset: 1000,
          status: "uncertain",
          receipt: null,
        },
      ]);

      let reads = 0;

      test.backend.read = (input) =>
        Effect.sync(() => {
          reads++;

          return reads === 1
            ? { ...observation(input.requestId), state: "missing" as const, receiptId: null }
            : observation(input.requestId);
        });
      const fiber = yield* test.run.pipe(Effect.forkChild);

      yield* test.start;
      yield* TestClock.adjust("1 second");
      expect(test.admitted).toEqual([request]);
      yield* Fiber.interrupt(fiber);
    }),
);

it.effect("does not retry when reconciliation fails and cleans up its observer", () =>
  Effect.gen(function* () {
    const test = yield* setup([
      {
        request,
        delegationId: "old-item",
        sessionId: "old-session",
        offset: 1000,
        status: "uncertain",
        receipt: null,
      },
    ]);

    test.backend.read = () => Effect.fail(unavailable);
    const fiber = yield* test.run.pipe(Effect.exit, Effect.forkChild);

    yield* test.start;
    const exit = yield* Fiber.join(fiber);

    expect(Exit.isFailure(exit)).toBe(true);
    expect(test.admitted).toEqual([]);
    expect(test.finalized()).toBe(true);
  }),
);

it.effect("a spoken correction fences an in-flight result without cancelling accepted work", () =>
  Effect.gen(function* () {
    const retained: VoiceRequest = {
      request,
      delegationId: "item",
      sessionId: "live-new",
      offset: 100,
      status: "accepted",
      receipt: observation(),
    };

    const test = yield* setup([retained]);
    const entered = yield* Deferred.make<void>();
    const result = yield* Deferred.make<VoiceWork>();

    test.backend.read = () =>
      Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(result)));
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* Deferred.await(entered);
    yield* test.offer(caption("correction", "Actually Thursday", 500));
    yield* Effect.yieldNow;
    yield* Deferred.succeed(result, observation(request.requestId, "completed"));
    yield* TestClock.adjust("1 second");
    expect(test.sent.filter((event) => event.type === "session.commentary.append")).toEqual([]);
    expect(test.admitted).toEqual([]);
    yield* Fiber.interrupt(fiber);
  }),
);

it.effect(
  "sends only selected public results, correlates acceptance, and times out unacknowledged appends",
  () =>
    Effect.gen(function* () {
      const test = yield* setup([
        {
          request,
          delegationId: "item",
          sessionId: "live-new",
          offset: 100,
          status: "accepted",
          receipt: observation(),
        },
      ]);

      test.backend.read = () => Effect.succeed(observation(request.requestId, "completed"));
      const fiber = yield* test.run.pipe(Effect.exit, Effect.forkChild);

      yield* test.start;
      yield* TestClock.adjust("1 second");
      expect(test.sent[0]).toMatchObject({
        type: "session.commentary.append",
        delegation_id: "item",
        content: "Saved your Lisbon trip.",
      });
      yield* TestClock.adjust("16 seconds");
      expect(Exit.isFailure(yield* Fiber.join(fiber))).toBe(true);
      expect(test.finalized()).toBe(true);
      expect(test.admitted).toEqual([]);
    }),
);

it.effect(
  "typed steering waits for the previous append acknowledgment without losing its correlation",
  () =>
    Effect.gen(function* () {
      const test = yield* setup([
        {
          request,
          delegationId: "item",
          sessionId: "live-new",
          offset: 100,
          status: "accepted",
          receipt: observation(),
        },
      ]);

      test.backend.read = () => Effect.succeed(observation(request.requestId, "completed"));
      const fiber = yield* test.run.pipe(Effect.forkChild);

      yield* test.start;
      yield* TestClock.adjust("1 second");
      expect(test.sent).toHaveLength(1);
      test.type();
      yield* TestClock.adjust("1 second");
      expect(test.sent).toHaveLength(1);
      yield* test.offer(
        Schema.decodeUnknownSync(LiveEvent)({
          type: "session.commentary.appended",
          client_event_id: test.sent[0]?.event_id,
        }),
      );
      yield* TestClock.adjust("1 second");
      expect(test.sent).toHaveLength(2);
      expect(test.sent[1]).toMatchObject({ type: "session.thinking.append", delegation_id: null });
      expect(test.sent[1]?.content).toContain("Thursday instead of Friday");
      expect(test.admitted).toEqual([]);
      yield* Fiber.interrupt(fiber);
    }),
);

it.effect("storage failures before and after admission preserve the frozen retry identity", () =>
  Effect.gen(function* () {
    for (const afterAdmission of [false, true]) {
      const test = yield* setup();
      const persist = test.backend.persist;

      test.backend.persist = (requests) =>
        !afterAdmission || test.admitted.length > 0
          ? Effect.fail(new VoiceError({ message: "Storage unavailable" }))
          : persist(requests);
      const fiber = yield* test.run.pipe(Effect.exit, Effect.forkChild);

      yield* test.start;
      yield* test.offer(caption("speech", "Save a Lisbon trip"));
      yield* test.offer(delegation("item"));
      yield* TestClock.adjust("1 second");
      expect(Exit.isFailure(yield* Fiber.join(fiber))).toBe(true);
      expect(test.admitted).toHaveLength(afterAdmission ? 1 : 0);
      if (afterAdmission) {
        expect(test.stored()[0]?.status).toBe("prepared");
        expect(test.stored()[0]?.request).toEqual(test.admitted[0]);
      }
      expect(test.finalized()).toBe(true);
    }
  }),
);

it.effect(
  "close times out without losing a request; startup failure and defects release observation",
  () =>
    Effect.gen(function* () {
      const test = yield* setup([
        {
          request,
          delegationId: "item",
          sessionId: "live-new",
          offset: 100,
          status: "accepted",
          receipt: observation(),
        },
      ]);

      const fiber = yield* test.run.pipe(Effect.forkChild);

      yield* test.start;
      yield* Deferred.succeed(test.stop, undefined);
      yield* TestClock.adjust("6 seconds");
      yield* Fiber.join(fiber);
      expect(test.sent).toContainEqual({ type: "session.close" });
      expect(test.stored()[0]?.request).toEqual(request);
      expect(test.finalized()).toBe(true);
      const startup = yield* setup();
      const stalled = yield* startup.run.pipe(Effect.exit, Effect.forkChild);

      yield* TestClock.adjust("16 seconds");
      expect(Exit.isFailure(yield* Fiber.join(stalled))).toBe(true);
      expect(startup.finalized()).toBe(true);

      const defect = yield* setup([
        {
          request,
          delegationId: "item",
          sessionId: "old",
          offset: 100,
          status: "uncertain",
          receipt: null,
        },
      ]);

      defect.backend.read = () => Effect.die("transport defect");
      const died = yield* defect.run.pipe(Effect.exit, Effect.forkChild);

      yield* defect.start;
      const exit = yield* Fiber.join(died);

      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
      expect(defect.finalized()).toBe(true);
    }),
);

it("selects public output by receipt and attempt and leaves structured previews provisional", () => {
  const work = observation();

  expect(voiceUpdate({ ...work, superseded: true }, null)).toBeNull();
  expect(
    voiceUpdate(work, { ...emptyProgress, submissionId: "foreign", text: "PRIVATE" }),
  ).toBeNull();
  expect(
    voiceUpdate(work, {
      ...emptyProgress,
      submissionId: work.submissionId,
      attemptId: "attempt-two",
      revision: 9,
      text: "A draft",
    }),
  ).toMatchObject({
    kind: "progress",
    key: "attempt-two:9",
    text: "Provisional planner response (work is still running): A draft",
  });
  expect(
    voiceUpdate({ ...work, state: "failed", text: "PRIVATE failure details" }, null)?.text,
  ).not.toContain("PRIVATE");
  const event = caption("caption", "Hello");

  if (event.type === "session.input_transcript.delta")
    expect(appendCaption([event], event)).toEqual([event]);
  expectTypeOf<Effect.Error<ReturnType<typeof runVoiceSession>>>().toEqualTypeOf<VoiceError>();
  expectTypeOf<Effect.Services<ReturnType<typeof runVoiceSession>>>().toEqualTypeOf<never>();
});

it("joins transcript fragments into readable speaker rows without merging overlapping speakers or later input", () => {
  const fragments: Caption[] = [
    {
      type: "session.input_transcript.delta",
      event_id: "one",
      start_ms: 0,
      end_ms: 200,
      delta: "Please save",
    },
    {
      type: "session.input_transcript.delta",
      event_id: "two",
      start_ms: 200,
      end_ms: 400,
      delta: " Lisbon.",
    },
    {
      type: "session.output_transcript.delta",
      event_id: "three",
      start_ms: 300,
      end_ms: 500,
      delta: "Three days?",
    },
    {
      type: "session.input_transcript.delta",
      event_id: "four",
      start_ms: 450,
      end_ms: 650,
      delta: "Yes.",
    },
    {
      type: "session.input_transcript.delta",
      event_id: "five",
      start_ms: 800,
      end_ms: 1000,
      delta: " Actually four.",
    },
  ];

  expect(captionRows(fragments)).toEqual([
    { id: "one", speaker: "You", text: "Please save Lisbon." },
    { id: "three", speaker: "AI voice", text: "Three days?" },
    { id: "four", speaker: "You", text: "Yes. Actually four." },
  ]);
  expect(delegationMessage(fragments, 650)).toBe(
    "Voice conversation (automatic transcript):\n\nYou: Please save Lisbon.\n\nAI voice: Three days?\n\nYou: Yes.",
  );
  expect(delegationMessage([fragments[2]!], 650)).toBeNull();
  expect(delegationMessage([{ ...fragments[0]!, delta: "x".repeat(4000) }], 650)).toBeNull();
});

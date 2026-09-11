import { it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { expect, expectTypeOf } from "vite-plus/test";

import {
  defaultPlannerSettings,
  PlannerError,
  type SendMessageRequest,
  type VoiceWork,
  type ResearchScoutActivity,
  type TripApp,
} from "../src/domain.ts";
import type { planner, previousTextPlanner } from "../src/server/planner.ts";
import { emptyProgress } from "../src/server/progress.ts";
import {
  appendCaption,
  captionRows,
  delegationMessage,
  voiceUpdate,
  type VoiceRequest,
} from "../src/voice/delegation.ts";
import { contextParts, LiveEvent, VoiceError } from "../src/voice/protocol.ts";
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
    background: () => null,
    typedRevision: () => typed,
    typedContext: () => "Thursday instead of Friday",
    typedRequest: () =>
      typed
        ? { ...request, requestId: "typed-request", message: "Thursday instead of Friday" }
        : null,
    speech: () => [],
    caption: () => {},
    context: () => "",
    answers: () => [],
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
    yield* test.offer(delegation("new-correction", 900));
    yield* Effect.yieldNow;
    yield* Deferred.succeed(result, observation(request.requestId, "completed"));
    yield* TestClock.adjust("1 second");
    expect(test.sent.filter((event) => event.type === "session.commentary.append")).toEqual([]);
    expect(test.admitted).toHaveLength(1);
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
      expect(test.sent[1]).toMatchObject({
        type: "session.instructions.append",
        delegation_id: null,
      });
      yield* test.offer(
        Schema.decodeUnknownSync(LiveEvent)({
          type: "session.instructions.appended",
          client_event_id: test.sent[1]?.event_id,
        }),
      );
      yield* TestClock.adjust("1 second");
      expect(test.sent[2]).toMatchObject({ type: "session.thinking.append", delegation_id: null });
      expect(test.sent[2]?.content).toContain("Thursday instead of Friday");
      yield* test.offer(
        Schema.decodeUnknownSync(LiveEvent)({
          type: "session.thinking.appended",
          client_event_id: test.sent[2]?.event_id,
        }),
      );
      yield* TestClock.adjust("1 second");
      expect(test.stored().at(-1)?.request.requestId).toBe("typed-request");
      expect(test.sent[3]).toMatchObject({
        type: "session.commentary.append",
        delegation_id: null,
        content: "Saved your Lisbon trip.",
      });
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
  expectTypeOf<Effect.Error<ReturnType<typeof planner.instructions>>>().toEqualTypeOf<
    Effect.Error<ReturnType<typeof previousTextPlanner.instructions>>
  >();
  expectTypeOf<Effect.Services<ReturnType<typeof planner.instructions>>>().toEqualTypeOf<
    Effect.Services<ReturnType<typeof previousTextPlanner.instructions>>
  >();
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
  expect(delegationMessage(fragments, 650)).toBe("Please save Lisbon.\nYes.");
  expect(delegationMessage([fragments[2]!], 650)).toBeNull();
  expect(delegationMessage([{ ...fragments[0]!, delta: "x".repeat(4000) }], 650)).toBeNull();
});

it.effect(
  "brief speech delays a result without abandoning it, and later research continues the same answer",
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

      let answers: ReadonlyArray<{ id: string; text: string }> = [];

      test.backend.answers = () => answers;
      test.backend.read = () => Effect.succeed(observation(request.requestId, "completed"));
      const fiber = yield* test.run.pipe(Effect.forkChild);

      yield* test.offer(caption("acknowledgment", "Mm", 500));
      yield* test.start;
      yield* TestClock.adjust("1 second");
      expect(test.sent).toHaveLength(0);
      yield* TestClock.adjust("1 second");
      expect(test.sent[0]).toMatchObject({
        type: "session.commentary.append",
        content: "Saved your Lisbon trip.",
      });
      yield* test.offer(
        Schema.decodeUnknownSync(LiveEvent)({
          type: "session.commentary.appended",
          client_event_id: test.sent[0]?.event_id,
        }),
      );
      answers = [
        { id: "finished", text: "Saved your Lisbon trip." },
        { id: "research", text: "The two quieter options are now on screen." },
      ];
      yield* TestClock.adjust("1 second");
      expect(test.sent[1]).toMatchObject({
        type: "session.commentary.append",
        content: "The two quieter options are now on screen.",
      });
      yield* test.offer(
        Schema.decodeUnknownSync(LiveEvent)({
          type: "session.commentary.appended",
          client_event_id: test.sent[1]?.event_id,
        }),
      );
      yield* TestClock.adjust("2 seconds");
      expect(test.sent).toHaveLength(2);
      expect(test.admitted).toEqual([]);
      yield* Fiber.interrupt(fiber);
    }),
);

const researchRequest: VoiceRequest = {
  request,
  delegationId: "item",
  sessionId: "live-new",
  offset: 100,
  status: "accepted",
  receipt: observation(),
};

const activeScout: ResearchScoutActivity = {
  id: "scout-one",
  title: "Private hot tub cottages",
  task: "PRIVATE task body",
  state: "active",
  progress: { ...emptyProgress, text: "PRIVATE provisional findings" },
  activity: [{ id: "error", kind: "failure", text: "PRIVATE diagnostic" }],
};

const acknowledge = (test: Effect.Success<ReturnType<typeof setup>>) =>
  test.offer(
    Schema.decodeUnknownSync(LiveEvent)({
      type: "session.thinking.appended",
      client_event_id: test.sent.at(-1)?.event_id,
    }),
  );

it.effect("activity and elapsed time do not produce waiting announcements", () =>
  Effect.gen(function* () {
    const test = yield* setup([researchRequest]);

    test.backend.background = () => ({ scouts: [activeScout] });
    test.backend.progress = () => ({
      ...emptyProgress,
      submissionId: observation().submissionId,
      attemptId: "attempt",
      tools: [{ id: "read", label: "Reading a rental listing", state: "running" }],
    });
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* TestClock.adjust("60 seconds");
    expect(test.sent).toEqual([]);
    expect(test.admitted).toEqual([]);
    yield* Fiber.interrupt(fiber);
    expect(test.finalized()).toBe(true);
  }),
);

it.effect("speaks each settled finding once after receiving its complete summary and caveats", () =>
  Effect.gen(function* () {
    const test = yield* setup([researchRequest]);
    let scouts = [activeScout];

    test.backend.background = () => ({ scouts });
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* TestClock.adjust("1 second");

    const summary =
      "Harbor Cottage has a private hot tub and a separate writing room. " +
      "The beach is a short walk away. ".repeat(20) +
      "Caveat: holiday availability and the total price are unverified.";

    scouts = [{ ...activeScout, state: "idle", finding: { id: "settled-one", text: summary } }];
    // A changing parent preview must not starve a complete research note.
    let revision = 0;

    test.backend.progress = () => ({
      ...emptyProgress,
      submissionId: observation().submissionId,
      attemptId: "attempt",
      revision: revision++,
      text: "Public parent preview",
    });
    for (let step = 0; step < 12; step++) {
      yield* TestClock.adjust("500 millis");
      if (test.sent.length) yield* acknowledge(test);
    }

    const quiet = test.sent.filter(
      (event) =>
        event.type === "session.thinking.append" &&
        String(event.content).startsWith("Research note"),
    );

    const spoken = test.sent.filter((event) => event.type === "session.commentary.append");

    expect(
      quiet
        .map((event) => String(event.content).replace(/^Research note \d+, part \d+\/\d+: /, ""))
        .join(""),
    ).toBe(summary);
    expect(spoken).toHaveLength(1);
    expect(spoken[0]?.content).toContain("Harbor Cottage");
    expect(spoken[0]?.content).toContain("preserve all caveats");
    expect(test.sent.indexOf(spoken[0]!)).toBeGreaterThan(test.sent.indexOf(quiet.at(-1)!));
    expect(JSON.stringify(test.sent)).not.toContain("PRIVATE");
    expect(
      test.sent.every((event) => new TextEncoder().encode(String(event.content)).length <= 420),
    ).toBe(true);
    test.backend.progress = () => null;
    yield* TestClock.adjust("60 seconds");
    expect(test.sent.filter((event) => event.type === "session.commentary.append")).toHaveLength(1);
    expect(test.admitted).toEqual([]);
    yield* Fiber.interrupt(fiber);
  }),
);

it.effect("later research continues after the initial reply and yields to ongoing speech", () =>
  Effect.gen(function* () {
    const test = yield* setup([researchRequest]);
    let scouts = [activeScout];

    test.backend.background = () => ({ scouts });
    test.backend.read = () => Effect.succeed(observation(request.requestId, "completed"));
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* TestClock.adjust("1 second");
    expect(test.sent[0]?.content).toBe("Saved your Lisbon trip.");
    yield* acknowledge(test);
    scouts = [
      {
        ...activeScout,
        state: "idle",
        finding: { id: "later", text: "Harbor Cottage has a private hot tub." },
      },
    ];
    yield* TestClock.adjust("500 millis");
    expect(test.sent.at(-1)?.type).toBe("session.thinking.append");
    yield* acknowledge(test);
    yield* test.offer(
      Schema.decodeUnknownSync(LiveEvent)({
        ...caption("speaking", "And good food"),
        type: "session.output_transcript.delta",
      }),
    );
    yield* TestClock.adjust("1 second");
    expect(test.sent).toHaveLength(2);
    yield* TestClock.adjust("500 millis");
    expect(test.sent.at(-1)).toMatchObject({
      type: "session.commentary.append",
      delegation_id: "item",
    });
    expect(test.sent.at(-1)?.content).toContain("private hot tub");
    yield* Fiber.interrupt(fiber);
  }),
);

it.effect("a typed correction discards a partially delivered research note", () =>
  Effect.gen(function* () {
    const test = yield* setup([researchRequest]);
    let scouts = [activeScout];

    test.backend.background = () => ({ scouts });
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* TestClock.adjust("1 second");
    scouts = [
      {
        ...activeScout,
        state: "idle",
        finding: { id: "old", text: "Old Friday options. ".repeat(50) },
      },
    ];
    yield* TestClock.adjust("500 millis");
    expect(test.sent[0]?.type).toBe("session.thinking.append");
    test.type();
    yield* acknowledge(test);
    for (let step = 0; step < 5; step++) {
      yield* TestClock.adjust("500 millis");
      yield* acknowledge(test);
    }
    expect(test.sent.some((event) => event.type === "session.instructions.append")).toBe(true);
    expect(test.sent.filter((event) => event.type === "session.commentary.append")).toEqual([]);
    scouts = [
      {
        ...activeScout,
        state: "idle",
        finding: { id: "new", text: "Harbor Cottage is available Thursday." },
      },
    ];
    yield* TestClock.adjust("500 millis");
    yield* acknowledge(test);
    yield* TestClock.adjust("500 millis");
    expect(test.sent.at(-1)?.content).toContain("available Thursday");
    expect(test.sent.at(-1)?.type).toBe("session.commentary.append");
    yield* Fiber.interrupt(fiber);
  }),
);

it.effect("superseded work and failed scouts cannot supply spoken findings", () =>
  Effect.gen(function* () {
    const test = yield* setup([researchRequest]);
    let scouts = [activeScout];

    test.backend.background = () => ({ scouts });
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* TestClock.adjust("1 second");
    scouts = [{ ...activeScout, state: "failed", finding: { id: "old", text: "Stale option" } }];
    yield* TestClock.adjust("5 seconds");
    expect(test.sent).toEqual([]);
    scouts = scouts.map((scout) => ({ ...scout, state: "idle" }));
    test.backend.read = () => Effect.succeed({ ...observation(), superseded: true });
    yield* TestClock.adjust("5 seconds");
    expect(test.sent).toEqual([]);
    yield* Fiber.interrupt(fiber);
  }),
);

it.effect("a planner reply cannot consume or discard a different scout's pending finding", () =>
  Effect.gen(function* () {
    const test = yield* setup([researchRequest]);
    let scouts = [activeScout];

    test.backend.background = () => ({ scouts });
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* TestClock.adjust("1 second");
    scouts = [
      {
        ...activeScout,
        state: "idle",
        finding: {
          id: "race-finding",
          text: "The night race offers a 50 km route. Entry availability remains unverified.",
        },
      },
    ];
    yield* TestClock.adjust("500 millis");
    yield* acknowledge(test);
    test.backend.read = () =>
      Effect.succeed({
        ...observation(request.requestId, "completed"),
        text: "Your website layout is saved.",
      });
    for (let step = 0; step < 8; step++) {
      yield* TestClock.adjust("500 millis");
      yield* acknowledge(test);
    }
    const spoken = test.sent.filter((event) => event.type === "session.commentary.append");

    expect(spoken.some((event) => String(event.content).includes("website layout"))).toBe(true);
    expect(spoken.filter((event) => String(event.content).includes("50 km route"))).toHaveLength(1);
    yield* Fiber.interrupt(fiber);
  }),
);

it.effect(
  "website readiness reaches voice after the editor and build finish, without another planner request",
  () =>
    Effect.gen(function* () {
      const test = yield* setup();

      let app: TripApp = {
        id: "app-one",
        tripId: "trip-one",
        revision: 1,
        url: "https://example.com/trip",
        repoName: "trip-app",
        sourceCommit: "a".repeat(40),
        activeCommit: null,
        pendingCommit: "a".repeat(40),
        status: "building",
        error: null,
        updatedAt: "2026-09-10T23:16:00Z",
        versions: [],
      };

      let editing = true;

      test.backend.background = () => ({
        app,
        editor: { ...activeScout, state: editing ? "active" : "idle" },
      });
      const fiber = yield* test.run.pipe(Effect.forkChild);

      yield* test.start;
      yield* TestClock.adjust("1 second");
      yield* acknowledge(test);
      app = { ...app, status: "ready", activeCommit: app.sourceCommit, pendingCommit: null };
      yield* TestClock.adjust("2 seconds");
      expect(test.sent.filter((event) => event.type === "session.commentary.append")).toEqual([]);
      editing = false;
      for (let step = 0; step < 8; step++) {
        yield* TestClock.adjust("500 millis");
        yield* acknowledge(test);
      }
      const spoken = test.sent.filter((event) => event.type === "session.commentary.append");

      expect(spoken).toHaveLength(1);
      expect(spoken[0]?.content).toContain("ready to open");
      expect(spoken[0]?.content).toContain(app.url);
      expect(test.admitted).toEqual([]);
      yield* Fiber.interrupt(fiber);
    }),
);

it("splits complete Unicode research summaries without losing caveats or exceeding append limits", () => {
  const summary = "海辺の宿 🏖️ ".repeat(100) + "Price unconfirmed.";
  const parts = contextParts(summary);

  expect(parts.join("")).toBe(summary);
  expect(parts.every((part) => new TextEncoder().encode(part).length <= 330)).toBe(true);
  expect(contextParts("")).toEqual([]);
});

it.effect(
  "delivers both research answers when they arrive together instead of consuming only the newest",
  () =>
    Effect.gen(function* () {
      const test = yield* setup([researchRequest]);

      test.backend.read = () => Effect.succeed(observation(request.requestId, "completed"));
      let answers: ReadonlyArray<{ id: string; text: string }> = [];

      test.backend.answers = () => answers;
      const fiber = yield* test.run.pipe(Effect.forkChild);

      yield* test.start;
      yield* TestClock.adjust("2 seconds");
      yield* acknowledge(test);
      answers = [
        { id: "race", text: "The race has a verified 50 km route." },
        { id: "food", text: "The forest restaurant opens on Fridays." },
      ];
      for (let step = 0; step < 8; step++) {
        yield* TestClock.adjust("500 millis");
        yield* acknowledge(test);
      }

      const spoken = test.sent
        .filter((event) => event.type === "session.commentary.append")
        .map((event) => event.content);

      expect(spoken).toEqual([
        "Saved your Lisbon trip.",
        "The race has a verified 50 km route.",
        "The forest restaurant opens on Fridays.",
      ]);
      yield* Fiber.interrupt(fiber);
    }),
);

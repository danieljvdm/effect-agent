import { it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vite-plus/test";

import {
  defaultPlannerSettings,
  PlannerError,
  type SendMessageRequest,
  type VoiceWork,
} from "../src/domain.ts";
import { type VoiceRequest } from "../src/voice/delegation.ts";
import { LiveEvent, VoiceError } from "../src/voice/protocol.ts";
import { runVoiceSession, VoiceBackend, VoiceConnection } from "../src/voice/session.ts";

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
  receiptId: state === "missing" ? null : `receipt-${requestId}`,
  submissionId: state === "missing" ? null : `submission-${requestId}`,
  runId: state === "missing" ? null : "run-one",
  superseded: false,
  text: state === "completed" ? "Saved your Lisbon trip." : null,
});

const caption = (id: string, text: string, start = 0) =>
  Schema.decodeSync(LiveEvent)({
    type: "session.input_transcript.delta",
    event_id: id,
    delta: text,
    start_ms: start,
    end_ms: start + 100,
  });

const delegation = (id: string, offset = 1000) =>
  Schema.decodeSync(LiveEvent)({
    type: "session.delegation.created",
    event_id: `event-${id}`,
    offset_ms: offset,
    delegation: { id, target: "client" },
  });

const setup = Effect.fn("voiceTest.setup")(function* (retained: ReadonlyArray<VoiceRequest> = []) {
  const queue = yield* Queue.bounded<LiveEvent, VoiceError>(128);
  const stop = yield* Deferred.make<void>();
  const admitted: SendMessageRequest[] = [];
  let stored = retained;

  const backend: { -readonly [K in keyof VoiceBackend["Service"]]: VoiceBackend["Service"][K] } = {
    submit: (input) =>
      Effect.sync(() => {
        admitted.push(input);
      }),
    read: (input) => Effect.succeed(observation(input.requestId)),
    progress: () => null,
    background: () => null,
    typedRevision: () => 0,
    typedContext: () => "Thursday instead of Friday",
    typedRequest: () => null,
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

  const run = runVoiceSession(envelope, retained, stop).pipe(
    Effect.provideService(VoiceConnection, {
      events: Stream.fromQueue(queue),
      send: () => Effect.void,
      silence: Effect.void,
      resume: Effect.void,
    }),
    Effect.provideService(VoiceBackend, backend),
  );

  const offer = (event: LiveEvent) => Queue.offer(queue, event);

  return {
    backend,
    run,
    admitted,
    offer,
    stop,
    stored: () => stored,
    start: offer({ type: "session.started", session: { id: "live-new" } }),
  };
});

it.effect("reconciles retained identities and retries only missing work with frozen requests", () =>
  Effect.gen(function* () {
    const older = ["prepared", "uncertain", "uncertain"] as const;

    const retained: VoiceRequest[] = older.map((status, index) => ({
      request: { ...request, requestId: `older-${index}`, message: `Frozen request ${index}` },
      delegationId: `old-item-${index}`,
      sessionId: "old-session",
      offset: index * 1000,
      status,
      receipt: null,
    }));

    retained.push({
      request,
      delegationId: "latest-item",
      sessionId: "old-session",
      offset: 3000,
      status: "settled",
      receipt: observation(request.requestId, "completed"),
    });
    const test = yield* setup(retained);

    test.backend.read = (input) =>
      Effect.sync(() => {
        const known =
          input.requestId === request.requestId ||
          input.requestId === "older-2" ||
          test.admitted.some((item) => item.requestId === input.requestId);

        return known
          ? {
              ...observation(input.requestId, "completed"),
              text:
                input.requestId === request.requestId ? "Saved your Lisbon trip." : "Older result",
            }
          : observation(input.requestId, "missing");
      });
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* TestClock.adjust("1 second");
    expect(test.admitted).toEqual(retained.slice(0, 2).map((item) => item.request));
    expect(test.stored().map((item) => item.request.requestId)).toEqual(
      retained.map((item) => item.request.requestId),
    );
    yield* Fiber.interrupt(fiber);
  }),
);

it.effect("does not retry missing work when a later retained lookup times out", () =>
  Effect.gen(function* () {
    const retained: VoiceRequest[] = ["missing", "unavailable"].map((requestId) => ({
      request: { ...request, requestId },
      delegationId: requestId,
      sessionId: "old-session",
      offset: 1000,
      status: "uncertain",
      receipt: null,
    }));

    const test = yield* setup(retained);
    const entered = yield* Deferred.make<void>();

    test.backend.read = (input) =>
      input.requestId === "missing"
        ? Effect.succeed(observation(input.requestId, "missing"))
        : Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never));
    const fiber = yield* test.run.pipe(Effect.exit, Effect.forkChild);

    yield* test.start;
    yield* Deferred.await(entered);
    {
      yield* TestClock.adjust("11 seconds");
      expect(Exit.isFailure(yield* Fiber.join(fiber))).toBe(true);
    }
    expect(test.admitted).toEqual([]);
    expect(test.stored().map((item) => item.request)).toEqual(retained.map((item) => item.request));
  }),
);

it.effect("keeps reconciling an uncertain request after a newer spoken request", () =>
  Effect.gen(function* () {
    const test = yield* setup();
    const submit = test.backend.submit;
    let visible = false;

    test.backend.submit = (input) =>
      submit(input).pipe(
        Effect.andThen(
          Effect.suspend(() =>
            test.admitted.length === 1 ? Effect.fail(unavailable) : Effect.void,
          ),
        ),
      );
    test.backend.read = (input) =>
      Effect.sync(() =>
        observation(
          input.requestId,
          input.requestId === test.admitted[0]?.requestId && !visible ? "missing" : "pending",
        ),
      );
    const fiber = yield* test.run.pipe(Effect.forkChild);

    yield* test.start;
    yield* test.offer(caption("first", "Find a hotel"));
    yield* test.offer(delegation("first-request"));
    yield* TestClock.adjust("1 second");
    expect(test.stored()[0]?.status).toBe("uncertain");
    {
      yield* test.offer(caption("second", "And flights", 1100));
      yield* test.offer(delegation("second-request", 2000));
    }
    yield* TestClock.adjust("1 second");
    expect(test.stored()).toHaveLength(2);
    expect(test.stored()[0]?.status).toBe("uncertain");
    const latestId = test.stored().at(-1)?.request.requestId;

    visible = true;
    yield* TestClock.adjust("1 second");
    expect(test.stored()[0]).toMatchObject({
      request: test.admitted[0],
      status: "accepted",
      receipt: observation(test.admitted[0]?.requestId),
    });
    expect(test.stored().at(-1)?.request.requestId).toBe(latestId);
    expect(test.admitted).toHaveLength(2);
    yield* Fiber.interrupt(fiber);
  }),
);

for (const canEvict of [true, false])
  it.effect(
    `preserves unresolved identities when the retry cache ${canEvict ? "evicts known work" : "is full"}`,
    () =>
      Effect.gen(function* () {
        const retained: VoiceRequest[] = Array.from({ length: 16 }, (_, index) => ({
          request: { ...request, requestId: `retained-${index}` },
          delegationId: `old-item-${index}`,
          sessionId: "old-session",
          offset: index * 1000,
          status: canEvict && index > 0 ? "accepted" : "uncertain",
          receipt: null,
        }));

        const test = yield* setup(retained);

        test.backend.submit = () => Effect.fail(unavailable);
        test.backend.read = (input) => Effect.succeed(observation(input.requestId, "missing"));
        const fiber = yield* test.run.pipe(Effect.exit, Effect.forkChild);

        yield* test.start;
        yield* TestClock.adjust("1 second");
        const beforeNewRequest = test.stored();

        yield* test.offer(caption("new", "Find a hotel"));
        yield* test.offer(delegation("new-request"));
        yield* TestClock.adjust("1 second");
        expect(test.stored()).toHaveLength(16);
        expect(test.stored()[0]).toMatchObject({
          request: retained[0]?.request,
          status: "uncertain",
        });
        if (canEvict) {
          expect(test.stored().some((item) => item.request.requestId === "retained-1")).toBe(false);
          expect(test.stored().at(-1)?.request.message).toBe("Find a hotel");
          yield* Fiber.interrupt(fiber);
        } else {
          expect(Exit.isFailure(yield* Fiber.join(fiber))).toBe(true);
          expect(test.stored()).toEqual(beforeNewRequest);
        }
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
    }
  }),
);

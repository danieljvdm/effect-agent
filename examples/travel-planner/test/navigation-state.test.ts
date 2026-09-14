import { Schema } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { AccountError } from "../src/auth/account.ts";
import type { PlannerWorkerDetail } from "../src/domain.ts";
import { PlannerSnapshot, PlannerWorkerRequest, SavedTrip } from "../src/domain.ts";
import {
  activeTripAtom,
  conversationStatusAtom,
  plannerAtom,
  savedTripsAtom,
  sidebarTripsAtom,
  selectTripAtom,
  selectionAtom,
  sessionAtom,
} from "../src/state.ts";

const trip = (id: string) =>
  Schema.decodeSync(SavedTrip)({
    id,
    conversationId: id,
    title: id,
    destination: id,
    revision: 1,
    summary: "A saved trip",
    startDate: null,
    endDate: null,
    travelers: 1,
    days: [],
    notes: [],
    published: null,
  });

const trips = [trip("lisbon"), trip("kyoto")];

const snapshot = (id: string, pending = 0) =>
  Schema.decodeSync(PlannerSnapshot)({
    conversationId: id,
    messages: [{ id: `${id}-answer`, role: "assistant", text: `${id} history`, tripId: id }],
    trips,
    activity: [],
    pending,
    pendingSubmissionIds: pending ? [`${id}-submission`] : [],
    usage: {
      model: "Test model",
      inputTokens: null,
      outputTokens: null,
      estimatedCostMicrousd: null,
    },
  });

const Packet = Schema.Struct({
  id: Schema.Union([Schema.String, Schema.Number]),
  tag: Schema.String,
  payload: Schema.Unknown,
});

const Payload = Schema.Struct({ conversationId: Schema.NullOr(Schema.String) });

const fetchMock = vi.fn<typeof fetch>();

const setup = () => {
  vi.useFakeTimers();
  vi.stubGlobal("location", new URL("https://planner.test"));
  let email = "danieljmerwe@gmail.com";

  const requests: Array<{
    readonly conversationId: string | null;
    readonly email: string;
    readonly succeed: (value: PlannerSnapshot) => void;
    readonly fail: () => void;
  }> = [];

  const workers: Array<{
    readonly conversationId: string;
    readonly workerId: string;
    readonly sourceSequence: number;
    aborted: boolean;
    completed: boolean;
    readonly succeed: (value: PlannerWorkerDetail) => void;
    readonly fail: () => void;
  }> = [];

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

      if (packet.tag === "GetPlannerWorker") {
        const payload = Schema.decodeUnknownSync(PlannerWorkerRequest)(packet.payload);

        return new Promise<Response>((resolve, reject) => {
          const item = {
            ...payload,
            aborted: false,
            completed: false,
            succeed: (value: PlannerWorkerDetail) => {
              item.completed = true;
              resolve(response({ _tag: "Success", value }));
            },
            fail: () => {
              item.completed = true;
              resolve(
                response({
                  _tag: "Failure",
                  cause: [
                    {
                      _tag: "Fail",
                      error: {
                        _tag: "PlannerError",
                        code: "unavailable",
                        message: "Try again",
                      },
                    },
                  ],
                }),
              );
            },
          };

          request.signal.addEventListener(
            "abort",
            () => {
              item.aborted = true;
              reject(new DOMException("Aborted", "AbortError"));
            },
            { once: true },
          );
          workers.push(item);
        });
      }
      const payload = Schema.decodeUnknownSync(Payload)(packet.payload);

      return new Promise<Response>((resolve) => {
        requests.push({
          conversationId: payload.conversationId,
          email,
          succeed: (value) => resolve(response({ _tag: "Success", value })),
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
      });
    }),
  );

  // Match RegistryProvider: no default retention, cleanup runs asynchronously.
  const registry = AtomRegistry.make({
    scheduleTask: (task) => {
      const timer = setTimeout(task, 0);

      return () => clearTimeout(timer);
    },
  });

  registry.set(selectionAtom, { conversationId: "lisbon", tripId: "lisbon" });

  registry.set(sessionAtom, AsyncResult.success({ subjectId: email, displayName: email }));

  const unmounts = [
    registry.mount(plannerAtom),
    registry.mount(savedTripsAtom),
    registry.mount(sidebarTripsAtom),
    registry.mount(conversationStatusAtom),
    registry.mount(activeTripAtom),
  ];

  return {
    registry,
    requests,
    workers,
    revalidateSession: () =>
      registry.set(sessionAtom, AsyncResult.waiting(registry.get(sessionAtom))),
    finishSession: (value: string | AccountError) => {
      if (typeof value === "string") {
        email = value;
        registry.set(sessionAtom, AsyncResult.success({ subjectId: email, displayName: email }));
      } else registry.set(sessionAtom, AsyncResult.fail(value));
    },
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

const flush = () => vi.advanceTimersByTimeAsync(1);

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("keeps conversations without model-saved trips reachable and replaces their label when saved", async () => {
  const fixture = setup();
  const { registry, requests } = fixture;
  const conversation = { conversationId: "golf", title: "Golf and surf with Aaron" };

  try {
    await flush();
    requests[0]!.succeed({ ...snapshot("lisbon"), conversations: [conversation] });
    await flush();
    const entry = registry.get(sidebarTripsAtom).find((item) => item.conversationId === "golf");

    expect(entry).toMatchObject({ ...conversation, id: null });
    if (!entry) throw new Error("Missing conversation");
    registry.set(selectTripAtom, entry);
    expect(registry.get(selectionAtom)).toEqual({ conversationId: "golf", tripId: null });
    await flush();
    requests[1]!.succeed({ ...snapshot("golf"), conversations: [conversation] });
    await flush();
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom)).messages[0]?.text).toBe(
      "golf history",
    );

    await vi.advanceTimersByTimeAsync(2_010);
    requests[2]!.succeed({
      ...snapshot("golf"),
      conversations: [conversation],
      trips: [
        ...trips,
        { ...trip("golf-trip"), conversationId: "golf", title: "Holiday golf and surf" },
      ],
    });
    await flush();
    expect(
      registry.get(sidebarTripsAtom).filter((item) => item.conversationId === "golf"),
    ).toMatchObject([{ id: "golf-trip", title: "Holiday golf and surf" }]);
    expect(registry.get(activeTripAtom)?.id).toBe("golf-trip");

    fixture.setEmail("guest@example.com");
    await flush();
    expect(registry.get(sidebarTripsAtom)).toEqual([]);
  } finally {
    fixture.close();
  }
});

it("preserves the loaded conversation through repeated reconnect failures without reusing it after access rejection", async () => {
  const fixture = setup();
  const { registry, requests } = fixture;
  const frames: string[] = [];
  const unsubscribe = registry.subscribe(conversationStatusAtom, (status) => frames.push(status));

  try {
    await flush();
    requests[0]!.succeed(snapshot("lisbon"));
    await flush();
    frames.length = 0;

    for (let cycle = 0; cycle < 6; cycle++) {
      fixture.revalidateSession();
      await flush();
      fixture.finishSession(new AccountError({ code: "unavailable", message: "Reconnecting" }));
      await vi.advanceTimersByTimeAsync(5_000);
      expect(registry.get(savedTripsAtom)).toEqual(trips);
      expect(AsyncResult.getOrThrow(registry.get(plannerAtom))).toEqual(snapshot("lisbon"));
      fixture.revalidateSession();
      await flush();
      fixture.finishSession("danieljmerwe@gmail.com");
      await flush();
      requests.at(-1)!.succeed(snapshot("lisbon"));
      await flush();
    }

    expect(frames.every((status) => status === "ready")).toBe(true);
    fixture.revalidateSession();
    await flush();
    fixture.finishSession(new AccountError({ code: "unauthorized", message: "Access removed" }));
    await flush();
    expect(AsyncResult.value(registry.get(plannerAtom))._tag).toBe("None");
    expect(registry.get(savedTripsAtom)).toEqual([]);
    fixture.revalidateSession();
    await flush();
    expect(AsyncResult.value(registry.get(plannerAtom))._tag).toBe("None");
    fixture.finishSession("guest@example.com");
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("loading");
    expect(registry.get(savedTripsAtom)).toEqual([]);
  } finally {
    unsubscribe();
    fixture.close();
  }
});

it("keeps every loaded frame visible during delayed polls and session revalidation, then clears a rejected session", async () => {
  const fixture = setup();
  const { registry, requests } = fixture;
  const frames: string[] = [];
  const unsubscribe = registry.subscribe(conversationStatusAtom, (status) => frames.push(status));

  try {
    await flush();
    requests[0]!.succeed(snapshot("lisbon"));
    await flush();
    frames.length = 0;
    await vi.advanceTimersByTimeAsync(2_010);
    expect(requests).toHaveLength(2);
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    requests[1]!.fail();
    await flush();
    await vi.advanceTimersByTimeAsync(2_010);
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    requests.at(-1)!.succeed(snapshot("lisbon"));
    await flush();
    fixture.revalidateSession();
    await flush();
    const requestsBeforeRevalidation = requests.length;

    await vi.advanceTimersByTimeAsync(2_010);
    expect(requests).toHaveLength(requestsBeforeRevalidation);
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom))).toEqual(snapshot("lisbon"));
    expect(registry.get(savedTripsAtom)).toEqual(trips);
    expect(frames.every((status) => status === "ready")).toBe(true);
    fixture.finishSession("danieljmerwe@gmail.com");
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    expect(frames.every((status) => status === "ready")).toBe(true);
    fixture.revalidateSession();
    await flush();
    fixture.finishSession(new AccountError({ code: "unauthorized", message: "Sign in again" }));
    await flush();
    expect(AsyncResult.value(registry.get(plannerAtom))._tag).toBe("None");
    expect(registry.get(savedTripsAtom)).toEqual([]);
  } finally {
    unsubscribe();
    fixture.close();
  }
});

it("restores cached history immediately and polls only the selected conversation, including refresh failures", async () => {
  const fixture = setup();
  const { registry, requests } = fixture;

  try {
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("loading");
    expect(requests).toHaveLength(1);
    requests[0]!.succeed(snapshot("lisbon", 1));
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    registry.set(selectTripAtom, trips[1]!);
    expect(registry.get(conversationStatusAtom)).toBe("loading");
    expect(AsyncResult.value(registry.get(plannerAtom))._tag).toBe("None");
    expect(registry.get(savedTripsAtom)).toEqual(trips);
    expect(registry.get(activeTripAtom)?.id).toBe("kyoto");
    await flush();

    const latest = {
      ...snapshot("kyoto"),
      trips: trips.map((trip) => ({ ...trip, title: `${trip.title} updated` })),
    };

    requests[1]!.succeed(latest);
    await flush();
    await vi.advanceTimersByTimeAsync(2_010);
    expect(requests.map(({ conversationId }) => conversationId)).toEqual([
      "lisbon",
      "kyoto",
      "kyoto",
    ]);
    registry.set(selectTripAtom, trips[0]!);
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom))).toEqual(snapshot("lisbon", 1));
    expect(registry.get(savedTripsAtom)).toEqual(latest.trips);
    requests[2]!.fail();
    await flush();
    registry.set(selectTripAtom, trips[1]!);
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    expect(AsyncResult.isFailure(registry.get(plannerAtom))).toBe(true);
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom))).toEqual(latest);
  } finally {
    fixture.close();
  }
});

it("lets slow conversation reads finish before scheduling another poll", async () => {
  const fixture = setup();
  const { registry, requests } = fixture;

  try {
    await flush();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(requests).toHaveLength(1);
    requests[0]!.succeed(snapshot("lisbon"));
    await flush();
    await vi.advanceTimersByTimeAsync(2_010);
    expect(requests).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(requests).toHaveLength(2);
    expect(registry.get(conversationStatusAtom)).toBe("ready");

    const updated = {
      ...snapshot("lisbon"),
      messages: [
        ...snapshot("lisbon").messages,
        {
          id: "new-reply",
          role: "assistant" as const,
          text: "New research arrived",
          tripId: "lisbon",
        },
      ],
    };

    requests[1]!.succeed(updated);
    await flush();
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom))).toEqual(updated);
  } finally {
    fixture.close();
  }
});

it("keeps unfinished and failed uncached loads separate and never reuses another session's cache", async () => {
  const fixture = setup();
  const { registry, requests } = fixture;

  try {
    await flush();
    registry.set(selectTripAtom, trips[1]!);
    await flush();
    requests[0]!.succeed(snapshot("lisbon"));
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("loading");
    expect(AsyncResult.value(registry.get(plannerAtom))._tag).toBe("None");
    requests[1]!.fail();
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("error");
    registry.set(selectTripAtom, trips[0]!);
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom))).toEqual(snapshot("lisbon"));
    fixture.revalidateSession();
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    fixture.finishSession("guest@example.com");
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("loading");
    expect(registry.get(savedTripsAtom)).toEqual([]);
    expect(registry.get(activeTripAtom)).toBeUndefined();
    expect(AsyncResult.value(registry.get(plannerAtom))._tag).toBe("None");
    expect(requests.at(-1)).toMatchObject({ email: "guest@example.com", conversationId: "lisbon" });
  } finally {
    fixture.close();
  }
});

const workerDetail = (text: string): PlannerWorkerDetail => ({
  state: "idle",
  progress: { submissionId: null, attemptId: null, revision: 0, text, tools: [] },
  activity: [],
});

const withWorkers = (conversationId: string) => ({
  ...snapshot(conversationId),
  scouts: ["slow", "failure", "fast", "queued"].map((id, index) => ({
    id,
    sourceSequence: index + 10,
    title: id,
    task: `Research ${id}`,
    ...workerDetail(""),
    state: "loading" as const,
  })),
  editor: {
    id: "editor",
    sourceSequence: 20,
    task: "Build the app",
    ...workerDetail(""),
    state: "loading" as const,
  },
});

it("shows main history and worker overviews first, bounds parallel details, and isolates failure and timeout", async () => {
  const fixture = setup();
  const { registry, requests, workers } = fixture;

  try {
    await flush();
    expect(workers).toHaveLength(0);
    requests[0]!.succeed(withWorkers("lisbon"));
    await flush();
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom)).messages[0]?.text).toBe(
      "lisbon history",
    );
    expect(workers.map(({ workerId }) => workerId)).toEqual(["slow", "failure", "fast"]);
    expect(
      AsyncResult.getOrThrow(registry.get(plannerAtom)).scouts?.map(({ state }) => state),
    ).toEqual(["loading", "loading", "loading", "loading"]);
    workers[1]!.fail();
    workers[2]!.succeed(workerDetail("Fast result"));
    await flush();
    expect(workers.map(({ workerId }) => workerId)).toEqual([
      "slow",
      "failure",
      "fast",
      "queued",
      "editor",
    ]);
    expect(
      AsyncResult.getOrThrow(registry.get(plannerAtom)).scouts?.map(({ state }) => state),
    ).toEqual(["loading", "unavailable", "idle", "loading"]);
    workers[3]!.succeed(workerDetail("Queued result"));
    workers[4]!.succeed(workerDetail("Editor result"));
    await flush();
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom)).editor?.progress.text).toBe(
      "Editor result",
    );
    await vi.advanceTimersByTimeAsync(3_010);
    expect(workers[0]!.aborted).toBe(true);
    expect(workers.filter(({ workerId }) => workerId === "fast")).toHaveLength(1);
    expect(workers.filter(({ workerId }) => workerId === "editor")).toHaveLength(1);
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom)).scouts?.[0]?.state).toBe(
      "unavailable",
    );
    expect(registry.get(conversationStatusAtom)).toBe("ready");
    expect(
      workers.filter((worker) => !worker.aborted && !worker.completed).length,
    ).toBeLessThanOrEqual(3);
  } finally {
    fixture.close();
  }
  await flush();
  expect(workers.every((worker) => worker.completed || worker.aborted)).toBe(true);
});

it("cancels worker reads and queued permits on conversation switch, rejecting stale results and task locators", async () => {
  const fixture = setup();
  const { registry, requests, workers } = fixture;

  try {
    await flush();
    requests[0]!.succeed(withWorkers("lisbon"));
    await flush();
    registry.set(selectTripAtom, trips[1]!);
    await flush();
    expect(workers).toHaveLength(3);
    expect(workers.every((worker) => worker.aborted)).toBe(true);
    workers[0]!.succeed(workerDetail("Late Lisbon result"));
    requests[1]!.succeed(withWorkers("kyoto"));
    await flush();
    expect(workers.slice(3).map(({ conversationId }) => conversationId)).toEqual([
      "kyoto",
      "kyoto",
      "kyoto",
    ]);
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom)).scouts?.[0]?.progress.text).toBe("");
    workers[3]!.succeed(workerDetail("Kyoto result"));
    await flush();
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom)).scouts?.[0]?.progress.text).toBe(
      "Kyoto result",
    );
    await vi.advanceTimersByTimeAsync(2_010);
    const updated = withWorkers("kyoto");

    updated.scouts[0] = { ...updated.scouts[0]!, sourceSequence: 99, task: "A new request" };
    requests.at(-1)!.succeed(updated);
    await flush();
    expect(AsyncResult.getOrThrow(registry.get(plannerAtom)).scouts?.[0]).toMatchObject({
      sourceSequence: 99,
      task: "A new request",
      state: "loading",
      progress: { text: "" },
    });
  } finally {
    fixture.close();
  }
  await flush();
  expect(workers.every((worker) => worker.completed || worker.aborted)).toBe(true);
});

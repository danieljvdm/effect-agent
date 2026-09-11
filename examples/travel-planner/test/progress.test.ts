import type { OpenAiClient } from "@effect/ai-openai";
import { OpenAiSchema } from "@effect/ai-openai";
import { it } from "@effect/vitest";
import { Clock, Deferred, Effect, Exit, Fiber, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import { expect, expectTypeOf } from "vite-plus/test";

import { defaultPlannerSettings, PlannerError, PlannerProgress } from "../src/domain.ts";
import { observeOpenAi } from "../src/server/models.ts";
import { serveProgress, watchProgress } from "../src/server/progress-http.ts";
import { emptyProgress, PlannerAttempt, ProgressStore, trackTool } from "../src/server/progress.ts";
import { ownerEmail, fixtureSession, fixtureOwner } from "./fixtures/identity.ts";

it.effect("bounds progress and ignores writers from replaced attempts", () =>
  Effect.gen(function* () {
    const store = yield* ProgressStore;
    const first = yield* store.begin("submission", "first");

    yield* first.text("x".repeat(5000));
    for (let index = 0; index < 30; index++)
      yield* first.tool(`tool-${index}`, "Reading a listing", "running");
    const bounded = yield* store.read;

    expect(bounded.text).toHaveLength(4000);
    expect(bounded.tools).toHaveLength(24);
    expect(Schema.is(PlannerProgress)(bounded)).toBe(true);
    expect(bounded.startedAt).toBe(0);
    yield* TestClock.adjust("100 millis");
    const current = yield* store.begin("submission", "replacement");

    yield* current.text("New attempt");
    const before = yield* store.read;

    expect(before.startedAt).toBe(100);
    expect(before.completedAt).toBeUndefined();

    yield* first.text("Stale text");
    yield* first.finish;
    expect(yield* store.read).toEqual(before);
    yield* current.newResponse;
    expect((yield* store.read).text).toBe("");
    yield* current.tool("unfinished", "Reading", "running");
    yield* TestClock.adjust("50 millis");
    yield* current.finish;
    expect((yield* store.read).completedAt).toBe(150);
    expect((yield* store.read).tools[0]).toMatchObject({
      state: "failed",
      startedAt: 100,
      completedAt: 150,
    });
    yield* TestClock.adjust("50 millis");
    yield* current.finish;
    expect((yield* store.read).completedAt).toBe(150);
    expect((yield* store.read).tools[0]?.completedAt).toBe(150);
  }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect(
  "passes provider events through unchanged while observing only provider diagnostics and search lifecycle",
  () =>
    Effect.gen(function* () {
      const store = yield* ProgressStore;
      const writer = yield* store.begin("submission", "attempt");

      const source = [
        {
          type: "response.output_item.added",
          output_index: 0,
          item: {
            type: "web_search_call",
            id: "search",
            status: "in_progress",
            action: { type: "search", queries: ["Tahoe cabins"] },
          },
        },
        {
          type: "response.reasoning_summary_text.delta",
          item_id: "reasoning",
          output_index: 1,
          summary_index: 0,
          delta: "private reasoning",
        },
        {
          type: "response.output_text.delta",
          item_id: "answer",
          output_index: 2,
          content_index: 0,
          delta: "A cabin ",
        },
        {
          type: "response.output_text.delta",
          item_id: "answer",
          output_index: 2,
          content_index: 0,
          delta: "with a hot tub.",
        },
        {
          type: "response.output_item.done",
          output_index: 0,
          item: {
            type: "web_search_call",
            id: "search",
            status: "completed",
            action: { type: "search", queries: ["Tahoe cabins"] },
          },
        },
      ].map((event, sequence_number) =>
        Schema.decodeUnknownSync(OpenAiSchema.ResponseStreamEvent)({ ...event, sequence_number }),
      );

      const frames: PlannerProgress[] = [];

      const client: OpenAiClient.Service = {
        client: HttpClient.make(() => Effect.die("No network expected")),
        createResponse: () => Effect.die("No nonstreaming model call expected"),
        createEmbedding: () => Effect.die("No embedding call expected"),
        createResponseStream: () =>
          Effect.succeed([
            HttpClientResponse.fromWeb(
              HttpClientRequest.post("https://provider.example"),
              new Response(),
            ),
            Stream.fromIterable(source),
          ]),
      };

      const [, stream] = yield* observeOpenAi(client, writer).createResponseStream({
        model: "gpt-5.6-luna",
        input: [],
      });

      const observed = yield* Stream.runCollect(
        stream.pipe(
          Stream.tap(() =>
            store.read.pipe(
              Effect.map((frame) => {
                frames.push(frame);
              }),
            ),
          ),
        ),
      );

      expect(observed).toEqual(source);
      expect(frames[0]?.tools).toEqual([
        { id: "search", label: "Searching the web", state: "running", startedAt: 0 },
      ]);
      expect(frames.at(-1)?.tools[0]?.state).toBe("complete");
      expect(frames.at(-1)?.text).toBe("");
      expect(JSON.stringify(frames)).not.toContain("private reasoning");
      expect(JSON.stringify(frames)).not.toContain("Tahoe cabins");
    }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect(
  "reports native tool start before completion and preserves interruption, failures, and defects",
  () =>
    Effect.gen(function* () {
      const store = yield* ProgressStore;
      const progress = yield* store.begin("submission", "attempt");

      for (const mode of ["success", "failure", "defect", "interrupt"] as const) {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();

        const operation = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(
            mode === "failure"
              ? Effect.fail(new PlannerError({ code: "storage", message: "Failed" }))
              : mode === "defect"
                ? Effect.die("Tool defect")
                : Effect.succeed("done"),
          ),
        );

        const fiber = yield* trackTool(mode, "Reading an Airbnb listing", operation).pipe(
          Effect.provideService(PlannerAttempt, {
            billingOwner: Effect.succeed("travel-planner-owner-v1"),
            progress,
            settings: Effect.succeed(defaultPlannerSettings),
          }),
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        expect((yield* store.read).tools.find(({ id }) => id === mode)?.state).toBe("running");
        const start = yield* Clock.currentTimeMillis;

        yield* TestClock.adjust("250 millis");
        yield* progress.tool(mode, "Reading an Airbnb listing", "running");
        expect((yield* store.read).tools.find(({ id }) => id === mode)?.startedAt).toBe(start);
        if (mode === "interrupt") yield* Fiber.interrupt(fiber);
        else yield* Deferred.succeed(release, undefined);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isSuccess(exit)).toBe(mode === "success");
        if (mode === "interrupt") expect(Exit.hasInterrupts(exit)).toBe(true);
        expect((yield* store.read).tools.find(({ id }) => id === mode)?.state).toBe(
          mode === "success" ? "complete" : "failed",
        );
        const done = (yield* store.read).tools.find(({ id }) => id === mode);

        expect(done).toMatchObject({ startedAt: start, completedAt: start + 250 });
        yield* TestClock.adjust("100 millis");
        yield* progress.tool(
          mode,
          "Reading an Airbnb listing",
          mode === "success" ? "complete" : "failed",
        );
        expect((yield* store.read).tools.find(({ id }) => id === mode)).toEqual(done);
      }
    }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect("runs direct tools without an attempt observer and preserves their dependencies", () =>
  Effect.gen(function* () {
    const store = yield* ProgressStore;
    const before = yield* store.read;
    const operation = Effect.flatMap(ProgressStore, () => Effect.fail("expected" as const));
    const tracked = trackTool("direct", "Reading travel details", operation);

    expectTypeOf<Effect.Error<typeof tracked>>().toEqualTypeOf<"expected">();
    expectTypeOf<Effect.Services<typeof tracked>>().toEqualTypeOf<ProgressStore>();
    expect(yield* tracked.pipe(Effect.exit)).toEqual(Exit.fail("expected"));
    expect(yield* store.read).toEqual(before);
  }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect(
  "streams replacement frames, skips unchanged polls, and finalizes observation on interruption",
  () =>
    Effect.gen(function* () {
      const store = yield* ProgressStore;
      const writer = yield* store.begin("submission", "attempt");
      const frames = yield* Ref.make<PlannerProgress[]>([]);
      const started = yield* Deferred.make<void>();
      const changed = yield* Deferred.make<void>();
      const closed = yield* Ref.make(false);

      const fiber = yield* watchProgress(store.read).pipe(
        Stream.tap((frame) =>
          Ref.update(frames, (values) => [...values, frame]).pipe(
            Effect.andThen(
              frame.text === "Live"
                ? Deferred.succeed(changed, undefined)
                : Deferred.succeed(started, undefined),
            ),
          ),
        ),
        Stream.ensuring(Ref.set(closed, true)),
        Stream.runDrain,
        Effect.forkChild,
      );

      yield* Deferred.await(started);
      yield* TestClock.adjust("400 millis");
      expect(yield* Ref.get(frames)).toHaveLength(1);
      yield* writer.text("Live");
      yield* TestClock.adjust("200 millis");
      yield* Deferred.await(changed);
      expect((yield* Ref.get(frames)).at(-1)?.text).toBe("Live");
      yield* Fiber.interrupt(fiber);
      expect(yield* Ref.get(closed)).toBe(true);
      yield* writer.text(" continues");
      expect((yield* store.read).text).toBe("Live continues");
    }).pipe(Effect.provide(ProgressStore.layer)),
);

it("streams HTTP before completion and cancellation disposes the request without forwarding private owner IDs", async () => {
  const addresses: string[] = [];

  const response = await Effect.runPromise(
    serveProgress(
      new Request("https://planner.example/api/progress", {
        method: "POST",
        headers: { "content-type": "application/ndjson" },
        body:
          JSON.stringify({
            _tag: "Request",
            id: "0",
            tag: "WatchProgress",
            payload: { conversationId: "travel-planner-owner-v1" },
            headers: [],
          }) + "\n",
      }),
      {
        THREADS: {
          getByName: (name) => {
            addresses.push(name);

            return {
              plannerProgress: async () => JSON.stringify({ ...emptyProgress, text: "Live" }),
            };
          },
        },
      },
      fixtureSession("friend@example.com"),
    ),
  );

  const reader = response.body?.getReader();

  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  const chunk = await reader?.read();

  expect(chunk?.done).toBe(false);
  expect(new TextDecoder().decode(chunk?.value)).toContain("Live");
  expect(addresses[0]).toBe(`${fixtureOwner("friend@example.com")}--travel-planner-owner-v1`);
  await reader?.cancel();
  reader?.releaseLock();

  const deniedAddresses: string[] = [];

  const forbidden = await Effect.runPromise(
    serveProgress(
      new Request("https://planner.example/api/progress", {
        method: "POST",
        headers: { "content-type": "application/ndjson" },
        body:
          JSON.stringify({
            _tag: "Request",
            id: "0",
            tag: "WatchProgress",
            payload: { conversationId: `member-${"a".repeat(64)}--trip` },
            headers: [],
          }) + "\n",
      }),
      {
        THREADS: {
          getByName: (name) => {
            deniedAddresses.push(name);

            return { plannerProgress: async () => JSON.stringify(emptyProgress) };
          },
        },
      },
      fixtureSession(ownerEmail),
    ),
  );

  expect(await forbidden.text()).toContain("Invalid conversation");
  expect(deniedAddresses).toEqual([]);
});

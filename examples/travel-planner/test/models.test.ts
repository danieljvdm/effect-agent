import * as Agent from "@effect-agent/core/Agent";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Redacted, Ref, Result, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { expect, expectTypeOf } from "vite-plus/test";

import { TripToolsLive } from "../src/agent.ts";
import {
  defaultPlannerSettings,
  PlannerError,
  PlannerSettings,
  Trip,
  TripSiteStore,
} from "../src/domain.ts";
import { CheckedFinishResearch, CheckedFinishResearchLive } from "../src/research/completion.ts";
import { ScoutFindings } from "../src/research/contracts.ts";
import { FailureDiagnostics, type FailureDiagnostic } from "../src/server/diagnostics.ts";
import { liveModel, observeOpenAi, selectableModel } from "../src/server/models.ts";
// Retain these protocol/legacy-publication regressions against the admitted v5 definition.
import { previousResponsePlanner as planner } from "../src/server/planner.ts";
import { PlannerAttempt, ProgressStore } from "../src/server/progress.ts";
import { observePublicOutput } from "../src/server/public-output.ts";
import { TripRepository } from "../src/server/trips.ts";
import { FixtureBrowserLive } from "./fixtures/browser.ts";

const RequestBody = Schema.Struct({
  model: Schema.String,
  reasoning: Schema.Struct({ effort: Schema.String }),
  service_tier: Schema.String,
  max_output_tokens: Schema.Number,
  max_tool_calls: Schema.Number,
  parallel_tool_calls: Schema.Boolean,
  store: Schema.Boolean,
  stream: Schema.Boolean,
});

const answer = (model: string) => {
  const item = {
    type: "message",
    id: "answer",
    role: "assistant",
    status: "completed",
    content: [{ type: "output_text", text: "A useful answer.", annotations: [] }],
  };

  const response = { id: "response", object: "response", model, created_at: 0, output: [item] };

  const events = [
    { type: "response.created", response: { ...response, output: [] } },
    {
      type: "response.output_item.added",
      output_index: 0,
      item: { ...item, status: "in_progress", content: [] },
    },
    {
      type: "response.output_text.delta",
      item_id: "answer",
      output_index: 0,
      content_index: 0,
      delta: "A useful answer.",
    },
    { type: "response.output_item.done", output_index: 0, item },
    { type: "response.completed", response },
  ];

  return new Response(
    events
      .map((event, sequence_number) => `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`)
      .join(""),
    { headers: { "content-type": "text/event-stream" } },
  );
};

const functionAnswer = (
  calls: ReadonlyArray<{ readonly name: string; readonly params: Schema.Json }>,
  turn: number,
) => {
  const items = calls.map((call, index) => ({
    type: "function_call",
    id: `item-${turn}-${index}`,
    call_id: `call-${turn}-${index}-${call.name}`,
    name: call.name,
    arguments: JSON.stringify(call.params),
    status: "completed",
  }));

  const response = {
    id: "response",
    object: "response",
    model: "gpt-5.6-luna",
    created_at: 0,
    output: items,
  };

  const events = [
    { type: "response.created", response: { ...response, output: [] } },
    ...items.flatMap((item, output_index) => [
      {
        type: "response.output_item.added",
        output_index,
        item: { ...item, arguments: "", status: "in_progress" },
      },
      {
        type: "response.function_call_arguments.delta",
        output_index,
        item_id: item.id,
        delta: item.arguments,
      },
      {
        type: "response.function_call_arguments.done",
        output_index,
        item_id: item.id,
        name: item.name,
        arguments: item.arguments,
      },
      { type: "response.output_item.done", output_index, item },
    ]),
    { type: "response.completed", response },
  ];

  return new Response(
    events
      .map((event, sequence_number) => `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`)
      .join(""),
    { headers: { "content-type": "text/event-stream" } },
  );
};

it.effect(
  "corrects a mixed completion batch through the real provider client before running each handler once",
  () =>
    Effect.gen(function* () {
      const store = yield* ProgressStore;

      const RequestWithTools = Schema.Struct({
        ...RequestBody.fields,
        tool_choice: Schema.String,
        tools: Schema.Array(
          Schema.Struct({ type: Schema.String, name: Schema.optionalKey(Schema.String) }),
        ),
        input: Schema.Array(Schema.Unknown),
      });

      const trip = Trip.make({
        id: "date-advice",
        revision: 1,
        title: "Date advice fixture",
        destination: "Tahoe",
        summary: "A saved trip",
        startDate: null,
        endDate: null,
        travelers: 2,
        days: [],
        notes: [],
        published: null,
      });

      const getTrip = { name: "get_trip", params: { tripId: trip.id } };

      const deliver = {
        name: "deliver_response",
        params: { message: "Compare the dates before booking.", content: null },
      };

      for (const mixed of [false, true]) {
        const progress = yield* store.begin(`submission-${mixed}`, `attempt-${mixed}`);
        const requests: Array<typeof RequestWithTools.Type> = [];
        const started: string[] = [];
        const succeeded: string[] = [];
        const failed: string[] = [];
        const getCalls = yield* Ref.make(0);

        const fetch: typeof globalThis.fetch = async (_url, init) => {
          const request = Schema.decodeUnknownSync(Schema.fromJsonString(RequestWithTools))(
            await new Response(init?.body).text(),
          );

          requests.push(request);
          const ordinaryTurn = requests.length - (mixed ? 1 : 0);

          if (mixed && requests.length === 2) {
            expect(started).toEqual([]);
            expect(succeeded).toEqual([]);
            expect(failed).toEqual(["get_trip", "deliver_response"]);
          }

          return functionAnswer(
            mixed && requests.length === 1
              ? [getTrip, deliver]
              : ordinaryTurn === 1
                ? [getTrip]
                : [deliver],
            requests.length,
          );
        };

        const repository = Layer.succeed(TripRepository, {
          get: () => Ref.update(getCalls, (count) => count + 1).pipe(Effect.as(trip)),
          list: Effect.die("Unexpected list"),
          listConversations: Effect.die("Unexpected conversation list"),
          rememberConversation: () => Effect.die("Unexpected conversation registration"),
          conversationId: () => Effect.die("Unexpected conversation lookup"),
          save: () => Effect.die("Unexpected save"),
          recordPublication: () => Effect.die("Unexpected publication"),
        });

        const sites = Layer.succeed(TripSiteStore, {
          publish: () => Effect.die("Unexpected publication"),
          load: () => Effect.die("Unexpected public site"),
        });

        const model = selectableModel(Redacted.make("fake-api-key")).pipe(
          Layer.provide(
            Layer.succeed(PlannerAttempt, {
              billingOwner: Effect.succeed("travel-planner-owner-v1"),
              progress,
              settings: Effect.succeed(defaultPlannerSettings),
            }),
          ),
        );

        const outcome = yield* AgentRuntime.stream(planner, {
          message: "Which dates should I choose?",
          selectedTripId: trip.id,
          publication: null,
        }).pipe(
          Stream.tap((event) =>
            Effect.sync(() => {
              if (event._tag === "ToolCallStarted") started.push(event.toolName);
              if (event._tag === "ToolCallSucceeded") succeeded.push(event.toolName);
              if (event._tag === "ToolCallFailed") failed.push(event.toolName);
            }),
          ),
          Stream.runCollect,
          Effect.provide([
            model,
            repository,
            sites,
            TripToolsLive("date-conversation"),
            FixtureBrowserLive,
            IdGenerator.layer,
            ThreadHistory.layerTransient,
          ]),
          Effect.provideService(FetchHttpClient.Fetch, fetch),
          Effect.result,
        );

        expect(requests).toHaveLength(mixed ? 3 : 2);
        for (const request of requests) {
          expect(request.parallel_tool_calls).toBe(false);
          expect(request.tool_choice).toBe("required");
          expect(request.tools.map((tool) => tool.name)).toContain("get_trip");
          expect(request.tools.map((tool) => tool.name)).toContain("deliver_response");
        }
        if (mixed) {
          const correction = JSON.stringify(requests[1]?.input);

          expect(correction).toContain("ModelProtocolError");
          expect(correction).toContain("none of its tools ran");
          expect(correction).toContain("call-1-0-get_trip");
          expect(correction).toContain("call-1-1-deliver_response");
        }
        expect(failed).toEqual(mixed ? ["get_trip", "deliver_response"] : []);
        expect(Result.isSuccess(outcome)).toBe(true);
        if (Result.isSuccess(outcome))
          expect(outcome.success.find((event) => event._tag === "RunCompleted")).toMatchObject({
            output: deliver.params.message,
          });
        expect(started).toEqual(["get_trip", "deliver_response"]);
        expect(succeeded).toEqual(started);
        expect(yield* Ref.get(getCalls)).toBe(1);
        expect(JSON.stringify(requests.at(-1)?.input)).toContain(trip.title);
        expect((yield* store.read).text).toBe(deliver.params.message);
      }
    }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect(
  "sends admitted model, reasoning and fast choices through the real OpenAI SDK and exposes actual model metadata",
  () =>
    Effect.gen(function* () {
      const store = yield* ProgressStore;
      const requests: Array<typeof RequestBody.Type> = [];

      const fetch: typeof globalThis.fetch = async (_url, init) => {
        const body = Schema.decodeUnknownSync(Schema.fromJsonString(RequestBody))(
          await new Response(init?.body).text(),
        );

        requests.push(body);

        return answer(body.model);
      };

      const selections: PlannerSettings[] = [
        defaultPlannerSettings,
        { model: "gpt-5.6-luna", reasoningEffort: "none", fast: true },
        { model: "gpt-6-astra", reasoningEffort: "max", fast: true },
      ];

      for (const [index, settings] of selections.entries()) {
        const progress = yield* store.begin(`submission-${index}`, `attempt-${index}`);

        const model = selectableModel(Redacted.make("fake-api-key")).pipe(
          Layer.provide(
            Layer.succeed(PlannerAttempt, {
              billingOwner: Effect.succeed("travel-planner-owner-v1"),
              settings: Effect.succeed(settings),
              progress,
            }),
          ),
        );

        const actual = yield* Effect.gen(function* () {
          const label = yield* Model.ModelName;

          yield* Stream.runDrain(LanguageModel.streamText({ prompt: "A public test question" }));

          return label;
        }).pipe(Effect.provide(model), Effect.provideService(FetchHttpClient.Fetch, fetch));

        expect(actual).toBe(settings.model);
        expect(requests.at(-1)).toEqual({
          model: settings.model,
          reasoning: { effort: settings.reasoningEffort },
          service_tier: settings.fast ? "fast" : "default",
          max_output_tokens: 16_384,
          max_tool_calls: 4,
          parallel_tool_calls: false,
          store: false,
          stream: true,
        });
        expect((yield* store.read).text).toBe("A useful answer.");
      }
      expectTypeOf<
        Layer.Services<ReturnType<typeof selectableModel>>
      >().toEqualTypeOf<PlannerAttempt>();
    }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect(
  "keeps one model binding for an attempt even if later input choices change, and fails closed if admitted settings cannot be read",
  () =>
    Effect.gen(function* () {
      const store = yield* ProgressStore;
      const progress = yield* store.begin("submission", "attempt");

      const selection = yield* Ref.make<PlannerSettings>({
        model: "gpt-5.6-luna",
        reasoningEffort: "low",
        fast: false,
      });

      const settings = yield* Effect.cached(Ref.get(selection));

      const model = selectableModel(Redacted.make("fake-api-key")).pipe(
        Layer.provide(
          Layer.succeed(PlannerAttempt, {
            billingOwner: Effect.succeed("travel-planner-owner-v1"),
            settings,
            progress,
          }),
        ),
      );

      const models: string[] = [];

      const fetch: typeof globalThis.fetch = async (_url, init) => {
        const body = Schema.decodeUnknownSync(Schema.fromJsonString(RequestBody))(
          await new Response(init?.body).text(),
        );

        models.push(body.model);

        return answer(body.model);
      };

      yield* Effect.gen(function* () {
        yield* Stream.runDrain(LanguageModel.streamText({ prompt: "First turn" }));
        yield* Ref.set(
          selection,
          PlannerSettings.make({ model: "gpt-6-astra", reasoningEffort: "max", fast: true }),
        );
        yield* Stream.runDrain(LanguageModel.streamText({ prompt: "Joined follow-up" }));
      }).pipe(Effect.provide(model), Effect.provideService(FetchHttpClient.Fetch, fetch));
      expect(models).toEqual(["gpt-5.6-luna", "gpt-5.6-luna"]);

      const unavailable = selectableModel(Redacted.make("fake-api-key")).pipe(
        Layer.provide(
          Layer.succeed(PlannerAttempt, {
            billingOwner: Effect.succeed("travel-planner-owner-v1"),
            progress,
            settings: Effect.fail(
              new PlannerError({ code: "unavailable", message: "Settings unavailable" }),
            ),
          }),
        ),
      );

      const error = yield* Stream.runDrain(
        LanguageModel.streamText({ prompt: "Must not reach provider" }),
      ).pipe(
        Effect.provide(unavailable),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
        Effect.flip,
      );

      expect(error._tag).toBe("AiError");
      expect(models).toHaveLength(2);
    }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect("retains the exact legacy model identity and rejects unsupported UI settings", () =>
  Effect.gen(function* () {
    const legacy = yield* liveModel({
      THREADS: {
        getByName: () => ({
          modelCredential: async () => "null",
          demoAccessAllowed: async () => false,
        }),
      },
    }).pipe(
      Effect.provide(
        ConfigProvider.layer(ConfigProvider.fromEnvRecord({ OPENAI_API_KEY: "fake-api-key" })),
      ),
    );

    expect(legacy.identity).toBe(
      '{"provider":"openai","name":"gpt-5.6-luna","store":false,"max_output_tokens":4096,"max_tool_calls":1,"reasoning":{"effort":"low"}}',
    );
    expect(
      Schema.is(PlannerSettings)({ model: "gpt-6-astra", reasoningEffort: "none", fast: false }),
    ).toBe(false);
    expect(
      Schema.is(PlannerSettings)({ model: "arbitrary-model", reasoningEffort: "low", fast: false }),
    ).toBe(false);
  }),
);

it.effect("distinguishes completed, unfinished and failed searches in public SSE progress", () =>
  Effect.gen(function* () {
    const store = yield* ProgressStore;
    const progress = yield* store.begin("search-submission", "search-attempt");

    const items = ["completed", "searching", "failed"].map((status, index) => ({
      type: "web_search_call",
      id: `search-${index}`,
      status,
      action: { type: "search", query: "Boston Mexico nonstop" },
    }));

    const events = items.flatMap((item, output_index) => [
      {
        type: "response.output_item.added",
        output_index,
        item: { ...item, status: "in_progress" },
      },
      { type: "response.output_item.done", output_index, item },
    ]);

    const fetch: typeof globalThis.fetch = async () =>
      new Response(
        events
          .map(
            (event, sequence_number) =>
              `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
          )
          .join(""),
        { headers: { "content-type": "text/event-stream" } },
      );

    const client = yield* OpenAiClient.make({ apiKey: Redacted.make("fake-api-key") }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
    );

    const [, stream] = yield* observeOpenAi(client, progress).createResponseStream({
      model: "gpt-5.6-luna",
      input: [],
    });

    const received = yield* Stream.runCollect(stream);

    yield* progress.finish;

    expect(received.map((event) => event.type)).toEqual(events.map((event) => event.type));
    expect((yield* store.read).tools.map(({ id, state }) => ({ id, state }))).toEqual([
      { id: "search-0", state: "complete" },
      { id: "search-1", state: "incomplete" },
      { id: "search-2", state: "failed" },
    ]);
    expect((yield* store.read).tools.every((tool) => tool.completedAt !== undefined)).toBe(true);
  }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect("streams only deliver-response message arguments through the real SDK SSE decoder", () =>
  Effect.gen(function* () {
    const store = yield* ProgressStore;
    const progress = yield* store.begin("submission", "attempt");
    const writes: string[] = [];
    const frames: string[] = [];

    const chunks = [
      '{"content":{"notes":"CARD_SECRET"},"message":"A ',
      '\\"quiet\\" stay\\',
      "nTahoe ",
      "\\uD83",
      "D\\uDE80",
      '."}',
    ];

    const responseArguments = chunks.join("");

    const item = {
      type: "function_call",
      id: "response-item",
      call_id: "response-call",
      name: "deliver_response",
      arguments: responseArguments,
      status: "completed",
    };

    const events = [
      {
        type: "response.output_item.added",
        output_index: 0,
        item: {
          type: "function_call",
          id: "save-item",
          call_id: "save-call",
          name: "save_trip",
          arguments: "",
          status: "in_progress",
        },
      },
      {
        type: "response.function_call_arguments.delta",
        output_index: 0,
        item_id: "save-item",
        delta: '{"message":"TOOL_SECRET","notes":["NOTE_SECRET"]}',
      },
      {
        type: "response.reasoning_summary_text.delta",
        item_id: "reasoning",
        output_index: 1,
        summary_index: 0,
        delta: "REASONING_SECRET",
      },
      {
        type: "response.output_item.added",
        output_index: 2,
        item: { ...item, arguments: "", status: "in_progress" },
      },
      ...chunks.map((delta) => ({
        type: "response.function_call_arguments.delta",
        output_index: 2,
        item_id: "response-item",
        delta,
      })),
      {
        type: "response.function_call_arguments.delta",
        output_index: 0,
        item_id: "save-item",
        delta: '{"message":"UNRELATED_TOOL_SECRET"}',
      },
      {
        type: "response.output_item.added",
        output_index: 3,
        item: {
          ...item,
          id: "duplicate-item",
          call_id: "duplicate-call",
          arguments: "",
          status: "in_progress",
        },
      },
      {
        type: "response.function_call_arguments.delta",
        output_index: 3,
        item_id: "duplicate-item",
        delta: '{"message":"DUPLICATE_SECRET","content":null}',
      },
      {
        type: "response.function_call_arguments.done",
        output_index: 2,
        item_id: "response-item",
        arguments: responseArguments,
        name: "deliver_response",
      },
      { type: "response.output_item.done", output_index: 2, item },
      {
        type: "response.completed",
        response: {
          id: "response",
          object: "response",
          model: "gpt-5.6-luna",
          created_at: 0,
          output: [item],
        },
      },
    ].map((event, sequence_number) => ({ ...event, sequence_number }));

    const fetch: typeof globalThis.fetch = async () =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            const encoder = new TextEncoder();

            for (const event of events) {
              const packet = encoder.encode(`data: ${JSON.stringify(event)}\n\n`);

              // Split transport chunks independently of argument boundaries.
              controller.enqueue(packet.slice(0, 13));
              controller.enqueue(packet.slice(13));
            }
            controller.close();
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      );

    const client = yield* OpenAiClient.make({ apiKey: Redacted.make("fake-api-key") }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
    );

    const native = yield* LanguageModel.LanguageModel.pipe(
      Effect.provide(
        OpenAiLanguageModel.model("gpt-5.6-luna").pipe(
          Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, client)),
        ),
      ),
    );

    const observed = observePublicOutput(native, {
      ...progress,
      text: (delta) =>
        progress.text(delta).pipe(
          Effect.andThen(
            Effect.sync(() => {
              writes.push(delta);
            }),
          ),
        ),
    });

    const responseTools = Toolkit.make(
      Tool.make("deliver_response", {
        parameters: Schema.Struct({ message: Schema.String, content: Schema.Unknown }),
        success: Schema.Void,
      }),
    );

    const stream = observed.streamText({
      prompt: "Research a stay",
      disableToolCallResolution: true,
      toolkit: responseTools,
    });

    const received = yield* Stream.runCollect(
      stream.pipe(
        Stream.tap(() =>
          store.read.pipe(
            Effect.map((frame) => {
              frames.push(frame.text);
            }),
          ),
        ),
      ),
    ).pipe(Effect.provide(responseTools.toLayer({ deliver_response: () => Effect.void })));

    expect(received.some((part) => part.type === "tool-params-start")).toBe(true);
    expect(writes.length).toBeGreaterThan(2);
    expect(writes[0]).toBe("A ");
    expect(writes.join("")).toBe('A "quiet" stay\nTahoe 🚀.');
    expect(frames).toContain("A ");
    expect(frames.at(-1)).toBe('A "quiet" stay\nTahoe 🚀.');
    expect(JSON.stringify(frames)).not.toContain("SECRET");
    expect((yield* store.read).tools).toEqual([]);
  }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect("records provider request failures with causes without changing the returned error", () =>
  Effect.gen(function* () {
    const store = yield* ProgressStore;
    const writer = yield* store.begin("failed-request", "attempt");
    const captured = yield* Ref.make<FailureDiagnostic[]>([]);

    const fetch: typeof globalThis.fetch = async () =>
      new Response(
        JSON.stringify({
          error: {
            message: "Provider unavailable",
            type: "server_error",
            code: "service_unavailable",
          },
        }),
        {
          status: 503,
          headers: { "content-type": "application/json", "x-request-id": "req-provider-test" },
        },
      );

    const client = yield* OpenAiClient.make({ apiKey: Redacted.make("sk-PRIVATE123456789") }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
    );

    const result = yield* observeOpenAi(client, writer)
      .createResponseStream({ model: "gpt-6-astra", input: [] })
      .pipe(
        Effect.exit,
        Effect.provideService(FailureDiagnostics, {
          append: (value) => Ref.update(captured, (values) => [...values, value]),
          list: Effect.succeed([]),
        }),
      );

    expect(result._tag).toBe("Failure");
    const saved = yield* Ref.get(captured);

    expect(saved).toHaveLength(1);
    expect(saved[0]?.operation).toBe("OpenAI: response request failed");
    expect(saved[0]?.text).toContain("Provider unavailable");
    expect(saved[0]?.text).toContain("503");
    expect(saved[0]?.text).toContain("req-provider-test");
    expect(saved[0]?.text).not.toContain("PRIVATE123456789");
  }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect(
  "bills the verified account, observes key rotation/removal between calls, and never falls back to a host key",
  () =>
    Effect.gen(function* () {
      const encryption = new Uint8Array(32);

      const key = yield* Effect.promise(() =>
        crypto.subtle.importKey("raw", encryption, "AES-GCM", false, ["encrypt"]),
      );

      const seal = (owner: string, secret: string) =>
        Effect.gen(function* () {
          const iv = crypto.getRandomValues(new Uint8Array(12));

          const encrypted = yield* Effect.promise(() =>
            crypto.subtle.encrypt(
              {
                name: "AES-GCM",
                iv,
                additionalData: new TextEncoder().encode(`travel-planner:openai:v1:${owner}`),
              },
              key,
              new TextEncoder().encode(secret),
            ),
          );

          return JSON.stringify({
            version: 1,
            iv: btoa(String.fromCharCode(...iv)),
            ciphertext: btoa(String.fromCharCode(...new Uint8Array(encrypted))),
            lastFour: secret.slice(-4),
            updatedAt: "2026-09-10T00:00:00Z",
          });
        });

      const alice = "alice-account";
      const bob = "bob-account";

      const records = new Map([
        [alice, yield* seal(alice, "sk-private-alice-1111")],
        [bob, yield* seal(bob, "sk-private-bob-2222")],
      ]);

      const lookedUp: string[] = [];

      const live = yield* liveModel({
        BYOK_ENCRYPTION_KEY: btoa(String.fromCharCode(...encryption)),
        THREADS: {
          getByName: (owner) => ({
            demoAccessAllowed: async () => false,
            modelCredential: async () => {
              lookedUp.push(owner);

              return records.get(owner) ?? "null";
            },
          }),
        },
      });

      const requests: string[] = [];

      const fetch: typeof globalThis.fetch = async (_url, init) => {
        requests.push(new Headers(init?.headers).get("authorization") ?? "");

        return answer("gpt-5.6-luna");
      };

      const store = yield* ProgressStore;
      const progress = yield* store.begin("byok-submission", "byok-attempt");
      const ask = Stream.runDrain(LanguageModel.streamText({ prompt: "A fixture request" }));

      yield* Effect.gen(function* () {
        yield* ask;
        records.set(alice, yield* seal(alice, "sk-private-rotated-3333"));
        yield* ask;
        records.delete(alice);
        const failure = yield* Effect.flip(ask);

        expect(failure.message).toContain("Connect your OpenAI API key");
      }).pipe(
        Effect.provide(
          live.selectable.pipe(
            Layer.provide(
              Layer.succeed(PlannerAttempt, {
                billingOwner: Effect.succeed(alice),
                settings: Effect.succeed(defaultPlannerSettings),
                progress,
              }),
            ),
          ),
        ),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
      );
      yield* ask.pipe(
        Effect.provide(
          live.selectable.pipe(
            Layer.provide(
              Layer.succeed(PlannerAttempt, {
                billingOwner: Effect.succeed(bob),
                settings: Effect.succeed(defaultPlannerSettings),
                progress,
              }),
            ),
          ),
        ),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
      );
      expect(requests).toEqual([
        "Bearer sk-private-alice-1111",
        "Bearer sk-private-rotated-3333",
        "Bearer sk-private-bob-2222",
      ]);
      expect(lookedUp).toEqual([alice, alice, alice, bob]);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ProgressStore.layer,
          ConfigProvider.layer(
            ConfigProvider.fromEnvRecord({ OPENAI_API_KEY: "sk-must-never-use-host-key" }),
          ),
        ),
      ),
    ),
);

it.effect(
  "lets the scout correct oversized findings through OpenAI and the agent loop before completing",
  () =>
    Effect.gen(function* () {
      const evidence = {
        summary:
          "Ferry Building food stops and Golden Gate Park suit a balanced San Francisco week. October dates and hotel prices remain unverified.",
        sources: [
          {
            title: "San Francisco",
            url: "https://www.sftravel.com/",
            notes: "Destination reference; no hotel availability confirmed.",
            photos: [],
          },
        ],
      };

      const oversized = { ...evidence, summary: "Research detail. ".repeat(300) };

      const oversizedBytes = {
        ...evidence,
        summary: "😀".repeat(1900),
        sources: Array.from({ length: 6 }, () => evidence.sources[0]),
      };

      const invalidUrl = {
        ...evidence,
        sources: [{ ...evidence.sources[0], url: "https://localhost/secret" }],
      };

      for (const rejected of [oversized, oversizedBytes, invalidUrl]) {
        const drafts = [rejected, evidence];
        const requests: string[] = [];

        const scout = Agent.make("research-validation-fixture", {
          input: Schema.String,
          output: ScoutFindings,
          toolkit: Toolkit.make(CheckedFinishResearch),
          policy: { maxTurns: 5, maxToolCalls: 5 },
          instructions:
            "Finish the existing research. Correct rejected drafts without starting over.",
          completion: { tool: "finish_research", required: true, project: ({ result }) => result },
        });

        const fetch: typeof globalThis.fetch = async (_url, init) => {
          requests.push(await new Response(init?.body).text());
          const draft = drafts[requests.length - 1];

          if (!draft) throw new Error("Unexpected additional research turn");

          return functionAnswer([{ name: "finish_research", params: draft }], requests.length);
        };

        const client = OpenAiClient.layer({ apiKey: Redacted.make("fake-api-key") }).pipe(
          Layer.provide(FetchHttpClient.layer),
        );

        const events = yield* AgentRuntime.stream(
          scout,
          "Plan one week in San Francisco from Louisville; preserve the research already gathered.",
        ).pipe(
          Stream.runCollect,
          Effect.provide([
            OpenAiLanguageModel.model("gpt-6-astra").pipe(Layer.provide(client)),
            CheckedFinishResearchLive,
            IdGenerator.layer,
            ThreadHistory.layerTransient,
          ]),
          Effect.provideService(FetchHttpClient.Fetch, fetch),
        );

        expect(requests).toHaveLength(2);
        for (const request of requests.slice(1)) {
          expect(request).toContain("Findings were not accepted");
          expect(request).toContain("8192 bytes");
          expect(request).toContain("Louisville");
        }
        expect(events.filter((event) => event._tag === "ToolCallFailed")).toHaveLength(1);
        expect(events.filter((event) => event._tag === "ToolCallSucceeded")).toHaveLength(1);
        const completed = events.find((event) => event._tag === "RunCompleted");

        expect(completed?.output).toEqual(evidence);
      }
      expectTypeOf<Layer.Services<typeof CheckedFinishResearchLive>>().toEqualTypeOf<never>();
      expectTypeOf<Layer.Error<typeof CheckedFinishResearchLive>>().toEqualTypeOf<never>();
    }),
);

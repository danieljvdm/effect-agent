import {
  BrowserSessionError,
  BrowserSessionReference,
  BrowserSessions,
  type BrowserSession,
} from "@effect-agent/platform-cloudflare/browser-session";
import { assert, it } from "@effect/vitest";
import { Config, Effect, Option, Redacted, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import puppeteer from "puppeteer-core";
import browserPuppeteer from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { makeBrowser, type Action } from "../src/browser.ts";
import { scenarios, seed, type RunInput } from "../src/contract.ts";
import { emptyControl, makeOwner, type Control } from "../src/owner.ts";
import { makeTrace, Trace } from "../src/telemetry.ts";

const request = (
  scenario: RunInput["scenario"],
  temperature?: RunInput["temperature"],
): RunInput => ({
  id: crypto.randomUUID(),
  scenario,
  mode: "scripted",
  ...(temperature === undefined ? {} : { temperature }),
  prompt: "",
  screenshots: false,
  liveView: false,
});

it.live(
  "performs all preset workflows in Chromium, verifies the complete board and prepares fresh browsers and retries only before the first action",
  (test) =>
    Effect.gen(function* () {
      const executable = yield* Config.option(Config.String("BROWSER_TEST_EXECUTABLE"));

      if (Option.isNone(executable)) return test.skip();

      const chrome = yield* Effect.acquireRelease(
        Effect.promise(() =>
          puppeteer.launch({ executablePath: executable.value, headless: true }),
        ),
        (value) => Effect.promise(() => value.close()),
      );

      const connection = yield* Effect.acquireRelease(
        Effect.promise(() => browserPuppeteer.connect({ browserWSEndpoint: chrome.wsEndpoint() })),
        (value) => Effect.promise(() => value.disconnect()),
      );

      const page = yield* Effect.promise(() => connection.newPage());

      const reference = BrowserSessionReference.make({
        version: 1,
        sessionId: Redacted.make(crypto.randomUUID()),
        contextId: Redacted.make("local"),
        targetId: Redacted.make("page"),
        expiresAt: Date.now() + 600_000,
        commandTimeoutMillis: 5_000,
      });

      const unused = () => Effect.die("Not used in the local Chromium proof");

      let failAfterReady = false;

      const session: BrowserSession = {
        reference,
        run: (authorize, action) =>
          authorize.pipe(
            Effect.andThen(
              Effect.gen(function* () {
                if (failAfterReady && (yield* owner.snapshot()).report?.readyAt !== null)
                  return yield* new BrowserSessionError({
                    reason: "provider",
                    dispatch: "possibly-dispatched",
                    cleanup: "not-requested",
                  });

                return yield* Effect.tryPromise({
                  try: () => action(page),
                  catch: () =>
                    new BrowserSessionError({
                      reason: "provider",
                      dispatch: "possibly-dispatched",
                      cleanup: "not-requested",
                    }),
                });
              }),
            ),
          ),
        fillCredential: unused,
        handoff: unused,
        getLiveView: unused,
        getReadOnlyLiveView: unused,
        getHandoffState: unused,
      };

      let state: Control = emptyControl;
      let created = 0;
      let closed = 0;
      let detached = 0;
      let failedAttachments = 0;

      const modelConfig: Parameters<typeof makeOwner>[1] = {
        model: "test-model",
        apiKey: "test-key-not-a-credential",
        apiUrl: "https://model.test/v1",
        apiType: "responses",
        jevApiKey: "test-jev-key",
        models: [
          {
            model: "gpt-6-luna",
            label: "Luna",
            apiKey: "test-selected-key",
            apiUrl: "https://selected-model.test/v1",
            apiType: "responses",
          },
        ],
      };

      const owner = makeOwner(
        {
          read: () => state,
          write: (value) => {
            state = value;
          },
          alarm: () => Effect.void,
        },
        modelConfig,
      );

      const services = BrowserSessions.of({
        createAttached: unused,
        create: (_, retain) =>
          Effect.sync(() => {
            created++;
          }).pipe(Effect.andThen(retain(reference)), Effect.as(reference)),
        attach: () =>
          failedAttachments-- > 0
            ? Effect.fail(
                new BrowserSessionError({
                  reason: "provider",
                  dispatch: "not-dispatched",
                  cleanup: "not-requested",
                }),
              )
            : Effect.acquireRelease(Effect.succeed(session), () =>
                Effect.sync(() => {
                  detached++;
                }),
              ),
        close: () =>
          Effect.sync(() => {
            closed++;
          }),
        keepAlive: () => Effect.void,
      });

      yield* Effect.gen(function* () {
        for (const scenario of scenarios) {
          const report = yield* owner.run(request(scenario.id));

          assert.strictEqual(report.status, "passed", report.message);
          assert.strictEqual(report.cleanup, "closed");
          assert.isNotNull(report.verifiedAt);
          assert.isNotNull(report.readyAt);
          assert.isTrue(
            report.spans
              .filter((span) => span.phase === "action")
              .every((span) => span.start >= (report.readyAt ?? Infinity)),
          );
          assert.isAbove(report.firstActionAt ?? 0, 0);
          assert.strictEqual(report.spans.filter((span) => span.phase === "model").length, 0);
          assert.isTrue(report.spans.every((span) => span.duration !== null));
          if (scenario.id === "create")
            assert.deepStrictEqual(report.board.at(-1), {
              id: 5,
              title: "Ship demo",
              assignee: "Alex",
              priority: "High",
              status: "Todo",
            });
          if (scenario.id === "triage")
            assert.deepStrictEqual(
              report.board
                .filter((task) => task.assignee === "Sam")
                .map((task) => [task.title, task.priority, task.status]),
              [
                ["Review onboarding", "High", "Doing"],
                ["Update help center", "High", "Doing"],
              ],
            );
          if (scenario.id === "batch")
            assert.deepStrictEqual(
              report.board.slice(4).map((task) => [task.title, task.status]),
              [
                ["Write launch notes", "Done"],
                ["Record demo", "Todo"],
                ["Publish release", "Todo"],
              ],
            );
        }
        assert.strictEqual(created, 3);
        assert.strictEqual(closed, 3);
        assert.strictEqual(detached, 3);
        failedAttachments = 1;
        const first = yield* owner.run(request("create", "warm"));
        const second = yield* owner.run(request("create", "warm"));

        assert.strictEqual(first.status, "passed");
        assert.strictEqual(second.status, "passed");
        assert.strictEqual(second.board.length, 5, "Every run must start with a fresh fixture");
        assert.strictEqual(created, 6, "One fresh browser retry, then another independent run");
        assert.strictEqual(closed, 6);
        assert.strictEqual(
          first.spans.filter((span) => span.name === "Attach to browser").length,
          2,
        );
        assert.isTrue(
          first.spans.some((span) => span.outcome === "failure"),
          "Keep failed preparation evidence",
        );
        assert.strictEqual(second.cleanup, "closed");
        assert.isUndefined(
          second.input.temperature,
          "Legacy warm requests normalize to automatic preparation",
        );
        failAfterReady = true;
        const actionFailure = yield* owner.run(request("create"));

        assert.strictEqual(actionFailure.status, "failed");
        assert.isNotNull(actionFailure.readyAt);
        assert.strictEqual(created, 7, "Never retry a failure after the flow has started");
        assert.strictEqual(closed, 7);
        failAfterReady = false;

        const actions: ReadonlyArray<Action> = [
          { kind: "click", ref: "new-task" },
          { kind: "fill", ref: "title", value: "Ship demo" },
          { kind: "select", ref: "assignee", value: "Alex" },
          { kind: "select", ref: "priority", value: "High" },
          { kind: "select", ref: "status", value: "Todo" },
          { kind: "click", ref: "save" },
        ];

        // Replace only provider transport; native Effect AI decoding, tools, agent loop and Chrome run.
        for (const [mode, apiType, grounding] of [
          ["agent", "responses", "direct"],
          ["batched", "responses", "direct"],
          ["agent", "chat-completions", "direct"],
          ["batched", "chat-completions", "direct"],
          ["batched", "responses", "jev"],
        ] as const) {
          modelConfig.apiType = apiType;
          modelConfig.model = apiType === "chat-completions" ? "@cf/test-model" : "test-model";

          const requestedActions =
            grounding === "jev"
              ? actions.map((action) => ({
                  kind: action.kind,
                  target: action.ref === "new-task" ? "New task button" : action.ref,
                  ...(action.kind === "click" ? {} : { value: action.value }),
                }))
              : actions;

          const calls =
            mode === "agent"
              ? requestedActions.map((action) => ({ action }))
              : [{ actions: requestedActions.slice(0, 1) }, { actions: requestedActions.slice(1) }];

          let ordinal = 0;
          let decisionCalls = 0;

          const report = yield* owner
            .run({
              ...request("create"),
              mode,
              grounding,
              ...(grounding === "jev"
                ? {
                    model: "gpt-6-luna" as const,
                    reasoning: "max" as const,
                    serviceTier: "default" as const,
                  }
                : {}),
            })
            .pipe(
              Effect.provideService(FetchHttpClient.Fetch, async (url, init) => {
                const endpoint =
                  typeof url === "string" ? url : url instanceof URL ? url.href : url.url;

                if (endpoint === "https://api.typesafe.ai/v1/systemone") {
                  const body = Schema.decodeUnknownSync(
                    Schema.fromJsonString(
                      Schema.Struct({
                        model: Schema.String,
                        questions: Schema.Record(
                          Schema.String,
                          Schema.Struct({ criteria: Schema.Record(Schema.String, Schema.String) }),
                        ),
                      }),
                    ),
                  )(
                    init?.body instanceof Uint8Array
                      ? new TextDecoder().decode(init.body)
                      : init?.body,
                  );

                  assert.strictEqual(body.model, "jev-latest");
                  const expected = decisionCalls++ === 0 ? actions.slice(0, 1) : actions.slice(1);

                  assert.strictEqual(Object.keys(body.questions).length, expected.length);

                  return new Response(
                    JSON.stringify({
                      model: "jev-latest",
                      answers: Object.fromEntries(
                        expected.map((action, index) => {
                          const question = body.questions[`element_${index}`];

                          assert.isDefined(question?.criteria[action.ref]);

                          return [
                            `element_${index}`,
                            {
                              type: "choice",
                              choice: action.ref,
                              confidence: 1,
                              probabilities: Object.fromEntries(
                                Object.keys(question?.criteria ?? {}).map((ref) => [
                                  ref,
                                  ref === action.ref ? 1 : 0,
                                ]),
                              ),
                            },
                          ];
                        }),
                      ),
                      usage: { input_tokens: 100, output_tokens: 5 },
                    }),
                    { headers: { "content-type": "application/json" } },
                  );
                }
                assert.strictEqual(
                  endpoint,
                  `https://${grounding === "jev" ? "selected-model" : "model"}.test/v1/${apiType === "responses" ? "responses" : "chat/completions"}`,
                );
                if (grounding === "jev")
                  assert.strictEqual(
                    new Headers(init?.headers).get("authorization"),
                    "Bearer test-selected-key",
                  );
                if (apiType === "responses") {
                  const body = Schema.decodeUnknownSync(
                    Schema.fromJsonString(
                      Schema.Struct({
                        service_tier: Schema.String,
                        reasoning: Schema.Struct({ effort: Schema.String, summary: Schema.String }),
                        max_output_tokens: Schema.Number,
                      }),
                    ),
                  )(
                    init?.body instanceof Uint8Array
                      ? new TextDecoder().decode(init.body)
                      : init?.body,
                  );

                  assert.strictEqual(body.service_tier, grounding === "jev" ? "default" : "fast");
                  assert.strictEqual(body.reasoning.effort, grounding === "jev" ? "max" : "none");
                  assert.strictEqual(body.reasoning.summary, "auto");
                  assert.strictEqual(body.max_output_tokens, 16_384);
                }
                const params = calls[ordinal++];
                const toolName = params === undefined ? "finish" : "act";
                const argumentsJson = JSON.stringify(params ?? { message: "Saved the task." });

                if (apiType === "chat-completions") {
                  const requestBody = Schema.decodeUnknownSync(
                    Schema.fromJsonString(
                      Schema.Struct({
                        model: Schema.String,
                        tools: Schema.Array(
                          Schema.Struct({ function: Schema.Struct({ name: Schema.String }) }),
                        ),
                        messages: Schema.Array(
                          Schema.Struct({
                            role: Schema.String,
                            content: Schema.String,
                            tool_calls: Schema.optionalKey(
                              Schema.Array(
                                Schema.Struct({
                                  id: Schema.String,
                                  function: Schema.Struct({
                                    name: Schema.String,
                                    arguments: Schema.String,
                                  }),
                                }),
                              ),
                            ),
                            tool_call_id: Schema.optionalKey(Schema.String),
                          }),
                        ),
                      }),
                    ),
                  )(
                    init?.body instanceof Uint8Array
                      ? new TextDecoder().decode(init.body)
                      : init?.body,
                  );

                  assert.strictEqual(requestBody.model, "@cf/test-model");
                  assert.deepStrictEqual(
                    requestBody.tools.map((tool) => tool.function.name),
                    ["finish", "observe", "act"],
                  );
                  assert.deepStrictEqual(
                    requestBody.messages.flatMap(
                      (message) => message.tool_calls?.map((call) => call.id) ?? [],
                    ),
                    calls.slice(0, ordinal - 1).map((_, index) => `call_${index + 1}`),
                    "Cloudflare content normalization must preserve tool-call identity across turns",
                  );
                  assert.deepStrictEqual(
                    requestBody.messages.flatMap((message) =>
                      message.tool_call_id ? [message.tool_call_id] : [],
                    ),
                    calls.slice(0, ordinal - 1).map((_, index) => `call_${index + 1}`),
                  );

                  const events = [
                    {
                      id: `chat_${ordinal}`,
                      object: "chat.completion.chunk",
                      created: 1,
                      model: "resolved-test-model",
                      choices: [
                        {
                          index: 0,
                          delta: {
                            role: "assistant",
                            tool_calls: [
                              {
                                index: 0,
                                id: `call_${ordinal}`,
                                type: "function",
                                function: { name: toolName, arguments: argumentsJson },
                              },
                            ],
                          },
                          finish_reason: null,
                        },
                      ],
                    },
                    {
                      id: `chat_${ordinal}`,
                      object: "chat.completion.chunk",
                      created: 1,
                      model: "resolved-test-model",
                      choices: [
                        {
                          index: 0,
                          delta: {},
                          finish_reason: "tool_calls",
                        },
                      ],
                      usage: { prompt_tokens: 12, completion_tokens: 3, total_tokens: 15 },
                    },
                  ];

                  return new Response(
                    events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join("") +
                      "data: [DONE]\n\n",
                    { headers: { "content-type": "text/event-stream" } },
                  );
                }

                const item = {
                  type: "function_call",
                  id: `fc_${ordinal}`,
                  call_id: `call_${ordinal}`,
                  name: toolName,
                  arguments: argumentsJson,
                  status: "completed",
                };

                const events = [
                  {
                    type: "response.created",
                    response: {
                      id: `resp_${ordinal}`,
                      object: "response",
                      model: "resolved-test-model",
                      created_at: 1,
                      status: "in_progress",
                      output: [],
                    },
                  },
                  { type: "response.output_item.added", output_index: 0, item },
                  {
                    type: "response.function_call_arguments.done",
                    output_index: 0,
                    item_id: item.id,
                    arguments: argumentsJson,
                  },
                  { type: "response.output_item.done", output_index: 0, item },
                  {
                    type: "response.completed",
                    response: {
                      id: `resp_${ordinal}`,
                      object: "response",
                      model: "resolved-test-model",
                      created_at: 1,
                      status: "completed",
                      service_tier: grounding === "jev" ? "default" : "fast",
                      output: [item],
                      usage: {
                        input_tokens: 12,
                        output_tokens: 3,
                        total_tokens: 15,
                        input_tokens_details: { cached_tokens: 0 },
                        output_tokens_details: { reasoning_tokens: 2 },
                      },
                    },
                  },
                ];

                return new Response(
                  events
                    .map(
                      (event, sequence_number) =>
                        `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
                    )
                    .join(""),
                  { headers: { "content-type": "text/event-stream" } },
                );
              }),
            );

          assert.strictEqual(report.status, "passed", report.message);
          if (grounding === "jev") assert.strictEqual(report.model, "gpt-6-luna");
          assert.strictEqual(report.spans.filter((span) => span.phase === "action").length, 6);
          assert.strictEqual(decisionCalls, grounding === "jev" ? 2 : 0);
          const decisionSpans = report.spans.filter((span) => span.phase === "decision");

          assert.strictEqual(decisionSpans.length, grounding === "jev" ? 2 : 0);
          if (grounding === "jev") {
            assert.deepStrictEqual(
              decisionSpans.flatMap((span) => span.choices?.map((choice) => choice.ref) ?? []),
              ["new-task", "title", "assignee", "priority", "status", "save"],
            );
            assert.isTrue(
              decisionSpans.every(
                (span) =>
                  span.model === "jev-latest" &&
                  span.inputTokens === 100 &&
                  span.outcome === "success",
              ),
            );
          }
          const modelSpans = report.spans.filter((span) => span.phase === "model");

          if (apiType === "responses") {
            assert.isTrue(
              modelSpans.every(
                (span) => span.serviceTier === (grounding === "jev" ? "default" : "fast"),
              ),
            );
            assert.isTrue(modelSpans.every((span) => span.reasoningTokens === 2));
            assert.strictEqual(report.input.reasoning, grounding === "jev" ? "max" : "none");
          }

          assert.strictEqual(modelSpans.length, mode === "agent" ? 7 : 3);
          assert.isTrue(
            modelSpans.every((span) => !span.name.includes("jev")),
            "Decision provider metadata must not replace planner identity",
          );
          assert.deepStrictEqual(
            modelSpans.map((span) => [span.model, span.inputTokens, span.outputTokens]),
            modelSpans.map(() => ["resolved-test-model", 12, 3]),
          );
        }
      }).pipe(Effect.provideService(BrowserSessions, services));

      const trace = yield* makeTrace(request("create"), "none");

      const browser = yield* makeBrowser(session, false, () => {}).pipe(
        Effect.provideService(Trace, trace),
      );

      yield* browser.prepare;
      yield* browser.observe();
      yield* browser.act([{ kind: "click", ref: "new-task" }]);

      const partial = yield* browser.act([
        { kind: "fill", ref: "title", value: "Do not save" },
        { kind: "click", ref: "not-observed" },
        { kind: "click", ref: "save" },
      ]);

      assert.strictEqual(partial.completed, 1);
      assert.include(partial.error ?? "", "not observed");
      assert.strictEqual(
        partial.observation?.controls.find((value) => value.ref === "title")?.value,
        "Do not save",
      );
      assert.deepStrictEqual(yield* browser.readBoard, seed);

      let kitesurfCloses = 0;
      const chromiumAllocations = created;

      modelConfig.kitesurf = (retainClose) =>
        Effect.sync(() => {
          retainClose(
            Effect.sync(() => {
              kitesurfCloses++;
            }),
          );

          return {
            ...session,
            identity: {
              product: "Chromium fixture",
              revision: "@chromium-fixture",
              userAgent: "Chromium fixture",
            },
          };
        });

      const mismatched = yield* owner
        .run({ ...request("create"), engine: "kitesurf" })
        .pipe(Effect.provideService(BrowserSessions, services));

      assert.strictEqual(mismatched.status, "failed", "Chromium must not be measured as Kitesurf");
      assert.isNull(mismatched.readyAt);
      assert.strictEqual(mismatched.cleanup, "closed");
      assert.strictEqual(kitesurfCloses, 1);
      assert.strictEqual(
        created,
        chromiumAllocations,
        "Kitesurf must not allocate a persistent Chromium session",
      );
      assert.isTrue(mismatched.spans.every((span) => span.phase !== "action"));
    }),
  { timeout: 60_000 },
);

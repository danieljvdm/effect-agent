import {
  BrowserSessionError,
  BrowserSessionReference,
  type BrowserSession,
} from "@effect-agent/platform-cloudflare/browser-session";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { assert, expectTypeOf, it } from "@effect/vitest";
import type { Scope } from "effect";
import { Config, Effect, Exit, Fiber, Layer, Option, Redacted, Schema } from "effect";
import { InMemory } from "effect-agent";
import { DecisionModel } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import puppeteer from "puppeteer-core";
import browserPuppeteer from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { makeBrowser, Browser } from "../src/browser.ts";
import { defaultChallenge, LabError, racePrompt, type RunInput } from "../src/contract.ts";
import { executeTask } from "../src/runner.ts";
import { cohort, comparisons } from "../src/state.ts";
import { makeTrace, Trace } from "../src/telemetry.ts";
import {
  articleTitle,
  articleUrl,
  makeWikipedia,
  runWikipedia,
  Wikipedia,
} from "../src/wikipedia.ts";

expectTypeOf<Effect.Error<ReturnType<typeof makeWikipedia>>>().toEqualTypeOf<LabError>();
expectTypeOf<Effect.Services<ReturnType<typeof makeWikipedia>>>().toEqualTypeOf<
  Scope.Scope | Browser | Trace
>();
expectTypeOf<
  Effect.Services<ReturnType<Effect.Success<ReturnType<typeof makeWikipedia>>["follow"]>>
>().toEqualTypeOf<never>();

const request = (): RunInput => ({
  id: crypto.randomUUID(),
  scenario: "wikipedia",
  mode: "agent",
  temperature: "cold",
  prompt: racePrompt(defaultChallenge),
  screenshots: false,
  liveView: false,
  wikipedia: defaultChallenge,
});

it.effect(
  "keeps races with different endpoints out of the same speed cohort and excludes namespace/URL shortcuts",
  () =>
    Effect.gen(function* () {
      for (const url of [
        "https://en.wikipedia.org/wiki/Special:Search",
        "https://en.wikipedia.org/wiki/Talk:Mars",
        "https://en.wikipedia.org/wiki/Mars#History",
        "https://en.wikipedia.org/wiki/Mars?search=x",
        "https://evil.test/wiki/Mars",
        "https://en.wikipedia.org/w/index.php?title=Mars",
        "https://en.wikipedia.org/wiki/%3AFile%3Ax",
        "https://en.wikipedia.org/wiki/%ZZ",
      ])
        assert.isUndefined(articleTitle(url), url);
      assert.strictEqual(
        articleTitle("https://en.wikipedia.org/wiki/Nelson_Mandela"),
        "Nelson Mandela",
      );
      assert.strictEqual(articleTitle(articleUrl("AC/DC")), "AC/DC");
      const trace = yield* makeTrace(request(), "gpt-6-luna");

      trace.update({ status: "passed", verifiedAt: 10 });
      const report = trace.snapshot();

      const samples = [
        report,
        {
          ...report,
          input: { ...report.input, wikipedia: { start: "Earth", target: "Nelson Mandela" } },
        },
        { ...report, input: { ...report.input, wikipedia: { start: "Mars", target: "Earth" } } },
      ].map((report) => ({ report, clientElapsedMillis: 20 }));

      assert.strictEqual(cohort(samples, report).length, 1);
      assert.strictEqual(comparisons(samples, report)[0]?.count, 1);

      const driverSamples = [
        report,
        { ...report, input: { ...report.input, wikiDriver: "jev" as const } },
      ].map((report) => ({ report, clientElapsedMillis: 20 }));

      assert.strictEqual(
        cohort(driverSamples, report).length,
        1,
        "Different link visibility and route drivers must not share a cohort",
      );
      assert.strictEqual(comparisons(driverSamples, report).length, 2);
    }),
);

const response = (ordinal: number, parameters: object) => {
  const item = {
    type: "function_call",
    id: `fc_${ordinal}`,
    call_id: `call_${ordinal}`,
    name: "follow",
    arguments: JSON.stringify(parameters),
    status: "completed",
  };

  const base = { id: `resp_${ordinal}`, object: "response", model: "test-model", created_at: 1 };

  const events = [
    { type: "response.created", response: { ...base, status: "in_progress", output: [] } },
    { type: "response.output_item.added", output_index: 0, item },
    {
      type: "response.function_call_arguments.done",
      output_index: 0,
      item_id: item.id,
      arguments: item.arguments,
    },
    { type: "response.output_item.done", output_index: 0, item },
    {
      type: "response.completed",
      response: {
        ...base,
        status: "completed",
        output: [item],
        usage: {
          input_tokens: 20,
          output_tokens: 5,
          total_tokens: 25,
          input_tokens_details: { cached_tokens: 0 },
          output_tokens_details: { reasoning_tokens: 0 },
        },
      },
    },
  ];

  return new Response(
    events
      .map((event, sequence_number) => `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`)
      .join(""),
    { headers: { "content-type": "text/event-stream" } },
  );
};

it.live(
  "clicks real article anchors, verifies redirects, completes directly from arrival, and releases guards on every exit",
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
      const unused = () => Effect.die("Unused browser service");

      const session: BrowserSession = {
        reference: BrowserSessionReference.make({
          version: 1,
          sessionId: Redacted.make(crypto.randomUUID()),
          contextId: Redacted.make("test"),
          targetId: Redacted.make("test"),
          expiresAt: Date.now() + 600_000,
          commandTimeoutMillis: 15_000,
        }),
        run: (authorize, action) =>
          authorize.pipe(
            Effect.andThen(
              Effect.tryPromise({
                try: () => action(page),
                catch: () =>
                  new BrowserSessionError({
                    reason: "provider",
                    dispatch: "possibly-dispatched",
                    cleanup: "not-requested",
                  }),
              }),
            ),
          ),
        fillCredential: unused,
        handoff: unused,
        getLiveView: unused,
        getReadOnlyLiveView: unused,
        getHandoffState: unused,
      };

      let wrongLanding = false;
      let lookupFailure: "unavailable" | "missing" | "malformed" | undefined;
      const requested: Array<string> = [];

      // A lower-priority fixture response preserves the production guard's ability to abort forbidden requests.
      page.on("request", (request) => {
        if (request.isInterceptResolutionHandled()) return;
        const url = new URL(request.url());

        requested.push(url.href);
        if (url.pathname === "/w/api.php") {
          void request.respond(
            {
              contentType: "application/json",
              status: lookupFailure === "unavailable" ? 503 : 200,
              body: JSON.stringify(
                lookupFailure === "malformed"
                  ? {}
                  : {
                      query: {
                        pages: [
                          lookupFailure === "missing"
                            ? { ns: 0, title: "Missing article", missing: true }
                            : { pageid: 1, ns: 0, title: "Nelson Mandela" },
                        ],
                      },
                    },
              ),
            },
            1,
          );

          return;
        }
        if (url.pathname === "/wiki/Madiba") {
          void request.respond(
            { status: 302, headers: { location: articleUrl("Nelson Mandela") } },
            1,
          );

          return;
        }
        const title = articleTitle(url.href) ?? "Invalid";

        const links =
          title === "Mars"
            ? '<a href="/wiki/Earth">Earth</a><a href="/wiki/Special:Search">Search</a><a href="https://example.com/wiki/Nelson_Mandela">External</a><a href="/wiki/Nelson_Mandela#Life">Fragment</a><a style="display:none" href="/wiki/Nelson_Mandela">Hidden</a>' +
              Array.from(
                { length: 85 },
                (_, index) => `<a href="/wiki/Filler_${index}">Filler ${index}</a>`,
              ).join("")
            : title === "Filler 84"
              ? '<a href="/wiki/Earth">Earth</a>'
              : title === "Earth"
                ? '<a href="/wiki/Madiba">Madiba</a>'
                : "";

        void request.respond(
          {
            contentType: "text/html",
            body: `<!doctype html><html><head><link rel="canonical" href="${wrongLanding && title === "Nelson Mandela" ? articleUrl("Earth") : url.href}"></head><body class="ns-0"><h1 id="firstHeading">${title}</h1><div id="mw-content-text"><div class="mw-parser-output"><p>Article about ${title}.</p>${links}</div></div></body></html>`,
          },
          1,
        );
      });
      const listenerCount = page.listenerCount("request");

      for (const failure of ["unavailable", "missing", "malformed"] as const) {
        lookupFailure = failure;
        const setupTrace = yield* makeTrace(request(), "jev-latest");

        const setupBrowser = yield* makeBrowser(session, false, () => {}).pipe(
          Effect.provideService(Trace, setupTrace),
        );

        const result = yield* executeTask({ ...request(), wikiDriver: "jev" }, "unused", "").pipe(
          Effect.provideService(Browser, setupBrowser),
          Effect.provideService(Trace, setupTrace),
          Effect.scoped,
          Effect.result,
        );

        assert.strictEqual(result._tag, "Failure");
        if (result._tag === "Failure")
          assert.strictEqual(result.failure.code, failure === "missing" ? "invalid" : "browser");
        assert.isNull(setupTrace.snapshot().readyAt);
        assert.isFalse(
          setupTrace
            .snapshot()
            .spans.some((span) => ["decision", "model", "action"].includes(span.phase)),
        );
        assert.strictEqual(page.listenerCount("request"), listenerCount);
      }
      lookupFailure = undefined;
      let setupCalls = 0;

      const fenced: BrowserSession = {
        ...session,
        run: (authorize, action) =>
          Effect.suspend(() =>
            ++setupCalls === 1
              ? session.run(authorize, action)
              : Effect.fail(
                  new BrowserSessionError({
                    reason: "provider",
                    dispatch: "not-dispatched",
                    cleanup: "not-requested",
                  }),
                ),
          ),
      };

      const fencedTrace = yield* makeTrace(request(), "jev-latest");

      const fencedBrowser = yield* makeBrowser(fenced, false, () => {}).pipe(
        Effect.provideService(Trace, fencedTrace),
      );

      const fencedFailure = yield* makeWikipedia(defaultChallenge).pipe(
        Effect.provideService(Browser, fencedBrowser),
        Effect.provideService(Trace, fencedTrace),
        Effect.scoped,
        Effect.result,
      );

      assert.strictEqual(
        fencedFailure._tag,
        "Failure",
        "Cleanup refusal must preserve the typed preparation failure",
      );
      assert.strictEqual(
        page.listenerCount("request"),
        listenerCount,
        "Remove the local guard even when the browser refuses commands",
      );
      assert.isTrue(
        fencedTrace
          .snapshot()
          .spans.some(
            (span) => span.name === "Release navigation guard" && span.outcome === "failure",
          ),
      );

      for (const grounded of [false, true]) {
        const trace = yield* makeTrace(request(), "test-model");

        const browser = yield* makeBrowser(session, false, () => {}).pipe(
          Effect.provideService(Trace, trace),
        );

        let modelCalls = 0;
        let decisions = 0;

        const planner = OpenAiLanguageModel.model("test-model").pipe(
          Layer.provide(
            OpenAiClient.layer({
              apiKey: Redacted.make("test-not-a-key"),
              apiUrl: "https://model.test/v1",
            }),
          ),
          Layer.provide(FetchHttpClient.layer),
        );

        const selector = Layer.effect(
          DecisionModel.DecisionModel,
          DecisionModel.make({
            decide: (request) =>
              Effect.sync(() => {
                decisions++;
                const question = request.decisions.element_0;

                assert.strictEqual(question?._tag, "Classify");

                const refs =
                  question?._tag === "Classify"
                    ? Object.keys(question.criteria).filter((key) => key !== "__none__")
                    : [];

                const ref = refs[0] ?? "invalid";

                return {
                  answers: {
                    element_0: {
                      _tag: "Classify",
                      label: ref,
                      probabilities: Object.fromEntries(
                        ["__none__", ...refs].map((key) => [key, key === ref ? 1 : 0]),
                      ),
                    },
                  },
                  usage: { inputTokens: 50, outputTokens: 2 },
                };
              }),
          }),
        );

        yield* Effect.gen(function* () {
          const wiki = yield* makeWikipedia(defaultChallenge);

          const result = yield* runWikipedia(defaultChallenge, grounded).pipe(
            Effect.provideService(Wikipedia, wiki),
            Effect.provide([InMemory.layer, planner, selector]),
            Effect.provideService(FetchHttpClient.Fetch, async () => {
              modelCalls++;
              assert.isAtMost(modelCalls, 2, "Arrival must settle without a third model call");

              return response(
                modelCalls,
                grounded
                  ? { target: modelCalls === 1 ? "Earth" : "Madiba" }
                  : { ref: modelCalls === 1 ? "p1-l0" : "p2-l0" },
              );
            }),
          );

          assert.include(result.output.message, "2 hops");
          assert.deepStrictEqual(
            trace.snapshot().race?.path.map((hop) => hop.title),
            ["Mars", "Earth", "Nelson Mandela"],
          );
          assert.strictEqual(trace.snapshot().status, "passed");
          assert.strictEqual(trace.snapshot().race?.path.at(-1)?.via?.url, articleUrl("Madiba"));
          assert.strictEqual(decisions, grounded ? 2 : 0);
        }).pipe(
          Effect.provideService(Browser, browser),
          Effect.provideService(Trace, trace),
          Effect.scoped,
        );
        assert.strictEqual(
          page.listenerCount("request"),
          listenerCount,
          "Navigation guard must detach after completion",
        );
      }

      const jevInput = { ...request(), wikiDriver: "jev" as const };
      const routeTrace = yield* makeTrace(jevInput, "jev-latest");
      let routeCalls = 0;

      const routeBrowser = yield* makeBrowser(session, false, () => {}).pipe(
        Effect.provideService(Trace, routeTrace),
      );

      yield* executeTask(
        jevInput,
        "unused-planner",
        "",
        undefined,
        "responses",
        "test-not-a-key",
      ).pipe(
        Effect.provideService(Browser, routeBrowser),
        Effect.provideService(Trace, routeTrace),
        Effect.provideService(FetchHttpClient.Fetch, async (url, init) => {
          const endpoint = typeof url === "string" ? url : url instanceof URL ? url.href : url.url;

          assert.strictEqual(
            endpoint,
            "https://api.typesafe.ai/v1/systemone",
            "Jev-only must never call a planner API",
          );

          const body = Schema.decodeUnknownSync(
            Schema.fromJsonString(
              Schema.Struct({
                state: Schema.Struct({
                  current: Schema.String,
                  destination: Schema.String,
                  path: Schema.Array(Schema.String),
                }),
                questions: Schema.Record(
                  Schema.String,
                  Schema.Struct({ criteria: Schema.Record(Schema.String, Schema.String) }),
                ),
              }),
            ),
          )(init?.body instanceof Uint8Array ? new TextDecoder().decode(init.body) : init?.body);

          const expected = ["Filler 84", "Earth", "Madiba"][routeCalls++];

          assert.strictEqual(body.state.destination, "Nelson Mandela");
          assert.strictEqual(body.state.path.length, routeCalls);
          const criteria = body.questions.route_0?.criteria ?? {};

          const choice = Object.entries(criteria).find(([, description]) =>
            description.startsWith(`${expected} |`),
          );

          assert.isDefined(choice, "A useful link beyond the planner's first 80 must reach Jev");
          const ref = choice?.[0] ?? "invalid";

          return new Response(
            JSON.stringify({
              model: "jev-latest",
              answers: {
                route_0: {
                  type: "choice",
                  choice: ref,
                  confidence: 1,
                  probabilities: Object.fromEntries(
                    Object.keys(criteria).map((key) => [key, key === ref ? 0.99 : 0]),
                  ),
                },
              },
              usage: { input_tokens: 80, output_tokens: 1 },
            }),
            { headers: { "content-type": "application/json" } },
          );
        }),
        Effect.scoped,
      );
      assert.strictEqual(routeCalls, 1, "The later fixture pages each have only one eligible link");
      assert.deepStrictEqual(
        routeTrace.snapshot().race?.path.map((hop) => hop.title),
        ["Mars", "Filler 84", "Earth", "Nelson Mandela"],
      );
      assert.strictEqual(routeTrace.snapshot().status, "passed");
      assert.isNotNull(routeTrace.snapshot().readyAt);
      assert.isTrue(
        routeTrace
          .snapshot()
          .spans.filter((span) => ["decision", "action"].includes(span.phase))
          .every((span) => span.start >= (routeTrace.snapshot().readyAt ?? Infinity)),
      );
      assert.strictEqual(
        routeTrace.snapshot().spans.filter((span) => span.phase === "model").length,
        0,
      );
      assert.strictEqual(
        page.listenerCount("request"),
        listenerCount,
        "Jev-only must release its navigation guard",
      );

      const trace = yield* makeTrace(request(), "test-model");

      const browser = yield* makeBrowser(session, false, () => {}).pipe(
        Effect.provideService(Trace, trace),
      );

      yield* Effect.gen(function* () {
        const wiki = yield* makeWikipedia(defaultChallenge);

        assert.strictEqual(
          wiki.initial.totalLinks,
          86,
          "Forbidden and hidden links must not be candidates",
        );
        assert.strictEqual(wiki.initial.nextOffset, 80);
        yield* wiki.read(80);
        const stale = yield* wiki.follow("p1-l0").pipe(Effect.result);

        assert.strictEqual(stale._tag, "Failure");
        assert.strictEqual(page.url(), articleUrl("Mars"));
        yield* wiki.read(0);
        // Real Wikipedia inserts links while hydrating. Preserve the observed anchor
        // capability even when unrelated links shift its original DOM position.
        yield* Effect.promise(() =>
          page.evaluate(() => {
            const link = document.createElement("a");

            link.href = "/wiki/Unexpected";
            link.textContent = "Inserted by page layout";
            const body = document.querySelector<HTMLElement>(".mw-parser-output");

            body?.insertBefore(link, body.firstChild);
          }),
        );
        yield* wiki.follow("p1-l0");
        assert.strictEqual(page.url(), articleUrl("Earth"));
        const oldPage = yield* wiki.follow("p1-l0").pipe(Effect.result);

        assert.strictEqual(oldPage._tag, "Failure");
        assert.strictEqual(trace.snapshot().verifiedAt, null);
        wrongLanding = true;
        const falseWin = yield* wiki.follow("p2-l0").pipe(Effect.result);

        assert.strictEqual(falseWin._tag, "Failure");
        assert.strictEqual(
          trace.snapshot().verifiedAt,
          null,
          "URL alone cannot override a mismatched canonical page",
        );
        wrongLanding = false;
      }).pipe(
        Effect.provideService(Browser, browser),
        Effect.provideService(Trace, trace),
        Effect.scoped,
      );
      assert.strictEqual(page.listenerCount("request"), listenerCount);

      // 894818e5 silently omitted a destination after anchor 10,000 for planner runs.
      for (const fullLinks of [false, true]) {
        yield* Effect.gen(function* () {
          const wiki = yield* makeWikipedia(defaultChallenge, fullLinks);

          yield* Effect.promise(() =>
            page.$eval(".mw-parser-output", (body) => {
              body.innerHTML =
                '<a href="/wiki/Earth">Earth</a>'.repeat(10_000) +
                '<a href="/wiki/Nelson_Mandela">Nelson Mandela</a>';
            }),
          );
          const result = yield* wiki.read().pipe(Effect.result);

          assert.strictEqual(
            result._tag,
            "Failure",
            "An incomplete link set must never be presented as complete",
          );
          if (result._tag === "Failure") {
            assert.strictEqual(result.failure.code, "invalid");
            assert.include(result.failure.message, "10,000-anchor");
          }
        }).pipe(
          Effect.provideService(Browser, browser),
          Effect.provideService(Trace, trace),
          Effect.scoped,
        );
        assert.strictEqual(page.listenerCount("request"), listenerCount);
      }

      for (const exit of ["failure", "defect", "timeout", "interruption"] as const) {
        const work = Effect.gen(function* () {
          yield* makeWikipedia(defaultChallenge);
          if (exit === "failure")
            return yield* new LabError({ code: "browser", message: "Expected failure" });
          if (exit === "defect") return yield* Effect.die("Expected defect");

          return yield* Effect.never;
        }).pipe(
          Effect.provideService(Browser, browser),
          Effect.provideService(Trace, trace),
          Effect.scoped,
        );

        if (exit === "interruption") {
          const fiber = yield* Effect.forkChild(work);

          yield* Effect.sleep("250 millis");
          yield* Fiber.interrupt(fiber);
        } else {
          const result = yield* work.pipe(Effect.timeout("500 millis"), Effect.exit);

          assert.isTrue(Exit.isFailure(result));
        }
        assert.strictEqual(
          page.listenerCount("request"),
          listenerCount,
          `${exit} must release interception`,
        );
      }
      assert.isFalse(
        requested.some((url) => url.includes("example.com")),
        "No external article may be clicked",
      );
    }),
  { timeout: 45_000 },
);

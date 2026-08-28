import {
  CapturePageMarkdown,
  CapturePageScrape,
  CapturePageStructured,
  PageCapture,
  PageCaptureLimits,
  PageCaptureRequest,
  PageUrlTarget,
} from "@effect-agent/sandbox";
import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Redacted } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionRpcError,
  BrowserQuickActionWorkersAi,
  BrowserQuickActionWorkersAiPolicyError,
  browserQuickActionWorkersAiCaptureLayer,
  type BrowserQuickActionClient,
} from "../src/browser-quick-action.ts";
import { browserRestWorkersAiCaptureLayer } from "../src/browser-rest-capture.ts";

const markdownRequest = (limit = 1_024) =>
  PageCaptureRequest.make({
    target: PageUrlTarget.make({ url: "https://docs.example.com/page" }),
    action: CapturePageMarkdown.make({}),
    engine: "chromium",
    limits: PageCaptureLimits.make({ maxOutputBytes: limit }),
  });

const structuredRequest = PageCaptureRequest.make({
  target: PageUrlTarget.make({ url: "https://docs.example.com/page" }),
  action: CapturePageStructured.make({ responseFormat: { type: "object" } }),
  engine: "chromium",
  limits: PageCaptureLimits.make({ maxOutputBytes: 1_024 }),
});

const scrapeRequest = (limit = 1024 * 1024) =>
  PageCaptureRequest.make({
    target: PageUrlTarget.make({ url: "https://docs.example.com/page" }),
    action: CapturePageScrape.make({ selectors: [".plan", "#faq"] }),
    engine: "chromium",
    limits: PageCaptureLimits.make({ maxOutputBytes: limit }),
  });

const scrapeElement = {
  text: "Pro",
  html: '<article class="plan">Pro</article>',
  attributes: [{ name: "class", value: "plan" }],
  left: 10,
  top: 20,
  width: 300,
  height: 120,
};

const json = (body: unknown, init?: ResponseInit) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json" }, ...init });

interface Adapter {
  readonly name: string;
  readonly layer: (
    responses: ReadonlyArray<Response>,
    denied?: boolean,
  ) => {
    readonly layer: Layer.Layer<PageCapture>;
    readonly calls: () => number;
  };
}

const adapters: ReadonlyArray<Adapter> = [
  {
    name: "Worker binding",
    layer: (responses, denied) => {
      let index = 0;
      const invoke =
        (action: "screenshot" | "content" | "markdown" | "links" | "scrape" | "json") => () => {
          const response = responses[index++];
          return response === undefined
            ? Effect.fail(
                BrowserQuickActionRpcError.make({ action, cause: new Error("missing response") }),
              )
            : Effect.succeed(response);
        };
      const binding: BrowserQuickActionClient = {
        screenshot: invoke("screenshot"),
        content: invoke("content"),
        markdown: invoke("markdown"),
        links: invoke("links"),
        scrape: invoke("scrape"),
        json: invoke("json"),
      };
      const authorities = Layer.merge(
        Layer.succeed(BrowserQuickActionBrowserBinding)(binding),
        BrowserQuickActionWorkersAi.layer({
          authorizeAndAccount: () =>
            denied
              ? Effect.fail(
                  BrowserQuickActionWorkersAiPolicyError.make({
                    reason: "authorization",
                    message: "denied",
                  }),
                )
              : Effect.void,
        }),
      );
      return {
        layer: browserQuickActionWorkersAiCaptureLayer().pipe(Layer.provide(authorities)),
        calls: () => index,
      };
    },
  },
  {
    name: "REST",
    layer: (responses, denied) => {
      let index = 0;
      const client = HttpClient.make((request) => {
        const response = responses[index++];
        return response === undefined
          ? Effect.die("missing response")
          : Effect.succeed(HttpClientResponse.fromWeb(request, response));
      });
      const authorities = Layer.merge(
        Layer.succeed(HttpClient.HttpClient)(client),
        BrowserQuickActionWorkersAi.layer({
          authorizeAndAccount: () =>
            denied
              ? Effect.fail(
                  BrowserQuickActionWorkersAiPolicyError.make({
                    reason: "authorization",
                    message: "denied",
                  }),
                )
              : Effect.void,
        }),
      );
      return {
        layer: browserRestWorkersAiCaptureLayer({
          accountId: "account",
          apiToken: Redacted.make("token"),
        }).pipe(Layer.provide(authorities)),
        calls: () => index,
      };
    },
  },
];

const capture = (layer: Layer.Layer<PageCapture>, request: PageCaptureRequest) =>
  Effect.gen(function* () {
    return yield* (yield* PageCapture).capture(request);
  }).pipe(Effect.provide(layer));

describe.each(adapters)("PageCapture adapter contract: $name", (adapter) => {
  it.effect("projects a Markdown action and decodes a bounded success", () =>
    Effect.gen(function* () {
      const harness = adapter.layer([json({ success: true, result: "# Page" })]);
      const result = yield* capture(harness.layer, markdownRequest());
      expect(result.output).toMatchObject({ _tag: "PageMarkdownCaptured", markdown: "# Page" });
      expect(harness.calls()).toBe(1);
    }),
  );

  it.effect("decodes a grouped selector scrape success", () =>
    Effect.gen(function* () {
      const harness = adapter.layer([
        json({
          success: true,
          result: [
            { selector: ".plan", results: [scrapeElement] },
            { selector: "#faq", results: [] },
          ],
        }),
      ]);
      const result = yield* capture(harness.layer, scrapeRequest());
      expect(result.output).toMatchObject({
        _tag: "PageScrapeCaptured",
        groups: [
          { selector: ".plan", results: [{ text: "Pro", width: 300 }] },
          { selector: "#faq", results: [] },
        ],
      });
      expect(harness.calls()).toBe(1);
    }),
  );

  it.effect("rejects malformed and over-count selector scrape responses", () =>
    Effect.gen(function* () {
      const malformed = [{ selector: ".plan", results: [{ ...scrapeElement, width: "wide" }] }];
      const aggregateOverflow = [
        { selector: ".plan", results: Array.from({ length: 2_048 }, () => scrapeElement) },
        { selector: "#faq", results: Array.from({ length: 2_049 }, () => scrapeElement) },
      ];
      const attributeOverflow = [
        {
          selector: ".plan",
          results: [
            {
              ...scrapeElement,
              attributes: Array.from({ length: 65 }, (_, index) => ({
                name: `data-${String(index)}`,
                value: "x",
              })),
            },
          ],
        },
      ];
      for (const result of [malformed, aggregateOverflow, attributeOverflow]) {
        const error = yield* capture(
          adapter.layer([json({ success: true, result })]).layer,
          scrapeRequest(),
        ).pipe(Effect.flip);
        expect(error._tag).toBe("PageCaptureProtocolError");
      }
    }),
  );

  it.effect("rejects a selector scrape response over its aggregate byte budget", () =>
    Effect.gen(function* () {
      const error = yield* capture(
        adapter.layer([
          json({
            success: true,
            result: [
              { selector: ".plan", results: [{ ...scrapeElement, text: "x".repeat(2_048) }] },
            ],
          }),
        ]).layer,
        scrapeRequest(1_024),
      ).pipe(Effect.flip);
      expect(error._tag).toBe("PageCaptureOutputLimitError");
    }),
  );

  it.effect("keeps malformed, provider failure, rate, and quota responses typed", () =>
    Effect.gen(function* () {
      for (const [response, tag] of [
        [json({ success: true, nope: true }), "PageCaptureProtocolError"],
        [json({ success: false, errors: [{ message: "nope" }] }), "PageCaptureNavigationError"],
        [
          json(
            { success: false, errors: [{ message: "rate" }] },
            { status: 429, headers: { "retry-after": "7" } },
          ),
          "PageCaptureRateLimitedError",
        ],
        [
          json({ success: false, errors: [{ message: "daily quota" }] }, { status: 429 }),
          "PageCaptureRateLimitedError",
        ],
      ] as const) {
        const exit = yield* capture(adapter.layer([response]).layer, markdownRequest()).pipe(
          Effect.exit,
        );
        expect(String(exit)).toContain(tag);
      }
    }),
  );

  it.effect("distinguishes rate and quota failures and preserves a safe Retry-After hint", () =>
    Effect.gen(function* () {
      const rate = yield* capture(
        adapter.layer([
          json(
            { success: false, errors: [{ message: "rate" }] },
            { status: 429, headers: { "retry-after": "7" } },
          ),
        ]).layer,
        markdownRequest(),
      ).pipe(Effect.flip);
      expect(rate).toMatchObject({
        _tag: "PageCaptureRateLimitedError",
        reason: "rate",
        retryAfterMillis: 7_000,
      });
      const quota = yield* capture(
        adapter.layer([
          json({ success: false, errors: [{ message: "daily quota" }] }, { status: 429 }),
        ]).layer,
        markdownRequest(),
      ).pipe(Effect.flip);
      expect(quota).toMatchObject({ _tag: "PageCaptureRateLimitedError", reason: "quota" });
    }),
  );

  it.effect("stops oversized streams at the first violating chunk and finalizes them", () =>
    Effect.gen(function* () {
      let reads = 0;
      let cancelled = false;
      const stream = new ReadableStream<Uint8Array>(
        {
          pull(controller) {
            reads += 1;
            controller.enqueue(new Uint8Array(512));
          },
          cancel() {
            cancelled = true;
          },
        },
        { highWaterMark: 0 },
      );
      const exit = yield* capture(
        adapter.layer([new Response(stream, { headers: { "content-type": "application/json" } })])
          .layer,
        markdownRequest(1_024),
      ).pipe(Effect.exit);
      expect(String(exit)).toContain("PageCaptureOutputLimitError");
      expect(reads).toBe(3);
      expect(cancelled).toBe(true);
    }),
  );

  it.effect("authorizes structured extraction before provider execution", () =>
    Effect.gen(function* () {
      const harness = adapter.layer([json({ success: true, result: {} })], true);
      const exit = yield* capture(harness.layer, structuredRequest).pipe(Effect.exit);
      expect(String(exit)).toContain("PageCaptureInferencePolicyError");
      expect(harness.calls()).toBe(0);
    }),
  );

  it.effect("cancels an interrupted response stream", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      let cancelled = false;
      const stream = new ReadableStream<Uint8Array>(
        {
          pull() {
            Effect.runSync(Deferred.succeed(started, undefined));
            return new Promise<void>(() => {});
          },
          cancel() {
            cancelled = true;
          },
        },
        { highWaterMark: 0 },
      );
      const program = capture(
        adapter.layer([new Response(stream, { headers: { "content-type": "application/json" } })])
          .layer,
        markdownRequest(),
      );
      const fiber = yield* Effect.forkChild(program);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      expect(cancelled).toBe(true);
    }),
  );
});

import {
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionRpcError,
  browserQuickActionScreenshotLayer,
  type BrowserQuickActionClient,
} from "@effect-agent/platform-cloudflare/CloudflareBrowser";
import { PageHtmlTarget, PageUrlTarget } from "@effect-agent/sandbox/PageCapture";
import {
  PageScreenshot,
  PageScreenshotLimits,
  PageScreenshotRequest,
  type PageScreenshotError,
  type PageScreenshotResult,
} from "@effect-agent/sandbox/PageScreenshot";
import { Deferred, Duration, Effect, Exit, Fiber, Layer, Logger } from "effect";
import { describe, expect, it } from "vite-plus/test";

type LayerRequirements<Value> =
  Value extends Layer.Layer<infer _Output, infer _Error, infer Requirements> ? Requirements : never;
type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type ScreenshotRequiresBinding = Equal<
  LayerRequirements<ReturnType<typeof browserQuickActionScreenshotLayer>>,
  BrowserQuickActionBrowserBinding
>;

interface RequestOverrides {
  readonly engine?: "chromium" | "kitesurf";
  readonly maxOutputBytes?: number;
  readonly target?: PageScreenshotRequest["target"];
  readonly fullPage?: boolean;
}

const request = (overrides: RequestOverrides = {}) =>
  PageScreenshotRequest.make({
    target:
      overrides.target ?? PageUrlTarget.make({ url: "https://example.com/screenshot-source" }),
    engine: overrides.engine ?? "chromium",
    limits: PageScreenshotLimits.make({ maxOutputBytes: overrides.maxOutputBytes ?? 1_024 }),
    fullPage: overrides.fullPage ?? true,
    navigation: {
      waitUntil: "networkidle0",
      timeoutMillis: 15_000,
      waitForSelector: { selector: "main", timeoutMillis: 5_000 },
    },
    viewport: { width: 1_280, height: 720 },
    resourcePolicy: {
      rejectResourceTypes: ["media"],
      allowRequestPatterns: ["https://example.com/*"],
    },
  });

const unusedRpcError = BrowserQuickActionRpcError.make({
  action: "screenshot",
  cause: new Error("unused Quick Action"),
});

const makeClient = (
  screenshot: BrowserQuickActionClient["screenshot"],
): BrowserQuickActionClient => ({
  screenshot,
  content: () => Effect.fail(unusedRpcError),
  markdown: () => Effect.fail(unusedRpcError),
  links: () => Effect.fail(unusedRpcError),
  scrape: () => Effect.fail(unusedRpcError),
  json: () => Effect.fail(unusedRpcError),
});

const screenshotLayer = (client: BrowserQuickActionClient): Layer.Layer<PageScreenshot> =>
  browserQuickActionScreenshotLayer().pipe(
    Layer.provide(Layer.succeed(BrowserQuickActionBrowserBinding)(client)),
  );

const captureEffect = (client: BrowserQuickActionClient, input = request()) =>
  Effect.gen(function* () {
    return yield* (yield* PageScreenshot).capture(input);
  }).pipe(Effect.provide(screenshotLayer(client)));

const capture = (
  client: BrowserQuickActionClient,
  input = request(),
): Promise<PageScreenshotResult> => Effect.runPromise(captureEffect(client, input));

const captureError = (
  client: BrowserQuickActionClient,
  input = request(),
): Promise<PageScreenshotError> =>
  Effect.runPromise(captureEffect(client, input).pipe(Effect.flip));

interface TrackedStream {
  readonly stream: ReadableStream<Uint8Array>;
  readonly reads: () => number;
  readonly cancelled: () => boolean;
}

const trackedChunks = (
  chunks: ReadonlyArray<Uint8Array>,
  options: { readonly cancelError?: Error; readonly close?: boolean } = {},
): TrackedStream => {
  let index = 0;
  let reads = 0;
  let cancelled = false;

  const stream = new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        reads += 1;
        const chunk = chunks[index++];

        if (chunk === undefined) {
          if (options.close !== false) controller.close();

          return;
        }
        controller.enqueue(chunk);
        if (options.close !== false && index === chunks.length) controller.close();
      },
      cancel() {
        cancelled = true;
        if (options.cancelError !== undefined) throw options.cancelError;
      },
    },
    { highWaterMark: 0 },
  );

  return { stream, reads: () => reads, cancelled: () => cancelled };
};

const repeatedChunks = (chunkSize: number, cancelError?: Error): TrackedStream => {
  let reads = 0;
  let cancelled = false;

  const stream = new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        reads += 1;
        controller.enqueue(new Uint8Array(chunkSize));
      },
      cancel() {
        cancelled = true;
        if (cancelError !== undefined) throw cancelError;
      },
    },
    { highWaterMark: 0 },
  );

  return { stream, reads: () => reads, cancelled: () => cancelled };
};

const blockingStream = (started: Deferred.Deferred<void>): TrackedStream => {
  let reads = 0;
  let cancelled = false;

  const stream = new ReadableStream<Uint8Array>(
    {
      pull() {
        reads += 1;
        Effect.runSync(Deferred.succeed(started, undefined));

        return new Promise<void>(() => {});
      },
      cancel() {
        cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );

  return { stream, reads: () => reads, cancelled: () => cancelled };
};

const pngResponse = (body: BodyInit | null, init: ResponseInit = {}): Response => {
  const headers = new Headers(init.headers);

  if (!headers.has("Content-Type")) headers.set("Content-Type", "image/png");

  return new Response(body, { ...init, headers });
};

describe("Browser Run PNG screenshot adapter", () => {
  it("keeps native binding authority visible in the Layer requirement", () => {
    const proof: ScreenshotRequiresBinding = true;

    expect(proof).toBe(true);
  });

  it("projects URL and HTML requests and returns only bounded PNG bytes", async () => {
    const calls: Array<BrowserRunScreenshotOptions> = [];

    const responses = [
      pngResponse(new Uint8Array([137, 80, 78, 71])),
      pngResponse(new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10])),
    ];

    const client = makeClient((options) =>
      Effect.sync(() => {
        calls.push(options);
        const response = responses.shift();

        return response ?? new Response(null, { status: 500 });
      }),
    );

    const urlRequest = request();
    const fromUrl = await capture(client, urlRequest);

    const fromHtml = await capture(
      client,
      request({ target: PageHtmlTarget.make({ html: "<main>proof</main>" }), fullPage: false }),
    );

    expect([...fromUrl.bytes]).toEqual([137, 80, 78, 71]);
    expect(fromUrl.mediaType).toBe("image/png");
    expect([...fromHtml.bytes]).toEqual([137, 80, 78, 71, 13, 10, 26, 10]);
    expect(calls[0]?.viewport).not.toBe(urlRequest.viewport);
    expect(Object.getPrototypeOf(calls[0]?.viewport ?? null)).toBe(Object.prototype);
    expect(calls).toEqual([
      expect.objectContaining({
        url: "https://example.com/screenshot-source",
        gotoOptions: { waitUntil: "networkidle0", timeout: 15_000 },
        waitForSelector: { selector: "main", timeout: 5_000 },
        viewport: { width: 1_280, height: 720 },
        rejectResourceTypes: ["media"],
        allowRequestPattern: ["https://example.com/*"],
        screenshotOptions: { type: "png", encoding: "binary", fullPage: true },
      }),
      expect.objectContaining({
        html: "<main>proof</main>",
        screenshotOptions: { type: "png", encoding: "binary", fullPage: false },
      }),
    ]);
  });

  it("fails MIME and declared-length preflight before acquiring a reader", async () => {
    for (const scenario of [
      {
        contentType: undefined,
        contentLength: undefined,
        expectedTag: "PageCaptureProtocolError",
        expectedMessage: "The screenshot response was not image/png",
      },
      {
        contentType: "image/jpeg",
        contentLength: undefined,
        expectedTag: "PageCaptureProtocolError",
        expectedMessage: "The screenshot response was not image/png",
      },
      {
        contentType: "image/png",
        contentLength: "33",
        expectedTag: "PageScreenshotOutputLimitError",
        expectedMessage: undefined,
      },
    ] as const) {
      const tracked = trackedChunks([new Uint8Array(33)], { close: false });
      const headers = new Headers();

      if (scenario.contentType !== undefined) headers.set("Content-Type", scenario.contentType);
      if (scenario.contentLength !== undefined)
        headers.set("Content-Length", scenario.contentLength);

      const error = await captureError(
        makeClient(() => Effect.succeed(new Response(tracked.stream, { headers }))),
        request({ maxOutputBytes: 32 }),
      );

      expect(error._tag).toBe(scenario.expectedTag);
      expect(error._tag === "PageCaptureProtocolError" ? error.message : undefined).toBe(
        scenario.expectedMessage,
      );
      expect(tracked.reads()).toBe(0);
      expect(tracked.cancelled()).toBe(true);
      expect(tracked.stream.locked).toBe(false);
    }
  });

  it("ignores missing or malformed lengths but treats streamed bytes as authoritative", async () => {
    for (const contentLength of [undefined, "broken"] as const) {
      const tracked = trackedChunks([new Uint8Array([137, 80, 78, 71])]);
      const headers = new Headers({ "Content-Type": "image/png" });

      if (contentLength !== undefined) headers.set("Content-Length", contentLength);

      const result = await capture(
        makeClient(() => Effect.succeed(new Response(tracked.stream, { headers }))),
      );

      expect(result.bytes.byteLength).toBe(4);
      expect(tracked.stream.locked).toBe(false);
    }

    const underreported = trackedChunks([new Uint8Array(16), new Uint8Array(17)]);

    const error = await captureError(
      makeClient(() =>
        Effect.succeed(pngResponse(underreported.stream, { headers: { "Content-Length": "1" } })),
      ),
      request({ maxOutputBytes: 32 }),
    );

    expect(error).toMatchObject({
      _tag: "PageScreenshotOutputLimitError",
      limit: 32,
      observed: 33,
    });
    expect(underreported.stream.locked).toBe(false);
  });

  it("stops an oversized stream on the first violating chunk, then cancels and unlocks", async () => {
    const tracked = repeatedChunks(512);

    const error = await captureError(
      makeClient(() => Effect.succeed(pngResponse(tracked.stream))),
      request({ maxOutputBytes: 1_024 }),
    );

    expect(error).toMatchObject({
      _tag: "PageScreenshotOutputLimitError",
      limit: 1_024,
      observed: 1_536,
    });
    expect(tracked.reads()).toBe(3);
    expect(tracked.cancelled()).toBe(true);
    expect(tracked.stream.locked).toBe(false);
  });

  it("keeps provider, HTTP, rate, and engine failures exact and typed", async () => {
    const foreignCause = new Error("private provider failure");

    const rpc = await captureError(
      makeClient(() =>
        Effect.fail(BrowserQuickActionRpcError.make({ action: "screenshot", cause: foreignCause })),
      ),
    );

    expect(rpc).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: "The browser binding rejected the screenshot",
      cause: foreignCause,
    });

    const clientFor = (response: Response) => makeClient(() => Effect.succeed(response));

    expect(await captureError(clientFor(new Response("bad", { status: 400 })))).toMatchObject({
      _tag: "PageCaptureNavigationError",
      message: "The screenshot Quick Action answered HTTP 400",
    });
    expect(await captureError(clientFor(new Response("bad", { status: 500 })))).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: "The screenshot Quick Action answered HTTP 500",
    });
    expect(
      await captureError(
        clientFor(new Response("busy", { status: 429, headers: { "Retry-After": "7" } })),
      ),
    ).toMatchObject({
      _tag: "PageCaptureRateLimitedError",
      reason: "rate",
      retryAfterMillis: 7_000,
      message: "The screenshot Quick Action was rate limited",
    });

    let calls = 0;

    const unsupported = await captureError(
      makeClient(() =>
        Effect.sync(() => {
          calls += 1;

          return pngResponse(new Uint8Array([137, 80, 78, 71]));
        }),
      ),
      request({ engine: "kitesurf" }),
    );

    expect(unsupported).toMatchObject({
      _tag: "PageCaptureUnsupportedError",
      feature: "engine",
    });
    expect(calls).toBe(0);
  });

  it("cancels and unlocks acquired readers on success, typed failure, defect, timeout, and interruption", async () => {
    const success = trackedChunks([new Uint8Array([137, 80, 78, 71])]);

    await capture(makeClient(() => Effect.succeed(pngResponse(success.stream))));
    expect(success.stream.locked).toBe(false);

    const expectedFailure = repeatedChunks(8);

    await captureError(
      makeClient(() => Effect.succeed(pngResponse(expectedFailure.stream))),
      request({ maxOutputBytes: 8 }),
    );
    expect(expectedFailure.cancelled()).toBe(true);
    expect(expectedFailure.stream.locked).toBe(false);

    const beforeDefect = trackedChunks([new Uint8Array([137, 80, 78, 71])]);

    const defect = await Effect.runPromise(
      captureEffect(makeClient(() => Effect.succeed(pngResponse(beforeDefect.stream)))).pipe(
        Effect.andThen(Effect.die("defect after capture")),
        Effect.exit,
      ),
    );

    expect(Exit.isFailure(defect)).toBe(true);
    expect(beforeDefect.stream.locked).toBe(false);

    await Effect.runPromise(
      Effect.gen(function* () {
        const timeoutStarted = yield* Deferred.make<void>();
        const timed = blockingStream(timeoutStarted);

        const timeoutFiber = yield* captureEffect(
          makeClient(() => Effect.succeed(pngResponse(timed.stream))),
        ).pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(25),
            orElse: () => Effect.fail("timeout"),
          }),
          Effect.forkChild,
        );

        yield* Deferred.await(timeoutStarted);
        const timeoutExit = yield* Fiber.await(timeoutFiber);

        expect(Exit.isFailure(timeoutExit)).toBe(true);
        expect(timed.cancelled()).toBe(true);
        expect(timed.stream.locked).toBe(false);

        const interruptedStarted = yield* Deferred.make<void>();
        const interrupted = blockingStream(interruptedStarted);

        const interruptedFiber = yield* Effect.forkChild(
          captureEffect(makeClient(() => Effect.succeed(pngResponse(interrupted.stream)))),
        );

        yield* Deferred.await(interruptedStarted);
        yield* Fiber.interrupt(interruptedFiber);
        const interruptedExit = yield* Fiber.await(interruptedFiber);

        expect(Exit.isFailure(interruptedExit)).toBe(true);
        expect(interrupted.cancelled()).toBe(true);
        expect(interrupted.stream.locked).toBe(false);
      }),
    );
  });

  it("keeps screenshot bytes out of errors, cleanup logs, and non-byte result metadata", async () => {
    const sentinel = "private-screenshot-byte-sentinel";
    const encoded = new TextEncoder().encode(sentinel);
    const successBytes = new Uint8Array(8 + encoded.byteLength);

    successBytes.set([137, 80, 78, 71, 13, 10, 26, 10]);
    successBytes.set(encoded, 8);
    const logs: Array<string> = [];

    const logger = Logger.make<unknown, void>(({ cause, message }) => {
      logs.push(`${String(message)} ${String(cause)}`);
    });

    const result = await Effect.runPromise(
      captureEffect(
        makeClient(() => Effect.succeed(pngResponse(successBytes))),
        request({ maxOutputBytes: successBytes.byteLength }),
      ).pipe(Effect.provide(Logger.layer([logger]))),
    );

    expect(new TextDecoder().decode(result.bytes)).toContain(sentinel);
    expect(
      JSON.stringify({ implementation: result.implementation, mediaType: result.mediaType }),
    ).not.toContain(sentinel);

    const hostileBody = trackedChunks([encoded], { close: false });

    const mimeError = await Effect.runPromise(
      captureEffect(
        makeClient(() =>
          Effect.succeed(
            new Response(hostileBody.stream, { headers: { "Content-Type": "text/plain" } }),
          ),
        ),
      ).pipe(Effect.flip, Effect.provide(Logger.layer([logger]))),
    );

    expect(JSON.stringify(mimeError)).not.toContain(sentinel);

    const cleanupFailure = repeatedChunks(8, new Error(sentinel));

    const limitError = await Effect.runPromise(
      captureEffect(
        makeClient(() => Effect.succeed(pngResponse(cleanupFailure.stream))),
        request({ maxOutputBytes: 8 }),
      ).pipe(Effect.flip, Effect.provide(Logger.layer([logger]))),
    );

    expect(limitError).toMatchObject({ _tag: "PageScreenshotOutputLimitError" });
    expect(JSON.stringify(limitError)).not.toContain(sentinel);
    expect(logs.join("\n")).toContain("Canceling the screenshot response failed");
    expect(logs.join("\n")).not.toContain(sentinel);
  });
});

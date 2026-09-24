import {
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionRpcError,
  browserQuickActionScreenshotLayer,
  type BrowserQuickActionClient,
} from "@effect-agent/platform-cloudflare/cloudflare-browser";
import { Effect, Layer } from "effect";
import { PageUrlTarget } from "effect-agent/page-capture";
import {
  PageScreenshot,
  PageScreenshotLimits,
  PageScreenshotRequest,
  type PageScreenshotError,
} from "effect-agent/page-screenshot";
import { describe, expect, it } from "vite-plus/test";

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

const pngResponse = (body: BodyInit | null, init: ResponseInit = {}): Response => {
  const headers = new Headers(init.headers);

  if (!headers.has("Content-Type")) headers.set("Content-Type", "image/png");

  return new Response(body, { ...init, headers });
};

describe("Browser Run PNG screenshot adapter", () => {
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
});

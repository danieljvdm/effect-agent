import {
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionRpcError,
  BrowserQuickActionWorkersAi,
  browserQuickActionCaptureLayer,
  browserQuickActionWorkersAiCaptureLayer,
  type BrowserQuickActionClient,
  type BrowserQuickActionWorkersAiPolicy,
} from "@effect-agent/platform-cloudflare/cloudflare-browser";
import { Effect, Layer } from "effect";
import {
  CapturePageMarkdown,
  PageCapture,
  PageCaptureLimits,
  PageCaptureRequest,
  PageUrlTarget,
  type PageCaptureError,
} from "effect-agent/page-capture";
import { describe, expect, it } from "vite-plus/test";

interface RecordedCall {
  readonly action: "screenshot" | "content" | "markdown" | "links" | "scrape" | "json";
  readonly options: unknown;
}

/** A scripted binding: hands back the queued responses in order. */
const makeBinding = (responses: ReadonlyArray<Response | Error>) => {
  const calls: Array<RecordedCall> = [];
  let index = 0;

  const respond = (
    action: RecordedCall["action"],
    options: unknown,
  ): Effect.Effect<Response, BrowserQuickActionRpcError> => {
    calls.push({ action, options });
    const next = responses[index];

    index += 1;
    if (next === undefined) {
      return Effect.fail(
        BrowserQuickActionRpcError.make({
          action,
          cause: new Error("no scripted response left"),
        }),
      );
    }

    return next instanceof Error
      ? Effect.fail(BrowserQuickActionRpcError.make({ action, cause: next }))
      : Effect.succeed(next);
  };

  const binding: BrowserQuickActionClient = {
    screenshot: (options) => respond("screenshot", options),
    content: (options) => respond("content", options),
    markdown: (options) => respond("markdown", options),
    links: (options) => respond("links", options),
    scrape: (options) => respond("scrape", options),
    json: (options) => respond("json", options),
  };

  return { binding, calls };
};

const request = (
  action: PageCaptureRequest["action"],
  overrides?: Partial<{
    readonly engine: PageCaptureRequest["engine"];
    readonly maxOutputBytes: number;
    readonly navigation: PageCaptureRequest["navigation"];
    readonly viewport: PageCaptureRequest["viewport"];
    readonly resourcePolicy: PageCaptureRequest["resourcePolicy"];
  }>,
): PageCaptureRequest =>
  PageCaptureRequest.make({
    target: PageUrlTarget.make({ url: "https://docs.example.com/pricing" }),
    action,
    engine: overrides?.engine ?? "chromium",
    limits: PageCaptureLimits.make({ maxOutputBytes: overrides?.maxOutputBytes ?? 128 * 1024 }),
    ...(overrides?.navigation === undefined ? {} : { navigation: overrides.navigation }),
    ...(overrides?.viewport === undefined ? {} : { viewport: overrides.viewport }),
    ...(overrides?.resourcePolicy === undefined
      ? {}
      : { resourcePolicy: overrides.resourcePolicy }),
  });

const captureError = (
  binding: BrowserQuickActionClient,
  input: PageCaptureRequest,
  workersAi?: BrowserQuickActionWorkersAiPolicy,
): Promise<PageCaptureError> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const port = yield* PageCapture;

      return yield* port.capture(input).pipe(Effect.flip);
    }).pipe(Effect.provide(captureLayer(binding, workersAi))),
  );

const captureLayer = (
  binding: BrowserQuickActionClient,
  workersAi?: BrowserQuickActionWorkersAiPolicy,
): Layer.Layer<PageCapture> =>
  workersAi === undefined
    ? browserQuickActionCaptureLayer().pipe(
        Layer.provide(Layer.succeed(BrowserQuickActionBrowserBinding)(binding)),
      )
    : browserQuickActionWorkersAiCaptureLayer().pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(BrowserQuickActionBrowserBinding)(binding),
            BrowserQuickActionWorkersAi.layer(workersAi),
          ),
        ),
      );

describe("Browser Run Quick Action PageCapture adapter", () => {
  it("stops oversized streams at the first exceeding chunk and releases their reader", async () => {
    let chunksRead = 0;
    let canceled = false;

    const stream = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          chunksRead += 1;
          controller.enqueue(new Uint8Array(512));
        },
        cancel() {
          canceled = true;
        },
      },
      { highWaterMark: 0 },
    );

    const { binding } = makeBinding([new Response(stream)]);

    const error = await captureError(
      binding,
      request(CapturePageMarkdown.make({}), { maxOutputBytes: 1_024 }),
    );

    expect(error).toMatchObject({
      _tag: "PageCaptureOutputLimitError",
      limit: 1_024,
      observed: 1_536,
    });
    expect(chunksRead).toBe(3);
    expect(canceled).toBe(true);
    expect(stream.locked).toBe(false);
  });
});

import {
  BrowserQuickActionBrowserBinding,
  browserQuickActionCaptureLayer,
  browserQuickActionScreenshotLayer,
  type BrowserQuickActionClient,
} from "@effect-agent/platform-cloudflare/CloudflareBrowser";
import {
  CapturePageMarkdown,
  PageCapture,
  PageCaptureRequest,
  PageUrlTarget,
  type PageCaptureError,
} from "@effect-agent/sandbox/PageCapture";
import {
  PageScreenshot,
  PageScreenshotRequest,
  type PageScreenshotError,
} from "@effect-agent/sandbox/PageScreenshot";
import { expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Logger } from "effect";
import { TestClock } from "effect/testing";

const capture = (
  kind: "capture" | "screenshot",
  response: Response,
): Effect.Effect<unknown, PageCaptureError | PageScreenshotError> => {
  const client: BrowserQuickActionClient = {
    content: () => Effect.succeed(response),
    markdown: () => Effect.succeed(response),
    links: () => Effect.succeed(response),
    scrape: () => Effect.succeed(response),
    json: () => Effect.succeed(response),
    screenshot: () => Effect.succeed(response),
  };

  const binding = Layer.succeed(BrowserQuickActionBrowserBinding)(client);

  const common = {
    target: PageUrlTarget.make({ url: "https://example.com" }),
    engine: "chromium" as const,
    limits: { maxOutputBytes: 8 },
  };

  return kind === "capture"
    ? Effect.gen(function* () {
        return yield* (yield* PageCapture).capture(
          PageCaptureRequest.make({ ...common, action: CapturePageMarkdown.make({}) }),
        );
      }).pipe(Effect.provide(browserQuickActionCaptureLayer().pipe(Layer.provide(binding))))
    : Effect.gen(function* () {
        return yield* (yield* PageScreenshot).capture(
          PageScreenshotRequest.make({ ...common, fullPage: false }),
        );
      }).pipe(Effect.provide(browserQuickActionScreenshotLayer().pipe(Layer.provide(binding))));
};

it.effect.each([
  { kind: "capture", failure: "overflow" },
  { kind: "capture", failure: "interruption" },
  { kind: "screenshot", failure: "overflow" },
  { kind: "screenshot", failure: "interruption" },
  { kind: "screenshot", failure: "content-type" },
] as const)("bounds stalled $kind response cancellation after $failure", ({ kind, failure }) =>
  Effect.gen(function* () {
    const reading = yield* Deferred.make<void>();
    const canceling = yield* Deferred.make<void>();
    const runSync = Effect.runSyncWith(yield* Effect.context<never>());
    const warnings: Array<unknown> = [];
    let cancelCalls = 0;

    const body = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          runSync(Deferred.succeed(reading, undefined));
          if (failure === "overflow") controller.enqueue(new Uint8Array(16));
        },
        cancel() {
          cancelCalls++;
          runSync(Deferred.succeed(canceling, undefined));

          return new Promise<void>(() => {});
        },
      },
      { highWaterMark: 0 },
    );

    const response = new Response(body, {
      headers: {
        "content-type":
          kind === "screenshot" && failure !== "content-type" ? "image/png" : "application/json",
      },
    });

    const fiber = yield* capture(kind, response).pipe(
      Effect.provide(
        Logger.layer([
          Logger.make((event) => {
            warnings.push(event.message);
          }),
        ]),
      ),
      Effect.forkChild,
    );

    if (failure === "interruption") {
      yield* Deferred.await(reading);
      yield* Fiber.interrupt(fiber).pipe(Effect.forkChild);
    }
    yield* Deferred.await(canceling);
    yield* TestClock.adjust("1 second");
    const exit = yield* Fiber.await(fiber);

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      if (failure === "interruption") {
        expect(Cause.hasInterrupts(exit.cause)).toBe(true);
        expect(Cause.hasFails(exit.cause)).toBe(false);
      } else {
        expect(Cause.squash(exit.cause)).toMatchObject({
          _tag:
            failure === "content-type"
              ? "PageCaptureProtocolError"
              : kind === "capture"
                ? "PageCaptureOutputLimitError"
                : "PageScreenshotOutputLimitError",
        });
      }
      expect(Cause.hasDies(exit.cause)).toBe(false);
    }
    expect(cancelCalls).toBe(1);
    expect(body.locked).toBe(false);
    expect(warnings).toEqual([
      [
        kind === "capture"
          ? "Canceling the Quick Action response failed"
          : "Canceling the screenshot response failed",
      ],
    ]);
  }),
);

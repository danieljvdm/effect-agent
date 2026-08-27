import { WebCapture, WebCaptureScrapeSuccess, WebCaptureSuccess } from "@effect-agent/capabilities";
import {
  BrowserQuickActionBrowserBinding,
  browserQuickActionCaptureLayer,
  browserQuickActionScreenshotLayer,
} from "@effect-agent/platform-cloudflare/browser-quick-action";
import {
  BrowserRunHandoffRequest,
  BrowserRunInteractiveBinding,
  BrowserRunInteractiveHost,
  BrowserRunLiveViewRequest,
  browserRunInteractiveHostLayer,
} from "@effect-agent/platform-cloudflare/interactive-browser";
import {
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  BrowserScreenshotRequest,
  BrowserScrollRequest,
  InteractiveBrowserPolicy,
  PageScreenshot,
  PageScreenshotLimits,
  PageScreenshotRequest,
  PageUrlTarget,
} from "@effect-agent/sandbox";
import { Duration, Effect, Layer, Option, Redacted, Schema, Stream } from "effect";
import { Worker, WorkerEnvironment } from "effect-cf";
import { Toolkit } from "effect/unstable/ai";

import {
  BrowserRunInteractiveProof,
  BrowserRunWorkerProofResult,
  PROOF_FACT,
  PROOF_SOURCE_URL,
} from "./contract.ts";

const proofCapture = WebCapture.make("capture_example_domain", {
  description: "Read the fixed Example Domain proof page as Markdown.",
  urls: ["example.com"],
  actions: ["markdown"],
  maxResponseBytes: 4 * 1_024,
});

const PROOF_SCRAPE_SELECTORS = ["h1", "a"] as const;
const proofScrape = WebCapture.makeScrape("scrape_example_domain", {
  description: "Scrape the fixed Example Domain proof page by selector.",
  urls: ["example.com"],
  maxResponseBytes: 16 * 1_024,
});

const SCREENSHOT_MAX_OUTPUT_BYTES = 256 * 1_024;
const INTERACTIVE_MAX_TEXT_BYTES = 4 * 1_024;
const QUICK_ACTION_PACING_DELAY = Duration.seconds(11);
const PNG_SIGNATURE = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const EXAMPLE_DOMAIN_REQUEST_PATTERN = "^https://example\\.com(?::[0-9]+)?(?:[/?#]|$)";

const screenshotRequest = PageScreenshotRequest.make({
  target: PageUrlTarget.make({ url: PROOF_SOURCE_URL }),
  engine: "chromium",
  limits: PageScreenshotLimits.make({ maxOutputBytes: SCREENSHOT_MAX_OUTPUT_BYTES }),
  fullPage: false,
  viewport: { width: 1_280, height: 720 },
  resourcePolicy: { allowRequestPatterns: [EXAMPLE_DOMAIN_REQUEST_PATTERN] },
});

const interactivePolicy = InteractiveBrowserPolicy.make({
  network: { _tag: "ExactHosts", allowedHosts: ["example.com"] },
  maxActions: 7,
  maxElapsedMillis: 90_000,
  maxReturnedBytes: SCREENSHOT_MAX_OUTPUT_BYTES,
});

const interactiveNavigateRequest = BrowserNavigateRequest.make({ url: PROOF_SOURCE_URL });
const interactiveReadTextRequest = BrowserReadTextRequest.make({});

const hasPngSignature = (bytes: Uint8Array): boolean =>
  bytes.length >= PNG_SIGNATURE.length &&
  PNG_SIGNATURE.every((expected, index) => bytes[index] === expected);

class WorkerCaptureProofError extends Schema.TaggedError<WorkerCaptureProofError>()(
  "WorkerCaptureProofError",
  { message: Schema.String },
) {}

const proofLayer = Layer.unwrap(
  Effect.map(WorkerEnvironment, (env) => {
    const quickActionLayer = Layer.mergeAll(
      browserQuickActionCaptureLayer(),
      browserQuickActionScreenshotLayer(),
    ).pipe(Layer.provide(BrowserQuickActionBrowserBinding.layer({ browser: env.BROWSER })));
    const interactiveLayer = browserRunInteractiveHostLayer().pipe(
      Layer.provide(BrowserRunInteractiveBinding.layer({ browser: env.BROWSER })),
    );
    const browserRunLayer = Layer.merge(quickActionLayer, interactiveLayer);
    return Layer.merge(proofCapture.handlers, proofScrape.handlers).pipe(
      Layer.provideMerge(browserRunLayer),
    );
  }),
);

const runProof = Effect.gen(function* () {
  const toolkit = yield* Toolkit.make(proofCapture.tool);
  const results = yield* toolkit.handle("capture_example_domain", {
    url: PROOF_SOURCE_URL,
    action: "markdown",
  });
  const last = yield* Stream.runLast(results);
  if (Option.isNone(last) || last.value.preliminary) {
    return yield* WorkerCaptureProofError.make({
      message: "The WebCapture handler did not return a final result",
    });
  }
  const result = last.value.result;
  if (!Schema.is(WebCaptureSuccess)(result) || !result.markdown?.includes(PROOF_FACT)) {
    return yield* WorkerCaptureProofError.make({
      message: "The Markdown capture did not contain the expected stable fact",
    });
  }
  yield* Effect.sleep(QUICK_ACTION_PACING_DELAY);
  const scrapeToolkit = yield* Toolkit.make(proofScrape.tool);
  const scrapeResults = yield* scrapeToolkit.handle("scrape_example_domain", {
    url: PROOF_SOURCE_URL,
    selectors: PROOF_SCRAPE_SELECTORS,
  });
  const scrapeLast = yield* Stream.runLast(scrapeResults);
  if (Option.isNone(scrapeLast) || scrapeLast.value.preliminary) {
    return yield* WorkerCaptureProofError.make({
      message: "The selector scrape handler did not return a final result",
    });
  }
  const scrapeResult = scrapeLast.value.result;
  const heading = Schema.is(WebCaptureScrapeSuccess)(scrapeResult)
    ? scrapeResult.groups.find((group) => group.selector === "h1")
    : undefined;
  if (
    heading === undefined ||
    !heading.results.some((element) => element.text.includes(PROOF_FACT))
  ) {
    return yield* WorkerCaptureProofError.make({
      message: "The selector scrape did not contain the expected stable heading",
    });
  }
  yield* Effect.sleep(QUICK_ACTION_PACING_DELAY);
  const screenshots = yield* PageScreenshot;
  const screenshot = yield* screenshots.capture(screenshotRequest);
  if (screenshot.mediaType !== "image/png" || !hasPngSignature(screenshot.bytes)) {
    return yield* WorkerCaptureProofError.make({
      message: "The screenshot was not a PNG with the expected signature",
    });
  }
  const interactive = yield* Effect.scoped(
    Effect.gen(function* () {
      const browsers = yield* BrowserRunInteractiveHost;
      const session = yield* browsers.open(interactivePolicy);
      const handle = session.handle;
      const navigation = yield* handle.navigate(interactiveNavigateRequest);
      if (navigation.url !== PROOF_SOURCE_URL) {
        return yield* WorkerCaptureProofError.make({
          message: "The interactive browser did not finish at the expected URL",
        });
      }
      const page = yield* handle.readText(interactiveReadTextRequest);
      if (
        !page.text.includes(PROOF_FACT) ||
        new TextEncoder().encode(page.text).byteLength > INTERACTIVE_MAX_TEXT_BYTES
      ) {
        return yield* WorkerCaptureProofError.make({
          message: "The interactive browser text did not contain the expected stable fact",
        });
      }
      const scrolled = yield* handle.scroll(BrowserScrollRequest.make({ deltaX: 0, deltaY: 128 }));
      if (scrolled.url !== PROOF_SOURCE_URL) {
        return yield* WorkerCaptureProofError.make({
          message: "The interactive scroll changed the expected page URL",
        });
      }
      const image = yield* handle.screenshot(BrowserScreenshotRequest.make({ fullPage: false }));
      if (image.mediaType !== "image/png" || !hasPngSignature(image.bytes)) {
        return yield* WorkerCaptureProofError.make({
          message: "The interactive screenshot was not a PNG with the expected signature",
        });
      }
      const liveView = yield* session.getLiveView(
        BrowserRunLiveViewRequest.make({ mode: "tab", expiresInMs: 60_000 }),
      );
      const handoff = yield* session.handoff(
        BrowserRunHandoffRequest.make({
          instructions: "Temporary browser proof. The host will close this session immediately.",
          timeout: 5_000,
        }),
      );
      const handoffState = yield* session.getHandoffState;
      if (
        !handoffState.active ||
        handoffState.handoffId === undefined ||
        Redacted.value(handoffState.handoffId) !== Redacted.value(handoff.handoffId) ||
        !Redacted.isRedacted(session.sessionId) ||
        !Redacted.isRedacted(liveView.devtoolsFrontendUrl)
      ) {
        return yield* WorkerCaptureProofError.make({
          message: "The interactive host controls did not return the expected private state",
        });
      }
      yield* session.close;
      const afterClose = yield* handle.readText(interactiveReadTextRequest).pipe(Effect.flip);
      if (afterClose._tag !== "InteractiveBrowserExpiredError") {
        return yield* WorkerCaptureProofError.make({
          message: "The explicitly closed browser handle did not reject further actions",
        });
      }
      return BrowserRunInteractiveProof.make({
        finalUrl: PROOF_SOURCE_URL,
        readFact: PROOF_FACT,
        screenshot: { mediaType: "image/png", pngSignatureValid: true },
        scrolled: true,
        liveViewCreated: true,
        handoffActive: true,
        closed: true,
      });
    }),
  );
  return Response.json(
    BrowserRunWorkerProofResult.make({
      sourceUrl: PROOF_SOURCE_URL,
      action: "markdown",
      fact: PROOF_FACT,
      scrape: {
        selectors: PROOF_SCRAPE_SELECTORS,
        headingFact: PROOF_FACT,
      },
      screenshot: {
        mediaType: "image/png",
        pngSignatureValid: true,
      },
      interactive,
    }),
  );
}).pipe(
  Effect.catch(() =>
    Effect.succeed(
      Response.json({ error: "The Browser Run binding proof failed" }, { status: 502 }),
    ),
  ),
);

export default Worker.make(proofLayer, runProof);

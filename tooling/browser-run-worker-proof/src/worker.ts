import { BrowserSessions } from "@effect-agent/platform-cloudflare/browser-session";
import {
  BrowserQuickActionBrowserBinding,
  CloudflareBrowser,
  browserQuickActionScreenshotLayer,
} from "@effect-agent/platform-cloudflare/cloudflare-browser";
import {
  BrowserRunHandoffRequest,
  BrowserRunCleanupError,
  CloudflareInteractiveBrowser,
  BrowserRunInteractiveHost,
  BrowserRunLiveViewRequest,
} from "@effect-agent/platform-cloudflare/interactive-browser";
import {
  Config,
  Duration,
  Effect,
  Layer,
  Option,
  Predicate,
  Redacted,
  RegExp,
  Schema,
  Stream,
} from "effect";
import {
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  BrowserScreenshotRequest,
  BrowserScrollRequest,
  InteractiveBrowserPolicy,
  InteractiveBrowserActionError,
} from "effect-agent/interactive-browser";
import { PageCaptureRateLimitedError, PageUrlTarget } from "effect-agent/page-capture";
import {
  PageScreenshot,
  PageScreenshotLimits,
  PageScreenshotRequest,
} from "effect-agent/page-screenshot";
import * as WebCapture from "effect-agent/web-capture";
import {
  WebCaptureFailure,
  WebCaptureScrapeSuccess,
  WebCaptureSuccess,
} from "effect-agent/web-capture";
import { Worker, WorkerEnvironment } from "effect-cf";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import type { BrowserRunProofStage } from "./contract.ts";
import {
  BrowserRunInteractiveProof,
  BrowserRunWorkerProofFailure,
  BrowserRunWorkerProofResult,
  PROOF_FACT,
  PROOF_SOURCE_PATH,
  ProviderTag,
} from "./contract.ts";
import { credentialFixture, runCredentialProof } from "./credentials.ts";
import { uploadFixture, runUploadProof } from "./uploads.ts";

const PROOF_SCRAPE_SELECTORS = ["h1", "a"] as const;
const SCREENSHOT_MAX_OUTPUT_BYTES = 256 * 1_024;
const INTERACTIVE_MAX_TEXT_BYTES = 4 * 1_024;
const QUICK_ACTION_PACING_DELAY = Duration.seconds(11);
const PNG_SIGNATURE = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const interactiveReadTextRequest = BrowserReadTextRequest.make({});

const hasPngSignature = (bytes: Uint8Array): boolean =>
  bytes.length >= PNG_SIGNATURE.length &&
  PNG_SIGNATURE.every((expected, index) => bytes[index] === expected);

class WorkerCaptureProofError extends Schema.TaggedError<WorkerCaptureProofError>()(
  "WorkerCaptureProofError",
  { message: Schema.String },
) {}

const proofLayer = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;

    const lifecycleConfig = yield* Config.all({
      accountId: Config.String("CLOUDFLARE_ACCOUNT_ID"),
      apiToken: Config.Redacted("BROWSER_RENDERING_API_TOKEN"),
    });

    const quickActionLayer = browserQuickActionScreenshotLayer().pipe(
      Layer.provide(BrowserQuickActionBrowserBinding.layer({ browser: env.BROWSER })),
    );

    const interactiveLayer = CloudflareInteractiveBrowser.hostLayer({
      browser: env.BROWSER,
      ...lifecycleConfig,
    }).pipe(Layer.provide(FetchHttpClient.layer));

    const sessionLayer = BrowserSessions.layer({
      browser: env.BROWSER,
      ...lifecycleConfig,
    }).pipe(Layer.provide(FetchHttpClient.layer));

    return Layer.mergeAll(quickActionLayer, interactiveLayer, sessionLayer);
  }),
);

const runProof = Effect.gen(function* () {
  const request = yield* Worker.NativeRequest;
  const env = yield* WorkerEnvironment;
  const source = new URL(PROOF_SOURCE_PATH, request.url);
  const sourceUrl = source.href;

  const proofCapture = WebCapture.make("capture_proof_page", {
    description: "Read the owned proof page as Markdown.",
    urls: [source.hostname],
    actions: ["markdown"],
    maxResponseBytes: 4 * 1_024,
  });

  const proofScrape = WebCapture.makeScrape("scrape_proof_page", {
    description: "Scrape the owned proof page by selector.",
    urls: [source.hostname],
    maxResponseBytes: 16 * 1_024,
  });

  const captureLayer = Layer.merge(
    CloudflareBrowser.layer(proofCapture, { browser: env.BROWSER }),
    CloudflareBrowser.layer(proofScrape, { browser: env.BROWSER }),
  );

  const screenshotRequest = PageScreenshotRequest.make({
    target: PageUrlTarget.make({ url: sourceUrl }),
    engine: "chromium",
    limits: PageScreenshotLimits.make({ maxOutputBytes: SCREENSHOT_MAX_OUTPUT_BYTES }),
    fullPage: false,
    viewport: { width: 1_280, height: 720 },
    resourcePolicy: { allowRequestPatterns: [`^${RegExp.escape(source.origin)}(?:[/?#]|$)`] },
  });

  const interactivePolicy = InteractiveBrowserPolicy.make({
    network: { _tag: "ExactHosts", allowedHosts: [source.hostname] },
    maxActions: 7,
    maxElapsedMillis: 90_000,
    maxReturnedBytes: SCREENSHOT_MAX_OUTPUT_BYTES,
  });

  const interactiveNavigateRequest = BrowserNavigateRequest.make({ url: sourceUrl });
  let stage: typeof BrowserRunProofStage.Type = "capture";
  // A capture tool returns its provider failure as a result; keep only the tag and backoff hint.
  let provider: Pick<WebCaptureFailure, "errorTag" | "retryAfterMillis"> | undefined;

  return yield* Effect.gen(function* () {
    const toolkit = yield* Toolkit.make(proofCapture.tool);

    const results = yield* toolkit.handle("capture_proof_page", {
      url: sourceUrl,
      action: "markdown",
    });

    const last = yield* Stream.runLast(results);

    if (Option.isNone(last) || last.value.preliminary) {
      return yield* WorkerCaptureProofError.make({
        message: "The WebCapture handler did not return a final result",
      });
    }
    const result = last.value.result;

    if (Schema.is(WebCaptureFailure)(result)) provider = result;
    if (!Schema.is(WebCaptureSuccess)(result) || !result.markdown?.includes(PROOF_FACT)) {
      return yield* WorkerCaptureProofError.make({
        message: "The Markdown capture did not contain the expected stable fact",
      });
    }
    yield* Effect.sleep(QUICK_ACTION_PACING_DELAY);
    stage = "scrape";
    const scrapeToolkit = yield* Toolkit.make(proofScrape.tool);

    const scrapeResults = yield* scrapeToolkit.handle("scrape_proof_page", {
      url: sourceUrl,
      selectors: PROOF_SCRAPE_SELECTORS,
    });

    const scrapeLast = yield* Stream.runLast(scrapeResults);

    if (Option.isNone(scrapeLast) || scrapeLast.value.preliminary) {
      return yield* WorkerCaptureProofError.make({
        message: "The selector scrape handler did not return a final result",
      });
    }
    const scrapeResult = scrapeLast.value.result;

    if (Schema.is(WebCaptureFailure)(scrapeResult)) provider = scrapeResult;

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

    stage = "screenshot";
    const screenshot = yield* screenshots.capture(screenshotRequest);

    if (screenshot.mediaType !== "image/png" || !hasPngSignature(screenshot.bytes)) {
      return yield* WorkerCaptureProofError.make({
        message: "The screenshot was not a PNG with the expected signature",
      });
    }

    const interactive = yield* Effect.scoped(
      Effect.gen(function* () {
        const browsers = yield* BrowserRunInteractiveHost;

        stage = "open";
        const session = yield* browsers.open(interactivePolicy);
        const handle = session.handle;

        stage = "navigate";
        const navigation = yield* handle.navigate(interactiveNavigateRequest);

        if (navigation.url !== sourceUrl) {
          return yield* WorkerCaptureProofError.make({
            message: "The interactive browser did not finish at the expected URL",
          });
        }
        stage = "read";
        const page = yield* handle.readText(interactiveReadTextRequest);

        if (
          !page.text.includes(PROOF_FACT) ||
          new TextEncoder().encode(page.text).byteLength > INTERACTIVE_MAX_TEXT_BYTES
        ) {
          return yield* WorkerCaptureProofError.make({
            message: "The interactive browser text did not contain the expected stable fact",
          });
        }
        stage = "scroll";

        const scrolled = yield* handle.scroll(
          BrowserScrollRequest.make({ deltaX: 0, deltaY: 128 }),
        );

        if (scrolled.url !== sourceUrl) {
          return yield* WorkerCaptureProofError.make({
            message: "The interactive scroll changed the expected page URL",
          });
        }
        stage = "interactive-screenshot";
        const image = yield* handle.screenshot(BrowserScreenshotRequest.make({ fullPage: false }));

        if (image.mediaType !== "image/png" || !hasPngSignature(image.bytes)) {
          return yield* WorkerCaptureProofError.make({
            message: "The interactive screenshot was not a PNG with the expected signature",
          });
        }
        stage = "live-view";

        const liveView = yield* session.getLiveView(
          BrowserRunLiveViewRequest.make({ mode: "tab", expiresInMs: 60_000 }),
        );

        stage = "handoff";

        const handoff = yield* session.handoff(
          BrowserRunHandoffRequest.make({
            instructions: "Temporary browser proof. The host will close this session immediately.",
            timeout: 5_000,
          }),
        );

        stage = "handoff-state";
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
        stage = "close";
        yield* session.close;
        stage = "closed-handle";
        const afterClose = yield* handle.readText(interactiveReadTextRequest).pipe(Effect.flip);

        if (afterClose._tag !== "InteractiveBrowserExpiredError") {
          return yield* WorkerCaptureProofError.make({
            message: "The explicitly closed browser handle did not reject further actions",
          });
        }

        return BrowserRunInteractiveProof.make({
          finalUrl: sourceUrl,
          readFact: PROOF_FACT,
          screenshot: { mediaType: "image/png", pngSignatureValid: true },
          scrolled: true,
          liveViewCreated: true,
          handoffActive: true,
          closed: true,
        });
      }),
    );

    stage = "browser-credentials";
    const browserCredentials = yield* runCredentialProof(new URL(request.url).origin);

    stage = "file-upload";
    const fileUpload = yield* runUploadProof(new URL(request.url).origin);

    return Response.json(
      BrowserRunWorkerProofResult.make({
        sourceUrl,
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
        browserCredentials,
        fileUpload,
      }),
    );
  }).pipe(
    Effect.provide(captureLayer),
    Effect.catch((error) => {
      const providerTag =
        provider?.errorTag ??
        (stage === "screenshot" && Predicate.hasProperty(error, "_tag") ? error._tag : undefined);

      const retryAfterMillis =
        provider?.retryAfterMillis ??
        (stage === "screenshot" && Schema.is(PageCaptureRateLimitedError)(error)
          ? error.retryAfterMillis
          : undefined);

      return Effect.succeed(
        Response.json(
          BrowserRunWorkerProofFailure.make({
            error: "The Browser Run binding proof failed",
            stage,
            ...(Schema.is(InteractiveBrowserActionError)(error) &&
            Schema.is(BrowserRunCleanupError)(error.cause)
              ? {
                  cleanupReason: error.cause.reason,
                  ...(error.cause.status === undefined
                    ? {}
                    : { cleanupStatus: error.cause.status }),
                }
              : {}),
            ...(Schema.is(ProviderTag)(providerTag) ? { providerTag } : {}),
            ...(retryAfterMillis === undefined ? {} : { retryAfterMillis }),
          }),
          { status: 502 },
        ),
      );
    }),
  );
});

export default Worker.make(
  proofLayer,
  Effect.gen(function* () {
    const request = yield* Worker.NativeRequest;

    if (new URL(request.url).pathname === PROOF_SOURCE_PATH)
      return new Response(
        `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>${PROOF_FACT}</title></head><body style="min-height:200vh"><h1 id="proof">${PROOF_FACT}</h1><p>This page belongs to the isolated browser proof.</p><a href="#proof">Proof heading</a></body></html>`,
        { headers: { "content-type": "text/html; charset=utf-8" } },
      );
    if (new URL(request.url).pathname.startsWith("/uploads/")) return yield* uploadFixture(request);

    return yield* new URL(request.url).pathname.startsWith("/credentials/")
      ? credentialFixture(request).pipe(
          Effect.orElseSucceed(() => new Response("Fixture failed", { status: 500 })),
        )
      : runProof;
  }),
);

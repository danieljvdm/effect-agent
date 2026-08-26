import { WebCapture, WebCaptureSuccess } from "@effect-agent/capabilities";
import {
  BrowserQuickActionBrowserBinding,
  browserQuickActionCaptureLayer,
  browserQuickActionScreenshotLayer,
} from "@effect-agent/platform-cloudflare/browser-quick-action";
import {
  PageScreenshot,
  PageScreenshotLimits,
  PageScreenshotRequest,
  PageUrlTarget,
} from "@effect-agent/sandbox";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import { Worker, WorkerEnvironment } from "effect-cf";
import { Toolkit } from "effect/unstable/ai";

import { BrowserRunWorkerProofResult, PROOF_FACT, PROOF_SOURCE_URL } from "./contract.ts";

const proofCapture = WebCapture.make("capture_example_domain", {
  description: "Read the fixed Example Domain proof page as Markdown.",
  urls: ["example.com"],
  actions: ["markdown"],
  maxResponseBytes: 4 * 1_024,
});

const SCREENSHOT_MAX_OUTPUT_BYTES = 256 * 1_024;
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

const hasPngSignature = (bytes: Uint8Array): boolean =>
  bytes.length >= PNG_SIGNATURE.length &&
  PNG_SIGNATURE.every((expected, index) => bytes[index] === expected);

class WorkerCaptureProofError extends Schema.TaggedError<WorkerCaptureProofError>()(
  "WorkerCaptureProofError",
  { message: Schema.String },
) {}

const proofLayer = Layer.unwrap(
  Effect.map(WorkerEnvironment, (env) => {
    const browserBindingLayer = BrowserQuickActionBrowserBinding.layer({ browser: env.BROWSER });
    const browserRunLayer = Layer.mergeAll(
      browserQuickActionCaptureLayer(),
      browserQuickActionScreenshotLayer(),
    ).pipe(Layer.provide(browserBindingLayer));
    return proofCapture.handlers.pipe(Layer.provideMerge(browserRunLayer));
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
  const screenshots = yield* PageScreenshot;
  const screenshot = yield* screenshots.capture(screenshotRequest);
  if (screenshot.mediaType !== "image/png" || !hasPngSignature(screenshot.bytes)) {
    return yield* WorkerCaptureProofError.make({
      message: "The screenshot was not a PNG with the expected signature",
    });
  }
  return Response.json(
    BrowserRunWorkerProofResult.make({
      sourceUrl: PROOF_SOURCE_URL,
      action: "markdown",
      fact: PROOF_FACT,
      screenshot: {
        mediaType: "image/png",
        pngSignatureValid: true,
      },
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

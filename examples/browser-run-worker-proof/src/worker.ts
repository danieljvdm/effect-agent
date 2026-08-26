import { WebCapture, WebCaptureSuccess } from "@effect-agent/capabilities";
import {
  BrowserQuickActionBrowserBinding,
  browserQuickActionCaptureLayer,
} from "@effect-agent/platform-cloudflare/browser-quick-action";
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

class WorkerCaptureProofError extends Schema.TaggedError<WorkerCaptureProofError>()(
  "WorkerCaptureProofError",
  { message: Schema.String },
) {}

const captureHandlersLayer = Layer.unwrap(
  Effect.map(WorkerEnvironment, (env) =>
    proofCapture.handlers.pipe(
      Layer.provide(
        browserQuickActionCaptureLayer().pipe(
          Layer.provide(BrowserQuickActionBrowserBinding.layer({ browser: env.BROWSER })),
        ),
      ),
    ),
  ),
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
  return Response.json(
    BrowserRunWorkerProofResult.make({
      sourceUrl: PROOF_SOURCE_URL,
      action: "markdown",
      fact: PROOF_FACT,
    }),
  );
}).pipe(
  Effect.catch(() =>
    Effect.succeed(
      Response.json({ error: "The Browser Run binding proof failed" }, { status: 502 }),
    ),
  ),
);

export default Worker.make(captureHandlersLayer, runProof);

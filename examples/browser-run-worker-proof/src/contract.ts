import { Schema } from "effect";

export const PROOF_SOURCE_URL = "https://example.com/";
export const PROOF_FACT = "Example Domain";

const ScreenshotProof = Schema.Struct({
  mediaType: Schema.Literal("image/png"),
  pngSignatureValid: Schema.Literal(true),
});

export class BrowserRunWorkerProofResult extends Schema.Class<BrowserRunWorkerProofResult>(
  "@effect-agent/example-browser-run-worker-proof/BrowserRunWorkerProofResult",
)({
  sourceUrl: Schema.Literal(PROOF_SOURCE_URL),
  action: Schema.Literal("markdown"),
  fact: Schema.Literal(PROOF_FACT),
  screenshot: ScreenshotProof,
}) {}

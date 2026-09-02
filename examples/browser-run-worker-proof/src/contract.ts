import { Schema } from "effect";

export const PROOF_SOURCE_URL = "https://example.com/";
export const PROOF_FACT = "Example Domain";

export const BrowserRunProofStage = Schema.Literals([
  "capture",
  "scrape",
  "screenshot",
  "open",
  "navigate",
  "read",
  "scroll",
  "interactive-screenshot",
  "live-view",
  "handoff",
  "handoff-state",
  "close",
  "closed-handle",
]);

export class BrowserRunWorkerProofFailure extends Schema.Class<BrowserRunWorkerProofFailure>(
  "BrowserRunWorkerProofFailure",
)({
  error: Schema.Literal("The Browser Run binding proof failed"),
  stage: BrowserRunProofStage,
  cleanupReason: Schema.optionalKey(
    Schema.Literals([
      "configuration",
      "authorization",
      "rate-limited",
      "provider",
      "malformed",
      "timeout",
      "pending",
    ]),
  ),
  cleanupStatus: Schema.optionalKey(Schema.Int),
}) {}

const ScreenshotProof = Schema.Struct({
  mediaType: Schema.Literal("image/png"),
  pngSignatureValid: Schema.Literal(true),
});

const ScrapeProof = Schema.Struct({
  selectors: Schema.Tuple([Schema.Literal("h1"), Schema.Literal("a")]),
  headingFact: Schema.Literal(PROOF_FACT),
});

export class BrowserRunInteractiveProof extends Schema.Class<BrowserRunInteractiveProof>(
  "@effect-agent/example-browser-run-worker-proof/BrowserRunInteractiveProof",
)({
  finalUrl: Schema.Literal(PROOF_SOURCE_URL),
  readFact: Schema.Literal(PROOF_FACT),
  screenshot: ScreenshotProof,
  scrolled: Schema.Literal(true),
  liveViewCreated: Schema.Literal(true),
  handoffActive: Schema.Literal(true),
  closed: Schema.Literal(true),
}) {}

export class BrowserRunWorkerProofResult extends Schema.Class<BrowserRunWorkerProofResult>(
  "@effect-agent/example-browser-run-worker-proof/BrowserRunWorkerProofResult",
)({
  sourceUrl: Schema.Literal(PROOF_SOURCE_URL),
  action: Schema.Literal("markdown"),
  fact: Schema.Literal(PROOF_FACT),
  scrape: ScrapeProof,
  screenshot: ScreenshotProof,
  interactive: BrowserRunInteractiveProof,
}) {}

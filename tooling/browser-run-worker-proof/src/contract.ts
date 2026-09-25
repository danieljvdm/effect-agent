import { Option, Schema } from "effect";

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
  "browser-credentials",
  "file-upload",
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

export const describeBrowserRunProofFailure = (status: number, body: unknown): string => {
  const prefix = `HTTP ${status}`;

  const failure = Schema.decodeUnknownOption(BrowserRunWorkerProofFailure)(body);

  if (Option.isNone(failure)) return `${prefix}; invocation was not retried`;

  const detail = failure.value;

  const cleanup =
    detail.cleanupReason === undefined
      ? ""
      : `; cleanup=${detail.cleanupReason}${detail.cleanupStatus === undefined ? "" : ` (${detail.cleanupStatus})`}`;

  return `${prefix}; stage=${detail.stage}${cleanup}; invocation was not retried`;
};

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
  browserCredentials: Schema.Struct({
    loginLayouts: Schema.Literal(2),
    authenticatedContinuation: Schema.Literal(true),
    retainedSession: Schema.Literal(true),
    revokedFillRefused: Schema.Literal(true),
    cardFilled: Schema.Literal(true),
    closed: Schema.Literal(true),
  }),
  fileUpload: Schema.Struct({
    normalInput: Schema.Literal(true),
    dynamicChooser: Schema.Literal(true),
    checksumMatched: Schema.Literal(true),
    closed: Schema.Literal(true),
  }),
}) {}

import { Data, Effect, Option, Schema, Stream } from "effect";
import { PageCaptureTargetUrl } from "effect-agent/page-capture";

export const PROOF_SOURCE_PATH = "/source";
export const PROOF_FACT = "Browser Run proof fixture";

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

/** A provider's typed failure class; only tags and backoff hints leave the proof Worker. */
export const ProviderTag = Schema.String.check(Schema.isPattern(/^[A-Za-z][A-Za-z0-9]{0,63}$/));

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
  providerTag: Schema.optionalKey(ProviderTag),
  retryAfterMillis: Schema.optionalKey(Schema.Natural),
}) {}

export const describeBrowserRunProofFailure = (status: number, body: unknown): string => {
  const prefix = `HTTP ${status}`;

  const failure = Schema.decodeUnknownOption(BrowserRunWorkerProofFailure)(body);

  if (Option.isNone(failure)) return prefix;

  const detail = failure.value;

  const cleanup = detail.cleanupReason === undefined ? "" : `; cleanup=${detail.cleanupReason}`;

  const cleanupStatus =
    detail.cleanupStatus === undefined ? "" : `; cleanupStatus=${detail.cleanupStatus}`;

  const provider = detail.providerTag === undefined ? "" : `; provider=${detail.providerTag}`;

  const retryAfter =
    detail.retryAfterMillis === undefined ? "" : `; retryAfterMillis=${detail.retryAfterMillis}`;

  return `${prefix}; stage=${detail.stage}${cleanup}${cleanupStatus}${provider}${retryAfter}`;
};

// Stateless Quick Actions run before the proof opens any browser session.
const quickActionStages = new Set<typeof BrowserRunProofStage.Type>([
  "capture",
  "scrape",
  "screenshot",
]);

// Rate limits stay final: adapters cannot reliably tell a rate limit from exhausted quota.
const transientProviderTags = new Set(["PageCaptureProtocolError", "PageCaptureNavigationError"]);

/**
 * A provider protocol or navigation failure in a Quick Action stage left no browser to clean up
 * and no fixture state, so the whole proof may run again. Rate limits, long backoff hints,
 * product assertions and every later stage are final.
 */
export const transientBrowserRunProofFailure = (
  body: unknown,
): Option.Option<BrowserRunWorkerProofFailure> =>
  Schema.decodeUnknownOption(BrowserRunWorkerProofFailure)(body).pipe(
    Option.filter(
      (detail) =>
        quickActionStages.has(detail.stage) &&
        detail.providerTag !== undefined &&
        transientProviderTags.has(detail.providerTag) &&
        (detail.retryAfterMillis ?? 0) <= 60_000,
    ),
  );

class ProofFailureBodyTooLarge extends Data.TaggedError("ProofFailureBodyTooLarge")<{
  readonly limit: number;
}> {}

/** Reads at most 4 KiB of JSON; an oversized, slow or malformed body becomes `null`. */
export const readBrowserRunProofFailure = <E, R>(
  stream: Stream.Stream<Uint8Array, E, R>,
): Effect.Effect<unknown, never, R> =>
  Stream.runFoldEffect(
    stream,
    () => new Uint8Array(),
    (body, chunk) => {
      if (body.byteLength + chunk.byteLength > 4_096)
        return Effect.fail(new ProofFailureBodyTooLarge({ limit: 4_096 }));
      const combined = new Uint8Array(body.byteLength + chunk.byteLength);

      combined.set(body);
      combined.set(chunk, body.byteLength);

      return Effect.succeed(combined);
    },
  ).pipe(
    Effect.timeout("2 seconds"),
    Effect.flatMap((bytes) =>
      Effect.try(() => new TextDecoder("utf-8", { fatal: true }).decode(bytes)),
    ),
    Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))),
    Effect.orElseSucceed(() => null),
  );

export const describeBrowserRunProofFailureFromStream = <E, R>(
  status: number,
  stream: Stream.Stream<Uint8Array, E, R>,
): Effect.Effect<string, never, R> =>
  Effect.map(readBrowserRunProofFailure(stream), (body) =>
    describeBrowserRunProofFailure(status, body),
  );

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
  finalUrl: PageCaptureTargetUrl,
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
  sourceUrl: PageCaptureTargetUrl,
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

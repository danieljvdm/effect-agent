/// <reference types="@cloudflare/workers-types" />

import {
  PageCapture,
  PageCaptureInferenceUse,
  PageCaptureInferencePolicyError,
  PageCaptureNavigationError,
  PageCaptureOutputLimitError,
  PageCaptureProtocolError,
  PageCaptureRateLimitedError,
  PageCaptureResourceUse,
  PageCaptureResult,
  PageCaptureUnsupportedError,
  PageScreenshot,
  PageScreenshotOutputLimitError,
  PageScreenshotResult,
  type PageScreenshotCapture,
  type PageScreenshotError,
  type PageScreenshotRequest,
  PageContentCaptured,
  PageLinksCaptured,
  PageMarkdownCaptured,
  PageScrapeCaptured,
  PageStructuredCaptured,
  SandboxImplementation,
  type PageCaptureAction,
  type PageCaptureCapture,
  type PageCaptureError,
  type PageCaptureOutput,
  type PageCaptureRequest,
} from "@effect-agent/sandbox";
import { Context, Effect, Layer, Option, Schema } from "effect";

/**
 * The Cloudflare Browser Run Quick Action `PageCapture` adapter (capability
 * spec §9.2). Each capture is one stateless `quickAction()` RPC on the
 * Wrangler `browser` binding: the platform renders the target in a managed
 * headless browser and returns one bounded output; the adapter holds no
 * session and no state between passes. The binding requires a Worker
 * compatibility date of `2026-03-24` or later, and local `wrangler dev` needs
 * remote mode (`"remote": true` on the binding) because `quickAction` has no
 * local implementation.
 *
 * Rendered output is untrusted, attacker-influenced content; this adapter
 * only bounds and types it. Deployment class `E` only: no durability claim.
 */
export const browserQuickActionImplementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "cloudflare-browser-quick-action",
});

/**
 * Effect-native client captured by the binding service. Its option types come
 * directly from the pinned Workers declarations rather than a local copy.
 */
export interface BrowserQuickActionClient {
  readonly screenshot: (
    options: BrowserRunScreenshotOptions,
  ) => Effect.Effect<Response, BrowserQuickActionRpcError>;
  readonly content: (
    options: BrowserRunContentOptions,
  ) => Effect.Effect<Response, BrowserQuickActionRpcError>;
  readonly markdown: (
    options: BrowserRunMarkdownOptions,
  ) => Effect.Effect<Response, BrowserQuickActionRpcError>;
  readonly links: (
    options: BrowserRunLinksOptions,
  ) => Effect.Effect<Response, BrowserQuickActionRpcError>;
  readonly scrape: (
    options: BrowserRunScrapeOptions,
  ) => Effect.Effect<Response, BrowserQuickActionRpcError>;
  readonly json: (
    options: BrowserRunJsonOptions,
  ) => Effect.Effect<Response, BrowserQuickActionRpcError>;
}

/** A native binding RPC rejected before it returned an HTTP response. */
export class BrowserQuickActionRpcError extends Schema.TaggedError<BrowserQuickActionRpcError>()(
  "BrowserQuickActionRpcError",
  {
    action: Schema.Literals(["screenshot", "content", "markdown", "links", "scrape", "json"]),
    cause: Schema.Defect(),
  },
) {}

export interface BrowserQuickActionCaptureOptions {
  /** The resolved Wrangler `browser` binding (DEPLOY-014: supplied, never ambient). */
  readonly browser: BrowserRun;
}

/** Host-owned browser binding authority, supplied explicitly at the composition root. */
export class BrowserQuickActionBrowserBinding extends Context.Service<
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionClient
>()("@effect-agent/platform-cloudflare/BrowserQuickActionBrowserBinding") {
  static layer(
    options: BrowserQuickActionCaptureOptions,
  ): Layer.Layer<BrowserQuickActionBrowserBinding> {
    const browser = options.browser;
    const invoke = Effect.fn("BrowserQuickActionBrowserBinding.invoke")(function* (
      action: "screenshot" | "content" | "markdown" | "links" | "scrape" | "json",
      evaluate: () => Promise<Response>,
    ): Effect.fn.Return<Response, BrowserQuickActionRpcError> {
      return yield* Effect.tryPromise({
        try: evaluate,
        catch: (cause) => BrowserQuickActionRpcError.make({ action, cause }),
      });
    });
    return Layer.succeed(BrowserQuickActionBrowserBinding)({
      screenshot: (request) =>
        invoke("screenshot", () => browser.quickAction("screenshot", request)),
      content: (request) => invoke("content", () => browser.quickAction("content", request)),
      markdown: (request) => invoke("markdown", () => browser.quickAction("markdown", request)),
      links: (request) => invoke("links", () => browser.quickAction("links", request)),
      scrape: (request) => invoke("scrape", () => browser.quickAction("scrape", request)),
      json: (request) => invoke("json", () => browser.quickAction("json", request)),
    });
  }
}

/** Host-owned authorization and accounting for one Workers AI extraction. */
export interface BrowserQuickActionWorkersAiPolicy {
  readonly authorizeAndAccount: (
    request: PageCaptureRequest,
  ) => Effect.Effect<void, BrowserQuickActionWorkersAiPolicyError>;
}

/** Host-only diagnostic for a denied or unaccounted Workers AI extraction. */
export class BrowserQuickActionWorkersAiPolicyError extends Schema.TaggedError<BrowserQuickActionWorkersAiPolicyError>()(
  "BrowserQuickActionWorkersAiPolicyError",
  {
    reason: Schema.Literals(["authorization", "accounting"]),
    message: Schema.String.check(Schema.isMaxLength(8_000)),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Explicit host-owned authority and accounting for separately billed Workers AI extraction. */
export class BrowserQuickActionWorkersAi extends Context.Service<
  BrowserQuickActionWorkersAi,
  BrowserQuickActionWorkersAiPolicy
>()("@effect-agent/platform-cloudflare/BrowserQuickActionWorkersAi") {
  static layer(
    policy: BrowserQuickActionWorkersAiPolicy,
  ): Layer.Layer<BrowserQuickActionWorkersAi> {
    return Layer.succeed(BrowserQuickActionWorkersAi)(policy);
  }
}

const MAX_DIAGNOSTIC_LENGTH = 8_000;
const boundedDiagnostic = (message: string): string => message.slice(0, MAX_DIAGNOSTIC_LENGTH);

const QuickActionSuccessEnvelope = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Json,
});

const QuickActionErrorEnvelope = Schema.Struct({
  success: Schema.Literal(false),
  errors: Schema.Array(
    Schema.Struct({
      message: Schema.String,
      code: Schema.optionalKey(Schema.Number),
      detail: Schema.optionalKey(Schema.String),
      path: Schema.optionalKey(Schema.String),
    }),
  ),
  rawAiResponse: Schema.optionalKey(Schema.String),
});

const QuickActionEnvelope = Schema.Union([QuickActionSuccessEnvelope, QuickActionErrorEnvelope]);
const decodeEnvelope = Schema.decodeUnknownOption(Schema.fromJsonString(QuickActionEnvelope));

/** Project the schema-validated request onto Cloudflare's native common options. */
const quickActionCommonOptions = (request: PageCaptureRequest): BrowserRunCommonOptions => {
  const options: BrowserRunBaseOptions = {};
  const navigation = request.navigation;
  if (navigation !== undefined) {
    const goto: NonNullable<BrowserRunBaseOptions["gotoOptions"]> = {};
    if (navigation.waitUntil !== undefined) goto.waitUntil = navigation.waitUntil;
    if (navigation.timeoutMillis !== undefined) goto.timeout = navigation.timeoutMillis;
    if (Object.keys(goto).length > 0) options.gotoOptions = goto;
    if (navigation.waitForSelector !== undefined) {
      options.waitForSelector = {
        selector: navigation.waitForSelector.selector,
        ...(navigation.waitForSelector.timeoutMillis === undefined
          ? {}
          : { timeout: navigation.waitForSelector.timeoutMillis }),
      };
    }
  }
  if (request.viewport !== undefined) {
    options.viewport = { width: request.viewport.width, height: request.viewport.height };
  }
  if (request.resourcePolicy !== undefined) {
    if (request.resourcePolicy.rejectResourceTypes !== undefined) {
      options.rejectResourceTypes = [...request.resourcePolicy.rejectResourceTypes];
    }
    if (request.resourcePolicy.allowRequestPatterns !== undefined) {
      options.allowRequestPattern = [...request.resourcePolicy.allowRequestPatterns];
    }
  }
  return request.target._tag === "PageUrlTarget"
    ? { ...options, url: request.target.url }
    : { ...options, html: request.target.html };
};

/** Dispatch through Cloudflare's native action-specific overloads. */
const executeQuickAction = (
  browser: BrowserQuickActionClient,
  request: PageCaptureRequest,
): Effect.Effect<Response, BrowserQuickActionRpcError> => {
  const options = quickActionCommonOptions(request);
  switch (request.action._tag) {
    case "CapturePageContent": {
      return browser.content(options);
    }
    case "CapturePageMarkdown": {
      return browser.markdown(options);
    }
    case "CapturePageLinks": {
      return browser.links({
        ...options,
        ...(request.action.visibleLinksOnly === undefined
          ? {}
          : { visibleLinksOnly: request.action.visibleLinksOnly }),
      });
    }
    case "CapturePageScrape": {
      return browser.scrape({
        ...options,
        elements: request.action.selectors.map((selector) => ({ selector })),
      });
    }
    case "CapturePageStructured": {
      return browser.json({
        ...options,
        response_format: {
          type: "json_schema",
          json_schema: request.action.responseFormat,
        },
        ...(request.action.prompt === undefined ? {} : { prompt: request.action.prompt }),
      });
    }
  }
};

const protocolError = (message: string, cause?: unknown): PageCaptureProtocolError =>
  PageCaptureProtocolError.make({
    implementation: browserQuickActionImplementation,
    message: boundedDiagnostic(message),
    ...(cause === undefined ? {} : { cause }),
  });

const navigationError = (message: string, cause?: unknown): PageCaptureNavigationError =>
  PageCaptureNavigationError.make({
    implementation: browserQuickActionImplementation,
    message: boundedDiagnostic(message),
    ...(cause === undefined ? {} : { cause }),
  });

/** Preserve bounded remote diagnostics for the host without exposing their text to a model. */
const privateResponseCause = (bodyText: string): Error | undefined =>
  bodyText.length === 0 ? undefined : new Error(boundedDiagnostic(bodyText));

const releaseResponseReader = (
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Effect.Effect<void> =>
  Effect.tryPromise({
    try: () => reader.cancel(),
    catch: (cause) => protocolError("Canceling the Quick Action response failed", cause),
  }).pipe(
    Effect.catch((error) => Effect.logWarning(error.message)),
    Effect.ensuring(
      Effect.try({
        try: () => reader.releaseLock(),
        catch: (cause) => protocolError("Releasing the Quick Action response failed", cause),
      }).pipe(Effect.catch((error) => Effect.logWarning(error.message))),
    ),
  );

const readBoundedResponse = Effect.fn("BrowserQuickActionCapture.readResponse")(function* (
  response: Response,
  request: PageCaptureRequest,
) {
  const body = response.body;
  if (body === null) return "";

  const reader = yield* Effect.acquireRelease(
    Effect.try({
      try: () => body.getReader(),
      catch: (cause) => protocolError("Opening the Quick Action response failed", cause),
    }),
    releaseResponseReader,
  );
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
  let observedBytes = 0;
  let bodyText = "";

  while (true) {
    const chunk = yield* Effect.tryPromise({
      try: () => reader.read(),
      catch: (cause) => protocolError("Reading the Quick Action response failed", cause),
    });
    if (chunk.done) break;

    observedBytes += chunk.value.byteLength;
    if (observedBytes > request.limits.maxOutputBytes) {
      return yield* PageCaptureOutputLimitError.make({
        implementation: browserQuickActionImplementation,
        limit: request.limits.maxOutputBytes,
        observed: observedBytes,
      });
    }

    bodyText += yield* Effect.try({
      try: () => decoder.decode(chunk.value, { stream: true }),
      catch: (cause) => protocolError("Decoding the Quick Action response failed", cause),
    });
  }

  return (
    bodyText +
    (yield* Effect.try({
      try: () => decoder.decode(),
      catch: (cause) => protocolError("Decoding the Quick Action response failed", cause),
    }))
  );
}, Effect.scoped);

/**
 * Retry-After arrives in whole seconds; a non-integer form (an HTTP date) is
 * dropped rather than guessed at.
 */
const retryAfterMillis = (response: Response): number | undefined => {
  const header = response.headers.get("Retry-After");
  if (header === null) return undefined;
  const seconds = Number(header);
  if (!Number.isSafeInteger(seconds) || seconds < 0) return undefined;
  const millis = seconds * 1_000;
  return Number.isSafeInteger(millis) ? millis : undefined;
};

const browserMillis = (response: Response): number | undefined => {
  const header = response.headers.get("X-Browser-Ms-Used");
  if (header === null) return undefined;
  const millis = Number(header);
  return Number.isSafeInteger(millis) && millis >= 0 ? millis : undefined;
};

/** Only trusted response metadata chooses transport framing; page text never does. */
const isJsonResponse = (response: Response): boolean => {
  const contentType = response.headers.get("Content-Type");
  if (contentType === null) return false;
  const mediaType = contentType.split(";", 1)[0]?.trim().toLowerCase();
  return mediaType === "application/json" || mediaType?.endsWith("+json") === true;
};

const parseOutput = (
  action: PageCaptureAction,
  bodyText: string,
  response: Response,
): PageCaptureOutput | PageCaptureNavigationError | PageCaptureProtocolError => {
  if (!isJsonResponse(response)) {
    return protocolError(
      "The Quick Action success response was not a JSON response envelope",
      privateResponseCause(bodyText),
    );
  }
  const envelope = decodeEnvelope(bodyText);
  if (Option.isNone(envelope)) {
    return protocolError(
      "The JSON Quick Action response did not carry a valid response envelope",
      privateResponseCause(bodyText),
    );
  }
  if (!envelope.value.success) {
    return navigationError(
      "The Quick Action reported a navigation failure",
      privateResponseCause(bodyText),
    );
  }
  switch (action._tag) {
    case "CapturePageContent":
    case "CapturePageMarkdown": {
      if (typeof envelope.value.result !== "string") {
        return protocolError("The Quick Action envelope carried a non-text result");
      }
      return action._tag === "CapturePageContent"
        ? PageContentCaptured.make({ html: envelope.value.result })
        : PageMarkdownCaptured.make({ markdown: envelope.value.result });
    }
    case "CapturePageLinks": {
      const decoded = Schema.decodeUnknownOption(PageLinksCaptured)({
        _tag: "PageLinksCaptured",
        links: envelope.value.result,
      });
      if (Option.isNone(decoded)) {
        return protocolError("The links Quick Action did not return a bounded array of valid URLs");
      }
      return decoded.value;
    }
    case "CapturePageScrape": {
      const decoded = Schema.decodeUnknownOption(PageScrapeCaptured)({
        _tag: "PageScrapeCaptured",
        groups: envelope.value.result,
      });
      if (Option.isNone(decoded)) {
        return protocolError(
          "The scrape Quick Action did not return bounded grouped element records",
        );
      }
      return decoded.value;
    }
    case "CapturePageStructured": {
      return PageStructuredCaptured.make({ value: envelope.value.result });
    }
  }
};

const isQuotaMessage = (text: string): boolean => /time limit|daily|quota/i.test(text);

const makeCapture = (
  browser: BrowserQuickActionClient,
  workersAi?: BrowserQuickActionWorkersAiPolicy,
): PageCaptureCapture =>
  Effect.fn("BrowserQuickActionCapture.capture")(function* (
    request: PageCaptureRequest,
  ): Effect.fn.Return<PageCaptureResult, PageCaptureError> {
    if (request.engine !== "chromium") {
      return yield* PageCaptureUnsupportedError.make({
        implementation: browserQuickActionImplementation,
        feature: "engine",
        message:
          "The browser binding's quickAction() exposes no engine selector; kitesurf requires the REST or CDP surface",
      });
    }

    const usesWorkersAi = request.action._tag === "CapturePageStructured";
    if (usesWorkersAi) {
      if (workersAi === undefined) {
        return yield* PageCaptureUnsupportedError.make({
          implementation: browserQuickActionImplementation,
          feature: "action",
          message:
            "Structured capture invokes separately billed Workers AI and requires an explicit authorization and accounting policy",
        });
      }
      yield* workersAi.authorizeAndAccount(request).pipe(
        Effect.mapError((cause) =>
          PageCaptureInferencePolicyError.make({
            implementation: browserQuickActionImplementation,
            provider: "cloudflare-workers-ai",
            reason: cause.reason,
            message:
              cause.reason === "authorization"
                ? "Workers AI extraction was not authorized"
                : "Workers AI extraction could not be accounted for",
            cause,
          }),
        ),
      );
    }

    const response = yield* executeQuickAction(browser, request).pipe(
      Effect.mapError((error) =>
        protocolError("The browser binding rejected the Quick Action", error.cause),
      ),
    );
    const bodyText = yield* readBoundedResponse(response, request);
    if (response.status === 429) {
      const retryAfter = retryAfterMillis(response);
      const reason = isQuotaMessage(bodyText) ? "quota" : "rate";
      const cause = privateResponseCause(bodyText);
      return yield* PageCaptureRateLimitedError.make({
        implementation: browserQuickActionImplementation,
        reason,
        ...(retryAfter === undefined ? {} : { retryAfterMillis: retryAfter }),
        ...(cause === undefined ? {} : { cause }),
        message:
          reason === "quota"
            ? "The Quick Action exceeded its browser quota"
            : "The Quick Action was rate limited",
      });
    }
    if (!response.ok) {
      const message = `The Quick Action answered HTTP ${response.status}`;
      const cause = privateResponseCause(bodyText);
      if (response.status >= 500) {
        return yield* protocolError(message, cause);
      }
      return yield* navigationError(message, cause);
    }
    const output = parseOutput(request.action, bodyText, response);
    if (
      output._tag === "PageCaptureNavigationError" ||
      output._tag === "PageCaptureProtocolError"
    ) {
      return yield* output;
    }
    const millis = browserMillis(response);
    return PageCaptureResult.make({
      implementation: browserQuickActionImplementation,
      output,
      resourceUse: PageCaptureResourceUse.make({
        ...(millis === undefined ? {} : { browserMillis: millis }),
        ...(usesWorkersAi
          ? {
              inference: PageCaptureInferenceUse.make({
                provider: "cloudflare-workers-ai",
                modelCalls: 1,
              }),
            }
          : {}),
      }),
    });
  });

/**
 * Ordinary Quick Actions require host-owned browser binding authority. Workers
 * AI stays unavailable unless the host deliberately selects its separate Layer.
 */
export const browserQuickActionCaptureLayer = (): Layer.Layer<
  PageCapture,
  never,
  BrowserQuickActionBrowserBinding
> =>
  Layer.effect(
    PageCapture,
    Effect.map(BrowserQuickActionBrowserBinding, (browser) =>
      PageCapture.of({ capture: makeCapture(browser) }),
    ),
  );

/** Structured Quick Actions require host-owned browser and Workers AI authority. */
export const browserQuickActionWorkersAiCaptureLayer = (): Layer.Layer<
  PageCapture,
  never,
  BrowserQuickActionBrowserBinding | BrowserQuickActionWorkersAi
> =>
  Layer.effect(
    PageCapture,
    Effect.gen(function* () {
      const browser = yield* BrowserQuickActionBrowserBinding;
      const workersAi = yield* BrowserQuickActionWorkersAi;
      return PageCapture.of({ capture: makeCapture(browser, workersAi) });
    }),
  );

/** Host-owned Quick Action binding and optional, separately billed extraction authority. */
export interface CloudflareBrowserOptions extends BrowserQuickActionCaptureOptions {
  readonly workersAi?: BrowserQuickActionWorkersAiPolicy;
}

/** Compose WebCapture handlers with the Cloudflare Quick Action adapter. */
export const CloudflareBrowser = {
  /**
   * Supply a WebCapture definition and the resolved Worker browser binding.
   * Supports capture, scrape, and extraction definitions without importing capabilities.
   * Only PageCapture is provided; other handler requirements and errors stay visible.
   * Extraction fails closed unless workersAi explicitly authorizes and accounts for it.
   * Capture limits, typed failures, tracing, and scoped response cleanup are unchanged.
   */
  layer: <A, E, R>(
    definition: { readonly handlers: Layer.Layer<A, E, R> },
    options: CloudflareBrowserOptions,
  ): Layer.Layer<A, E, Exclude<R, PageCapture>> => {
    const capture =
      options.workersAi === undefined
        ? browserQuickActionCaptureLayer()
        : browserQuickActionWorkersAiCaptureLayer().pipe(
            Layer.provide(BrowserQuickActionWorkersAi.layer(options.workersAi)),
          );

    return definition.handlers.pipe(
      Layer.provide(capture.pipe(Layer.provide(BrowserQuickActionBrowserBinding.layer(options)))),
    );
  },
};

const screenshotOptions = (request: PageScreenshotRequest): BrowserRunScreenshotOptions => {
  const options: BrowserRunBaseOptions = {};
  if (
    request.navigation?.waitUntil !== undefined ||
    request.navigation?.timeoutMillis !== undefined
  ) {
    options.gotoOptions = {
      ...(request.navigation.waitUntil === undefined
        ? {}
        : { waitUntil: request.navigation.waitUntil }),
      ...(request.navigation.timeoutMillis === undefined
        ? {}
        : { timeout: request.navigation.timeoutMillis }),
    };
  }
  if (request.navigation?.waitForSelector !== undefined) {
    options.waitForSelector = {
      selector: request.navigation.waitForSelector.selector,
      ...(request.navigation.waitForSelector.timeoutMillis === undefined
        ? {}
        : { timeout: request.navigation.waitForSelector.timeoutMillis }),
    };
  }
  if (request.viewport !== undefined) {
    options.viewport = { width: request.viewport.width, height: request.viewport.height };
  }
  if (request.resourcePolicy?.rejectResourceTypes !== undefined) {
    options.rejectResourceTypes = [...request.resourcePolicy.rejectResourceTypes];
  }
  if (request.resourcePolicy?.allowRequestPatterns !== undefined) {
    options.allowRequestPattern = [...request.resourcePolicy.allowRequestPatterns];
  }
  return {
    ...options,
    ...(request.target._tag === "PageUrlTarget"
      ? { url: request.target.url }
      : { html: request.target.html }),
    screenshotOptions: { type: "png", encoding: "binary", fullPage: request.fullPage },
  };
};

const cancelBody = (body: ReadableStream<Uint8Array>): Effect.Effect<void> =>
  Effect.tryPromise({
    try: () => body.cancel(),
    catch: () => undefined,
  }).pipe(Effect.catch(() => Effect.void));

const releaseScreenshotReader = (
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Effect.Effect<void> =>
  Effect.tryPromise({ try: () => reader.cancel(), catch: () => undefined }).pipe(
    Effect.catch(() => Effect.logWarning("Canceling the screenshot response failed")),
    Effect.ensuring(
      Effect.try({
        try: () => reader.releaseLock(),
        catch: () => undefined,
      }).pipe(Effect.catch(() => Effect.logWarning("Releasing the screenshot response failed"))),
    ),
  );

const pngResponse = (response: Response): boolean =>
  response.headers.get("Content-Type")?.split(";", 1)[0]?.trim().toLowerCase() === "image/png";

const declaredLength = (response: Response): number | undefined => {
  const raw = response.headers.get("Content-Length");
  if (raw === null || !/^(0|[1-9][0-9]*)$/.test(raw)) return undefined;
  const length = Number(raw);
  return Number.isSafeInteger(length) ? length : undefined;
};

const readScreenshot = Effect.fn("BrowserQuickActionScreenshot.read")(function* (
  response: Response,
  request: PageScreenshotRequest,
) {
  const body = response.body;
  if (body === null) {
    return yield* protocolError("The screenshot response had no body");
  }
  if (!pngResponse(response)) {
    yield* cancelBody(body);
    return yield* protocolError("The screenshot response was not image/png");
  }
  const length = declaredLength(response);
  if (length !== undefined && length > request.limits.maxOutputBytes) {
    yield* cancelBody(body);
    return yield* PageScreenshotOutputLimitError.make({
      implementation: browserQuickActionImplementation,
      limit: request.limits.maxOutputBytes,
      observed: length,
    });
  }
  const reader = yield* Effect.acquireRelease(
    Effect.try({
      try: () => body.getReader(),
      catch: (cause) => protocolError("Opening the screenshot response failed", cause),
    }),
    releaseScreenshotReader,
  );
  const chunks: Array<Uint8Array> = [];
  let observed = 0;
  while (true) {
    const next = yield* Effect.tryPromise({
      try: () => reader.read(),
      catch: (cause) => protocolError("Reading the screenshot response failed", cause),
    });
    if (next.done) break;
    observed += next.value.byteLength;
    if (observed > request.limits.maxOutputBytes) {
      return yield* PageScreenshotOutputLimitError.make({
        implementation: browserQuickActionImplementation,
        limit: request.limits.maxOutputBytes,
        observed,
      });
    }
    chunks.push(next.value);
  }
  const bytes = new Uint8Array(observed);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}, Effect.scoped);

const makeScreenshot = (browser: BrowserQuickActionClient): PageScreenshotCapture =>
  Effect.fn("BrowserQuickActionScreenshot.capture")(function* (
    request: PageScreenshotRequest,
  ): Effect.fn.Return<PageScreenshotResult, PageScreenshotError> {
    if (request.engine !== "chromium") {
      return yield* PageCaptureUnsupportedError.make({
        implementation: browserQuickActionImplementation,
        feature: "engine",
        message: "The browser binding's screenshot action exposes no engine selector",
      });
    }
    const response = yield* browser
      .screenshot(screenshotOptions(request))
      .pipe(
        Effect.mapError((error) =>
          protocolError("The browser binding rejected the screenshot", error.cause),
        ),
      );
    if (response.status === 429) {
      const body = response.body;
      if (body !== null) yield* cancelBody(body);
      return yield* PageCaptureRateLimitedError.make({
        implementation: browserQuickActionImplementation,
        reason: "rate",
        ...(retryAfterMillis(response) === undefined
          ? {}
          : { retryAfterMillis: retryAfterMillis(response) }),
        message: "The screenshot Quick Action was rate limited",
      });
    }
    if (!response.ok) {
      const body = response.body;
      if (body !== null) yield* cancelBody(body);
      const message = `The screenshot Quick Action answered HTTP ${response.status}`;
      return yield* response.status >= 500 ? protocolError(message) : navigationError(message);
    }
    const bytes = yield* readScreenshot(response, request);
    return PageScreenshotResult.make({
      implementation: browserQuickActionImplementation,
      mediaType: "image/png",
      bytes,
    });
  });

/** Native Browser Run screenshot adapter. It retains PNG bytes only for the caller's result. */
export const browserQuickActionScreenshotLayer = (): Layer.Layer<
  PageScreenshot,
  never,
  BrowserQuickActionBrowserBinding
> =>
  Layer.effect(
    PageScreenshot,
    Effect.map(BrowserQuickActionBrowserBinding, (browser) =>
      PageScreenshot.of({ capture: makeScreenshot(browser) }),
    ),
  );

import {
  PageCapture,
  PageCaptureInferenceUse,
  PageCaptureNavigationError,
  PageCaptureOutputLimitError,
  PageCaptureProtocolError,
  PageCaptureRateLimitedError,
  PageCaptureResourceUse,
  PageCaptureResult,
  PageCaptureUnsupportedError,
  PageContentCaptured,
  PageLinksCaptured,
  PageMarkdownCaptured,
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
 * The structural surface this adapter needs from the Wrangler `browser`
 * binding. Typed here rather than through a platform global so the adapter
 * compiles against the pinned `@cloudflare/workers-types` regardless of when
 * that package ships a `quickAction` declaration.
 */
export interface BrowserQuickActionBinding {
  quickAction(action: string, options: Record<string, unknown>): Promise<Response>;
}

export interface BrowserQuickActionCaptureOptions {
  /** The resolved Wrangler `browser` binding (DEPLOY-014: supplied, never ambient). */
  readonly browser: BrowserQuickActionBinding;
}

/** Host-owned browser binding authority, supplied explicitly at the composition root. */
export class BrowserQuickActionBrowserBinding extends Context.Service<
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionBinding
>()("@effect-agent/platform-cloudflare/BrowserQuickActionBrowserBinding") {
  static layer(
    options: BrowserQuickActionCaptureOptions,
  ): Layer.Layer<BrowserQuickActionBrowserBinding> {
    return Layer.succeed(BrowserQuickActionBrowserBinding)(options.browser);
  }
}

/** Host-owned authorization and accounting for one Workers AI extraction. */
export interface BrowserQuickActionWorkersAiPolicy {
  readonly authorizeAndAccount: (
    request: PageCaptureRequest,
  ) => Effect.Effect<void, PageCaptureError>;
}

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

/** The REST-shaped Quick Action response envelope; raw payloads also occur. */
const QuickActionEnvelope = Schema.Struct({
  success: Schema.Boolean,
  result: Schema.optionalKey(Schema.Json),
  errors: Schema.optionalKey(Schema.Json),
});

const decodeEnvelope = (text: string): Option.Option<typeof QuickActionEnvelope.Type> => {
  try {
    return Schema.decodeUnknownOption(QuickActionEnvelope)(JSON.parse(text));
  } catch {
    return Option.none();
  }
};

const decodeJson = (text: string): Option.Option<Schema.Json> => {
  try {
    return Schema.decodeUnknownOption(Schema.Json)(JSON.parse(text));
  } catch {
    return Option.none();
  }
};

const quickActionName = (action: PageCaptureAction): string => {
  switch (action._tag) {
    case "CapturePageContent": {
      return "content";
    }
    case "CapturePageMarkdown": {
      return "markdown";
    }
    case "CapturePageLinks": {
      return "links";
    }
    case "CapturePageStructured": {
      return "json";
    }
  }
};

/** Project the schema-validated request onto the Quick Action wire options. */
const quickActionOptions = (request: PageCaptureRequest): Record<string, unknown> => {
  const options: Record<string, unknown> = {};
  if (request.target._tag === "PageUrlTarget") {
    options.url = request.target.url;
  } else {
    options.html = request.target.html;
  }
  const navigation = request.navigation;
  if (navigation !== undefined) {
    const goto: Record<string, unknown> = {};
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
      options.rejectResourceTypes = request.resourcePolicy.rejectResourceTypes;
    }
    if (request.resourcePolicy.allowRequestPatterns !== undefined) {
      options.allowRequestPattern = request.resourcePolicy.allowRequestPatterns;
    }
  }
  switch (request.action._tag) {
    case "CapturePageLinks": {
      if (request.action.visibleLinksOnly !== undefined) {
        options.visibleLinksOnly = request.action.visibleLinksOnly;
      }
      break;
    }
    case "CapturePageStructured": {
      options.response_format = {
        type: "json_schema",
        json_schema: request.action.responseFormat,
      };
      if (request.action.prompt !== undefined) {
        options.prompt = request.action.prompt;
      }
      break;
    }
    case "CapturePageContent":
    case "CapturePageMarkdown": {
      break;
    }
  }
  return options;
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
  const expectsEnvelope = isJsonResponse(response);
  const envelope = expectsEnvelope ? decodeEnvelope(bodyText) : Option.none();
  if (expectsEnvelope && Option.isNone(envelope)) {
    return protocolError(
      "The JSON Quick Action response did not carry a valid response envelope",
      privateResponseCause(bodyText),
    );
  }
  if (Option.isSome(envelope) && !envelope.value.success) {
    return navigationError(
      "The Quick Action reported a navigation failure",
      privateResponseCause(bodyText),
    );
  }
  switch (action._tag) {
    case "CapturePageContent":
    case "CapturePageMarkdown": {
      // The envelope wraps the text on the REST shape; a raw text body is the
      // payload itself. An envelope whose result is not text is a protocol
      // violation rather than something to coerce.
      let text = bodyText;
      if (Option.isSome(envelope)) {
        if (typeof envelope.value.result !== "string") {
          return protocolError("The Quick Action envelope carried a non-text result");
        }
        text = envelope.value.result;
      }
      return action._tag === "CapturePageContent"
        ? PageContentCaptured.make({ html: text })
        : PageMarkdownCaptured.make({ markdown: text });
    }
    case "CapturePageLinks": {
      const values = Option.isSome(envelope)
        ? envelope.value.result
        : Option.getOrUndefined(decodeJson(bodyText));
      const decoded = Schema.decodeUnknownOption(PageLinksCaptured)({
        _tag: "PageLinksCaptured",
        links: values,
      });
      if (Option.isNone(decoded)) {
        return protocolError("The links Quick Action did not return a bounded array of valid URLs");
      }
      return decoded.value;
    }
    case "CapturePageStructured": {
      if (Option.isSome(envelope)) {
        if (envelope.value.result === undefined) {
          return protocolError("The json Quick Action envelope carried no result");
        }
        return PageStructuredCaptured.make({ value: envelope.value.result });
      }
      const value = decodeJson(bodyText);
      if (Option.isNone(value)) {
        return protocolError("The json Quick Action did not return a JSON value");
      }
      return PageStructuredCaptured.make({ value: value.value });
    }
  }
};

const isQuotaMessage = (text: string): boolean => /time limit|daily|quota/i.test(text);

const makeCapture = (
  browser: BrowserQuickActionBinding,
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
      yield* workersAi.authorizeAndAccount(request);
    }

    const response = yield* Effect.tryPromise({
      try: () => browser.quickAction(quickActionName(request.action), quickActionOptions(request)),
      catch: (cause) => protocolError("The browser binding rejected the Quick Action", cause),
    });
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

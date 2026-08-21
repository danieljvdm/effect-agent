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
import { Effect, Layer, Option, Schema } from "effect";

import { safeCauseMessage } from "./boundary.ts";

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
  /** The resolved Wrangler `browser` binding (DEPLOY-010: supplied, never ambient). */
  readonly browser: BrowserQuickActionBinding;
  /**
   * Explicit authority for Cloudflare's separately billed Workers AI model.
   * Structured capture is denied unless this policy authorizes and accounts
   * for its model call before the browser binding is invoked.
   */
  readonly workersAi?: BrowserQuickActionWorkersAiPolicy | undefined;
}

/** Host-owned authorization and accounting for one Workers AI extraction. */
export interface BrowserQuickActionWorkersAiPolicy {
  readonly authorizeAndAccount: (
    request: PageCaptureRequest,
  ) => Effect.Effect<void, PageCaptureError>;
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

const protocolError = (message: string): PageCaptureProtocolError =>
  PageCaptureProtocolError.make({
    implementation: browserQuickActionImplementation,
    message: boundedDiagnostic(message),
  });

const navigationError = (message: string): PageCaptureNavigationError =>
  PageCaptureNavigationError.make({
    implementation: browserQuickActionImplementation,
    message: boundedDiagnostic(message),
  });

const releaseResponseReader = (
  reader: ReadableStreamDefaultReader<Uint8Array>,
): Effect.Effect<void> =>
  Effect.tryPromise({
    try: () => reader.cancel(),
    catch: (cause) =>
      protocolError(
        `Canceling the Quick Action response failed: ${safeCauseMessage(cause, "no diagnostic")}`,
      ),
  }).pipe(
    Effect.catch((error) => Effect.logWarning(error.message)),
    Effect.ensuring(
      Effect.try({
        try: () => reader.releaseLock(),
        catch: (cause) =>
          protocolError(
            `Releasing the Quick Action response failed: ${safeCauseMessage(cause, "no diagnostic")}`,
          ),
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
      catch: (cause) =>
        protocolError(
          `Opening the Quick Action response failed: ${safeCauseMessage(cause, "no diagnostic")}`,
        ),
    }),
    releaseResponseReader,
  );
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
  let observedBytes = 0;
  let bodyText = "";

  while (true) {
    const chunk = yield* Effect.tryPromise({
      try: () => reader.read(),
      catch: (cause) =>
        protocolError(
          `Reading the Quick Action response failed: ${safeCauseMessage(cause, "no diagnostic")}`,
        ),
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
      catch: (cause) =>
        protocolError(
          `Decoding the Quick Action response failed: ${safeCauseMessage(cause, "no diagnostic")}`,
        ),
    });
  }

  return (
    bodyText +
    (yield* Effect.try({
      try: () => decoder.decode(),
      catch: (cause) =>
        protocolError(
          `Decoding the Quick Action response failed: ${safeCauseMessage(cause, "no diagnostic")}`,
        ),
    }))
  );
}, Effect.scoped);

const renderErrors = (envelope: typeof QuickActionEnvelope.Type): string => {
  try {
    const encoded = JSON.stringify(envelope.errors ?? envelope);
    return encoded === undefined ? "The Quick Action reported failure" : encoded;
  } catch {
    return "The Quick Action reported failure";
  }
};

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

/** Bound and validate a links payload instead of letting `.make` throw on hostile values. */
const boundedLinks = (values: ReadonlyArray<Schema.Json>): ReadonlyArray<string> =>
  values
    .filter((value): value is string => typeof value === "string")
    .filter((value) => value.length > 0 && value.length <= 8 * 1024)
    .slice(0, 4_096);

const parseOutput = (
  action: PageCaptureAction,
  bodyText: string,
): PageCaptureOutput | PageCaptureNavigationError | PageCaptureProtocolError => {
  const envelope = decodeEnvelope(bodyText);
  if (Option.isSome(envelope) && !envelope.value.success) {
    return navigationError(renderErrors(envelope.value));
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
      if (!Array.isArray(values)) {
        return protocolError("The links Quick Action did not return an array of URLs");
      }
      return PageLinksCaptured.make({ links: boundedLinks(values) });
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

const makeCapture = (options: BrowserQuickActionCaptureOptions): PageCaptureCapture =>
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
      const workersAi = options.workersAi;
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
      try: () =>
        options.browser.quickAction(quickActionName(request.action), quickActionOptions(request)),
      catch: (cause) =>
        protocolError(
          `The browser binding rejected the Quick Action: ${safeCauseMessage(cause, "no diagnostic")}`,
        ),
    });
    const bodyText = yield* readBoundedResponse(response, request);
    if (response.status === 429) {
      const retryAfter = retryAfterMillis(response);
      return yield* PageCaptureRateLimitedError.make({
        implementation: browserQuickActionImplementation,
        reason: isQuotaMessage(bodyText) ? "quota" : "rate",
        ...(retryAfter === undefined ? {} : { retryAfterMillis: retryAfter }),
        message: boundedDiagnostic(bodyText),
      });
    }
    if (!response.ok) {
      const message = `The Quick Action answered ${response.status}: ${bodyText}`;
      if (response.status >= 500) {
        return yield* protocolError(message);
      }
      return yield* navigationError(message);
    }
    const output = parseOutput(request.action, bodyText);
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

/** Layer building the Browser Run Quick Action `PageCapture` from the resolved binding. */
export const browserQuickActionCaptureLayer = (
  options: BrowserQuickActionCaptureOptions,
): Layer.Layer<PageCapture> =>
  Layer.succeed(PageCapture)(PageCapture.of({ capture: makeCapture(options) }));

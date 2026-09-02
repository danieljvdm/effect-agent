import {
  PageCapture,
  PageCaptureInferencePolicyError,
  PageCaptureInferenceUse,
  PageCaptureNavigationError,
  PageCaptureOutputLimitError,
  PageCaptureProtocolError,
  PageCaptureRateLimitedError,
  PageCaptureResourceUse,
  PageCaptureResult,
  PageContentCaptured,
  PageLinksCaptured,
  PageMarkdownCaptured,
  PageScrapeCaptured,
  PageStructuredCaptured,
  PageCaptureUnsupportedError,
  SandboxImplementation,
  type PageCaptureAction,
  type PageCaptureCapture,
  type PageCaptureError,
  type PageCaptureOutput,
  type PageCaptureRequest,
} from "@effect-agent/sandbox";
import { Effect, Layer, Option, Redacted, Schema, Stream } from "effect";
import { HttpClient, HttpClientRequest, type HttpClientError } from "effect/unstable/http";

import {
  BrowserQuickActionWorkersAi,
  type BrowserQuickActionWorkersAiPolicy,
} from "./browser-quick-action.ts";

/** Node-safe REST PageCapture implementation; it never imports Worker runtime modules. */
export const browserRestCaptureImplementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "cloudflare-browser-rest",
});

const API_ORIGIN = "https://api.cloudflare.com";
const MAX_DIAGNOSTIC_LENGTH = 8_000;
const boundedDiagnostic = (message: string): string => message.slice(0, MAX_DIAGNOSTIC_LENGTH);

/** Explicit construction inputs; credentials are only projected into the fixed-origin Authorization header. */
export interface BrowserRestCaptureOptions {
  readonly accountId: string;
  readonly apiToken: Redacted.Redacted<string>;
}

const RestSuccessEnvelope = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Json,
  errors: Schema.optionalKey(Schema.Array(Schema.Unknown)),
  meta: Schema.optionalKey(Schema.Unknown),
});

const RestErrorEnvelope = Schema.Struct({
  success: Schema.Literal(false),
  errors: Schema.Array(
    Schema.Struct({
      message: Schema.String,
      code: Schema.optionalKey(Schema.Number),
    }),
  ),
  result: Schema.optionalKey(Schema.Json),
  meta: Schema.optionalKey(Schema.Unknown),
});

const RestEnvelope = Schema.Union([RestSuccessEnvelope, RestErrorEnvelope]);
const decodeEnvelope = Schema.decodeUnknownOption(Schema.fromJsonString(RestEnvelope));

const protocolError = (message: string, cause?: unknown): PageCaptureProtocolError =>
  PageCaptureProtocolError.make({
    implementation: browserRestCaptureImplementation,
    message: boundedDiagnostic(message),
    ...(cause === undefined ? {} : { cause }),
  });

const navigationError = (message: string, cause?: unknown): PageCaptureNavigationError =>
  PageCaptureNavigationError.make({
    implementation: browserRestCaptureImplementation,
    message: boundedDiagnostic(message),
    ...(cause === undefined ? {} : { cause }),
  });

const privateResponseCause = (
  bodyText: string,
  apiToken: Redacted.Redacted<string>,
): Error | undefined => {
  if (bodyText.length === 0) return undefined;
  const token = Redacted.value(apiToken);
  const diagnostic = boundedDiagnostic(bodyText);

  return new Error(token.length === 0 ? diagnostic : diagnostic.replaceAll(token, "[REDACTED]"));
};

const requestBody = (request: PageCaptureRequest): Record<string, unknown> => {
  const body: Record<string, unknown> =
    request.target._tag === "PageUrlTarget"
      ? { url: request.target.url }
      : { html: request.target.html };

  if (request.navigation !== undefined) {
    const goto: Record<string, unknown> = {};

    if (request.navigation.waitUntil !== undefined) goto.waitUntil = request.navigation.waitUntil;
    if (request.navigation.timeoutMillis !== undefined)
      goto.timeout = request.navigation.timeoutMillis;
    if (Object.keys(goto).length > 0) body.gotoOptions = goto;
    if (request.navigation.waitForSelector !== undefined) {
      body.waitForSelector = {
        selector: request.navigation.waitForSelector.selector,
        ...(request.navigation.waitForSelector.timeoutMillis === undefined
          ? {}
          : { timeout: request.navigation.waitForSelector.timeoutMillis }),
      };
    }
  }
  if (request.viewport !== undefined) body.viewport = request.viewport;
  if (request.resourcePolicy?.rejectResourceTypes !== undefined) {
    body.rejectResourceTypes = [...request.resourcePolicy.rejectResourceTypes];
  }
  if (request.resourcePolicy?.allowRequestPatterns !== undefined) {
    body.allowRequestPattern = [...request.resourcePolicy.allowRequestPatterns];
  }

  return body;
};

const actionName = (
  action: PageCaptureAction,
): "content" | "markdown" | "links" | "scrape" | "json" => {
  switch (action._tag) {
    case "CapturePageContent":
      return "content";
    case "CapturePageMarkdown":
      return "markdown";
    case "CapturePageLinks":
      return "links";
    case "CapturePageScrape":
      return "scrape";
    case "CapturePageStructured":
      return "json";
  }
};

const actionBody = (request: PageCaptureRequest): Record<string, unknown> => {
  const body = requestBody(request);

  switch (request.action._tag) {
    case "CapturePageContent":
    case "CapturePageMarkdown":
      return body;
    case "CapturePageLinks":
      return {
        ...body,
        ...(request.action.visibleLinksOnly === undefined
          ? {}
          : { visibleLinksOnly: request.action.visibleLinksOnly }),
      };
    case "CapturePageScrape":
      return {
        ...body,
        elements: request.action.selectors.map((selector) => ({ selector })),
      };
    case "CapturePageStructured":
      return {
        ...body,
        response_format: { type: "json_schema", json_schema: request.action.responseFormat },
        ...(request.action.prompt === undefined ? {} : { prompt: request.action.prompt }),
      };
  }
};

const retryAfterMillis = (
  headers: Readonly<Record<string, string | undefined>>,
): number | undefined => {
  const seconds = Number(headers["retry-after"]);

  if (!Number.isSafeInteger(seconds) || seconds < 0) return undefined;
  const millis = seconds * 1_000;

  return Number.isSafeInteger(millis) ? millis : undefined;
};

const browserMillis = (
  headers: Readonly<Record<string, string | undefined>>,
): number | undefined => {
  const millis = Number(headers["x-browser-ms-used"]);

  return Number.isSafeInteger(millis) && millis >= 0 ? millis : undefined;
};

/** Response framing is provider metadata, never content controlled by the rendered page. */
const isJsonResponse = (headers: Readonly<Record<string, string | undefined>>): boolean => {
  const contentType = headers["content-type"];

  if (contentType === undefined) return false;
  const mediaType = contentType.split(";", 1)[0]?.trim().toLowerCase();

  return mediaType === "application/json" || mediaType?.endsWith("+json") === true;
};

const readBoundedResponse = Effect.fn("BrowserRestCapture.readResponse")(function* (
  response: { readonly stream: Stream.Stream<Uint8Array, HttpClientError.HttpClientError> },
  request: PageCaptureRequest,
): Effect.fn.Return<string, PageCaptureError> {
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });

  const chunks = yield* Stream.runFoldEffect<
    Uint8Array,
    HttpClientError.HttpClientError,
    never,
    { readonly observed: number; readonly text: string },
    PageCaptureOutputLimitError | PageCaptureProtocolError,
    never
  >(
    response.stream,
    () => ({ observed: 0, text: "" }),
    (state, chunk) => {
      const observed = state.observed + chunk.byteLength;

      if (observed > request.limits.maxOutputBytes) {
        return Effect.fail(
          PageCaptureOutputLimitError.make({
            implementation: browserRestCaptureImplementation,
            limit: request.limits.maxOutputBytes,
            observed,
          }),
        );
      }

      return Effect.try({
        try: () => ({ observed, text: state.text + decoder.decode(chunk, { stream: true }) }),
        catch: (cause) => protocolError("Decoding the Browser Run response failed", cause),
      });
    },
  ).pipe(
    Effect.mapError((error) =>
      Schema.is(PageCaptureOutputLimitError)(error) || Schema.is(PageCaptureProtocolError)(error)
        ? error
        : protocolError("Reading the Browser Run response failed", error),
    ),
  );

  return yield* Effect.try({
    try: () => chunks.text + decoder.decode(),
    catch: (cause) => protocolError("Decoding the Browser Run response failed", cause),
  });
});

const parseOutput = (
  action: PageCaptureAction,
  bodyText: string,
  apiToken: Redacted.Redacted<string>,
): PageCaptureOutput | PageCaptureNavigationError | PageCaptureProtocolError => {
  const envelope = decodeEnvelope(bodyText);

  if (Option.isNone(envelope)) {
    return protocolError(
      "The Browser Run response did not carry a valid response envelope",
      privateResponseCause(bodyText, apiToken),
    );
  }
  if (!envelope.value.success) {
    return navigationError(
      "Browser Run reported a navigation failure",
      privateResponseCause(bodyText, apiToken),
    );
  }
  switch (action._tag) {
    case "CapturePageContent":
    case "CapturePageMarkdown":
      if (typeof envelope.value.result !== "string") {
        return protocolError("The Browser Run response envelope carried a non-text result");
      }

      return action._tag === "CapturePageContent"
        ? PageContentCaptured.make({ html: envelope.value.result })
        : PageMarkdownCaptured.make({ markdown: envelope.value.result });
    case "CapturePageLinks": {
      const decoded = Schema.decodeUnknownOption(PageLinksCaptured)({
        _tag: "PageLinksCaptured",
        links: envelope.value.result,
      });

      return Option.isSome(decoded)
        ? decoded.value
        : protocolError("Browser Run links did not return a bounded array of valid URLs");
    }
    case "CapturePageScrape": {
      const decoded = Schema.decodeUnknownOption(PageScrapeCaptured)({
        _tag: "PageScrapeCaptured",
        groups: envelope.value.result,
      });

      return Option.isSome(decoded)
        ? decoded.value
        : protocolError("Browser Run scrape did not return bounded grouped element records");
    }
    case "CapturePageStructured":
      return PageStructuredCaptured.make({ value: envelope.value.result });
  }
};

const isQuotaMessage = (text: string): boolean => /time limit|daily|quota/i.test(text);

const makeCapture = (
  client: HttpClient.HttpClient,
  options: BrowserRestCaptureOptions,
  workersAi?: BrowserQuickActionWorkersAiPolicy,
): PageCaptureCapture =>
  Effect.fn("BrowserRestCapture.capture")(function* (
    request: PageCaptureRequest,
  ): Effect.fn.Return<PageCaptureResult, PageCaptureError> {
    const usesWorkersAi = request.action._tag === "CapturePageStructured";

    if (usesWorkersAi) {
      if (workersAi === undefined) {
        return yield* PageCaptureUnsupportedError.make({
          implementation: browserRestCaptureImplementation,
          feature: "action",
          message:
            "Structured capture invokes separately billed Workers AI and requires an explicit authorization and accounting policy",
        });
      }
      yield* workersAi.authorizeAndAccount(request).pipe(
        Effect.mapError((cause) =>
          PageCaptureInferencePolicyError.make({
            implementation: browserRestCaptureImplementation,
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
    const action = actionName(request.action);
    const path = `${API_ORIGIN}/client/v4/accounts/${encodeURIComponent(options.accountId)}/browser-rendering/${action}`;

    const requestWithBody = yield* HttpClientRequest.post(path).pipe(
      request.engine === "kitesurf"
        ? HttpClientRequest.setUrlParam("browser", "kitesurf")
        : (value) => value,
      HttpClientRequest.bearerToken(options.apiToken),
      HttpClientRequest.acceptJson,
      HttpClientRequest.bodyJson(actionBody(request)),
      Effect.mapError((cause) => protocolError("Encoding the Browser Run request failed", cause)),
    );

    const response = yield* client
      .execute(requestWithBody)
      .pipe(
        Effect.mapError((cause) => protocolError("Calling the Browser Run REST API failed", cause)),
      );

    const bodyText = yield* readBoundedResponse(response, request);

    if (response.status === 429) {
      const reason = isQuotaMessage(bodyText) ? "quota" : "rate";

      return yield* PageCaptureRateLimitedError.make({
        implementation: browserRestCaptureImplementation,
        reason,
        ...(retryAfterMillis(response.headers) === undefined
          ? {}
          : { retryAfterMillis: retryAfterMillis(response.headers) }),
        ...(privateResponseCause(bodyText, options.apiToken) === undefined
          ? {}
          : { cause: privateResponseCause(bodyText, options.apiToken) }),
        message:
          reason === "quota"
            ? "Browser Run exceeded its browser quota"
            : "Browser Run was rate limited",
      });
    }
    if (response.status < 200 || response.status >= 300) {
      const error =
        response.status >= 500
          ? protocolError(
              `Browser Run answered HTTP ${String(response.status)}`,
              privateResponseCause(bodyText, options.apiToken),
            )
          : navigationError(
              `Browser Run answered HTTP ${String(response.status)}`,
              privateResponseCause(bodyText, options.apiToken),
            );

      return yield* error;
    }
    if (!isJsonResponse(response.headers)) {
      return yield* protocolError(
        "The Browser Run success response was not a JSON response envelope",
        privateResponseCause(bodyText, options.apiToken),
      );
    }
    const output = parseOutput(request.action, bodyText, options.apiToken);

    if (
      output._tag === "PageCaptureNavigationError" ||
      output._tag === "PageCaptureProtocolError"
    ) {
      return yield* output;
    }

    return PageCaptureResult.make({
      implementation: browserRestCaptureImplementation,
      output,
      resourceUse: PageCaptureResourceUse.make({
        ...(browserMillis(response.headers) === undefined
          ? {}
          : { browserMillis: browserMillis(response.headers) }),
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

/** REST PageCapture layer for Node and other non-Worker composition roots. */
export const browserRestCaptureLayer = (
  options: BrowserRestCaptureOptions,
): Layer.Layer<PageCapture, never, HttpClient.HttpClient> =>
  Layer.effect(
    PageCapture,
    Effect.map(HttpClient.HttpClient, (client) =>
      PageCapture.of({ capture: makeCapture(client, options) }),
    ),
  );

/** REST structured extraction additionally requires explicit Workers AI authorization/accounting. */
export const browserRestWorkersAiCaptureLayer = (
  options: BrowserRestCaptureOptions,
): Layer.Layer<PageCapture, never, HttpClient.HttpClient | BrowserQuickActionWorkersAi> =>
  Layer.effect(
    PageCapture,
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const workersAi = yield* BrowserQuickActionWorkersAi;

      return PageCapture.of({ capture: makeCapture(client, options, workersAi) });
    }),
  );

/** Explicit REST credentials and optional authorization for separately billed extraction. */
export interface CloudflareBrowserRestOptions extends BrowserRestCaptureOptions {
  readonly workersAi?: BrowserQuickActionWorkersAiPolicy;
}

/** Node-safe WebCapture handler assembly; the application supplies its HttpClient. */
export const CloudflareBrowserRest = {
  /**
   * Supports capture, scrape, and extraction definitions. Only PageCapture is supplied;
   * handler errors and other services remain visible. Extraction fails closed without
   * workersAi. Existing host policies, response limits, and scoped cleanup are unchanged.
   */
  layer: <A, E, R>(
    definition: { readonly handlers: Layer.Layer<A, E, R> },
    options: CloudflareBrowserRestOptions,
  ): Layer.Layer<A, E, Exclude<R, PageCapture> | HttpClient.HttpClient> => {
    const capture =
      options.workersAi === undefined
        ? browserRestCaptureLayer(options)
        : browserRestWorkersAiCaptureLayer(options).pipe(
            Layer.provide(BrowserQuickActionWorkersAi.layer(options.workersAi)),
          );

    return definition.handlers.pipe(Layer.provide(capture));
  },
};

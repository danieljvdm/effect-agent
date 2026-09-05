import { ToolExecutionClass as ToolExecutionClassAnnotation } from "@effect-agent/engine/DurableStep";
import {
  CapturePageContent,
  CapturePageLinks,
  CapturePageMarkdown,
  CapturePageScrape,
  CapturePageStructured,
  PageCapture,
  PageCaptureLimits,
  PageCaptureRequest,
  PageCaptureResponseFormat,
  PageResourcePolicy,
  PageScrapeCaptured,
  PageUrlTarget,
  type PageCaptureAction,
  type PageCaptureEngine,
  type PageCaptureError,
} from "@effect-agent/sandbox/PageCapture";
import { Effect, Option, Schema, type Layer } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

/**
 * Web capture (capability spec §9.2): native Effect AI Tools over the
 * stateless `PageCapture` port. The builders follow the Delegation pattern:
 * every security-relevant choice — the host allowlist, the action set, the
 * engine, the response byte budget — is fixed at construction and never
 * model-selectable. Construction fails closed on an invalid policy. The
 * handler Layer's `PageCapture` and Schema-decoding requirements stay
 * visible in `R`.
 *
 * Everything a capture returns is untrusted, attacker-influenced content
 * (security spec §9); results are bounded here and pass through the engine's
 * ordinary result redaction like any other Tool output.
 */

const maxFailureTextLength = 4 * 1024;
const BoundedFailureText = Schema.String.check(Schema.isMaxLength(maxFailureTextLength));
const BoundedErrorTag = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const BoundedUrl = Schema.NonEmptyString.check(Schema.isMaxLength(8 * 1024));
const BoundedContent = Schema.String.check(Schema.isMaxLength(1024 * 1024));
const BoundedSelector = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));

/** The model-selectable capture actions the first slice exposes. */
export const WebCaptureAction = Schema.Literals(["markdown", "content", "links"]);
export type WebCaptureAction = typeof WebCaptureAction.Type;

/** Model-decoded parameters: one absolute https URL plus one capture action. */
export const WebCaptureParameters = Schema.Struct({
  url: BoundedUrl,
  action: WebCaptureAction,
});

/** Model-decoded extraction parameters: one absolute https URL. */
export const WebCaptureExtractParameters = Schema.Struct({
  url: BoundedUrl,
});

/** Model-decoded scrape parameters: one allowed URL and 1..64 CSS selectors. */
export const WebCaptureScrapeParameters = Schema.Struct({
  url: BoundedUrl,
  selectors: Schema.Array(BoundedSelector).check(Schema.isMinLength(1), Schema.isMaxLength(64)),
});

/**
 * The bounded model-visible capture success. Exactly the field matching the
 * requested action is present.
 */
export class WebCaptureSuccess extends Schema.Class<WebCaptureSuccess>(
  "@effect-agent/capabilities/WebCaptureSuccess",
)({
  url: BoundedUrl,
  action: WebCaptureAction,
  markdown: Schema.optionalKey(BoundedContent),
  html: Schema.optionalKey(BoundedContent),
  links: Schema.optionalKey(Schema.Array(BoundedUrl).check(Schema.isMaxLength(4_096))),
}) {}

/**
 * The bounded model-visible failure envelope. `failureMode: "return"` turns
 * it into a failed Tool result, so a model can pick a different target or
 * action without a blind retry. `retryAfterMillis` carries the platform's
 * backoff hint when the capture was rate-limited.
 */
export class WebCaptureFailure extends Schema.TaggedError<WebCaptureFailure>()(
  "WebCaptureFailure",
  {
    errorTag: BoundedErrorTag,
    message: BoundedFailureText,
    retryAfterMillis: Schema.optionalKey(Schema.Natural),
  },
) {}

/** The bounded model-visible selector scrape success. */
export class WebCaptureScrapeSuccess extends Schema.Class<WebCaptureScrapeSuccess>(
  "@effect-agent/capabilities/WebCaptureScrapeSuccess",
)({
  url: BoundedUrl,
  groups: PageScrapeCaptured.fields.groups,
}) {}

/** The native Effect AI Tool created by `WebCapture.make`. */
export type WebCaptureTool<Name extends string> = Tool.Tool<
  Name,
  {
    readonly parameters: typeof WebCaptureParameters;
    readonly success: typeof WebCaptureSuccess;
    readonly failure: typeof WebCaptureFailure;
    readonly failureMode: "return";
  }
>;

/** The native Effect AI Tool created by `WebCapture.makeExtract`. */
export type WebCaptureExtractTool<Name extends string, S extends Schema.Top> = Tool.Tool<
  Name,
  {
    readonly parameters: typeof WebCaptureExtractParameters;
    readonly success: S;
    readonly failure: typeof WebCaptureFailure;
    readonly failureMode: "return";
  },
  S["DecodingServices"]
>;

/** The native Effect AI Tool created by `WebCapture.makeScrape`. */
export type WebCaptureScrapeTool<Name extends string> = Tool.Tool<
  Name,
  {
    readonly parameters: typeof WebCaptureScrapeParameters;
    readonly success: typeof WebCaptureScrapeSuccess;
    readonly failure: typeof WebCaptureFailure;
    readonly failureMode: "return";
  }
>;

/** Singleton Tool record provided by one web-capture handler Layer. */
export type WebCaptureTools<Name extends string> = {
  readonly [Key in Name]: WebCaptureTool<Name>;
};

/** Singleton Tool record provided by one extraction handler Layer. */
export type WebCaptureExtractTools<Name extends string, S extends Schema.Top> = {
  readonly [Key in Name]: WebCaptureExtractTool<Name, S>;
};

/** Singleton Tool record provided by one selector-scrape handler Layer. */
export type WebCaptureScrapeTools<Name extends string> = {
  readonly [Key in Name]: WebCaptureScrapeTool<Name>;
};

/** Construction requirements of a schema-shaped extraction handler Layer. */
export type WebCaptureExtractLayerRequirements<S extends Schema.Top> =
  | PageCapture
  | S["DecodingServices"];

interface WebCaptureSharedOptions {
  /** Model-visible description; the builder appends the policy contract. */
  readonly description: string;
  /**
   * Allowed target hosts: bare host names (`docs.example.com`) or one-level
   * wildcards (`*.example.com`, matching the apex and every subdomain). The
   * list must be non-empty — capture is deny-by-default (security spec §9).
   */
  readonly urls: ReadonlyArray<string>;
  /** Rendering engine; defaults to `chromium`. */
  readonly engine?: PageCaptureEngine | undefined;
  /**
   * Response byte budget measured on the UTF-8 encoded platform response.
   * Default 131072 (128 KiB); construction fails closed outside
   * [1024, 1048576].
   */
  readonly maxResponseBytes?: number | undefined;
}

export interface WebCaptureOptions extends WebCaptureSharedOptions {
  /** Model-selectable actions; defaults to every first-slice action. */
  readonly actions?: ReadonlyArray<WebCaptureAction> | undefined;
}

export interface WebCaptureExtractOptions<S extends Schema.Top> extends WebCaptureSharedOptions {
  /**
   * The extraction Schema. Its derived JSON Schema shapes the platform-side
   * extraction, and the untrusted result is decoded through this exact
   * Schema before it becomes the Tool success. Any decoding services remain
   * visible in the handler Layer's requirements.
   */
  readonly schema: S;
  /** Optional natural-language guidance sent alongside the derived schema. */
  readonly prompt?: string | undefined;
}

export interface WebCaptureScrapeOptions extends WebCaptureSharedOptions {}

/**
 * An immutable web-capture definition: one model-facing Tool over a fixed
 * URL policy, plus the handler Layer that runs captures through the
 * `PageCapture` port. It owns no acquired resources.
 */
export interface WebCaptureDefinition<Name extends string> {
  readonly name: Name;
  /** The assembled model-facing description including the policy contract. */
  readonly description: string;
  /** The normalized allowed-host patterns. */
  readonly urlPatterns: ReadonlyArray<string>;
  readonly actions: ReadonlyArray<WebCaptureAction>;
  readonly engine: PageCaptureEngine;
  readonly maxResponseBytes: number;
  /** The ordinary Effect AI Tool to include in the model-facing Toolkit. */
  readonly tool: WebCaptureTool<Name>;
  /** Handler Layer; the `PageCapture` requirement stays visible in `R`. */
  readonly handlers: Layer.Layer<Tool.HandlersFor<WebCaptureTools<Name>>, never, PageCapture>;
}

/** An immutable schema-shaped extraction definition. */
export interface WebCaptureExtractDefinition<Name extends string, S extends Schema.Top> {
  readonly name: Name;
  readonly description: string;
  readonly urlPatterns: ReadonlyArray<string>;
  readonly engine: PageCaptureEngine;
  readonly maxResponseBytes: number;
  /** The JSON Schema document derived from the extraction Schema. */
  readonly responseFormat: PageCaptureResponseFormat;
  readonly tool: WebCaptureExtractTool<Name, S>;
  readonly handlers: Layer.Layer<
    Tool.HandlersFor<WebCaptureExtractTools<Name, S>>,
    never,
    WebCaptureExtractLayerRequirements<S>
  >;
}

/** An immutable selector-scrape definition. */
export interface WebCaptureScrapeDefinition<Name extends string> {
  readonly name: Name;
  readonly description: string;
  readonly urlPatterns: ReadonlyArray<string>;
  readonly engine: PageCaptureEngine;
  readonly maxResponseBytes: number;
  readonly tool: WebCaptureScrapeTool<Name>;
  readonly handlers: Layer.Layer<Tool.HandlersFor<WebCaptureScrapeTools<Name>>, never, PageCapture>;
}

const defaultMaxResponseBytes = 128 * 1024;
const allActions: ReadonlyArray<WebCaptureAction> = ["markdown", "content", "links"];

const HOST_PATTERN = /^(\*\.)?[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/;

const compileUrlPolicy = (patterns: ReadonlyArray<string>): ReadonlyArray<string> => {
  if (patterns.length === 0) {
    throw new Error(
      "Web capture requires at least one allowed host pattern; capture is deny-by-default",
    );
  }

  return Object.freeze(
    patterns.map((raw) => {
      const pattern = raw.trim().toLowerCase();

      if (!HOST_PATTERN.test(pattern)) {
        throw new Error(
          `Web capture host pattern ${JSON.stringify(raw)} must be a bare host name such as docs.example.com or *.example.com — no scheme, path, port, or credentials`,
        );
      }

      return pattern;
    }),
  );
};

const compileResourcePolicy = (patterns: ReadonlyArray<string>): PageResourcePolicy =>
  PageResourcePolicy.make({
    allowRequestPatterns: patterns.map((pattern) => {
      const wildcard = pattern.startsWith("*.");
      const escapedHost = (wildcard ? pattern.slice(2) : pattern).replaceAll(".", "\\.");
      const subdomains = wildcard ? "(?:[a-z0-9-]+\\.)*" : "";

      return `^https://${subdomains}${escapedHost}(?::[0-9]+)?(?:[/?#]|$)`;
    }),
  });

const validateMaxResponseBytes = (value: number | undefined): number => {
  const bytes = value ?? defaultMaxResponseBytes;

  // Fail closed on an invalid budget: NaN would make every comparison false.
  if (!Number.isSafeInteger(bytes) || bytes < 1_024 || bytes > 1024 * 1024) {
    throw new Error(
      `Web capture maxResponseBytes must be an integer between 1024 and ${1024 * 1024}; received ${String(value)}`,
    );
  }

  return bytes;
};

const hostMatches = (host: string, pattern: string): boolean => {
  if (pattern.startsWith("*.")) {
    const base = pattern.slice(2);

    return host === base || host.endsWith(`.${base}`);
  }

  return host === pattern;
};

const failure = (errorTag: string, message: string, retryAfterMillis?: number): WebCaptureFailure =>
  WebCaptureFailure.make({
    errorTag,
    message: message.slice(0, maxFailureTextLength),
    ...(retryAfterMillis === undefined ? {} : { retryAfterMillis }),
  });

const decodeUrl = Schema.decodeUnknownOption(Schema.URLFromString);

/**
 * Fail-closed target validation over untrusted model output: absolute https
 * URLs without embedded credentials, whose host matches the construction-time
 * allowlist. Returns the denial, or `undefined` for an allowed target.
 */
const deniedUrl = (raw: string, patterns: ReadonlyArray<string>): WebCaptureFailure | undefined => {
  const parsed = decodeUrl(raw);

  if (Option.isNone(parsed)) {
    return failure("WebCaptureInvalidUrl", `${raw.slice(0, 256)} is not an absolute URL`);
  }
  const url = parsed.value;

  if (url.protocol !== "https:") {
    return failure("WebCaptureUrlDenied", "Only https:// targets are allowed");
  }
  if (url.username !== "" || url.password !== "") {
    return failure("WebCaptureUrlDenied", "URLs carrying credentials are not allowed");
  }
  const host = url.hostname.toLowerCase();

  if (!patterns.some((pattern) => hostMatches(host, pattern))) {
    return failure(
      "WebCaptureUrlDenied",
      `Host ${host} is outside the allowed set: ${patterns.join(", ")}`,
    );
  }

  return undefined;
};

const portFailure = (error: PageCaptureError): WebCaptureFailure => {
  switch (error._tag) {
    case "PageCaptureRateLimitedError": {
      return failure(
        error._tag,
        error.message === "" ? "The platform rate-limited the capture" : error.message,
        error.retryAfterMillis,
      );
    }
    case "PageCaptureOutputLimitError": {
      return failure(
        error._tag,
        `The page response of ${error.observed} bytes exceeds the ${error.limit}-byte budget; capture a smaller page or a narrower action`,
      );
    }
    case "PageCaptureNavigationError":
    case "PageCaptureInferencePolicyError":
    case "PageCaptureProtocolError":
    case "PageCaptureUnsupportedError": {
      return failure(error._tag, error.message);
    }
  }
};

const policyContract = (
  actionsText: string,
  patterns: ReadonlyArray<string>,
  maxResponseBytes: number,
): string =>
  `Allowed actions: ${actionsText}. Allowed hosts: ${patterns.join(", ")} (https:// only). Responses are bounded to ${maxResponseBytes} bytes; a larger page fails with a typed result you can react to.`;

const captureAction = (action: WebCaptureAction): PageCaptureAction => {
  switch (action) {
    case "markdown": {
      return CapturePageMarkdown.make({});
    }
    case "content": {
      return CapturePageContent.make({});
    }
    case "links": {
      return CapturePageLinks.make({});
    }
  }
};

const make = <const Name extends string>(
  name: Name,
  options: WebCaptureOptions,
): WebCaptureDefinition<Name> => {
  const patterns = compileUrlPolicy(options.urls);
  const resourcePolicy = compileResourcePolicy(patterns);
  const maxResponseBytes = validateMaxResponseBytes(options.maxResponseBytes);
  const engine = options.engine ?? "chromium";
  const actions = Object.freeze([...new Set(options.actions ?? allActions)]);

  if (actions.length === 0) {
    throw new Error("Web capture requires at least one model-selectable action");
  }

  const description = [
    options.description,
    "",
    policyContract(actions.join(", "), patterns, maxResponseBytes),
  ].join("\n");

  const tool = Tool.make(name, {
    description,
    parameters: WebCaptureParameters,
    success: WebCaptureSuccess,
    failure: WebCaptureFailure,
    failureMode: "return",
  })
    .annotate(Tool.Readonly, false)
    .annotate(ToolExecutionClassAnnotation, "uncertain") as WebCaptureTool<Name>;

  const toolkit = Toolkit.make(tool);
  const allowed = new Set(actions);

  const build = Effect.gen(function* () {
    const pageCapture = yield* PageCapture;

    const invoke = Effect.fn(`WebCapture.${name}`)(function* (parameters: {
      readonly url: string;
      readonly action: WebCaptureAction;
    }) {
      if (!allowed.has(parameters.action)) {
        return yield* failure(
          "WebCaptureActionDenied",
          `Action ${parameters.action} is not enabled; allowed actions: ${actions.join(", ")}`,
        );
      }
      const denied = deniedUrl(parameters.url, patterns);

      if (denied !== undefined) {
        return yield* denied;
      }

      const result = yield* pageCapture
        .capture(
          PageCaptureRequest.make({
            target: PageUrlTarget.make({ url: parameters.url }),
            action: captureAction(parameters.action),
            engine,
            limits: PageCaptureLimits.make({ maxOutputBytes: maxResponseBytes }),
            resourcePolicy,
          }),
        )
        .pipe(Effect.catch((error) => Effect.fail(portFailure(error))));

      const output = result.output;

      if (parameters.action === "markdown" && output._tag === "PageMarkdownCaptured") {
        return WebCaptureSuccess.make({
          url: parameters.url,
          action: parameters.action,
          markdown: output.markdown,
        });
      }
      if (parameters.action === "content" && output._tag === "PageContentCaptured") {
        return WebCaptureSuccess.make({
          url: parameters.url,
          action: parameters.action,
          html: output.html,
        });
      }
      if (parameters.action === "links" && output._tag === "PageLinksCaptured") {
        return WebCaptureSuccess.make({
          url: parameters.url,
          action: parameters.action,
          links: output.links,
        });
      }

      return yield* failure(
        "WebCaptureProtocolMismatch",
        `The capture adapter answered a ${parameters.action} request with ${output._tag}`,
      );
    });

    return { [name]: invoke } as unknown as Toolkit.HandlersFrom<WebCaptureTools<Name>>;
  });

  /**
   * TypeScript cannot unify the two spellings of the singleton Tool record
   * over a generic `Name` (the same limit as Code Mode); the assertions pin
   * the layer to its documented requirement surface and never bypass
   * validation.
   */
  const handlers = toolkit.toLayer(
    build as unknown as Effect.Effect<
      Toolkit.HandlersFrom<Toolkit.ToolsByName<readonly [WebCaptureTool<Name>]>>,
      never,
      PageCapture
    >,
  ) as unknown as Layer.Layer<Tool.HandlersFor<WebCaptureTools<Name>>, never, PageCapture>;

  return Object.freeze({
    name,
    description,
    urlPatterns: patterns,
    actions,
    engine,
    maxResponseBytes,
    tool,
    handlers,
  });
};

const makeExtract = <const Name extends string, S extends Schema.Top>(
  name: Name,
  options: WebCaptureExtractOptions<S>,
): WebCaptureExtractDefinition<Name, S> => {
  const patterns = compileUrlPolicy(options.urls);
  const resourcePolicy = compileResourcePolicy(patterns);
  const maxResponseBytes = validateMaxResponseBytes(options.maxResponseBytes);
  const engine = options.engine ?? "chromium";
  const prompt = options.prompt;

  if (prompt !== undefined && (prompt.length === 0 || prompt.length > 8_192)) {
    throw new Error("Web capture extraction prompt must be between 1 and 8192 characters");
  }

  // The derived JSON Schema is BOTH the platform-side response format and
  // documentation that the deriver can express the Schema at all; anything it
  // cannot derive fails construction closed rather than degrading at runtime.
  const derived = ((): PageCaptureResponseFormat => {
    const jsonSchema = Tool.getJsonSchemaFromSchema(options.schema);
    const decoded = Schema.decodeUnknownOption(PageCaptureResponseFormat)(jsonSchema);

    if (Option.isNone(decoded)) {
      throw new Error(
        `Web capture cannot derive a bounded object JSON response format from the extraction Schema for ${name}`,
      );
    }

    return decoded.value;
  })();

  const description = [
    options.description,
    "",
    `Extracts structured data matching a fixed schema from the rendered page. ${policyContract("structured extraction", patterns, maxResponseBytes)}`,
  ].join("\n");

  const tool = Tool.make(name, {
    description,
    parameters: WebCaptureExtractParameters,
    success: options.schema,
    failure: WebCaptureFailure,
    failureMode: "return",
  })
    .annotate(Tool.Readonly, false)
    .annotate(ToolExecutionClassAnnotation, "uncertain") as WebCaptureExtractTool<Name, S>;

  const toolkit = Toolkit.make(tool);
  const decodeExtracted = Schema.decodeUnknownEffect(options.schema);

  const build = Effect.gen(function* () {
    const pageCapture = yield* PageCapture;

    const invoke = Effect.fn(`WebCapture.${name}`)(function* (parameters: {
      readonly url: string;
    }) {
      const denied = deniedUrl(parameters.url, patterns);

      if (denied !== undefined) {
        return yield* denied;
      }

      const result = yield* pageCapture
        .capture(
          PageCaptureRequest.make({
            target: PageUrlTarget.make({ url: parameters.url }),
            action: CapturePageStructured.make({
              responseFormat: derived,
              ...(prompt === undefined ? {} : { prompt }),
            }),
            engine,
            limits: PageCaptureLimits.make({ maxOutputBytes: maxResponseBytes }),
            resourcePolicy,
          }),
        )
        .pipe(Effect.catch((error) => Effect.fail(portFailure(error))));

      if (result.output._tag !== "PageStructuredCaptured") {
        return yield* failure(
          "WebCaptureProtocolMismatch",
          `The capture adapter answered a structured request with ${result.output._tag}`,
        );
      }

      // The platform's extraction is untrusted; only the original Effect
      // Schema decides whether the value is the promised shape.
      return yield* decodeExtracted(result.output.value).pipe(
        Effect.mapError(() =>
          failure(
            "WebCaptureDecodeFailed",
            "The extracted value did not match the expected schema",
          ),
        ),
      );
    });

    return { [name]: invoke } as unknown as Toolkit.HandlersFrom<WebCaptureExtractTools<Name, S>>;
  });

  // Same private-assertion contract as `make` above.
  const handlers = toolkit.toLayer(
    build as unknown as Effect.Effect<
      Toolkit.HandlersFrom<Toolkit.ToolsByName<readonly [WebCaptureExtractTool<Name, S>]>>,
      never,
      WebCaptureExtractLayerRequirements<S>
    >,
  ) as unknown as Layer.Layer<
    Tool.HandlersFor<WebCaptureExtractTools<Name, S>>,
    never,
    WebCaptureExtractLayerRequirements<S>
  >;

  return Object.freeze({
    name,
    description,
    urlPatterns: patterns,
    engine,
    maxResponseBytes,
    responseFormat: derived,
    tool,
    handlers,
  });
};

const makeScrape = <const Name extends string>(
  name: Name,
  options: WebCaptureScrapeOptions,
): WebCaptureScrapeDefinition<Name> => {
  const patterns = compileUrlPolicy(options.urls);
  const resourcePolicy = compileResourcePolicy(patterns);
  const maxResponseBytes = validateMaxResponseBytes(options.maxResponseBytes);
  const engine = options.engine ?? "chromium";

  const description = [
    options.description,
    "",
    `Scrapes rendered elements for 1 to 64 CSS selectors. ${policyContract("selector scrape", patterns, maxResponseBytes)}`,
  ].join("\n");

  const tool = Tool.make(name, {
    description,
    parameters: WebCaptureScrapeParameters,
    success: WebCaptureScrapeSuccess,
    failure: WebCaptureFailure,
    failureMode: "return",
  })
    .annotate(Tool.Readonly, false)
    .annotate(ToolExecutionClassAnnotation, "uncertain") as WebCaptureScrapeTool<Name>;

  const toolkit = Toolkit.make(tool);

  const build = Effect.gen(function* () {
    const pageCapture = yield* PageCapture;

    const invoke = Effect.fn(`WebCapture.${name}`)(function* (parameters: {
      readonly url: string;
      readonly selectors: ReadonlyArray<string>;
    }) {
      const denied = deniedUrl(parameters.url, patterns);

      if (denied !== undefined) {
        return yield* denied;
      }

      const result = yield* pageCapture
        .capture(
          PageCaptureRequest.make({
            target: PageUrlTarget.make({ url: parameters.url }),
            action: CapturePageScrape.make({ selectors: parameters.selectors }),
            engine,
            limits: PageCaptureLimits.make({ maxOutputBytes: maxResponseBytes }),
            resourcePolicy,
          }),
        )
        .pipe(Effect.catch((error) => Effect.fail(portFailure(error))));

      if (result.output._tag !== "PageScrapeCaptured") {
        return yield* failure(
          "WebCaptureProtocolMismatch",
          `The capture adapter answered a selector scrape request with ${result.output._tag}`,
        );
      }

      return WebCaptureScrapeSuccess.make({
        url: parameters.url,
        groups: result.output.groups,
      });
    });

    return { [name]: invoke } as unknown as Toolkit.HandlersFrom<WebCaptureScrapeTools<Name>>;
  });

  const handlers = toolkit.toLayer(
    build as unknown as Effect.Effect<
      Toolkit.HandlersFrom<Toolkit.ToolsByName<readonly [WebCaptureScrapeTool<Name>]>>,
      never,
      PageCapture
    >,
  ) as unknown as Layer.Layer<Tool.HandlersFor<WebCaptureScrapeTools<Name>>, never, PageCapture>;

  return Object.freeze({
    name,
    description,
    urlPatterns: patterns,
    engine,
    maxResponseBytes,
    tool,
    handlers,
  });
};

/** Web capture builder namespace (capability spec §9.2). */
export { make, makeExtract, makeScrape };

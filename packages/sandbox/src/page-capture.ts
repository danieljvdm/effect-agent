import { Context, Encoding, Predicate, Schema, type Effect } from "effect";

import { SANDBOX_DIAGNOSTIC_MAX_LENGTH, SandboxImplementation } from "./sandbox.ts";

/**
 * Page capture (capability spec §9.2): one stateless, schema-first rendering
 * pass over a remote or supplied page — navigate (or load raw HTML), render
 * with JavaScript, and return exactly one bounded output. It is a sibling of
 * the command-shaped `Sandbox` and callback-shaped `CodeExecutor` ports: a
 * browser is intrinsically an egress device, so it carries its own explicit
 * capture contract instead of widening the sandbox network policy that every
 * existing adapter rejects.
 *
 * Everything a capture returns is untrusted, attacker-influenced content
 * (security spec §9). Stateful browser sessions, screenshots, PDFs, snapshot
 * bundles, crawling, and accessibility trees are deliberately outside this
 * port's first slice.
 */

const MAX_OUTPUT_BYTES = 8 * 1024 * 1024;
const MAX_LINKS = 4_096;
const MAX_SCRAPE_SELECTORS = 64;
const MAX_SCRAPE_ELEMENTS = 4_096;
const MAX_SCRAPE_ATTRIBUTES = 64;
const MAX_RESPONSE_FORMAT_BYTES = 64 * 1024;
const MAX_RESPONSE_FORMAT_DEPTH = 32;
const MAX_RESPONSE_FORMAT_NODES = 4_096;
const MAX_RESPONSE_FORMAT_COLLECTION_LENGTH = 256;
// Schema `isMaxLength` counts UTF-16 elements: these are transport sanity
// bounds. The authoritative byte budget is `PageCaptureLimits.maxOutputBytes`,
// which adapters enforce on the UTF-8 encoded response.
const BoundedOutputText = Schema.String.check(Schema.isMaxLength(MAX_OUTPUT_BYTES));
const BoundedUrl = Schema.NonEmptyString.check(Schema.isMaxLength(8 * 1024));
const BoundedHtml = Schema.NonEmptyString.check(Schema.isMaxLength(2 * 1024 * 1024));
const BoundedPrompt = Schema.NonEmptyString.check(Schema.isMaxLength(8 * 1024));
const BoundedSelector = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const BoundedAttributeName = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const BoundedAttributeValue = Schema.String.check(Schema.isMaxLength(64 * 1_024));
const BoundedScrapeText = Schema.String.check(Schema.isMaxLength(1024 * 1_024));
const BoundedScrapeHtml = Schema.String.check(Schema.isMaxLength(2 * 1024 * 1_024));
const BoundedPattern = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const BoundedMessage = Schema.String.check(Schema.isMaxLength(SANDBOX_DIAGNOSTIC_MAX_LENGTH));
const BoundedInferenceProvider = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const BoundedSchemaProperty = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const BoundedSchemaText = Schema.String.check(Schema.isMaxLength(8 * 1024));
const BoundedSchemaReference = Schema.NonEmptyString.check(Schema.isMaxLength(8 * 1024));
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
/** Bounded by the platform's 60-second browser inactivity ceiling. */
const BoundedTimeoutMillis = PositiveInt.check(Schema.isLessThanOrEqualTo(60_000));

// The platform-neutral package targets bare ES2023, while supported runtimes
// provide the WHATWG URL constructor. Missing or malformed runtime support
// denies URLs instead of guessing at web syntax or importing a Node builtin.
const pageUrlConstructor = Reflect.get(globalThis, "URL");

const isCredentialFreeWebUrl = (value: string, allowHttp: boolean): boolean => {
  if (typeof pageUrlConstructor !== "function") return false;

  try {
    const parsed: unknown = Reflect.construct(pageUrlConstructor, [value]);
    if (!Predicate.isObject(parsed)) return false;

    const protocol: unknown = Reflect.get(parsed, "protocol");
    const hostname: unknown = Reflect.get(parsed, "hostname");
    const username: unknown = Reflect.get(parsed, "username");
    const password: unknown = Reflect.get(parsed, "password");

    return (
      (protocol === "https:" || (allowHttp && protocol === "http:")) &&
      typeof hostname === "string" &&
      hostname.length > 0 &&
      username === "" &&
      password === ""
    );
  } catch {
    return false;
  }
};

/** Absolute, bounded HTTPS navigation URL that never carries embedded credentials. */
export const PageCaptureTargetUrl = BoundedUrl.check(
  Schema.makeFilter((value) => isCredentialFreeWebUrl(value, false), {
    title: "an absolute HTTPS URL without embedded credentials",
  }),
);
export type PageCaptureTargetUrl = typeof PageCaptureTargetUrl.Type;

/** Discovered HTTP(S) links are data, never navigation authority, and carry no credentials. */
export const PageCaptureLinkUrl = BoundedUrl.check(
  Schema.makeFilter((value) => isCredentialFreeWebUrl(value, true), {
    title: "an absolute HTTP or HTTPS URL without embedded credentials",
  }),
);
export type PageCaptureLinkUrl = typeof PageCaptureLinkUrl.Type;

const ResponseFormatPrimitive = Schema.Literals([
  "string",
  "number",
  "boolean",
  "array",
  "object",
  "null",
  "integer",
]);
const ResponseFormatPrimitiveList = Schema.Array(ResponseFormatPrimitive).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(7),
  Schema.isUnique(),
);

type ResponseFormatNode = Schema.JsonObject & {
  readonly $schema?: string | undefined;
  readonly $id?: string | undefined;
  readonly $ref?: string | undefined;
  readonly $anchor?: string | undefined;
  readonly $dynamicRef?: string | undefined;
  readonly $dynamicAnchor?: string | undefined;
  readonly $comment?: string | undefined;
  readonly title?: string | undefined;
  readonly description?: string | undefined;
  readonly default?: Schema.Json | undefined;
  readonly deprecated?: boolean | undefined;
  readonly readOnly?: boolean | undefined;
  readonly writeOnly?: boolean | undefined;
  readonly examples?: ReadonlyArray<Schema.Json> | undefined;
  readonly type?:
    | typeof ResponseFormatPrimitive.Type
    | typeof ResponseFormatPrimitiveList.Type
    | undefined;
  readonly enum?: ReadonlyArray<Schema.Json> | undefined;
  readonly const?: Schema.Json | undefined;
  readonly multipleOf?: number | undefined;
  readonly maximum?: number | undefined;
  readonly exclusiveMaximum?: number | undefined;
  readonly minimum?: number | undefined;
  readonly exclusiveMinimum?: number | undefined;
  readonly maxLength?: number | undefined;
  readonly minLength?: number | undefined;
  readonly pattern?: string | undefined;
  readonly format?: string | undefined;
  readonly maxItems?: number | undefined;
  readonly minItems?: number | undefined;
  readonly uniqueItems?: boolean | undefined;
  readonly prefixItems?: ReadonlyArray<ResponseFormatNode> | undefined;
  readonly properties?: Readonly<Record<string, ResponseFormatNode>> | undefined;
  readonly patternProperties?: Readonly<Record<string, ResponseFormatNode>> | undefined;
  readonly required?: ReadonlyArray<string> | undefined;
  readonly dependentRequired?: Readonly<Record<string, ReadonlyArray<string>>> | undefined;
  readonly dependentSchemas?: Readonly<Record<string, ResponseFormatNode>> | undefined;
  readonly items?: ResponseFormatNode | boolean | undefined;
  readonly additionalItems?: ResponseFormatNode | boolean | undefined;
  readonly unevaluatedItems?: ResponseFormatNode | boolean | undefined;
  readonly contains?: ResponseFormatNode | boolean | undefined;
  readonly minContains?: number | undefined;
  readonly maxContains?: number | undefined;
  readonly additionalProperties?: ResponseFormatNode | boolean | undefined;
  readonly unevaluatedProperties?: ResponseFormatNode | boolean | undefined;
  readonly propertyNames?: ResponseFormatNode | boolean | undefined;
  readonly minProperties?: number | undefined;
  readonly maxProperties?: number | undefined;
  readonly anyOf?: ReadonlyArray<ResponseFormatNode> | undefined;
  readonly oneOf?: ReadonlyArray<ResponseFormatNode> | undefined;
  readonly allOf?: ReadonlyArray<ResponseFormatNode> | undefined;
  readonly not?: ResponseFormatNode | boolean | undefined;
  readonly contentEncoding?: string | undefined;
  readonly contentMediaType?: string | undefined;
  readonly contentSchema?: ResponseFormatNode | boolean | undefined;
  readonly $defs?: Readonly<Record<string, ResponseFormatNode>> | undefined;
  readonly definitions?: Readonly<Record<string, ResponseFormatNode>> | undefined;
};

const ResponseFormatNode: Schema.Codec<ResponseFormatNode> = Schema.suspend(
  (): Schema.Codec<ResponseFormatNode> =>
    Schema.Struct(responseFormatFields()).pipe(
      Schema.annotate({ parseOptions: { onExcessProperty: "error" } }),
    ),
);

const responseFormatFields = () => {
  const boundedNodes = Schema.Array(ResponseFormatNode).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(MAX_RESPONSE_FORMAT_COLLECTION_LENGTH),
  );
  const boundedDefinitions = Schema.Record(BoundedSchemaProperty, ResponseFormatNode).check(
    Schema.isMaxProperties(MAX_RESPONSE_FORMAT_COLLECTION_LENGTH),
  );
  const boundedRequired = Schema.Array(BoundedSchemaProperty).check(
    Schema.isMaxLength(MAX_RESPONSE_FORMAT_COLLECTION_LENGTH),
    Schema.isUnique(),
  );
  const subschema = Schema.Union([Schema.Boolean, ResponseFormatNode]);

  return {
    $schema: Schema.optionalKey(BoundedSchemaReference),
    $id: Schema.optionalKey(BoundedSchemaReference),
    $ref: Schema.optionalKey(BoundedSchemaReference),
    $anchor: Schema.optionalKey(BoundedSchemaReference),
    $dynamicRef: Schema.optionalKey(BoundedSchemaReference),
    $dynamicAnchor: Schema.optionalKey(BoundedSchemaReference),
    $comment: Schema.optionalKey(BoundedSchemaText),
    title: Schema.optionalKey(BoundedSchemaText),
    description: Schema.optionalKey(BoundedSchemaText),
    default: Schema.optionalKey(Schema.Json),
    deprecated: Schema.optionalKey(Schema.Boolean),
    readOnly: Schema.optionalKey(Schema.Boolean),
    writeOnly: Schema.optionalKey(Schema.Boolean),
    examples: Schema.optionalKey(
      Schema.Array(Schema.Json).check(Schema.isMaxLength(MAX_RESPONSE_FORMAT_COLLECTION_LENGTH)),
    ),
    type: Schema.optionalKey(Schema.Union([ResponseFormatPrimitive, ResponseFormatPrimitiveList])),
    enum: Schema.optionalKey(
      Schema.Array(Schema.Json).check(
        Schema.isMinLength(1),
        Schema.isMaxLength(MAX_RESPONSE_FORMAT_COLLECTION_LENGTH),
        Schema.isUnique(),
      ),
    ),
    const: Schema.optionalKey(Schema.Json),
    multipleOf: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThan(0))),
    maximum: Schema.optionalKey(Schema.Finite),
    exclusiveMaximum: Schema.optionalKey(Schema.Finite),
    minimum: Schema.optionalKey(Schema.Finite),
    exclusiveMinimum: Schema.optionalKey(Schema.Finite),
    maxLength: Schema.optionalKey(Schema.Natural),
    minLength: Schema.optionalKey(Schema.Natural),
    pattern: Schema.optionalKey(BoundedSchemaText),
    format: Schema.optionalKey(BoundedSchemaText),
    maxItems: Schema.optionalKey(Schema.Natural),
    minItems: Schema.optionalKey(Schema.Natural),
    uniqueItems: Schema.optionalKey(Schema.Boolean),
    prefixItems: Schema.optionalKey(boundedNodes),
    items: Schema.optionalKey(subschema),
    additionalItems: Schema.optionalKey(subschema),
    unevaluatedItems: Schema.optionalKey(subschema),
    contains: Schema.optionalKey(subschema),
    minContains: Schema.optionalKey(Schema.Natural),
    maxContains: Schema.optionalKey(Schema.Natural),
    properties: Schema.optionalKey(boundedDefinitions),
    patternProperties: Schema.optionalKey(boundedDefinitions),
    required: Schema.optionalKey(boundedRequired),
    dependentRequired: Schema.optionalKey(
      Schema.Record(BoundedSchemaProperty, boundedRequired).check(
        Schema.isMaxProperties(MAX_RESPONSE_FORMAT_COLLECTION_LENGTH),
      ),
    ),
    dependentSchemas: Schema.optionalKey(boundedDefinitions),
    additionalProperties: Schema.optionalKey(subschema),
    unevaluatedProperties: Schema.optionalKey(subschema),
    propertyNames: Schema.optionalKey(subschema),
    minProperties: Schema.optionalKey(Schema.Natural),
    maxProperties: Schema.optionalKey(Schema.Natural),
    anyOf: Schema.optionalKey(boundedNodes),
    oneOf: Schema.optionalKey(boundedNodes),
    allOf: Schema.optionalKey(boundedNodes),
    not: Schema.optionalKey(subschema),
    contentEncoding: Schema.optionalKey(BoundedSchemaText),
    contentMediaType: Schema.optionalKey(BoundedSchemaText),
    contentSchema: Schema.optionalKey(subschema),
    $defs: Schema.optionalKey(boundedDefinitions),
    definitions: Schema.optionalKey(boundedDefinitions),
  };
};

const ResponseFormatDocument = Schema.Struct({
  ...responseFormatFields(),
  type: Schema.Literal("object"),
}).pipe(Schema.annotate({ parseOptions: { onExcessProperty: "error" } }));

const isResponseFormatDocument = Schema.is(ResponseFormatDocument);

/** Preflight graph bounds before recursively decoding an untrusted JSON Schema. */
const isBoundedResponseFormat = (input: unknown): input is typeof ResponseFormatDocument.Type => {
  if (!Predicate.isObject(input)) return false;

  const pending: Array<{ readonly value: unknown; readonly depth: number }> = [
    { value: input, depth: 0 },
  ];
  const visited = new WeakSet<object>();
  let nodes = 0;
  let textUnits = 0;

  try {
    while (pending.length > 0) {
      const current = pending.pop();
      if (
        current === undefined ||
        current.depth > MAX_RESPONSE_FORMAT_DEPTH ||
        ++nodes > MAX_RESPONSE_FORMAT_NODES
      ) {
        return false;
      }

      const value = current.value;
      if (value === null || typeof value === "boolean") continue;
      if (typeof value === "number") {
        if (!Number.isFinite(value)) return false;
        continue;
      }
      if (typeof value === "string") {
        textUnits += value.length;
        if (textUnits > MAX_RESPONSE_FORMAT_BYTES) return false;
        continue;
      }
      if (!Predicate.isObjectOrArray(value) || visited.has(value)) return false;
      visited.add(value);

      const entries = Array.isArray(value)
        ? value.map((entry, index) => [index, entry] as const)
        : Object.entries(value);
      if (entries.length > MAX_RESPONSE_FORMAT_COLLECTION_LENGTH) return false;

      for (const [key, entry] of entries) {
        textUnits += typeof key === "string" ? key.length : 0;
        if (textUnits > MAX_RESPONSE_FORMAT_BYTES) return false;
        pending.push({ value: entry, depth: current.depth + 1 });
      }
    }

    if (!isResponseFormatDocument(input)) return false;
    const encoded = JSON.stringify(input);
    return Encoding.encodeHex(encoded).length / 2 <= MAX_RESPONSE_FORMAT_BYTES;
  } catch {
    return false;
  }
};

/** A platform-supported object JSON Schema with bounded depth, entries, nodes, and UTF-8 bytes. */
export const PageCaptureResponseFormat = Schema.declare(isBoundedResponseFormat, {
  identifier: "@effect-agent/sandbox/PageCaptureResponseFormat",
  description: "An object JSON Schema bounded to 64 KiB, depth 32, and 4096 nodes",
});
export type PageCaptureResponseFormat = typeof PageCaptureResponseFormat.Type;

/** Capture a URL after a full render, the common case. */
export class PageUrlTarget extends Schema.TaggedClass<PageUrlTarget>()("PageUrlTarget", {
  url: PageCaptureTargetUrl,
}) {}

/** Render caller-supplied HTML instead of navigating. */
export class PageHtmlTarget extends Schema.TaggedClass<PageHtmlTarget>()("PageHtmlTarget", {
  html: BoundedHtml,
}) {}

/** What the pass renders: a navigated URL or raw HTML. */
export const PageCaptureTarget = Schema.Union([PageUrlTarget, PageHtmlTarget]);
export type PageCaptureTarget = typeof PageCaptureTarget.Type;

/**
 * The rendering engine. `chromium` is the full headless browser; `kitesurf`
 * is a lightweight stateless engine an adapter may support for lower-cost
 * captures. An adapter that cannot honor the requested engine
 * rejects it typed rather than silently substituting the other.
 */
export const PageCaptureEngine = Schema.Literals(["chromium", "kitesurf"]);
export type PageCaptureEngine = typeof PageCaptureEngine.Type;

/** Return the fully rendered HTML document. */
export class CapturePageContent extends Schema.TaggedClass<CapturePageContent>()(
  "CapturePageContent",
  {},
) {}

/** Return the rendered page converted to Markdown. */
export class CapturePageMarkdown extends Schema.TaggedClass<CapturePageMarkdown>()(
  "CapturePageMarkdown",
  {},
) {}

/** Return the hyperlinks discovered on the rendered page. */
export class CapturePageLinks extends Schema.TaggedClass<CapturePageLinks>()("CapturePageLinks", {
  visibleLinksOnly: Schema.optionalKey(Schema.Boolean),
}) {}

/** Return rendered element records grouped by their requested CSS selector. */
export class CapturePageScrape extends Schema.TaggedClass<CapturePageScrape>()(
  "CapturePageScrape",
  {
    selectors: Schema.Array(BoundedSelector).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(MAX_SCRAPE_SELECTORS),
    ),
  },
) {}

/**
 * Extract structured data from the rendered page. `responseFormat` is a
 * bounded, object-shaped JSON Schema document, typically derived from an
 * Effect Schema. The untrusted result must still be decoded by the caller
 * through the original Effect Schema.
 */
export class CapturePageStructured extends Schema.TaggedClass<CapturePageStructured>()(
  "CapturePageStructured",
  {
    responseFormat: PageCaptureResponseFormat,
    prompt: Schema.optionalKey(BoundedPrompt),
  },
) {}

/** Exactly one bounded output per pass. */
export const PageCaptureAction = Schema.Union([
  CapturePageContent,
  CapturePageMarkdown,
  CapturePageLinks,
  CapturePageScrape,
  CapturePageStructured,
]);
export type PageCaptureAction = typeof PageCaptureAction.Type;

/** Wait for one CSS selector to appear before capturing. */
export class PageSelectorWait extends Schema.Class<PageSelectorWait>("PageSelectorWait")({
  selector: BoundedSelector,
  timeoutMillis: Schema.optionalKey(BoundedTimeoutMillis),
}) {}

/** Navigation and readiness options applied before the capture. */
export class PageNavigationOptions extends Schema.Class<PageNavigationOptions>(
  "PageNavigationOptions",
)({
  waitUntil: Schema.optionalKey(
    Schema.Literals(["load", "domcontentloaded", "networkidle0", "networkidle2"]),
  ),
  timeoutMillis: Schema.optionalKey(BoundedTimeoutMillis),
  waitForSelector: Schema.optionalKey(PageSelectorWait),
}) {}

/** The rendered viewport. */
export class PageViewport extends Schema.Class<PageViewport>("PageViewport")({
  width: PositiveInt.check(Schema.isLessThanOrEqualTo(7_680)),
  height: PositiveInt.check(Schema.isLessThanOrEqualTo(7_680)),
}) {}

/**
 * Request egress policy for the render. Rejected resource types are never
 * fetched; when request patterns are given, only matching navigation,
 * redirect, and subresource requests are allowed. Calling capabilities own
 * the capture-target allowlist and project it into this policy so rendered
 * JavaScript cannot escape the same construction-time authority.
 */
export class PageResourcePolicy extends Schema.Class<PageResourcePolicy>("PageResourcePolicy")({
  rejectResourceTypes: Schema.optionalKey(
    Schema.Array(
      Schema.Literals([
        "document",
        "stylesheet",
        "image",
        "media",
        "font",
        "script",
        "texttrack",
        "xhr",
        "fetch",
        "eventsource",
        "websocket",
        "manifest",
        "other",
      ]),
    ).check(Schema.isMaxLength(16)),
  ),
  allowRequestPatterns: Schema.optionalKey(
    Schema.Array(BoundedPattern).check(Schema.isMaxLength(64)),
  ),
}) {}

/** Limits an adapter must either enforce or reject. Bytes are UTF-8 encoded. */
export class PageCaptureLimits extends Schema.Class<PageCaptureLimits>("PageCaptureLimits")({
  maxOutputBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_OUTPUT_BYTES)),
}) {}

/** Schema-first page capture request: one target, one action, one output. */
export class PageCaptureRequest extends Schema.Class<PageCaptureRequest>("PageCaptureRequest")({
  target: PageCaptureTarget,
  action: PageCaptureAction,
  engine: PageCaptureEngine,
  limits: PageCaptureLimits,
  navigation: Schema.optionalKey(PageNavigationOptions),
  viewport: Schema.optionalKey(PageViewport),
  resourcePolicy: Schema.optionalKey(PageResourcePolicy),
}) {}

/** The rendered HTML document. */
export class PageContentCaptured extends Schema.TaggedClass<PageContentCaptured>()(
  "PageContentCaptured",
  {
    html: BoundedOutputText,
  },
) {}

/** The rendered page as Markdown. */
export class PageMarkdownCaptured extends Schema.TaggedClass<PageMarkdownCaptured>()(
  "PageMarkdownCaptured",
  {
    markdown: BoundedOutputText,
  },
) {}

/** The hyperlinks discovered on the rendered page. */
export class PageLinksCaptured extends Schema.TaggedClass<PageLinksCaptured>()(
  "PageLinksCaptured",
  {
    links: Schema.Array(PageCaptureLinkUrl).check(Schema.isMaxLength(MAX_LINKS)),
  },
) {}

/** The extracted structured value; untrusted until decoded by the caller. */
export class PageStructuredCaptured extends Schema.TaggedClass<PageStructuredCaptured>()(
  "PageStructuredCaptured",
  {
    value: Schema.Json,
  },
) {}

/** One bounded HTML attribute returned by a selector scrape. */
export class PageScrapeAttribute extends Schema.Class<PageScrapeAttribute>("PageScrapeAttribute")({
  name: BoundedAttributeName,
  value: BoundedAttributeValue,
}) {}

/** One rendered element returned by a selector scrape. */
export class PageScrapeElement extends Schema.Class<PageScrapeElement>("PageScrapeElement")({
  text: BoundedScrapeText,
  html: BoundedScrapeHtml,
  attributes: Schema.Array(PageScrapeAttribute).check(Schema.isMaxLength(MAX_SCRAPE_ATTRIBUTES)),
  left: Schema.Finite,
  top: Schema.Finite,
  width: Schema.Finite,
  height: Schema.Finite,
}) {}

/** All rendered elements matched by one requested selector. */
export class PageScrapeGroup extends Schema.Class<PageScrapeGroup>("PageScrapeGroup")({
  selector: BoundedSelector,
  results: Schema.Array(PageScrapeElement).check(Schema.isMaxLength(MAX_SCRAPE_ELEMENTS)),
}) {}

const PageScrapeGroups = Schema.Array(PageScrapeGroup).check(
  Schema.isMaxLength(MAX_SCRAPE_SELECTORS),
  Schema.makeFilter(
    (groups) =>
      groups.reduce((total, group) => total + group.results.length, 0) <= MAX_SCRAPE_ELEMENTS,
    { title: "at most 4096 aggregate scraped elements" },
  ),
);

/** Bounded rendered element records, preserving selector grouping. */
export class PageScrapeCaptured extends Schema.TaggedClass<PageScrapeCaptured>()(
  "PageScrapeCaptured",
  {
    groups: PageScrapeGroups,
  },
) {}

/** Exactly one output kind per pass, matching the requested action. */
export const PageCaptureOutput = Schema.Union([
  PageContentCaptured,
  PageMarkdownCaptured,
  PageLinksCaptured,
  PageScrapeCaptured,
  PageStructuredCaptured,
]);
export type PageCaptureOutput = typeof PageCaptureOutput.Type;

/** Explicit accounting for model inference performed by a capture adapter. */
export class PageCaptureInferenceUse extends Schema.Class<PageCaptureInferenceUse>(
  "PageCaptureInferenceUse",
)({
  provider: BoundedInferenceProvider,
  modelCalls: PositiveInt,
}) {}

/** Accounting an adapter observed for one capture. */
export class PageCaptureResourceUse extends Schema.Class<PageCaptureResourceUse>(
  "PageCaptureResourceUse",
)({
  browserMillis: Schema.optionalKey(Schema.Natural),
  inference: Schema.optionalKey(PageCaptureInferenceUse),
}) {}

/** The bounded outcome of one capture pass. */
export class PageCaptureResult extends Schema.Class<PageCaptureResult>("PageCaptureResult")({
  implementation: SandboxImplementation,
  output: PageCaptureOutput,
  resourceUse: PageCaptureResourceUse,
}) {}

/**
 * The platform refused the pass for rate or quota reasons. `retryAfterMillis`
 * carries the platform's own backoff hint when one exists; retrying earlier
 * only re-fails.
 */
export class PageCaptureRateLimitedError extends Schema.TaggedError<PageCaptureRateLimitedError>()(
  "PageCaptureRateLimitedError",
  {
    implementation: SandboxImplementation,
    reason: Schema.Literals(["rate", "quota"]),
    retryAfterMillis: Schema.optionalKey(Schema.Natural),
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Navigation, readiness, or capture of the target page failed. */
export class PageCaptureNavigationError extends Schema.TaggedError<PageCaptureNavigationError>()(
  "PageCaptureNavigationError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Host authorization or accounting blocked provider inference before capture. */
export class PageCaptureInferencePolicyError extends Schema.TaggedError<PageCaptureInferencePolicyError>()(
  "PageCaptureInferencePolicyError",
  {
    implementation: SandboxImplementation,
    provider: BoundedInferenceProvider,
    reason: Schema.Literals(["authorization", "accounting"]),
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The adapter cannot honestly honor one requested feature (CAP-010 idiom). */
export class PageCaptureUnsupportedError extends Schema.TaggedError<PageCaptureUnsupportedError>()(
  "PageCaptureUnsupportedError",
  {
    implementation: SandboxImplementation,
    feature: Schema.Literals([
      "engine",
      "action",
      "target",
      "navigation",
      "viewport",
      "resource-policy",
    ]),
    message: BoundedMessage,
  },
) {}

/** The response exceeded the request's output byte budget. */
export class PageCaptureOutputLimitError extends Schema.TaggedError<PageCaptureOutputLimitError>()(
  "PageCaptureOutputLimitError",
  {
    implementation: SandboxImplementation,
    limit: PositiveInt,
    observed: Schema.Natural,
  },
) {}

/** The platform or transport produced a value outside the capture protocol. */
export class PageCaptureProtocolError extends Schema.TaggedError<PageCaptureProtocolError>()(
  "PageCaptureProtocolError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Expected capture failures. Interruption stays an Effect interruption. */
export const PageCaptureError = Schema.Union([
  PageCaptureRateLimitedError,
  PageCaptureNavigationError,
  PageCaptureInferencePolicyError,
  PageCaptureUnsupportedError,
  PageCaptureOutputLimitError,
  PageCaptureProtocolError,
]);
export type PageCaptureError = typeof PageCaptureError.Type;

/**
 * Stateless page-capture port. One `capture` is one pass: the adapter owns no
 * session, replay, approval, or Conversation semantics, identifies its
 * isolation posture honestly (CAP-010), enforces or rejects every requested
 * limit and policy, and returns exactly one bounded output. Capture results
 * are untrusted input to whatever reads them.
 */
export class PageCapture extends Context.Service<
  PageCapture,
  {
    readonly capture: (
      request: PageCaptureRequest,
    ) => Effect.Effect<PageCaptureResult, PageCaptureError>;
  }
>()("@effect-agent/sandbox/PageCapture") {}

/** Type helper for implementations that preserve the public PageCapture contract. */
export type PageCaptureCapture = (
  request: PageCaptureRequest,
) => Effect.Effect<PageCaptureResult, PageCaptureError>;

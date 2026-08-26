import { Context, Schema, type Scope, type Stream } from "effect";

import { PageCaptureTargetUrl } from "./page-capture.ts";
import { SandboxImplementation } from "./sandbox.ts";

const MAX_PAGE_BYTES = 8 * 1024 * 1024;
const MAX_TOTAL_BYTES = 64 * 1024 * 1024;
const MAX_PAGES = 100;
const MAX_DEPTH = 10;
const MAX_DEADLINE_MILLIS = 10 * 60_000;
const MAX_DIAGNOSTIC_LENGTH = 8_000;
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const BoundedMarkdown = Schema.String.check(Schema.isMaxLength(MAX_PAGE_BYTES));
const BoundedMessage = Schema.String.check(Schema.isMaxLength(MAX_DIAGNOSTIC_LENGTH));
const HttpStatus = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(100),
  Schema.isLessThanOrEqualTo(599),
);

/** One credential-free HTTPS URL. The first adapter confines every record to its exact host. */
export const PageCrawlStartUrl = PageCaptureTargetUrl;
export type PageCrawlStartUrl = typeof PageCrawlStartUrl.Type;

/** The declared uses Cloudflare evaluates against a site's Content Signals. */
export const PageCrawlPurpose = Schema.Literals(["search", "ai-input", "ai-train"]);
export type PageCrawlPurpose = typeof PageCrawlPurpose.Type;

/** Immutable caller limits for one scoped crawl. */
export class PageCrawlLimits extends Schema.Class<PageCrawlLimits>("PageCrawlLimits")({
  maxPages: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_PAGES)),
  maxDepth: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_DEPTH)),
  maxPageBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_PAGE_BYTES)),
  maxTotalBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_TOTAL_BYTES)),
  deadlineMillis: PositiveInt.check(Schema.isLessThanOrEqualTo(MAX_DEADLINE_MILLIS)),
}) {}

/** A rendered Markdown crawl rooted at one exact HTTPS host. */
export class PageCrawlRequest extends Schema.Class<PageCrawlRequest>("PageCrawlRequest")({
  startUrl: PageCrawlStartUrl,
  purposes: Schema.Array(PageCrawlPurpose).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(3),
    Schema.isUnique(),
  ),
  limits: PageCrawlLimits,
}) {}

/** Cloudflare's documented status for one discovered URL. */
export const PageCrawlRecordStatus = Schema.Literals([
  "queued",
  "completed",
  "disallowed",
  "skipped",
  "errored",
  "cancelled",
]);
export type PageCrawlRecordStatus = typeof PageCrawlRecordStatus.Type;

/** Bounded origin metadata when Cloudflare reached the URL. */
export class PageCrawlRecordMetadata extends Schema.Class<PageCrawlRecordMetadata>(
  "PageCrawlRecordMetadata",
)({
  status: HttpStatus,
  url: PageCrawlStartUrl,
  title: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(4_096))),
}) {}

/** One untrusted page record. Non-completed statuses may have no Markdown. */
export class PageCrawlRecord extends Schema.Class<PageCrawlRecord>("PageCrawlRecord")({
  url: PageCrawlStartUrl,
  status: PageCrawlRecordStatus,
  markdown: Schema.optionalKey(BoundedMarkdown),
  metadata: Schema.optionalKey(PageCrawlRecordMetadata),
}) {}

/** Cloudflare refused a request because of a request-rate or browser-quota limit. */
export class PageCrawlRateLimitedError extends Schema.TaggedError<PageCrawlRateLimitedError>()(
  "PageCrawlRateLimitedError",
  {
    implementation: SandboxImplementation,
    reason: Schema.Literals(["rate", "quota"]),
    retryAfterMillis: Schema.optionalKey(Schema.Natural),
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The REST transport or provider response violated the crawl protocol. */
export class PageCrawlProtocolError extends Schema.TaggedError<PageCrawlProtocolError>()(
  "PageCrawlProtocolError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The first record or operation that crossed one immutable caller limit. */
export class PageCrawlLimitError extends Schema.TaggedError<PageCrawlLimitError>()(
  "PageCrawlLimitError",
  {
    implementation: SandboxImplementation,
    limit: Schema.Literals(["pages", "page-bytes", "total-bytes", "deadline"]),
    maximum: PositiveInt,
    observed: Schema.Natural,
    message: BoundedMessage,
  },
) {}

/** A documented non-success terminal job status. */
export class PageCrawlTerminalError extends Schema.TaggedError<PageCrawlTerminalError>()(
  "PageCrawlTerminalError",
  {
    implementation: SandboxImplementation,
    status: Schema.Literals([
      "errored",
      "cancelled_by_user",
      "cancelled_due_to_timeout",
      "cancelled_due_to_limits",
    ]),
    message: BoundedMessage,
  },
) {}

export const PageCrawlError = Schema.Union([
  PageCrawlRateLimitedError,
  PageCrawlProtocolError,
  PageCrawlLimitError,
  PageCrawlTerminalError,
]);
export type PageCrawlError = typeof PageCrawlError.Type;

/** Scoped exact-host crawl stream. Provider job identity and polling stay adapter-private. */
export class PageCrawl extends Context.Service<
  PageCrawl,
  {
    readonly crawl: (
      request: PageCrawlRequest,
    ) => Stream.Stream<PageCrawlRecord, PageCrawlError, Scope.Scope>;
  }
>()("@effect-agent/sandbox/PageCrawl") {}

export type PageCrawlCrawl = (
  request: PageCrawlRequest,
) => Stream.Stream<PageCrawlRecord, PageCrawlError, Scope.Scope>;

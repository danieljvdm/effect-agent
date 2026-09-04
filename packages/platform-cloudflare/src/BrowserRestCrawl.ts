import {
  PageCrawl,
  PageCrawlLimitError,
  PageCrawlProtocolError,
  PageCrawlRateLimitedError,
  PageCrawlRecord,
  PageCrawlTerminalError,
  type PageCrawlCrawl,
  type PageCrawlError,
  type PageCrawlRequest,
} from "@effect-agent/sandbox/PageCrawl";
import { SandboxImplementation } from "@effect-agent/sandbox/Sandbox";
import {
  Duration,
  Effect,
  Layer,
  Option,
  Redacted,
  Ref,
  Schedule,
  Schema,
  Scope,
  Stream,
} from "effect";
import {
  HttpClient,
  HttpClientRequest,
  type HttpClientError,
  type HttpClientResponse,
} from "effect/unstable/http";

/** Node-safe REST crawl implementation. It never exposes or persists provider job identity. */
export const browserRestCrawlImplementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "cloudflare-browser-rest-crawl",
});

const API_ORIGIN = "https://api.cloudflare.com";
const POLL_INTERVAL = Duration.seconds(1);
const CANCEL_TIMEOUT = Duration.seconds(30);
const MAX_CONTROL_RESPONSE_BYTES = 64 * 1024;
const MAX_RESULTS_RESPONSE_BYTES = 12 * 1024 * 1024;
const MAX_DIAGNOSTIC_LENGTH = 8_000;
const MAX_CURSOR_LENGTH = 1_024;
const MAX_RECORDS_PER_RESPONSE = 10_000;
const BoundedJobId = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const BoundedCursorString = Schema.NonEmptyString.check(Schema.isMaxLength(MAX_CURSOR_LENGTH));
// The generated API response Schema says string; the current product example returns a number.
const ProviderCursor = Schema.Union([BoundedCursorString, Schema.Natural]);

const ProviderJobStatus = Schema.Literals([
  "running",
  "completed",
  "errored",
  "cancelled_by_user",
  "cancelled_due_to_timeout",
  "cancelled_due_to_limits",
]);

const ProviderResult = Schema.Struct({
  id: BoundedJobId,
  status: ProviderJobStatus,
  browserSecondsUsed: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
  total: Schema.optionalKey(Schema.Natural),
  finished: Schema.optionalKey(Schema.Natural),
  skipped: Schema.optionalKey(Schema.Natural),
  records: Schema.Array(PageCrawlRecord).check(Schema.isMaxLength(MAX_RECORDS_PER_RESPONSE)),
  cursor: Schema.optionalKey(ProviderCursor),
});

const CreateEnvelope = Schema.Struct({
  success: Schema.Literal(true),
  result: BoundedJobId,
});

const ResultEnvelope = Schema.Struct({
  success: Schema.Literal(true),
  result: ProviderResult,
});

const DeleteEnvelope = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Struct({
    job_id: BoundedJobId,
    message: Schema.String.check(Schema.isMaxLength(MAX_DIAGNOSTIC_LENGTH)),
  }),
});

/** Explicit credentials. The token is projected only into fixed-origin Authorization headers. */
export interface BrowserRestCrawlOptions {
  readonly accountId: string;
  readonly apiToken: Redacted.Redacted<string>;
}

const boundedDiagnostic = (message: string): string => message.slice(0, MAX_DIAGNOSTIC_LENGTH);

const privateCause = (value: unknown, apiToken: Redacted.Redacted<string>): Error | undefined => {
  const raw = boundedDiagnostic(String(value));

  if (raw.length === 0) return undefined;
  const token = Redacted.value(apiToken);

  return new Error(token.length === 0 ? raw : raw.replaceAll(token, "[REDACTED]"));
};

const protocolError = (message: string, cause?: unknown): PageCrawlProtocolError =>
  PageCrawlProtocolError.make({
    implementation: browserRestCrawlImplementation,
    message: boundedDiagnostic(message),
    ...(cause === undefined ? {} : { cause }),
  });

const limitError = (
  request: PageCrawlRequest,
  limit: PageCrawlLimitError["limit"],
  observed: number,
): PageCrawlLimitError => {
  const maximum =
    limit === "pages"
      ? request.limits.maxPages
      : limit === "page-bytes"
        ? request.limits.maxPageBytes
        : limit === "total-bytes"
          ? request.limits.maxTotalBytes
          : request.limits.deadlineMillis;

  return PageCrawlLimitError.make({
    implementation: browserRestCrawlImplementation,
    limit,
    maximum,
    observed,
    message: `The crawl exceeded its ${limit} limit`,
  });
};

const retryAfterMillis = (
  headers: Readonly<Record<string, string | undefined>>,
): number | undefined => {
  const seconds = Number(headers["retry-after"]);

  if (!Number.isSafeInteger(seconds) || seconds < 0) return undefined;
  const millis = seconds * 1_000;

  return Number.isSafeInteger(millis) ? millis : undefined;
};

const isJsonResponse = (headers: Readonly<Record<string, string | undefined>>): boolean => {
  const contentType = headers["content-type"];

  if (contentType === undefined) return false;
  const mediaType = contentType.split(";", 1)[0]?.trim().toLowerCase();

  return mediaType === "application/json" || mediaType?.endsWith("+json") === true;
};

const isQuotaMessage = (bodyText: string): boolean => /time limit|daily|quota/i.test(bodyText);

const endpoint = (options: BrowserRestCrawlOptions, jobId?: string): string =>
  `${API_ORIGIN}/client/v4/accounts/${encodeURIComponent(options.accountId)}/browser-rendering/crawl${
    jobId === undefined ? "" : `/${encodeURIComponent(jobId)}`
  }`;

const readBoundedResponse = Effect.fn("BrowserRestCrawl.readBoundedResponse")(function* (
  response: HttpClientResponse.HttpClientResponse,
  maximum: number,
): Effect.fn.Return<string, PageCrawlProtocolError> {
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });

  const state = yield* Stream.runFoldEffect<
    Uint8Array,
    HttpClientError.HttpClientError,
    never,
    { readonly observed: number; readonly text: string },
    PageCrawlProtocolError,
    never
  >(
    response.stream,
    () => ({ observed: 0, text: "" }),
    (current, chunk) => {
      const observed = current.observed + chunk.byteLength;

      if (observed > maximum) {
        return Effect.fail(
          protocolError(
            `The Browser Run crawl response exceeded ${String(maximum)} transport bytes`,
          ),
        );
      }

      return Effect.try({
        try: () => ({ observed, text: current.text + decoder.decode(chunk, { stream: true }) }),
        catch: (cause) => protocolError("Decoding the Browser Run crawl response failed", cause),
      });
    },
  ).pipe(
    Effect.mapError((cause) =>
      Schema.is(PageCrawlProtocolError)(cause)
        ? cause
        : protocolError("Reading the Browser Run crawl response failed", cause),
    ),
  );

  return yield* Effect.try({
    try: () => state.text + decoder.decode(),
    catch: (cause) => protocolError("Decoding the Browser Run crawl response failed", cause),
  });
});

const deadlineFailure = Effect.fn("BrowserRestCrawl.deadlineFailure")(function* (
  request: PageCrawlRequest,
  startedAt: number,
) {
  const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

  return yield* limitError(request, "deadline", Math.max(0, now - startedAt));
});

const withinDeadline = Effect.fn("BrowserRestCrawl.withinDeadline")(function* <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  request: PageCrawlRequest,
  startedAt: number,
): Effect.fn.Return<A, E | PageCrawlLimitError, R> {
  const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  const elapsed = Math.max(0, now - startedAt);
  const remaining = request.limits.deadlineMillis - elapsed;

  if (remaining <= 0) return yield* limitError(request, "deadline", elapsed);

  return yield* effect.pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(remaining),
      orElse: () => deadlineFailure(request, startedAt),
    }),
  );
});

const checkDeadline = Effect.fn("BrowserRestCrawl.checkDeadline")(function* (
  request: PageCrawlRequest,
  startedAt: number,
) {
  const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
  const elapsed = Math.max(0, now - startedAt);

  if (elapsed >= request.limits.deadlineMillis) {
    return yield* limitError(request, "deadline", elapsed);
  }
});

const normalizedCursor = (cursor: typeof ProviderCursor.Type): string => String(cursor);

interface PaginationState {
  readonly cursor: Option.Option<string>;
  readonly seen: ReadonlyArray<string>;
}

interface RecordLimitsState {
  readonly pages: number;
  readonly totalBytes: number;
}

const sameHost = (expectedHost: string, url: string): boolean => new URL(url).host === expectedHost;

const makeCrawl =
  (client: HttpClient.HttpClient, options: BrowserRestCrawlOptions): PageCrawlCrawl =>
  (input) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const startedAt = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
        const startHost = new URL(input.startUrl).host;

        const executeJson = Effect.fn("BrowserRestCrawl.executeJson")(function* <
          S extends Schema.Top,
        >(request: HttpClientRequest.HttpClientRequest, schema: S, maximum: number) {
          const authorized = HttpClientRequest.bearerToken(request, options.apiToken).pipe(
            HttpClientRequest.acceptJson,
          );

          const response = yield* client
            .execute(authorized)
            .pipe(
              Effect.mapError((cause) =>
                protocolError(
                  "Calling the Browser Run crawl REST API failed",
                  privateCause(cause, options.apiToken),
                ),
              ),
            );

          const bodyText = yield* readBoundedResponse(response, maximum);

          if (response.status === 429) {
            const reason = isQuotaMessage(bodyText) ? "quota" : "rate";

            return yield* PageCrawlRateLimitedError.make({
              implementation: browserRestCrawlImplementation,
              reason,
              ...(retryAfterMillis(response.headers) === undefined
                ? {}
                : { retryAfterMillis: retryAfterMillis(response.headers) }),
              message:
                reason === "quota"
                  ? "Browser Run exceeded its browser quota"
                  : "Browser Run crawl was rate limited",
              ...(privateCause(bodyText, options.apiToken) === undefined
                ? {}
                : { cause: privateCause(bodyText, options.apiToken) }),
            });
          }
          if (response.status < 200 || response.status >= 300) {
            return yield* protocolError(
              `Browser Run crawl answered HTTP ${String(response.status)}`,
              privateCause(bodyText, options.apiToken),
            );
          }
          if (!isJsonResponse(response.headers)) {
            return yield* protocolError(
              "The Browser Run crawl success response was not JSON",
              privateCause(bodyText, options.apiToken),
            );
          }

          return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(bodyText).pipe(
            Effect.mapError((cause) =>
              protocolError(
                "Browser Run returned a malformed crawl response",
                privateCause(cause, options.apiToken),
              ),
            ),
          );
        });

        const createJob = Effect.gen(function* () {
          const requestWithBody = yield* HttpClientRequest.post(endpoint(options)).pipe(
            HttpClientRequest.bodyJson({
              url: input.startUrl,
              crawlPurposes: [...input.purposes],
              limit: input.limits.maxPages,
              depth: input.limits.maxDepth,
              formats: ["markdown"],
              render: true,
              options: {
                includeExternalLinks: false,
                includeSubdomains: false,
              },
            }),
            Effect.mapError((cause) =>
              protocolError("Encoding the Browser Run crawl request failed", cause),
            ),
          );

          const created = yield* withinDeadline(
            executeJson(requestWithBody, CreateEnvelope, MAX_CONTROL_RESPONSE_BYTES),
            input,
            startedAt,
          );

          return created.result;
        });

        const job = yield* Effect.uninterruptibleMask((restore) =>
          restore(createJob).pipe(
            Effect.flatMap((id) =>
              Effect.gen(function* () {
                const state = yield* Ref.make<"running" | "terminal">("running");
                const scope = yield* Effect.scope;

                const cancelIfRunning = Ref.modify(state, (current) =>
                  current === "running"
                    ? [true, "terminal" as const]
                    : [false, "terminal" as const],
                ).pipe(
                  Effect.flatMap((shouldCancel) => {
                    if (!shouldCancel) return Effect.void;

                    const cancellation = executeJson(
                      HttpClientRequest.delete(endpoint(options, id)),
                      DeleteEnvelope,
                      MAX_CONTROL_RESPONSE_BYTES,
                    ).pipe(
                      Effect.flatMap((deleted) =>
                        deleted.result.job_id === id
                          ? Effect.void
                          : protocolError("Browser Run cancelled a different crawl job"),
                      ),
                    );

                    return cancellation.pipe(
                      Effect.timeoutOrElse({
                        duration: CANCEL_TIMEOUT,
                        orElse: () =>
                          protocolError("Cancelling the Browser Run crawl exceeded 30 seconds"),
                      }),
                    );
                  }),
                );

                yield* Scope.addFinalizer(
                  scope,
                  cancelIfRunning.pipe(
                    Effect.catchCause(() =>
                      Effect.logWarning("Browser Run crawl cancellation failed"),
                    ),
                  ),
                );

                return { id, state } as const;
              }),
            ),
          ),
        );

        const fetchResult = Effect.fn("BrowserRestCrawl.fetchResult")(function* (
          cursor: Option.Option<string>,
          statusOnly: boolean,
        ) {
          let request = HttpClientRequest.get(endpoint(options, job.id));

          if (statusOnly) request = HttpClientRequest.setUrlParam(request, "limit", "1");
          if (Option.isSome(cursor)) {
            request = HttpClientRequest.setUrlParam(request, "cursor", cursor.value);
          }

          // `limit=1` still permits one complete record, so poll GETs use the bounded result cap.
          const envelope = yield* withinDeadline(
            executeJson(request, ResultEnvelope, MAX_RESULTS_RESPONSE_BYTES),
            input,
            startedAt,
          );

          if (envelope.result.id !== job.id) {
            return yield* protocolError("Browser Run returned a different crawl job identity");
          }

          return envelope.result;
        });

        const terminal = yield* Effect.uninterruptibleMask((restore) =>
          restore(
            withinDeadline(
              Effect.repeat(fetchResult(Option.none(), true), {
                schedule: Schedule.spaced(POLL_INTERVAL),
                until: (result) => result.status !== "running",
              }),
              input,
              startedAt,
            ),
          ).pipe(Effect.tap(() => Ref.set(job.state, "terminal"))),
        );

        if (terminal.status === "running") {
          return yield* protocolError("Browser Run polling stopped before a terminal status");
        }
        if (terminal.status !== "completed") {
          return yield* PageCrawlTerminalError.make({
            implementation: browserRestCrawlImplementation,
            status: terminal.status,
            message: `Browser Run crawl ended with status ${terminal.status}`,
          });
        }

        const pages = Stream.paginate<PaginationState, PageCrawlRecord, PageCrawlError>(
          { cursor: Option.none(), seen: [] },
          (state) =>
            Effect.gen(function* () {
              const result = yield* fetchResult(state.cursor, false);

              if (result.status !== "completed") {
                return yield* protocolError(
                  "Browser Run changed crawl status during result pagination",
                );
              }
              if (result.cursor === undefined) {
                return [result.records, Option.none()] as const;
              }
              const cursor = normalizedCursor(result.cursor);

              if (state.seen.includes(cursor)) {
                return yield* protocolError("Browser Run repeated a crawl result cursor");
              }
              if (state.seen.length >= input.limits.maxPages) {
                return yield* protocolError("Browser Run returned too many crawl result cursors");
              }

              return [
                result.records,
                Option.some({ cursor: Option.some(cursor), seen: [...state.seen, cursor] }),
              ] as const;
            }),
        );

        return pages.pipe(
          Stream.rechunk(1),
          Stream.mapAccumEffect(
            (): RecordLimitsState => ({ pages: 0, totalBytes: 0 }),
            (state, record) =>
              Effect.gen(function* () {
                yield* checkDeadline(input, startedAt);
                const pages = state.pages + 1;

                if (pages > input.limits.maxPages) {
                  return yield* limitError(input, "pages", pages);
                }
                if (!sameHost(startHost, record.url)) {
                  return yield* protocolError("Browser Run returned an off-host crawl record");
                }
                if (record.metadata !== undefined && !sameHost(startHost, record.metadata.url)) {
                  return yield* protocolError("Browser Run returned off-host crawl metadata");
                }

                const pageBytes =
                  record.markdown === undefined
                    ? 0
                    : new TextEncoder().encode(record.markdown).byteLength;

                if (pageBytes > input.limits.maxPageBytes) {
                  return yield* limitError(input, "page-bytes", pageBytes);
                }
                const totalBytes = state.totalBytes + pageBytes;

                if (totalBytes > input.limits.maxTotalBytes) {
                  return yield* limitError(input, "total-bytes", totalBytes);
                }

                return [{ pages, totalBytes }, [record]] as const;
              }),
          ),
        );
      }),
    );

/** Cloudflare REST PageCrawl Layer for Node and other non-Worker composition roots. */
export const browserRestCrawlLayer = (
  options: BrowserRestCrawlOptions,
): Layer.Layer<PageCrawl, never, HttpClient.HttpClient> =>
  Layer.effect(
    PageCrawl,
    Effect.map(HttpClient.HttpClient, (client) =>
      PageCrawl.of({ crawl: makeCrawl(client, options) }),
    ),
  );

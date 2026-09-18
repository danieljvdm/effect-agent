import type { HTTPRequest } from "@cloudflare/puppeteer";
import { Effect, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { BrowserRunFailure, browserFailure } from "./browser-failure.ts";

const TestUrl = Schema.String.check(
  Schema.isMaxLength(8_192),
  Schema.makeFilter(
    (value) => {
      try {
        const url = new URL(value);

        return (
          (url.protocol === "https:" ||
            (url.protocol === "http:" &&
              ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname))) &&
          url.username === "" &&
          url.password === "" &&
          url.search === "" &&
          url.hash === ""
        );
      } catch {
        return false;
      }
    },
    { title: "a credential-free HTTPS URL, or loopback HTTP URL" },
  ),
);

/**
 * Host-owned finite-HTTP test routing for the controlled page. The entry pathname maps to staging `/`;
 * other same-origin paths are preserved. Never supply production storage or
 * credentials: browser-admitted cookies and Authorization reach staging.
 * Response bodies are limited to 8 MiB / 30 seconds. Service workers, sockets,
 * streaming, popups, binary/multipart uploads, detached sessions and human handoff are unsupported.
 * Service-worker registration/background traffic and uncontrolled targets are not intercepted.
 */
export class BrowserRunOriginOverride extends Schema.Class<BrowserRunOriginOverride>(
  "BrowserRunOriginOverride",
)(
  Schema.Struct({ productionUrl: TestUrl, stagingOrigin: TestUrl }).check(
    Schema.makeFilter(
      (value) => {
        const production = new URL(value.productionUrl);
        const staging = new URL(value.stagingOrigin);

        return staging.pathname === "/" && production.origin !== staging.origin;
      },
      { title: "a distinct staging origin without a pathname" },
    ),
  ),
) {}

const MAX_RESPONSE_BYTES = 8 * 1024 * 1024;

const failure = () =>
  new BrowserRunFailure({ operation: "interactive.originOverride", reason: "malformed" });

/** Foreign Puppeteer callback boundary; Effect owns the upstream request and its deadline. */
export const makeOriginOverride = Effect.fnUntraced(function* (override: BrowserRunOriginOverride) {
  const fetch = yield* FetchHttpClient.Fetch;
  const context = yield* Effect.context<never>();
  const production = new URL(override.productionUrl);
  const staging = new URL(override.stagingOrigin);

  const publicUrl = (value: string, base: URL): string => {
    const url = new URL(value, base);

    if (url.origin === staging.origin) {
      if (url.pathname === "/") url.pathname = production.pathname;
      url.protocol = production.protocol;
      url.hostname = production.hostname;
      url.port = production.port;
    }
    if (url.origin !== production.origin || url.username !== "" || url.password !== "")
      throw failure();

    return url.href;
  };

  return (request: HTTPRequest, signal: AbortSignal): Promise<void> => {
    const browserUrl = new URL(request.url());

    if (browserUrl.origin !== production.origin) return request.continue();

    const mapped = Effect.tryPromise({
      try: async (signal) => {
        if (request.resourceType() === "eventsource" || request.redirectChain().length > 10)
          throw failure();
        const url = new URL(browserUrl);

        if (url.pathname === production.pathname || url.pathname === `${production.pathname}/`)
          url.pathname = "/";
        url.protocol = staging.protocol;
        url.hostname = staging.hostname;
        url.port = staging.port;
        const headers = new Headers(request.headers());

        // Preserve Chromium's decision for this request, including credentials:'omit'.
        // This fetch has no browser cookie jar and must never reconstruct one.
        headers.delete("host");
        headers.delete("content-length");
        headers.delete("proxy-authorization");
        headers.delete("connection");
        headers.delete("accept-encoding");
        const contentType = headers.get("content-type") ?? "";

        const mediaType = contentType.split(";", 1)[0]?.trim().toLowerCase() ?? "";

        if (
          request.hasPostData() &&
          !(
            mediaType.startsWith("text/") ||
            ["application/json", "application/xml", "application/x-www-form-urlencoded"].includes(
              mediaType,
            ) ||
            /^application\/[a-z0-9!#$&^_.+-]+\+(?:json|xml)$/.test(mediaType)
          )
        )
          throw failure();
        const body = request.hasPostData() ? await request.fetchPostData() : undefined;

        if (request.hasPostData() && body === undefined) throw failure();

        const response = await fetch(url, {
          method: request.method(),
          headers,
          body,
          redirect: "manual",
          signal,
        });

        const responseHeaders: Record<string, string | string[]> = {};
        const reader = response.body?.getReader();

        try {
          if (
            response.status === 101 ||
            response.headers.get("content-type")?.startsWith("text/event-stream")
          )
            throw failure();
          for (const [name, value] of response.headers) {
            if (
              [
                "content-length",
                "content-encoding",
                "transfer-encoding",
                "connection",
                "alt-svc",
                "set-cookie",
              ].includes(name)
            )
              continue;
            responseHeaders[name] =
              name === "location" || name === "content-location"
                ? publicUrl(value, url)
                : name === "access-control-allow-origin" && value === staging.origin
                  ? production.origin
                  : value;
          }

          const cookies = response.headers.getSetCookie().map((value) => {
            const defaultPath = url.pathname.slice(0, url.pathname.lastIndexOf("/")) || "/";
            const cookie = /;\s*path=/i.test(value) ? value : `${value}; Path=${defaultPath}`;

            return cookie.replace(/;\s*domain=([^;]+)/gi, (attribute, domain: string) =>
              domain.trim().replace(/^\./, "").toLowerCase() === staging.hostname
                ? `; Domain=${production.hostname}`
                : attribute,
            );
          });

          if (cookies.length > 0) responseHeaders["set-cookie"] = cookies;
          responseHeaders["cache-control"] = "no-store";
          const chunks: Uint8Array[] = [];
          let length = 0;

          while (reader !== undefined) {
            const chunk = await reader.read();

            if (chunk.done) break;
            length += chunk.value.byteLength;
            if (length > MAX_RESPONSE_BYTES) throw failure();
            chunks.push(chunk.value);
          }
          const bytes = new Uint8Array(length);
          let offset = 0;

          for (const chunk of chunks) {
            bytes.set(chunk, offset);
            offset += chunk.byteLength;
          }
          // Return mapped redirects to Chromium. Each hop is intercepted again,
          // so redirect cookies and credentials modes remain browser decisions.
          await request.respond({ status: response.status, headers: responseHeaders, body: bytes });
        } finally {
          await reader?.cancel();
        }
      },
      catch: (cause) => browserFailure("interactive.originOverride", cause),
    }).pipe(
      Effect.timeoutOrElse({
        duration: "30 seconds",
        orElse: () =>
          Effect.fail(
            new BrowserRunFailure({ operation: "interactive.originOverride", reason: "timeout" }),
          ),
      }),
      Effect.withTracerEnabled(false),
    );

    return Effect.runPromiseWith(context)(mapped, { signal }).catch(async (cause) => {
      // The host records the failure. Never continue a failed mapped request.
      if (!request.isInterceptResolutionHandled()) await request.abort("failed");
      throw cause;
    });
  };
});

import * as WebCapture from "@effect-agent/capabilities/WebCapture";
import {
  browserRestCaptureImplementation,
  browserRestCaptureLayer,
  browserRestWorkersAiCaptureLayer,
  CloudflareBrowserRest,
} from "@effect-agent/platform-cloudflare/BrowserRestCapture";
import {
  BrowserQuickActionWorkersAi,
  BrowserQuickActionWorkersAiPolicyError,
} from "@effect-agent/platform-cloudflare/CloudflareBrowser";
import {
  CapturePageMarkdown,
  CapturePageScrape,
  CapturePageStructured,
  PageCapture,
  PageCaptureLimits,
  PageCaptureRequest,
  PageUrlTarget,
} from "@effect-agent/sandbox/PageCapture";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Logger, Redacted, Ref, Schema, Stream, type Layer } from "effect";
import { Toolkit } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { expectTypeOf } from "vite-plus/test";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type LayerRequirements<Value> =
  Value extends Layer.Layer<infer _Output, infer _Error, infer Requirements> ? Requirements : never;
type RestRequiresHttpClient = Equal<
  LayerRequirements<ReturnType<typeof browserRestCaptureLayer>>,
  HttpClient.HttpClient
>;
type RestWorkersAiRequiresVisibleAuthorities = Equal<
  LayerRequirements<ReturnType<typeof browserRestWorkersAiCaptureLayer>>,
  HttpClient.HttpClient | BrowserQuickActionWorkersAi
>;

const request = (engine: "chromium" | "kitesurf" = "chromium", maxOutputBytes = 1_024) =>
  PageCaptureRequest.make({
    target: PageUrlTarget.make({ url: "https://docs.example.com/page" }),
    action: CapturePageMarkdown.make({}),
    engine,
    limits: PageCaptureLimits.make({ maxOutputBytes }),
  });

const response = (body: string, status = 200, headers: HeadersInit = {}) =>
  new Response(body, { status, headers: { "content-type": "application/json", ...headers } });

const scrapeRequest = PageCaptureRequest.make({
  target: PageUrlTarget.make({ url: "https://docs.example.com/page" }),
  action: CapturePageScrape.make({ selectors: [".plan", "#faq"] }),
  engine: "chromium",
  limits: PageCaptureLimits.make({ maxOutputBytes: 16 * 1024 }),
});

const captureWith = (client: HttpClient.HttpClient, input = request()) =>
  Effect.gen(function* () {
    const capture = yield* PageCapture;

    return yield* capture.capture(input);
  }).pipe(
    Effect.provide(
      browserRestCaptureLayer({
        accountId: "account-id",
        apiToken: Redacted.make("secret-rest-token"),
      }),
    ),
    Effect.provideService(HttpClient.HttpClient, client),
  );

describe("Browser Run REST PageCapture adapter", () => {
  it.effect(
    "assembles extraction handlers while retaining HTTP injection and explicit AI authority",
    () =>
      Effect.gen(function* () {
        const definition = WebCapture.makeExtract("extract_plan", {
          description: "Read plan",
          urls: ["docs.example.com"],
          schema: Schema.Struct({ name: Schema.String }),
        });

        const calls: Array<string> = [];

        const client = HttpClient.make((request) =>
          Effect.sync(() => {
            calls.push("request");

            return HttpClientResponse.fromWeb(
              request,
              response('{"success":true,"result":{"name":"Pro"}}'),
            );
          }),
        );

        const options = { accountId: "account", apiToken: Redacted.make("token") };
        const denied = CloudflareBrowserRest.layer(definition, options);

        const allowed = CloudflareBrowserRest.layer(definition, {
          ...options,
          workersAi: {
            authorizeAndAccount: () =>
              Effect.sync(() => {
                calls.push("authorize");
              }),
          },
        });

        expectTypeOf<Layer.Services<typeof allowed>>().toEqualTypeOf<HttpClient.HttpClient>();

        const invoke = Effect.gen(function* () {
          const toolkit = yield* Toolkit.make(definition.tool);

          return yield* Stream.runCollect(
            yield* toolkit.handle("extract_plan", { url: "https://docs.example.com/" }),
          );
        });

        const refused = yield* invoke.pipe(
          Effect.provide(denied),
          Effect.provideService(HttpClient.HttpClient, client),
        );

        expect(refused).toMatchObject([{ result: { errorTag: "PageCaptureUnsupportedError" } }]);
        expect(calls).toEqual([]);

        const result = yield* invoke.pipe(
          Effect.provide(allowed),
          Effect.provideService(HttpClient.HttpClient, client),
        );

        expect(result).toMatchObject([{ result: { name: "Pro" } }]);
        expect(calls).toEqual(["authorize", "request"]);
      }),
  );
  it("keeps its Node-safe Layer requirements visible", () => {
    const httpClient: RestRequiresHttpClient = true;
    const workersAi: RestWorkersAiRequiresVisibleAuthorities = true;

    expect(httpClient && workersAi).toBe(true);
  });
  it.effect(
    "uses the stable browser-rendering endpoint and adds only Kitesurf's browser query",
    () =>
      Effect.gen(function* () {
        const urls = yield* Ref.make<ReadonlyArray<string>>([]);

        const client = HttpClient.make((request, url) =>
          Ref.update(urls, (current) => [...current, url.href]).pipe(
            Effect.as(
              HttpClientResponse.fromWeb(request, response('{"success":true,"result":"# Page"}')),
            ),
          ),
        );

        yield* captureWith(client, request("chromium"));
        yield* captureWith(client, request("kitesurf"));
        expect(yield* Ref.get(urls)).toEqual([
          "https://api.cloudflare.com/client/v4/accounts/account-id/browser-rendering/markdown",
          "https://api.cloudflare.com/client/v4/accounts/account-id/browser-rendering/markdown?browser=kitesurf",
        ]);
      }),
  );

  it.effect("posts selector elements to the stable scrape endpoint", () =>
    Effect.gen(function* () {
      const seen = yield* Ref.make<{ readonly url: string; readonly body: unknown } | undefined>(
        undefined,
      );

      const client = HttpClient.make((request, url) =>
        Effect.gen(function* () {
          const body =
            request.body._tag === "Uint8Array"
              ? JSON.parse(new TextDecoder().decode(request.body.body))
              : undefined;

          yield* Ref.set(seen, { url: url.href, body });

          return HttpClientResponse.fromWeb(request, response('{"success":true,"result":[]}'));
        }),
      );

      yield* captureWith(client, scrapeRequest);
      expect(yield* Ref.get(seen)).toEqual({
        url: "https://api.cloudflare.com/client/v4/accounts/account-id/browser-rendering/scrape",
        body: {
          url: "https://docs.example.com/page",
          elements: [{ selector: ".plan" }, { selector: "#faq" }],
        },
      });
    }),
  );

  it.effect("keeps the token at the fixed origin Authorization boundary", () =>
    Effect.gen(function* () {
      const logs: Array<string> = [];

      const logger = Logger.make<unknown, void>(({ cause, message }) => {
        logs.push(`${String(message)} ${String(cause)}`);
      });

      const calls = yield* Ref.make(0);

      const seen = yield* Ref.make<
        | {
            readonly url: string;
            readonly authorization: string | undefined;
            readonly body: string;
          }
        | undefined
      >(undefined);

      const client = HttpClient.make((request, url) =>
        Effect.gen(function* () {
          const call = yield* Ref.getAndUpdate(calls, (count) => count + 1);

          yield* Ref.set(seen, {
            url: url.href,
            authorization: request.headers.authorization,
            body: JSON.stringify(request.body),
          });

          return HttpClientResponse.fromWeb(
            request,
            call === 0
              ? response('{"success":true,"result":"# Page"}')
              : response('{"success":false,"errors":[{"message":"secret-rest-token"}]}', 400),
          );
        }),
      );

      const result = yield* captureWith(client).pipe(Effect.provide(Logger.layer([logger])));

      const error = yield* captureWith(client).pipe(
        Effect.flip,
        Effect.provide(Logger.layer([logger])),
      );

      expect(yield* Ref.get(seen)).toEqual({
        url: "https://api.cloudflare.com/client/v4/accounts/account-id/browser-rendering/markdown",
        authorization: "Bearer secret-rest-token",
        body: expect.any(String),
      });
      expect((yield* Ref.get(seen))?.url).not.toContain("secret-rest-token");
      expect((yield* Ref.get(seen))?.body).not.toContain("secret-rest-token");
      expect(JSON.stringify(result)).not.toContain("secret-rest-token");
      expect(error.message).not.toContain("secret-rest-token");
      expect(JSON.stringify(error)).not.toContain("secret-rest-token");
      expect(logs.join("\n")).not.toContain("secret-rest-token");
    }),
  );

  it.effect("keeps malformed, non-success, quota, and oversized responses typed", () =>
    Effect.gen(function* () {
      const cases = [
        { response: response("not-json"), tag: "PageCaptureProtocolError" },
        {
          response: response('{"success":false,"errors":[{"message":"bad"}]}', 400),
          tag: "PageCaptureNavigationError",
        },
        {
          response: response("quota secret-rest-token", 429, { "retry-after": "7" }),
          tag: "PageCaptureRateLimitedError",
        },
        {
          response: response('{"success":true,"result":"too long"}'),
          tag: "PageCaptureOutputLimitError",
          limit: 1,
        },
      ] as const;

      for (const scenario of cases) {
        const client = HttpClient.make((request) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, scenario.response)),
        );

        const exit = yield* captureWith(
          client,
          request("chromium", "limit" in scenario ? scenario.limit : 1_024),
        ).pipe(Effect.exit);

        expect(exit._tag).toBe("Failure");
        if (exit._tag === "Failure") expect(String(exit.cause)).toContain(scenario.tag);
      }
    }),
  );

  it.effect("fails closed on a non-JSON success response", () =>
    Effect.gen(function* () {
      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response("<html>provider diagnostic secret-rest-token</html>", {
              status: 200,
              headers: { "content-type": "text/html" },
            }),
          ),
        ),
      );

      const error = yield* captureWith(client).pipe(Effect.flip);

      expect(error.message).toContain("JSON response envelope");
      expect(error.message).not.toContain("secret-rest-token");
    }),
  );

  it.effect("requires Workers AI authorization before structured REST capture", () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);

      const client = HttpClient.make((request) =>
        Ref.update(calls, (count) => count + 1).pipe(
          Effect.as(HttpClientResponse.fromWeb(request, response('{"success":true,"result":{}}'))),
        ),
      );

      const input = PageCaptureRequest.make({
        target: PageUrlTarget.make({ url: "https://docs.example.com/page" }),
        action: CapturePageStructured.make({ responseFormat: { type: "object" } }),
        engine: "kitesurf",
        limits: PageCaptureLimits.make({ maxOutputBytes: 1_024 }),
      });

      const denied = BrowserQuickActionWorkersAiPolicyError.make({
        reason: "authorization",
        message: "tenant denied",
      });

      const exit = yield* Effect.gen(function* () {
        const capture = yield* PageCapture;

        return yield* capture.capture(input);
      }).pipe(
        Effect.provide(
          browserRestWorkersAiCaptureLayer({
            accountId: "account-id",
            apiToken: Redacted.make("secret-rest-token"),
          }),
        ),
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provideService(BrowserQuickActionWorkersAi, {
          authorizeAndAccount: () => Effect.fail(denied),
        }),
        Effect.exit,
      );

      expect(exit._tag).toBe("Failure");
      expect(yield* Ref.get(calls)).toBe(0);
      if (exit._tag === "Failure")
        expect(String(exit.cause)).toContain("PageCaptureInferencePolicyError");
    }),
  );

  it("loads as a Node-safe subpath", async () => {
    const module = await import("@effect-agent/platform-cloudflare/BrowserRestCapture");

    expect(module.browserRestCaptureImplementation).toBe(browserRestCaptureImplementation);
  });
});

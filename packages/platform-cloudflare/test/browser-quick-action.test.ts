import {
  CapturePageContent,
  CapturePageLinks,
  CapturePageMarkdown,
  CapturePageStructured,
  PageCapture,
  PageCaptureInferencePolicyError,
  PageCaptureLimits,
  PageCaptureRequest,
  PageUrlTarget,
  type PageCaptureError,
  type PageCaptureResult,
} from "@effect-agent/sandbox";
import { Deferred, Effect, Fiber, Layer } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  BrowserQuickActionBrowserBinding,
  BrowserQuickActionRpcError,
  BrowserQuickActionWorkersAi,
  BrowserQuickActionWorkersAiPolicyError,
  browserQuickActionCaptureLayer,
  browserQuickActionWorkersAiCaptureLayer,
  type BrowserQuickActionCaptureOptions,
  type BrowserQuickActionClient,
  type BrowserQuickActionWorkersAiPolicy,
} from "../src/browser-quick-action.ts";

interface RecordedCall {
  readonly action: "screenshot" | "content" | "markdown" | "links" | "json";
  readonly options: unknown;
}

/** A scripted binding: hands back the queued responses in order. */
const makeBinding = (responses: ReadonlyArray<Response | Error>) => {
  const calls: Array<RecordedCall> = [];
  let index = 0;
  const respond = (
    action: RecordedCall["action"],
    options: unknown,
  ): Effect.Effect<Response, BrowserQuickActionRpcError> => {
    calls.push({ action, options });
    const next = responses[index];
    index += 1;
    if (next === undefined) {
      return Effect.fail(
        BrowserQuickActionRpcError.make({
          action,
          cause: new Error("no scripted response left"),
        }),
      );
    }
    return next instanceof Error
      ? Effect.fail(BrowserQuickActionRpcError.make({ action, cause: next }))
      : Effect.succeed(next);
  };
  const binding: BrowserQuickActionClient = {
    screenshot: (options) => respond("screenshot", options),
    content: (options) => respond("content", options),
    markdown: (options) => respond("markdown", options),
    links: (options) => respond("links", options),
    json: (options) => respond("json", options),
  };
  return { binding, calls };
};

const request = (
  action: PageCaptureRequest["action"],
  overrides?: Partial<{
    readonly engine: PageCaptureRequest["engine"];
    readonly maxOutputBytes: number;
    readonly navigation: PageCaptureRequest["navigation"];
    readonly viewport: PageCaptureRequest["viewport"];
    readonly resourcePolicy: PageCaptureRequest["resourcePolicy"];
  }>,
): PageCaptureRequest =>
  PageCaptureRequest.make({
    target: PageUrlTarget.make({ url: "https://docs.example.com/pricing" }),
    action,
    engine: overrides?.engine ?? "chromium",
    limits: PageCaptureLimits.make({ maxOutputBytes: overrides?.maxOutputBytes ?? 128 * 1024 }),
    ...(overrides?.navigation === undefined ? {} : { navigation: overrides.navigation }),
    ...(overrides?.viewport === undefined ? {} : { viewport: overrides.viewport }),
    ...(overrides?.resourcePolicy === undefined
      ? {}
      : { resourcePolicy: overrides.resourcePolicy }),
  });

const capture = (
  binding: BrowserQuickActionClient,
  input: PageCaptureRequest,
  workersAi?: BrowserQuickActionWorkersAiPolicy,
): Promise<PageCaptureResult> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const port = yield* PageCapture;
      return yield* port.capture(input);
    }).pipe(Effect.provide(captureLayer(binding, workersAi))),
  );

const captureError = (
  binding: BrowserQuickActionClient,
  input: PageCaptureRequest,
  workersAi?: BrowserQuickActionWorkersAiPolicy,
): Promise<PageCaptureError> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const port = yield* PageCapture;
      return yield* port.capture(input).pipe(Effect.flip);
    }).pipe(Effect.provide(captureLayer(binding, workersAi))),
  );

const captureLayer = (
  binding: BrowserQuickActionClient,
  workersAi?: BrowserQuickActionWorkersAiPolicy,
): Layer.Layer<PageCapture> =>
  workersAi === undefined
    ? browserQuickActionCaptureLayer().pipe(
        Layer.provide(Layer.succeed(BrowserQuickActionBrowserBinding)(binding)),
      )
    : browserQuickActionWorkersAiCaptureLayer().pipe(
        Layer.provide(
          Layer.merge(
            Layer.succeed(BrowserQuickActionBrowserBinding)(binding),
            BrowserQuickActionWorkersAi.layer(workersAi),
          ),
        ),
      );

const jsonResponse = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: { "Content-Type": "application/json" },
    ...init,
  });

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type LayerRequirements<Value> =
  Value extends Layer.Layer<infer _Output, infer _Error, infer Requirements> ? Requirements : never;
type BrowserBindingAuthorityIsVisible = Equal<
  LayerRequirements<ReturnType<typeof browserQuickActionCaptureLayer>>,
  BrowserQuickActionBrowserBinding
>;
type WorkersAiAuthorityIsVisible = Equal<
  LayerRequirements<ReturnType<typeof browserQuickActionWorkersAiCaptureLayer>>,
  BrowserQuickActionBrowserBinding | BrowserQuickActionWorkersAi
>;
type NativeBrowserRunIsLayerInput = Equal<BrowserQuickActionCaptureOptions["browser"], BrowserRun>;

describe("Browser Run Quick Action PageCapture adapter", () => {
  it("keeps browser RPC and separately billed Workers AI authority visible in adapter Layers", () => {
    const browserProof: BrowserBindingAuthorityIsVisible = true;
    const workersAiProof: WorkersAiAuthorityIsVisible = true;
    const nativeBindingProof: NativeBrowserRunIsLayerInput = true;
    expect(browserProof).toBe(true);
    expect(workersAiProof).toBe(true);
    expect(nativeBindingProof).toBe(true);
  });

  it("projects the request onto the wire options and unwraps the envelope", async () => {
    const { binding, calls } = makeBinding([
      jsonResponse(
        { success: true, result: "# Pricing" },
        { headers: { "Content-Type": "application/json", "X-Browser-Ms-Used": "1234" } },
      ),
    ]);
    const result = await capture(
      binding,
      request(CapturePageMarkdown.make({}), {
        navigation: {
          waitUntil: "networkidle0",
          timeoutMillis: 15_000,
          waitForSelector: { selector: "#plans", timeoutMillis: 5_000 },
        },
        viewport: { width: 1_280, height: 800 },
        resourcePolicy: {
          rejectResourceTypes: ["image"],
          allowRequestPatterns: ["https://docs.example.com/*"],
        },
      }),
    );
    expect(calls).toHaveLength(1);
    expect(calls[0].action).toBe("markdown");
    expect(calls[0].options).toMatchObject({
      url: "https://docs.example.com/pricing",
      gotoOptions: { waitUntil: "networkidle0", timeout: 15_000 },
      waitForSelector: { selector: "#plans", timeout: 5_000 },
      viewport: { width: 1_280, height: 800 },
      rejectResourceTypes: ["image"],
      allowRequestPattern: ["https://docs.example.com/*"],
    });
    expect(result.output).toMatchObject({ _tag: "PageMarkdownCaptured", markdown: "# Pricing" });
    expect(result.resourceUse.browserMillis).toBe(1_234);
    expect(result.implementation.identity).toBe("cloudflare-browser-quick-action");
  });

  it("decodes native content, links, and explicitly authorized structured envelopes", async () => {
    const rawHtml = "<!doctype html><h1>Pricing</h1>";
    const { binding, calls } = makeBinding([
      jsonResponse({ success: true, result: rawHtml }),
      jsonResponse({
        success: true,
        result: ["https://docs.example.com/a", "https://docs.example.com/b"],
      }),
      jsonResponse({ success: true, result: { plans: [{ name: "Pro" }] } }),
    ]);
    const content = await capture(binding, request(CapturePageContent.make({})));
    expect(content.output).toMatchObject({ _tag: "PageContentCaptured", html: rawHtml });

    const links = await capture(binding, request(CapturePageLinks.make({})));
    expect(links.output).toMatchObject({
      _tag: "PageLinksCaptured",
      links: ["https://docs.example.com/a", "https://docs.example.com/b"],
    });

    const authorized: Array<PageCaptureRequest["action"]["_tag"]> = [];
    const structured = await capture(
      binding,
      request(CapturePageStructured.make({ responseFormat: { type: "object" } })),
      {
        authorizeAndAccount: (input) =>
          Effect.sync(() => {
            expect(calls).toHaveLength(2);
            authorized.push(input.action._tag);
          }),
      },
    );
    expect(structured.output).toMatchObject({
      _tag: "PageStructuredCaptured",
      value: { plans: [{ name: "Pro" }] },
    });
    expect(authorized).toEqual(["CapturePageStructured"]);
    expect(structured.resourceUse.inference).toEqual({
      provider: "cloudflare-workers-ai",
      modelCalls: 1,
    });
  });

  it("rejects malformed and oversized link arrays instead of silently filtering their values", async () => {
    const hostilePayloads: ReadonlyArray<ReadonlyArray<unknown>> = [
      ["https://docs.example.com/a", 7],
      ["https://docs.example.com/a", ""],
      ["https://docs.example.com/a", "not a URL"],
      ["https://docs.example.com/a", "javascript:alert(1)"],
      ["https://docs.example.com/a", "https://user:secret@docs.example.com/private"],
      ["https://docs.example.com/a", "x".repeat(8 * 1024 + 1)],
      Array.from({ length: 4_097 }, () => "https://a"),
    ];
    const { binding } = makeBinding(
      hostilePayloads.map((result) => jsonResponse({ success: true, result })),
    );

    for (const _payload of hostilePayloads) {
      expect(await captureError(binding, request(CapturePageLinks.make({})))).toMatchObject({
        _tag: "PageCaptureProtocolError",
        message: expect.stringContaining("bounded array of valid URLs"),
      });
    }
  });

  it("rejects success bodies outside the native BrowserRun response envelope", async () => {
    const rawFailure = JSON.stringify({ success: false, errors: "page content" });
    const rawSuccess = JSON.stringify({ success: true, result: "page content" });
    const { binding } = makeBinding([
      new Response(rawFailure, { headers: { "Content-Type": "text/markdown; charset=utf-8" } }),
      new Response(rawSuccess, { headers: { "Content-Type": "text/plain" } }),
    ]);

    expect(await captureError(binding, request(CapturePageMarkdown.make({})))).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: expect.stringContaining("JSON response envelope"),
    });
    expect(await captureError(binding, request(CapturePageContent.make({})))).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: expect.stringContaining("JSON response envelope"),
    });
  });

  it("denies hidden or host-refused Workers AI extraction before invoking the browser", async () => {
    const { binding, calls } = makeBinding([]);
    const structured = request(CapturePageStructured.make({ responseFormat: { type: "object" } }));

    expect(await captureError(binding, structured)).toMatchObject({
      _tag: "PageCaptureUnsupportedError",
      feature: "action",
      message: expect.stringContaining("authorization and accounting"),
    });
    expect(calls).toHaveLength(0);

    const denial = BrowserQuickActionWorkersAiPolicyError.make({
      reason: "authorization",
      message: "Workers AI is outside this tenant's provider budget",
    });
    const refused = await captureError(binding, structured, {
      authorizeAndAccount: () => Effect.fail(denial),
    });
    expect(refused).toMatchObject({
      _tag: "PageCaptureInferencePolicyError",
      provider: "cloudflare-workers-ai",
      reason: "authorization",
      message: "Workers AI extraction was not authorized",
    });
    expect(refused).toBeInstanceOf(PageCaptureInferencePolicyError);
    expect(refused._tag === "PageCaptureInferencePolicyError" && refused.cause).toBe(denial);
    expect(calls).toHaveLength(0);
  });

  it("maps 429 responses to typed rate/quota failures with the backoff hint", async () => {
    const privateDiagnostic = "token=host-only-secret";
    const { binding } = makeBinding([
      new Response(`Too many requests; ${privateDiagnostic}`, {
        status: 429,
        headers: { "Retry-After": "12" },
      }),
      new Response(`Browser time limit exceeded for today; ${privateDiagnostic}`, { status: 429 }),
    ]);
    const rate = await captureError(binding, request(CapturePageMarkdown.make({})));
    expect(rate).toMatchObject({
      _tag: "PageCaptureRateLimitedError",
      reason: "rate",
      retryAfterMillis: 12_000,
      message: "The Quick Action was rate limited",
    });
    expect(rate.message).not.toContain(privateDiagnostic);
    expect(rate._tag === "PageCaptureRateLimitedError" && rate.cause).toMatchObject({
      message: expect.stringContaining(privateDiagnostic),
    });
    const quota = await captureError(binding, request(CapturePageMarkdown.make({})));
    expect(quota).toMatchObject({
      _tag: "PageCaptureRateLimitedError",
      reason: "quota",
      message: "The Quick Action exceeded its browser quota",
    });
    expect(quota.message).not.toContain(privateDiagnostic);
    expect(quota._tag === "PageCaptureRateLimitedError" && quota.cause).toMatchObject({
      message: expect.stringContaining(privateDiagnostic),
    });
  });

  it("drops overflowing Retry-After values without defecting the typed rate-limit failure", async () => {
    const { binding } = makeBinding([
      new Response("Too many requests", {
        status: 429,
        headers: { "Retry-After": String(Number.MAX_SAFE_INTEGER) },
      }),
    ]);

    const error = await captureError(binding, request(CapturePageMarkdown.make({})));
    expect(error).toMatchObject({ _tag: "PageCaptureRateLimitedError", reason: "rate" });
    expect(error).not.toHaveProperty("retryAfterMillis");
  });

  it("keeps remote failures typed: 4xx, 5xx, and reported envelope errors", async () => {
    const privateDiagnostic = "token=host-only-secret";
    const { binding } = makeBinding([
      new Response(`bad request; ${privateDiagnostic}`, { status: 400 }),
      new Response(`internal; ${privateDiagnostic}`, { status: 500 }),
      jsonResponse({
        success: false,
        errors: [{ code: 1, message: `navigation failed; ${privateDiagnostic}` }],
      }),
      jsonResponse({ result: "missing success discriminator" }),
    ]);
    const navigation = await captureError(binding, request(CapturePageMarkdown.make({})));
    expect(navigation).toMatchObject({
      _tag: "PageCaptureNavigationError",
      message: "The Quick Action answered HTTP 400",
    });
    expect(navigation.message).not.toContain(privateDiagnostic);
    expect(navigation._tag === "PageCaptureNavigationError" && navigation.cause).toMatchObject({
      message: expect.stringContaining(privateDiagnostic),
    });

    const protocol = await captureError(binding, request(CapturePageMarkdown.make({})));
    expect(protocol).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: "The Quick Action answered HTTP 500",
    });
    expect(protocol.message).not.toContain(privateDiagnostic);
    expect(protocol._tag === "PageCaptureProtocolError" && protocol.cause).toMatchObject({
      message: expect.stringContaining(privateDiagnostic),
    });

    const envelope = await captureError(binding, request(CapturePageMarkdown.make({})));
    expect(envelope).toMatchObject({
      _tag: "PageCaptureNavigationError",
      message: "The Quick Action reported a navigation failure",
    });
    expect(envelope.message).not.toContain(privateDiagnostic);
    expect(envelope._tag === "PageCaptureNavigationError" && envelope.cause).toMatchObject({
      message: expect.stringContaining(privateDiagnostic),
    });
    expect(await captureError(binding, request(CapturePageMarkdown.make({})))).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: expect.stringContaining("valid response envelope"),
    });
  });

  it("enforces the output byte budget on the encoded response", async () => {
    const { binding } = makeBinding([jsonResponse({ success: true, result: "x".repeat(4_096) })]);
    const error = await captureError(
      binding,
      request(CapturePageMarkdown.make({}), { maxOutputBytes: 1_024 }),
    );
    expect(error).toMatchObject({ _tag: "PageCaptureOutputLimitError", limit: 1_024 });
  });

  it("stops oversized streams at the first exceeding chunk and releases their reader", async () => {
    let chunksRead = 0;
    let canceled = false;
    const stream = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          chunksRead += 1;
          controller.enqueue(new Uint8Array(512));
        },
        cancel() {
          canceled = true;
        },
      },
      { highWaterMark: 0 },
    );
    const { binding } = makeBinding([new Response(stream)]);

    const error = await captureError(
      binding,
      request(CapturePageMarkdown.make({}), { maxOutputBytes: 1_024 }),
    );

    expect(error).toMatchObject({
      _tag: "PageCaptureOutputLimitError",
      limit: 1_024,
      observed: 1_536,
    });
    expect(chunksRead).toBe(3);
    expect(canceled).toBe(true);
    expect(stream.locked).toBe(false);
  });

  it("cancels and unlocks an in-flight response reader when capture is interrupted", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const readStarted = yield* Deferred.make<void>();
        let canceled = false;
        const stream = new ReadableStream<Uint8Array>(
          {
            pull() {
              Effect.runSync(Deferred.succeed(readStarted, undefined));
              return new Promise<void>(() => {});
            },
            cancel() {
              canceled = true;
            },
          },
          { highWaterMark: 0 },
        );
        const { binding } = makeBinding([new Response(stream)]);
        const program = Effect.gen(function* () {
          const port = yield* PageCapture;
          return yield* port.capture(request(CapturePageMarkdown.make({})));
        }).pipe(Effect.provide(captureLayer(binding)));

        const fiber = yield* Effect.forkChild(program);
        yield* Deferred.await(readStarted);
        yield* Fiber.interrupt(fiber);

        expect(canceled).toBe(true);
        expect(stream.locked).toBe(false);
      }),
    );
  });

  it("rejects kitesurf typed without calling the binding and types binding rejections", async () => {
    const foreignCause = new Error("binding exploded; token=host-only-secret");
    const { binding, calls } = makeBinding([foreignCause]);
    const unsupported = await captureError(
      binding,
      request(CapturePageMarkdown.make({}), { engine: "kitesurf" }),
    );
    expect(unsupported).toMatchObject({ _tag: "PageCaptureUnsupportedError", feature: "engine" });
    expect(calls).toHaveLength(0);

    const failure = await captureError(binding, request(CapturePageMarkdown.make({})));
    expect(failure).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: "The browser binding rejected the Quick Action",
    });
    expect(failure.message).not.toContain("host-only-secret");
    expect(failure._tag === "PageCaptureProtocolError" && failure.cause).toBe(foreignCause);
  });

  it("preserves foreign stream-read failures inside the typed protocol error", async () => {
    const foreignCause = new Error("response stream exploded; token=host-only-secret");
    const response = new Response(
      new ReadableStream<Uint8Array>({
        pull() {
          throw foreignCause;
        },
      }),
    );
    const { binding } = makeBinding([response]);

    const failure = await captureError(binding, request(CapturePageMarkdown.make({})));
    expect(failure).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: "Reading the Quick Action response failed",
    });
    expect(failure.message).not.toContain("host-only-secret");
    expect(failure._tag === "PageCaptureProtocolError" && failure.cause).toBe(foreignCause);
  });
});

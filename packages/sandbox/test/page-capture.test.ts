import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CapturePageStructured,
  PageCaptureError,
  PageCaptureInferenceUse,
  PageCaptureOutput,
  PageCaptureRateLimitedError,
  PageCaptureRequest,
  PageCaptureResult,
  PageUrlTarget,
  SandboxImplementation,
} from "../src/index.ts";

const implementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "test-page-capture",
});

describe("PageCapture schemas", () => {
  it("round-trips a complete structured capture request", () => {
    const request = PageCaptureRequest.make({
      target: PageUrlTarget.make({ url: "https://docs.example.com/pricing" }),
      action: CapturePageStructured.make({
        responseFormat: { type: "object", properties: { plans: { type: "array" } } },
        prompt: "Extract the pricing plans",
      }),
      engine: "chromium",
      limits: { maxOutputBytes: 128 * 1024 },
      navigation: {
        waitUntil: "networkidle0",
        timeoutMillis: 15_000,
        waitForSelector: { selector: "#plans", timeoutMillis: 5_000 },
      },
      viewport: { width: 1_280, height: 800 },
      resourcePolicy: {
        rejectResourceTypes: ["image", "media", "font"],
        allowRequestPatterns: ["https://docs.example.com/*"],
      },
    });

    expect(
      Schema.decodeSync(PageCaptureRequest)(Schema.encodeSync(PageCaptureRequest)(request)),
    ).toEqual(request);
  });

  it("keeps outputs, results, and expected failures schema-decodable", () => {
    const result = PageCaptureResult.make({
      implementation,
      output: { _tag: "PageMarkdownCaptured", markdown: "# Title" },
      resourceUse: {
        browserMillis: 1_234,
        inference: { provider: "cloudflare-workers-ai", modelCalls: 1 },
      },
    });
    const failure = PageCaptureRateLimitedError.make({
      implementation,
      reason: "rate",
      retryAfterMillis: 7_000,
      message: "429 Too many requests",
    });

    expect(
      Schema.decodeSync(PageCaptureOutput)({ _tag: "PageLinksCaptured", links: ["https://a"] }),
    ).toEqual({ _tag: "PageLinksCaptured", links: ["https://a"] });
    expect(
      Schema.decodeSync(PageCaptureResult)(Schema.encodeSync(PageCaptureResult)(result)),
    ).toEqual(result);
    expect(
      Schema.decodeSync(PageCaptureError)(Schema.encodeSync(PageCaptureError)(failure)),
    ).toEqual(failure);
  });

  it("accepts only bounded object JSON Schema documents for structured capture", () => {
    const invalidDocuments: ReadonlyArray<unknown> = [
      null,
      "object",
      [],
      {},
      { type: "string" },
      { type: "object", properties: [] },
      { type: "object", properties: { price: 7 } },
      { type: "object", properties: { price: { type: "unsupported" } } },
      { type: "object", required: ["price", "price"] },
      { type: "object", description: "x".repeat(64 * 1024 + 1) },
      {
        type: "object",
        properties: Object.fromEntries(
          Array.from({ length: 257 }, (_, index) => [`field${String(index)}`, { type: "string" }]),
        ),
      },
    ];

    let tooDeep: unknown = { type: "string" };
    for (let depth = 0; depth < 40; depth++) {
      tooDeep = { type: "object", properties: { nested: tooDeep } };
    }

    const cyclic: { readonly type: "object"; readonly properties: Record<string, unknown> } = {
      type: "object",
      properties: {},
    };
    cyclic.properties.self = cyclic;

    for (const responseFormat of [...invalidDocuments, tooDeep, cyclic]) {
      expect(
        Schema.decodeUnknownExit(CapturePageStructured)({
          _tag: "CapturePageStructured",
          responseFormat,
        })._tag,
      ).toBe("Failure");
    }
  });

  it("rejects out-of-bounds untrusted values at the Schema boundary", () => {
    expect(() =>
      PageCaptureRequest.make({
        target: PageUrlTarget.make({ url: "https://docs.example.com" }),
        action: { _tag: "CapturePageContent" },
        engine: "chromium",
        limits: { maxOutputBytes: 16 * 1024 * 1024 },
      }),
    ).toThrow(/Schema validation failed/);
    expect(() => PageUrlTarget.make({ url: "" })).toThrow(/Schema validation failed/);
    expect(() =>
      PageCaptureInferenceUse.make({ provider: "cloudflare-workers-ai", modelCalls: 0 }),
    ).toThrow(/Schema validation failed/);
    expect(() =>
      PageCaptureRequest.make({
        target: PageUrlTarget.make({ url: "https://docs.example.com" }),
        action: { _tag: "CapturePageContent" },
        engine: "chromium",
        limits: { maxOutputBytes: 1_024 },
        navigation: { timeoutMillis: 120_000 },
      }),
    ).toThrow(/Schema validation failed/);
  });
});

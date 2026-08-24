import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CapturePageStructured,
  PageCaptureError,
  PageCaptureInferenceUse,
  PageCaptureInferencePolicyError,
  PageCaptureOutput,
  PageCaptureProtocolError,
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
        responseFormat: {
          type: "object",
          title: "Pricing plans",
          description: "The public subscription plans.",
          properties: {
            plans: {
              type: "array",
              minItems: 1,
              items: { $ref: "#/$defs/Plan" },
            },
          },
          required: ["plans"],
          additionalProperties: false,
          $defs: {
            Plan: {
              type: "object",
              properties: {
                tier: { type: "string", enum: ["Basic", "Business"] },
                monthlyUsd: { type: "number", minimum: 0 },
              },
              required: ["tier", "monthlyUsd"],
            },
          },
        },
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
    const foreignCause = new Error("browser RPC failed");
    const protocolFailure = PageCaptureProtocolError.make({
      implementation,
      message: "The browser RPC failed",
      cause: foreignCause,
    });
    const inferenceFailure = PageCaptureInferencePolicyError.make({
      implementation,
      provider: "cloudflare-workers-ai",
      reason: "authorization",
      message: "The capture provider was not authorized",
      cause: new Error("tenant budget denied the provider"),
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
    expect(
      Schema.decodeSync(PageCaptureError)(Schema.encodeSync(PageCaptureError)(protocolFailure)),
    ).toMatchObject({
      _tag: "PageCaptureProtocolError",
      message: "The browser RPC failed",
      cause: expect.objectContaining({ message: "browser RPC failed" }),
    });
    expect(
      Schema.decodeSync(PageCaptureError)(Schema.encodeSync(PageCaptureError)(inferenceFailure)),
    ).toMatchObject({
      _tag: "PageCaptureInferencePolicyError",
      provider: "cloudflare-workers-ai",
      reason: "authorization",
      cause: expect.objectContaining({ message: "tenant budget denied the provider" }),
    });
  });

  it("accepts only credential-free HTTPS targets and credential-free absolute web links", () => {
    for (const url of [
      "not a URL",
      "http://docs.example.com/pricing",
      "javascript:alert(1)",
      "data:text/html,hello",
      "/pricing",
      "https://user:secret@docs.example.com/pricing",
      "https://user@docs.example.com/pricing",
    ]) {
      expect(Schema.decodeUnknownExit(PageUrlTarget)({ _tag: "PageUrlTarget", url })._tag).toBe(
        "Failure",
      );
    }

    expect(
      Schema.decodeSync(PageCaptureOutput)({
        _tag: "PageLinksCaptured",
        links: ["http://docs.example.com/legacy", "https://docs.example.com/current"],
      }),
    ).toEqual({
      _tag: "PageLinksCaptured",
      links: ["http://docs.example.com/legacy", "https://docs.example.com/current"],
    });

    for (const link of [
      "not a URL",
      "javascript:alert(1)",
      "mailto:person@example.com",
      "/pricing",
      "https://user:secret@docs.example.com/pricing",
    ]) {
      expect(
        Schema.decodeUnknownExit(PageCaptureOutput)({
          _tag: "PageLinksCaptured",
          links: [link],
        })._tag,
      ).toBe("Failure");
    }
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
      { type: "object", $ref: 42 },
      { type: "object", enum: "not-an-array" },
      { type: "object", description: 7 },
      { type: "object", unsupportedKeyword: true },
      { type: "object", properties: { price: { type: "number", minimum: "zero" } } },
      { type: "object", properties: { price: { type: "number", unsupportedKeyword: true } } },
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

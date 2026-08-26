import { Schema, type Effect, type Scope } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  BrowserActionResult,
  BrowserClickRequest,
  BrowserFillRequest,
  BrowserNavigateRequest,
  BrowserNavigationResult,
  BrowserReadTextRequest,
  BrowserTextResult,
  InteractiveBrowserActionError,
  InteractiveBrowserBusyError,
  InteractiveBrowserCapacityError,
  InteractiveBrowserError,
  InteractiveBrowserExpiredError,
  InteractiveBrowserHost,
  InteractiveBrowserLimitError,
  InteractiveBrowserPolicy,
  InteractiveBrowserPolicyDeniedError,
  InteractiveBrowserProtocolError,
  InteractiveBrowserUnsupportedError,
  SandboxImplementation,
  type BrowserHandle,
  type InteractiveBrowser,
  type InteractiveBrowserError as BrowserError,
} from "../src/index.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Open = InteractiveBrowser["Service"]["open"];
type OpenResult =
  ReturnType<Open> extends Effect.Effect<infer A, infer E, infer R> ? [A, E, R] : never;

describe("InteractiveBrowser schemas", () => {
  it("round-trips policy, requests, results, and typed errors", () => {
    const implementation = SandboxImplementation.make({
      isolation: "isolated",
      identity: "interactive-test",
    });
    const policy = InteractiveBrowserPolicy.make({
      allowedHosts: ["example.com", "example.com:8443"],
      maxActions: 3,
      maxElapsedMillis: 1_000,
      maxReturnedBytes: 1_024,
    });
    expect(
      Schema.decodeSync(InteractiveBrowserPolicy)(
        Schema.encodeSync(InteractiveBrowserPolicy)(policy),
      ),
    ).toEqual(policy);
    expect(BrowserNavigateRequest.make({ url: "https://example.com/" }).url).toBe(
      "https://example.com/",
    );
    expect(BrowserReadTextRequest.make({ selector: "main" }).selector).toBe("main");
    expect(BrowserFillRequest.make({ selector: "#q", value: "value" }).value).toBe("value");
    expect(BrowserClickRequest.make({ selector: "button" }).selector).toBe("button");
    expect(BrowserNavigationResult.make({ url: "https://example.com/final" }).url).toBe(
      "https://example.com/final",
    );
    expect(BrowserActionResult.make({ url: "https://example.com/after" }).url).toBe(
      "https://example.com/after",
    );
    expect(BrowserTextResult.make({ text: "page" }).text).toBe("page");
    const errors: ReadonlyArray<BrowserError> = [
      InteractiveBrowserPolicyDeniedError.make({
        implementation,
        message: "policy denied",
      }),
      InteractiveBrowserBusyError.make({ implementation, message: "busy" }),
      InteractiveBrowserCapacityError.make({ implementation, message: "capacity" }),
      InteractiveBrowserExpiredError.make({ implementation, message: "expired" }),
      InteractiveBrowserActionError.make({
        implementation,
        operation: "navigate",
        message: "navigation failed",
      }),
      InteractiveBrowserProtocolError.make({ implementation, message: "malformed output" }),
      InteractiveBrowserLimitError.make({
        implementation,
        limit: "returned-bytes",
        maximum: 8,
        observed: 9,
        message: "too much text",
      }),
      InteractiveBrowserUnsupportedError.make({
        implementation,
        feature: "click",
        message: "click unsupported",
      }),
    ];
    for (const error of errors) {
      const encoded = Schema.encodeSync(InteractiveBrowserError)(error);
      expect(Schema.decodeSync(InteractiveBrowserError)(encoded)._tag).toBe(error._tag);
    }
  });

  it("rejects malformed authorities, requests, and limits", () => {
    for (const host of [
      "EXAMPLE.com",
      "https://example.com",
      "user@example.com",
      "example.com:443",
      "example.com/",
    ])
      expect(Schema.decodeUnknownExit(InteractiveBrowserHost)(host)._tag).toBe("Failure");
    const valid = {
      allowedHosts: ["example.com"],
      maxActions: 1,
      maxElapsedMillis: 1,
      maxReturnedBytes: 1,
    };
    for (const value of [
      { ...valid, allowedHosts: [] },
      { ...valid, allowedHosts: ["example.com", "example.com"] },
      { ...valid, allowedHosts: Array.from({ length: 65 }, (_, index) => `h${index}.example`) },
      { ...valid, maxActions: 0 },
      { ...valid, maxActions: 1_001 },
      { ...valid, maxElapsedMillis: 0 },
      { ...valid, maxElapsedMillis: 600_001 },
      { ...valid, maxReturnedBytes: 0 },
      { ...valid, maxReturnedBytes: 8 * 1024 * 1024 + 1 },
    ])
      expect(Schema.decodeUnknownExit(InteractiveBrowserPolicy)(value)._tag).toBe("Failure");
    for (const value of [{ url: "http://example.com" }, { url: "https://u:p@example.com" }])
      expect(Schema.decodeUnknownExit(BrowserNavigateRequest)(value)._tag).toBe("Failure");
    for (const value of [{ selector: "" }, { selector: "x".repeat(1_025) }])
      expect(Schema.decodeUnknownExit(BrowserClickRequest)(value)._tag).toBe("Failure");
    for (const value of [{ selector: "" }, { selector: "x".repeat(1_025) }])
      expect(Schema.decodeUnknownExit(BrowserReadTextRequest)(value)._tag).toBe("Failure");
    expect(
      Schema.decodeUnknownExit(BrowserFillRequest)({ selector: "#q", value: "x".repeat(65_537) })
        ._tag,
    ).toBe("Failure");
    expect(
      Schema.decodeUnknownExit(BrowserTextResult)({ text: "x".repeat(8 * 1024 * 1024 + 1) })._tag,
    ).toBe("Failure");
    expect(Schema.decodeUnknownExit(BrowserActionResult)({ url: "http://example.com" })._tag).toBe(
      "Failure",
    );
  });

  it("rejects malformed values for every expected error shape", () => {
    const implementation = { isolation: "isolated", identity: "test" };
    const values: ReadonlyArray<unknown> = [
      { _tag: "UnknownInteractiveBrowserError", implementation, message: "unknown" },
      {
        _tag: "InteractiveBrowserPolicyDeniedError",
        implementation,
        message: "x".repeat(8 * 1024 + 1),
      },
      { _tag: "InteractiveBrowserBusyError", implementation },
      { _tag: "InteractiveBrowserCapacityError", implementation, message: 42 },
      { _tag: "InteractiveBrowserExpiredError", implementation, message: null },
      {
        _tag: "InteractiveBrowserActionError",
        implementation,
        operation: "download",
        message: "unsupported operation",
      },
      { _tag: "InteractiveBrowserProtocolError", implementation },
      {
        _tag: "InteractiveBrowserLimitError",
        implementation,
        limit: "actions",
        maximum: 0,
        observed: -1,
        message: "bad limit",
      },
      {
        _tag: "InteractiveBrowserUnsupportedError",
        implementation,
        feature: "cookies",
        message: "unsupported",
      },
    ];
    for (const value of values) {
      expect(Schema.decodeUnknownExit(InteractiveBrowserError)(value)._tag).toBe("Failure");
    }
  });

  it("pins scoped open and a non-Schema handle", () => {
    const result: Equal<OpenResult, [BrowserHandle, BrowserError, Scope.Scope]> = true;
    const handleIsNotSchema: Equal<
      BrowserHandle extends typeof InteractiveBrowserError.Type ? true : false,
      false
    > = true;
    expect(result && handleIsNotSchema).toBe(true);
  });
});

import { Schema, type Effect, type Scope } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  BrowserActionResult,
  BrowserClickRequest,
  BrowserFillRequest,
  BrowserNavigateRequest,
  BrowserNavigationResult,
  BrowserReadTextRequest,
  BrowserScreenshotRequest,
  BrowserScrollRequest,
  BrowserTextResult,
  InteractiveBrowserActionError,
  InteractiveBrowserBusyError,
  InteractiveBrowserCapacityError,
  InteractiveBrowserError,
  InteractiveBrowserExpiredError,
  InteractiveBrowserHost,
  InteractiveBrowserLimitError,
  type InteractiveBrowserNetworkPolicy,
  InteractiveBrowserPolicy,
  InteractiveBrowserPolicyDeniedError,
  InteractiveBrowserProtocolError,
  InteractiveBrowserTargetUrl,
  InteractiveBrowserUnsupportedError,
  SandboxImplementation,
  type BrowserHandle,
  type InteractiveBrowser,
  type InteractiveBrowserError as BrowserError,
  type PageScreenshotResult,
  type ProtectedBrowser,
  type ProtectedBrowserHandle,
  type ProtectedBrowserError,
  type BrowserCredentialAccess,
  CredentialOrigin,
  CardCredential,
} from "../src/index.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Open = InteractiveBrowser["Service"]["open"];
type OpenResult =
  ReturnType<Open> extends Effect.Effect<infer A, infer E, infer R> ? [A, E, R] : never;

describe("InteractiveBrowser schemas", () => {
  it("keeps protected material and authority outside the model contract", () => {
    const open: Equal<
      ReturnType<ProtectedBrowser["Service"]["open"]>,
      Effect.Effect<
        ProtectedBrowserHandle,
        ProtectedBrowserError,
        Scope.Scope | BrowserCredentialAccess
      >
    > = true;

    const channels: Equal<
      Extract<
        keyof ProtectedBrowserHandle,
        "fill" | "screenshot" | "sessionId" | "getLiveView" | "handoff"
      >,
      never
    > = true;

    expect(open && channels).toBe(true);
    for (const origin of [
      "https://example.com/",
      "https://example.com:443",
      "http://example.com",
      "https://EXAMPLE.com",
      "https://u:p@example.com",
    ]) {
      expect(Schema.decodeUnknownExit(CredentialOrigin)(origin)._tag).toBe("Failure");
    }
    expect(
      Schema.decodeUnknownExit(CardCredential)({
        _tag: "CardCredential",
        number: "4111111111111111",
      })._tag,
    ).toBe("Failure");
  });
  it("round-trips policy, requests, results, and typed errors", () => {
    const implementation = SandboxImplementation.make({
      isolation: "isolated",
      identity: "interactive-test",
    });

    for (const network of [
      { _tag: "ExactHosts", allowedHosts: ["example.com", "example.com:8443"] },
      { _tag: "PublicWeb" },
      { _tag: "Unrestricted" },
    ] satisfies ReadonlyArray<InteractiveBrowserNetworkPolicy>) {
      const policy = InteractiveBrowserPolicy.make({
        network,
        maxActions: 3,
        maxElapsedMillis: 1_000,
        maxReturnedBytes: 1_024,
      });

      expect(
        Schema.decodeSync(InteractiveBrowserPolicy)(
          Schema.encodeSync(InteractiveBrowserPolicy)(policy),
        ),
      ).toEqual(policy);
    }
    expect(BrowserNavigateRequest.make({ url: "https://example.com/" }).url).toBe(
      "https://example.com/",
    );
    for (const url of ["http://example.com/", "https://example.org/", "http://127.0.0.1:8080/"]) {
      expect(InteractiveBrowserTargetUrl.make(url)).toBe(url);
      expect(BrowserNavigateRequest.make({ url }).url).toBe(url);
      expect(BrowserNavigationResult.make({ url }).url).toBe(url);
      expect(BrowserActionResult.make({ url }).url).toBe(url);
    }
    expect(BrowserReadTextRequest.make({ selector: "main" }).selector).toBe("main");
    expect(BrowserFillRequest.make({ selector: "#q", value: "value" }).value).toBe("value");
    expect(BrowserClickRequest.make({ selector: "button" }).selector).toBe("button");
    const screenshot = BrowserScreenshotRequest.make({ fullPage: false });

    expect(
      Schema.decodeSync(BrowserScreenshotRequest)(
        Schema.encodeSync(BrowserScreenshotRequest)(screenshot),
      ),
    ).toEqual(screenshot);
    const scroll = BrowserScrollRequest.make({ deltaX: -100_000, deltaY: 100_000 });

    expect(
      Schema.decodeSync(BrowserScrollRequest)(Schema.encodeSync(BrowserScrollRequest)(scroll)),
    ).toEqual(scroll);
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
      "*",
      "*.example.com",
      "example.*",
    ])
      expect(Schema.decodeUnknownExit(InteractiveBrowserHost)(host)._tag).toBe("Failure");

    const valid = {
      network: { _tag: "ExactHosts", allowedHosts: ["example.com"] },
      maxActions: 1,
      maxElapsedMillis: 1,
      maxReturnedBytes: 1,
    };

    for (const value of [
      { ...valid, allowedHosts: ["other.example"] },
      { ...valid, network: undefined },
      { ...valid, network: { _tag: "ExactHosts", allowedHosts: [] } },
      { ...valid, network: { _tag: "ExactHosts", allowedHosts: ["example.com", "example.com"] } },
      {
        ...valid,
        network: {
          _tag: "ExactHosts",
          allowedHosts: Array.from({ length: 65 }, (_, index) => `h${index}.example`),
        },
      },
      { ...valid, network: { _tag: "PublicWeb", allowedHosts: ["example.com"] } },
      { ...valid, network: { _tag: "PublicWeb", allowPrivate: true } },
      { ...valid, network: { _tag: "Unrestricted", allowedHosts: ["example.com"] } },
      { ...valid, network: { _tag: "Unknown" } },
      { ...valid, maxActions: 0 },
      { ...valid, maxActions: 1_001 },
      { ...valid, maxElapsedMillis: 0 },
      { ...valid, maxElapsedMillis: 600_001 },
      { ...valid, maxReturnedBytes: 0 },
      { ...valid, maxReturnedBytes: 8 * 1024 * 1024 + 1 },
    ])
      expect(Schema.decodeUnknownExit(InteractiveBrowserPolicy)(value)._tag).toBe("Failure");
    for (const url of [
      "https://u:p@example.com",
      "http://u:p@example.com",
      "file:///tmp/page",
      "about:blank",
      "javascript:alert(1)",
      "data:text/html,hello",
      "ftp://example.com/",
      "/relative",
      `https://example.com/${"a".repeat(8192)}`,
    ]) {
      expect(Schema.decodeUnknownExit(BrowserNavigateRequest)({ url })._tag).toBe("Failure");
      expect(Schema.decodeUnknownExit(BrowserNavigationResult)({ url })._tag).toBe("Failure");
      expect(Schema.decodeUnknownExit(BrowserActionResult)({ url })._tag).toBe("Failure");
    }
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
    for (const value of [{}, { fullPage: "true" }]) {
      expect(Schema.decodeUnknownExit(BrowserScreenshotRequest)(value)._tag).toBe("Failure");
    }
    for (const value of [
      { deltaX: 0 },
      { deltaX: 0, deltaY: 0.5 },
      { deltaX: -100_001, deltaY: 0 },
      { deltaX: 0, deltaY: 100_001 },
      { deltaX: Number.NaN, deltaY: 0 },
      { deltaX: 0, deltaY: Number.POSITIVE_INFINITY },
    ]) {
      expect(Schema.decodeUnknownExit(BrowserScrollRequest)(value)._tag).toBe("Failure");
    }
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

    const screenshot: Equal<
      ReturnType<BrowserHandle["screenshot"]>,
      Effect.Effect<PageScreenshotResult, BrowserError>
    > = true;

    const scroll: Equal<
      ReturnType<BrowserHandle["scroll"]>,
      Effect.Effect<BrowserActionResult, BrowserError>
    > = true;

    const close: Equal<BrowserHandle["close"], Effect.Effect<void, BrowserError>> = true;

    const hasNoHostControls: Equal<
      Extract<keyof BrowserHandle, "sessionId" | "getLiveView" | "handoff" | "getHandoffState">,
      never
    > = true;

    expect(result && handleIsNotSchema && screenshot && scroll && close && hasNoHostControls).toBe(
      true,
    );
  });
});

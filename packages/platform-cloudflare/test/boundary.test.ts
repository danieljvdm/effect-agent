import { describe, expect, it } from "vite-plus/test";

import {
  cloudflareFailureSignals,
  safeCauseDiagnostic,
  safeCauseMessage,
} from "../src/internal/boundary.ts";

describe("Cloudflare foreign failure boundaries", () => {
  it("keeps hostile diagnostics and signal probes total", () => {
    const hostileError = Object.create(Error.prototype, {
      message: {
        get() {
          throw new Error("message getter escaped");
        },
      },
      name: {
        get() {
          throw new Error("name getter escaped");
        },
      },
    });

    const hostileProxy = new Proxy(
      {},
      {
        get() {
          throw new Error("property getter escaped");
        },
        getPrototypeOf() {
          throw new Error("prototype probe escaped");
        },
      },
    );

    expect(safeCauseMessage(hostileError, "fallback message")).toBe("fallback message");
    expect(safeCauseDiagnostic(hostileError, "fallback diagnostic")).toBe("fallback diagnostic");
    expect(safeCauseMessage(hostileProxy, "fallback proxy")).toBe("fallback proxy");
    expect(cloudflareFailureSignals(hostileProxy)).toEqual({});
    expect(safeCauseMessage("x".repeat(10_000), "fallback")).toHaveLength(8_192);
  });

  it("preserves Cloudflare reset and overload classifications", () => {
    expect(cloudflareFailureSignals({ retryable: false, overloaded: true })).toEqual({
      retryable: false,
      overloaded: true,
    });
    expect(cloudflareFailureSignals({ durableObjectReset: true })).toEqual({ retryable: true });
  });
});

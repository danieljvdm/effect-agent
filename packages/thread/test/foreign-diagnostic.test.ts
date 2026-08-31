import { describe, expect, it } from "@effect/vitest";

import { inspectForeignDiagnostic, safeUnknownString } from "../src/foreign-diagnostic.ts";

describe("foreign failure diagnostics", () => {
  it("reads only Schema-validated diagnostic fields", () => {
    expect(
      inspectForeignDiagnostic({
        _tag: "SupplierUnavailable",
        message: "supplier timed out",
        secret: "must not be projected",
      }),
    ).toEqual({ tag: "SupplierUnavailable", message: "supplier timed out" });
  });

  it("keeps hostile getters, proxies, and coercion hooks total", () => {
    const hostile = new Proxy(Object.create(null), {
      get() {
        throw new Error("getter escaped");
      },
      getOwnPropertyDescriptor() {
        throw new Error("descriptor escaped");
      },
      ownKeys() {
        throw new Error("keys escaped");
      },
    });
    const hostileCoercion = {
      [Symbol.toPrimitive]() {
        throw new Error("coercion escaped");
      },
    };

    expect(inspectForeignDiagnostic(hostile)).toEqual({});
    expect(safeUnknownString(hostileCoercion, "fallback")).toBe("fallback");
  });
});

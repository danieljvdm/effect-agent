import { Schema } from "effect";
import { CapturePageStructured } from "effect-agent/page-capture";
import { describe, expect, it } from "vite-plus/test";

describe("Structured capture graph bounds", () => {
  it("rejects deep and cyclic structured capture schema graphs", () => {
    let tooDeep: unknown = { type: "string" };

    for (let depth = 0; depth < 40; depth++) {
      tooDeep = { type: "object", properties: { nested: tooDeep } };
    }

    const cyclic: { readonly type: "object"; readonly properties: Record<string, unknown> } = {
      type: "object",
      properties: {},
    };

    cyclic.properties.self = cyclic;

    for (const [index, responseFormat] of [tooDeep, cyclic].entries()) {
      expect(
        Schema.decodeUnknownExit(CapturePageStructured)({
          _tag: "CapturePageStructured",
          responseFormat,
        })._tag,
        `invalid response format ${index}`,
      ).toBe("Failure");
    }
  });
});

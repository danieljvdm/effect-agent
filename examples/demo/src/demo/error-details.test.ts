import { describe, expect, it } from "vite-plus/test";

import { decodeErrorDetails, toDemoRunFailure } from "./error-details";
import { DemoRunFailure } from "./operational-contracts";

describe("demo failure mapping", () => {
  it("decodes safe string fields tolerantly", () => {
    expect(decodeErrorDetails({ _tag: "AiError", message: "boom" })).toEqual({
      _tag: "AiError",
      message: "boom",
    });
    expect(decodeErrorDetails(42)).toEqual({});
  });

  it("passes an already-typed DemoRunFailure through unchanged", () => {
    const failure = DemoRunFailure.make({
      errorTag: "DemoRunAlreadyActive",
      message: "Finish or stop the current ephemeral Run before starting another.",
    });

    expect(toDemoRunFailure(failure)).toBe(failure);
  });

  it("keeps the specific tag, redacts credentials, and bounds the message", () => {
    const mapped = toDemoRunFailure({
      _tag: "AiError",
      message: `The provider refused key sk-${"a".repeat(40)} for this request.`,
    });

    expect(mapped.errorTag).toBe("AiError");
    expect(mapped.message).toContain("[redacted]");
    expect(mapped.message).not.toContain("sk-");

    const oversized = toDemoRunFailure({ _tag: "AiError", message: "x".repeat(5_000) });
    expect(oversized.message.length).toBe(1_000);
  });

  it("falls back to the generic tag for untagged defects and plain values", () => {
    const fromDefect = toDemoRunFailure(new Error("The supplier catalog crashed."));
    expect(fromDefect.errorTag).toBe("DemoRunError");
    expect(fromDefect.message).toContain("crashed");

    const fromString = toDemoRunFailure("plain failure");
    expect(fromString.errorTag).toBe("DemoRunError");
    expect(fromString.message).toBe("plain failure");
  });
});

import { describe, expect, it } from "vite-plus/test";

import { Effect } from "effect";

describe("Phase 0 toolchain", () => {
  it("executes an Effect program through the Vite+ test runner", () => {
    expect(Effect.runSync(Effect.succeed("ready"))).toBe("ready");
  });
});

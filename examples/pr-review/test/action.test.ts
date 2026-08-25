import { describe, expect, it } from "@effect/vitest";

import { prepareReviewSurface } from "../src/action.ts";
import type { ChangedFile } from "../src/github.ts";

const file = (path: string, patch: string | undefined): ChangedFile => ({
  path,
  status: "modified",
  additions: 1,
  deletions: 0,
  patch,
});

describe("GitHub diff admission", () => {
  it("PRR-003 bounds admitted files and reports the remainder", () => {
    const files = Array.from({ length: 102 }, (_, index) =>
      file(`src/file-${String(index)}.ts`, "@@ -0,0 +1 @@\n+export const value = 1;"),
    );
    const surface = prepareReviewSurface(files, []);

    expect(surface.changes).toHaveLength(100);
    expect(surface.unreviewedPaths).toEqual(["src/file-100.ts", "src/file-101.ts"]);
  });

  it("PRR-003 distinguishes ignored and unavailable files", () => {
    const surface = prepareReviewSurface(
      [file("bun.lock", "@@ -1 +1 @@\n-old\n+new"), file("assets/image.png", undefined)],
      ["**/*.lock"],
    );

    expect(surface.changes).toHaveLength(0);
    expect(surface.ignoredPaths).toEqual(["bun.lock"]);
    expect(surface.unreviewedPaths).toEqual(["assets/image.png"]);
  });
});

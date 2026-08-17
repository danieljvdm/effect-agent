import { describe, expect, it } from "@effect/vitest";

import { typescriptNodeArguments, workerEntry, workerNodeArguments } from "./harness.ts";

describe("crash worker Node arguments", () => {
  it("uses the tsx loader rather than Node's removed transform-types flag", () => {
    expect(workerNodeArguments()).toEqual(["--import", "tsx", workerEntry]);
    expect(workerNodeArguments()).not.toContain("--experimental-transform-types");
    expect(typescriptNodeArguments("worker.ts")).toEqual(["--import", "tsx", "worker.ts"]);
  });
});

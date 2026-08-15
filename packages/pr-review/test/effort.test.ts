import { describe, expect, it } from "@effect/vitest";

import {
  EFFORT_ALIASES,
  parseEffortPosition,
  PROVIDER_EFFORT_RUNGS,
  resolveEffortRung,
} from "../src/index.ts";

describe("parseEffortPosition", () => {
  it("accepts every alias name case-insensitively", () => {
    expect(parseEffortPosition("low")).toBe(0);
    expect(parseEffortPosition("MEDIUM")).toBe(0.25);
    expect(parseEffortPosition(" high ")).toBe(0.5);
    expect(parseEffortPosition("xhigh")).toBe(0.75);
    expect(parseEffortPosition("max")).toBe(1);
  });

  it("accepts a bare number on [0, 1] and nothing outside it", () => {
    expect(parseEffortPosition("0")).toBe(0);
    expect(parseEffortPosition("0.6")).toBe(0.6);
    expect(parseEffortPosition("1")).toBe(1);
    expect(parseEffortPosition("1.5")).toBeUndefined();
    expect(parseEffortPosition("-0.1")).toBeUndefined();
    expect(parseEffortPosition("NaN")).toBeUndefined();
    expect(parseEffortPosition("hgih")).toBeUndefined();
    expect(parseEffortPosition("")).toBeUndefined();
  });
});

describe("resolveEffortRung", () => {
  it("rounds down between rungs so resolution never costs more than asked", () => {
    expect(resolveEffortRung(0, PROVIDER_EFFORT_RUNGS.openai)).toBe("low");
    expect(resolveEffortRung(0.5, PROVIDER_EFFORT_RUNGS.openai)).toBe("medium");
    expect(resolveEffortRung(0.75, PROVIDER_EFFORT_RUNGS.openai)).toBe("high");
    expect(resolveEffortRung(1, PROVIDER_EFFORT_RUNGS.openai)).toBe("xhigh");
    expect(resolveEffortRung(1, PROVIDER_EFFORT_RUNGS.anthropic)).toBe("high");
    expect(resolveEffortRung(0.5, PROVIDER_EFFORT_RUNGS.anthropic)).toBe("medium");
  });

  it("clamps out-of-range positions instead of inventing rungs", () => {
    expect(resolveEffortRung(-1, PROVIDER_EFFORT_RUNGS.openai)).toBe("low");
    expect(resolveEffortRung(2, PROVIDER_EFFORT_RUNGS.openai)).toBe("xhigh");
  });

  it("lands every alias on a rung each provider actually offers", () => {
    for (const position of Object.values(EFFORT_ALIASES)) {
      for (const rungs of [PROVIDER_EFFORT_RUNGS.openai, PROVIDER_EFFORT_RUNGS.anthropic]) {
        expect(rungs).toContain(resolveEffortRung(position, rungs));
      }
    }
  });
});

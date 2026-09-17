import { describe, expect, it } from "@effect/vitest";

import { catalogue, commonTools, grade, tasks } from "../src/fixture.ts";

describe("benchmark correctness oracle", () => {
  it("requires exact requested values and real calls, independently of response wording", () => {
    const task = tasks[0]!;
    const calls = [{ name: "get_order_shipping", id: "ORD-104" }];

    expect(grade(task, ["TRACK-Q7M4", "in_transit"], calls)).toBe(true);
    for (const values of [
      [],
      ["in_transit"],
      ["not in_transit", "TRACK-Q7M4"],
      ["in_transit", "TRACK-Q7M4", "invented"],
      ["in_transit", "in_transit"],
    ]) {
      expect(grade(task, values, calls)).toBe(false);
    }
    expect(grade(task, task.evidence, [])).toBe(false);
    expect(grade(task, task.evidence, [{ name: "get_order_shipping", id: "ORD-999" }])).toBe(false);
  });

  it("keeps forced misses discoverable and the full-catalogue baseline intact", () => {
    expect(Object.keys(catalogue)).toHaveLength(50);
    expect(commonTools).toHaveLength(8);
    expect(tasks.filter((task) => task.withhold !== undefined)).toHaveLength(2);
    for (const task of tasks) {
      for (const name of task.withhold ?? []) {
        expect(Object.keys(catalogue)).toContain(name);
        expect(commonTools).not.toContain(name);
        expect(task.requiredCalls.some((call) => call.startsWith(`${name}/`))).toBe(true);
      }
    }
  });
});

import { describe, expect, it } from "@effect/vitest";
import { Result } from "effect";

import {
  scheduleDueOccurrence,
  scheduleNextAfter,
} from "../../src/durable/internal/schedule-time.ts";

const success = Result.getOrThrow;

describe("Schedule timing", () => {
  it("uses the named zone across the repeated fall DST hour", () => {
    const repeated = {
      _tag: "Cron" as const,
      expression: "30 1 * * *",
      timeZone: "America/New_York",
    };

    const first = success(scheduleNextAfter(repeated, Date.parse("2026-11-01T05:29:00.000Z")));

    expect(first).toBe(Date.parse("2026-11-01T05:30:00.000Z"));
    if (first === null) throw new Error("expected a fall DST occurrence");
    expect(success(scheduleNextAfter(repeated, first))).toBe(
      Date.parse("2026-11-02T06:30:00.000Z"),
    );
    expect(success(scheduleNextAfter(repeated, Date.parse("2026-11-01T06:15:00Z")))).toBe(
      Date.parse("2026-11-02T06:30:00Z"),
    );

    const folded = success(
      scheduleDueOccurrence(
        repeated,
        Date.parse("2026-10-31T05:30:00Z"),
        Date.parse("2026-11-01T06:15:00Z"),
      ),
    );

    expect(folded?.intendedAtMillis).toBe(Date.parse("2026-11-01T05:30:00Z"));
    expect(folded?.nextAtMillis).toBe(Date.parse("2026-11-02T06:30:00Z"));
    expect(
      success(
        scheduleDueOccurrence(
          repeated,
          Date.parse("2026-11-01T05:30:00.000Z"),
          Date.parse("2026-11-01T06:45:00.000Z"),
        ),
      ),
    ).toEqual({
      intendedAtMillis: Date.parse("2026-11-01T05:30:00.000Z"),
      nextAtMillis: Date.parse("2026-11-02T06:30:00.000Z"),
      skippedRange: null,
    });
  });

  it("coalesces long named-zone downtime across sparse dates and whole-day gaps", () => {
    for (const [expression, timeZone, cursor, now, intended, next] of [
      [
        "0 0 29 2 *",
        "America/New_York",
        "2000-02-29T05:00Z",
        "2026-03-08T07:45Z",
        "2024-02-29T05:00Z",
        "2028-02-29T05:00Z",
      ],
      [
        "0 12 * * *",
        "Pacific/Apia",
        "2011-12-28T22:00Z",
        "2011-12-31T00:00Z",
        "2011-12-30T22:00Z",
        "2011-12-31T22:00Z",
      ],
    ] as const) {
      const due = success(
        scheduleDueOccurrence(
          { _tag: "Cron", expression, timeZone },
          Date.parse(cursor),
          Date.parse(now),
        ),
      );

      expect(due?.intendedAtMillis).toBe(Date.parse(intended));
      expect(due?.nextAtMillis).toBe(Date.parse(next));
    }
  });
});

import { describe, expect, it } from "@effect/vitest";
import { Result } from "effect";

import {
  normalizeScheduleTiming,
  scheduleDueOccurrence,
  scheduleInitialCursor,
  scheduleNextAfter,
} from "../src/index.ts";

const success = Result.getOrThrow;

describe("Schedule timing", () => {
  it("resolves relative and interval requests once from the captured time", () => {
    expect(
      success(normalizeScheduleTiming({ _tag: "After", delayMillis: 5_000 }, 10_000, 60_000)),
    ).toEqual({ _tag: "At", atMillis: 15_000 });

    const defaultAnchor = success(
      normalizeScheduleTiming({ _tag: "Interval", everyMillis: 60_000 }, 10_000, 60_000),
    );

    expect(defaultAnchor).toEqual({
      _tag: "Interval",
      everyMillis: 60_000,
      anchorMillis: 70_000,
    });
    expect(success(scheduleInitialCursor(defaultAnchor, 10_000))).toBe(70_000);

    const pastAnchor = success(
      normalizeScheduleTiming(
        { _tag: "Interval", everyMillis: 60_000, anchorMillis: 10_000 },
        130_000,
        60_000,
      ),
    );

    expect(success(scheduleInitialCursor(pastAnchor, 130_000))).toBe(190_000);
  });

  it("validates cron frequency structurally without calendar enumeration", () => {
    expect(
      Result.isFailure(
        normalizeScheduleTiming(
          { _tag: "Cron", expression: "* * * * * *", timeZone: "UTC" },
          0,
          60_000,
        ),
      ),
    ).toBe(true);
    expect(
      Result.isFailure(
        normalizeScheduleTiming(
          { _tag: "Cron", expression: "* * * * *", timeZone: "UTC" },
          0,
          120_000,
        ),
      ),
    ).toBe(true);
    expect(
      success(
        normalizeScheduleTiming(
          { _tag: "Cron", expression: "*/5 * * * *", timeZone: "UTC" },
          0,
          300_000,
        ),
      ),
    ).toEqual({ _tag: "Cron", expression: "*/5 * * * *", timeZone: "UTC" });
  });

  it("coalesces intervals and includes a cron firing at the captured second", () => {
    expect(
      success(
        scheduleDueOccurrence(
          { _tag: "Interval", everyMillis: 60_000, anchorMillis: 60_000 },
          60_000,
          300_000,
        ),
      ),
    ).toEqual({
      intendedAtMillis: 300_000,
      nextAtMillis: 360_000,
      skippedRange: { fromMillis: 60_000, toMillis: 300_000 },
    });

    expect(
      success(
        scheduleDueOccurrence(
          { _tag: "Cron", expression: "* * * * *", timeZone: "UTC" },
          0,
          120_000,
        ),
      ),
    ).toEqual({
      intendedAtMillis: 120_000,
      nextAtMillis: 180_000,
      skippedRange: { fromMillis: 0, toMillis: 120_000 },
    });
  });

  it("does not infer elapsed-time frequency from local spacing across DST", () => {
    const timing = {
      _tag: "Cron" as const,
      expression: "30 3 * * *",
      timeZone: "America/New_York",
    };

    const first = success(scheduleNextAfter(timing, Date.parse("2026-03-07T00:00:00Z")));

    expect(first).toBe(Date.parse("2026-03-07T08:30:00Z"));
    const second = success(scheduleNextAfter(timing, Date.parse("2026-03-07T08:30:00Z")));

    expect(second).toBe(Date.parse("2026-03-08T07:30:00Z"));
    expect(Result.isFailure(normalizeScheduleTiming(timing, 0, 86_400_000))).toBe(true);
    expect(Result.isSuccess(normalizeScheduleTiming(timing, 0, 60_000))).toBe(true);
    for (const timeZone of ["UTC", "Etc/UTC", "+05:30"]) {
      expect(
        Result.isSuccess(normalizeScheduleTiming({ ...timing, timeZone }, 0, 86_400_000)),
      ).toBe(true);
    }
  });

  it("uses the named zone across spring and fall DST transitions", () => {
    const timing = {
      _tag: "Cron" as const,
      expression: "30 2 * * *",
      timeZone: "America/New_York",
    };

    expect(success(scheduleNextAfter(timing, Date.parse("2026-03-07T07:31:00.000Z")))).toBe(
      Date.parse("2026-03-08T07:30:00.000Z"),
    );
    for (const cursor of ["2026-03-07T07:30:00Z", "2026-03-08T07:30:00Z"]) {
      for (const now of ["2026-03-08T07:30:00Z", "2026-03-08T08:00:00Z"]) {
        const due = success(scheduleDueOccurrence(timing, Date.parse(cursor), Date.parse(now)));

        expect(due?.intendedAtMillis).toBe(Date.parse("2026-03-08T07:30:00Z"));
        expect(due?.nextAtMillis).toBe(Date.parse("2026-03-09T06:30:00Z"));
      }
    }

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
        "* * * * *",
        "America/New_York",
        "2000-01-01T05:00Z",
        "2026-03-08T07:15Z",
        "2026-03-08T07:15Z",
        "2026-03-08T07:16Z",
      ],
      [
        "30 2 * * *",
        "America/New_York",
        "2000-01-01T07:30Z",
        "2026-03-08T07:45Z",
        "2026-03-08T07:30Z",
        "2026-03-09T06:30Z",
      ],
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

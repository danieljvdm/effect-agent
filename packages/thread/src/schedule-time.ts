import { Cron, DateTime, Option, Result, Schema } from "effect";

import {
  ScheduleValidationError,
  ScheduleInstant,
  type ScheduleSkippedRange,
  type ScheduleTiming,
  type ScheduleTimingRequest,
} from "./schedule.ts";

const invalid = (message: string) => Result.fail(ScheduleValidationError.make({ message }));

const checkedInstant = (millis: number, label: string) =>
  Schema.decodeUnknownResult(ScheduleInstant)(millis).pipe(
    Result.mapError(() =>
      ScheduleValidationError.make({
        message: `${label} is outside the supported ScheduleInstant range`,
      }),
    ),
  );

const values = (set: ReadonlySet<number>, size: number): ReadonlyArray<number> =>
  set.size === 0 ? Array.from({ length: size }, (_, index) => index) : [...set];

/**
 * Cron frequency proof without calendar enumeration. It checks the minimum circular spacing of
 * the allowed hour/minute pairs. Day restrictions may make a rule sparser, so this deliberately
 * rejects some safe rules when a configured minimum exceeds one minute.
 */
const validateCronFrequency = (
  cron: Cron.Cron,
  minimumMillis: number,
): Result.Result<void, ScheduleValidationError> => {
  if (cron.seconds.size !== 1) {
    return invalid("Cron must select exactly one seconds value");
  }
  if (minimumMillis <= 60_000) return Result.succeed(undefined);
  if (
    Option.isSome(cron.tz) &&
    DateTime.isTimeZoneNamed(cron.tz.value) &&
    cron.tz.value.id !== "UTC"
  ) {
    return invalid("Cron minimums above one minute require UTC or a fixed-offset time zone");
  }

  const minuteOfDay = values(cron.hours, 24).flatMap((hour) =>
    values(cron.minutes, 60).map((minute) => hour * 60 + minute),
  );

  minuteOfDay.sort((left, right) => left - right);
  let minimumGap = 24 * 60;

  for (let index = 0; index < minuteOfDay.length; index += 1) {
    const current = minuteOfDay[index];
    const next = minuteOfDay[(index + 1) % minuteOfDay.length];

    if (current === undefined || next === undefined) continue;
    const gap = next > current ? next - current : 24 * 60 - current + next;

    minimumGap = Math.min(minimumGap, gap);
  }

  return minimumGap * 60_000 >= minimumMillis
    ? Result.succeed(undefined)
    : invalid(`Cron may fire more often than the configured ${minimumMillis}ms minimum`);
};

export const parseScheduleCron = (
  expression: string,
  timeZone: string,
  minimumMillis: number,
): Result.Result<Cron.Cron, ScheduleValidationError> => {
  const parsed = Cron.parse(expression, timeZone);

  if (Result.isFailure(parsed)) return invalid(parsed.failure.message);
  const frequency = validateCronFrequency(parsed.success, minimumMillis);

  return Result.isFailure(frequency)
    ? Result.fail(frequency.failure)
    : Result.succeed(parsed.success);
};

export const normalizeScheduleTiming = (
  request: ScheduleTimingRequest,
  nowMillis: number,
  minimumMillis: number,
): Result.Result<ScheduleTiming, ScheduleValidationError> => {
  switch (request._tag) {
    case "At":
      return Result.succeed(request);
    case "After": {
      const at = checkedInstant(nowMillis + request.delayMillis, "Relative deadline");

      return Result.map(at, (atMillis) => ({ _tag: "At" as const, atMillis }));
    }
    case "Interval": {
      if (request.everyMillis < minimumMillis) {
        return invalid(`Interval must be at least ${minimumMillis}ms`);
      }

      const anchor = checkedInstant(
        request.anchorMillis ?? nowMillis + request.everyMillis,
        "Interval anchor",
      );

      return Result.map(anchor, (anchorMillis) => ({
        _tag: "Interval" as const,
        everyMillis: request.everyMillis,
        anchorMillis,
      }));
    }
    case "Cron": {
      const timeZone = request.timeZone ?? "UTC";
      const cron = parseScheduleCron(request.expression, timeZone, minimumMillis);

      if (Result.isFailure(cron)) return Result.fail(cron.failure);

      return Result.succeed({ _tag: "Cron", expression: request.expression, timeZone });
    }
  }
};

const parsedStoredCron = (timing: Extract<ScheduleTiming, { _tag: "Cron" }>) => {
  const parsed = Cron.parse(timing.expression, timing.timeZone);

  return Result.isFailure(parsed)
    ? invalid(`Stored cron is invalid: ${parsed.failure.message}`)
    : Result.succeed(parsed.success);
};

/** Strictly selects the first occurrence after an instant. */
export const scheduleNextAfter = (
  timing: ScheduleTiming,
  afterMillis: number,
): Result.Result<number | null, ScheduleValidationError> => {
  switch (timing._tag) {
    case "At":
      return Result.succeed(timing.atMillis > afterMillis ? timing.atMillis : null);
    case "Interval": {
      if (afterMillis < timing.anchorMillis) return Result.succeed(timing.anchorMillis);
      const steps = Math.floor((afterMillis - timing.anchorMillis) / timing.everyMillis) + 1;
      const next = timing.anchorMillis + steps * timing.everyMillis;

      return checkedInstant(next, "Next interval occurrence");
    }
    case "Cron": {
      const cron = parsedStoredCron(timing);

      if (Result.isFailure(cron)) return Result.fail(cron.failure);

      return Result.try({
        try: () => {
          let next = Cron.next(cron.success, afterMillis).getTime();

          // Inside a fall fold Effect may select the first, already elapsed local instant.
          while (next <= afterMillis) {
            const following = Cron.next(cron.success, next).getTime();

            if (following <= next) throw new Error("Cron did not advance");
            next = following;
          }

          return next;
        },
        catch: () => ScheduleValidationError.make({ message: "Cron has no supported next firing" }),
      }).pipe(Result.flatMap((next) => checkedInstant(next, "Next cron occurrence")));
    }
  }
};

/** Initial cursor. Past one-shots remain due; recurring rules start strictly after registration. */
export const scheduleInitialCursor = (
  timing: ScheduleTiming,
  nowMillis: number,
): Result.Result<number | null, ScheduleValidationError> =>
  timing._tag === "At" ? Result.succeed(timing.atMillis) : scheduleNextAfter(timing, nowMillis);

export interface DueScheduleOccurrence {
  readonly intendedAtMillis: number;
  readonly nextAtMillis: number | null;
  readonly skippedRange: ScheduleSkippedRange | null;
}

/** Coalesces recurring occurrences using the same forward sequence as registration. */
export const scheduleDueOccurrence = (
  timing: ScheduleTiming,
  cursorMillis: number,
  nowMillis: number,
): Result.Result<DueScheduleOccurrence | null, ScheduleValidationError> => {
  if (cursorMillis > nowMillis) return Result.succeed(null);
  if (timing._tag === "At") {
    return Result.succeed({
      intendedAtMillis: cursorMillis,
      nextAtMillis: null,
      skippedRange: null,
    });
  }

  let intendedAtMillis: number;

  if (timing._tag === "Interval") {
    const steps = Math.floor((nowMillis - timing.anchorMillis) / timing.everyMillis);

    intendedAtMillis = Math.max(cursorMillis, timing.anchorMillis + steps * timing.everyMillis);
  } else {
    const cron = parsedStoredCron(timing);

    if (Result.isFailure(cron)) return Result.fail(cron.failure);

    const due = Result.try({
      try: () => {
        let intended = cursorMillis;
        // Reverse traversal is safe only without offset transitions. Named-zone reverse
        // traversal has different gap/fold semantics from the forward occurrence sequence.
        const zone = Option.getOrUndefined(cron.success.tz);

        if (zone !== undefined && (DateTime.isTimeZoneOffset(zone) || zone.id === "UTC")) {
          intended = Math.max(cursorMillis, Cron.prev(cron.success, nowMillis + 1_000).getTime());
        } else {
          // Seek by calendar date in UTC, where reverse traversal has no DST ambiguity.
          // Include two prior civil days to cover even IANA date-line shifts. There are no
          // matching dates between this seed day and that boundary, so forward traversal
          // visits only the seed day and the recent days, regardless of downtime length.
          const calendar = Cron.make({
            ...cron.success,
            seconds: [0],
            minutes: [0],
            hours: [0],
            tz: DateTime.zoneMakeNamedUnsafe("UTC"),
          });

          const localDay = DateTime.makeUnsafe(
            DateTime.toDate(DateTime.makeZonedUnsafe(nowMillis, { timeZone: zone })),
          ).pipe(DateTime.startOf("day"), DateTime.subtract({ days: 2 }));

          const seedDay = Cron.prev(calendar, DateTime.toEpochMillis(localDay) + 1_000);

          const seed =
            DateTime.makeZonedUnsafe(seedDay, {
              timeZone: zone,
              adjustForTimeZone: true,
            }).pipe(DateTime.toEpochMillis) - 1_000;

          if (seed > cursorMillis) {
            const first = Cron.next(cron.success, seed).getTime();

            if (first <= nowMillis) intended = Math.max(intended, first);
          }
        }
        let next = Cron.next(cron.success, intended).getTime();

        while (next <= nowMillis) {
          if (next <= intended) throw new Error("Cron did not advance");
          intended = next;
          next = Cron.next(cron.success, intended).getTime();
        }

        return { intended, next };
      },
      catch: () => ScheduleValidationError.make({ message: "Cron has no supported due firing" }),
    });

    if (Result.isFailure(due)) return Result.fail(due.failure);
    const next = checkedInstant(due.success.next, "Next cron occurrence");

    if (Result.isFailure(next)) return Result.fail(next.failure);

    return Result.succeed({
      intendedAtMillis: due.success.intended,
      nextAtMillis: next.success,
      skippedRange:
        due.success.intended === cursorMillis
          ? null
          : {
              fromMillis: cursorMillis,
              toMillis: due.success.intended,
            },
    });
  }

  const next = scheduleNextAfter(timing, nowMillis);

  if (Result.isFailure(next)) return Result.fail(next.failure);

  return Result.succeed({
    intendedAtMillis,
    nextAtMillis: next.success,
    skippedRange:
      intendedAtMillis === cursorMillis
        ? null
        : { fromMillis: cursorMillis, toMillis: intendedAtMillis },
  });
};

export const scheduleResumeCursor = (
  timing: ScheduleTiming,
  existingCursor: number | null,
  nowMillis: number,
): Result.Result<number | null, ScheduleValidationError> => {
  if (timing._tag === "At") return Result.succeed(existingCursor);

  return scheduleNextAfter(timing, nowMillis);
};

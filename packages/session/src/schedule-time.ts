import { Cron, DateTime, Option, Result } from "effect";

import {
  ScheduleValidationError,
  type ScheduleSkippedRange,
  type ScheduleTiming,
  type ScheduleTimingRequest,
} from "./schedule.ts";

const MAX_INSTANT = 8_640_000_000_000_000;

const invalid = (message: string) => Result.fail(ScheduleValidationError.make({ message }));

const checkedInstant = (millis: number, label: string) =>
  Number.isSafeInteger(millis) && millis >= 0 && millis <= MAX_INSTANT
    ? Result.succeed(millis)
    : invalid(`${label} is outside the supported ScheduleInstant range`);

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
      return Result.map(checkedInstant(next, "Next interval occurrence"), (value) => value);
    }
    case "Cron": {
      const cron = parsedStoredCron(timing);
      if (Result.isFailure(cron)) return Result.fail(cron.failure);
      return Result.try({
        try: () => Cron.next(cron.success, afterMillis).getTime(),
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

/** Coalesces due recurring occurrences without walking every missed firing. */
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
    const latest = Result.try({
      try: () => {
        const exclusiveBoundary = Math.floor(nowMillis / 1_000) * 1_000 + 1_000;
        if (exclusiveBoundary > MAX_INSTANT) {
          throw new RangeError("Cron due boundary is outside the supported range");
        }
        return Cron.prev(cron.success, exclusiveBoundary).getTime();
      },
      catch: () => ScheduleValidationError.make({ message: "Cron has no supported due firing" }),
    });
    if (Result.isFailure(latest)) return Result.fail(latest.failure);
    intendedAtMillis = Math.max(cursorMillis, latest.success);
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

# Effect DateTime

Prefer Effect `DateTime` over vanilla JavaScript `Date` for application logic. Keep `Date` as an interoperability type at external boundaries, not the domain model.

## Workflow

1. Inspect the installed `effect` version and `DateTime` source before relying on exact signatures. DateTime APIs are version-sensitive.
2. Identify whether each value represents an absolute instant, a wall-clock time in a named zone, or only a calendar date. Do not silently collapse these meanings.
3. Decode external strings, numbers, and `Date` objects at the boundary. Use `Schema.DateTimeUtc`, `Schema.DateTimeUtcFromString`, `Schema.DateTimeUtcFromMillis`, `Schema.DateTimeUtcFromDate`, or `Schema.DateTimeZoned` as appropriate.
4. Keep domain values as `DateTime.Utc` by default. Use `DateTime.Zoned` when calendar operations or presentation must retain an IANA time zone.
5. Use `DateTime.now` inside Effect programs so the current time comes from the `Clock` service. Reserve `DateTime.nowUnsafe()` and `Date.now()` for explicit synchronous host boundaries.
6. Perform comparisons, arithmetic, rounding, and formatting with `DateTime` operations, then convert to `Date` or epoch milliseconds only when an external API requires them.
7. Test time-dependent behavior with `it.effect` and `TestClock`; advance virtual time instead of sleeping or consulting the live clock.

## Construction and boundaries

- Prefer safe decoding for untrusted input. `DateTime.make` and zoned constructors return `Option`; Schema decoders produce structured parse failures.
- Use `DateTime.makeUnsafe` only for literals and values already validated by the program. Do not turn user input into defects.
- Prefer explicit ISO 8601 strings with offsets at wire boundaries. Avoid implementation-dependent or locale-formatted date strings.
- Do not invent a midnight instant for a genuinely date-only value such as a birthday. Preserve a validated `YYYY-MM-DD` or a domain-specific year/month/day structure until an actual time and zone are chosen.
- Use `DateTime.makeZonedFromString` or `Schema.DateTimeZoned` when a serialized value must preserve its zone.
- Use `DateTime.toDateUtc`, `DateTime.toEpochMillis`, and `DateTime.fromDateUnsafe` only at interop boundaries.

```ts
import { DateTime, Effect, Schema } from "effect";

const Timestamp = Schema.DateTimeUtcFromString;

const expiresAt = DateTime.makeUnsafe("2030-01-01T00:00:00Z");

const isExpired = Effect.gen(function* () {
  const now = yield* DateTime.now;
  return DateTime.isLessThanOrEqualTo(expiresAt, now);
});
```

## Instants, zones, and arithmetic

- Treat `DateTime.Utc` as an instant without retained zone information. Treat `DateTime.Zoned` as the same kind of instant plus a zone used for wall-clock parts, formatting, and zone-aware transformations.
- Use `DateTime.setZone` or `setZoneNamed` to view the same instant in another zone. When constructing from local wall-clock parts, use `makeZoned` with `adjustForTimeZone: true` and choose a deliberate `disambiguation` policy for DST gaps and repeated times.
- Use `DateTime.addDuration` and `subtractDuration` for elapsed time. Use `DateTime.add` and `subtract` for calendar arithmetic such as days, months, and years; these operations account for a zoned value's calendar rules.
- Use `DateTime.startOf`, `endOf`, or `nearest` rather than hand-editing fields. State `weekStartsOn` when week boundaries are domain-sensitive.
- Use `DateTime.Order`, `Equivalence`, `min`, `max`, `between`, and comparison helpers rather than comparing formatted strings or mutable `Date` objects.
- Use `formatIso` for UTC interchange, `formatIsoZoned` when preserving a zone, and `format` / `formatIntl` for presentation. Do not persist locale-formatted output.

## Current time and tests

Code that asks what time it is must remain clock-driven:

```ts
import { assert, it } from "@effect/vitest";
import { DateTime, Duration, Effect } from "effect";
import { TestClock } from "effect/testing";

it.effect("expires after an hour", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe("2030-01-01T00:00:00Z")));
    const startedAt = yield* DateTime.now;

    yield* TestClock.adjust("1 hour");
    const now = yield* DateTime.now;

    assert.strictEqual(Duration.toMillis(DateTime.distance(startedAt, now)), 60 * 60 * 1000);
  }),
);
```

`@effect/vitest` supplies `TestClock` to `it.effect`. Fork work that sleeps, times out, retries, or follows a schedule before advancing the clock. Use `it.live` only when a test intentionally needs real time.

## Completion check

Confirm that domain code uses `DateTime`, untrusted inputs are decoded, UTC versus zoned intent is explicit, DST behavior is deliberate, elapsed and calendar arithmetic are not confused, serialization is stable, and time-dependent tests use `TestClock` without wall-clock sleeps.

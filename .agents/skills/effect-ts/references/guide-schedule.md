# Schedule Guide

Use Effect test services and `TestClock` for schedule and polling tests. Keep
time authority in the Effect environment and avoid wall-clock sleeps.

This guide targets the `Schedule` API in Effect v4 beta.102.

Key source files:

- `packages/effect/src/Schedule.ts`
- `packages/effect/test/Schedule.test.ts`
- `packages/effect/src/Effect.ts`
- `packages/effect/src/Stream.ts`

## Mental Model

`Schedule<Output, Input, Error, Env>` describes whether an operation should
recur, how long it should wait, what it emits as schedule output, what input it
examines, and which services or failures the policy itself introduces.

Schedules drive:

- `Effect.retry` for failed effects
- `Effect.repeat` for successful effects
- `Effect.schedule` for scheduled execution
- `Stream.fromSchedule` and other stream recurrence APIs

Prefer a schedule over mutable counters, hand-written sleep loops, or recursive
retry code.

## Preferred Rule

Start with the smallest policy that expresses the requirement:

- count only: `Schedule.recurs`
- constant spacing: `Schedule.spaced`
- interval cadence: `Schedule.fixed` or `Schedule.windowed`
- growing backoff: `Schedule.exponential` or `Schedule.fibonacci`
- elapsed-time or count bound: `Schedule.upTo`
- phase change: `Schedule.andThen`
- parallel policy combination: `Schedule.max` or `Schedule.min`
- custom delay: `Schedule.modifyDelay` or `Schedule.addDelay`

Keep retryability classification in the typed error model. Keep timing,
attempt limits, and backoff in the schedule.

## Retry, Repeat, And Schedule

Use the operator that matches the control flow:

```ts
const retried = Effect.retry(loadRemote, retryPolicy);
const repeated = Effect.repeat(refreshCache, refreshPolicy);
const scheduled = Effect.schedule(runJob, nightly);
```

- `retry` steps the schedule when the effect fails; the schedule input is the
  typed error.
- `repeat` steps the schedule after success; the schedule input is the success
  value.
- `schedule` applies schedule timing to an effect directly.

## Core Constructors

### `Schedule.recurs`

Use `recurs(n)` for at most `n` recurrences. The first effect evaluation occurs
before the schedule is stepped, so `recurs(3)` permits one initial attempt and
up to three retries.

```ts
const retryThreeTimes = Schedule.recurs(3);
```

### `Schedule.forever`

`forever` recurs indefinitely with no delay. Combine it with delay or bounding
combinators unless a tight loop is intentional.

```ts
const unbounded = Schedule.forever;
```

### `Schedule.spaced`

Use `spaced` when each delay starts after the preceding action completes.

```ts
const pollEverySecond = Schedule.spaced("1 second");
```

### `Schedule.fixed`

Use `fixed` for a regular cadence that accounts for the time spent running the
action. This differs from naïve spacing when the action itself is slow.

```ts
const everyMinute = Schedule.fixed("1 minute");
```

### `Schedule.windowed`

Use `windowed` to align work to the nearest interval boundary.

```ts
const flushOnTenSecondWindows = Schedule.windowed("10 seconds");
```

### `Schedule.duration`

`duration` recurs once after the given delay, then completes.

```ts
const onceAfterOneSecond = Schedule.duration("1 second");
```

### `Schedule.during`

`during` recurs only within an elapsed-time window.

```ts
const forThirtySeconds = Schedule.during("30 seconds");
```

### `Schedule.cron`

Use `cron` for calendar-based wall-clock schedules. String parsing can fail
with `CronParseError`, so keep that failure visible where the expression is not
a trusted constant.

```ts
const nightly = Schedule.cron("0 0 * * *");
```

## Backoff

### `Schedule.exponential`

Use exponential backoff for transient external failures.

```ts
const backoff = Schedule.exponential("100 millis", 2);
```

Bound the policy explicitly:

```ts
const boundedBackoff = Schedule.exponential("100 millis").pipe(
  Schedule.upTo({ duration: "30 seconds", times: 6 }),
  Schedule.jittered,
);
```

### `Schedule.fibonacci`

Use Fibonacci backoff when growth should be gentler than exponential.

```ts
const fibonacciBackoff = Schedule.fibonacci("100 millis").pipe(Schedule.upTo({ times: 6 }));
```

### `Schedule.jittered`

Use jitter for distributed clients, workers, or polling loops that might
otherwise synchronize their retries.

```ts
const jittered = Schedule.exponential("200 millis").pipe(Schedule.jittered);
```

## Bounding Policies

Use `Schedule.upTo` to bound an existing schedule by recurrence count, elapsed
duration, or both. When both are present, the first reached limit stops the
schedule.

```ts
const bounded = Schedule.spaced("1 second").pipe(
  Schedule.upTo({
    duration: "20 seconds",
    times: 5,
  }),
);
```

Prefer `upTo` over rebuilding count and elapsed-time checks with mutable state.

## Sequencing Policies

### `Schedule.andThen`

Use `andThen` when one policy should complete before another begins.

```ts
const quickThenSlow = Schedule.exponential("100 millis").pipe(
  Schedule.upTo({ times: 3 }),
  Schedule.andThen(Schedule.spaced("5 seconds").pipe(Schedule.upTo({ times: 5 }))),
);
```

This is the v4 replacement for older examples that used `Schedule.either` to
describe retry phases.

### `Schedule.andThenResult`

Use `andThenResult` when downstream logic needs to distinguish which phase
produced the current output. It emits a `Result` carrying the phase output.

## Combining Policies

### `Schedule.max`

`max([...])` recurs only while every schedule can recur and waits for the
largest delay. Use it to enforce several stop conditions while retaining the
slowest applicable cadence.

```ts
const countedBackoff = Schedule.max([Schedule.exponential("100 millis"), Schedule.recurs(5)]);
```

### `Schedule.min`

`min([...])` continues while at least one schedule can recur and uses the
smallest available delay. Use it only when that "any policy may continue"
behavior is intended.

```ts
const fastestAvailable = Schedule.min([Schedule.spaced("1 second"), Schedule.spaced("5 seconds")]);
```

Do not treat `max` and `min` as ordinary numeric delay combinators without
considering their completion semantics.

## Transforming Delays And Outputs

### `Schedule.modifyDelay`

Use `modifyDelay` to replace the computed delay from schedule metadata.

```ts
const cappedDelay = Schedule.exponential("100 millis").pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, Duration.seconds(5))),
  ),
);
```

### `Schedule.addDelay`

Use `addDelay` to add an effectful duration to the schedule's existing delay.
For retries, inspect the typed error through `metadata.input`.

```ts
const serverAware = Schedule.spaced("1 second").pipe(
  Schedule.setInputType<RateLimitedError>(),
  Schedule.addDelay(({ input: error }) =>
    error._tag === "RateLimited" ? Effect.succeed(error.retryAfter) : Effect.succeed(Duration.zero),
  ),
);
```

### `Schedule.map`

Use `map` to transform schedule output without changing its completion or
timing behavior.

### `Schedule.passthrough`

Use `passthrough` when the schedule should emit its inputs rather than its
native outputs.

### `Schedule.tap`

Use `tap` for attempt-level observability. The callback receives metadata
including `input`, `output`, `attempt`, `elapsed`, and `duration`.

```ts
const observed = Schedule.exponential("100 millis").pipe(
  Schedule.setInputType<TransientError>(),
  Schedule.upTo({ times: 5 }),
  Schedule.tap(({ attempt, duration, input }) =>
    Effect.logWarning("retry.scheduled").pipe(
      Effect.annotateLogs({
        attempt,
        delay: String(duration),
        errorTag: input._tag,
      }),
    ),
  ),
);
```

## Advanced Construction And Inspection

Use these only when the standard constructors and combinators do not express
the policy:

- `Schedule.fromStep` and `Schedule.fromStepWithMetadata` for custom schedules
- `Schedule.toStep` and `Schedule.toStepWithMetadata` for manual stepping
- `Schedule.toStepWithSleep` when a low-level consumer should apply delays
- `Schedule.setInputType<T>()` when adapting an input-independent schedule

The old collection helpers `collectInputs`, `collectOutputs`, `collectWhile`,
`delays`, and `reduce` are not part of the current v4 Schedule API. Use
`map`, `tap`, explicit stepping, or a purpose-built accumulator instead.

## Testing Schedules

- Use `TestClock` or the test clock provided by `@effect/vitest`.
- Advance virtual time explicitly.
- Assert retry counts, outputs, and final typed failures.
- Use low-level stepping only when the schedule itself is the unit under test.
- Do not add wall-clock sleeps or timing tolerances to hide nondeterminism.

## Best Practices

1. Express recurrence policy with `Schedule`, not custom loops.
2. Bound production retries by count, duration, or both.
3. Add jitter for distributed retry behavior.
4. Use `andThen` for phases and `max`/`min` only for their documented combined
   completion semantics.
5. Inspect typed errors through schedule metadata instead of broadening them to
   `unknown`.
6. Instrument meaningful attempts with `Schedule.tap`.
7. Test timing with `TestClock`.
8. Verify constructors and combinators against the target repository's
   installed Effect version.

## Anti-Patterns

- mutable retry counters embedded in business workflows
- `Effect.forever` plus hand-written sleeps as a Schedule substitute
- unbounded retries for external calls without an explicit operational reason
- retrying authorization, validation, or other non-transient failures
- using removed v4 beta APIs copied from an older guide
- wall-clock schedule tests

## Canonical Source To Study

- `packages/effect/src/Schedule.ts`
- `packages/effect/test/Schedule.test.ts`
- `packages/effect/src/Effect.ts`
- `packages/effect/src/Stream.ts`

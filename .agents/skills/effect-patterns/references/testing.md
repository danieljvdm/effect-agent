# Effect Testing

Use this when adding tests for Effect services, workflows, and runtime
boundaries.

## Rules

Use Effect-aware test helpers when the repo provides them.

Keep tests deterministic.

Use `Layer` for injected dependencies and fakes. Avoid hidden global singletons.

Use `TestClock` or runtime test clocks for time-driven behavior. Do not use
wall-clock sleeps in green tests.

Use `Effect.exit` when asserting expected failures. Assert the tagged error type
and important payload fields.

Decode and encode protocol payloads through Schema when the boundary uses
Schema.

Keep one executable green suite. Do not land `.red.test.ts` suites or red-only
scripts.

## Avoid Copying Repo-Specific Claims

Shared guide snapshots should not include exact package-version claims from a
single repo.

Do not require agents to reread external URLs unless the target repo explicitly
owns that process.

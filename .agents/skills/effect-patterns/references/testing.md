# Effect Testing

Use this when adding tests for Effect services, workflows, and runtime
boundaries.

## Rules

Use Effect-aware test helpers when the repo provides them.

Keep tests deterministic.

Scope injected dependencies and fakes with `Layer`.

Use `TestClock` or runtime test clocks for time-driven behavior. Do not use
wall-clock sleeps in green tests.

Use `Effect.exit` when asserting expected failures. Assert the tagged error type
and important payload fields.

Decode and encode protocol payloads through Schema when the boundary uses
Schema.

Keep one executable green suite. Do not land `.red.test.ts` suites or red-only
scripts.

## Honest Test Layers

Make each test Layer's name match the behavior it provides:

- Use `Layer.succeed` for a complete static implementation.
- Add `layerTest` and a test-control service when reusable state, failure
  injection, or observation belongs to a real authority seam.
- Use `layerMemory` only when the in-memory implementation faithfully preserves
  the service's observable contract.
- Use a real local adapter when persistence, transactions, serialization, or
  protocol behavior matters.
- Keep narrow one-off fakes in their tests as local fixtures.

Exercise the same service interface as production callers. Back a service and
its reusable test-control service with the same scoped object when they share
state. Keep partial implementations local rather than advertising them as
general test or in-memory Layers.

Complete a service-test change when each substitute is complete for its
advertised name and tests observe outcomes through the public service
interface.

## Avoid Copying Repo-Specific Claims

Shared guide snapshots should not include exact package-version claims from a
single repo.

Do not require agents to reread external URLs unless the target repo explicitly
owns that process.

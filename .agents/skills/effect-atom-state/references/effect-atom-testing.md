# Deterministic Effect Atom testing

Test cache policy below React first with `AtomRegistry.make()`. Add a React
integration test for provider placement, hook behavior, browser-only SSR
boundaries, hydration, or Strict Mode ownership.

Use fake timers, a request counter, controllable Effects, and explicit mounts:

```ts
const registry = AtomRegistry.make({ defaultIdleTTL: 1_000 });
const unmount = registry.mount(queryAtom);
const first = registry.get(queryAtom);

// Flush Effect work and advance the test runner's fake timers.

unmount();
```

Use the repository's established `Effect.yieldNow` or test-clock pattern rather
than wall-clock sleeps. Prefer installed APIs and predicates such as
`AsyncResult.isSuccess`; use the cleanup returned by `registry.mount(atom)`.

## Remount reuse

1. Mount and resolve the query; assert request count `1`.
2. Unmount, advance less than idle TTL, and remount the same atom identity.
3. Assert cached success is immediately available and no fresh request occurs.

## Stale refresh

1. Resolve once through an SWR wrapper.
2. Advance past `staleTime` but not idle TTL.
3. Remount or emit the injected focus signal.
4. Assert the previous success remains with `waiting: true`, then a second
   success arrives and request count becomes `2`.
5. Prove a fresh mount or focus signal does not request.

## TTL eviction

1. Resolve and unmount.
2. Advance to just before TTL and assert reuse.
3. Advance through TTL, flush disposal, and remount.
4. Assert initial/waiting behavior and a new request.

## Polling cleanup

1. Mount the `Atom.withRefresh` wrapper and resolve once.
2. Advance one interval and assert one forced refresh.
3. Unmount, advance several intervals, and assert the counter stops changing.

## Mutation invalidation

1. Mount list and detail queries with explicit keys.
2. Run a successful mutation and assert only intended queries refresh.
3. Run a failed mutation and assert nothing invalidates.
4. Dispose the queries and assert invalidation handlers are removed.

## Strict Mode and action ownership

Render the owner inside `StrictMode`. Prove development setup-cleanup replay
does not write `Atom.Interrupt` or publish an interrupted failure before an
explicit cancellation. For multi-request actions, unmount during the sequence
and prove the chosen policy. If the atom is shared, prove unmounting one
consumer does not cancel work still owned by another.

## Aggregate stability

Mount a route atom using `AsyncResult.all`, resolve every input, unmount, and
remount inside the retention window. Assert the aggregate never returns to
`Initial`. Then expire one input and prove that input causes the reset.

For runtime/layer tests, seed `runtime.layer` through `RegistryProvider`
`initialValues` with a deterministic test layer. Replace network services
without changing the production atom graph.

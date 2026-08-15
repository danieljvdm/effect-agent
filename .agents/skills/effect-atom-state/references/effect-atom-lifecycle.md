# Effect Atom lifecycle

Treat query atoms as shared read state and mutations as actions owned by an
initiating UI or workflow. Choose registry scope, atom identity, retention,
freshness, polling, and cancellation independently.

## Registry and runtime scope

`RegistryProvider` creates its registry on first render; later option changes do
not rebuild it. Provider unmount schedules disposal after a short grace period,
while moving or keying the provider changes the cache boundary. Place one
provider around the application subtree that should share client state; nested
or route-local providers create separate caches.

The registry stores nodes, values, subscriptions, idle timers, and finalizers.
An Atom runtime supplies Effect services, and `Atom.context({ memoMap })` lets
several runtimes share layer construction. Create neither a registry nor a memo
map per query or render. Keep request-specific authentication out of a
process-global server memo map.

## Stable identity and families

Export fixed queries directly. Use `Atom.family` when a parameter selects the
resource. Prefer a primitive family argument; give object keys deliberate Effect
`Equal`/`Hash` semantics or reuse the same object. Calling a family with a stable
primitive during render already returns its stable member; an extra `useMemo`
does not improve identity.

## Three independent clocks

| Control                                         | Clock starts                 | Effect                                                                                  | It does not mean           |
| ----------------------------------------------- | ---------------------------- | --------------------------------------------------------------------------------------- | -------------------------- |
| Registry/default idle TTL or query `timeToLive` | When the atom becomes unused | Retain the cached node until idle eviction                                              | The value remains fresh    |
| `Atom.swr({ staleTime })`                       | At the latest success        | Revalidate stale data on configured mount/focus signals while showing the prior success | The node survives unmount  |
| `Atom.withRefresh(interval)`                    | While its wrapper is mounted | Force periodic refresh until disposal                                                   | Fresh requests are skipped |

Set retention long enough for the intended navigation/remount reuse window.
`staleTime` cannot rescue an already evicted node. Manual refresh, invalidation,
and polling are forceful and do not consult SWR freshness. Applying `keepAlive`
to a polling wrapper intentionally keeps that polling lifetime alive.

Atom registries are in-memory caches, not durable or shared storage. New tabs,
hard reloads, processes, and providers have separate caches. Idle TTL is not a
maximum-entry bound; finite TTLs are important for unbounded family keys.

## React mounts and action ownership

- `useAtomValue(atom)` subscribes for rendering;
- `useAtomSet(atom)` mounts a writable atom and returns a setter;
- `useAtom(atom)` subscribes and writes when one component owns both behaviors.

Use `useAtom` instead of pairing `useAtomValue` and `useAtomSet` for the same
atom in one component. The split form creates a value subscription plus a
separate mount and obscures ownership.

Unmount releases only that hook's subscription or mount. Registry eviction waits
for remaining consumers and idle TTL, then runs node finalizers. Component
unmount, registry eviction, and `Atom.Interrupt` are distinct events.

Use `Atom.Interrupt` for explicit cancellation. A cleanup write publishes an
interrupted `AsyncResult.Failure`; it is not passive unmount release, can run
during React Strict Mode replay, and can cancel work owned by another consumer.

Place a multi-request action in an owner that lives for the whole sequence. If
navigation or disconnect must not leave a partially completed fan-out, use a
stable workflow atom/service or one durable server-side command.

## Keep aggregates stable

`AsyncResult.all` returns the first non-success input, so an aggregate is only
as reusable as its least-stable input. A newly allocated or evicted input
returns to `Initial` and makes the aggregate appear reset even while other
queries remain cached.

Define every input as a module-scoped singleton or stable family member with
compatible retention. Aggregate inside a derived atom so the registry owns
recomputation; memoizing only the `AsyncResult.all` call does not repair an
unstable input atom.

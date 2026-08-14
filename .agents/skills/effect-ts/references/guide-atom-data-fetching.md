# Effect Atom Data Fetching

Model server data as stable atoms owned outside React renders. Give the application one intentional registry boundary and one compatible runtime factory; then choose cache retention, freshness, polling, and invalidation independently.

## Workflow

1. Inspect the installed `effect` and `@effect/atom-react` versions and their source before copying signatures. These APIs live under `effect/unstable/reactivity` and may move.
2. Locate every `RegistryProvider`, runtime factory, query atom, mutation atom, and route-level `AsyncResult.all`. Draw the ownership boundary before changing behavior.
3. Keep one `RegistryProvider` for the intended client application lifetime. Define the shared runtime factory, API service, query families, and mutation atoms at module scope rather than in components.
4. Choose each lifecycle control for its actual job:
   - idle `timeToLive`: retain an unused registry value before disposal;
   - `Atom.swr({ staleTime })`: decide when mount/focus revalidation is needed;
   - `Atom.withRefresh`: force periodic refresh while mounted.
5. Give queries stable identities and matching reactivity keys. Let successful mutations invalidate those keys rather than manually coordinating every consumer.
6. Keep browser-only signals behind an SSR-safe boundary. Decide explicitly whether initial data is client-only or hydrated from a request-scoped server registry.
7. Verify lifecycle behavior with fake time and request counters, not sleeps.

## Ownership rules

- Treat a query as shared read state: export one atom or `Atom.family` and let components subscribe.
- Treat an action as an event owned by the initiating UI or workflow: export the mutation atom, invoke it with `useAtomSet`, and observe its result only where useful.
- Never allocate a query atom in render. For parameterized queries, use a stable scalar or Effect `Hash`/`Equal` value as the family argument.
- Ensure every input to a route-level `AsyncResult.all` has stable atom identity and compatible retention. One newly allocated or immediately evicted input returns to `Initial` and makes the whole aggregate appear reset even when the other inputs remain cached.
- Do not describe manual refresh or polling as freshness caching. Refresh is forceful; `staleTime` only gates SWR's automatic mount/focus decisions.

## React hooks and action lifetime

Choose the hook that expresses the component's ownership:

- `useAtomValue(atom)` subscribes for rendering without returning a setter;
- `useAtomSet(atom)` mounts a writable atom and returns a setter without rendering from its value;
- `useAtom(atom)` subscribes and returns a setter when the same component both renders and writes the atom.

Do not pair `useAtomValue(atom)` with `useAtomSet(atom)` for the same atom in one component. The split form creates both a value subscription and a separate mount. Use `useAtom(atom)` instead. Calling an `Atom.family` directly with a stable primitive during render already returns the stable family member; a surrounding `useMemo` is not needed merely to preserve atom identity.

The React hooks release their subscriptions and mounts when their component lifetime ends. Registry node removal then follows the atom's idle TTL and remaining consumers, and node disposal runs the atom lifetime finalizers. Do not confuse hook cleanup, idle retention, and operation cancellation: another consumer or a nonzero TTL can intentionally keep the node and its work alive after one component unmounts.

Do not add a cleanup-only effect such as `useEffect(() => () => set(Atom.Interrupt), [set])` merely to mirror component unmount. `Atom.Interrupt` is a write that publishes an interrupted `AsyncResult.Failure`, not a passive release function. The cleanup also runs on dependency changes and during React Strict Mode's development effect replay, and it can interrupt work still owned by another consumer. Use `Atom.Interrupt` for an explicit user cancellation or a deliberately scoped cancellation policy, and test that ownership under Strict Mode.

Decide whether an action must survive its initiating component before choosing its owner. A sequential client fan-out can partially complete if route unmount disposes its owner after earlier mutations succeed but before later ones start. If the whole sequence must be durable, move it to a stable workflow owner or one server-side command instead of relying on a route component's hook lifetime.

## Completion check

Confirm one registry boundary, stable atom identity, deliberate TTL/staleness/polling values, matching query and mutation keys, an intentional React hook and action lifetime, SSR-safe browser access, and tests for every lifecycle behavior changed.

# Effect Atom cache lifecycle

## Registry and runtime scope

`RegistryProvider` creates one `AtomRegistry` on its first render. Its options do not rebuild that registry later. Provider unmount schedules disposal after a short grace period, so a quick React remount can reuse the same registry; moving or keying the provider still changes the cache boundary.

Place one provider around the client application subtree that should share data. Nested or route-local providers create separate caches.

### React mounts and registry disposal

`useAtomValue` keeps an atom active through its value subscription. `useAtomSet` and `useAtomRefresh` mount an atom through a React effect, while `useAtom` combines one value subscription with a setter. Use the combined hook when one component reads and writes the same atom; composing `useAtomValue` and `useAtomSet` adds a second mount and obscures which lifetime owns the work.

Unmount releases only that hook's subscription or mount. The registry removes a node only after it has no remaining consumers and its idle TTL permits removal. Node disposal runs registered finalizers, including cancellation of interruptible Effect work. Therefore component unmount, registry eviction, and `Atom.Interrupt` are distinct events; do not use an unconditional interrupt write as a substitute for releasing a React mount.

An atom runtime and a registry solve different problems:

- the registry stores atom nodes, values, subscriptions, idle timers, and finalizers;
- `Atom.context({ memoMap })` creates runtimes that share `Layer` construction through one `Layer.MemoMap`;
- the module-level `Atom.runtime` uses Effect's module-level default memo map.

Create one client runtime factory when several API/services must share layers:

```ts
import { Layer } from "effect";
import { Atom } from "effect/unstable/reactivity";

export const appAtomRuntime = Atom.context({
  memoMap: Layer.makeMemoMapUnsafe(),
});
```

Pass `appAtomRuntime` to each `AtomHttpApi.Service`. Do not create a memo map per query or component. On an SSR server, do not put request-specific authentication or services into a process-global memo map; use a request-scoped atom environment or keep the atom data path client-only.

## Stable identity and families

Export fixed queries directly. Use `Atom.family` when a parameter selects the resource:

```ts
export const projectAtom = Atom.family((projectId: string) =>
  ApiClient.query("projects", "get", {
    params: { projectId },
    timeToLive: "5 minutes",
    reactivityKeys: { projects: [projectId] },
  }).pipe(Atom.swr({ staleTime: "30 seconds", revalidateOnMount: true })),
);
```

The family must receive a stable key. Prefer a primitive ID. If the key is an object, give it deliberate Effect `Equal`/`Hash` semantics or reuse the same object; repeated object literals can produce distinct family entries.

## Three independent clocks

| Control                                         | Clock starts                      | What happens                                                                                         | What it does not mean                                 |
| ----------------------------------------------- | --------------------------------- | ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| Registry/default idle TTL or query `timeToLive` | When an atom becomes unused       | The registry keeps the cached node until idle eviction                                               | The value is fresh during that period                 |
| `Atom.swr({ staleTime })`                       | From the latest success timestamp | A stale value can revalidate automatically on mount or focus while the prior success remains visible | The node survives an unmount long enough to be reused |
| `Atom.withRefresh(interval)`                    | While the wrapper is mounted      | A timer force-refreshes the source and is canceled on disposal                                       | Fresh requests are skipped                            |

Set retention long enough for the navigation/remount reuse window. `staleTime` cannot rescue a source that idle eviction already removed. A common policy is a longer `timeToLive`, a shorter `staleTime`, and polling only on screens that truly need it.

Manual `registry.refresh`, `useAtomRefresh`, invalidation, and `Atom.withRefresh` are forceful. They do not consult SWR freshness. Polling stops when the polling wrapper's lifetime is disposed because its finalizer clears the timer; applying `keepAlive` to that wrapper intentionally keeps polling alive.

## The `AsyncResult.all` route reset

`AsyncResult.all` returns the first non-success input. Therefore a route aggregate is only as reusable as its least-stable input:

```ts
const routeDataAtom = Atom.make((get) =>
  AsyncResult.all({
    project: get(projectAtom("p-1")),
    // Bad if created during render or rebuilt for every route visit:
    preferences: get(makePreferencesAtom()),
  }),
);
```

If `preferences` is a fresh or evicted atom, it starts at `Initial`; the aggregate also looks initial even though `project` is cached. Fix the input's ownership and retention. Define a singleton/family atom outside render and give it a deliberate idle TTL. Memoizing only the `AsyncResult.all` call does not repair an unstable input atom.

`AsyncResult.all` also constructs a new success container. Keep aggregation inside a derived atom so the registry controls recomputation instead of rebuilding the container ad hoc in render.

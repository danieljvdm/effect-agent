---
name: effect-atom-state
description: Manage client-side state and server data in React with Effect Atom, which fills the role TanStack/React Query, SWR, Zustand, Redux, or Jotai play in other stacks — use this skill instead of reaching for those libraries or hand-rolled useEffect fetching whenever a component needs shared state, data fetching, caching, mutations, or optimistic updates. Also use when reading or dispatching atoms (useAtomValue, useAtomSet, useAtom), refactoring promise chains or useState-held server state into atom workflows, choosing reactivity keys and invalidation, deriving AtomHttpApi or HttpApiClient clients from a shared contract, deciding where the Effect→Promise boundary sits, or wiring RegistryProvider and SSR with TanStack Start.
---

# Effect Atom State

Effect Atom is the client-state and server-data layer in an Effect repository:
the role other stacks give TanStack Query, SWR, Zustand, Redux, or Jotai.
Every need that would reach for one of those libraries — or for hand-rolled
`useEffect` fetching — is atom work; never introduce a second state or query
library beside it.

Business logic stays in Effect for as long as possible. Components read
`AsyncResult` values and dispatch actions; workflows, optimistic updates, and
cache invalidation live in atoms, not in promise chains at the React boundary.

Effect Atom APIs are version-sensitive. Read the target repository's manifests
and lockfile, inspect its existing imports, and confirm exact signatures from
the installed `effect` and `@effect/atom-react` declarations before editing.
In current Effect v4 betas the atom modules live in
`effect/unstable/reactivity` and the React hooks in `@effect/atom-react`.

## Build the client state graph

1. Inventory the existing `RegistryProvider`, runtime factories,
   `AtomHttpApi.Service` clients, query atoms and families, mutation and
   workflow atoms, reactivity-key constructors, state atoms, and promise-mode
   dispatch sites. Finish when every consumer of the affected state is
   identified.
2. Read [effect-atom-client.md](references/effect-atom-client.md), then derive
   one `AtomHttpApi.Service` per contract, stable query atoms or families,
   mutation atoms, and one reactivity-key vocabulary. For non-React Effect
   code, use the direct `HttpApiClient` branch. Finish when consumers call the
   shared contract rather than redefining wire types or using ad hoc `fetch`
   for declared endpoints.
3. Read [effect-atom-workflows.md](references/effect-atom-workflows.md), then
   express every multi-step action — mutate then invalidate, optimistic echo
   then rollback — as an `Atom.fn` effect composing other atoms through the fn
   context. Finish when no component or route chains `.then`/`.catch` on a
   dispatch and no `useState` holds state a workflow atom must own.
4. Read [effect-atom-lifecycle.md](references/effect-atom-lifecycle.md) when
   changing registry scope, atom identity, retention, freshness, polling,
   cancellation, or aggregate stability.
5. Read [effect-atom-testing.md](references/effect-atom-testing.md), then give
   changed atom behavior deterministic coverage below React first, with a
   deterministic HTTP layer so request encoding, invalidation, and lifecycle
   remain observable. Run the repository's format, lint, typecheck, and test
   commands. Finish when changed queries, mutations, invalidation, and
   workflow atoms have deterministic tests and every repository check passes.

## Optional branches

- Read [tanstack-start.md](references/tanstack-start.md) when the client is
  TanStack Start, SSR, hydration, `ClientOnly`, loaders, server functions, or a
  proxied separate API.
- Use the `$build-effect-apis` skill when the change reaches the contract or
  server: shared `HttpApiEndpoint`/`HttpApiGroup` definitions, handlers,
  middleware, or runtime assembly.

## Keep the Promise boundary logic-free

The Effect→Promise boundary sits at the outermost edge and carries no logic.

- Return a promise-mode dispatch (`useAtomSet(action, { mode: "promise" })`)
  bare to a leaf component whose contract is promise-shaped — a pending
  button, a composer that owns its in-flight state. A `.then` or `.catch`
  chain in a component or route is a violation: move that logic into the
  action's Effect.
- Express multi-step workflows as `Atom.fn` effects composing other atoms
  through the fn context: `get.setResult` awaits another fn atom, `get.set`
  writes state atoms. Reads through the fn context callable are untracked, so
  mutating a state atom from inside the effect cannot re-trigger the workflow.
- Declare cross-query invalidation as reactivity keys on the mutation; never
  chain a manual refresh at a call site. When several `AtomHttpApi` services
  share one Atom runtime, one `Reactivity` instance spans them, so a mutation
  on one client invalidates another client's query keys.
- Keep optimistic UI state in `Atom.family` state atoms keyed by the entity,
  not `useState`, so the workflow atom that writes it owns its lifecycle.
- Genuine view state — controlled inputs, open/closed toggles, reconciling
  optimistic rows against rendered props — stays in React; do not force it
  into Effect.

A repository may reinforce the boundary with a lint warning on `then` scoped
to component and route modules, with a documented local suppression for a
genuinely promise-shaped contract, but the boundary reasoning remains the
source of truth.

## Boundary rules

- Let client data modules own API services, query identity, cache policy,
  invalidation keys, mutation atoms, and workflow atoms.
- Let workflow atoms own orchestration, optimistic echo, rollback, and
  cross-query invalidation.
- Let UI action owners own navigation, toasts, form reset, and presentation
  derived from `AsyncResult` state.
- Let React own view state that no atom needs to write.

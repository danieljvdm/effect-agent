# Effect Atom state

Effect Atom owns shared client state and server data in an Effect application.
React renders results and dispatches actions. Keep workflows, optimistic updates,
and cross-query invalidation in Effect; keep view-local state in React.

Read the repository's Effect instructions and inspect installed declarations for
the APIs being changed. Preserve existing runtime and registry ownership.

## Read for the changed boundary

- API clients, queries, or mutations: [client state](effect-atom-client.md).
- Multi-step actions, optimistic updates, or Promise dispatch:
  [workflows](effect-atom-workflows.md).
- Registry scope, atom identity, retention, freshness, polling, or cancellation:
  [lifecycle](effect-atom-lifecycle.md).
- TanStack Start loaders, SSR, hydration, or server functions:
  [TanStack Start](tanstack-start.md).
- A regression test allowed by repository policy or explicitly requested by the
  user: [testing](effect-atom-testing.md). Read only the relevant cases.

## Ownership constraints

- Derive clients from the shared HTTP contract. Change contracts and server
  handlers with the [HTTP API guide](../api/index.md) when that work is in scope.
- Declare cross-query invalidation as mutation reactivity keys. Queries and
  mutations that must invalidate each other need the same Reactivity instance.
- Compose multi-step actions in workflow atoms. Return Promise-mode dispatch
  directly to a component with a Promise-shaped contract; put success, failure,
  and sequencing logic in the Effect workflow.
- Keep optimistic state with the workflow that writes it, keyed by entity where
  needed. Preserve ordinary controlled inputs and view-local toggles in React.
- Reuse the application's state layer instead of introducing another query or
  shared-state library for a single component.

## Verify

Follow the repository's command authority and testing policy. Exercise the
changed user flow and the relevant invalidation or lifecycle behavior. Reuse
existing checks; changed atoms do not by themselves justify committed tests.

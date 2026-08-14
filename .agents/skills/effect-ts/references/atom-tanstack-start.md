# Effect Atom TanStack Start integration

TanStack Start code is isomorphic by default, including route loaders. Treat every module used by a route as server-capable unless an explicit boundary says otherwise.

## Choose an SSR strategy

Use one of these models deliberately:

### Client-only atom data

- Put one `RegistryProvider` in the app/root component so client navigations share a registry.
- Render atom consumers that touch browser-only APIs inside `ClientOnly` from `@tanstack/react-router`.
- Accept that the fallback is the server-rendered state and fetching begins on the client.
- Keep the client runtime and API service as module singletons.

```tsx
import { RegistryProvider } from "@effect/atom-react";
import { ClientOnly } from "@tanstack/react-router";

export function AppShell() {
  return (
    <RegistryProvider defaultIdleTTL={30_000}>
      <ClientOnly fallback={<DashboardSkeleton />}>
        <Dashboard />
      </ClientOnly>
    </RegistryProvider>
  );
}
```

### SSR plus hydration

- Create the registry and any request-specific runtime/layers per request; never share user/auth state through a process-global registry or memo map.
- Give decoded query atoms deterministic `serializationKey` values.
- Mount/run the required serializable atoms on the server, dehydrate only the intended values, and pass them through the document safely.
- Create the client registry once, then hydrate matching atom identities before descendants consume them. Use `HydrationBoundary` where the installed React adapter supports it.
- Verify that server and client construct the same API service, family arguments, and serialization keys.

Prefer framework loaders/server functions when they already own SSR data. Do not build a second atom SSR cache merely to mirror loader data; seed atoms from the loader or keep atom fetching client-only.

## Focus is browser-only

`Atom.windowFocusSignal` reads `window` and `document.visibilityState` when mounted. Do not mount it during SSR. Put focus-enabled consumers behind `ClientOnly`, or inject a no-op server signal and the browser signal on the client.

```ts
const project = projectAtom(projectId).pipe(
  Atom.swr({
    staleTime: "30 seconds",
    revalidateOnFocus: true,
    focusSignal: Atom.windowFocusSignal,
  }),
);
```

`revalidateOnFocus: true` respects `staleTime`; `"always"` forces a request on every focus signal.

## In-memory limits

Effect Atom's registry cache is in memory and scoped to that registry:

- a new tab, hard reload, server process, or newly created provider starts another cache;
- idle TTL evicts only unused atoms and is not a maximum-entry or byte-size bound;
- `keepAlive` and infinite TTL can grow memory with unbounded family keys;
- browser memory is not durable or shared across users/devices;
- hydration transfers a snapshot, not a persistent distributed cache.

For large or unbounded parameter spaces, use finite TTLs and avoid `keepAlive`. Put durable/shared caching at the HTTP, server, CDN, or database layer.

Primary TanStack references: [execution model](https://tanstack.com/start/latest/docs/framework/react/guide/execution-model) and [`ClientOnly`](https://tanstack.com/router/latest/docs/api/router/clientOnlyComponent).

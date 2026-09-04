# TanStack Start integration

TanStack Start modules are isomorphic unless an explicit boundary says
otherwise. Choose one data-execution model before wiring Effect Atom.

## Client-only atom data

Use this when identity, focus signals, or other dependencies require browser
globals.

1. Mount one `RegistryProvider` around the application subtree whose client
   navigations should share cache state.
2. Put browser-dependent atom consumers behind `ClientOnly`.
3. Keep the runtime, `AtomHttpApi.Service`, query families, and mutations as
   client module singletons.
4. Render a useful server fallback and accept that API fetching begins after
   hydration.

```tsx
import { RegistryProvider } from "@effect/atom-react";
import { ClientOnly, Outlet } from "@tanstack/react-router";

export function Root() {
  return (
    <RegistryProvider defaultIdleTTL={60_000}>
      <ClientOnly fallback={<AppSkeleton />}>
        <Outlet />
      </ClientOnly>
    </RegistryProvider>
  );
}
```

`Atom.windowFocusSignal`, `localStorage`, `window`, and `document` belong in
this client-only branch.

## SSR or loader-owned data

Prefer TanStack loaders or server functions when they already own SSR data.
Pass loader data into the client state graph or keep the query loader-owned;
avoid maintaining two independent server caches for the same request.

When atom SSR is intentional:

- create the registry and request-specific runtime/layers per request;
- give serializable queries deterministic `serializationKey` values;
- mount or run only the intended serializable atoms on the server;
- dehydrate only intended values;
- create the browser registry once and hydrate matching atom identities before
  descendants read them;
- use `HydrationBoundary` when the installed React adapter supports it.

Process-global registries or memo maps can leak request-specific authentication
and server state across users. Verify request isolation with concurrent SSR
tests.

## Focus is browser-only

`Atom.windowFocusSignal` reads `window` and `document.visibilityState` when
mounted. Keep focus-enabled consumers behind `ClientOnly`, or inject a no-op
server signal and the browser signal on the client. `revalidateOnFocus: true`
respects `staleTime`; `"always"` forces a request on each focus signal.

## Separate API development

When TanStack Start and the Effect API run as separate local processes, proxy a
stable prefix and WebSocket upgrades through the app dev server:

```ts
export default defineConfig({
  plugins: [tanstackStart(), react()],
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8787",
        changeOrigin: true,
        ws: true,
      },
    },
  },
});
```

Keep the client base URL aligned with proxy and production routing. If
credentials cross origins, configure CORS and cookie/header behavior at the API
edge and test the deployed topology, not only the same-origin development path.
